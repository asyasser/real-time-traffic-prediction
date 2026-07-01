# Real-Time Traffic Prediction — Infrastructure Setup

## What this stage covers
Set up the local infrastructure (via Docker) that the streaming pipeline 
will run on: Kafka (message broker), Redis (real-time data store), 
MongoDB (historical data store), and Grafana (dashboards).

## Environment
- WSL2 Ubuntu (noble/24.04)
- Docker Engine + Docker Compose plugin installed directly (no Docker Desktop)

## Services (docker-compose.yml)

| Service | Purpose | Port | Notes |
|---|---|---|---|
| **kafka** | Message broker — receives replayed sensor readings | 9092 | KRaft mode (no Zookeeper). Topic `traffic-raw` created. |
| **redis** | Real-time store — latest predictions for live dashboard | 6379 | Persistence enabled (appendonly) |
| **mongo** | Historical store — predictions over time for reporting | 27017 | Single-node replica set `rs0` (required for Spark connector) |
| **mongo-init** | One-shot job to initialize the Mongo replica set | - | Runs once on first `up`, harmless to re-run |
| **grafana** | Dashboards — realtime (from Redis) + historical (from Mongo) | 3000 | Anonymous admin access enabled for dev |

## Key setup notes / gotchas
- Kafka's `CLUSTER_ID` must be a valid base64 UUID (generate with 
  `docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid`), 
  not an arbitrary string — Kafka will exit on startup otherwise.
- Mongo replica set is confirmed healthy via `rs.status()` → `PRIMARY`, `ok: 1`.
- All services use named Docker volumes — data persists across restarts. 
  Avoid `docker compose down -v` (deletes volumes).

## How to bring it up / down
```bash
docker compose up -d      # start everything
docker compose ps -a      # check status
docker compose down       # stop (keeps data)
```

## Verification commands
```bash
docker exec -it redis redis-cli ping                          # -> PONG
docker exec -it mongo mongosh --eval "rs.status()"             # -> ok: 1
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092  # -> traffic-raw
```

## Status: ✅ Infrastructure verified working

## Next step
Build the Kafka producer (`kafka/producer/producer.py`) to replay the 
test set into the `traffic-raw` topic, simulating live sensor readings.


---

## Stage 2: Kafka Producer ✅

`kafka/producer/producer.py` replays `data/metr_la_TEST_raw.parquet` 
(6855 timestamps × 207 sensors) into the `traffic-raw` topic.

- One JSON message per timestamp: `{"timestamp": "...", "speeds": {sensor_id: value, ...}}`
- Time features (hour/dow/weekend/rush-hour) NOT sent — Spark recomputes from timestamp
- Configurable replay speed via `--interval` (seconds between messages)

### Setup
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r kafka/producer/requirements.txt
```
Note: use `kafka-python-ng` (not `kafka-python`) — the original is broken on Python 3.12.

### Run
```bash
cd kafka/producer
python3 producer.py --interval 0.5
```

### Verify
```bash
docker exec -it kafka kafka-console-consumer --topic traffic-raw \
  --bootstrap-server localhost:9092 --from-beginning --max-messages 3
```

## Status: ✅ Verified — messages flowing correctly into Kafka

## Next step
Build the Spark Structured Streaming consumer (`spark/streaming_job.py`):
read from Kafka, recompute time features, scale, build 12-step sliding 
windows per sensor, run BLSTM inference, write to Redis + Mongo.

---

## Stage 3: Spark Structured Streaming Pipeline ✅

`spark/streaming_job.py` implements the full streaming inference pipeline using
PySpark Structured Streaming with `foreachBatch` processing.

### Dependencies
- Java 17 (OpenJDK) — required by Spark
- PySpark 3.5.1
- PyTorch 2.3.1
- scikit-learn 1.6.1 (must match version used to save scaler)
- joblib 1.4.2
- redis 5.0.4
- pymongo 4.7.2
- setuptools (distutils shim required for PySpark + Python 3.12 compatibility)

Install:
```bash
sudo apt install -y openjdk-17-jdk
source venv/bin/activate
pip install -r spark/requirements.txt
pip install setuptools
```

### Architecture: foreachBatch approach
Rather than using Spark's native stateful operators (`flatMapGroupsWithState`),
all processing happens inside a `foreachBatch` callback operating on Pandas
DataFrames per micro-batch. This simplifies debugging and makes PyTorch
inference trivial while keeping the Spark layer responsible for Kafka
consumption and micro-batch scheduling.

### Processing pipeline (per micro-batch)
Each micro-batch goes through the following steps inside `process_batch()`:

**1. Kafka deserialization**
Spark reads raw JSON messages from the `traffic-raw` topic and parses them
using a defined schema: `{timestamp: string, speeds: map<string, double>}`.

**2. Pandas conversion**
The Spark DataFrame is converted to Pandas via `toPandas()` for downstream
Python-native processing.

**3. Speeds map flattening**
The `speeds` map column (207 sensor_id → speed pairs) is expanded into 207
individual columns, enforcing the exact column order defined in
`data/sensor_ids.json`. This order must match the order used during training.

**4. Time feature recomputation**
Six cyclical/binary time features are recomputed from the timestamp:
- `hour_sin`, `hour_cos` — sin/cos encoding of hour of day (0–23)
- `dow_sin`, `dow_cos` — sin/cos encoding of day of week (0–6)
- `is_weekend` — 1 if Saturday/Sunday
- `is_rush_hour` — 1 if 7–9am or 4–7pm

These are NOT sent over Kafka (they're deterministic from timestamp) and are
recomputed in Spark to keep the producer lean.

**5. MinMaxScaler normalization**
`data/scaler.pkl` (sklearn MinMaxScaler fitted on train speed columns only)
is loaded once at startup and applied to the 207 speed columns per batch.
Time features are left unscaled — matching the training pipeline exactly.
scikit-learn version must be 1.6.1 to match the saved scaler version.

**6. Sliding window buffer**
A module-level `deque(maxlen=12)` accumulates scaled feature vectors
(213-dim: 207 speeds + 6 time features) row by row. Once the buffer reaches
12 steps (1 hour of history at 5-min intervals), inference is triggered on
every subsequent row (sliding window, step=1).

**7. BLSTM inference**
The 12-step buffer is stacked into a `(1, 12, 213)` PyTorch tensor and fed
to the BLSTM model loaded from `models/blstm.pth`.

Model architecture:
- 2-layer Bidirectional LSTM, 128 hidden units, dropout=0.1
- Input: `(batch, 12, 213)`
- Output: `(batch, 3, 207)` — 3 prediction horizons × 207 sensors

**8. Inverse transform**
Predictions (in [0,1] scaled space) are inverse-transformed using
`scaler.inverse_transform()` back to mph for human-readable output and
downstream sink writes.

### Key implementation notes
- `startingOffsets: latest` — Spark only reads new messages, not replayed history
- Model layer named `self.lstm` (not `self.blstm`) to match the key names
  saved in the `.pth` state dict from training
- `torch.load(..., map_location="cpu")` — inference runs on CPU (no GPU needed
  for single-record streaming inference)
- Ctrl+C shutdown errors (Py4J network errors) are normal and harmless

### Run
```bash
# Terminal 1 — Spark job
cd spark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming_job.py

# Terminal 2 — Kafka producer
cd kafka/producer
source ../../venv/bin/activate
python3 producer.py --interval 0.5
```

### Sample inference output

Model loaded successfully
Timestamp: 2012-06-04 06:55:00

Buffer full (12 steps) — inference run

Pred shape: (3, 207)

Sensor 773869 — 5min: 66.84 mph | 10min: 67.18 mph | 15min: 67.17 mph
Timestamp: 2012-06-04 07:00:00

Buffer full (12 steps) — inference run

Pred shape: (3, 207)

Sensor 773869 — 5min: 66.31 mph | 10min: 67.16 mph | 15min: 67.06 mph

### Status: ✅ Full inference pipeline verified working

### Next step
Write prediction results to:
- **Redis** — Hash per sensor for real-time Grafana dashboard
- **MongoDB** — predictions_history collection for historical reporting


---

## Stage 4: Redis + MongoDB Sinks ✅

### Overview — what happens after inference

Every time the sliding window buffer fills up (12 consecutive timesteps),
the BLSTM model produces a prediction for that moment: speeds 5, 10, and 15
minutes into the future, for all 207 sensors at once. This single prediction
"event" then gets written to two different databases, because they serve two
completely different purposes:

- **Redis** answers the question: *"What is happening on the network right
  now?"* — it only ever holds the most recent values, refreshed every tick.
- **MongoDB** answers the question: *"How has the network/model behaved over
  time?"* — it keeps every single prediction ever made, forever (until
  manually cleared), so we can analyze trends, compute errors, and build
  historical reports.

Both writes happen inside the same `foreachBatch` function in
`streaming_job.py`, right after inference. The function `write_to_redis(...)`
and `write_to_mongo(...)` are called with the same five arguments:
`timestamp, sensor_ids, actual_speeds, pred_5min, pred_10min, pred_15min`.

---

### Redis — real-time snapshot store

File: `spark/sinks/redis_sink.py`

Redis is an in-memory key-value database. Think of it as a giant dictionary
that lives in RAM — extremely fast to read/write, but data can expire or
disappear (which is actually a *feature* here, not a bug — explained below).

We store three kinds of keys:

**1. Per-sensor "latest" hash — `sensor:{sensor_id}`**

A "hash" in Redis is like a small dictionary/struct attached to one key.
For sensor `773869`, the key `sensor:773869` holds:

| Field | Meaning |
|---|---|
| `ts` | Timestamp of this reading (e.g. `2012-06-04T08:55:00`) |
| `actual` | The real observed speed at this timestamp (mph) |
| `pred_5min` | Model's prediction for 5 minutes ahead (mph) |
| `pred_10min` | Model's prediction for 10 minutes ahead (mph) |
| `pred_15min` | Model's prediction for 15 minutes ahead (mph) |

Every time a new prediction is made, this hash is **completely overwritten**
with the new values — it never accumulates history. So at any moment, this
key tells you "as of right now, sensor 773869 is doing X mph, and the model
thinks it'll be doing Y mph in 15 minutes."

This is what Grafana's real-time dashboard reads to build live gauges,
tables, or a map showing current sensor states.

**Why TTL (Time To Live)?** We attach a 600-second (10-minute) expiry to this
key (`pipe.expire(key, TTL_SECONDS)`). Every time the key is updated, the
expiry timer resets. If the streaming pipeline ever stops (crashes, you hit
Ctrl+C, etc.), no new updates arrive, the TTL eventually runs out, and the key
disappears from Redis entirely. This means the live dashboard will show
"no data" instead of frozen, increasingly-stale numbers from a dead pipeline
— which is the correct behavior for something labeled "real-time."

**2. Per-sensor rolling trend list — `sensor:{sensor_id}:trend`**

This is a Redis "list" (an ordered array). Each new prediction gets pushed to
the front (`lpush`), and the list is trimmed (`ltrim`) to keep only the most
recent 24 entries. At 5-minute intervals, 24 entries = 2 hours of history.

Each entry is a small JSON string:
```json
{"ts": "2012-06-04T08:55:00", "actual": 67.33, "pred_15min": 65.42}
```

This gives Grafana enough recent history to draw a short "sparkline" trend
chart for a sensor (e.g. "last 2 hours, actual vs predicted") without needing
to query MongoDB — keeping the real-time dashboard fast and self-contained.
TTL here is 1200s (20 min) — slightly longer than the per-sensor hash, since
it holds more data and we want it to persist briefly even through short gaps.

**3. Network-wide summary — `network:summary`**

A single hash (not per-sensor) summarizing the entire network at this moment:

| Field | Meaning |
|---|---|
| `ts` | Timestamp of this reading |
| `avg_actual` | Average actual speed across all 207 sensors (mph) |
| `avg_pred_15min` | Average 15-min-ahead prediction across all sensors (mph) |
| `n_congested` | Count of sensors currently below 35 mph (congestion threshold) |
| `n_sensors` | Total sensor count (207) — useful for computing percentages in Grafana |

This is what feeds top-level "KPI" style panels — e.g. big numbers showing
"Network avg speed: 55 mph" or "26 / 207 sensors congested."

**How the write actually happens (pipeline)**

All these writes are batched into a single Redis "pipeline" — instead of
sending 207 sensors × 3 operations (hash + trend list + expiry) as separate
network round-trips, they're queued up and sent to Redis in one go
(`pipe.execute()`). This is much faster for ~600+ operations per tick.

---

### MongoDB — historical record store

File: `spark/sinks/mongo_sink.py`

MongoDB is a document database — instead of rows/columns like a spreadsheet,
it stores flexible JSON-like "documents." We use one collection,
`predictions_history`, in database `traffic_db`.

**One document per timestamp** — every time the model makes a prediction, a
new document is *inserted* (never overwritten, never deleted). Over the
6855-timestamp test set, this collection will grow to ~6843 documents (6855
minus the first 12 used to fill the initial window).

**Document structure:**

```json
{
  "_id": ObjectId("..."),               // auto-generated by Mongo
  "timestamp": "2012-06-04T11:35:00",   // this prediction's timestamp
  "sensors": {
    "716328": {
      "actual": 54.375,
      "pred_5min": 58.50,
      "pred_10min": 58.56,
      "pred_15min": 58.57
    },
    "716331": { ... },
    ... (all 207 sensors)
  },
  "network_avg_actual": 55.15,
  "network_avg_pred_5min": ...,
  "network_avg_pred_10min": ...,
  "network_avg_pred_15min": 56.44
}
```

Each document is essentially "a complete snapshot of the network's actual and
predicted state at one point in time" — both per-sensor detail (for deep
dives) and network-wide aggregates (for quick overview charts).

**Why no pre-computed error fields?**

You might expect fields like `error_5min` or `mae` to be stored directly.
We deliberately don't do this, for a structural reason: at the moment a
prediction is made (e.g. "at 11:35, sensor X will be doing 58.5 mph in 5
minutes"), the *actual* value for 11:40 doesn't exist yet — it arrives in a
*future* document (the one timestamped 11:40).

So computing "how wrong was the 5-min-ahead prediction made at 11:35?"
requires comparing:
- `predictions_history` document at `timestamp = 11:35`, field `sensors.X.pred_5min`
- `predictions_history` document at `timestamp = 11:40`, field `sensors.X.actual`

This is a **join across two documents** — naturally expressed as a MongoDB
aggregation query (or directly in a Grafana panel query) at dashboard-render
time, rather than something we can compute and store at write-time. This
keeps the write path simple and fast, and gives full flexibility later:
error-over-time, error-by-horizon, error-by-sensor, error-by-time-of-day —
all just different aggregation queries over the same raw collection.

**Index on `timestamp`**

```python
db[COLLECTION_NAME].create_index("timestamp")
```

This is created once when the sink first connects. Without it, every
time-range query (e.g. "give me all documents from the last 24 hours") would
require scanning the entire collection. With the index, MongoDB can jump
directly to the relevant range — essential since this collection grows by
one document every ~0.5-2 seconds during replay (depending on `--interval`).

---

### Connectivity fix: Mongo replica set + WSL host

**The problem:** When `mongo-init` ran `rs.initiate()` inside the Docker
network, it registered the replica set's only member using the
Docker-internal hostname `mongo:27017`. When our PySpark job (running
directly on the WSL host, not in a container) tries to connect with
`replicaSet=rs0`, PyMongo first connects, discovers the replica set topology,
sees the member is called `mongo:27017`, and tries to reconnect to that
address — but `mongo` doesn't resolve to anything on the host system, only
inside the Docker network. Connection fails with `ServerSelectionTimeoutError`.

**The fix:** Add a line to `/etc/hosts` on the WSL host: 127.0.0.1 mongo

Now, when PyMongo's topology discovery returns `mongo:27017` as the member
address, the host's DNS resolution maps `mongo` → `127.0.0.1`, which is where
Mongo's port `27017` is actually exposed (via `docker-compose.yml`'s `ports:`
mapping). Connection succeeds, and we keep the semantically correct
connection string:
```python
MONGO_URI = "mongodb://localhost:27017/?replicaSet=rs0"
```

---

### How it all fits together (one full cycle)

1. Producer sends one timestamp's worth of sensor readings to Kafka
2. Spark reads it, flattens to 207 columns + 6 time features, scales it
3. The scaled row is appended to the 12-step buffer
4. If the buffer is full: BLSTM runs inference → 3 horizons × 207 sensors
5. Predictions are inverse-scaled back to mph
6. `write_to_redis(...)`:
   - Overwrites `sensor:{id}` hash (latest snapshot, TTL 10min)
   - Pushes to `sensor:{id}:trend` list (rolling 2h window, TTL 20min)
   - Overwrites `network:summary` hash (TTL 10min)
7. `write_to_mongo(...)`:
   - Inserts one new document into `predictions_history` (permanent)
8. Repeat for the next timestamp

### Verification commands
```bash
# Latest snapshot for one sensor
docker exec -it redis redis-cli HGETALL sensor:773869

# Network-wide summary
docker exec -it redis redis-cli HGETALL network:summary

# Rolling trend (last entries) for one sensor
docker exec -it redis redis-cli LRANGE sensor:773869:trend 0 -1

# Most recent historical document
docker exec -it mongo mongosh traffic_db --eval "db.predictions_history.find().sort({timestamp:-1}).limit(1).pretty()"

# Total documents stored so far
docker exec -it mongo mongosh traffic_db --eval "db.predictions_history.countDocuments()"
```

## Status: ✅ Verified — predictions flowing into both Redis and MongoDB

## Next step
Build Grafana dashboards:
- **Real-time tab** (Redis datasource) — live sensor map/gauges/table from
  `sensor:{id}` hashes, network KPIs from `network:summary`, short trend
  sparklines from `sensor:{id}:trend`
- **Historical tab** (Mongo datasource) — error-over-time, error-by-horizon,
  per-sensor performance, time-of-day breakdowns, all via aggregation queries
  over `predictions_history`


  ---

## Stage 5: Grafana Real-Time Dashboard (Redis) ✅

### What this stage covers

Connected Grafana to Redis and built the first two panels of the real-time
dashboard — a "right now" view of the network and a per-sensor prediction
gauge. This is the visualization layer for the data that's been flowing into
Redis since Stage 4.

### Setting up the Redis datasource

In Grafana (`http://localhost:3000`), under **Connections → Data sources →
Add data source**, the **Redis** plugin was added with:
- **Type**: Redis (the plain option — NOT RedisJSON, RedisGraph, RedisGears,
  RedisSearch, or RedisTimeSeries, which are for separate Redis modules we
  don't use)
- **Connection mode**: Standalone (single Redis instance, no cluster/sentinel)
- **Address**: `redis:6379` — this is the Docker-internal hostname, since
  Grafana and Redis are both containers on the same docker-compose network
  and can reach each other by service name

Test succeeded immediately — no extra configuration needed.

### Panel 1 — Network Summary (the "KPI strip")

**What it shows**: a single panel with 5 numbers/values that summarize the
entire 207-sensor network at the current moment in the replay:
- `ts` — the timestamp this data represents (e.g. `2012-06-08T05:30:00`)
- `avg_actual` — average current speed across all 207 sensors (mph)
- `avg_pred_15min` — average predicted speed 15 minutes ahead (mph)
- `n_congested` — how many of the 207 sensors are currently below 35mph
- `n_sensors` — always 207, included for context/percentage calculations

**How it's built**:
- Visualization type: **Stat** (big number display)
- Query: `hgetall network:summary` — fetches the entire hash in one go
- Important setting: under "Value options", **Fields** was changed from
  the default ("Numeric Fields" — which would hide `ts` since it's text) to
  **"All Fields"**, so the timestamp string displays alongside the numbers

**Why showing `ts` matters even on a "real-time" dashboard**: it's a sanity
check (confirms data is fresh / pipeline is running) and gives context for
which point in the 2012 replay we're currently viewing.

**Refresh**: dashboard-level auto-refresh set to **5 seconds** (the fastest
option available), so this panel updates automatically without manual clicks.

### Panel 2 — Sensor Predictions (the "drill-down" view)

**What it shows**: for one chosen sensor, a Gauge visualization with three
circles showing the model's predictions for that sensor 5, 10, and 15
minutes into the future (in mph). The panel title dynamically shows which
sensor is selected.

**How it's built**:
- Visualization type: **Gauge** (3 circular gauges, one per field returned)
- Query: `hmget sensor:$sensor_id pred_5min pred_10min pred_15min`
  - `hmget` (vs `hgetall`) was used specifically to fetch only these 3
    fields — this excludes the `actual` field, which we didn't want shown
    in this panel
  - `$sensor_id` is a **dashboard variable** (see below) — Grafana
    substitutes it with whichever sensor is currently selected
- Panel title: `Sensor $sensor_id — Predictions` — also uses the variable,
  so the title text itself updates when you change sensors

**Dashboard variable — `sensor_id`**:
Created via dashboard **Settings → Variables → Add variable**:
- Name: `sensor_id`
- Type: **Custom** (a manually-typed comma-separated list of sensor IDs)
- This creates a dropdown at the top of the dashboard — selecting a
  different sensor ID automatically re-runs Panel 2's query with the new ID

Currently the variable's list contains only a handful of sample sensor IDs.
It can later be expanded to all 207, or converted to a **Query**-type
variable that pulls IDs dynamically from Redis/data.

### Dead ends and key decisions (worth understanding for the report)

**1. Attempted: a "trend over time" line chart per sensor (Redis Lists)**

The original plan was a 3rd panel: a line chart showing how a sensor's
predicted speed evolved over the last ~2 hours, using a rolling Redis List
(`sensor:{id}:trend`, populated via `lpush`/`ltrim` in `redis_sink.py`).

This failed because **the Grafana Redis plugin's command list does not
include `lrange`** — the command needed to read a range of values from a
Redis List. The plugin supports `llen` (list length) but nothing to actually
read list contents in a way Grafana can chart.

**2. Attempted: switch to Redis Streams (`xadd`/`xrange`)**

Redis Streams ARE supported by the plugin (`xrange`, `xrevrange`, `xlen`,
`xinfo stream`) and are purpose-built for time-ordered data — so
`redis_sink.py` was updated to use `xadd` with `maxlen` (auto-trimming)
instead of `lpush`/`ltrim`.

This got further — the panel rendered a chart with real data points — but
hit a **timestamp mismatch problem**: Redis Stream entry IDs are
auto-generated based on the **wall-clock time when the entry was written**
(i.e., 2026, whenever the pipeline happens to be running). But the actual
*data* represents **2012 timestamps** (stored in the `ts` field of each
entry). Grafana's time-range picker filters by Stream entry ID (2026 time),
not by the `ts` field inside each entry (2012 time) — so the dashboard's
time range and the data's actual timestamps never lined up, producing
"Data outside time range" errors.

**3. Decision: drop Redis-based trend charts entirely**

Rather than fight this mismatch, trend-over-time charts were moved
conceptually to **MongoDB** (Stage 6, not yet built). Each MongoDB document
in `predictions_history` has a `timestamp` field containing the actual 2012
date/time from the data itself — Grafana's time-range filtering works
correctly against this, since it's a real timestamp field in the data, not
an auto-generated write-time ID.

**Practical implication**: the real-time (Redis) dashboard now ONLY shows
**current-moment snapshots** — "what is true right now" — with no history or
trends. All historical/trend visualization happens via MongoDB.

**4. Decision: no real-time error metric**

Computing prediction error in real-time would require comparing a
prediction made N minutes ago (for the current timestamp) against the
actual value that just arrived — which means looking backward through
history. Since trend/history now lives in MongoDB anyway, error metrics
(MAE, MAPE, per-horizon, etc.) will be computed there via aggregation
queries (as already planned in Stage 4), not duplicated in Redis.

**5. `WRONGTYPE` errors during experimentation**

When switching `sensor:{id}:trend` from a List (`lpush`) to a Stream
(`xadd`), old keys still existed in Redis as Lists — Redis refuses to run
Stream commands on a key that's already a different type. Fixed with:
```bash
docker exec -it redis redis-cli FLUSHALL
```
This is safe because all Redis data here is ephemeral/regenerable —
restarting the pipeline repopulates everything from scratch.

### Current dashboard state
- 2 working panels: Network Summary (Stat) + Sensor Predictions (Gauge)
- 1 dashboard variable: `sensor_id` (Custom, sample list)
- Auto-refresh: 5 seconds

## Status: ✅ Real-time dashboard functional with 2 panels

## Next step
Set up a MongoDB datasource in Grafana (no official native plugin exists —
needs investigation) and build historical dashboards: error-over-time,
error-by-horizon, per-sensor performance, time-of-day breakdowns — all via
aggregation queries over `predictions_history`.