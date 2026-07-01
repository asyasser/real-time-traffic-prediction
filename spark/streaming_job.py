
import time
import json
import joblib
import numpy as np
import pandas as pd
import torch
from collections import deque
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, MapType, DoubleType
import sys
sys.path.append("..")
from sinks.redis_sink import write_to_redis
from sinks.mongo_sink import write_to_mongo

import os
import csv
CURRENT_INTERVAL = os.environ.get("PRODUCER_INTERVAL", "unknown")
METRICS_FILE = f"../metrics_{CURRENT_INTERVAL}.csv"

if not os.path.exists(METRICS_FILE):
    with open(METRICS_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "e2e", "spark_lag", "inference_ms", "sink_write_ms"])

with open("../data/sensor_ids.json") as f:
    SENSOR_IDS = json.load(f)

scaler = joblib.load("../data/scaler.pkl")

TIME_FEATURES = ["hour_sin", "hour_cos", "dow_sin", "dow_cos", "is_weekend", "is_rush_hour"]
ALL_FEATURES = SENSOR_IDS + TIME_FEATURES  # 207 + 6 = 213

WINDOW_SIZE = 12
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "traffic-raw"

class BLSTMModel(torch.nn.Module):
    def __init__(self, input_size=213, hidden_size=128, num_layers=2,
                 output_steps=3, num_sensors=207, dropout=0.1):
        super().__init__()
        self.lstm = torch.nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            bidirectional=True,
            dropout=dropout
        )
        self.fc = torch.nn.Linear(hidden_size * 2, output_steps * num_sensors)
        self.output_steps = output_steps
        self.num_sensors = num_sensors

    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :])
        return out.view(-1, self.output_steps, self.num_sensors)

model = BLSTMModel()
model.load_state_dict(torch.load("../models/blstm.pth", map_location="cpu"))
model.eval()
print("Model loaded successfully")

window_buffer = deque(maxlen=WINDOW_SIZE)

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("produced_at", DoubleType(), True),
    StructField("speeds", MapType(StringType(), DoubleType()), True),
])

spark = (
    SparkSession.builder
    .appName("MetrLAStreamingStageC")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.timestamp", "data.produced_at", "data.speeds")
)

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["timestamp"]
    hour = ts.dt.hour + ts.dt.minute / 60.0
    dow = ts.dt.dayofweek
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    df["dow_sin"]  = np.sin(2 * np.pi * dow / 7)
    df["dow_cos"]  = np.cos(2 * np.pi * dow / 7)
    df["is_weekend"]  = (dow >= 5).astype(float)
    df["is_rush_hour"] = (((hour >= 7) & (hour < 9)) | ((hour >= 16) & (hour < 19))).astype(float)
    return df

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Step 1: record when Spark received this batch
    t_received = time.time()

    pdf = batch_df.toPandas()
    pdf["timestamp"] = pd.to_datetime(pdf["timestamp"])

    speeds_expanded = pd.DataFrame(pdf["speeds"].tolist(), index=pdf.index)
    speeds_expanded = speeds_expanded[SENSOR_IDS]

    pdf = pd.concat([pdf[["timestamp", "produced_at"]], speeds_expanded], axis=1)
    pdf = add_time_features(pdf)
    pdf = pdf.sort_values("timestamp").reset_index(drop=True)

    pdf_scaled = pdf.copy()
    pdf_scaled[SENSOR_IDS] = scaler.transform(pdf[SENSOR_IDS])

    for idx, row in pdf_scaled.iterrows():
        feature_vector = row[ALL_FEATURES].values.astype(np.float32)
        window_buffer.append(feature_vector)

        if len(window_buffer) == WINDOW_SIZE:
            # Step 2: inference timing
            t_inference_start = time.time()

            window_array = np.stack(list(window_buffer), axis=0)
            tensor = torch.tensor(window_array).unsqueeze(0)

            with torch.no_grad():
                pred = model(tensor)

            pred_np = pred.squeeze(0).numpy()
            pred_mph = scaler.inverse_transform(pred_np)

            t_inference_end = time.time()

            ts = row["timestamp"]
            ts_str = ts.isoformat()
            produced_at = float(row["produced_at"])

            actual_speeds = pdf.loc[idx, SENSOR_IDS].values.astype(np.float32)
            pred_5min  = pred_mph[0]
            pred_10min = pred_mph[1]
            pred_15min = pred_mph[2]

            # Step 3: sink writes timing
            t_write_start = time.time()
            write_to_redis(ts_str, SENSOR_IDS, actual_speeds, pred_5min, pred_10min, pred_15min)
            write_to_mongo(ts_str, SENSOR_IDS, actual_speeds, pred_5min, pred_10min, pred_15min)
            t_write_end = time.time()

            # Step 4: compute latency breakdown
            e2e_latency = t_write_end - produced_at
            spark_receive_latency = t_received - produced_at
            inference_time = t_inference_end - t_inference_start
            write_time = t_write_end - t_write_start

            print(f"\n[METRICS] ts={ts_str}")
            print(f"  E2E latency       : {e2e_latency:.3f}s")
            print(f"  Spark receive lag : {spark_receive_latency:.3f}s")
            print(f"  Inference time    : {inference_time:.4f}s")
            print(f"  Sink write time   : {write_time:.3f}s")

query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)
    .start()
)

query.awaitTermination()