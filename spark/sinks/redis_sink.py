
import json
import redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379
TTL_SECONDS = 600          # 10 minutes
ROLLING_WINDOW = 24        # last 24 points = 2 hours at 5-min intervals

_redis_client = None


def get_redis_client():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    return _redis_client


def write_to_redis(timestamp_str, sensor_ids, actual_speeds, pred_5, pred_10, pred_15):
    r = get_redis_client()
    pipe = r.pipeline()

    total_actual = 0.0
    total_pred_15 = 0.0
    n_congested = 0

    for i, sid in enumerate(sensor_ids):
        actual = float(actual_speeds[i])
        p5 = float(pred_5[i])
        p10 = float(pred_10[i])
        p15 = float(pred_15[i])

        key = f"sensor:{sid}"
        pipe.hset(key, mapping={
            "ts": timestamp_str,
            "actual": actual,
            "pred_5min": p5,
            "pred_10min": p10,
            "pred_15min": p15,
        })
        pipe.expire(key, TTL_SECONDS)

        trend_key = f"sensor:{sid}:trend"
        pipe.xadd(trend_key, {
            "ts": timestamp_str,
            "pred_5min": p5,
            "pred_10min": p10,
            "pred_15min": p15,
        }, maxlen=ROLLING_WINDOW, approximate=True)
        pipe.expire(trend_key, TTL_SECONDS * 2)

        total_actual += actual
        total_pred_15 += p15
        if actual < 35.0:  # congestion threshold
            n_congested += 1

    n = len(sensor_ids)
    pipe.hset("network:summary", mapping={
        "ts": timestamp_str,
        "avg_actual": total_actual / n,
        "avg_pred_15min": total_pred_15 / n,
        "n_congested": n_congested,
        "n_sensors": n,
    })
    pipe.expire("network:summary", TTL_SECONDS)

    pipe.execute()
