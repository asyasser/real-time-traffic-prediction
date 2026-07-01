

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/?replicaSet=rs0"
DB_NAME = "traffic_db"
COLLECTION_NAME = "predictions_history"

_mongo_client = None


def get_mongo_collection():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(MONGO_URI)
        db = _mongo_client[DB_NAME]
        db[COLLECTION_NAME].create_index("timestamp")
    return _mongo_client[DB_NAME][COLLECTION_NAME]


def write_to_mongo(timestamp_str, sensor_ids, actual_speeds, pred_5, pred_10, pred_15):
    coll = get_mongo_collection()

    sensors_doc = {}
    total_actual = 0.0
    total_pred_5 = 0.0
    total_pred_10 = 0.0
    total_pred_15 = 0.0

    for i, sid in enumerate(sensor_ids):
        actual = float(actual_speeds[i])
        p5 = float(pred_5[i])
        p10 = float(pred_10[i])
        p15 = float(pred_15[i])

        sensors_doc[sid] = {
            "actual": actual,
            "pred_5min": p5,
            "pred_10min": p10,
            "pred_15min": p15,
        }

        total_actual += actual
        total_pred_5 += p5
        total_pred_10 += p10
        total_pred_15 += p15

    n = len(sensor_ids)
    doc = {
        "timestamp": timestamp_str,
        "sensors": sensors_doc,
        "network_avg_actual": total_actual / n,
        "network_avg_pred_5min": total_pred_5 / n,
        "network_avg_pred_10min": total_pred_10 / n,
        "network_avg_pred_15min": total_pred_15 / n,
    }

    coll.insert_one(doc)
