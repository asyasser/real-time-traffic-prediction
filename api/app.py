"""
Small REST API exposing MongoDB predictions_history for Grafana (via Infinity datasource).
Endpoints:
GET /api/sensor_history?sensor_id=773869&hours=24
GET /api/network_history?hours=24
GET /api/error_by_horizon
GET /api/error_by_sensor
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

MONGO_URI = "mongodb://mongo:27017/?replicaSet=rs0"
client = MongoClient(MONGO_URI)
db = client["traffic_db"]
coll = db["predictions_history"]

@app.route("/api/sensors")
def get_sensors():
    """Return list of all sensor IDs."""
    import json
    with open("/app/data/sensor_ids.json") as f:
        sensor_ids = json.load(f)
    return jsonify(sensor_ids)


@app.route("/api/sensor_history")
def sensor_history():
    sensor_id = request.args.get("sensor_id", "773869")
    hours = float(request.args.get("hours", 24))

    docs = list(coll.find({}, {"timestamp": 1, f"sensors.{sensor_id}": 1}).sort("timestamp", 1))

    result = []
    for doc in docs:
        s = doc.get("sensors", {}).get(sensor_id)
        if s:
            result.append({
                "timestamp": doc["timestamp"],
                "actual": s.get("actual"),
                "pred_5min": s.get("pred_5min"),
                "pred_10min": s.get("pred_10min"),
                "pred_15min": s.get("pred_15min"),
            })

    return jsonify(result)


@app.route("/api/network_history")
def network_history():
    docs = list(coll.find({}, {
        "timestamp": 1,
        "network_avg_actual": 1,
        "network_avg_pred_5min": 1,
        "network_avg_pred_10min": 1,
        "network_avg_pred_15min": 1,
    }).sort("timestamp", 1))

    result = [
        {
            "timestamp": d["timestamp"],
            "network_avg_actual": d.get("network_avg_actual"),
            "network_avg_pred_5min": d.get("network_avg_pred_5min"),
            "network_avg_pred_10min": d.get("network_avg_pred_10min"),
            "network_avg_pred_15min": d.get("network_avg_pred_15min"),
        }
        for d in docs
    ]
    return jsonify(result)


@app.route("/api/error_by_horizon")
def error_by_horizon():
    docs = list(coll.find({}, {"timestamp": 1, "sensors": 1}).sort("timestamp", 1))

    actual_lookup = {}
    for d in docs:
        ts = d["timestamp"]
        actual_lookup[ts] = {sid: v["actual"] for sid, v in d.get("sensors", {}).items()}

    errors = {"5min": [], "10min": [], "15min": []}
    horizon_minutes = {"5min": 5, "10min": 10, "15min": 15}

    for d in docs:
        ts = datetime.fromisoformat(d["timestamp"])
        for horizon, mins in horizon_minutes.items():
            target_ts = (ts + timedelta(minutes=mins)).isoformat()
            if target_ts in actual_lookup:
                for sid, sensor_data in d.get("sensors", {}).items():
                    pred_key = f"pred_{horizon}"
                    pred_val = sensor_data.get(pred_key)
                    actual_val = actual_lookup[target_ts].get(sid)
                    if pred_val is not None and actual_val is not None and actual_val > 0:
                        err = abs(pred_val - actual_val)
                        errors[horizon].append((err, pred_val, actual_val))

    result = []
    for h, vals in errors.items():
        if not vals:
            continue
        abs_errors = [v[0] for v in vals]
        actuals = [v[2] for v in vals]
        mae = sum(abs_errors) / len(abs_errors)
        rmse = (sum(e**2 for e in abs_errors) / len(abs_errors)) ** 0.5
        mape = sum(e / a for e, a in zip(abs_errors, actuals)) / len(abs_errors) * 100
        result.append({
            "horizon": h,
            "mae": round(mae, 4),
            "rmse": round(rmse, 4),
            "mape": round(mape, 4),
            "n": len(vals)
        })

    return jsonify(result)


@app.route("/api/error_by_sensor")
def error_by_sensor():
    """
    Compute per-sensor MAE for 15-min horizon (comparing pred_15min(T) vs actual(T+15min)).
    """
    docs = list(coll.find({}, {"timestamp": 1, "sensors": 1}).sort("timestamp", 1))

    actual_lookup = {}
    for d in docs:
        actual_lookup[d["timestamp"]] = {sid: v["actual"] for sid, v in d.get("sensors", {}).items()}

    sensor_errors = {}

    for d in docs:
        ts = datetime.fromisoformat(d["timestamp"])
        target_ts = (ts + timedelta(minutes=15)).isoformat()
        if target_ts in actual_lookup:
            for sid, sensor_data in d.get("sensors", {}).items():
                pred_val = sensor_data.get("pred_15min")
                actual_val = actual_lookup[target_ts].get(sid)
                if pred_val is not None and actual_val is not None:
                    sensor_errors.setdefault(sid, []).append(abs(pred_val - actual_val))

    result = [
        {"sensor_id": sid, "mae_15min": sum(v) / len(v)}
        for sid, v in sensor_errors.items()
    ]
    result.sort(key=lambda x: x["mae_15min"], reverse=True)
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
