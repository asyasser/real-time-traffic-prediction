# Real-Time Traffic Speed Prediction
### End-to-End Streaming ML Pipeline on METR-LA Dataset

---

## Overview

This project implements a real-time traffic speed forecasting system using the METR-LA dataset (207 highway sensors across Los Angeles). A Bidirectional LSTM model trained offline is deployed inside a Spark Structured Streaming pipeline that simulates live sensor feeds via Kafka, stores predictions in Redis and MongoDB, and visualizes results through Grafana dashboards.

---

## Architecture
[Kafka Producer] → [Kafka] → [Spark Structured Streaming] → [Redis]   → [Grafana]
(BLSTM Inference)      → [MongoDB] → [Flask API] → [Grafana]

---

## Stack

| Component | Technology |
|---|---|
| Message Broker | Apache Kafka (KRaft mode) |
| Stream Processor | Apache Spark 3.5.1 (PySpark) |
| ML Model | PyTorch — Bidirectional LSTM |
| Real-time Store | Redis 7 |
| Historical Store | MongoDB 7 (replica set) |
| API Layer | Flask 3.0 |
| Dashboards | Grafana 11.0.0 |
| Infrastructure | Docker Compose |

---

## Project Structure

├── docker-compose.yml        # Full infrastructure definition
├── kafka/producer/           # Test set replay producer
├── spark/                    # Structured Streaming job + sinks
├── api/                      # Flask REST API (MongoDB → Grafana)
├── grafana/provisioning/     # Dashboard + datasource configs
├── data/                     # Sensor IDs, scaler (not tracked)
└── models/                   # BLSTM weights (not tracked)

---

## Quick Start

**1. Start infrastructure:**
```bash
docker compose up -d
```

**2. Install dependencies:**
```bash
sudo apt install -y openjdk-17-jdk
pip install -r spark/requirements.txt
pip install kafka-python-ng==2.2.2 pyarrow==16.1.0
echo "127.0.0.1 mongo" | sudo tee -a /etc/hosts
```

**3. Run the pipeline (2 terminals):**
```bash
# Terminal 1
cd spark && spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming_job.py

# Terminal 2
cd kafka/producer && python3 producer.py --interval 0.5
```

**4. Open Grafana:** `http://localhost:3000`

---

## Dataset

METR-LA — 207 loop detectors on Los Angeles highways, 5-minute intervals, March–June 2012.
Train/Val/Test split: 70% / 10% / 20% (temporal, no shuffling).

---

## Requirements

- Docker Engine 29.5.3 + Docker Compose plugin
- Python 3.12, Java 17 (OpenJDK)
- WSL2 Ubuntu 24.04
