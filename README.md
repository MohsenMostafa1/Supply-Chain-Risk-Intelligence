# supply-chain-risk-intelligence

Data flow summary:

    IoT sensors → Kafka → two parallel paths:

        Spark Streaming → real‑time anomaly scoring → MongoDB (operational).

        Spark + HDFS → raw storage as Parquet.

    Nightly Airflow job: HDFS → PostgreSQL (warehouse).

    Kubeflow uses warehouse data to retrain ML model, tracked in MLflow.

    Best model is promoted to KServe for real‑time inference (called by Spark Streaming job).

    Grafana visualises everything.


![Python](https://img.shields.io/badge/python-3.9-blue.svg)
![Spark](https://img.shields.io/badge/Spark-3.3.2-orange)
![Kafka](https://img.shields.io/badge/Kafka-3.4.0-black)
![MLflow](https://img.shields.io/badge/MLflow-2.3.2-blue)
![Kubeflow](https://img.shields.io/badge/Kubeflow-1.7-red)
![License](https://img.shields.io/badge/license-MIT-green)

**A production‑ready MLOps pipeline that simulates real‑time IoT sensor data, detects anomalies with Spark + Kafka, manages models with MLflow, orchestrates retraining with Kubeflow, and monitors everything – built for AI‑driven smart city and industrial IoT solutions.**

---

## 📌 Business Context – Why this project 


- **Real‑time anomaly detection** for vehicle fleets / industrial machinery.
- **Scalable big data ingestion** (Kafka + Spark) to handle millions of IoT events.
- **MLOps** (MLflow + Kubeflow) to manage model lifecycle, retraining, and governance.
- **Data lake + warehouse** (HDFS + PostgreSQL) for historical analytics.
- **Full observability** (Prometheus + Grafana) for production systems.

By implementing this project, you showcase the **end‑to‑end AI infrastructure** that Orion Valley uses to build reliable, intelligent systems at scale.

---

## 🏗️ High‑Level Architecture

![Architecture Diagram](https://raw.githubusercontent.com/yourusername/orion-valley-predictive-maintenance/main/docs/architecture.png)

*If the image doesn’t load, see the ASCII diagram below:*
┌─────────────┐ ┌─────────────┐ ┌─────────────────┐
│ IoT Fleet │────▶│ Kafka │────▶│ Spark Streaming │
│ (Simulator)│ │ Broker │ │ (Real‑time) │
└─────────────┘ └──────┬──────┘ └────────┬────────┘
│ │
│ (raw data) │ (aggregates)
▼ ▼
┌─────────────┐ ┌─────────────┐
│ HDFS │ │ MongoDB │
│ (Data Lake)│ │ (Operational│
└──────┬──────┘ │ Store) │
│ └──────┬──────┘
│ (batch ETL) │
▼ │
┌─────────────┐ │
│ PostgreSQL │◀───────────┘
│ (Warehouse) │ (real‑time alerts)
└──────┬──────┘
│ (training data)
▼
┌─────────────────────────────────────────────────────┐
│ MLOps Layer │
│ ┌─────────┐ ┌─────────┐ ┌─────────┐ │
│ │ Airflow │──▶│Kubeflow │──▶│ MLflow │ │
│ └─────────┘ └─────────┘ └────┬────┘ │
│ (scheduler) (pipeline) │ (registry) │
└───────────────────────────────────┼────────────────┘
│
▼
┌─────────────┐
│ KServe / │
│ FastAPI │
│ (Model API) │
└─────────────┘

**Data flow:**  
1. Simulated vehicles send sensor data to **Kafka** every second.  
2. **Spark Structured Streaming** reads from Kafka, computes sliding‑window features, and:
   - Writes raw data to **HDFS** (Parquet, partitioned by vehicle).  
   - Writes real‑time anomaly scores to **MongoDB**.  
3. **Airflow** runs a nightly ETL from HDFS → **PostgreSQL** (daily aggregates).  
4. **Kubeflow** orchestrates model retraining using PostgreSQL data.  
5. **MLflow** tracks experiments, parameters, metrics, and stores the model registry.  
6. The best model is served via **FastAPI/KServe** for real‑time inference.  
7. **Prometheus + Grafana** monitor Kafka lag, Spark jobs, and prediction health.

---

## 🛠️ Tech Stack & Open Source Tools

| Layer               | Technology                                                                 | Why used for Orion Valley                                                                 |
|--------------------- |----------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| **Stream ingestion** | Apache Kafka, Zookeeper                                                    | High‑throughput, fault‑tolerant event bus for IoT data.                                   |
| **Stream processing**| Apache Spark Structured Streaming                                         | Exactly‑once semantics, unified batch/stream API, HDFS integration.                      |
| **Data lake**        | Hadoop HDFS (Parquet format)                                               | Cost‑effective, durable storage for petabytes of raw sensor data.                        |
| **Operational DB**   | MongoDB                                                                    | Flexible schema, fast indexed lookups for real‑time dashboards.                           |
| **Data warehouse**   | PostgreSQL                                                                 | ACID‑compliant, complex analytical queries for BI and model training.                     |
| **Workflow orchestration**| Apache Airflow                                                        | Reliable scheduling of batch ETL and model retraining jobs.                               |
| **ML orchestration**| Kubeflow Pipelines (on Kubernetes)                                          | Reproducible, scalable ML pipelines with GPU support.                                     |
| **Experiment tracking & registry**| MLflow                                                        | Model versioning, parameter logging, and promotion to production.                         |
| **Model serving**   | FastAPI (or KServe for serverless)                                          | Low‑latency REST API for real‑time predictions.                                           |
| **Monitoring**      | Prometheus + Grafana                                                        | Full observability of data pipelines and model performance.                               |
| **Containerisation**| Docker, Docker Compose, Kubernetes (Minikube for local dev)                 | Reproducible environments, easy scaling.                                                  |
| **CI/CD**           | GitHub Actions                                                              | Automated testing on every push.                                                          |


## 🚀 Getting Started (Local Development)

### Prerequisites
- **Docker** & **Docker Compose** (v2.0+)
- **Python 3.9+** with `pip`
- **Java 11** (for Spark)
- **Minikube** (optional, for Kubeflow production simulation)

## Running Integration Tests Locally

1. Start all services: `docker-compose up -d`
2. Wait for Kafka to be ready (about 30 seconds).
3. Run integration tests: `pytest tests/integration/ -v`
   
### Step 1 – Clone the repository
```python
git clone https://github.com/yourusername/orion-valley-predictive-maintenance.git
cd orion-valley-predictive-maintenance
```

### Step 2 – Start all required services
```python
# Start Kafka, Zookeeper, HDFS, PostgreSQL, MongoDB, MLflow, Airflow, Prometheus, Grafana
docker-compose up -d

# Wait 30 seconds for services to stabilise
```

### Step 3 – Install Python dependencies
```python
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
pip install -r requirements.txt
```

### Step 4 – Seed test data
```python
bash scripts/setup_test_data.sh
```

### Step 5 – Run the simulated IoT producer
```python
make produce
# or: python data/generator/kafka_producer.py
```
You should see JSON messages being printed every second.

### Step 6 – Start the Spark streaming job
```python
make stream
# or: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
#      processing/streaming/streaming_anomaly_job.py
```
The streaming job will:

    Read from Kafka topic iot-sensor-data.

    Compute 10‑second rolling aggregates.

    Write anomalies to MongoDB (orion.anomalies collection).

    Write raw data to HDFS at hdfs://localhost:9000/user/raw_iot/.

### Step 7 – Train a model and log to MLflow
```python
make train
# or: python ml_pipeline/train_isolation_forest.py
```
Open http://localhost:5000 to see the MLflow UI with your experiment.

### Step 8 – Serve the model API
```python
make serve
# or: uvicorn serving.api.fastapi_app:app --reload --port 8000
```
Test the API:
```python
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"temperature": 110, "vibration": 4.2, "rpm": 5800}'
```

### Step 9 – Trigger the Airflow DAG 
Access Airflow UI at http://localhost:8081 (user: airflow, password: airflow).

Find the DAG orion_batch_etl_and_retraining and trigger it manually.

### Step 10 – View Grafana dashboards
Open http://localhost:3000 (user: admin, password: admin).

Add Prometheus as a data source (http://prometheus:9090).

Import a dashboard for Kafka/Spark metrics (or create your own).

### 🧪 Running Tests
We provide unit tests (no external dependencies) and integration tests (require Kafka + PostgreSQL).
```python
# Run all tests
pytest

# Run only unit tests
pytest tests/unit/

# Run integration tests (services must be up)
docker-compose up -d   # ensure Kafka & PostgreSQL are running
pytest tests/integration/
```
The GitHub Actions CI pipeline (.github/workflows/ci.yml) will run the entire test suite on every push.

### 🤝 Contributing

This project is a showcase for technical vision. If you find a bug or want to improve the architecture, please open an issue or submit a pull request. We follow standard GitHub flow.

### 📄 License

This project is open‑source under the MIT License. You are free to use, modify, and distribute it for any purpose, including commercial Orion Valley client deployments.
