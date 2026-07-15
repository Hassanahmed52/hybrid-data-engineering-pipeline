# Hybrid Data Engineering Pipeline (Batch + Stream)

A hybrid batch + streaming data pipeline for real-time e-commerce analytics. It loads **15,000+ historical transactions** in bulk, streams simulated live transactions through **Apache Kafka**, processes them with **Apache Spark Structured Streaming**, stores results in **PostgreSQL**, and visualizes everything in a **Streamlit dashboard**.

---

## Tech Stack

- Apache Kafka
- Apache Spark (Structured Streaming)
- PostgreSQL
- Streamlit
- SQLAlchemy
- Docker Compose
- Python

---

## Architecture

```text
                Batch Layer
CSV (Historical)
        │
        ▼
pipeline_init.py
        │
        ▼
   PostgreSQL
        ▲
        │
spark_processor.py ◄──── Kafka ◄──── stream_producer.py
        │
        ▼
   dashboard.py (Streamlit)
```

---

## How It Works

### Batch Layer

- `pipeline_init.py` loads **15,000+ historical transactions** into PostgreSQL.

### Stream Producer

- `stream_producer.py` publishes simulated live transactions to Kafka at **1 transaction per second**.

### Stream Processing

- `spark_processor.py`
  - Reads messages from Kafka.
  - Performs revenue aggregation using a **2-minute tumbling window**.
  - Uses a **2-minute watermark**.
  - Writes aggregated results into PostgreSQL every minute.

### Dashboard

`dashboard.py` refreshes automatically every **60 seconds** and displays:

- Revenue by category
- Sales per minute
- Category rankings

Using windowed aggregation reduces database writes by approximately **98%** (8 writes per window instead of 120 individual writes).

---

## Data Model

Table: `sales_historical`

| Column | Description |
|---------|-------------|
| transaction_id | Transaction ID |
| category | Product category |
| amount | Sale amount |
| timestamp | Transaction timestamp |

Both historical and streamed data are stored in this table.

---

# Setup

## 1. Configure Environment

Create a `.env` file.

```env
DB_USER=hassan
DB_PASSWORD=your_password
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=analytics_db

KAFKA_BROKER=127.0.0.1:9092
```

---

## 2. Start Infrastructure

```bash
docker-compose up -d
```

---

## 3. Create Kafka Topic

```bash
docker exec -it hybrid-data-engineering-pipeline-kafka-1 \
kafka-topics \
--create \
--topic sales_topic \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1
```

---

## 4. Install Dependencies

```bash
python3 -m venv venv

source venv/bin/activate

pip install pandas sqlalchemy psycopg2-binary kafka-python streamlit plotly streamlit-autorefresh python-dotenv pyspark
```

---

## 5. Load Historical Data

```bash
python pipeline_init.py
```

---

## 6. Run the Pipeline

Open **three terminals**.

### Terminal 1

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 \
spark_processor.py
```

### Terminal 2

```bash
python stream_producer.py
```

### Terminal 3

```bash
streamlit run dashboard.py
```

Open:

```
http://localhost:8501
```

The historical data appears immediately. Streaming results begin appearing after the first **2-minute window** closes.

---

## Stop Everything

```bash
docker-compose down
```

---

# Results

- ✅ 15,000+ historical transactions loaded
- ✅ Real-time streaming with Apache Kafka
- ✅ Spark Structured Streaming window aggregation
- ✅ ~98% reduction in database writes
- ✅ PostgreSQL storage
- ✅ Live Streamlit dashboard
- ✅ Dashboard query latency: **100–200 ms**
- ✅ Sub-minute end-to-end processing latency

---

## Project Structure

```text
.
├── dashboard.py
├── docker-compose.yml
├── pipeline_init.py
├── spark_processor.py
├── stream_producer.py
├── sales_historical.csv
├── requirements.txt
├── .env
└── README.md
```