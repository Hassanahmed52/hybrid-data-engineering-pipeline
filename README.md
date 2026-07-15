```markdown
**Hybrid Data Engineering Pipeline (Batch + Stream)**

A hybrid batch + streaming data pipeline for real-time e-commerce analytics. Loads 15,000+ historical transactions in bulk, streams simulated live transactions through Kafka, processes both with Spark Structured Streaming using windowed aggregation, stores results in PostgreSQL, and visualizes everything on a live-refreshing Streamlit dashboard.

**Tech Stack**
Apache Kafka, Apache Spark (Structured Streaming), PostgreSQL, Streamlit, SQLAlchemy, Docker Compose

**Architecture**

CSV (historical) ──► pipeline_init.py ──► PostgreSQL

stream_producer.py ──► Kafka ──► spark_processor.py ──► PostgreSQL ──► dashboard.py (Streamlit)

**How It Works**
- **Batch layer**: `pipeline_init.py` loads 15,000 historical transactions into PostgreSQL.
- **Stream producer**: `stream_producer.py` publishes simulated live transactions to Kafka at 1 record/sec.
- **Stream processing**: `spark_processor.py` consumes from Kafka, aggregates revenue by category using a **2-minute tumbling window** with a 2-minute watermark, and writes results to PostgreSQL every minute.
- **Dashboard**: `dashboard.py` auto-refreshes every 60s, showing revenue by category, sales-per-minute trend, and category rankings.

Windowed aggregation cuts database writes by ~98% (8 category writes per window vs. 120 raw records).

**Data Model**
`sales_historical`: `transaction_id`, `category`, `amount`, `timestamp` — both batch and streamed data land in this one table.

**Setup**

**1. Configure environment** 
— create `.env`:

DB_USER=hassan
DB_PASSWORD=your_password
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=analytics_db
KAFKA_BROKER=127.0.0.1:9092
```

**2. Start infrastructure**
```bash
docker-compose up -d
```

**3. Create Kafka topic**
```bash
docker exec -it hybrid-data-engineering-pipeline-kafka-1 kafka-topics --create --topic sales_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**4. Install dependencies**
```bash
python3 -m venv venv
source venv/bin/activate
pip install pandas sqlalchemy psycopg2-binary kafka-python streamlit plotly streamlit-autorefresh python-dotenv pyspark
```

**5. Load batch data**
```bash
python pipeline_init.py
```

**6. Run each in its own terminal**
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 spark_processor.py
python stream_producer.py
streamlit run dashboard.py
```

Visit `http://localhost:8501`. Batch data shows immediately; streamed data appears after the first 2-minute window closes.

**Stop everything**
```bash
docker-compose down
```

## Results
- 15,000+ historical transactions processed via batch load
- ~98% reduction in streaming DB writes via windowed aggregation
- Dashboard queries: 100-200ms; sub-minute end-to-end latency
```