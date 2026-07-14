# Pulse

Pulse is a data engineering pipeline like stuff designed to ingest, refine, and analyze equity index data. I built this to solve the common problem of "data swamp" where raw data is dumped into unorganized, hard-to-query files. 

Pulse takes raw market data from historical sources and live API feeds, cleans it, and transforms it into an analysis ready structure. Like a basic ETL concept with focusing on getting the brone -> silver -> gold architecture!!

## The Problem
Getting market data into a format ready for analysis usually involves manual, fragile scripts or expensive, over-engineered cloud services. I wanted to build something that was:
1. **Automated:** 
2. **Efficient:** 
3. **Reproducible:** 
## How it works
- **Ingestion:** Data is pulled from APIs and processed through a message broker to ensure reliability.
- **Medallion Architecture:** Data moves through Bronze (raw), Silver (cleaned/typed), and Gold (analytics-ready) layers.
- **Transformation:** SQL models handle the heavy lifting, ensuring the final output is structured in a star schema optimized for quick queries.
- **Orchestration:** Everything is managed by a central orchestrator, so retries and scheduling happen automatically.

## Tech Stack
- **Language:** Python
- **Orchestration:** Airflow
- **Messaging:** Redpanda (Kafka-compatible)
- **Transformation:** dbt
- **Storage:** MinIO (S3-compatible)
- **Analytics:** DuckDB
- **Containerization:** Docker Compose
