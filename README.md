# Payments Streaming Pipeline

## Architecture

```
Producer (Python)
   │  fault injection: duplicates, late events, corrupt payloads
   ▼
Kafka (KRaft mode, 3 partitions, keyed by card_id)
   ▼
Bronze Layer (Spark Structured Streaming → Delta Lake)
   │  raw ingestion, zero data loss by design — accepts everything, even corrupt data
   ▼
Silver Layer (Spark Structured Streaming → Delta Lake)
   │  validation, quarantine of bad records, deduplication, late-event flagging
   │  idempotent writes via Delta MERGE (safe to restart, never double-processes)
   ▼
Gold Layer (three independent aggregation patterns)
   ├── Merchant Metrics        — 5-minute tumbling window
   ├── Fraud Velocity Alerts   — 5-minute sliding window (1-minute slide)
   └── Daily Summary           — 1-day tumbling window, by country × channel
   ▼
Airflow Orchestration
   ├── Triggers the batch-style Gold jobs on a schedule
   └── Runs an automated reconciliation check (Bronze vs Silver vs Quarantine math)
```

**Medallion architecture** (Bronze → Silver → Gold) — each layer has one clear responsibility, so a bug in validation logic never risks losing raw data, and a bug in aggregation never risks corrupting validated data.

---

## Tech stack

| Component | Choice | Why |
|---|---|---|
| Message broker | Apache Kafka 3.7 (KRaft mode) | No ZooKeeper dependency; dual internal/external listeners so host-side producer and containerized Spark can both connect correctly |
| Stream processing | Spark Structured Streaming 3.5.3 | Industry-standard for exactly-once, watermark-based streaming semantics |
| Storage format | Delta Lake 3.2.0 | ACID transactions, MERGE support for idempotent upserts, time-travel-capable |
| Orchestration | Apache Airflow 2.9.3 | Scheduling + automated data-quality verification, not just continuous streaming |
| Infrastructure | Docker Compose | Fully reproducible local environment, no cloud dependency |
| Language | Python (PySpark, Pydantic) | Typed event schemas, validated configuration |

---

## What each layer actually does

### Producer — synthetic payment events with deliberate fault injection

Rather than assuming clean input, the producer deliberately injects realistic failure modes so every downstream guarantee can be *tested*, not just assumed:

- **Duplicate events** (configurable rate) — resent from a recent-event buffer, simulating network retries or at-least-once delivery
- **Late events** — backdated 2–15 minutes, simulating network delay or clock drift
- **Corrupt events** — negative amounts, invalid currency codes, missing required fields, malformed JSON

Every injected fault is logged to a ground-truth file, so correctness can be checked against a known baseline rather than guessed at.

### Bronze — raw ingestion, never loses data

Reads from Kafka and writes everything to Delta, including malformed records (parsed into nulls rather than crashing the job). The principle: **Bronze's only job is to not lose anything** — validation is a Silver concern, not a Bronze one.

### Silver — validation, deduplication, idempotent writes

- Flags and quarantines invalid records (missing fields, non-positive amounts, invalid currency) with a specific rejection reason per row
- Deduplicates on `transaction_id`
- Flags late-arriving events using a watermark-based lateness check
- Writes via **Delta MERGE**, not plain append — meaning a restarted Silver job never reprocesses or double-writes a row it already handled. This was deliberately stress-tested: the Silver job was interrupted mid-run multiple times during development (Docker/WSL instability), and restarting it from its checkpoint recovered cleanly every time, with zero duplicate or missing rows.

**Verified correctness, one concrete example from a real run:**
```
Bronze total rows:              2113
Distinct transaction_ids:       2011   (102 duplicate copies correctly identified)
Quarantined (invalid) rows:       78
Expected Silver count:          1933   (2011 − 78)
Actual Silver count:            1933   ✓ exact match
```

### Gold — three deliberately different windowing strategies

| Job | Window type | Why this shape |
|---|---|---|
| **Merchant Metrics** | 5-min tumbling | Non-overlapping, so each transaction is counted in exactly one reporting period — correct for revenue/volume metrics, where double-counting would corrupt the numbers |
| **Fraud Velocity** | 5-min sliding (1-min slide) | Catches card-testing bursts regardless of where they fall relative to clock boundaries — a burst split across two tumbling windows would otherwise go undetected |
| **Daily Summary** | 1-day tumbling, by country × channel | Reporting-grade granularity for analysts, not operational real-time alerting |

Each job's correctness was independently verified against hand-computed expected results before being considered done — not just "it ran without crashing."

### Airflow — orchestration and automated data quality

- A continuously-running Structured Streaming job doesn't fit naturally into a scheduler built around tasks that *finish*. Solved using Spark's `Trigger.AvailableNow` mode: the Gold daily-summary job processes everything currently available and then terminates cleanly, making it a proper one-shot batch task.
- A second task runs an automated **reconciliation check**: independently recomputes `Bronze distinct − Quarantine = expected Silver`, compares it to the actual Silver row count, and fails the task (via exit code) if they don't match — turning a manual "eyeball the numbers" habit into an automated data-quality gate.
- Runs on a lightweight `SequentialExecutor` + SQLite backend (no separate Postgres container) — a deliberate infrastructure choice to keep the stack's memory footprint sustainable on constrained local hardware, without giving up real DAG scheduling and dependency semantics.

---

## Guarantees this pipeline provides

- **No data loss** — Bronze accepts and persists every event, even malformed ones
- **No duplicate processing in Silver/Gold** — enforced via Delta MERGE, not just application-level dedup logic
- **Safe restarts** — every streaming job resumes from its checkpoint; demonstrated under real, unplanned interruptions during development, not just in theory
- **Late data handled explicitly** — flagged via watermark logic, not silently dropped or silently included as if on-time
- **Per-card ordering** — Kafka partitioning keyed by `card_id` ensures a single card's events are processed in order
- **Correctness is provable, not assumed** — an automated reconciliation check cross-verifies row counts across every layer

---

## Running it locally

```bash
# 1. Start core infrastructure
cd infra
docker compose up -d kafka spark

# 2. Create Kafka topics (first run only)
# payments.transactions (3 partitions), payments.dlq (1 partition)

# 3. Start the producer (separate terminal)
python -m src.producer.kafka_producer

# 4. Start the streaming jobs (separate terminals, left running)
docker exec -it -e PYTHONPATH=/opt/app payments-spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/app/src/streaming/bronze_job.py

# (same pattern for silver_job.py, gold_merchant_metrics.py, gold_fraud_velocity.py)

# 5. Start Airflow for orchestration + reconciliation
docker compose up -d airflow
docker exec -d payments-airflow airflow scheduler
docker exec -d payments-airflow airflow webserver --port 8080 --workers 1
# UI available at http://localhost:8081
```

Full setup details, environment variables, and troubleshooting notes are in [`docs/`](./docs).

---

## Project structure

```
payments-pipeline/
├── airflow/
│   └── dags/                  # daily_summary_dag.py — scheduling + reconciliation
├── infra/
│   └── docker-compose.yml     # Kafka, Spark, Airflow services
├── src/
│   ├── common/                 # config, logging, shared schemas
│   ├── producer/                # event generation, fault injection, ground truth
│   └── streaming/                # Bronze, Silver, Gold jobs + verification scripts
├── docs/
│   └── problem-statement.md    # guarantees and design rationale
└── checkpoints/ data/          # Delta tables and streaming checkpoints (gitignored)
```

---


## About

Built by Akhil Jagtap(Data Engineer)  as a portfolio project applying streaming data engineering concepts in a realistic, failure-aware system — not a tutorial walkthrough, but a pipeline debugged, broken, and fixed the way real systems are.
