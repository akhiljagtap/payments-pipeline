{
  "schema_version": 1,
  "transaction_id": "9f1c2e7a-4b8d-4c1e-9a55-0d2f7e6b1a10",
  "card_id": "card_00482",
  "merchant_id": "m_1093",
  "amount": 1249.50,
  "currency": "INR",
  "channel": "ECOM",
  "country": "IN",
  "status": "APPROVED",
  "event_time": "2026-09-21T14:03:22.418Z"
}


End-to-end flow, in words
Generator (producer) runs in a loop. It builds a payment, and sometimes, on purpose, duplicates it, delays it, or corrupts it. It sends the JSON to the Kafka topic payments.transactions and appends its counts to a ground truth log.
Kafka stores every event in order. Nothing is lost if Spark is down.
Spark Bronze job reads the topic and writes the raw event, exactly as received, plus Kafka metadata (partition, offset, arrival time), into a Bronze Delta table. There's no cleaning here, so the raw history stays intact for replay.
Spark Silver job reads Bronze and:
parses the JSON and enforces the schema
validates rules (amount above 0, known currency, required fields present)
sends invalid records to the quarantine table
removes duplicates by transaction_id using a watermark
enriches with merchant data via a join
writes clean rows to Silver, using MERGE so restarts don't create duplicates
Spark Gold jobs read Silver and produce the 5-minute merchant metrics, the daily summary, and the fraud alerts.
Airflow runs the batch-style jobs on a schedule (daily summary, data quality checks, reconciliation against the ground truth) and shows success or failure per task.
Quality report compares the counts at each layer and flags mismatches.
Optional at the end: a small Streamlit dashboard reading Gold.