# BankFlow Sentinel — Comprehensive Technical Viva Voce Q&A

This guide contains 25 examiner-level questions and crisp, authoritative answers tailored specifically to the **BankFlow Sentinel** codebase, architecture, and laboratory experiments.

---

### Q1: What core business and data engineering problem does BankFlow Sentinel solve?
**Answer**:
BankFlow Sentinel addresses the "silent data corruption" problem in multi-tier banking pipelines. Financial transactions pass through multiple asynchronous hops (Core Banking System $\rightarrow$ Payment Gateway $\rightarrow$ Settlement Clearinghouse). Network timeouts, schema mutations, delayed processing, or duplicate deliveries cause record drops or amount discrepancies. Sentinel tracks transactions stage-by-stage, performs automated reconciliation, logs discrepancies with transaction-level audit evidence, and presents an operational console for triage before discrepancies cause financial losses.

---

### Q2: Why is reconciliation performed on adjacent stages (Source $\rightarrow$ Payment, Payment $\rightarrow$ Settlement) rather than just Source $\rightarrow$ Settlement?
**Answer**:
Adjacent-stage reconciliation isolates **fault localization**. If a transaction drops between Source and Settlement, an end-to-end comparison only tells you that the transaction never settled; it cannot identify whether the failure occurred at the Payment Gateway or the Clearinghouse. By comparing adjacent pairs, Sentinel pinpoints the exact boundary where the drop, amount delta, or delay occurred, reducing Mean Time to Resolution (MTTR).

---

### Q3: How is idempotency achieved if Kafka re-delivers the same batch message multiple times?
**Answer**:
Idempotency is enforced at multiple layers:
1. **ETL Layer (`etl/load.py`)**: Uses an explicit atomic transaction that deletes existing transaction records for that `(batch_id, stage)` before re-inserting (`DELETE FROM transactions WHERE batch_id = :b AND stage = :s`).
2. **Reconciliation Engine (`reconcile/engine.py`)**: Deletes existing incident rows and reconciliation metrics for `(batch_id, stage)` before executing checks (`DELETE FROM incidents WHERE batch_id = :b AND stage = :s`).
3. **Database Constraints**: `batches` uses `ON CONFLICT (batch_id) DO UPDATE`.
Reprocessing the same batch produces identical counts and replaces tickets without duplicating rows.

---

### Q4: Why are Kafka offsets committed manually (`enable.auto.commit=False`) rather than automatically?
**Answer**:
Automatic offset commits happen on a background timer regardless of processing success. If the consumer crashes while executing ETL or reconciliation, auto-commit would advance the partition offset, resulting in **silent data loss**. BankFlow Sentinel uses manual commit (`consumer.commit(message)`) strictly **after** both ETL transformation and reconciliation are committed to PostgreSQL, guaranteeing **at-least-once processing**.

---

### Q5: Why does the system employ two different databases (PostgreSQL and MongoDB)?
**Answer**:
1. **MongoDB (NoSQL Document Store)**: Acts as an ingestion landing zone for raw event payloads. Upstream banking sources frequently alter payload structures or metadata. MongoDB accepts arbitrary, unstructured JSON instantly without schema validation errors and maintains an immutable audit trail.
2. **PostgreSQL (Relational Store)**: Serves as the analytical ledger. Reconciliation requires relational integrity, foreign key cascades, complex multi-table `JOIN`s, window functions (`LAG`, `DENSE_RANK`), and OLAP groupings that are either inefficient or unsupported in document stores.

---

### Q6: Why does the `transactions` table NOT have a UNIQUE constraint on `txn_id`?
**Answer**:
In production banking pipelines, duplicate transaction delivery is a common operational failure (e.g., payment gateway re-trying a timeout). If `txn_id` had a `UNIQUE` constraint, the database would throw an integrity violation and abort the entire ingestion batch. By allowing non-unique `txn_id` entries per stage, Sentinel successfully ingests the faulty batch and lets the reconciliation engine flag the duplicate anomaly (`discrepancy_type = 'duplicate'`).

---

### Q7: How does `reconcile/engine.py` ensure exactly one incident per `(batch_id, stage, discrepancy_type)`?
**Answer**:
Rather than raising an incident row for every individual faulty transaction (which would flood the operations team with thousands of alerts), the engine aggregates all offending IDs for a specific fault type using PostgreSQL's `JSON_AGG(txn_id)` or Python aggregation. It inserts a single incident ticket with all offending transaction IDs encapsulated in the `evidence` column as a JSONB list.

---

### Q8: Why is the `evidence` column in the `incidents` table typed as `JSONB` instead of `TEXT` or a separate junction table?
**Answer**:
1. **Audit Integrity**: Storing the offending IDs as a JSON array preserves a frozen snapshot of affected transactions at detection time.
2. **Query Performance**: PostgreSQL `JSONB` is stored in a decomposed binary format that supports indexing (`GIN`), containment operators (`@>`), and length calculations without full string parsing.
3. **Storage Efficiency**: It avoids creating hundreds of thousands of individual relational join records for transient operational tickets.

---

### Q9: How is amount mismatch detected when financial numbers have floating-point precision issues?
**Answer**:
All financial amounts in PostgreSQL are typed as `NUMERIC(14,2)` rather than `FLOAT` or `DOUBLE PRECISION`. In SQL comparisons, amounts are compared using exact decimal equality:
```sql
ROUND(t.amount - s.amount, 2) != 0.00
```
This eliminates binary floating-point rounding errors (e.g. `0.1 + 0.2 != 0.3`).

---

### Q10: How is an SLA delay detected between stages?
**Answer**:
The engine joins upstream and downstream transaction timestamps (`ts`) on `txn_id` and computes the elapsed interval:
```sql
EXTRACT(EPOCH FROM (downstream.ts - upstream.ts)) > 1800
```
If the elapsed duration exceeds 1,800 seconds (30 minutes), the transaction is flagged as `delayed`.

---

### Q11: What is the semantic difference between `CUBE`, `ROLLUP`, and `GROUPING SETS` in `db/olap.sql`?
**Answer**:
* **`GROUP BY CUBE(A, B, C)`**: Generates all $2^3 = 8$ possible grouping combinations: `(A,B,C)`, `(A,B)`, `(A,C)`, `(B,C)`, `(A)`, `(B)`, `(C)`, and `()`.
* **`GROUP BY ROLLUP(A, B, C)`**: Generates hierarchical subtotals from left to right: `(A,B,C)`, `(A,B)`, `(A)`, and `()`. Suitable for drill-downs like `Date -> Stage -> Type`.
* **`GROUP BY GROUPING SETS(...)`**: Allows explicit specification of only the exact grouping combinations needed, avoiding unnecessary computation of unused Cartesian sub-cubes.

---

### Q12: What is the purpose of the `GROUPING()` function in `db/olap.sql`?
**Answer**:
When using `CUBE` or `ROLLUP`, aggregated subtotal rows contain `NULL` in the columns being collapsed. However, if a column naturally contained `NULL` in the source data, you could not distinguish between a subtotal row and a row with genuine null data. The `GROUPING(col)` function returns `1` if the column is currently aggregated into a subtotal, and `0` if it represents a regular grouped row.

---

### Q13: How does `LAG()` work in the analytical view `v_batch_summary`?
**Answer**:
`LAG(mismatch_count, 1, 0) OVER (PARTITION BY stage ORDER BY batch_id)` inspects the reconciliation results of the immediate preceding batch within the same processing stage. It enables the console to calculate batch-over-batch shift (`current_mismatches - prev_mismatches`) to detect sudden pipeline degradations.

---

### Q14: Why is `DENSE_RANK()` preferred over `RANK()` when prioritizing batches by incident density?
**Answer**:
`RANK()` leaves gaps in ranking numbers when there are ties (e.g., 1, 2, 2, 4), which distorts leaderboard displays and paging logic. `DENSE_RANK()` assigns consecutive integers without gaps (e.g., 1, 2, 2, 3), ensuring a clean, continuous hierarchy of operational severity.

---

### Q15: In `scripts/explain_demo.py`, what did `EXPLAIN (ANALYZE, BUFFERS)` reveal before and after indexing?
**Answer**:
* **Before Indexing**: The query executed a `Seq Scan on transactions`, reading all shared memory buffers sequentially (`4.18 ms` execution time, `412` buffers hit).
* **After Indexing (`idx_transactions_txn_id`)**: The planner switched to an `Index Scan using idx_transactions_txn_id`, reading only `4` buffers (`0.076 ms` execution time).
* **Outcome**: A **55x execution speedup** and a **99% reduction in disk I/O**.

---

### Q16: Why was Polars 14.3x faster than Pandas in the 1M transaction benchmark (0.39s vs 5.58s)?
**Answer**:
1. **Multi-Threaded Execution**: Polars is written in Rust and utilizes work-stealing parallelism (`rayon`) across all CPU cores, whereas Pandas is single-threaded in Python.
2. **Columnar Arrow Memory**: Polars uses Apache Arrow memory layouts with cache-friendly vectorized SIMD operations.
3. **Lazy Evaluation (`lazy()`)**: Polars optimizes the query execution graph, pushing down projections and filtering out unmatched rows before allocating join buffers.

---

### Q17: Why did DuckDB consume 68% less peak memory than Pandas in the benchmark (164 MB vs 512 MB)?
**Answer**:
DuckDB uses a **vectorized, streaming execution engine**. It processes data in chunks of 2,048 tuples (vectors) rather than materializing entire 1,000,000-row DataFrames in memory simultaneously. Pandas materializes intermediate Python object pointers for every join step, inflating memory overhead.

---

### Q18: Why were the benchmarks executed in isolated OS subprocesses?
**Answer**:
Python does not immediately return deallocated heap memory to the operating system after Pandas or Polars DataFrames are garbage collected. Running all three engines in a single process would corrupt peak memory measurements (`RSS`) because later engines would inherit the resident set size of earlier engines. Running each benchmark in an isolated subprocess ensures pristine, independent memory profiling.

---

### Q19: How does Delta Lake guarantee ACID transactions on parquet files?
**Answer**:
Delta Lake maintains an immutable transaction log named `_delta_log/` consisting of ordered JSON commit files. Every write, update, or delete is recorded as an atomic commit (`000000.json`, `000001.json`). Readers inspect the transaction log to identify which parquet data files are active and valid, preventing dirty reads or partial writes through **optimistic concurrency control**.

---

### Q20: What is Delta Lake Time Travel and how does it query historical states?
**Answer**:
Because Delta Lake's transaction log never overwrites parquet files in place (it appends new files and marks old files as removed in the log), earlier snapshots remain physically intact. Time travel allows querying historical data by specifying a version or timestamp:
```python
spark.read.format("delta").option("versionAsOf", 0).load(table_path)
```
This enables operational auditing of exact historical reconciliation metrics.

---

### Q21: What is the difference between Delta Lake schema enforcement and schema evolution (`mergeSchema`)?
**Answer**:
* **Schema Enforcement**: By default, Delta Lake rejects any write with columns that do not match the existing schema, throwing an `AnalysisException`. This prevents bad upstream pipelines from corrupting the lakehouse.
* **Schema Evolution (`mergeSchema`)**: When an intentional schema change occurs, `.option("mergeSchema", "true")` instructs Delta Lake to update the table metadata to accommodate the new columns without rewriting historical data.

---

### Q22: How does Prefect handle transient task failures, and how is the forced-failure demo implemented?
**Answer**:
Prefect tasks use `@task(retries=3, retry_delay_seconds=2)`. If a task raises an exception (e.g. temporary database lock or network blip), Prefect logs the exception and pauses before retrying. The `--fail-once` flag writes an attempt marker to a temporary file: attempt 1 raises an intentional exception, triggering the retry mechanism, and attempt 2 succeeds, proving automated resiliency.

---

### Q23: Why does `POST /events` return HTTP 202 Accepted instead of 200 OK or 201 Created?
**Answer**:
HTTP 202 Accepted indicates that the request has been accepted for processing, but processing has **not been completed**. Because the API publishes a message to Kafka and returns immediately while the actual ETL and reconciliation happen asynchronously downstream, HTTP 202 is the exact RFC-compliant status code.

---

### Q24: How does the Streamlit console prevent stale data without overloading PostgreSQL?
**Answer**:
The console uses `@st.cache_data(ttl=5)` on all database reader functions. Queries are cached in memory for 5 seconds, allowing high-frequency user interactions without executing duplicate SQL queries. When a user updates an incident (marking it Investigating or Resolved), the update function explicitly calls `get_incidents.clear()`, forcing an immediate cache bust.

---

### Q25: What happens if the background Kafka consumer is offline when a user clicks "Simulate Batch"?
**Answer**:
In `dashboard/app.py`, `trigger_batch_simulation` publishes the event via HTTP and polls `batches.status` for up to 18 seconds. If the background consumer fails to pick up the batch within the timeout window, the dashboard gracefully catches the condition, logs an informative notice, and automatically executes direct reconciliation via `orchestration.flow` as a fallback, ensuring the user is never left hanging.
