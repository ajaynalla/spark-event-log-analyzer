# Interview explanation

## Project pitch

“I extended my Python Spark analyzer into a stateless FastAPI service and a React dashboard. It reconstructs execution from listener events, separates wall-clock duration from summed task runtime, and exposes measured evidence for performance investigations. I chose bounded requests instead of introducing a cluster, queue or database. Separate Render static/Python services deploy from GitHub with automated tests and builds.”

## Reconstructing Spark execution

Spark's event logger persists listener events used by its history UI. Supported uncompressed JSONL has one event object per line with an `Event` discriminator. JobStart links job IDs to stage IDs. StageSubmitted/StageCompleted describe parent stage IDs, attempt, task count and timestamps. TaskStart/TaskEnd identify physical attempts, executor, partition index, timestamps and available metrics. Task End Reason is normally an object whose `Reason` is `Success`, `FetchFailed`, etc. ApplicationStart/ApplicationEnd define full elapsed time. Sources: [Spark monitoring](https://spark.apache.org/docs/latest/monitoring.html) and [listener definitions](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala).

Tasks are indexed by `(stage_id, stage_attempt, task_id, task_attempt)`. TaskEnd supersedes TaskStart; repeated terminal events do not multiply counts. Stage attempts stay separate. Partition `Index` groups extra attempts because Spark normally gives retries new physical Task IDs. Extra attempts may also reflect speculation; their count is not proof of failure retries. Parent links describe stage dependencies, not SQL operator plans. Success/failure totals count physical attempts, not unique partitions.

## Measurements

| Signal | Calculation | Interpretation and limit |
|---|---|---|
| Execution duration | App end − start; fallback observed window | Parallel task times must not be summed. Partial logs warn. |
| Stage elapsed | Completion − submission; fallback last finish − first launch | Attempt-specific, may be partial. Task time is a separate sum. |
| Shuffle read | Remote + local bytes | Known values over all attempts; missing task values stay null. |
| Shuffle candidate | Stage read + write ≥512 MiB | Original heuristic; not proof of network saturation. |
| Runtime skew | Max / mean of known runtimes, one successful attempt per partition | Flag ≥3×; cannot prove skewed keys. |
| Straggler | Successful attempt runtime ≥2.5× its stage baseline | Maximum contributes to mean, so small samples reduce sensitivity. |
| Partition imbalance | Max / mean of measured input + shuffle-read bytes, including measured zeros | Flag ≥2×; require at least two complete loads. |
| p95 | Nearest-rank successful-partition runtimes for web findings | Never interpolates beyond the observed maximum. |
| Executor totals | Known runtime, GC and traffic grouped by executor ID | Work totals, not utilization, CPU percentage or capacity. |

Failed attempts do not distort the web runtime baseline. Missing metrics become null; coverage warnings explain why aggregate totals are sums of known measurements. A zero total may mean missing measurements as well as measured zero. Unknown outcomes are not counted as failures. Missing partition indexes fall back to task IDs, making retry counts incomplete.

## Recommendations and correctness

The original six rules cover shuffle volume, runtime skew, stragglers, retries/failures, costly wide transformations and partition imbalance. Each returns rule ID, severity, stage/attempt, evidence and advice. Stage-name matching for joins/aggregations is heuristic. AQE, salting, partitioning, broadcast joins, GC or corrupt-input advice gives investigation hypotheses. The app cannot see actual keys, choose the correct join automatically or estimate an unmeasured speedup. Verify changes with repeatable workload experiments.

## Engineering tradeoffs

Read [architecture](ARCHITECTURE.md) for complexity and failure handling. Explicit limits make resource usage testable on a 512 MiB instance. Input lines stream from a temporary file; only selected fields persist in request memory. Unknown listener types are ignored. A single admission slot returns 429 while parsing runs off the async loop. Timeouts are cooperative. The API executes no uploaded code and retains no files after the request.

The public demo has no authentication, durable reports, SQL-plan reconstruction, compressed-codec support, directory merge or SLA. CORS is not authentication. Enterprise use would require identity, per-user quotas, reverse-proxy abuse controls, isolation and an authorized retention/storage policy.

Be ready to explain why stage elapsed differs from task time (parallelism), why partition index matters for retries (physical IDs change), why long tasks cannot prove key skew (GC/locality/contention), why Spark is unnecessary at runtime (logs already contain evidence), and what changes at multi-gigabyte scale (incremental aggregation, paginated storage, async jobs).
