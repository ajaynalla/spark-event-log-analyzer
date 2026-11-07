`apache-spark-event-log.jsonl` is a sanitized numeric test derivative of an Apache Spark fixture:
https://github.com/apache/spark/blob/master/core/src/test/resources/spark-events/application_1553914137147_0018

Apache Spark is copyright The Apache Software Foundation and contributors, licensed under Apache License 2.0. A copy is included as LICENSE.apache.txt. This derivative is used for parser compatibility testing, not as production performance evidence.

Modified November 7, 2026: removed all properties, URLs, hostnames, paths, stack traces, SQL, RDD descriptions and arbitrary environment/workload strings; replaced application/stage names and IDs with explicit test labels; retained only numeric timing/metric fields, numeric executor IDs and event/outcome enums. Unsupported events are replaced with a neutral ignored marker to preserve event count. Counts, supported task timings and numeric aggregates remain unchanged. The derivative is pinned in this repository; CI never fetches upstream data.
