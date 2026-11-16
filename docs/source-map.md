# Floci source map (what the port actually covers)

Source: github.com/floci-io/floci @ 2.5.0, Java/Quarkus, MIT.
Clone of record: ~/projects/floci-src (shallow, tag-pinned when porting starts).

Ground truth by service (java files · LOC · where the wire functions live):

| Service | Files | LOC | Handlers/Service | Handler LOC | Notes |
|---|---:|---:|---:|---:|---|
| SQS | 17 | 5,343 | 5 | 3,971 | SqsQueryHandler (query/XML). Port FIRST — smallest, full protocol adapter exercise. |
| S3 | 67 | 23,187 | 4 | 14,566 | S3Service 8.8K LOC: versioning, multipart, presigned, CORS, lifecycle, S3 Select, CloudFront OAC. Most is NOT needed for sugarbit v0 — envelope will scope. |
| DynamoDB | 86 | 27,072 | 9 | 13,260 | NativeDynamoDbJsonHandler + expression engine. The grammar evaluator is the deep part. |
| **Total** | **170** | **55,602** | **18** | **31,797** | |

License: MIT — port is legal with attribution (keep NOTICE with floci-io copyright).
License: MIT — port is legal with attribution (keep NOTICE with floci-io copyright).

## EventBridge (added to queue)

| | |
|---|---|
| Floci source | 39 Java files · 6,823 LOC (handler thin, 1075 LOC; service 2,529 LOC) |
| Sugarbit scope | 15 ops (buses, rules, targets, ingress, pattern-test) |
| Protocol | x-amz-json-1.0 — same as DDB, served by ticket 003 |
| Kernel pieces | Pattern-match engine ~150 LOC (5 recursive JSON match methods); TargetDispatcher ~301 LOC (routes to SQS/Lambda/HTTP). Floci: TargetDispatcher.java, EventBridgeService.java lines 1354–1503 |
| Tickets | 26 (kernel: pattern engine) · 27 (kernel: target dispatcher) · 28 (bus CRUD + rules) · 29 (targets + ingress) |
| Dependencies | Ticket 003 (JSON adapter already in queue) + ticket 004 (SQS adapter for SQS targets) |

Lambda targets: fail loudly (`NotImplemented(Lambda)`) until the execution model is decided.
Scheduled rules (cron/rate): deferred — sugarbit doesn't need them in v0. Floci source has
ScheduleExpressionParser.java (112 LOC) as reference when needed.

## What sugarbit v0 actually needs (envelope to be confirmed from sugarbit source)

- S3: create bucket, put/get/delete/head object, list (v1 prefix), no versioning/multipart/policy/lifecycle.
- SQS: create queue, send, receive, delete, attributes, visibility.
- DDB: create/describe table, put/get/delete/update item, query, batch, transactions (if sugarbit uses them — confirm from surface recording).

## Kernel tickets (Go, in flocigo)

The kernel (~3–4K LOC of Go estimated) is where the real work concentrates:

1. Router: one HTTP mux, three protocol adapters (S3 REST/XML, DDB x-amz-json, SQS query/XML).
2. SigV4 verify at the boundary (exists in internal/sigv4, one canonical bug to fix).
3. Error taxonomy: exact AWS error names/shapes/status per op.
4. Storage backends: disk (S3 objects), store file (DDB tables/items), in-memory queues.
5. Expression engines: DDB condition/update (from Java source, ported as a package with the Java tests as spec).
