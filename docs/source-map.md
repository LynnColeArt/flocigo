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
