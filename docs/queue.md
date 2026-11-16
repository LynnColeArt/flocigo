# Port queue (function-by-function)

Legend: [x] ported+golden-diffed · [ ] queued · (K) kernel ticket

## Kernel
- (K) [x] sigv4 sign/verify + round-trip + tamper tests
- (K) [x] S3 REST/XML parser + error taxonomy
- (K) [x] disk storage model (buckets/objects/meta)
- (K) [x] conformance harness: record against Floci, offline diff
- (K) [ ] sigv4 presign (S3 GET/PUT)
- (K) [ ] SQS query/XML adapter + async visibility timers
- (K) [ ] DynamoDB x-amz-json adapter + condition/update expression engine

## S3
- [x] CreateBucket · [x] ListBuckets · [x] PutObject · [x] GetObject
- [x] HeadObject · [x] DeleteObject · [x] DeleteBucket · [x] HeadBucket
- [x] ListObjectsV2 (prefix, v0)
- [ ] CopyObject · [ ] ListObjectsV2 pagination/delimiter · [ ] presigned op

## SQS (queued)
CreateQueue, ListQueues, SendMessage, ReceiveMessage (long-poll), DeleteMessage,
Get/SetQueueAttributes, visibility timeout, DLQ redrive.

## DynamoDB (queued, 14 tickets: 12–19 + 30–34)
CreateTable · DescribeTable · ListTables · DeleteTable · PutItem · GetItem ·
DeleteItem · UpdateItem · Query · Scan · BatchGetItem · BatchWriteItem ·
TransactWriteItems · TransactGetItems · ExecuteStatement (PartiQL, conditional)
Kernel: expression engine (condition + update evaluator, expression syntax)

Oracle: Floci authority on 127.0.0.1:8000 (Docker host-side; ported code ships
client-side). Recording gate: FLOCI_RECORD=1 with authority reachable.
