# Port queue (function-by-function)

Legend: [x] ported & wire-verified · [~] ported, library-only (not yet mounted) · [ ] queued · (K) kernel ticket

## Kernel
- (K) [x] sigv4 sign/verify + round-trip + tamper tests
- (K) [x] S3 REST/XML parser + error taxonomy
- (K) [x] disk storage model (buckets/objects/meta)
- (K) [x] conformance harness: record against Floci, offline diff
- (K) [x] sigv4 presign (S3 GET/PUT)
- (K) [x] SQS query/XML adapter + async visibility timers
- (K) [x] DynamoDB x-amz-json adapter + condition/update expression engine

## S3
- [x] CreateBucket · [x] ListBuckets · [x] PutObject · [x] GetObject
- [x] HeadObject · [x] DeleteObject · [x] DeleteBucket · [x] HeadBucket
- [x] ListObjectsV2 (prefix, delimiter, max-keys, continuation-token)
- [x] Multipart Upload (Initiate, UploadPart, Complete, Abort, ListParts, ListMultipartUploads)
- [x] Presigned URLs (v2 + v4)
- [x] CopyObject · [x] Bucket versioning · [x] Bucket/Object tagging

## SQS
- [x] CreateQueue · [x] ListQueues · [x] SendMessage · [x] ReceiveMessage (long-poll)
- [x] DeleteMessage · [x] GetQueueAttributes · [x] visibility timeout

## DynamoDB
- [x] CreateTable · [x] DescribeTable · [x] ListTables · [x] DeleteTable · [x] PutItem
- [x] GetItem · [x] DeleteItem · [x] UpdateItem · [x] Query · [x] Scan
- [x] BatchGetItem · [x] BatchWriteItem · [x] TransactWriteItems · [x] TransactGetItems
- [x] UpdateTable (schema evolution) · [x] TagResource / UntagResource / ListTagsOfResource
- [x] Expression engine (condition + update evaluator, reserved words)
- [x] PartiQL (ExecuteStatement parser + evaluator + storage hooks)
- [x] S3 Vectors (bucket CRUD, index CRUD, put/get/list/delete vectors, SearchVectors)

## EventBridge
- [x] Bus CRUD · [x] Rules CRUD · [x] EnableRule / DisableRule
- [x] Targets (PutTargets, ListTargetsByRule, RemoveTargets)
- [x] Ingress (PutEvents + pattern matching + JSONPath extraction)
- [x] TestEventPattern
- [x] Target dispatcher with SQS + HTTP adapters; Lambda adapter behind ErrLambdaNotImplemented

## SNS
- [~] Topic CRUD (CreateTopic, DeleteTopic, ListTopics, Get/SetTopicAttributes)
- [~] Subscribe / Unsubscribe / ListSubscriptions / ListSubscriptionsByTopic
- [x] Delivery (SQS + HTTP) via `internal/sns/delivery` Dispatcher
   — note: SNS service surface is **library-only**; `cmd/floci` does not mount it. It depends on a parent SNS service host, which is not yet in `cmd/floci`.

## Lambda
- [x] CreateFunction · [x] ListFunctions · [x] GetFunction · [x] DeleteFunction · [x] Invoke
- [x] Execution model: **in-process runner** (RequestResponse synchronous, Event
  tracked by WaitForIdle, DryRun validates only)
- [x] `floci/echo` in-process runner (the fake) and `provided.*` custom-runtime
  scriptRunner (bootstrap exec on the host)
- [x] `RegisterRunner(runtime, factory)` extension surface for later executors
- [x] mounted at `/2015-03-31/`

## Kinesis
- [x] CreateStream · [x] DeleteStream · [x] ListStreams · [x] DescribeStream
- [x] DescribeStreamSummary · [x] ListShards · [x] GetShardIterator
- [x] GetRecords (long-poll) · [x] PutRecord · [x] PutRecords
- [x] SplitShard · [x] MergeShards · [x] UpdateShardCount · [x] retention period
- [x] mounted at `/kinesis/`

## Step Functions
- [x] CreateStateMachine · [x] DescribeStateMachine · [x] DeleteStateMachine
- [x] ListStateMachines · [x] StartExecution · [x] DescribeExecution
- [x] ListExecutions · [x] StopExecution
- [x] Inline ASL interpreter: Succeed / Fail / Wait / Choice (equals, path,
  IsString/IsBoolean/IsNumeric/IsNull) / Task / Parallel / Map
- [~] Retry / Catch modelled; the interpreter executes one bounded pass
- [x] mounted at `/sfn/`

## Binary surface (`cmd/floci`)
| Route | Service |
|---|---|
| `/` | S3 REST/XML |
| `/2015-03-31/` | Lambda (in-process runner) |
| `/sqs/` | SQS query-XML |
| `/s3vectors/` | S3 Vectors (awsJson 1.0) |
| `/ddb/` | DynamoDB (awsjson1.0) — library surface, mounted |
| `/eventbridge/` | EventBridge (awsjson1.0) — library surface, mounted |
| `/kinesis/` | Kinesis (awsjson1.1) — library surface, mounted |
| `/sfn/` | Step Functions (awsjson1.0) — library surface, mounted |
| *(unmounted)* | SNS — library-only, needs parent service host |

Oracle: Floci authority on 127.0.0.1:8000 (Docker host-side; ported code ships
client-side). Recording gate: `FLOCI_RECORD=1` with authority reachable.
