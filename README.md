# flocigo

**Client-side Go port of the Floci AWS emulator.** Zero runtime dependencies,
one static binary, no Docker, embeddable as a library.

## What's here

`cmd/floci` serves **8 AWS-compatible services** on a single loopback binary:

| Route | Service | Transport |
|---|---|---|
| `/` | **S3** | REST/XML |
| `/2015-03-31/` | **Lambda** | in-process runner (echo + `provided.*` script exec) |
| `/sqs/` | **SQS** | query/XML |
| `/s3vectors/` | **S3 Vectors** | `application/x-amz-json-1.0` |
| `/ddb/` | **DynamoDB** | `x-amz-json-1.0` |
| `/eventbridge/` | **EventBridge** | `x-amz-json-1.0` |
| `/kinesis/` | **Kinesis** | `x-amz-json-1.1` |
| `/sfn/` | **Step Functions** | `x-amz-json-1.0` |

SNS delivery is available as a library (`internal/sns/delivery.Dispatcher`)
but is not mounted in `cmd/floci` because it needs an SNS topic parent that
isn't there yet.

## Quickstart

Build and run:

```sh
cd ~/projects/flocigo
go build -o floci-go ./cmd/floci
./floci-go -addr 127.0.0.1:9000 -data-dir /tmp/floci-data
```

Exercise the surface:

```sh
# S3
curl -X PUT http://127.0.0.1:9000/mybucket
curl -X PUT --data-binary "hello" http://127.0.0.1:9000/mybucket/key.txt
curl http://127.0.0.1:9000/mybucket/key.txt

# Lambda
curl -X POST http://127.0.0.1:9000/2015-03-31/functions -H 'Content-Type: application/json' -d '{"FunctionName":"hello","Runtime":"floci/echo","Code":{"ZipFile":"<base64>"},"Handler":"main.handler"}'
curl -X POST http://127.0.0.1:9000/2015-03-31/functions/hello/invocations -H 'Content-Type: application/json' -d '{"foo":"bar"}'

# DynamoDB
curl -X POST http://127.0.0.1:9000/ddb/ -H 'X-Amz-Target: DynamoDB_20120810.CreateTable' \
  -H 'Content-Type: application/x-amz-json-1.0' \
  -d '{"TableName":"users","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}'

# EventBridge
curl -X POST http://127.0.0.1:9000/eventbridge/ -H 'X-Amz-Target: AWSEvents.CreateEventBus' \
  -H 'Content-Type: application/x-amz-json-1.0' -d '{"Name":"my-bus"}'

# Kinesis
curl -X POST http://127.0.0.1:9000/kinesis/ -H 'X-Amz-Target: Kinesis_20131202.CreateStream' \
  -H 'Content-Type: application/x-amz-json-1.1' -d '{"StreamName":"ks","ShardCount":2}'

# Step Functions
curl -X POST http://127.0.0.1:9000/sfn/ -H 'X-Amz-Target: AWSStepFunctions.CreateStateMachine' \
  -H 'Content-Type: application/x-amz-json-1.0' \
  -d '{"name":"test","definition":"{\"StartAt\":\"S1\",\"States\":{\"S1\":{\"Type\":\"Succeed\",\"End\":true}}}"}'

# SQS
curl -X POST http://127.0.0.1:9000/sqs/ --data 'Action=CreateQueue&QueueName=my-queue'
```

## Where the code lives

```
cmd/floci/           binary entrypoint that wires the 8 surfaces
internal/s3/         S3 REST/XML router + storage + presign
internal/sqs/        SQS query-XML service (CreateQueue..GetQueueAttributes)
internal/sqswire/    SQS wire helpers
internal/ddb/        DynamoDB service (Create/Delete/Update/Get/Query/Scan/Transactions)
internal/dynamodb/   DynamoDB TagResource surface
internal/eventbridge EventBridge service (buses, rules, targets, dispatcher)
internal/kinesis/    Kinesis service (streams, shards, records)
internal/sfn/        Step Functions service (state machines + inline ASL interpreter)
internal/partiql/    PartiQL parser + evaluator
internal/s3vectors/  S3 Vectors (bucket / index / vectors / SearchVectors)
internal/sns/        SNS delivery dispatcher (library-only)
internal/lambda/     Lambda in-process runner service
internal/errmap/     S3/DDB/SQS wire error taxonomy
internal/proto/      awsjson1.0 and awsquery protocol adapters
internal/sigv4/      SigV4 sign/verify
docs/queue.md        per-service port ledger (what's done, what's not)
docs/source-map/     Java source map (what Floci file maps to which Go file)
```

## Program of record

Function-by-function port with wire conformance against the running Floci
authority as the oracle. Ports `aws-sdk-*` clients unmodified: they talk HTTP,
SigV4 is verified at the boundary.

See **docs/queue.md** for the per-service ledger. See **PLAN.md** for the
function-by-function port plan, dependency graph and factory handoff notes.

## License

MIT (inherited from upstream Floci).
