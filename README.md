# floci-go

Client-side Go port of the Floci AWS emulator: **S3, SQS, DynamoDB** — zero
runtime dependencies, one static binary, no Docker, embeddable as a library.

Program of record: function-by-function port with wire conformance against the
running Floci authority as the oracle. Every merged op is verified by a golden
matrix recorded from Floci; ops not yet ported fail loudly (honest block) with
a clear local-mode diagnostic, never a guessed answer.

## Status

- Kernel: SigV4 sign/verify (tested), S3 storage model, S3 error taxonomy.
- S3 goldens recorded and diffed live against Floci on loopback.
- Queued: SQS (query/XML protocol adapter), DynamoDB (x-amz-json protocol
  adapter), ListObjectsV2 pagination, presigned URLs, CopyObject.
- Open scope item: record sugarbit's actual AWS surface to finalize the
  per-service op list (fail-loud boundary covers the rest).

Ports `aws-sdk-*` clients unmodified: they talk HTTP; SigV4 is verified at the
boundary. Services beyond the three listed are queued (see docs/queue.md).
