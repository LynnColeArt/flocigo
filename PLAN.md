# flocigo: function-by-function port plan

## Program of record

Port the Floci AWS emulator from Java/Quarkus to Go, function by function.
Target scope: **S3, SQS, DynamoDB**. Oracle: the running Floci authority
(wire diff) plus the Java source (semantic reference). License: MIT.
Delivery: one static Go binary, embeddable as a library, serving HTTP on
loopback — no Docker, no JVM, no cloud account.

## Design principles (inherited from the factory)

- **Honest block**: any op not yet implemented returns a clear
  `NotImplemented(op)` local diagnostic — never a guessed answer.
- **Golden matrix as review**: every op is verified against wire recordings
  from Floci. The review gate is a conformance diff, not a judgment call.
- **Single access path**: all authority reads/writes go through the same
  binary the agents use; no side channels, no raw table access.
- **Kernel first**: the first N tickets build the router, protocol adapters,
  SigV4, error taxonomy, and storage models. After that, individual ops are
  same-day lane work.

## Ticket format

Each ticket = one Go package with:
1. A pinned task body (the op to implement, from the envelope).
2. Golden test data (recorded wire captures from Floci).
3. A test file that must pass when the function is implemented.
4. A dependency list (what must already exist).

## Dependency graph

```
SigV4 canonical fix ──► Error taxonomy ──► Protocol adapters ──► First op per service
    │                                                   │
    └── SigV4 presign (S3) ──┐                          │
                             └── Presigned S3 op ──────┤

S3 protocol adapter ──► S3 ops (6, in dependency order)
DDB JSON adapter ──► DDB expression engine (kernel: condition + update evaluator) ──► DDB ops (13: CRUD, query, scan, batch, transactions)
DDB JSON adapter ──► DDB PartiQL engine (kernel, if sugarbit uses PartiQL) ──► PartiQL op
SQS query adapter ──► SQS ops (5, long-poll timing is the deep kernel piece)
DDB JSON adapter ──► EB pattern engine ──► EB ops (bus CRUD + targets + ingress)
```

## Review protocol

Each ticket runs through the factory's qualified coding lane:
1. Model request (bounded, fenced, options-omitted as qualified).
2. Validated edit (structural envelope + scope pin).
3. Worker compiles and runs the existing test suite in network-none Docker.
4. Review: golden diff against Floci wire recordings (model output vs recorded
   expected output), envelope scope check (no extra ops).
5. Merge if and only if: zero findings, compile ok, tests pass, golden matrix
   matches.

## Ticket ordering

Kernel dependencies resolve before op tickets. Ops within a service follow
the dependency order their implementations imply. Cross-service ordering is
free (no cross-service dependency in the envelope).

## Progress tracking

Ledger: each ticket records its completion in the factory memory authority
(ticket-scoped document: `PORT#<ticket>`, event `op_merged`, detail including
golden matrix result and the specific Go package files changed).
