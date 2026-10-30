# Active operator migration

## Supported path

With `FPDB_ENABLE_SHIFTDB=ON`, the actual TapDB `POpActor` exposes a stop-copy
protocol for in-memory `AggregatePOp` and `GroupPOp` before output completion.
The destination may be a local or remote CAF actor. All nodes must run the same
ShiftDB-enabled build (the context wire schema includes routing maps).

1. Reserve destination capacity, including decoded state, the encoded image and
   buffered input. Keep source capacity charged until commit. The caller owns
   these holds; the native helper does not automatically connect ResourceLedger.
2. Construct a **fresh independent** operator from the same canonical query plan
   at the desired node. Do not shallow-copy mutable aggregate functions. Set its
   query ID/name/producers/consumers identically and its execution placement and
   context appropriately. Spawn it using TapDB's `POpActor` remote factory, without
   sending START or CONNECT. Do not reuse an actor that processed input.
3. Call the blocking helper on a dedicated control-plane thread:

```cpp
caf::scoped_actor controller(system);
auto outcome = shiftdb::native::migrate_active(
    controller, source_actor, fresh_target_actor, unique_ticket, plan_fingerprint);
if (!outcome.active) {
  // Retain both actors and resource holds. Inspect outcome.stage/error and retry
  // the identical ticket, source, target and fingerprint after transport repair.
}
```

The fingerprint must be generated from a canonical operator plan by the caller;
it is not a caller-chosen replacement for checking plan identity. Native codecs
also validate operator/function state signatures. Serialized context carries
producer completion flags and consumer actor handles. The target restores the
received-tuple counters so already-complete producers are not awaited again.

## Protocol and ownership

`freeze` runs between actor handlers: no kernel update is concurrent with capture.
Subsequent input is queued before normal counters or completion bookkeeping.
`install` restores Arrow state into the inactive target and sets its context actor
pointer locally. `commit` changes the source into a forwarding proxy, releases its
kernel state, and forwards queued messages in order. `activate` drains the target
queue before ordinary processing resumes. Future input continues through the old
actor handle; upstream routing is therefore stable during cutover. This first
implementation retains the proxy until query teardown rather than optimizing it
away. Transfer uses CAF's network transport, not a new RDMA state-transfer path.

The source can never execute again after commit. Duplicate freeze/install/commit/
activate requests are idempotent for the same ticket; a different target is rejected.
On an RPC timeout, commit may already have happened. The client checks source status
and retries commit/activation; it never treats timeout as permission to roll back.
Only a serialized controller may cancel before sending commit: discard the prepared
target, then abort the frozen source. Do not race cancel with a commit RPC. There is
no automatic cancellation on failed requests.

## Enforced scope

- Predicate-controlled, completed, file-backed and unsupported operator types are
  rejected. Active join probe/build, scan, external sort, pre-copy and repeated
  migration of an already activated target are not supplied by this path.
- Queues allow at most 4096 messages / approximately 64 MiB of tuple payload.
  Overflow fails the query; this is not upstream credit backpressure. Metadata,
  heap overhead and the CAF mailbox are not bounded by this payload limit.
- Source forwarding preserves order on each reliable live CAF channel. Reconnect
  replay/deduplication, process-loss recovery and durable distributed ownership
  are not implemented in this native actor protocol. The standalone core journal
  does not turn these native in-memory actors into crash-recoverable owners.
- The placement planner does not automatically trigger these RPCs. The adapter
  caller selects a destination, reserves resources and invokes the helper.

## Validation

The core migration gate regression checks conflicting tickets, commit retries,
wrong peers, abort-before-commit and refusal to roll back a committed source.
`native/tests/tapdb_actor_migration_test.cpp` supplies a real GroupPOp/POpActor
continuation test: one batch before freeze, one queued during freeze, one after
migration, commit retry, and exact group sums. It is an in-process protocol test.

```sh
cmake -S . -B build -DFPDB_ENABLE_SHIFTDB=ON -DSHIFTDB_NATIVE_KERNEL_TESTS=ON
cmake --build build --target shiftdb-native-actor-tests -j4
(cd build && ctest -R shiftdb-native-actors --output-on-failure)
```

The native actor/client/test and CAF wire metadata were syntax-checked locally.
Full native linking, this actor test, and actual multi-host continuation have not
been run in this workspace; they require the compatible TapDB Arrow/CAF/LLVM stack.
Do not interpret the core test results as multi-node database validation.
