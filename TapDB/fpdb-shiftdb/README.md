# ShiftDB for TapDB — runtime core and native operator state

ShiftDB implements the paper's placement, state-handoff, and allocation-lifetime contracts in C++17. 

## Build and run

From the TapDB directory:

```sh
cmake -S . -B build-shiftdb -DFPDB_SHIFTDB_CORE_ONLY=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-shiftdb -j 4
(cd build-shiftdb && ctest --output-on-failure)
./build-shiftdb/fpdb-shiftdb/shiftdb-demo
```

The core-only path returns before TapDB's LLVM, Arrow, CAF and RDMA dependency setup. It runs on macOS and POSIX Linux with CMake 3.15+ and a C++17 compiler. `ctest` is run from its build directory for compatibility with CMake 3.19.

For address and undefined-behavior checks, add `-DSHIFTDB_SANITIZE=ON` to a separate build. The existing workspace build directories are one level above TapDB (`../build-shiftdb` and `../build-shiftdb-sanitize`).

The demo executes 100 batches of grouped aggregation. It snapshots after batch 40, copies/replays through 47, transfers ownership from storage-1 to storage-3, resumes at 48, and compares every group's sum/count to uninterrupted execution. Its timing is not a hardware benchmark.

## Paper-to-code map

| Paper mechanism | Executable files | Behavior |
|---|---|---|
| §5.1 DAG-Aware Placement | `planner.hpp`, `planner.cpp` | Validates DAGs and explicit adapter restrictions, propagates blocking dependencies through streaming paths, includes affected edge transfer and exposed transition costs, ranks positive feasible gains deterministically by normalized resource pressure |
| §5.2 Progress-Guided Handoff | `state.*`, `handoff.*` | Consistent snapshot/log boundary, versioned partition post-images, per-input frontiers, bounded out-of-order replay, source pause, target verification, commit then activation |
| Ownership commit | `ownership.*` | Single-writer fsync journal, conditional generation/route change, durable fence, idempotent commit, fail-closed unknown outcome |
| §5.3 Path-Coupled Admission | `resources.*` | Atomic multi-resource reservation, lease expiry, rollback, separate execution slots and logical byte credits, consumer references, old-route retention |
| §6 operator interfaces | `state.*` | Executable integer aggregation, duplicate-preserving inner join, stable in-memory sort and output cursor; no raw pointer serialization |
| TapDB boundary | `tapdb_bridge.hpp`, patched `Execution.cpp` | Exports actual physical operator names/types, graph edges, node IDs and remote-spawn flags; preserves upstream execution |

`best_move()` makes one decision against a supplied plan and telemetry snapshot. The caller reapplies it after updating the plan; a production controller must own that loop, refresh service/route measurements, and coordinate runtime RPCs. Blocking edges and streaming edges use different coarse cost recurrences. Costs are estimates, not a full pipelined query simulator. The executable scenario tables in the paper are not inputs to these correctness tests.

## State and lifecycle semantics

- An adapter instance represents one independently restorable partition. An input batch has an edge ID, contiguous sequence, and current owner generation. A duplicate sequence is ignored; a gap does not advance the frontier.
- State mutation and the frontier are staged together. Memory-budget failure leaves both unchanged. The budget bounds **encoded state**, not native heap/RSS. A production Arrow adapter must connect actual allocations to its memory pool and resource ledger.
- Snapshots carry a codec version, state kind, per-edge frontiers, sequence, phase, output cursor and contiguous publication acknowledgement. Integers use explicit little-endian encoding, not ABI-dependent containers. The FNV checksum detects accidental corruption; it is not authentication.
- Aggregate state retains signed integer sum/min/max and a non-null count; null values do not contribute. Signed overflow is rejected before publication. AVG can be derived from sum/count. DISTINCT, decimal/floating semantics, nullable keys and multi-column SQL schemas need native adapters.
- Inner join retains every build tuple and matched build/probe IDs, including duplicate keys. Build input after probing starts is rejected. This reference kernel uses a simple matching loop; it does not replace TapDB's optimized hash-join implementation. Outer/semi/anti joins remain unsupported.
- Sort retains rows, stable equal-key order, finalization and an output cursor. After a generated row crosses the reliable publication boundary, call `acknowledge_output(generation, next_cursor)`. Cutover waits until every generated row has a contiguous delivery acknowledgement; final consumer release remains a separate credit event. This is in-memory reference sort, not external-run migration. Gate actual output publication through `OwnershipStore::can_publish()`.
- Each delta is a **full post-image of the changed partition**, rather than an additive update. This makes duplicate replay safe but is deliberately unoptimized. Delta-log and reorder buffers are bounded. Input waits when the source log cannot grow; the caller catches up and retries.
- `Handoff::begin()` reserves target/path resources and copies the base. `catch_up()` applies deltas and returns contiguous acknowledgements. `cutover()` checks remaining benefit and pause budget, freezes the source, verifies final frontiers, and commits ownership before activating the target.
- Call `cancel()` after a declined action or exception. If the commit succeeded but its reply was lost, cancellation resolves the durable record and finishes activation; it cannot restore the old owner. If journal I/O is uncertain, both state copies/holds are retained for explicit resolution.
- Source state is retained until commit. This implementation does not reclaim partitions early from a remote base or spill file. Destination holds remain charged after commit until the owner explicitly releases them. Source occupied memory must be separately registered; the core does not infer it from container sizes.
- Leases use caller-provided monotonic ticks. A committed resource hold never expires implicitly. There is one serialized controller operation per partition; this is not a multi-writer distributed transaction service.
- The metadata journal survives reopening and refuses a corrupt/torn tail. The state itself is not crash-checkpointed by the demo. Full process-loss recovery requires durable snapshots/logs and a shared metadata service, not only this local journal.

## Correctness updates

- Credit collection requires execution completion, publication sealing, and the last consumer reference. Any event order is safe, and released reference IDs cannot be resurrected by a delayed registration retry.
- Failed joint reservations leave usage unchanged. Demand vectors and cost inputs reject nonfinite or negative values; expired/released tickets cannot acquire capacity again.
- Journal recovery validates owner transitions as well as checksums. It refuses correctly framed rollback records and remembers every used ticket across reopen. A process-termination test verifies a durable commit without graceful teardown.
- State codec v2 includes the output acknowledgement. A restored v1 image conservatively treats its emitted prefix as unacknowledged. Native Arrow envelopes use their own codec and are unchanged.
- Replay refuses input-frontier, output-cursor, output-acknowledgement, finalization, or join-phase regression. An invalid replay requires cancelling the handoff and discarding its inactive target. Retired source state cannot be reactivated until explicitly restored.
- The planner carries the producer's earliest start through streaming edges, preserving upstream blocking gates. Equal-score choices use deterministic ordering; scans have the same explicit legality/adapter checks as other operators.

## Native TapDB build

In an environment that already builds TapDB:

```sh
cmake -S . -B build -DFPDB_ENABLE_SHIFTDB=ON
cmake --build build -j 4
# Before launching an existing TapDB query test:
export SHIFTDB_PLAN_TRACE=/tmp/shiftdb-plan
```

`Execution::boot()` writes `/tmp/shiftdb-plan-<query-id>.json` before actor construction. Use unique query IDs within a trace directory. Without the environment variable the hook does no file I/O. The flag enables the library, operator state codecs and observation; it does not enable live migration. Existing TapDB deployment and benchmark commands remain those of upstream.

See [Native integration plan](docs/tapdb-integration.md) for exact implementation boundaries, and [Validation report](docs/validation.md) for the checks run in this workspace.


## Native operator work

The actual TapDB `AggregatePOp`, `GroupKernel`, `HashJoinBuildKernel` and `SortPOp`
now have snapshot/restore adapters. Group snapshots include pending input, and
join restore rebuilds TapDB's native hash index. Native Arrow types and null
payloads survive serialization. Source appenders are not consumed by snapshots.

See [Native operator interfaces, contracts and test commands](docs/native-operators.md).
The local suite includes real Arrow IPC and upstream appender execution when
the explicit dependency flags are enabled. Full native kernel tests are supplied separately for the complete
TapDB dependency environment; they have not been executed locally. Live actor
migration remains disabled until channel/ownership integration is complete.
