> Operator checkpoint codecs have now been implemented; see
> [native-operators.md](native-operators.md) for current coverage and calling contracts.
> The remaining integration steps below concern live actors and distributed execution.

# Native TapDB integration boundaries

## Changes already applied

- Root `CMakeLists.txt`: `FPDB_SHIFTDB_CORE_ONLY` builds/tests the module before heavyweight dependency setup; `FPDB_ENABLE_SHIFTDB` adds it to a native build.
- `fpdb-executor/CMakeLists.txt`: conditional linkage and compilation flag.
- `fpdb-executor/src/Execution.cpp`, `Execution::boot()`: calls the typed physical-plan observation bridge before actors are created. The bridge uses `PhysicalPlan::getPhysicalOps()` and actual `PhysicalOp` getters, including `producers()`, `consumers()`, `getNodeId()` and `spawnOnRemote()`.

These changes are available as `integration.patch`. Upstream originals are preserved in `upstream-original/` for review. They are not a separate runnable checkout.

## Next integration steps, in dependency order

1. **Telemetry and candidate construction.** Populate `Plan` with node-specific remaining service estimates and directed route throughput. Native node IDs are not automatically storage roles. Register per-operator destination compatibility and any required input routes; scan source location is an input to this same placement model. Keep an operator pinned only when its active-state adapter is unavailable. Observe actual actors/locations after TapDB's predicate-driven choice; the initial plan export alone is not runtime placement telemetry.
2. **CAF batch protocol.** Extend `fpdb-executor/message/TupleMessage.h`, `physical/POpContext.cpp` and associated CAF serializers with operator/query identity, input edge sequence, owner generation, route epoch and allocation ID. Register downstream references before exposing a message. Implement a reverse final-release message; receiving or sending a TupleSet is not consumer release. Version-gate peers so mixed builds do not deserialize incompatible messages.
3. **Aggregation.** `physical/group/GroupKernel.h` stores `aggregateResultsPartial_`, per-group appenders, final results and input schemas privately. Add a mutation-safe snapshot API in the kernel; do not serialize the CAF actor or copy `shared_ptr` addresses. The portable schema must represent each actual `AggregateResult`, typed Arrow scalar, null semantics, and grouping comparator. The reference integer adapter establishes the protocol, not Arrow equivalence.
4. **Hash join.** Adapt `HashJoinBuildKernel`, `HashJoinProbeAbstractKernel`, `HashJoinProbeKernel` and `TupleSetIndex` together. Preserve duplicate chains and tuple IDs; quiesce corresponding probes when moving build ownership. Native join variants must each pass whole-result tests. Register only supported join types.
5. **Sort.** `SortPOp` contains Arrow buffers and spill-file indices, while `LimitSortPOp` adds separate semantics. Export run manifests and comparators, secure file access at the target, checkpoint unread offsets and merge progress, and gate ordered output. The reference sort does not provide external-run transfer.
6. **Transport and metadata.** Bind base/delta chunks to the CAF/RDMA transport with bounded credits, checksums and retry IDs. Report each contiguous output-delivery frontier before cutover; this acknowledgement is separate from the last consumer's memory-release callback. All three credit events---execution complete, publication sealed, and final reference released---must arrive before memory or route retirement. Replace the single-host ownership journal with a cluster-visible conditional owner/route service. Persist state images and delta logs if recovery from process loss is required. Renew remote holds and resolve unknown commit replies before publishing.
7. **Execution controller.** Apply positive, feasible `best_move()` decisions to versioned plans through the handoff protocol. Recompute cost after each accepted action. Feed source/target/path peak demand to `ResourceLedger`; supply actual allocator coverage separately from logical output credits. No native plan mutation should precede complete preparation.

## End-to-end acceptance

For SSB/TPC-H, first compare typed results under uninterrupted and forced-migration execution, including join duplicate multiplicity, NULLs, decimals, ordered queries and empty input. Then inject duplicate/delayed chunks, cancellation, stale generations and lost commit replies. Only after native tests pass should hardware query runtimes replace estimated figure tables. The local C++ demo does not produce those query measurements.
