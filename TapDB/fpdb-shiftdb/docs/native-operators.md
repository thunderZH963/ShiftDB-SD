# Native TapDB operator state

## Implemented interfaces

`native/include/shiftdb/tapdb_operator_state.hpp` exposes overloaded
`TapDBStateCodec::snapshot(source, binding)` and
`TapDBStateCodec::restore(target, bytes, expected_binding)` for actual TapDB types.
The implementation is compiled into `fpdb-executor` with `FPDB_ENABLE_SHIFTDB=ON`.
It does not replace TapDB's execution kernels with reference integer operators.

| Native type | State captured | Restore operation |
|---|---|---|
| `AggregatePOp` | Every function's partial result map, Arrow scalar types/nulls, expression input schema or direct-column value type, received-input flag | Recompile expressions where needed; restore original partials for native finalization |
| `GroupKernel` | Group keys, partial result vectors, **pending input appenders**, input schema, pending row count | Recreate schema/column indexes, function compilation and appenders; retain unaggregated input |
| `HashJoinBuildKernel` | Key columns and the complete underlying build table | Rebuild native `TupleSetIndex`; retain duplicate rows and payload nulls |
| `SortPOp` | In-memory input table, sort-key order, input-arrival counters and input flag | Recreate buffer and invalidate cached Arrow sort options |

The checkpoint envelope binds a state image to an operator identity, canonical
plan fingerprint, generation, input frontiers and output frontier. Arrow IPC
preserves actual schemas, decimal scales, validity and row order. No raw C++ or
CAF pointers are transferred. The checksum detects corruption; it is not a
security signature. Encoded-image limits do not bound decoded heap allocation.

`ArrayAppender::snapshot() const` copies pending values into a separate Arrow
builder. It never calls `Finish()` on the live builder. Repeated checkpoints do
not consume input. `finalize()` now clears the validity buffer together with
values, so reuse does not retain previous null flags.

## Calling contract

1. Pause input delivery and quiesce the source actor **between** message handlers.
   Collect channel frontiers at the same boundary. These methods do not lock actors.
2. Construct an inactive target from the same canonical physical operator plan.
   Use separate aggregate function instances; copying shared function pointers
   across source and target would share mutable compilation caches.
3. Supply the controller's trusted identity/fingerprint/generation, encode, transfer,
   and restore. Restore failure requires discarding the fresh target; compilation
   caches may already have been initialized before a later validation fails.
4. Reinstall CAF producer/consumer connections, completion state, pending channels,
   routing and scheduler metadata separately. Restore does **not** restore a whole
   actor or grant publication rights.
5. Fence the old owner and activate the destination only through the ownership
   protocol. A successful snapshot or copy acknowledgement never authorizes deletion.

Snapshot frontiers are supplied by the caller, not inferred from TapDB tuple
messages. Therefore the plan exporter still reports
`active_migration_supported=false`: actor quiescence, delta capture, message
replay and ownership activation are not wired into live SQL execution yet.

## Operator coverage boundaries

- Aggregation retains native partials rather than prematurely finalizing AVG into
  a single value. Actual SQL function/type support remains TapDB's support.
- Grouped state is captured before finalization. Null grouping keys are not an
  enabled migration path: upstream `TupleKey` lacks nullable-key semantics.
- Build state is supported separately from probe execution. Active probe output,
  outer/semi/anti join match tracking and their channel state are not checkpointed.
  Nullable build keys are rejected; nullable payload columns are retained.
- Sort checkpoints require in-memory, pre-output buffers. Spill files, external
  merge runs and emitted sort prefixes require another adapter and are rejected.
- Scan offsets, filter/project input channels and pending outgoing tuple messages
  are not covered by these four state codecs.

## Run local Arrow and native appender tests

Provide an existing Arrow shared library/header tree, tl::expected 1.0.0 headers
and spdlog 1.8.1 headers. From TapDB:

```sh
cmake -S . -B build-arrow \
  -DFPDB_SHIFTDB_CORE_ONLY=ON -DSHIFTDB_ARROW_TESTS=ON \
  -DSHIFTDB_ARROW_INCLUDE_DIR=/path/to/arrow/include \
  -DSHIFTDB_ARROW_LIBRARY=/path/to/libarrow.dylib \
  -DSHIFTDB_APPENDER_TESTS=ON \
  -DSHIFTDB_EXPECTED_INCLUDE_DIR=/path/to/expected/include \
  -DSHIFTDB_SPDLOG_INCLUDE_DIR=/path/to/spdlog/include
cmake --build build-arrow -j4
(cd build-arrow && ctest --output-on-failure)
```

Use `.so` on Linux. These tests link actual Arrow and compile the actual TapDB
appender sources. The local run uses Arrow 17 from a project-local PyArrow wheel.
It is not a validation of upstream's complete Arrow 6/CAF/RDMA deployment.

For an environment with the full TapDB dependency stack:

```sh
cmake -S . -B build -DFPDB_ENABLE_SHIFTDB=ON -DSHIFTDB_NATIVE_KERNEL_TESTS=ON
cmake --build build --target shiftdb-native-kernel-tests -j4
(cd build && ctest -R shiftdb-native-kernels --output-on-failure)
```

That test exercises real `GroupKernel` continuation after snapshot/restore and
native duplicate-preserving `HashJoinBuildKernel` reconstruction. Its source was
syntax-checked locally; the full native test has **not** been linked/run here.
