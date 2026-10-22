# Validation

## Runtime update — 2026-10-22

Environment: macOS x86_64, AppleClang 15.0.0, CMake 3.19.1; local tests only.

| Check | Result |
|---|---|
| Release build, with Arrow IPC and native TapDB appenders | 13/13 CTest suites passed |
| Debug core build with AddressSanitizer and UndefinedBehaviorSanitizer | 11/11 CTest suites passed |
| Reference operator continuation | 1,160 CHECKs passed across 16 handoffs |
| Planner regressions | 55 CHECKs passed, including upstream blocking dependency propagation |
| Resource/credit regressions | 157 CHECKs passed, including all lifecycle event orders and concurrent reservation |
| Handoff/ownership regressions | Passed; covers unacknowledged output, regressing replay, retired source, stale generation, journal rollback, and process exit after fsync commit |
| Pinned TapDB originals and plan bridge source contracts | Verified |

Continuation cases cover grouped integer aggregation with null payloads, duplicate-preserving inner join after build and during probing, and stable sort after partial output. Every case runs both pre-copy and stop-copy, to remote storage and Compute destinations, then compares complete results with uninterrupted execution. These endpoints are in-memory adapter instances governed by the real local ownership journal; they are not separate cluster hosts.

The Arrow build uses the existing project-local Arrow 17.0.0 binary and compiles TapDB's actual `ArrayAppender` sources. Full native CAF operator continuation and Gen3-SA/Gen4-SA query performance are not established by these tests. The external Arrow binary is not sanitizer-instrumented; the 11-suite sanitizer run covers the runtime and reference adapters.

To reproduce the core checks from the module directory:

```sh
./scripts/build_and_test.sh /tmp/shiftdb-release
SHIFTDB_SANITIZE=ON SHIFTDB_BUILD_TYPE=Debug ./scripts/build_and_test.sh /tmp/shiftdb-sanitize
python3 scripts/verify_upstream.py
```

Reference state images now use codec v2 with output acknowledgements. Restore accepts v1 conservatively with no acknowledged emitted prefix. The native Arrow envelope is unchanged. Invalid state progress or journal transitions fail closed; cancelling a failed replay discards its inactive target.

## Historical validation — 2026-09-23

Environment: macOS x86_64, AppleClang 15.0.0, CMake 3.19.1. Source: TapDB commit `4fa813eafcb35b29723f40718d22c0a6a33c47cf` from the pinned codeload archive. No remote cluster was used.

| Check | Result |
|---|---|
| Core-only build through patched TapDB root CMake | Passed |
| Six CTest suites, Debug | 6/6 passed |
| Combined executable assertions | 123 checks passed |
| AddressSanitizer + UndefinedBehaviorSanitizer build | Passed |
| Six CTest suites with sanitizers | 6/6 passed, no reported sanitizer errors |
| 100-batch aggregation with state handoff | Matches uninterrupted sum/count for every group |
| Demo with sanitizers | Passed |
| Pinned upstream-file hashes and bridge signatures | Passed |
| Reverse patch dry-run against edited TapDB files | Passed |
| Native TapDB/CAF/Arrow build | Not run in the local dependency environment |
| Distributed transport/state recovery tests | Not run |
| SSB/TPC-H end-to-end queries on Gen3-SA/Gen4-SA | Not run |

The 123 checks are assertions, not 123 benchmark trials. They include concurrent accounting, duplicate/reordered events, generation fencing, integer overflow, log capacity, metadata persistence, and lost-commit-reply handling. The tests run the reference adapters, not full SQL operators. They provide no evidence for the estimated speedup figures in the manuscript.

Build/test outputs are available under `../../build-shiftdb/Testing/Temporary/LastTest.log` and `../../build-shiftdb-sanitize/Testing/Temporary/LastTest.log` relative to the module directory.


## Native operator increment

- Eight local CTest suites pass in the Arrow build: original six plus IPC state
  and actual TapDB `ArrayAppender` tests.
- Arrow IPC: 22 checks, using Arrow 17.0.0 from project-local PyArrow.
- Native appender: 66 checks across int32/int64/double/date64/bool/string,
  null validity, duplicate values, repeat snapshots, append-after-snapshot,
  independent buffers and finalize/reuse. These compile upstream appender sources.
- `tapdb_operator_state.cpp`, `arrow_state.cpp`, and the native kernel test pass
  syntax checking with TapDB headers, Arrow/Gandiva 6 source headers, CAF 0.18.5
  headers, expected 1.0.0 and spdlog 1.8.1. The local check uses PyArrow's generated
  Arrow configuration-header fallback; it is not an Arrow 6 binary compatibility
  test or the custom upstream CAF fork deployment test.
- Full `GroupKernel`/`HashJoinBuildKernel` continuation tests are provided but not
  linked/run: the complete upstream LLVM/Gandiva/CAF/RDMA build is not installed.
- No native operator migration across live nodes or SSB/TPC-H SQL test was run.

Local logs: `../../build-shiftdb-arrow/Testing/Temporary/LastTest.log`.

- AddressSanitizer + UndefinedBehaviorSanitizer: all eight suites also pass in
  `../../build-shiftdb-arrow-sanitize`, with no reported sanitizer errors.
  The external prebuilt Arrow library itself is not sanitizer-instrumented.
