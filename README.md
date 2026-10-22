# ShiftDB code

This repository contains the ShiftDB C++17 runtime core, operator-state adapters, TapDB integration changes, and tests. Its TapDB source is based on commit `4fa813eafcb35b29723f40718d22c0a6a33c47cf`; `TapDB/fpdb-shiftdb/UPSTREAM.json` records the integration base. Generated builds, local dependencies, benchmark data, and machine-specific TapDB `resources/` are omitted. Obtain the latter from the pinned upstream TapDB revision for a full deployment.

Start with [TapDB/fpdb-shiftdb/README.md](TapDB/fpdb-shiftdb/README.md) for the paper-to-code map and native operator contracts. ShiftDB core implementation is under `TapDB/fpdb-shiftdb/{include,src,native}`; TapDB integration changes are in the adjacent `fpdb-executor` and root CMake files.

## Build the independent core

```sh
cmake -S TapDB -B build-shiftdb -DFPDB_SHIFTDB_CORE_ONLY=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-shiftdb -j 4
(cd build-shiftdb && ctest --output-on-failure)
./build-shiftdb/fpdb-shiftdb/shiftdb-demo
```

The core build requires CMake 3.15+ and a C++17 compiler. A full TapDB build additionally requires TapDB's upstream dependencies. `FPDB_ENABLE_SHIFTDB=ON` enables the module and opt-in plan observation, but does not enable migration of live native CAF operators. The demo and unit tests check protocol behavior; they do not establish the paper's hardware performance results.

`implementation-manifest.json` records the current module source hashes and its pinned upstream revision. The public copy replaces hard-coded Redis and sample object-store credentials with `FPDB_REDIS_PASSWORD`, `FPDB_AIRMETTLE_ACCESS_KEY`, `FPDB_AIRMETTLE_SECRET_KEY`, `FPDB_MINIO_ACCESS_KEY`, and `FPDB_MINIO_SECRET_KEY` environment variables. New builds and local dependency directories are ignored by `.gitignore`.

## Runtime update

The current update fixes DAG timing through streaming operators, makes equal-score placement deterministic, and validates an entire move before changing a plan. The handoff protocol now checks source generation and contiguous output-delivery acknowledgements. State replay rejects regressing input/output frontiers, and journal recovery rejects invalid ownership transitions even when their checksums are valid. Output memory and old routes remain charged until completion, publication sealing, and the last consumer release have all arrived.

Regression tests include lease expiry, all lifecycle event orders, delayed/repeated events, publication before cutover, a process exit after durable ownership commit, and whole-result continuation. See [the validation report](TapDB/fpdb-shiftdb/docs/validation.md) for the build and dependency coverage.
