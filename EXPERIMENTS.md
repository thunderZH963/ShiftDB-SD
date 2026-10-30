# Reproducible host experiments

These scripts execute real local programs. Correctness fixtures are not hardware
performance experiments. No script generates placeholder results.

## Quick start

Requires Python 3.8+, CMake and a C++17 compiler. Run from the repository root:

```sh
python3 scripts/run_experiments.py all --list
./scripts/experiments/run_all.sh --repeat 1 --jobs 2
# Independent run, retaining a separate build configuration:
./scripts/experiments/run_all.sh --sanitize --build-type Debug
```

Each case has its own shell entry point and can be repeated with `--repeat N`.
All entry points accept `--output NEW_DIRECTORY`, `--build-dir DIRECTORY`,
`--timeout SECONDS`, and `--build-timeout SECONDS`. Existing result directories
are rejected rather than overwritten. Do not concurrently share a build directory;
use distinct `--build-dir` values for parallel invocations.

| Case | Entry point | What is exercised |
|---|---|---|
| `planner` | `scripts/experiments/run_planner.sh` | Planner contract checks |
| `resources` | `scripts/experiments/run_resources.sh` | Resources contract checks |
| `state` | `scripts/experiments/run_state.sh` | State contract checks |
| `ownership` | `scripts/experiments/run_ownership.sh` | Ownership contract checks |
| `handoff` | `scripts/experiments/run_handoff.sh` | Handoff contract checks |
| `bridge` | `scripts/experiments/run_bridge.sh` | Bridge contract checks |
| `planner-regression` | `scripts/experiments/run_planner_regression.sh` | planner regression checks |
| `resources-regression` | `scripts/experiments/run_resources_regression.sh` | resources regression checks |
| `handoff-regression` | `scripts/experiments/run_handoff_regression.sh` | handoff regression checks |
| `estimator-regression` | `scripts/experiments/run_estimator_regression.sh` | estimator regression checks |
| `migration_gate-regression` | `scripts/experiments/run_migration_gate_regression.sh` | migration_gate regression checks |
| `continuation` | `scripts/experiments/run_continuation.sh` | Whole-result continuation equivalence |
| `demo` | `scripts/experiments/run_demo.sh` | Telemetry-to-placement and handoff example |

## Recorded evidence

Every run writes a fresh UTC-stamped directory under `experiment-results/`:

- `metadata.json`: Git revision and pre-run worktree status, host, compiler,
  CMake version, build configuration, selected cases, repetitions and final status.
- `CMakeCache.txt`: resolved build configuration.
- `commands.json`: exact commands, return codes and elapsed wall times.
- `configure.log`, `build.log`, and one raw log per case/repetition.
- `runs.csv`: successfully completed cases and **harness** wall time.

A build failure, missing executable, timeout or nonzero test exit fails the run;
completed logs and failure metadata remain available. There is no empty CTest
selection that can silently pass. Harness duration includes process startup and
is not operator latency, query throughput, fairness, or a DPU speedup measurement.
Repeated correctness checks do not constitute independent workload samples.

## Scope and remaining experiments

Core tests cover the reference implementation and migration gate. They do not
link the actual native CAF/Arrow operator path. For dependency-specific validation,
see `TapDB/fpdb-shiftdb/scripts/check_native_syntax.py --help` and
`TapDB/fpdb-shiftdb/docs/active-migration.md`. Syntax checking is not link validation.
Native Arrow/appender tests require the documented `SHIFTDB_ARROW_TESTS` and
`SHIFTDB_APPENDER_TESTS` CMake options and compatible library/include paths;
configure these in a separate build and run its CTest suite from that build directory.

Multi-host active migration, SSB/TPC-H end-to-end latency, hardware-generation
comparisons and placement/credit ablations are not runnable from this repository
alone. They require a matching native engine and explicit deployment/workload
configuration. The demo uses fixture telemetry and is not a hardware benchmark.

## Harness regression tests

```sh
python3 -m unittest discover -s scripts -p 'test_*.py'
```
