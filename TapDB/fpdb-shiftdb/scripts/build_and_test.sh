#!/bin/sh
set -eu
module_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
build_dir=${1:-"$module_dir/../build-shiftdb"}
cmake -S "$module_dir/.." -B "$build_dir" -DFPDB_SHIFTDB_CORE_ONLY=ON \
  -DCMAKE_BUILD_TYPE="${SHIFTDB_BUILD_TYPE:-Release}" \
  -DSHIFTDB_SANITIZE="${SHIFTDB_SANITIZE:-OFF}"
cmake --build "$build_dir" -j 4
(cd "$build_dir" && ctest --output-on-failure)
"$build_dir/fpdb-shiftdb/shiftdb-demo"
