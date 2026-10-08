#!/usr/bin/env python3
"""Offline source-contract verification, not a native compilation test."""
import hashlib,json,pathlib
module=pathlib.Path(__file__).resolve().parents[1];root=module.parent
info=json.loads((module/'UPSTREAM.json').read_text())
for name,digest in info['original_files'].items():
 assert hashlib.sha256((module/'upstream-original'/name).read_bytes()).hexdigest()==digest,name
contracts={
 'fpdb-executor/include/fpdb/executor/physical/PhysicalPlan.h':['getPhysicalOps() const'],
 'fpdb-executor/include/fpdb/executor/physical/PhysicalOp.h':['getNodeId() const','spawnOnRemote() const','producers()','consumers()','getTypeString() const'],
 'fpdb-executor/src/Execution.cpp':['shiftdb::tapdb::trace_if_enabled(*physicalPlan_, queryId_)'],
 'fpdb-executor/CMakeLists.txt':['target_link_libraries(fpdb-executor PUBLIC fpdb-shiftdb)']}
for name,needles in contracts.items():
 text=(root/name).read_text()
 for needle in needles:assert needle in text,(name,needle)
print('Pinned upstream originals and TapDB bridge source contracts verified.')
