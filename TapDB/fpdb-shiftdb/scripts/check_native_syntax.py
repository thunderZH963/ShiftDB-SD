#!/usr/bin/env python3
"""Syntax-check adapters against caller-provided TapDB dependency headers.

No download or linking. Pass --include for Arrow, Gandiva, CAF (including generated
headers), expected, spdlog and other upstream include roots. Use a consistent
installed dependency stack for deployment validation.
"""
import argparse
import pathlib
import subprocess
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--include',action='append',default=[])
p.add_argument('--compiler',default='clang++')
a=p.parse_args()
m=pathlib.Path(__file__).resolve().parents[1];root=m.parent
includes=[*map(pathlib.Path,a.include),*root.glob('*/include'),m/'native/include']
for source in ('native/src/arrow_state.cpp','native/src/tapdb_operator_state.cpp','native/tests/tapdb_kernel_test.cpp'):
    subprocess.run([a.compiler,'-std=c++17','-fsyntax-only',*['-I'+str(x) for x in includes],str(m/source)],check=True)
print('Native adapter and test syntax checks passed; not a linked/runtime test.')
