import subprocess
import os

print("testing scripts/check_writer_gate_ci_wiring.py...")
res = subprocess.run(["python3", "scripts/check_writer_gate_ci_wiring.py"], capture_output=True, text=True)
print(res.stdout)
print(res.stderr)
print(res.returncode)

print("testing python3 scripts/giant_file_progress.py")
# The error was in giant_file_progress.py, wait, why did it fail in CI?
# In giant_file_progress.py:
# `giant progress failed: progress requires an exact same-repository PR`
