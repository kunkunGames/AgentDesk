import re
with open("scripts/audit_allowlist.toml", "r") as f:
    lines = f.readlines()
with open("scripts/audit_allowlist.toml", "w") as f:
    for line in lines:
        if "1c6f6dcd93167903" in line or "8806b21b9432ae8a" in line:
            continue
        f.write(line)
