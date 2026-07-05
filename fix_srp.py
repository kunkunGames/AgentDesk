import json

with open("scripts/audit_maintainability/baselines/route_srp.json", "r") as f:
    data = json.load(f)

data["total_count"] = 12

for key in ["src/server/routes/agents.rs", "src/server/routes/meetings.rs", "src/server/routes/pipeline.rs"]:
    if key in data["files"]:
        del data["files"][key]

with open("scripts/audit_maintainability/baselines/route_srp.json", "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
