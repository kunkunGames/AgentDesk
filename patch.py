import json

with open("migrations/postgres/immutable-checksums.json", "r") as f:
    data = json.load(f)

data["protected_migrations"].append({
    "path": "migrations/postgres/0072_routine_fallback_retry.sql",
    "sha256": "0b688ac9daf46d45a76ea478cf0414642f4df3fc482f83324827a02ce02b0121",
    "version": 72
})

with open("migrations/postgres/immutable-checksums.json", "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
