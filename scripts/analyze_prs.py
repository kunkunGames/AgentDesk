import subprocess
import json

def run(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    return result.stdout.strip(), result.returncode

print("Fetching PRs...")
prs_json, gh_code = run("gh pr list --repo kunkunGames/AgentDesk --state open --limit 50 --json number,title,headRefName,createdAt")

if gh_code != 0 or not prs_json:
    print("Warning: `gh` CLI not available or failed. Skipping PR analysis.")
    exit(0)

try:
    prs = json.loads(prs_json)
except Exception as e:
    print(f"Error parsing JSON: {e}")
    print(prs_json)
    exit(1)

inventory_refresh_count = 0

for pr in prs:
    num = pr['number']
    title = pr['title']
    print(f"\n# {num} - {title}")
    
    # Get diff stat
    stat, _ = run(f"gh pr diff {num} --repo kunkunGames/AgentDesk --stat")
    print(f"Stat:\n{stat}")

    # PR #214/#215 lesson: no-change PRs must have 0 changed files
    if "no-change" in title.lower():
        files_json, _ = run(f"gh pr view {num} --repo kunkunGames/AgentDesk --json files")
        try:
            files_data = json.loads(files_json)
            if files_data.get("files") and len(files_data["files"]) > 0:
                print(f"  [!] UNSAFE NO-CHANGE PR: Title claims no-change but modifies {len(files_data['files'])} files.")
        except Exception:
            pass

    # PR #199/#200/#201 lesson: check for multiple inventory refreshes
    if "inventory" in title.lower() and "refresh" in title.lower():
        inventory_refresh_count += 1

if inventory_refresh_count > 1:
    print("\n[!] WARNING: Multiple open inventory refresh PRs detected. Ensure strict duplicate-PR guard is followed.")
