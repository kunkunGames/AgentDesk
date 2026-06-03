import subprocess
import json

def run(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    return result.stdout.strip()

print("Fetching PRs...")
prs_json = run("gh pr list --repo kunkunGames/AgentDesk --state open --limit 50 --json number,title,headRefName,createdAt")
try:
    prs = json.loads(prs_json)
except Exception as e:
    print(f"Error parsing JSON: {e}")
    print(prs_json)
    exit(1)

for pr in prs:
    num = pr['number']
    title = pr['title']
    print(f"\n# {num} - {title}")
    
    # Get diff stat
    stat = run(f"gh pr diff {num} --repo kunkunGames/AgentDesk --stat")
    print(f"Stat:\n{stat}")
