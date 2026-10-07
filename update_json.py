import json

with open('scripts/giant_file_issue_metadata.json', 'r') as f:
    data = json.load(f)

data['refreshed_at'] = '2026-10-07T06:53:06Z'

with open('scripts/giant_file_issue_metadata.json', 'w') as f:
    json.dump(data, f, indent=2)
    f.write('\n')
