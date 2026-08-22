with open('scripts/giant_file_issue_metadata.json', 'r') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    if i == 138 and '"number": 5447,' in line:
        new_lines.append('      ]\n')
        new_lines.append('    },\n')
        new_lines.append('    {\n')
        new_lines.append(line)
    else:
        new_lines.append(line)

with open('scripts/giant_file_issue_metadata.json', 'w') as f:
    f.writelines(new_lines)
