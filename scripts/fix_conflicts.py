import os

def fix_file(filepath, conflict_blocks):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    for head, upstream, replacement in conflict_blocks:
        start_idx = content.find("<<<<<<< HEAD\n" + head)
        end_idx = content.find(">>>>>>> upstream/main\n", start_idx)
        if start_idx != -1 and end_idx != -1:
            end_idx += len(">>>>>>> upstream/main\n")
            content = content[:start_idx] + replacement + content[end_idx:]
        else:
            print(f"Could not find conflict in {filepath}")
            
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Fixed {filepath}")

# For generate_inventory_docs.py
head1 = 'TEST_FILE_NAMES = {"integration_tests.rs", "tests.rs", "high_risk_recovery.rs"}\n=======\n'
upstream1_start = 'TEST_FILE_NAMES = {"integration_tests.rs", "tests.rs"}\nGIANT_FILE_REGISTRY'
replacement1 = 'TEST_FILE_NAMES = {"integration_tests.rs", "tests.rs", "high_risk_recovery.rs"}\nGIANT_FILE_REGISTRY'

# Wait, the upstream has a lot of text. Let's just find the `<<<<<<< HEAD` to `>>>>>>> upstream/main` and do string manipulation.
with open("scripts/generate_inventory_docs.py", "r", encoding='utf-8') as f:
    text = f.read()

import re
text = re.sub(
    r'<<<<<<< HEAD\nTEST_FILE_NAMES = \{"integration_tests\.rs", "tests\.rs", "high_risk_recovery\.rs"\}\n=======\nTEST_FILE_NAMES = \{"integration_tests\.rs", "tests\.rs"\}',
    'TEST_FILE_NAMES = {"integration_tests.rs", "tests.rs", "high_risk_recovery.rs"}',
    text,
    flags=re.MULTILINE
)
text = text.replace('>>>>>>> upstream/main\n', '')
with open("scripts/generate_inventory_docs.py", "w", encoding='utf-8') as f:
    f.write(text)
print("Fixed scripts/generate_inventory_docs.py")
