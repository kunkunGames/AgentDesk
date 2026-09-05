import sys

with open('scripts/analyze_prs.py', 'r') as f:
    content = f.read()

# 1. Update no-change logic
content = content.replace(
    'if "no-change" in title.lower():',
    'if "no-change" in title.lower() or "no change" in title.lower():'
)
content = content.replace(
    'print(f"  [i] EMPTY NO-CHANGE PR: No changed files. If no durable queue-hygiene artifact is changed, it is a close candidate (report only).")',
    'print(f"  [!] EMPTY NO-CHANGE PR: No changed files. A no-change result should not become a PR unless it explicitly changes a queue-hygiene artifact. Consider closing.")'
)

# 2. Add is_generated_inventory_path helper
is_scratch_func = "    return False\n\ndef main():\n"
new_helper = """    return False

def is_generated_inventory_path(path):
    generated_inventory_files = {
        "docs/generated/module-inventory.md",
        "docs/generated/route-inventory.md",
        "docs/generated/worker-inventory.md",
    }
    return path in generated_inventory_files

def main():
"""
content = content.replace(is_scratch_func, new_helper)

# 3. Add generated inventory aggregation logic
loop_block = """        # Scratch file detection
        if files_data.get("files") is not None:
            scratch_files = []
            for f in files_data["files"]:
                path = f.get("path", "")
                if is_scratch_file_path(path):
                    scratch_files.append(path)
            if scratch_files:
                print(f"  [!] SCRATCH FILE DETECTED: PR includes scratch files like pr-body.md, plan.md, or test scripts ({', '.join(scratch_files)}).")"""

new_loop_block = """        # Scratch file detection
        if files_data.get("files") is not None:
            scratch_files = []
            generated_inventory_files = []
            other_files = []
            for f in files_data["files"]:
                path = f.get("path", "")
                if is_scratch_file_path(path):
                    scratch_files.append(path)
                if is_generated_inventory_path(path):
                    generated_inventory_files.append(path)
                else:
                    other_files.append(path)

            if scratch_files:
                print(f"  [!] SCRATCH FILE DETECTED: PR includes scratch files like pr-body.md, plan.md, or test scripts ({', '.join(scratch_files)}).")
            if generated_inventory_files and not other_files:
                print(f"  [!] GENERATED INVENTORY DRIFT ONLY: PR's entire diff is regenerated inventory docs ({', '.join(generated_inventory_files)}). Re-run scripts/generate_inventory_docs.py on main and file a no-change report instead of a PR.")"""

content = content.replace(loop_block, new_loop_block)

with open('scripts/analyze_prs.py', 'w') as f:
    f.write(content)
