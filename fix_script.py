with open('scripts/generate_inventory_docs.py', 'r') as f:
    lines = f.readlines()
with open('scripts/generate_inventory_docs.py', 'w') as f:
    skip = False
    for line in lines:
        if line.startswith('<<<<<<< ours'):
            continue
        if line.startswith('======='):
            skip = True
            continue
        if line.startswith('GIANT_FILE_REGISTRY'):
            skip = False
        if not skip:
            f.write(line)
