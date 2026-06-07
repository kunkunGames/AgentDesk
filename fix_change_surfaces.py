import re

def main():
    with open('docs/agent-maintenance/change-surfaces.md', 'r') as f:
        surfaces = f.read()

    new_surfaces = []
    lines = surfaces.splitlines()
    for i, line in enumerate(lines):
        if "src/services/auto_queue/activate_command.rs" in line:
            line = line.replace("1351", "1354")
        elif "src/services/opencode.rs" in line:
            line = line.replace("1881", "1886")
        elif "src/services/turn_orchestrator.rs" in line:
            line = line.replace("3089", "3090")
        new_surfaces.append(line)

    with open('docs/agent-maintenance/change-surfaces.md', 'w') as f:
        f.write('\n'.join(new_surfaces) + '\n')

if __name__ == '__main__':
    main()
