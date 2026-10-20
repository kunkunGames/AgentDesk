"""Closed Cargo selections shared by the timing runner and CI wiring guards."""
from __future__ import annotations

import re
import shlex

WATCHDOG_PREFIX = "services::discord::health::recovery::self_watchdog::"
CODEX_ID = ("services::codex_tui::rollout_tail::tests::"
            "always_fresh_pane_signal_reaches_hard_veto_cap_and_emits_done")
TIMING_FILTERS = (WATCHDOG_PREFIX, CODEX_ID)
SERIAL_PREFIX = ["python3", "scripts/ci/timing_test_isolation.py", "serial", "--"]
ROOT_UNSET = ["env", "-u", "AGENTDESK_ROOT_DIR"]
ARTIFACT_RUN = ["python3", "scripts/ci/lib-test-artifact.py", "run", "--"]


def is_timing_id(name: str) -> bool:
    return name.startswith(WATCHDOG_PREFIX) or name == CODEX_ID


def serial_command(*, prepared=False, unset_root=False):
    return [*(ROOT_UNSET if unset_root else []), *(ARTIFACT_RUN if prepared else []),
            "cargo", "test", "--lib", "--", *TIMING_FILTERS, "--test-threads=1"]


def serial_argv_allowed(command) -> bool:
    return any(list(command) == serial_command(prepared=prepared, unset_root=unset_root)
               for prepared, unset_root in ((False, False), (False, True), (True, True)))


def sweep_script(*, all_targets=False, prepared=False) -> str:
    if all_targets and prepared:
        raise ValueError("the prepared artifact contains only the library target")
    skips = shlex.join([item for value in TIMING_FILTERS for item in ("--skip", value)])
    cargo = [*(ARTIFACT_RUN if prepared else []), "cargo", "test",
             "--all-targets" if all_targets else "--lib"]
    broad = shlex.join(cargo) + ' -- "${NON_PG_SKIP_ARGS[@]}" ' + skips
    if not all_targets:
        broad = ('python3 scripts/run_test_lane.py --lane non-pg-sweep --max-summaries 4 '
                 '"${NON_PG_SKIP_ARGS[@]}" ' + skips + ' -- env -u AGENTDESK_ROOT_DIR ' + broad)
    serial = SERIAL_PREFIX + serial_command(prepared=prepared, unset_root=not all_targets)
    return "\n".join((
        "set -eo pipefail",
        "source scripts/ci/non-pg-test-filter.sh",
        "python3 scripts/ci/timing_test_isolation.py check",
        broad,
        "run_non_pg_filter_replay",
        shlex.join(serial),
        "",
    ))


def verified_timing_skip_lines(text: str) -> frozenset[int]:
    """Allow timing exclusions only inside an exact, complete literal run scalar.

    Return zero-based line indices. No partial block, folded YAML, extra command,
    conditional serial run, added skip, or duplicated phase can obtain an exception.
    This never invents a Cargo command: all analyzers still read the actual argv.
    """
    allowed = {sweep_script(all_targets=True), sweep_script(), sweep_script(prepared=True)}
    lines = text.splitlines(keepends=True)
    verified, context, index = set(), [], 0
    while index < len(lines):
        line = lines[index]
        if not line.strip() or line.lstrip().startswith("#"):
            index += 1
            continue
        indentation = len(line) - len(line.lstrip())
        while context and context[-1][0] >= indentation:
            context.pop()
        mapping = re.fullmatch(r"( *)(- )?([A-Za-z0-9_-]+):[ ]*(.*?)[\r\n]*", line)
        if mapping and mapping[2] and context and context[-1][1] == "steps":
            context.append((indentation, "@step"))
        # Skip EVERY YAML block scalar as a unit, including env data and an
        # outer run that contains a shell heredoc. A nested 'run: |' is data.
        block = re.search(r":[ ]*(?:(?:![^ ]+|&[^ ]+)[ ]+)*[|>][0-9+-]*[ ]*(?:#.*)?\n$", line)
        if block:
            end = index + 1
            while end < len(lines):
                value = lines[end]
                if value.strip() and len(value) - len(value.lstrip()) <= indentation:
                    break
                end += 1
            actual_step = (len(context) == 4 and context[0] == (0, "jobs")
                           and context[2][1] == "steps" and context[3][1] == "@step")
            if mapping and mapping[3] == "run" and mapping[4] == "|" and actual_step:
                body = lines[index + 1:end]
                content_indent = min((len(value) - len(value.lstrip())
                                      for value in body if value.strip()), default=0)
                normalized = "".join(value[content_indent:] if value.strip() else "\n"
                                     for value in body)
                if normalized.rstrip("\n") + "\n" in allowed:
                    verified.update(number for number in range(index + 1, end)
                                    if "--skip" in lines[number])
            index = end
            continue
        if mapping and not mapping[2] and not mapping[4]:
            context.append((indentation, mapping[3]))
        index += 1
    return frozenset(verified)
