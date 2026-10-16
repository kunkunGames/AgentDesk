#!/usr/bin/env python3
"""Adversarial operational checks against a real disposable Hub, with no LLM.

The fixture seeds implementation completions. Production code must execute the
checker, reconcile its actual process, and commit completion through the FSM.
Each scenario has an independent database and records observations before its
acceptance assertions, including when the binary fails those assertions.
"""
import argparse
import json
import os
from pathlib import Path
import time

from probe_managed_checks import worker
from probe_test_phase_lifecycle import host, require


def wait_until(predicate, seconds, message):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise RuntimeError(message)


def seed(service, database, card, *, completed=True):
    require(card.isidentifier(), "invalid fixture card identifier")
    database.query(f"INSERT INTO kanban_cards(id,title,status) VALUES('{card}','operating fixture','backlog')")
    status, result = service.request(f"managed-checks/{card}/enroll", {"template": "answer"})
    require(status == 200, f"enrollment failed: {result}")
    if completed:
        database.query(f"INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,result) VALUES('{card}_work','{card}','implementation','completed','fixture implementation')")
    database.query(f"UPDATE kanban_cards SET status='review',review_entered_at=NOW() WHERE id='{card}'")


def answer(template, value):
    path = Path(template["workspace"]) / "answer.txt"
    path.write_text(value)
    os.chown(path, 65534, 65534)


def state(database, card):
    return json.loads(database.query(
        f"SELECT jsonb_build_object('status',status,'state',verification_state,'blocked_reason',blocked_reason) FROM kanban_cards WHERE id='{card}'"
    )[0])


def receipts(database, card):
    return json.loads(database.query(
        f"SELECT COALESCE(jsonb_agg(jsonb_build_object('id',id,'status',status,'head_sha',head_sha,'key',idempotency_key,'evidence',evidence) ORDER BY created_at,id),'[]'::jsonb) FROM test_phase_runs WHERE card_id='{card}'"
    )[0])


def record(artifact, name, value):
    artifact.joinpath(name + ".json").write_text(json.dumps(value, indent=2) + "\n")


def starvation(service, database, template, artifact):
    # These are review entries whose implementation was never completed. They
    # must wait, but cannot occupy every bounded checker sweep indefinitely.
    for number in range(16):
        seed(service, database, f"pending_{number:02}", completed=False)
    seed(service, database, "independent")
    answer(template, "42")
    started = time.monotonic()
    try:
        wait_until(lambda: state(database, "independent")["status"] == "done", 95,
                   "independent card starved behind 16 review cards without completed implementation")
    finally:
        observed = {"independent": state(database, "independent"),
                    "receipts": receipts(database, "independent"),
                    "pending_count": int(database.query("SELECT count(*) FROM kanban_cards WHERE id LIKE 'pending_%' AND status='review'")[0]),
                    "elapsed_seconds": round(time.monotonic() - started, 3)}
        record(artifact, "starvation-observation", observed)
    require(observed["pending_count"] == 16, "unready cards were incorrectly completed")
    require(len(observed["receipts"]) == 1, "independent check was duplicated")
    return ["unready_cards_remain_waiting", "independent_checker_completes_within_three_ticks"], observed, observed["receipts"]


def checker_process(service):
    wrapper = str(service.root / "checker.py").encode()
    for path in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            if wrapper not in path.read_bytes().split(b"\0"):
                continue
            status = path.parent.joinpath("status").read_text()
            if f"PPid:\t{service.process.pid}\n" in status:
                return int(path.parent.name)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    return None


def rejected(service, database, template, artifact):
    for number in range(16):
        seed(service, database, f"rejected_{number:02}")
    forbidden = Path(template["workspace"]) / "outside.txt"
    forbidden.write_text("not permitted by the enrolled contract")
    os.chown(forbidden, 65534, 65534)

    def held_count():
        return int(database.query("SELECT count(*) FROM kanban_cards WHERE id LIKE 'rejected_%' AND verification_state->>'outcome'='unverifiable' AND verification_state->>'candidate_digest' IS NULL")[0])

    wait_until(lambda: held_count() == 16, 45, "out-of-scope candidates were not rejected")
    record(artifact, "rejected-capture", state(database, "rejected_00"))
    # Removing this owned fixture file fixes only the new implementation. Old
    # rejected attempts must remain held, without monopolizing all sweeps.
    forbidden.unlink()
    seed(service, database, "after_rejection")
    answer(template, "42")
    started = time.monotonic()
    try:
        wait_until(lambda: state(database, "after_rejection")["status"] == "done", 95,
                   "independent card starved behind 16 permanently rejected captures")
    finally:
        observed = {"independent": state(database, "after_rejection"),
                    "receipts": receipts(database, "after_rejection"),
                    "rejected_count": held_count(),
                    "elapsed_seconds": round(time.monotonic() - started, 3)}
        record(artifact, "rejected-starvation-observation", observed)
    require(observed["rejected_count"] == 16, "rejected attempts were silently retried")
    require(len(observed["receipts"]) == 1, "independent check was duplicated")
    return ["out_of_scope_captures_remain_rejected", "independent_check_rotates_past_rejected_captures"], observed, observed["receipts"]


def is_live(pid):
    try:
        # A zombie is terminated and cannot keep checking or write effects.
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] not in ("Z", "X")
    except FileNotFoundError:
        return False


def crash(service, database, template, artifact):
    seed(service, database, "crash_card")
    answer(template, "42")
    child = wait_until(lambda: checker_process(service), 45, "no actual checker process started")
    before = receipts(database, "crash_card")
    require(len(before) == 1 and before[0]["status"] == "running", "checker did not reserve a running receipt")
    record(artifact, "before-crash", {"card": state(database, "crash_card"), "receipts": before, "checker_pid": child})
    # Kill only the Hub, so the child's death must come from its parent-death
    # contract. Killing the process group would mask a broken runtime boundary.
    service.process.kill()
    service.process.wait(timeout=5)
    wait_until(lambda: not is_live(child), 5, "checker survived its Hub parent")
    service.stop()
    service.start()
    after_boot = receipts(database, "crash_card")
    require(after_boot == before, "restart prematurely replaced a live collector lease")
    try:
        wait_until(lambda: state(database, "crash_card")["status"] == "done", 125,
                   "crashed collector did not recover after its real lease expiry")
    finally:
        observed = {"card": state(database, "crash_card"), "receipts": receipts(database, "crash_card"),
                    "original_checker_terminated": not is_live(child)}
        record(artifact, "crash-recovery-observation", observed)
    runs = observed["receipts"]
    require(len(runs) == 2, "recovery did not use exactly one bounded retry")
    require(runs[0]["evidence"].get("reason") == "collector_lease_expired", "lost collector was not classified as unknown evidence")
    require(runs[1]["status"] == "passed" and runs[1]["evidence"].get("process_reaped") is True,
            "retry lacks real terminal evidence")
    require(runs[0]["head_sha"] == runs[1]["head_sha"], "restart changed the candidate")
    require(database.query("SELECT count(*) FROM kanban_audit_logs WHERE card_id='crash_card' AND to_status='done'") == ["1"],
            "restart duplicated completion")
    return ["parent_crash_terminates_checker", "restart_respects_unexpired_receipt", "expired_collector_retries_same_candidate",
            "real_retry_reaps_and_commits_once"], observed, runs


def failure(service, database, template, artifact):
    seed(service, database, "failed_card")
    answer(template, "wrong")
    wait_until(lambda: receipts(database, "failed_card") and receipts(database, "failed_card")[0]["status"] == "failed",
               45, "wrong answer did not fail its actual check")
    before = state(database, "failed_card")
    require(before["status"] == "review", "failed check completed the card")
    answer(template, "42")
    time.sleep(32)
    after = state(database, "failed_card")
    runs = receipts(database, "failed_card")
    record(artifact, "failure-observation", {"before": before, "after_edit_and_tick": after, "receipts": runs})
    require(after["status"] == "review" and len(runs) == 1, "live edit replaced a rejected immutable candidate")
    # An explicit new implementation attempt is the authorized correction path.
    database.query("INSERT INTO task_dispatches(id,kanban_card_id,dispatch_type,status,result) VALUES('corrected_work','failed_card','rework','completed','corrected fixture implementation')")
    wait_until(lambda: state(database, "failed_card")["status"] == "done", 45, "corrected attempt did not resume verification")
    final = {"card": state(database, "failed_card"), "receipts": receipts(database, "failed_card")}
    record(artifact, "corrected-attempt-observation", final)
    require(len(final["receipts"]) == 2 and final["receipts"][0]["head_sha"] != final["receipts"][1]["head_sha"],
            "new attempt failed to get its own candidate evidence")
    return ["failed_check_cannot_complete", "live_edit_cannot_replace_failed_candidate", "new_corrected_attempt_completes"], final, final["receipts"]


SCENARIOS = {"starvation": starvation, "rejected": rejected, "crash": crash, "failure": failure}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-container", default="adk24x7-native-build-20260926")
    parser.add_argument("--postgres-container", default="adk24x7-native-test-20260926")
    parser.add_argument("--pg-port", type=int, default=44620)
    parser.add_argument("--binary", default="/target/debug/agentdesk")
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--database", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.worker:
        return host(args, worker_script=Path(__file__).name, worker_args=("--scenario", args.scenario))
    # A real child remains observable long enough to interrupt it; its deadline
    # and collector lease are production values, with no test-only clock jump.
    script = "import json, pathlib, sys, time\ntime.sleep(3)\nprint(json.dumps({'passed': pathlib.Path(sys.argv[1], 'answer.txt').read_text() == '42'}))\n"
    return worker(args, exercise_fn=SCENARIOS[args.scenario], checker_source=script)


if __name__ == "__main__":
    raise SystemExit(main())
