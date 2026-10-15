"""Validate v5's local audit receipt and report its exact, partial evidence scope.

Hashes establish consistency with a trusted capture, not authenticity against
someone who can rewrite the capture, receipt and all artifacts together. No
model, install, E1/E4/E5 or elapsed-time acceptance is inferred from these files.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import PurePosixPath
import re


class EvidenceError(ValueError):
    pass


SCENARIO_CHECKS = {
    "starvation": {"unready_cards_remain_waiting", "independent_checker_completes_within_three_ticks"},
    "rejected": {"out_of_scope_captures_remain_rejected", "independent_check_rotates_past_rejected_captures"},
    "crash": {"parent_crash_terminates_checker", "restart_respects_unexpired_receipt",
              "expired_collector_retries_same_candidate", "real_retry_reaps_and_commits_once"},
    "failure": {"failed_check_cannot_complete", "live_edit_cannot_replace_failed_candidate",
                "new_corrected_attempt_completes"},
}
HTTP_CHECKS = {
    "admin_authentication_required", "raw_completion_waits_for_actual_checks",
    "automatic_tick_runs_checker_and_commits_done", "completion_and_audit_recorded_once",
    "public_receipt_forgery_rejected", "frozen_artifact_preserved_with_receipt",
    "human_wait_requires_authenticated_current_candidate_approval",
    "approval_wakes_fsm_and_records_authority", "restart_and_next_tick_do_not_duplicate_completion",
}
HTTP_ARTIFACT = "managed-checks-operating-final/result.json"
CAMPAIGN_CHECKS = {
    "protected_goal_supplies_roots_through_existing_queue",
    "failed_check_schedules_one_rework_while_independent_c_completes",
    "verified_predecessor_bytes_materialized_and_frozen_in_successor_contract",
    "actual_file_consumption_checks_drive_all_three_fsm_completions",
    "restart_and_next_tick_preserve_exactly_once_supply_rework_and_completion",
}
ROOT_INPUTS = {"Cargo.toml", "Cargo.lock", "build.rs", "defaults.json", "rust-toolchain.toml"}
SUMMARY = re.compile(
    r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; "
    r"(\d+) filtered out; finished in ([\d.]+)s$", re.MULTILINE
)


def require(condition, message):
    if not condition:
        raise EvidenceError(message)


def relative_file(root, name):
    require(isinstance(name, str) and bool(name), "missing evidence path")
    path = PurePosixPath(name)
    require(not path.is_absolute() and all(p not in ("", ".", "..") for p in name.split("/"))
            and "\\" not in name and ":" not in name, f"non-relative evidence path: {name}")
    resolved = (root / path).resolve()
    require(resolved.is_relative_to(root.resolve()), f"evidence escapes root: {name}")
    require(resolved.is_file(), f"missing evidence file: {name}")
    return resolved


def sha(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def require_sha(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "invalid SHA-256")
    return value


def read_json(path):
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    require(isinstance(data, dict), f"expected JSON object: {path.name}")
    return data


def source_inputs(root, receipt, capture):
    files = capture["files"]
    require(isinstance(files, list) and bool(files), "empty source capture")
    names = [item["path"] for item in files]
    require(names == sorted(set(names)), "source capture is not unique and sorted")
    require(ROOT_INPUTS.issubset(names), "source capture omits required build inputs")
    require(receipt["base_commit"] == capture["base_commit"], "base commit mismatch")
    require(type(receipt["input_files"]) is int and len(files) == receipt["input_files"], "source count mismatch")
    for item in files:
        require(sha(relative_file(root, item["path"])) == require_sha(item["sha256"]),
                f"source changed: {item['path']}")
    for directory in ("src", "migrations", "policies"):
        current = {p.relative_to(root).as_posix() for p in (root / directory).rglob("*") if p.is_file()}
        require(bool(current) and current.issubset(names), f"source capture omits current {directory} files")
    fingerprint = hashlib.sha256("".join(f"{item['path']}\0{item['sha256']}\n" for item in files).encode()).hexdigest()
    require(fingerprint == require_sha(capture["source_fingerprint"])
            == require_sha(receipt["source_fingerprint"]), "source fingerprint mismatch")
    return fingerprint


def audit(root, receipt_name):
    root = root.resolve()
    receipt_path = relative_file(root, receipt_name)
    artifacts = receipt_path.parent
    receipt = read_json(receipt_path)
    require(receipt.get("receipt_ready") is True, "capture receipt is incomplete")
    # This reader understands partial development evidence only. A caller cannot
    # promote it by flipping booleans or attaching an unverified measurement JSON.
    require(receipt.get("operational_ready") is False, "operational acceptance needs a separate verified journey")
    evidence = receipt["evidence_sha256"]
    require(isinstance(evidence, dict) and bool(evidence), "no evidence hashes")
    for name, expected in evidence.items():
        require(sha(relative_file(artifacts, name)) == require_sha(expected), f"evidence changed: {name}")

    def pinned(name, *, json_object=True):
        require(name in evidence, f"required evidence is not pinned: {name}")
        path = relative_file(artifacts, name)
        return read_json(path) if json_object else path.read_text(encoding="utf-8-sig")

    fingerprint = source_inputs(root, receipt, pinned(receipt["source_capture"]))
    binary = receipt["binary"]
    binary_hash = sha(relative_file(root, binary["path"]))
    require(binary_hash == require_sha(binary["sha256"]), "saved executable differs from receipt")
    for name, recorded in (("operations-current-build-exit.json", binary["build"]),
                           ("operations-current-regression-exit.json", receipt["rust"]["build_and_test"])):
        value = pinned(name)
        require(value == recorded and type(value["exit_code"]) is int and value["exit_code"] == 0,
                f"unsuccessful or inconsistent command: {name}")
    smoke = pinned("operations-final-cli-smoke.json")
    require(smoke == binary["cli_smoke"] and smoke["version_exit"] == smoke["help_exit"] == 0,
            "CLI startup evidence failed")
    rust = receipt["rust"]
    require(require_sha(rust["log_sha256"]) == evidence.get(rust["log"]), "regression log digest mismatch")
    log = pinned(rust["log"], json_object=False)
    matches = SUMMARY.findall(log)
    require(len(matches) == 1 and "test result: FAILED" not in log, "missing or ambiguous successful test result")
    passed, failed, ignored, measured, filtered = map(int, matches[0][:5])
    require(passed > 0 and failed == ignored == measured == 0, "regression did not run cleanly")
    require((passed, failed, ignored, filtered, float(matches[0][5])) ==
            (rust["passed"], rust["failed"], rust["ignored"], rust["filtered_out"], rust["test_seconds"]),
            "regression summary differs from receipt")

    verified = []

    def probe(name, checks, boots):
        data = pinned(name)
        actual = data.get("checks")
        require(data.get("passed") is True and data.get("binary_sha256") == binary_hash,
                f"probe failed or used another executable: {name}")
        require(isinstance(actual, list) and all(isinstance(c, str) for c in actual)
                and len(set(actual)) == len(actual) and checks.issubset(actual), f"missing probe assertions: {name}")
        require(type(data.get("boots")) is int and data["boots"] >= boots, f"restart not observed: {name}")
        require(isinstance(data.get("duration_seconds"), (int, float)) and data["duration_seconds"] > 0,
                f"missing probe duration: {name}")
        cleanup = str(PurePosixPath(name).with_name("cleanup.json"))
        require(pinned(cleanup).get("fixture_database_removed") is True, f"fixture cleanup unproven: {name}")
        verified.append({"artifact": name, "checks": actual, "boots": data["boots"],
                         "duration_seconds": data["duration_seconds"]})

    for scenario, checks in SCENARIO_CHECKS.items():
        probe(receipt["operating_probes"][scenario]["artifact"], checks, 2 if scenario == "crash" else 1)
    probe(HTTP_ARTIFACT, HTTP_CHECKS, 2)
    campaign_verified = "campaign_probe" in receipt
    if campaign_verified:
        probe(receipt["campaign_probe"], CAMPAIGN_CHECKS, 3)
    scope = receipt["scope"]
    for field in ("real_model_calls", "production_deployment", "os_service_or_reboot_acceptance", "observed_24_hours"):
        require(scope.get(field) is False, f"unsupported operational claim in partial receipt: {field}")
    blockers = receipt["blocking_requirements"]
    require(isinstance(blockers, list) and bool(blockers) and all(isinstance(x, str) and x for x in blockers),
            "missing implementation gaps")
    docs_drift = []
    for name, expected in receipt.get("document_sha256", {}).items():
        if sha(relative_file(root / "docs/24x7-native-v5", name)) != require_sha(expected):
            docs_drift.append(name)
    return {
        "schema_version": 1, "audited_at": datetime.now(timezone.utc).isoformat(),
        "evidence_valid": True, "activated": False, "operational_ready": False,
        "receipt": receipt_name, "receipt_sha256": sha(receipt_path),
        "source_fingerprint": fingerprint, "input_files": receipt["input_files"],
        "binary_sha256": binary_hash, "evidence_files_verified": len(evidence),
        "rust": {"passed": passed, "failed": failed, "ignored": ignored, "filtered_out": filtered},
        "verified_probes": verified,
        "confirmed": ["protected_checker_http_and_fsm", "checker_starvation_recovery",
                      "checker_crash_recovery", "new_candidate_rework"] +
                     (["protected_campaign_file_handoff_and_bounded_rework"] if campaign_verified else []),
        "unmet": blockers,
        "journeys": {"E1": ("partial: runtime supply, verified files and protected completion; implementation is a fixture, no real model acceptance"
                            if campaign_verified else "not accepted: no runtime supply, verified file consumption or protected campaign completion"),
                     "E4": "partial: local limits tested; authenticated backend settlement and writer containment missing",
                     "E5": "not executed: coherent bundle install and OS service recovery",
                     "V36": "not executed: no measured 24-hour operation"},
        "historical_document_changes": docs_drift,
        "scope": scope, "v36_counted_from_fixture_or_uptime": False,
    }
