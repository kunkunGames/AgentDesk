"""Read-only evidence for DAG learning; elapsed time never grants acceptance."""
from datetime import datetime, timezone
import hashlib
from urllib.parse import quote

MAX_SAMPLES = 600  # 50 hours at a five-minute interval.
MAX_GAP_SECONDS = 660


def timestamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Observation timestamps require an explicit timezone")
    return parsed.astimezone(timezone.utc)


def collect(api, root, campaign_id):
    """Whitelist projections: never retain provider text or authentication data."""
    observed_at = datetime.now(timezone.utc).isoformat()
    try:
        health = api.get("/api/health")
        path = "/api/campaigns/" + quote(campaign_id, safe="")
        campaign = api.get(path)["campaign"]
        execution = api.get(path + "/execution")
        if not execution.get("enrolled"):
            raise ValueError("Campaign is not enrolled")
        with (root / "bin/agentdesk").open("rb") as stream:
            binary = hashlib.file_digest(stream, "sha256").hexdigest()
        declared = health.get("release_source", {}).get("declared_build_provenance", {})
        doctor = health.get("latest_startup_doctor", {})
        learning = execution.get("dag_learning", {})
        occurrences = [{k: item.get(k) for k in (
            "child_id", "scheduled_at", "status", "hold_reason", "sampled",
            "accepted_work_tasks", "published_work_tasks")}
            for item in execution.get("occurrences", [])[:30]]
        return {
            "observed_at": observed_at, "available": True, "binary_sha256": binary,
            "health": {"status": health.get("status"),
                       "fully_recovered": health.get("fully_recovered"),
                       "doctor_failed": doctor.get("failed_count"),
                       "declared_binary_sha256": declared.get("binary_sha256"),
                       "source_manifest_sha256": declared.get("source_manifest_sha256")},
            "campaign": {"id": campaign_id, "status": campaign.get("status"),
                         "revision": campaign.get("revision"),
                         "hold_reason": execution.get("hold_reason"),
                         "elapsed_dispatch_seconds": execution.get("elapsed_dispatch_seconds"),
                         "observed_tool_calls": execution.get("observed_tool_calls"),
                         "parent_budget": execution["goal"]["budget"],
                         "occurrence_budget": execution.get("occurrence_budget")},
            "learning": {k: learning.get(k) for k in (
                "family_id", "min_samples", "sample_count", "last_review_sample_count",
                "new_samples_since_review", "samples_until_review", "review_wait_reason")},
            "active_template": {k: learning["active_template"].get(k) for k in ("id", "version", "status")}
                if learning.get("active_template") else None,
            "reviews": [{k: review.get(k) for k in ("id", "sample_count", "threshold", "status")}
                        for review in learning.get("reviews", [])[:20]],
            "occurrences": occurrences,
        }
    except Exception as error:
        # Error text may contain CLI/provider content. Persist only its type.
        return {"observed_at": observed_at, "available": False,
                "error_type": type(error).__name__, "campaign_id": campaign_id}


def assess(samples):
    if not samples:
        raise ValueError("At least one observation is required")
    times = [timestamp(item["observed_at"]) for item in samples]
    gaps = [(b-a).total_seconds() for a, b in zip(times, times[1:])]
    clock_regression = any(gap <= 0 for gap in gaps)
    binary = next((item.get("binary_sha256") for item in samples if item.get("available")), None)
    available = all(item.get("available") is True for item in samples)
    healthy = available and all(
        item.get("health", {}).get("status") == "healthy"
        and item.get("health", {}).get("fully_recovered") is True
        and item.get("health", {}).get("doctor_failed") == 0
        for item in samples)
    identity = binary is not None and available and all(
        item.get("binary_sha256") == binary
        and item.get("health", {}).get("declared_binary_sha256") == binary
        for item in samples)
    elapsed = max(0, (times[-1]-times[0]).total_seconds())
    gap_count = sum(gap > MAX_GAP_SECONDS for gap in gaps)
    held = [item["observed_at"] for item in samples if item.get("campaign", {}).get("hold_reason")
            or any(child.get("hold_reason") for child in item.get("occurrences", []))]
    return {"sample_count": len(samples), "elapsed_seconds": elapsed,
            "elapsed_24_hours": elapsed >= 86400, "observations_available": available,
            "gap_count": gap_count, "clock_regression": clock_regression,
            "deployment_identity_preserved": identity,
            "continuous_healthy": healthy and identity and not gap_count and not clock_regression,
            "held_observation_count": len(held), "full_v5_acceptance": False}
