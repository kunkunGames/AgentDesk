// Automation Candidate Detector
// Reads candidate_review:* observations (written by recommender escalation handler)
// and applies a quality gate. Passing candidates are forwarded to the agent for
// approval kv_meta write. Already approved/dispatched candidates are skipped.

const EVIDENCE_AGE_MAX_MS = 48 * 3600 * 1000;   // 48h — matches candidate_review TTL
const MIN_SCORE_THRESHOLD = 80;
const CHECKPOINT_VERSION = 1;
const SEEN_CANDIDATE_TTL_MS = 72 * 3600 * 1000;   // matches candidate_approved TTL
const EMITTED_RETRY_MS = 60 * 60 * 1000;          // retry if no durable approval marker appears
const MAX_EMIT_RETRIES = 5;                        // give up and mark stalled after this many no-shows

// --- Checkpoint helpers ---

function emptyCheckpoint() {
  return {
    version: CHECKPOINT_VERSION,
    seen_candidates: {},   // signature -> { first_seen_at, last_emitted_at, status }
    stats: {
      ticks: 0,
      approved_emitted: 0,
      skipped_already_approved: 0,
      skipped_quality_gate: 0,
      stalled_candidates: 0,
    },
  };
}

function loadCheckpoint(raw) {
  if (!raw || typeof raw !== "object" || raw.version !== CHECKPOINT_VERSION) {
    return emptyCheckpoint();
  }
  const cp = Object.assign(emptyCheckpoint(), raw);
  cp.seen_candidates = raw.seen_candidates || {};
  cp.stats = Object.assign(emptyCheckpoint().stats, raw.stats || {});
  return cp;
}

function nowIso(now) {
  return typeof now === "string" ? now : now.toISOString ? now.toISOString() : String(now);
}

function isRecentIso(value, nowStr, maxAgeMs) {
  const timestamp = new Date(value || "").getTime();
  if (!Number.isFinite(timestamp)) return false;
  return new Date(nowStr).getTime() - timestamp < maxAgeMs;
}

function observationKey(obs) {
  if (typeof obs.key === "string") return obs.key;
  if (typeof obs.evidence_ref === "string") {
    if (obs.evidence_ref.startsWith("kv_meta:routine_observation:")) {
      return obs.evidence_ref.slice("kv_meta:".length);
    }
    if (obs.evidence_ref.startsWith("routine_observation:")) {
      return obs.evidence_ref;
    }
  }
  return "";
}

function observationSignature(obs, prefix) {
  const key = observationKey(obs);
  return key.startsWith(prefix) ? key.slice(prefix.length) : null;
}

function observationPayload(obs) {
  return obs.value && typeof obs.value === "object" ? obs.value : obs;
}

// --- Prune expired seen_candidates ---

function pruneSeen(cp, nowStr) {
  const cutoff = new Date(nowStr).getTime() - SEEN_CANDIDATE_TTL_MS;
  for (const [sig, entry] of Object.entries(cp.seen_candidates)) {
    if (new Date(entry.first_seen_at).getTime() < cutoff) {
      delete cp.seen_candidates[sig];
    }
  }
}

// --- Quality gate ---

function passesQualityGate(candidate, nowStr) {
  const score = typeof candidate.score === "number" ? candidate.score : 0;
  if (score < MIN_SCORE_THRESHOLD) return { pass: false, reason: `score=${score} < ${MIN_SCORE_THRESHOLD}` };

  if (candidate.evidence_age_ms != null) {
    if (candidate.evidence_age_ms > EVIDENCE_AGE_MAX_MS) {
      return { pass: false, reason: `evidence_age=${candidate.evidence_age_ms}ms > ${EVIDENCE_AGE_MAX_MS}ms` };
    }
  } else if (candidate.last_seen_at) {
    const seenAtMs = new Date(candidate.last_seen_at).getTime();
    if (!Number.isFinite(seenAtMs)) {
      return { pass: false, reason: `invalid_last_seen_at=${candidate.last_seen_at}` };
    }
    const ageMs = new Date(nowStr).getTime() - seenAtMs;
    if (ageMs > EVIDENCE_AGE_MAX_MS) {
      return { pass: false, reason: `evidence_age=${ageMs}ms > ${EVIDENCE_AGE_MAX_MS}ms` };
    }
  }

  return { pass: true, reason: null };
}

// --- Direct approval write (Direction 1) ---
// Quality gate passing IS the approval: write candidate_approved directly so
// the executor can proceed without trusting the LLM to write the marker.

function writeApprovalKv(signature, candidate, nowStr) {
  const key = `routine_observation:candidate_approved:${signature}`;
  const value = JSON.stringify({
    signature,
    score: candidate.score || 0,
    approved_at: nowStr,
    category: candidate.category || "routine-candidate",
    suggested_automation: candidate.suggested_automation || "",
    outcome_summary: candidate.outcome_summary || "",
  });
  try {
    agentdesk.kv.set(key, value, 259200);  // 72h TTL
    return true;
  } catch (_e) {
    return false;
  }
}

// --- Main tick ---

agentdesk.routines.register({
  name: "Automation Candidate Detector",

  tick(ctx) {
    const nowStr = nowIso(ctx.now);
    const cp = loadCheckpoint(ctx.checkpoint);
    const observations = ctx.observations || [];

    pruneSeen(cp, nowStr);

    // Find candidate_review observations
    const reviewPrefix = "routine_observation:candidate_review:";
    const approvedPrefix = "routine_observation:candidate_approved:";
    const dispatchedPrefix = "routine_observation:candidate_dispatched:";
    const reviewObs = observations.filter((obs) => observationSignature(obs, reviewPrefix));

    // Find already-approved and dispatched signatures from observations
    const approvedSigs = new Set(
      observations
        .map((obs) => observationSignature(obs, approvedPrefix))
        .filter(Boolean)
    );
    const dispatchedSigs = new Set(
      observations
        .map((obs) => observationSignature(obs, dispatchedPrefix))
        .filter(Boolean)
    );

    cp.stats.ticks++;

    if (reviewObs.length === 0) {
      return {
        action: "complete",
        result: {
          status: "ok",
          summary: "검토할 후보 없음",
          review_count: 0,
        },
        checkpoint: cp,
      };
    }

    // Process each candidate_review observation
    const emitPrompts = [];
    const queuedSignatures = new Set();
    for (const obs of reviewObs) {
      const signature = observationSignature(obs, reviewPrefix);
      const candidate = observationPayload(obs);

      // Skip already approved/dispatched
      if (approvedSigs.has(signature) || dispatchedSigs.has(signature)) {
        cp.stats.skipped_already_approved++;
        cp.seen_candidates[signature] = cp.seen_candidates[signature] || {
          first_seen_at: nowStr,
          status: "skipped_already_handled",
        };
        continue;
      }

      // Skip only recent emits. If no durable approved/dispatched marker appears,
      // retry before the candidate_review marker can age out.
      const seen = cp.seen_candidates[signature];
      if (queuedSignatures.has(signature)) {
        cp.stats.skipped_already_approved++;
        continue;
      }
      // Give up if LLM has repeatedly failed to write the approved marker.
      if (seen && (seen.emit_count || 0) >= MAX_EMIT_RETRIES) {
        if (seen.status !== "stalled") {
          cp.seen_candidates[signature] = Object.assign({}, seen, { status: "stalled" });
        }
        cp.stats.stalled_candidates++;
        continue;
      }
      if (
        seen &&
        seen.status === "emitted" &&
        isRecentIso(seen.last_emitted_at || seen.first_seen_at, nowStr, EMITTED_RETRY_MS)
      ) {
        cp.stats.skipped_already_approved++;
        continue;
      }

      // Quality gate
      const gate = passesQualityGate(candidate, nowStr);
      if (!gate.pass) {
        cp.stats.skipped_quality_gate++;
        cp.seen_candidates[signature] = { first_seen_at: nowStr, status: "rejected", reason: gate.reason };
        continue;
      }

      emitPrompts.push({ signature, candidate });
      queuedSignatures.add(signature);
    }

    if (emitPrompts.length === 0) {
      return {
        action: "complete",
        result: {
          status: "ok",
          summary: `검토 ${reviewObs.length}건 처리됨 (승인 요청 없음)`,
          review_count: reviewObs.length,
          skipped_approved: cp.stats.skipped_already_approved,
          skipped_quality_gate: cp.stats.skipped_quality_gate,
        },
        checkpoint: cp,
      };
    }

    // Direction 1: write candidate_approved directly for the first passing candidate.
    // Quality gate (score >= 80, evidence age < 48h) is the approval mechanism.
    // Remaining candidates handled on next ticks.
    const { signature, candidate } = emitPrompts[0];
    writeApprovalKv(signature, candidate, nowStr);
    const previousSeen = cp.seen_candidates[signature];
    cp.seen_candidates[signature] = {
      first_seen_at: previousSeen?.first_seen_at || nowStr,
      last_emitted_at: nowStr,
      emit_count: (previousSeen?.emit_count || 0) + 1,
      status: "approved",
    };
    cp.stats.approved_emitted++;

    return {
      action: "complete",
      result: {
        status: "ok",
        summary: `후보 ${signature} 직접 승인 완료 (score=${candidate.score || 0})`,
        approved_signature: signature,
        approved_count: 1,
        remaining_review_count: emitPrompts.length - 1,
      },
      checkpoint: cp,
    };
  },
});
