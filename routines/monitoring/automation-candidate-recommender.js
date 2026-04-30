// Automation Candidate Recommender
// 매 tick마다 bounded observations를 checkpoint에 누적하고,
// 강한 근거(score >= 80)에서만 agent proposal을 생성한다.
// P0: read-only, no auto-implementation, proposal-only.

const SCORE_THRESHOLD = 80;
const DAILY_CAP = 3;
const COOLDOWN_HOURS = 6;
const MAX_EXAMPLES_PER_CANDIDATE = 3;
const PROMPT_CAP_BYTES = 12288;
const CHECKPOINT_CAP_BYTES = 65536;
const CHECKPOINT_VERSION = 1;
const CANDIDATE_TTL_DAYS = 30;
const KNOWN_CATEGORIES = new Set([
  "routine-candidate",
  "release-freshness",
  "outbox-delivery",
  "memento-hygiene",
  "api-friction",
]);

// --- Scoring weights ---
const WEIGHT_BASE = 10;
const WEIGHT_RECENCY_BONUS = 10;   // occurred in last 30 min
const WEIGHT_FIRST_SEEN = 5;       // new candidate bonus

// --- Checkpoint helpers ---

function emptyCheckpoint() {
  return {
    version: CHECKPOINT_VERSION,
    cursors: {},
    candidates: {},
    suppressions: {},
    recommendations: [],
    last_tick_at: null,
    stats: {
      ticks: 0,
      observations_seen: 0,
      agent_escalations: 0,
      recommendations_today: 0,
      recommendation_day: null,
    },
  };
}

function loadCheckpoint(raw) {
  if (!raw || typeof raw !== "object" || raw.version !== CHECKPOINT_VERSION) {
    return emptyCheckpoint();
  }
  const cp = Object.assign(emptyCheckpoint(), raw);
  cp.candidates = raw.candidates || {};
  cp.suppressions = raw.suppressions || {};
  cp.stats = Object.assign(emptyCheckpoint().stats, raw.stats || {});
  return cp;
}

function nowIso(now) {
  return typeof now === "string" ? now : now.toISOString ? now.toISOString() : String(now);
}

function dateOf(iso) {
  return iso ? iso.slice(0, 10) : null;
}

function addHours(isoStr, hours) {
  const ms = new Date(isoStr).getTime() + hours * 3600 * 1000;
  return new Date(ms).toISOString();
}

function simpleHash(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = ((h << 5) - h + str.charCodeAt(i)) | 0;
  }
  return (h >>> 0).toString(16);
}

function normalizeCategory(value) {
  if (typeof value === "string" && KNOWN_CATEGORIES.has(value)) {
    return value;
  }
  return "routine-candidate";
}

function observationOccurrences(obs) {
  const value = Number(obs.occurrences || obs.count || 1);
  if (!Number.isFinite(value) || value < 1) {
    return 1;
  }
  return Math.min(50, Math.floor(value));
}

// --- Daily cap reset ---

function resetDailyCapIfNeeded(cp, nowStr) {
  const today = dateOf(nowStr);
  if (cp.stats.recommendation_day !== today) {
    cp.stats.recommendations_today = 0;
    cp.stats.recommendation_day = today;
  }
}

// --- Suppression / inventory filter ---

function hasDurableAcceptance(entry) {
  return Boolean(entry && (entry.automation_ref || entry.source_ref));
}

function buildSuppressedSet(cp, inventory) {
  const exact = new Set();
  const prefixes = [];

  function addPattern(patternId) {
    if (!patternId) return;
    if (patternId.endsWith(":*")) {
      prefixes.push(patternId.slice(0, -1));
    } else {
      exact.add(patternId);
    }
  }

  // From checkpoint suppressions
  for (const [patternId, entry] of Object.entries(cp.suppressions || {})) {
    const state = entry.state;
    if (
      (state === "accepted" && hasDurableAcceptance(entry)) ||
      state === "implemented" ||
      state === "suppressed" ||
      state === "rejected"
    ) {
      addPattern(patternId);
    }
  }

  // From automation inventory (exact pattern_id match only)
  for (const item of inventory || []) {
    if (
      item.pattern_id &&
      ((item.status === "accepted" && hasDurableAcceptance(item)) ||
        item.status === "implemented" ||
        item.status === "suppressed" ||
        item.status === "rejected")
    ) {
      addPattern(item.pattern_id);
    }
  }

  // Candidate-level accepted/implemented/suppressed/rejected states
  for (const [patternId, candidate] of Object.entries(cp.candidates || {})) {
    const s = candidate.state;
    if (
      (s === "accepted" && hasDurableAcceptance(candidate)) ||
      s === "implemented" ||
      s === "suppressed" ||
      s === "rejected"
    ) {
      addPattern(patternId);
    }
  }

  return {
    has(patternId) {
      return exact.has(patternId) || prefixes.some((prefix) => patternId.startsWith(prefix));
    },
  };
}

function dropSuppressedCandidates(cp, suppressedSet) {
  for (const patternId of Object.keys(cp.candidates || {})) {
    if (suppressedSet.has(patternId)) {
      delete cp.candidates[patternId];
    }
  }

  if (Array.isArray(cp.recommendations)) {
    cp.recommendations = cp.recommendations.filter((item) => !suppressedSet.has(item.pattern_id));
  }
}

// --- Expired suppression cleanup ---

function pruneExpiredSuppressions(cp, nowStr) {
  for (const [patternId, entry] of Object.entries(cp.suppressions || {})) {
    if (entry.expires_at && entry.expires_at < nowStr) {
      delete cp.suppressions[patternId];
      if (cp.candidates[patternId]) {
        cp.candidates[patternId].state = "observing";
      }
    }
  }
}

function expireStaleCandidates(cp, nowStr) {
  const cutoff = new Date(new Date(nowStr).getTime() - CANDIDATE_TTL_DAYS * 24 * 3600 * 1000).toISOString();
  for (const candidate of Object.values(cp.candidates || {})) {
    if (
      candidate.last_seen_at &&
      candidate.last_seen_at < cutoff &&
      (candidate.state === "observing" || candidate.state === "recommended")
    ) {
      candidate.state = "expired";
    }
  }
}

// --- Score observations into candidates ---

function scoreObservations(cp, observations, suppressedSet, nowStr) {
  const thirtyMinAgo = new Date(new Date(nowStr).getTime() - 30 * 60 * 1000).toISOString();

  for (const obs of observations) {
    cp.stats.observations_seen++;

    const patternId = obs.signature;
    if (!patternId) continue;
    if (suppressedSet.has(patternId)) continue;

    const category = normalizeCategory(obs.category);
    let candidate = cp.candidates[patternId];
    if (!candidate) {
      candidate = {
        category,
        state: "observing",
        score: WEIGHT_FIRST_SEEN,
        evidence_count: 0,
        first_seen_at: obs.timestamp || nowStr,
        last_seen_at: null,
        examples: [],
        last_recommended_at: null,
        last_recommendation_hash: null,
        cooldown_until: null,
        automation_ref: null,
        has_error_evidence: false,
      };
      cp.candidates[patternId] = candidate;
    } else {
      candidate.category = normalizeCategory(candidate.category || category);
    }

    // Skip if already resolved
    if (candidate.state === "accepted" && !hasDurableAcceptance(candidate)) {
      candidate.state = "recommended";
    }
    if (
      (candidate.state === "accepted" && hasDurableAcceptance(candidate)) ||
      candidate.state === "implemented" ||
      candidate.state === "suppressed" ||
      candidate.state === "rejected"
    ) {
      continue;
    }

    const occurrences = observationOccurrences(obs);
    candidate.evidence_count += occurrences;
    candidate.last_seen_at = obs.timestamp || nowStr;

    // Score delta
    const weight = typeof obs.weight === "number" ? obs.weight : 1;
    const scoredOccurrences = Math.min(occurrences, 5);
    let delta = WEIGHT_BASE * weight * scoredOccurrences;
    if (weight === 2) {
      candidate.has_error_evidence = true;
    }

    // Recency bonus
    if (obs.timestamp && obs.timestamp >= thirtyMinAgo) {
      delta += WEIGHT_RECENCY_BONUS;
    }

    candidate.score = Math.min(100, candidate.score + delta);

    // Keep up to 3 examples
    if (candidate.examples.length < MAX_EXAMPLES_PER_CANDIDATE) {
      candidate.examples.push({
        summary: obs.summary,
        timestamp: obs.timestamp,
        evidence_ref: obs.evidence_ref,
        weight,
        occurrences,
      });
    }
  }
}

// --- Find best escalation candidate ---

function findEscalationCandidate(cp, nowStr) {
  if (cp.stats.recommendations_today >= DAILY_CAP) {
    return null;
  }

  let best = null;
  let bestScore = SCORE_THRESHOLD - 1;

  for (const [patternId, candidate] of Object.entries(cp.candidates || {})) {
    if (candidate.state !== "observing" && candidate.state !== "recommended") {
      continue;
    }
    if (candidate.score <= bestScore) {
      continue;
    }
    if (candidate.evidence_count < 5) {
      continue;
    }
    if (candidate.cooldown_until && candidate.cooldown_until > nowStr) {
      continue;
    }

    // Dedupe: same hash within cooldown
    const hash = simpleHash(patternId + ":" + candidate.evidence_count);
    if (candidate.last_recommendation_hash === hash) {
      continue;
    }

    bestScore = candidate.score;
    best = { patternId, candidate, hash };
  }

  return best;
}

// --- Mark candidate as recommended ---

function markRecommended(cp, escalation, nowStr) {
  const { patternId, candidate, hash } = escalation;
  const assessment = candidateAssessment(patternId, candidate);
  candidate.state = "recommended";
  candidate.last_recommended_at = nowStr;
  candidate.last_recommendation_hash = hash;
  candidate.cooldown_until = addHours(nowStr, COOLDOWN_HOURS);
  candidate.suggested_automation = assessment.suggestedAutomation;
  candidate.recommended_execution = assessment.recommendedExecution;
  candidate.before_after = assessment.beforeAfter;
  candidate.expected_files = assessment.expectedFiles;
  candidate.expected_side_effects = assessment.expectedSideEffects;
  candidate.verification_method = assessment.verificationMethod;
  candidate.gated_handoff = assessment.gatedHandoff;
  cp.stats.recommendations_today++;
  cp.stats.agent_escalations++;
  cp.recommendations.push({
    pattern_id: patternId,
    recommended_at: nowStr,
    hash,
    score: candidate.score,
    evidence_count: candidate.evidence_count,
  });
  // Keep recommendations list bounded
  if (cp.recommendations.length > 50) {
    cp.recommendations = cp.recommendations.slice(-50);
  }
}

// --- Agent prompt builder ---

function candidateAssessment(patternId, candidate) {
  const isErrorPattern = Boolean(candidate.has_error_evidence) ||
    (candidate.examples || []).some((example) => example.weight === 2);
  const category = normalizeCategory(candidate.category);
  const categoryProfiles = {
    "routine-candidate": {
      suggestedAutomation: isErrorPattern
        ? "Automatic retry or alert when this routine fails repeatedly"
        : "Scheduled routine to handle this pattern automatically",
      before: "Recurring routine evidence is only visible after manual log review.",
      after: "A bounded routine/rule can handle the repeated pattern or escalate it with cooldowns.",
      files: ["routines/monitoring/*.js", "src/services/routines/*"],
      sideEffects: "May add a routine or rule path; verify cooldown/dedup logic and Discord noise.",
      verification: "Run targeted routine loader tests and inspect checkpoint candidate fields.",
    },
    "release-freshness": {
      suggestedAutomation: "Release freshness monitor for stale deploy, version, or generated inventory signals",
      before: "Release drift is discovered manually after stale versions or generated docs are noticed.",
      after: "A freshness check proposes an update path before stale release state lingers.",
      files: ["scripts/*release*", "src/cli/*", "docs/generated/worker-inventory.md"],
      sideEffects: "May add read-only freshness checks; avoid publishing, tagging, or deploying automatically.",
      verification: "Run script checks plus the freshness fixture that proves no release side effects occur.",
    },
    "outbox-delivery": {
      suggestedAutomation: "Message outbox delivery monitor for repeated send or enqueue failures",
      before: "Delivery failures require manual DB/log inspection to spot repeat patterns.",
      after: "Repeated outbox failures are grouped into a bounded proposal with a clear delivery fix path.",
      files: ["src/services/message_outbox.rs", "src/services/routines/discord_log.rs", "src/services/discord/*"],
      sideEffects: "May change notification retry or fallback behavior; verify dedupe and delivery targets.",
      verification: "Run outbox/routine targeted tests and inspect a failed-delivery fixture.",
    },
    "memento-hygiene": {
      suggestedAutomation: "Memento hygiene digest monitor for repeated memory quality or routing issues",
      before: "Memory hygiene issues are scattered across raw notes and hard to act on safely.",
      after: "Only topic/count/latest-example digests are converted into a bounded proposal.",
      files: ["src/services/memory/*", "src/services/routines/store.rs", "routines/monitoring/*.js"],
      sideEffects: "Must not read or write raw memory bodies from this routine; verify digest truncation.",
      verification: "Run recommender digest fixtures and confirm the prompt contains no raw memory body.",
    },
    "api-friction": {
      suggestedAutomation: "API friction monitor for repeated docs or endpoint workflow breakdowns",
      before: "API friction repeats in agent replies without a consolidated remediation proposal.",
      after: "Repeated friction markers are grouped by fingerprint with docs and verification guidance.",
      files: ["src/services/api_friction.rs", "src/server/routes/*", "docs/*"],
      sideEffects: "May update docs or API routing; verify no DB-direct workaround is introduced.",
      verification: "Run API friction parsing tests and targeted routine recommender fixtures.",
    },
  };
  const profile = categoryProfiles[category] || categoryProfiles["routine-candidate"];
  const recommendedExecution = category === "routine-candidate" && candidate.score < 90
    ? "rule"
    : "agent";
  const title = patternId.replace(/\s+/g, " ").slice(0, 96);
  return {
    suggestedAutomation: profile.suggestedAutomation,
    recommendedExecution,
    beforeAfter: {
      before: profile.before,
      after: profile.after,
    },
    expectedFiles: profile.files,
    expectedSideEffects: profile.sideEffects,
    verificationMethod: profile.verification,
    gatedHandoff: {
      status: "requires_human_approval",
      kanban_card_draft: {
        title: `[automation-candidate] ${title}`,
        category,
        acceptance: [
          "Implementation is proposal-approved before any branch/card mutation",
          "Routine remains bounded and idempotent",
          "Verification command or fixture is recorded in the PR/card",
        ],
      },
      pr_draft: {
        title: `Implement automation candidate: ${title}`,
        body_hint: "Include Before/After, expected files, side effects, and verification evidence.",
      },
      side_effects: "none until a human explicitly approves the gated handoff",
    },
  };
}

function buildPrompt(escalation) {
  const { patternId, candidate } = escalation;
  const evidenceLines = (candidate.examples || [])
    .map((ex, i) => `${i + 1}. [${ex.timestamp || "?"}] ${ex.summary || ""} (occurrences=${ex.occurrences || 1})`)
    .join("\n");

  const {
    suggestedAutomation,
    recommendedExecution,
    beforeAfter,
    expectedFiles,
    expectedSideEffects,
    verificationMethod,
    gatedHandoff,
  } = candidateAssessment(patternId, candidate);
  const handoffAcceptance = (gatedHandoff.kanban_card_draft.acceptance || [])
    .map((item) => `- ${item}`)
    .join("\n");

  const raw = `# Automation Candidate Recommendation

Pattern: ${patternId}
Category: ${normalizeCategory(candidate.category)}
Score: ${candidate.score}/100
Evidence: ${candidate.evidence_count} occurrences (first: ${candidate.first_seen_at || "?"}, last: ${candidate.last_seen_at || "?"})

## Evidence Examples
${evidenceLines || "(none recorded)"}

## Before / After
- Before: ${beforeAfter.before}
- After: ${beforeAfter.after}

## Expected Implementation Files
${expectedFiles.map((file) => `- ${file}`).join("\n")}

## Assessment
- Suggested automation: ${suggestedAutomation}
- Recommended execution: ${recommendedExecution} (rule-based vs agent-driven)
- Potential side effects: ${expectedSideEffects}

## Verification Method
${verificationMethod}

## Gated Handoff Draft
- Status: ${gatedHandoff.status}
- Kanban title: ${gatedHandoff.kanban_card_draft.title}
- PR title: ${gatedHandoff.pr_draft.title}
- Handoff side effects: ${gatedHandoff.side_effects}
${handoffAcceptance}

## Instructions
Evaluate whether this automation is worth building. Provide:
1. Whether to automate (yes / no / defer), and why
2. If yes: proposed implementation approach and affected files/routines
3. Estimated side effects and how to verify the automation is working

DO NOT implement, modify files, restart services, write to memento, or create PRs/cards/issues.
This is a proposal-only request.`;

  if (raw.length <= PROMPT_CAP_BYTES) {
    return raw;
  }

  // Trim examples to fit cap
  const header = raw.split("## Evidence Examples")[0];
  const footer = "\n\n## Instructions\n" + raw.split("## Instructions\n")[1];
  const budget = PROMPT_CAP_BYTES - header.length - footer.length - 20;
  const trimmedEvidence = evidenceLines.slice(0, Math.max(0, budget));
  return header + "## Evidence Examples\n" + trimmedEvidence + footer;
}

// --- Checkpoint size guard ---

function guardCheckpointSize(cp) {
  const json = JSON.stringify(cp);
  if (json.length <= CHECKPOINT_CAP_BYTES) return cp;

  // Prune least-recently-observed candidates first.
  const entries = Object.entries(cp.candidates).sort(
    ([, a], [, b]) => String(a.last_seen_at || "").localeCompare(String(b.last_seen_at || ""))
  );
  let pruned = 0;
  for (const [patternId] of entries) {
    if (JSON.stringify(cp).length <= CHECKPOINT_CAP_BYTES) break;
    if (
      cp.candidates[patternId].state === "observing"
    ) {
      delete cp.candidates[patternId];
      pruned++;
    }
  }

  // Trim examples on remaining candidates
  for (const candidate of Object.values(cp.candidates)) {
    if (candidate.examples && candidate.examples.length > 1) {
      candidate.examples = candidate.examples.slice(-1);
    }
  }

  return cp;
}

// --- Main tick ---

agentdesk.routines.register({
  name: "Automation Candidate Recommender",

  tick(ctx) {
    const nowStr = nowIso(ctx.now);
    const cp = loadCheckpoint(ctx.checkpoint);
    const observations = ctx.observations || [];
    const inventory = ctx.automationInventory || [];

    resetDailyCapIfNeeded(cp, nowStr);
    pruneExpiredSuppressions(cp, nowStr);
    expireStaleCandidates(cp, nowStr);

    const suppressedSet = buildSuppressedSet(cp, inventory);
    dropSuppressedCandidates(cp, suppressedSet);
    scoreObservations(cp, observations, suppressedSet, nowStr);

    cp.stats.ticks++;
    cp.last_tick_at = nowStr;

    const escalation = findEscalationCandidate(cp, nowStr);

    if (!escalation) {
      const activeCandidates = Object.values(cp.candidates).filter(
        (c) => c.state === "observing" || c.state === "recommended"
      ).length;
      const summary = `observed=${observations.length}, candidates=${activeCandidates}, recommendations=${cp.stats.recommendations_today}`;
      return {
        action: "complete",
        checkpoint: guardCheckpointSize(cp),
        lastResult: summary,
      };
    }

    const prompt = buildPrompt(escalation);
    markRecommended(cp, escalation, nowStr);

    return {
      action: "agent",
      prompt,
      checkpoint: guardCheckpointSize(cp),
    };
  },
});
