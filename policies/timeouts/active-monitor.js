/* giant-file-exemption: reason=monitor-section-needs-further-split ticket=#1078 */
module.exports = function attachActiveMonitor(timeouts, helpers) {
  var findRecentInflightForSession = helpers.findRecentInflightForSession;

  // A stale heartbeat and an absent tmux pane say nothing about a native
  // child. Missing or conflicting backend/turn evidence preserves ownership.
  timeouts._runtimeKindForSession = function(sess, inflight) {
      var evidence = sess.runtime_evidence || {};
      var kind = evidence.runtime_kind || null;
      if (inflight) {
        if (inflight.session_key !== sess.session_key) return null;
        if ((inflight.dispatch_id || null) !== (sess.active_dispatch_id || null)) return null;
        if (inflight.runtime_kind) {
          if (kind && kind !== inflight.runtime_kind) return null;
          kind = inflight.runtime_kind;
        }
      }
      return kind;
    };

  timeouts._isTmuxRuntime = function(kind) {
      return kind === "legacy_tmux_wrapper" || kind === "claude_tui" || kind === "codex_tui";
    };

  timeouts._recoverStaleSession = function(sess, clearDispatch) {
      var result;
      try {
        result = agentdesk.timeouts.recoverStaleSession({
          snapshot: sess, clear_active_dispatch_id: clearDispatch
        });
      } catch (e) {
        agentdesk.log.warn("[deadlock] Stale owner recovery preserved " + sess.session_key + ": " + e);
        return false;
      }
      if (!result || result.applied !== true) {
        agentdesk.log.warn("[deadlock] Repair deferred (" + ((result && result.reason) || "empty_result") + "): " + sess.session_key);
        return false;
      }
      agentdesk.log.info("[deadlock] Proven dead tmux owner → idle: " + sess.session_key);
      return true;
    };

  // Full-key host observation: live/dead only for a legacy tmux row whose probe answered.
  timeouts._sessionHost = function(sessionKey) {
      if (!sessionKey || !String(sessionKey).trim()) return { state: "unknown", reason: "session_missing" };
      try {
        var observed = agentdesk.timeouts.observeSessionHost(sessionKey);
        if (observed && (observed.state === "live" || observed.state === "dead")) return observed;
        return { state: "unknown", reason: (observed && observed.reason) || "unknown" };
      } catch(e) {
        return { state: "unknown", reason: "error: " + e };
      }
    };

  timeouts._section_I = function() {
      // Repair missing/dead sessions; accepted live turns have no silence budget.
      // A Herdr, unresolved or conflicting host defers the whole row, inflight or not.
      var STALE_SCAN_MINUTES = 30;
      var hosts = new Map();
      function sessionHost(key) {
        if (!hosts.has(key)) {
          var host = timeouts._sessionHost(key);
          hosts.set(key, host);
          if (host.state === "unknown") agentdesk.log.warn("[deadlock] Session host unknown (" + host.reason + "); deferring " + key);
        }
        return hosts.get(key);
      }
      // 먼저: heartbeat가 신선한 working 세션의 카운터를 리셋 (비연속 스톨 누적 방지)
      agentdesk.timeouts.clearDeadlockCountersForFreshSessions(STALE_SCAN_MINUTES);

      // Old heartbeats and missing inflight files are candidates for an owner
      // probe. Only the guarded server facade can settle a proven-dead owner.
      // #219: Increased grace period from 3min to 10min — agents running long tool
      // calls (cargo build, subagents) may not send heartbeats for several minutes.
      var staleWorkingSessions = agentdesk.timeouts.listStaleWorkingSessions(10);
      var recovered = new Set();
      for (var sw = 0; sw < staleWorkingSessions.length; sw++) {
        var swRow = staleWorkingSessions[sw];
        var swKey = swRow.session_key;
        // A live-pane probe, not session existence: has-session is true for zombie panes.
        var swHost = sessionHost(swKey);
        if (swHost.state === "unknown") continue;
        var inflight;
        try {
          inflight = findRecentInflightForSession(swKey, swHost.tmux_name);
        } catch (e) {
          agentdesk.log.warn("[deadlock] Transient error looking up inflight for " + swKey + ": " + e);
          continue; // transient error, retry next time
        }
        var kind = timeouts._runtimeKindForSession(staleWorkingSessions[sw], inflight);
        if (!timeouts._isTmuxRuntime(kind)) continue;
        if (swHost.state === "dead") {
          // One guarded transaction delegates failure to the canonical owner;
          // a rejected/changed snapshot never performs a separate idle write.
          if (timeouts._recoverStaleSession(staleWorkingSessions[sw], true)) {
            recovered.add(swKey);
            agentdesk.kv.delete("deadlock_check:" + swKey);
          }
        }
      }

      // 데드락 의심 세션: sessions.last_heartbeat 기반 판별
      // deadlock-manager 자신의 세션은 제외 (자기 자신을 오탐하는 무한 루프 방지)
      var staleSessions = agentdesk.timeouts.listDeadlockCandidates(STALE_SCAN_MINUTES, 50);
      for (var dl = 0; dl < staleSessions.length; dl++) {
        var sess = staleSessions[dl];
        if (recovered.has(sess.session_key)) continue;
        var deadlockKey = "deadlock_check:" + sess.session_key;
        var dlHost = sessionHost(sess.session_key);
        if (dlHost.state === "unknown") continue;
        var inflight;
        try {
          inflight = findRecentInflightForSession(sess.session_key, dlHost.tmux_name);
        } catch (e) {
          continue;
        }
        agentdesk.kv.delete(deadlockKey);
        var kind = timeouts._runtimeKindForSession(sess, inflight);
        if (!timeouts._isTmuxRuntime(kind)) continue;
        if (dlHost.state !== "dead") continue;
        timeouts._recoverStaleSession(sess, false);
      }

      // Clean up deadlock counters for sessions no longer working
      agentdesk.timeouts.cleanupDeadlockCountersForInactiveSessions();

      // Clean up old deadlock history entries (7일 이상)
      var sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;
      agentdesk.timeouts.cleanupDeadlockHistoryBefore(sevenDaysAgo);
    };
};
