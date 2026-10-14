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
      if (!result || result.applied !== true) return false;
      agentdesk.log.info("[deadlock] Proven dead tmux owner → idle: " + sess.session_key);
      return true;
    };

  timeouts._tmuxHasLivePane = function(tmuxName) {
      try {
        // "=" prefix prevents tmux prefix-matching (exact_target convention)
        var out = agentdesk.exec("tmux", ["list-panes", "-t", "=" + tmuxName, "-F", "#{pane_dead}"]);
        // Success: lines of "0" (alive) or "1" (dead). Any "0" = live pane exists.
        // Failure: "ERROR: ..." (session gone)
        return typeof out === "string" && out.indexOf("ERROR") === -1 && out.indexOf("0") !== -1;
      } catch(e) {
        return false;
      }
    };

  timeouts._section_I = function() {
      // Repair missing/dead sessions; accepted live turns have no silence budget.
      var STALE_SCAN_MINUTES = 30;

      // 먼저: heartbeat가 신선한 working 세션의 카운터를 리셋 (비연속 스톨 누적 방지)
      agentdesk.timeouts.clearDeadlockCountersForFreshSessions(STALE_SCAN_MINUTES);

      // Old heartbeats and missing inflight files are candidates for an owner
      // probe. Only the guarded server facade can settle a proven-dead owner.
      // #219: Increased grace period from 3min to 10min — agents running long tool
      // calls (cargo build, subagents) may not send heartbeats for several minutes.
      var staleWorkingSessions = agentdesk.timeouts.listStaleWorkingSessions(10);
      var recovered = {};
      for (var sw = 0; sw < staleWorkingSessions.length; sw++) {
        var swKey = staleWorkingSessions[sw].session_key;
        var tmuxName = (swKey || "").split(":").pop();
        // #219: Check if tmux session has a live pane (not just session existence).
        // has-session returns true for zombie sessions with dead panes;
        // list-panes #{pane_dead} distinguishes live vs dead workers.
        var inflight;
        try {
          inflight = findRecentInflightForSession(swKey, tmuxName);
        } catch (e) {
          agentdesk.log.warn("[deadlock] Transient error looking up inflight for " + swKey + ": " + e);
          continue; // transient error, retry next time
        }
        var kind = timeouts._runtimeKindForSession(staleWorkingSessions[sw], inflight);
        if (!timeouts._isTmuxRuntime(kind)) continue;
        var tmuxAlive = timeouts._tmuxHasLivePane(tmuxName);
        if (!tmuxAlive || !inflight) {
          // One guarded transaction delegates failure to the canonical owner;
          // a rejected/changed snapshot never performs a separate idle write.
          if (timeouts._recoverStaleSession(staleWorkingSessions[sw], true)) recovered[swKey] = true;
        }
      }

      // 데드락 의심 세션: sessions.last_heartbeat 기반 판별
      // deadlock-manager 자신의 세션은 제외 (자기 자신을 오탐하는 무한 루프 방지)
      var staleSessions = agentdesk.timeouts.listDeadlockCandidates(STALE_SCAN_MINUTES, 50);
      for (var dl = 0; dl < staleSessions.length; dl++) {
        var sess = staleSessions[dl];
        if (recovered[sess.session_key]) continue;
        var deadlockKey = "deadlock_check:" + sess.session_key;
        var dlTmuxName = (sess.session_key || "").split(":").pop();
        var inflight;
        try {
          inflight = findRecentInflightForSession(sess.session_key, dlTmuxName);
        } catch (e) {
          continue;
        }
        agentdesk.kv.delete(deadlockKey);
        var kind = timeouts._runtimeKindForSession(sess, inflight);
        if (!timeouts._isTmuxRuntime(kind)) continue;
        if (timeouts._tmuxHasLivePane(dlTmuxName) && inflight) continue;
        timeouts._recoverStaleSession(sess, false);
      }

      // Clean up deadlock counters for sessions no longer working
      agentdesk.timeouts.cleanupDeadlockCountersForInactiveSessions();

      // Clean up old deadlock history entries (7일 이상)
      var sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;
      agentdesk.timeouts.cleanupDeadlockHistoryBefore(sevenDaysAgo);
    };
};
