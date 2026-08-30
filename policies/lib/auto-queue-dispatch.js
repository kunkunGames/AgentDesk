/** @module policies/lib/auto-queue-dispatch
 *
 * #1078: Extracted from auto-queue.js as part of the policy modularization pass.
 *
 * Dispatch-activation helpers for the auto-queue policy. Covers:
 *   - pipeline-state classification (terminal/dispatchable/free-path)
 *   - activation result interpretation (count/deferred shape)
 *   - the `activateRun(...)` bridge into the Rust auto-queue surface
 *   - the periodic "rotate active run sweep cursor" recovery primitive
 *
 * Depends on the global `agentdesk` surface and the global `autoQueueLog`
 * binding (wired up by auto-queue.js after requiring auto-queue-log.js).
 */

var _autoQueueLogLib = require("./auto-queue-log");
var autoQueueLog = _autoQueueLogLib.autoQueueLog;

function terminalStatesFromConfig(cfg) {
  var terminalStates = [];
  if (cfg && cfg.states) {
    for (var i = 0; i < cfg.states.length; i++) {
      var state = cfg.states[i];
      if (state && state.terminal && state.id) {
        terminalStates.push(state.id);
      }
    }
  }
  if (terminalStates.length === 0) {
    terminalStates.push("done");
  }
  return terminalStates;
}

function activationDispatchCount(result) {
  if (!result) return null;
  if (typeof result.count === "number") return result.count;
  if (typeof result.dispatched_count === "number") return result.dispatched_count;
  if (typeof result.dispatchedCount === "number") return result.dispatchedCount;
  if (typeof result.activated_count === "number") return result.activated_count;
  if (typeof result.activatedCount === "number") return result.activatedCount;
  return null;
}

function activationWasDeferred(result) {
  return result && result.deferred === true;
}

function rotateActiveRunSweepCursors(runIds) {
  if (!runIds || runIds.length === 0) return;
  try {
    var placeholders = runIds.map(function() { return "?"; }).join(",");
    agentdesk.db.execute(
      "UPDATE auto_queue_entries SET updated_at = datetime('now') WHERE status = 'pending' AND run_id IN (" + placeholders + ")",
      runIds
    );
  } catch (e) {
    autoQueueLog("warn", "failed to rotate active run sweep cursors: " + e, {});
  }
}

function _isDispatchableState(state, cfg) {
  if (!cfg || !cfg.transitions) return false;
  var hasGatedOut = false;
  for (var i = 0; i < cfg.transitions.length; i++) {
    var t = cfg.transitions[i];
    if (t.type === "gated") {
      if (t.to === state) return false;
      if (t.from === state) hasGatedOut = true;
    }
  }
  return hasGatedOut;
}

function _dispatchableTargets(cfg) {
  if (!cfg || !cfg.states) return [];
  var targets = [];
  var targetSet = Object.create(null);

  // #255: requested is the canonical preflight anchor when present.
  if (agentdesk.pipeline.hasState("requested", cfg)) {
    targets.push("requested");
    targetSet.requested = true;
  }

  var gatedOut = Object.create(null);
  var gatedIn = Object.create(null);
  if (cfg.transitions) {
    for (var i = 0; i < cfg.transitions.length; i++) {
      var t = cfg.transitions[i];
      if (t.type === "gated") {
        gatedOut[t.from] = true;
        gatedIn[t.to] = true;
      }
    }
  }

  for (var i = 0; i < cfg.states.length; i++) {
    var s = cfg.states[i];
    if (s.terminal) continue;
    var hasGatedOut = Object.prototype.hasOwnProperty.call(gatedOut, s.id);
    var hasGatedIn = Object.prototype.hasOwnProperty.call(gatedIn, s.id);
    if (!(hasGatedOut && !hasGatedIn)) continue;
    if (!Object.prototype.hasOwnProperty.call(targetSet, s.id)) {
      targets.push(s.id);
      targetSet[s.id] = true;
    }
  }
  return targets;
}

function _freePathToDispatchable(from, cfg) {
  var targets = _dispatchableTargets(cfg);
  if (targets.length === 0) return null;
  var targetSet = Object.create(null);
  for (var i = 0; i < targets.length; i++) {
    targetSet[targets[i]] = true;
  }
  if (Object.prototype.hasOwnProperty.call(targetSet, from)) return [];
  if (!cfg || !cfg.transitions) return null;

  var adj = Object.create(null);
  for (var i = 0; i < cfg.transitions.length; i++) {
    var t = cfg.transitions[i];
    if (t.type === "free") {
      if (!adj[t.from]) adj[t.from] = [];
      adj[t.from].push(t.to);
    }
  }

  var queue = [from];
  var queueHead = 0;
  var visited = Object.create(null);
  var parent = Object.create(null);
  visited[from] = true;

  while (queueHead < queue.length) {
    var cur = queue[queueHead++];
    var neighbors = adj[cur];
    if (!neighbors) continue;
    for (var i = 0; i < neighbors.length; i++) {
      var to = neighbors[i];
      if (Object.prototype.hasOwnProperty.call(visited, to)) continue;
      visited[to] = true;
      parent[to] = cur;
      if (Object.prototype.hasOwnProperty.call(targetSet, to)) {
        var reversePath = [to];
        var p = cur;
        while (p !== from) {
          reversePath.push(p);
          p = parent[p];
        }
        reversePath.reverse();
        return reversePath;
      }
      queue.push(to);
    }
  }

  return null;
}

function activateRun(runId, threadGroup, agentId) {
  if (!runId) return null;
  try {
    if (agentId !== null && agentId !== undefined) {
      var body = {
        run_id: runId,
        active_only: true,
        agent_id: agentId
      };
      if (threadGroup !== null && threadGroup !== undefined) {
        body.thread_group = threadGroup;
      }
      return agentdesk.autoQueue.activate(body);
    }
    return agentdesk.autoQueue.activate(runId, threadGroup);
  } catch (e) {
    autoQueueLog("warn", "activate bridge failed for run " + runId + ": " + e, {
      run_id: runId,
      thread_group: threadGroup,
      agent_id: agentId || null
    });
    return null;
  }
}

module.exports = {
  terminalStatesFromConfig: terminalStatesFromConfig,
  activationDispatchCount: activationDispatchCount,
  activationWasDeferred: activationWasDeferred,
  rotateActiveRunSweepCursors: rotateActiveRunSweepCursors,
  isDispatchableState: _isDispatchableState,
  dispatchableTargets: _dispatchableTargets,
  freePathToDispatchable: _freePathToDispatchable,
  activateRun: activateRun
};
