var timeouts = {
  name: "timeouts",
  priority: 100,

  onTick: function() {
    // 1. Requested timeout (45 min) → failed
    var staleRequested = agentdesk.db.query(
      "SELECT id, assigned_agent_id FROM kanban_cards WHERE status = 'requested' AND updated_at < datetime('now', '-45 minutes')"
    );
    for (var i = 0; i < staleRequested.length; i++) {
      agentdesk.db.execute(
        "UPDATE kanban_cards SET status = 'failed', updated_at = datetime('now') WHERE id = ?",
        [staleRequested[i].id]
      );
      agentdesk.log.warn("[timeout] Card " + staleRequested[i].id + " requested timeout → failed");
    }

    // 2. In-progress stale (2 hours) → blocked
    var staleInProgress = agentdesk.db.query(
      "SELECT id FROM kanban_cards WHERE status = 'in_progress' AND updated_at < datetime('now', '-2 hours')"
    );
    for (var i = 0; i < staleInProgress.length; i++) {
      agentdesk.db.execute(
        "UPDATE kanban_cards SET status = 'blocked', blocked_reason = 'Stalled: no activity for 2+ hours', updated_at = datetime('now') WHERE id = ?",
        [staleInProgress[i].id]
      );
      agentdesk.log.warn("[timeout] Card " + staleInProgress[i].id + " in_progress stale → blocked");
    }

    // 3. Stale dispatches (24 hours pending) → failed
    var staleDispatches = agentdesk.db.query(
      "SELECT id, kanban_card_id FROM task_dispatches WHERE status = 'pending' AND created_at < datetime('now', '-24 hours')"
    );
    for (var i = 0; i < staleDispatches.length; i++) {
      agentdesk.db.execute(
        "UPDATE task_dispatches SET status = 'failed', updated_at = datetime('now') WHERE id = ?",
        [staleDispatches[i].id]
      );
      if (staleDispatches[i].kanban_card_id) {
        agentdesk.db.execute(
          "UPDATE kanban_cards SET status = 'failed', updated_at = datetime('now') WHERE id = ? AND status NOT IN ('done','cancelled')",
          [staleDispatches[i].kanban_card_id]
        );
      }
      agentdesk.log.warn("[timeout] Dispatch " + staleDispatches[i].id + " stale 24h → failed");
    }

    // 4. Dispatch queue timeout (100 min) → remove from queue
    agentdesk.db.execute(
      "DELETE FROM dispatch_queue WHERE queued_at < datetime('now', '-100 minutes')"
    );
  }
};

agentdesk.registerPolicy(timeouts);
