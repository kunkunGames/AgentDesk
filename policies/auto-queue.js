var autoQueue = {
  name: "auto-queue",
  priority: 500,

  // When a card reaches terminal state, mark queue entry as done
  // and dispatch next pending entry for the same agent
  onCardTerminal: function(payload) {
    var cards = agentdesk.db.query(
      "SELECT assigned_agent_id FROM kanban_cards WHERE id = ?",
      [payload.card_id]
    );
    if (cards.length === 0 || !cards[0].assigned_agent_id) return;

    var agentId = cards[0].assigned_agent_id;

    // Guard: skip if all queue entries for this card are already done
    // (prevents duplicate dispatch when OnCardTerminal fires multiple times)
    var pendingEntries = agentdesk.db.query(
      "SELECT COUNT(*) as cnt FROM auto_queue_entries WHERE kanban_card_id = ? AND status != 'done'",
      [payload.card_id]
    );
    if (pendingEntries.length > 0 && pendingEntries[0].cnt === 0) return;

    // Mark queue entry as done
    agentdesk.db.execute(
      "UPDATE auto_queue_entries SET status = 'done', completed_at = datetime('now') WHERE kanban_card_id = ? AND status = 'dispatched'",
      [payload.card_id]
    );

    // Check if agent has any active (non-terminal) cards
    var active = agentdesk.db.query(
      "SELECT COUNT(*) as cnt FROM kanban_cards WHERE assigned_agent_id = ? AND status IN ('requested','in_progress','review')",
      [agentId]
    );
    if (active.length > 0 && active[0].cnt > 0) return;

    // Find next pending entry for this agent from active runs
    var nextEntry = agentdesk.db.query(
      "SELECT e.id, e.kanban_card_id, e.run_id, kc.title " +
      "FROM auto_queue_entries e " +
      "JOIN auto_queue_runs r ON e.run_id = r.id " +
      "JOIN kanban_cards kc ON e.kanban_card_id = kc.id " +
      "WHERE e.agent_id = ? AND e.status = 'pending' AND r.status = 'active' " +
      "ORDER BY e.priority_rank ASC LIMIT 1",
      [agentId]
    );

    if (nextEntry.length > 0) {
      var entry = nextEntry[0];
      agentdesk.log.info("[auto-queue] Dispatching next entry for " + agentId + ": " + entry.kanban_card_id);

      // Create dispatch first — only mark entry as dispatched on success
      // dispatch.create sets card status to 'requested' internally
      try {
        agentdesk.dispatch.create(
          entry.kanban_card_id,
          agentId,
          "implementation",
          entry.title
        );

        // Dispatch succeeded — now mark entry
        agentdesk.db.execute(
          "UPDATE auto_queue_entries SET status = 'dispatched', dispatched_at = datetime('now') WHERE id = ?",
          [entry.id]
        );

        // Check if run is complete
        var remaining = agentdesk.db.query(
          "SELECT COUNT(*) as cnt FROM auto_queue_entries WHERE run_id = ? AND status = 'pending'",
          [entry.run_id]
        );
        if (remaining.length > 0 && remaining[0].cnt === 0) {
          agentdesk.db.execute(
            "UPDATE auto_queue_runs SET status = 'completed', completed_at = datetime('now') WHERE id = ?",
            [entry.run_id]
          );
        }
      } catch (e) {
        agentdesk.log.warn("[auto-queue] dispatch failed, entry stays pending for retry: " + e);
      }
    }
  },

  // Periodic: check for idle agents with pending queue entries
  onTick: function() {
    var idleWithQueue = agentdesk.db.query(
      "SELECT DISTINCT e.agent_id " +
      "FROM auto_queue_entries e " +
      "JOIN auto_queue_runs r ON e.run_id = r.id " +
      "WHERE e.status = 'pending' AND r.status = 'active' " +
      "AND NOT EXISTS (" +
      "  SELECT 1 FROM kanban_cards kc " +
      "  WHERE kc.assigned_agent_id = e.agent_id " +
      "  AND kc.status IN ('requested','in_progress')" +
      ")"
    );

    for (var i = 0; i < idleWithQueue.length; i++) {
      agentdesk.log.info("[auto-queue] Idle agent " + idleWithQueue[i].agent_id + " has pending queue entries");
    }
  }
};

agentdesk.registerPolicy(autoQueue);
