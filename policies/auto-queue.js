var autoQueue = {
  name: "auto-queue",
  priority: 500,

  // When a card reaches terminal state, check if agent has next queued work
  onCardTerminal: function(payload) {
    var cards = agentdesk.db.query(
      "SELECT assigned_agent_id FROM kanban_cards WHERE id = ?",
      [payload.card_id]
    );
    if (cards.length === 0 || !cards[0].assigned_agent_id) return;

    var agentId = cards[0].assigned_agent_id;

    // Check if agent has any active (non-terminal) cards
    var active = agentdesk.db.query(
      "SELECT COUNT(*) as cnt FROM kanban_cards WHERE assigned_agent_id = ? AND status IN ('requested','in_progress','review')",
      [agentId]
    );
    if (active.length > 0 && active[0].cnt > 0) return;

    // Check dispatch queue for next item
    var queued = agentdesk.db.query(
      "SELECT dq.id as queue_id, dq.kanban_card_id, kc.title FROM dispatch_queue dq JOIN kanban_cards kc ON kc.id = dq.kanban_card_id WHERE kc.assigned_agent_id = ? ORDER BY dq.priority_score DESC, dq.queued_at ASC LIMIT 1",
      [agentId]
    );
    if (queued.length > 0) {
      agentdesk.log.info("[auto-queue] Agent " + agentId + " has next queued card: " + queued[0].kanban_card_id);
      // Remove from queue — the dispatch will be created by the API caller
      agentdesk.db.execute(
        "DELETE FROM dispatch_queue WHERE id = ?",
        [queued[0].queue_id]
      );
    }
  },

  // Periodic: check for idle agents with queued work
  onTick: function() {
    // Find agents that are idle and have queued cards
    var idleAgents = agentdesk.db.query(
      "SELECT DISTINCT a.id FROM agents a " +
      "JOIN dispatch_queue dq ON 1=1 " +
      "JOIN kanban_cards kc ON kc.id = dq.kanban_card_id AND kc.assigned_agent_id = a.id " +
      "WHERE a.status = 'idle' " +
      "AND NOT EXISTS (SELECT 1 FROM kanban_cards kc2 WHERE kc2.assigned_agent_id = a.id AND kc2.status IN ('requested','in_progress'))"
    );

    for (var i = 0; i < idleAgents.length; i++) {
      agentdesk.log.info("[auto-queue] Idle agent " + idleAgents[i].id + " has queued work");
    }
  }
};

agentdesk.registerPolicy(autoQueue);
