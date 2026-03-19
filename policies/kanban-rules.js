var rules = {
  name: "kanban-rules",
  priority: 10,

  onSessionStatusChange: function(payload) {
    if (payload.status === "working" && payload.dispatch_id) {
      var cards = agentdesk.db.query(
        "SELECT id, status FROM kanban_cards WHERE latest_dispatch_id = ?",
        [payload.dispatch_id]
      );
      if (cards.length > 0 && cards[0].status === "requested") {
        agentdesk.db.execute(
          "UPDATE kanban_cards SET status = 'in_progress', updated_at = datetime('now') WHERE id = ?",
          [cards[0].id]
        );
        agentdesk.log.info("[kanban] " + cards[0].id + " requested → in_progress");
      }
    }

    if (payload.status === "idle" && payload.dispatch_id) {
      var cards = agentdesk.db.query(
        "SELECT id, status FROM kanban_cards WHERE latest_dispatch_id = ?",
        [payload.dispatch_id]
      );
      if (cards.length > 0 && cards[0].status === "in_progress") {
        agentdesk.db.execute(
          "UPDATE kanban_cards SET status = 'review', updated_at = datetime('now') WHERE id = ?",
          [cards[0].id]
        );
        agentdesk.log.info("[kanban] " + cards[0].id + " in_progress → review");
      }
    }
  },

  onCardTerminal: function(payload) {
    agentdesk.log.info("[kanban] card " + payload.card_id + " reached terminal state: " + payload.status);
  },

  onTick: function() {
    // Placeholder for timeout checks
  }
};

agentdesk.registerPolicy(rules);
