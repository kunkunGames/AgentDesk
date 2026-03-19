var rules = {
  name: "kanban-rules",
  priority: 10,

  // Agent session status → card status mapping
  onSessionStatusChange: function(payload) {
    if (!payload.dispatch_id) return;

    var cards = agentdesk.db.query(
      "SELECT id, status FROM kanban_cards WHERE latest_dispatch_id = ?",
      [payload.dispatch_id]
    );
    if (cards.length === 0) return;
    var card = cards[0];

    if (payload.status === "working" && card.status === "requested") {
      agentdesk.db.execute(
        "UPDATE kanban_cards SET status = 'in_progress', started_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
        [card.id]
      );
      agentdesk.log.info("[kanban] " + card.id + " requested → in_progress");
    }

    if (payload.status === "idle" && card.status === "in_progress") {
      agentdesk.db.execute(
        "UPDATE kanban_cards SET status = 'review', updated_at = datetime('now') WHERE id = ?",
        [card.id]
      );
      agentdesk.log.info("[kanban] " + card.id + " in_progress → review");
    }
  },

  // Dispatch completed → update card based on result
  onDispatchCompleted: function(payload) {
    var dispatches = agentdesk.db.query(
      "SELECT id, kanban_card_id, to_agent_id, dispatch_type, chain_depth FROM task_dispatches WHERE id = ?",
      [payload.dispatch_id]
    );
    if (dispatches.length === 0) return;
    var dispatch = dispatches[0];
    if (!dispatch.kanban_card_id) return;

    var cards = agentdesk.db.query(
      "SELECT id, status, repo_id, assigned_agent_id FROM kanban_cards WHERE id = ?",
      [dispatch.kanban_card_id]
    );
    if (cards.length === 0) return;
    var card = cards[0];

    // Skip if card already terminal
    if (card.status === "done" || card.status === "cancelled") return;

    // Review/decision dispatch completed — handled by review-automation policy
    if (dispatch.dispatch_type === "review" || dispatch.dispatch_type === "review-decision") return;

    // XP reward for completed work
    var xpMap = { "low": 5, "medium": 10, "high": 18, "urgent": 30 };
    var cardPriority = agentdesk.db.query(
      "SELECT priority FROM kanban_cards WHERE id = ?", [card.id]
    );
    var priority = (cardPriority.length > 0) ? cardPriority[0].priority : "medium";
    var xp = xpMap[priority] || 10;
    xp += (dispatch.chain_depth || 0) * 2; // depth bonus

    if (dispatch.to_agent_id) {
      agentdesk.db.execute(
        "UPDATE agents SET xp = xp + ? WHERE id = ?",
        [xp, dispatch.to_agent_id]
      );
      agentdesk.log.info("[kanban] +" + xp + " XP to " + dispatch.to_agent_id);
    }

    // Check if card has deferred DoD
    var dodCards = agentdesk.db.query(
      "SELECT deferred_dod_json FROM kanban_cards WHERE id = ? AND deferred_dod_json IS NOT NULL",
      [card.id]
    );
    if (dodCards.length > 0 && dodCards[0].deferred_dod_json) {
      // Has DoD — transition to review for verification
      agentdesk.db.execute(
        "UPDATE kanban_cards SET status = 'review', updated_at = datetime('now') WHERE id = ?",
        [card.id]
      );
      agentdesk.log.info("[kanban] " + card.id + " → review (has DoD)");
    } else {
      // No DoD — go directly to done
      agentdesk.db.execute(
        "UPDATE kanban_cards SET status = 'done', completed_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
        [card.id]
      );
      agentdesk.log.info("[kanban] " + card.id + " → done (no DoD)");
    }
  },

  // Card status transition
  onCardTransition: function(payload) {
    agentdesk.log.info("[kanban] card " + payload.card_id + ": " + payload.from + " → " + payload.to);

    // Auto-set requested_at timestamp
    if (payload.to === "requested") {
      agentdesk.db.execute(
        "UPDATE kanban_cards SET updated_at = datetime('now') WHERE id = ?",
        [payload.card_id]
      );
    }
  },

  // Terminal state reached
  onCardTerminal: function(payload) {
    agentdesk.log.info("[kanban] card " + payload.card_id + " reached terminal: " + payload.status);

    // Update completed_at if done
    if (payload.status === "done") {
      agentdesk.db.execute(
        "UPDATE kanban_cards SET completed_at = datetime('now') WHERE id = ? AND completed_at IS NULL",
        [payload.card_id]
      );
    }
  }
};

agentdesk.registerPolicy(rules);
