var reviewAutomation = {
  name: "review-automation",
  priority: 50,

  // When a card enters review state, check review configuration
  onReviewEnter: function(payload) {
    var cards = agentdesk.db.query(
      "SELECT id, repo_id, assigned_agent_id, review_round, deferred_dod_json FROM kanban_cards WHERE id = ?",
      [payload.card_id]
    );
    if (cards.length === 0) return;
    var card = cards[0];

    // Check if review is enabled for this repo
    var reviewEnabled = agentdesk.config.get("review_enabled");
    if (reviewEnabled === "false" || reviewEnabled === false) {
      // Review disabled — skip directly to done
      agentdesk.db.execute(
        "UPDATE kanban_cards SET status = 'done', completed_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
        [card.id]
      );
      agentdesk.log.info("[review] Review disabled, card " + card.id + " → done");
      return;
    }

    // Increment review round
    var newRound = (card.review_round || 0) + 1;
    agentdesk.db.execute(
      "UPDATE kanban_cards SET review_round = ?, updated_at = datetime('now') WHERE id = ?",
      [newRound, card.id]
    );
    agentdesk.log.info("[review] Card " + card.id + " entering review round " + newRound);
  },

  // When a review/decision dispatch completes
  onDispatchCompleted: function(payload) {
    var dispatches = agentdesk.db.query(
      "SELECT id, kanban_card_id, dispatch_type, result FROM task_dispatches WHERE id = ?",
      [payload.dispatch_id]
    );
    if (dispatches.length === 0) return;
    var dispatch = dispatches[0];

    // Only handle review-type dispatches
    if (dispatch.dispatch_type !== "review" && dispatch.dispatch_type !== "review-decision") return;
    if (!dispatch.kanban_card_id) return;

    var result = null;
    try { result = JSON.parse(dispatch.result || "{}"); } catch(e) { result = {}; }
    var verdict = result.verdict || result.decision;

    if (!verdict) return;

    var cardId = dispatch.kanban_card_id;

    if (verdict === "pass" || verdict === "accept" || verdict === "approved") {
      // Review passed — check pipeline, otherwise done
      var stages = agentdesk.db.query(
        "SELECT id FROM pipeline_stages WHERE repo_id = (SELECT repo_id FROM kanban_cards WHERE id = ?) AND trigger_after = 'review_pass' LIMIT 1",
        [cardId]
      );
      if (stages.length > 0) {
        agentdesk.log.info("[review] Card " + cardId + " passed review, entering pipeline");
        // Pipeline policy will handle the next stage
      } else {
        agentdesk.db.execute(
          "UPDATE kanban_cards SET status = 'done', completed_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
          [cardId]
        );
        agentdesk.log.info("[review] Card " + cardId + " passed review → done");
      }
    } else if (verdict === "improve" || verdict === "reject" || verdict === "rework") {
      // Needs rework — back to ready for re-dispatch
      agentdesk.db.execute(
        "UPDATE kanban_cards SET status = 'ready', updated_at = datetime('now') WHERE id = ?",
        [cardId]
      );
      // Store review notes
      if (result.notes || result.feedback) {
        agentdesk.db.execute(
          "UPDATE kanban_cards SET review_notes = ? WHERE id = ?",
          [result.notes || result.feedback, cardId]
        );
      }
      agentdesk.log.info("[review] Card " + cardId + " needs rework → ready");
    } else {
      agentdesk.log.warn("[review] Unknown verdict '" + verdict + "' for card " + cardId);
    }
  }
};

agentdesk.registerPolicy(reviewAutomation);
