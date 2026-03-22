var pipeline = {
  name: "pipeline",
  priority: 200,

  // Card transition — check if ready cards should enter pipeline
  onCardTransition: function(payload) {
    if (payload.to !== "ready") return;

    // Check if repo has pipeline stages triggered on 'ready'
    var cards = agentdesk.db.query(
      "SELECT repo_id FROM kanban_cards WHERE id = ?",
      [payload.card_id]
    );
    if (cards.length === 0) return;

    var stages = agentdesk.db.query(
      "SELECT id, stage_name, agent_override_id FROM pipeline_stages WHERE repo_id = ? AND trigger_after = 'ready' ORDER BY stage_order ASC LIMIT 1",
      [cards[0].repo_id]
    );
    if (stages.length > 0) {
      agentdesk.db.execute(
        "UPDATE kanban_cards SET pipeline_stage_id = ?, updated_at = datetime('now') WHERE id = ?",
        [stages[0].id, payload.card_id]
      );
      agentdesk.log.info("[pipeline] Card " + payload.card_id + " assigned to stage: " + stages[0].stage_name);
    }
  },

  // Dispatch completed — advance to next pipeline stage
  onDispatchCompleted: function(payload) {
    var dispatches = agentdesk.db.query(
      "SELECT kanban_card_id, dispatch_type FROM task_dispatches WHERE id = ?",
      [payload.dispatch_id]
    );
    if (dispatches.length === 0 || !dispatches[0].kanban_card_id) return;

    var cardId = dispatches[0].kanban_card_id;

    // Get current pipeline stage
    var cards = agentdesk.db.query(
      "SELECT pipeline_stage_id, repo_id FROM kanban_cards WHERE id = ?",
      [cardId]
    );
    if (cards.length === 0 || !cards[0].pipeline_stage_id) return;

    var currentStageId = cards[0].pipeline_stage_id;
    var repoId = cards[0].repo_id;

    // Get current stage order
    var currentStage = agentdesk.db.query(
      "SELECT stage_order FROM pipeline_stages WHERE id = ?",
      [currentStageId]
    );
    if (currentStage.length === 0) return;

    // Find next stage
    var nextStage = agentdesk.db.query(
      "SELECT id, stage_name, agent_override_id FROM pipeline_stages WHERE repo_id = ? AND stage_order > ? ORDER BY stage_order ASC LIMIT 1",
      [repoId, currentStage[0].stage_order]
    );

    if (nextStage.length > 0) {
      agentdesk.db.execute(
        "UPDATE kanban_cards SET pipeline_stage_id = ?, updated_at = datetime('now') WHERE id = ?",
        [nextStage[0].id, cardId]
      );
      agentdesk.log.info("[pipeline] Card " + cardId + " advanced to stage: " + nextStage[0].stage_name);

      // Create dispatch for the next pipeline stage
      var stageAgent = nextStage[0].agent_override_id;
      if (!stageAgent) {
        // Fall back to card's assigned agent
        var cardAgent = agentdesk.db.query("SELECT assigned_agent_id FROM kanban_cards WHERE id = ?", [cardId]);
        stageAgent = (cardAgent.length > 0 && cardAgent[0].assigned_agent_id) ? cardAgent[0].assigned_agent_id : null;
      }
      if (stageAgent) {
        try {
          agentdesk.dispatch.create(
            cardId,
            stageAgent,
            "implementation",
            "[Pipeline: " + nextStage[0].stage_name + "] " + cardId
          );
          agentdesk.log.info("[pipeline] Dispatch created for stage " + nextStage[0].stage_name);
        } catch (e) {
          agentdesk.log.warn("[pipeline] Dispatch failed for stage " + nextStage[0].stage_name + ": " + e);
        }
      } else {
        // No agent — route to PM decision
        agentdesk.kanban.setStatus(cardId, "pending_decision");
        agentdesk.db.execute(
          "UPDATE kanban_cards SET blocked_reason = ? WHERE id = ?",
          ["Pipeline stage '" + nextStage[0].stage_name + "' has no assigned agent", cardId]
        );
      }
    } else {
      // No more stages — clear pipeline stage
      agentdesk.db.execute(
        "UPDATE kanban_cards SET pipeline_stage_id = NULL, updated_at = datetime('now') WHERE id = ?",
        [cardId]
      );
      agentdesk.log.info("[pipeline] Card " + cardId + " completed all pipeline stages");
    }
  }
};

agentdesk.registerPolicy(pipeline);
