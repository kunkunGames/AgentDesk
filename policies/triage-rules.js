var triage = {
  name: "triage-rules",
  priority: 300,

  // Periodic: auto-assign unassigned cards based on labels
  onTick: function() {
    // Find backlog cards without assigned agent
    var unassigned = agentdesk.db.query(
      "SELECT id, metadata, repo_id FROM kanban_cards WHERE status = 'backlog' AND assigned_agent_id IS NULL AND metadata IS NOT NULL"
    );

    for (var i = 0; i < unassigned.length; i++) {
      var card = unassigned[i];
      var metadata = {};
      try { metadata = JSON.parse(card.metadata); } catch(e) { continue; }

      var labels = (metadata.labels || "").toLowerCase();

      // Auto-assign based on agent label in metadata
      var agentMatch = labels.match(/agent:([a-z0-9_-]+)/);
      if (agentMatch) {
        var agentId = agentMatch[1];
        var agents = agentdesk.db.query(
          "SELECT id FROM agents WHERE id = ?",
          [agentId]
        );
        if (agents.length > 0) {
          agentdesk.db.execute(
            "UPDATE kanban_cards SET assigned_agent_id = ?, updated_at = datetime('now') WHERE id = ?",
            [agentId, card.id]
          );
          agentdesk.log.info("[triage] Auto-assigned card " + card.id + " to " + agentId);
        }
      }

      // Auto-set priority based on labels
      if (labels.indexOf("priority:urgent") >= 0 || labels.indexOf("critical") >= 0) {
        agentdesk.db.execute(
          "UPDATE kanban_cards SET priority = 'urgent' WHERE id = ? AND priority = 'medium'",
          [card.id]
        );
      } else if (labels.indexOf("priority:high") >= 0) {
        agentdesk.db.execute(
          "UPDATE kanban_cards SET priority = 'high' WHERE id = ? AND priority = 'medium'",
          [card.id]
        );
      } else if (labels.indexOf("priority:low") >= 0) {
        agentdesk.db.execute(
          "UPDATE kanban_cards SET priority = 'low' WHERE id = ? AND priority = 'medium'",
          [card.id]
        );
      }
    }
  }
};

agentdesk.registerPolicy(triage);
