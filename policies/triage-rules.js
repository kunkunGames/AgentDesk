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
        // Try exact match first, then with ch- prefix
        var agents = agentdesk.db.query(
          "SELECT id FROM agents WHERE id = ? OR id = ?",
          [agentId, "ch-" + agentId]
        );
        if (agents.length > 0) {
          agentId = agents[0].id; // Use the actual agent ID from DB
          agentdesk.db.execute(
            "UPDATE kanban_cards SET assigned_agent_id = ?, updated_at = datetime('now') WHERE id = ?",
            [agentId, card.id]
          );
          agentdesk.log.info("[triage] Auto-assigned card " + card.id + " to " + agentId);
        }
      }

      // If no agent label found, request PMD classification (async)
      if (!agentMatch) {
        // Check if we already requested classification (avoid spam)
        var alreadyRequested = agentdesk.db.query(
          "SELECT value FROM kv_meta WHERE key = ?",
          ["triage_requested:" + card.id]
        );
        if (alreadyRequested.length === 0) {
          agentdesk.db.execute(
            "INSERT OR REPLACE INTO kv_meta (key, value) VALUES (?, datetime('now'))",
            ["triage_requested:" + card.id]
          );
          // Send classification request to PMD via announce bot
          var issueNum = agentdesk.db.query(
            "SELECT github_issue_number, github_issue_url, title FROM kanban_cards WHERE id = ?",
            [card.id]
          );
          if (issueNum.length > 0 && issueNum[0].github_issue_url) {
            var port = agentdesk.config.get("health_port") || 8798;
            try {
              agentdesk.http.post("http://127.0.0.1:" + port + "/api/send", {
                target: "channel:1478652416533463101",
                content: "[ADK → PMD] 에이전트 자동 분류 요청\n\n" +
                  "#" + issueNum[0].github_issue_number + " " + issueNum[0].title + "\n" +
                  issueNum[0].github_issue_url + "\n\n" +
                  "이슈 내용을 분석하여 적절한 에이전트를 배정해주세요.\n" +
                  "POST /api/kanban-cards/" + card.id + "/assign {agent_id: \"...\"} 로 배정 가능합니다.",
                source: "triage-rules"
              });
              agentdesk.log.info("[triage] PMD classification requested for " + card.id);
            } catch(e) {
              agentdesk.log.warn("[triage] PMD request failed: " + e);
            }
          }
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
