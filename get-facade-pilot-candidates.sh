grep -rn "agentdesk.db.query(\"SELECT status FROM kanban_cards WHERE id = ?\"" policies/
grep -rn "agentdesk.db.query(\n *\"SELECT status FROM kanban_cards WHERE id = ?\"" policies/
grep -rn "SELECT status FROM kanban_cards WHERE id = ?" policies/
