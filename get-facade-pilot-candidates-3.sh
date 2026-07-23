# Looking for `agentdesk.db.query("SELECT ... FROM kanban_cards WHERE id = ?", [cardId])` in policies/review-automation.js
grep -n "SELECT status FROM kanban_cards WHERE id = ?" policies/review-automation.js
grep -n "SELECT status FROM kanban_cards WHERE id = ?" policies/__tests__/review-automation.test.js
