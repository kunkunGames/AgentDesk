# FacadePilot Learnings

- When writing tests for migrated AgentDesk facades using the `support/harness` `loadPolicy` function, mock facade data (e.g., `cards`) should be injected directly in the configuration object (e.g., `loadPolicy("...", { cards: { "card-id": { ... } } })`) rather than using `dbQuery` mocks for `agentdesk.db.query`.
- `agentdesk.cards.get(cardId)` is the preferred typed facade replacement for raw `SELECT ... FROM kanban_cards kc WHERE kc.id = ?` queries. It returns the card object directly or `null` if not found.
