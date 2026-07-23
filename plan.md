1. **Modify `policies/review-automation.js`**:
   - For line 464, replace `var lifecycleRows = agentdesk.db.query("SELECT status FROM kanban_cards WHERE id = ?", [dispatch.kanban_card_id]);` with `var card = agentdesk.cards.get(dispatch.kanban_card_id);` and `var currentStatus = card ? card.status : null;`.
   - For line 1128, replace `var rows = agentdesk.db.query("SELECT status FROM kanban_cards WHERE id = ?", [cardId]);` with `var card = agentdesk.cards.get(cardId);` and `if (!card) return false;` and `var currentStatus = card.status;`.
   - For line 1321, replace `var cardCheck = agentdesk.db.query("SELECT status FROM kanban_cards WHERE id = ?", [cardId]);` with `var card = agentdesk.cards.get(cardId);` and `if (card && agentdesk.pipeline.isTerminal(card.status, cfg))` and `var currentState = card ? card.status : null;`.

2. **Discover exact card ID in tests**:
   - Run a `grep` on `policies/__tests__/review-automation.test.js` to find the exact test card ID used by examining the `module.onDispatchCompleted({ card_id: "..." })` call in the tests near line 195 and 252.

3. **Update tests in `policies/__tests__/review-automation.test.js`**:
   - Replace the `SELECT status FROM kanban_cards WHERE id = ?` mock with `cards: { "<exact-card-id>": { id: "<exact-card-id>", status: "review" } }` at lines 195 and 252 where `loadPolicy` is called.

4. **Run Verification Commands**:
   - `npm run test:policies`
   - `git diff --check`
   - `./scripts/verify-dashboard.sh`
   - `cargo check --all-targets`

5. **Complete pre-commit step**:
   - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.

6. **Submit PR**:
   - Create branch `jules/facade-pilot/review-automation-status-facade`.
   - Create PR with title `FacadePilot: replace raw status db access with agentdesk.cards.get in review-automation`.
