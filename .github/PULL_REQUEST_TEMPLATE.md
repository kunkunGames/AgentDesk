<!-- Keep title concise (≤70 chars). Use the body for details. -->

## Summary
<!-- 1–3 bullets: what changed and why. -->

## Test plan
<!-- Bulleted checklist: how to verify the change. -->

## Risks, Rollback & Skipped checks
- **Risk assessment**: <!-- e.g. Low risk, only updates docs -->
- **Rollback plan**: <!-- e.g. Revert PR -->
- **Skipped checks**: <!-- e.g. Did not run e2e tests because this is a docs change -->

## Queue hygiene & duplicate check
- [ ] I have checked for open overlapping PRs (e.g., remote branches or `gh pr list --state open`).
- [ ] For generated inventory refreshes, I have verified no other open refresh PR exists for the same target on a current base.
- [ ] For no-change PRs, I have listed the exact overlapping PR numbers or branches in the PR body and verified the PR contains exactly zero changed files (`gh pr view --json files` or `git diff`).
- [ ] I am closing or recommending closure for low-signal or stale broad branches instead of attempting to salvage them in place.

## Dashboard / UI checklist
- [ ] **시안에 없는 기존 기능을 임의로 삭제하지 않았다.** Reference 시안(redesign reference)에서 빠진 위젯·필터·탭이라도 기존 dashboard에 있던 기능은 사용자 명시 제거 요청 없이 삭제하지 않는다. 시안의 톤·간격·타이포에 맞춰 확장하거나 별도 sub-issue로 분리한다. (관련 결정: #1254 audit, 2026-04-15 결정 기록)
- [ ] 에이전트 아바타는 sprite 컴포넌트(`AgentAvatar`)로 표시한다. inline `${emoji} ${name}` 문자열 패턴은 sprite 또는 name-only로 대체한다 (#1251 / #1254).
- [ ] 추가 또는 변경한 위젯이 mobile/desktop 모두에서 깨지지 않는다.

## Closes
<!-- e.g. Closes #1234 -->

## Boundary fingerprint

- Agent:
- Boundary:
- Primary files:
- Queue hygiene invariant:
- Related PRs/issues checked:
- Why this is non-overlapping:
