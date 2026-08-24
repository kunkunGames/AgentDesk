1. **Remove redundant `aria-label` and `dialogLabel` props from `<EmojiPicker>` instances**
   - In `dashboard/src/components/agent-manager/AgentFormModal.tsx` and `dashboard/src/components/agent-manager/DepartmentFormModal.tsx`, the `EmojiPicker` component is invoked with an `aria-label` and a `dialogLabel` that perfectly duplicate the fallback logic inside `EmojiPicker.tsx`. We will remove these redundant props.

2. **Fix `aria-valuetext` semantics in `AgentFormModal.tsx`**
   - Currently, `aria-valuetext` in `AgentFormModal.tsx` uses:
     `t({ ko: \`선택된 스프라이트: \${spriteNum}\`, en: \`Selected sprite: \${spriteNum}\` })`
     This injects "Selected sprite", but since the element has `role="spinbutton"`, the "Selected" word is redundant. We will change it to `t({ ko: \`스프라이트 \${spriteNum}\`, en: \`Sprite \${spriteNum}\` })`.
   - Also, for the fallback when `spriteNum` is 0:
     `t({ ko: \`선택된 아이콘: \${formValues.avatar_emoji || "🤖"}\`, en: \`Selected icon: \${formValues.avatar_emoji || "🤖"}\` })`
     We'll change this to `t({ ko: \`아이콘 \${formValues.avatar_emoji || "🤖"}\`, en: \`Icon \${formValues.avatar_emoji || "🤖"}\` })`.

3. **Verify changes**
   - Run `./scripts/verify-dashboard.sh` to ensure dashboard UI builds without type or formatting errors.

4. **Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.**
   - Run `pre_commit_instructions` and follow the steps.

5. **Submit the PR**
   - Branch: `jules/accessor/modal-accessibility-semantics`
   - PR Title: `Accessor: recreate clean modal icon semantics PR`
