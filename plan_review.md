The task requires fixing modal accessibility semantics, especially keyboard behavior, accessible names, selected-state semantics.
The instructions explicitly mention: "When improving sprite or emoji selectors in the dashboard, include both accessible names and selected-state semantics, and maintain consistent product terminology across labels."

Let's look at `DepartmentFormModal.tsx`, the color selector:
```tsx
                      aria-label={t({ ko: `색상 ${c}`, en: `Color ${c}` })}
                      aria-checked={form.color === c}
```
If we want to improve this:
```tsx
                      aria-label={form.color === c ? t({ ko: `선택된 색상: ${c}`, en: `Selected color: ${c}` }) : t({ ko: `색상 ${c}`, en: `Color ${c}` })}
```
Ah! This was exactly what PR #6273336575145637610 tried to do! Wait, if I look at that PR diff:
```diff
-                      aria-label={form.color === c ? t({ ko: `선택된 색상: ${c}`, en: `Selected color: ${c}` }) : t({ ko: `색상 ${c}`, en: `Color ${c}` })}
+                      aria-label={t({ ko: `색상 ${c}`, en: `Color ${c}` })}
```
Wait, the diff means the PR *removed* the `선택된 색상` text, because `role="radio"` combined with `aria-checked` already conveys the selected state to screen readers natively! Injecting "Selected" into the `aria-label` creates redundant announcements like "Selected color: #123456, selected, radio button".

Let's check `AgentFormModal.tsx`:
```tsx
              aria-valuetext={spriteNum ? t({ ko: `선택된 스프라이트: ${spriteNum}`, en: `Selected sprite: ${spriteNum}` }) : t({ ko: `선택된 아이콘: ${formValues.avatar_emoji || "🤖"}`, en: `Selected icon: ${formValues.avatar_emoji || "🤖"}` })}
```
Wait, `role="spinbutton"` conveys the current value via `aria-valuenow` and `aria-valuetext`. The screen reader will announce "Sprite number, spinbutton, Selected sprite: 1". The word "Selected" is perhaps redundant here too.
And wait, the PR diff for `AgentFormModal.tsx` in `origin/jules/accessor/modal-accessibility-semantics-6273336575145637610`:
```diff
-              aria-label={tr("스프라이트 아이콘", "Sprite icon")}
+              aria-label={tr("스프라이트 번호", "Sprite number")}
...
-              aria-valuetext={spriteNum ? t({ ko: `선택된 아이콘: 스프라이트 ${spriteNum}`, en: `Selected icon: Sprite ${spriteNum}` }) : t({ ko: `선택된 아이콘: ${formValues.avatar_emoji || "🤖"}`, en: `Selected icon: ${formValues.avatar_emoji || "🤖"}` })}
+              aria-valuetext={spriteNum ? t({ ko: `선택된 스프라이트: ${spriteNum}`, en: `Selected sprite: ${spriteNum}` }) : t({ ko: `선택된 아이콘: ${formValues.avatar_emoji || "🤖"}`, en: `Selected icon: ${formValues.avatar_emoji || "🤖"}` })}
```

Let's check `EmojiPicker.tsx`:
```tsx
        aria-label={
          ariaLabel ||
          (value
            ? tr({ ko: `선택된 아이콘: ${value}, 아이콘 변경`, en: `Selected icon: ${value}, change icon` })
            : tr({ ko: "아이콘 선택기 열기", en: "Open icon picker" }))
        }
```
And in `AgentFormModal.tsx`:
```tsx
                <EmojiPicker
                  id={emojiBtnId}
                  value={formValues.avatar_emoji}
                  onChange={(emoji) => setValue("avatar_emoji", emoji, { shouldDirty: true, shouldValidate: true })}
                  aria-label={
                    formValues.avatar_emoji
                      ? t({ ko: `선택된 아이콘: ${formValues.avatar_emoji}, 아이콘 변경`, en: `Selected icon: ${formValues.avatar_emoji}, change icon` })
                      : t({ ko: "아이콘 선택기 열기", en: "Open icon picker" })
                  }
```
This is fully redundant. We can remove the `aria-label` from `EmojiPicker` invocation in both `AgentFormModal.tsx` and `DepartmentFormModal.tsx`, as `EmojiPicker.tsx` already handles it nicely! Wait, but the PR prompt says: "When improving sprite or emoji selectors in the dashboard, include both accessible names and selected-state semantics, and maintain consistent product terminology across labels."

Let's check `AgentFormModal.tsx` emoji vs sprite labels.
In `AgentFormModal.tsx`:
```tsx
                <EmojiPicker
                  id={emojiBtnId}
                  value={formValues.avatar_emoji}
...
```
Sprite picker has:
```tsx
              <div
                className="flex flex-col items-center gap-1"
                role="group"
                aria-label={tr("스프라이트 선택", "Sprite selection")}
              >
```
Wait, in `AgentFormModal.tsx`, the up arrow increases the sprite number:
```tsx
                <button
                  type="button"
                  tabIndex={-1}
                  aria-label={tr("다음 스프라이트", "Next sprite")}
                  onClick={() => {
                    const next = Math.min(40, Math.max(1, spriteNum || 0) + 1);
                    setValue("sprite_number", next, { shouldDirty: true, shouldValidate: true });
                  }}
                >
                  ▲
                </button>
```
Is `▲` next or previous? In a spinbutton, `▲` usually increases the value, so it is "Next" in terms of number sequence (1, 2, 3), but "Up" in terms of spatial. The arrow keys logic:
```tsx
                if (e.key === "ArrowUp") {
                  e.preventDefault();
                  const next = Math.min(40, Math.max(1, spriteNum || 0) + 1);
```
So up arrow = next. That matches.

Let's look at `DepartmentFormModal.tsx`:
```tsx
                  <EmojiPicker
                    id={iconBtnId}
                    value={form.icon}
                    onChange={(emoji) => setValue("icon", emoji, { shouldDirty: true, shouldValidate: true })}
                    aria-label={
                      form.icon
                        ? t({ ko: `선택된 아이콘: ${form.icon}, 아이콘 변경`, en: `Selected icon: ${form.icon}, change icon` })
                        : t({ ko: "아이콘 선택기 열기", en: "Open icon picker" })
                    }
                    dialogLabel={t({ ko: "아이콘 선택", en: "Choose an icon" })}
                  />
```
Wait, if `EmojiPicker.tsx` provides these exact fallback labels, I can remove `aria-label` from `DepartmentFormModal.tsx` and `AgentFormModal.tsx` for `EmojiPicker`!

Let's check `EmojiPicker.tsx` fallback:
```tsx
        aria-label={
          ariaLabel ||
          (value
            ? tr({ ko: `선택된 아이콘: ${value}, 아이콘 변경`, en: `Selected icon: ${value}, change icon` })
            : tr({ ko: "아이콘 선택기 열기", en: "Open icon picker" }))
        }
```
It matches perfectly. So removing the `aria-label` from the caller `EmojiPicker` in `AgentFormModal.tsx` and `DepartmentFormModal.tsx` is one cleanup.

Wait, is there any semantic issue? "include both accessible names and selected-state semantics, and maintain consistent product terminology across labels"

In `AgentFormModal.tsx`:
```tsx
              <div
                className="flex flex-col items-center gap-1"
                role="group"
                aria-label={tr("스프라이트 선택", "Sprite selection")}
              >
```
But the parent is:
```tsx
            <div
              className="flex items-center gap-3 rounded focus:outline-none focus:ring-2 focus:ring-[var(--th-accent-primary)] focus:ring-offset-2 focus:ring-offset-[var(--th-bg-surface)]"
              role="spinbutton"
              aria-label={tr("스프라이트 번호", "Sprite number")}
...
```
Is it valid to have a `role="group"` inside a `role="spinbutton"`? No, `spinbutton` is a composite widget, but typically its children are just presentational or just the buttons. In WAI-ARIA, `spinbutton` can have buttons to increment and decrement. However, the `spinbutton` role does not require them to be inside it, or if they are, they shouldn't have conflicting roles like `group`.

Let's examine the modal accessibility selectors in the PR #196 or #202.
The PR #202 diff we couldn't see, but we can recreate it:
"2026-05-13 queue cleanup closed both #196 and #202 instead of choosing between two contaminated branches. Accessor should recreate one clean modal accessibility PR from current `main` when the idea is still valuable."
"When improving sprite or emoji selectors in the dashboard, include both accessible names and selected-state semantics, and maintain consistent product terminology across labels."

Let's check `EmojiPickerLibraryPanel.tsx`. It assigns `aria-pressed="true"` for the selected emoji in the list.
```tsx
      container.querySelectorAll("button.epr-emoji").forEach((button) => {
        const text = button.textContent ?? "";
        if (normalize(text) === target) {
          button.setAttribute("aria-pressed", "true");
        } else {
          button.setAttribute("aria-pressed", "false");
        }
        if (text && !button.hasAttribute("aria-label")) {
          button.setAttribute("aria-label", tr({ ko: `아이콘 ${text}`, en: `Icon ${text}` }));
        }
      });
```
This is good.

What about `AgentFormModal.tsx` and `DepartmentFormModal.tsx`?
If I look closely at `AgentFormModal.tsx`:
```tsx
                <button
                  type="button"
                  tabIndex={-1}
                  aria-label={tr("다음 스프라이트", "Next sprite")}
                  ...
```
```tsx
                <button
                  type="button"
                  tabIndex={-1}
                  aria-label={tr("이전 스프라이트", "Previous sprite")}
                  ...
```
Are there issues with `EmojiPicker` callers?
In `AgentFormModal.tsx`:
```tsx
              <div>
                <label htmlFor={emojiBtnId} className="block text-xs mb-1.5 font-medium" style={{ color: "var(--th-text-secondary)" }}>
                  {tr("아이콘", "Icon")}
                </label>
                <EmojiPicker
                  id={emojiBtnId}
```
Wait, the `<EmojiPicker>` is rendered inside `<label>`? No, it's alongside. `htmlFor={emojiBtnId}` matches `id={emojiBtnId}`. `EmojiPicker` renders a `<button id={id}>`. This is correct.

Let's check `DepartmentFormModal.tsx`:
```tsx
                <div>
                  <label htmlFor={iconBtnId} className="block text-xs mb-1.5 font-medium" style={{ color: "var(--th-text-secondary)" }}>
                    {tr("아이콘", "Icon")}
                  </label>
                  <EmojiPicker
                    id={iconBtnId}
```

Wait, `aria-valuetext` in `AgentFormModal.tsx` says:
```tsx
aria-valuetext={spriteNum ? t({ ko: `선택된 스프라이트: ${spriteNum}`, en: `Selected sprite: ${spriteNum}` }) : t({ ko: `선택된 아이콘: ${formValues.avatar_emoji || "🤖"}`, en: `Selected icon: ${formValues.avatar_emoji || "🤖"}` })}
```
Wait, in `AgentFormModal.tsx`, `t` comes from `const { t } = useI18n();`, which is perfectly fine. But notice it's mixing "Selected sprite" and "Selected icon".
Is there an inconsistency? "maintain consistent product terminology across labels"
In `AgentFormModal.tsx`:
```tsx
              aria-label={tr("스프라이트 번호", "Sprite number")}
```
But then we have:
```tsx
              <div
                className="flex flex-col items-center gap-1"
                role="group"
                aria-label={tr("스프라이트 선택", "Sprite selection")}
              >
```
And:
```tsx
                  aria-label={tr("다음 스프라이트", "Next sprite")}
```
```tsx
                  aria-label={tr("이전 스프라이트", "Previous sprite")}
```

Wait, what if the PR #196/202 was about the redundancy in `EmojiPicker`'s `aria-label`?
Removing the redundant `aria-label` from `<EmojiPicker />` in both files ensures we rely on `EmojiPicker`'s built-in accessibility. But wait, `dialogLabel` is still passed redundantly:
```tsx
                    dialogLabel={t({ ko: "아이콘 선택", en: "Choose an icon" })}
```
In `EmojiPicker`, `dialogLabel` defaults to `tr({ ko: "아이콘 선택", en: "Choose an icon" })`!
So both `aria-label` and `dialogLabel` passed to `EmojiPicker` are 100% redundant and exactly the same as the fallback values.

Let's verify `EmojiPicker.tsx`:
```tsx
  "aria-label": ariaLabel,
  dialogLabel,
```
```tsx
        aria-label={
          ariaLabel ||
          (value
            ? tr({ ko: `선택된 아이콘: ${value}, 아이콘 변경`, en: `Selected icon: ${value}, change icon` })
            : tr({ ko: "아이콘 선택기 열기", en: "Open icon picker" }))
        }
```
```tsx
          aria-label={dialogLabel || tr({ ko: "아이콘 선택", en: "Choose an icon" })}
```

Let's clean that up.
What else? "When improving sprite or emoji selectors in the dashboard, include both accessible names and selected-state semantics, and maintain consistent product terminology across labels."
If we look at `AgentFormModal.tsx`, `avatar_emoji` has `aria-label` in `EmojiPicker`. What about the `sprite` selector?
The sprite selector has:
```tsx
              aria-label={tr("스프라이트 번호", "Sprite number")}
              aria-valuenow={spriteNum || 0}
              aria-valuemin={0}
              aria-valuemax={40}
              aria-valuetext={spriteNum ? t({ ko: `선택된 스프라이트: ${spriteNum}`, en: `Selected sprite: ${spriteNum}` }) : t({ ko: `선택된 아이콘: ${formValues.avatar_emoji || "🤖"}`, en: `Selected icon: ${formValues.avatar_emoji || "🤖"}` })}
```
Should it just be `Selected sprite` or `Selected icon`? The instructions say "maintain consistent product terminology across labels."
In Korean, it uses `선택된 스프라이트: ${spriteNum}` and `선택된 아이콘: ${...}`.
But wait! If `spriteNum` is 0, it means no sprite is selected, so it falls back to the emoji icon. That's why it says `Selected icon: 🤖`.
Is this consistent? The terminology used elsewhere for emoji is "아이콘" (Icon). For sprite, it's "스프라이트" (Sprite). So this seems consistent.
But wait, look at `AgentFormModal.tsx` line 178:
```tsx
              <div
                className="flex flex-col items-center gap-1"
                role="group"
                aria-label={tr("스프라이트 선택", "Sprite selection")}
              >
```
Role `group` inside `spinbutton`? If screen reader focuses `spinbutton`, it reads the `aria-label` and `aria-valuetext`. The inner elements with `tabIndex={-1}` won't be focused by keyboard, but they can be clicked or touched. Their `aria-label` is "Next sprite" and "Previous sprite". This seems okay.

Is there any other issue?
Let's check `DepartmentFormModal.tsx` theme color selector:
```tsx
              <div role="radiogroup" aria-labelledby={themeColorLabelId}>
                <div id={themeColorLabelId} className="block text-xs mb-1.5 font-medium" style={{ color: "var(--th-text-secondary)" }}>
                  {tr("테마 색상", "Theme Color")}
                </div>
                <div className="flex gap-2 flex-wrap">
                  {DEPT_COLORS.map((c, index) => (
                    <button
                      key={c}
                      ref={(node) => {
                        colorButtonRefs.current[index] = node;
                      }}
                      type="button"
                      role="radio"
                      aria-label={t({ ko: `색상 ${c}`, en: `Color ${c}` })}
                      aria-checked={form.color === c}
```
Should `aria-label` have `Selected color:` when checked?
The PR `origin/jules/accessor/modal-accessibility-semantics-6273336575145637610` had a change exactly about this!
```diff
-                      aria-label={form.color === c ? t({ ko: `선택된 색상: ${c}`, en: `Selected color: ${c}` }) : t({ ko: `색상 ${c}`, en: `Color ${c}` })}
+                      aria-label={t({ ko: `색상 ${c}`, en: `Color ${c}` })}
```
Wait, wait! I looked at the reverse diff earlier?
Let me check the actual current content of `DepartmentFormModal.tsx`.
```tsx
                      aria-label={t({ ko: `색상 ${c}`, en: `Color ${c}` })}
```
It is ALREADY `aria-label={t({ ko: `색상 ${c}`, en: `Color ${c}` })}` without the "선택된 색상". So the fix from that PR was already merged or I misread? Wait, no, that PR branch was an unmerged branch, maybe someone else merged a fix?
Let's look at `DepartmentFormModal.tsx`:
```tsx
                      aria-label={t({ ko: `색상 ${c}`, en: `Color ${c}` })}
```
Yes, it's currently correct.

Let's re-read the PR instructions.
"PR #196 and PR #202 both target AgentFormModal/DepartmentFormModal accessibility, so always check those files and branches before proposing another modal selector change."
"2026-05-13 queue cleanup closed both #196 and #202 instead of choosing between two contaminated branches. Accessor should recreate one clean modal accessibility PR from current `main` when the idea is still valuable."
"When improving sprite or emoji selectors in the dashboard, include both accessible names and selected-state semantics, and maintain consistent product terminology across labels."

Wait, what if the PR wanted to ADD the selected state semantics to the Emoji Picker?
In `EmojiPickerLibraryPanel.tsx`, it adds `aria-pressed="true"`.
But the `EmojiPicker.tsx` button itself:
```tsx
        aria-label={
          ariaLabel ||
          (value
            ? tr({ ko: `선택된 아이콘: ${value}, 아이콘 변경`, en: `Selected icon: ${value}, change icon` })
            : tr({ ko: "아이콘 선택기 열기", en: "Open icon picker" }))
        }
```
This already has "Selected icon: ..., change icon".
What about the `DepartmentFormModal.tsx` emoji picker?
```tsx
                  <EmojiPicker
                    id={iconBtnId}
                    value={form.icon}
                    onChange={(emoji) => setValue("icon", emoji, { shouldDirty: true, shouldValidate: true })}
                    aria-label={
                      form.icon
                        ? t({ ko: `선택된 아이콘: ${form.icon}, 아이콘 변경`, en: `Selected icon: ${form.icon}, change icon` })
                        : t({ ko: "아이콘 선택기 열기", en: "Open icon picker" })
                    }
                    dialogLabel={t({ ko: "아이콘 선택", en: "Choose an icon" })}
                  />
```
If we look closely, `t()` is used here, but in `DepartmentFormModal.tsx` there is no `t` extracted from `useI18n()`. Wait!
In `DepartmentFormModal.tsx`:
```tsx
  const { t } = useI18n();
```
Ah, `t` IS extracted. I verified that.

What if the redundancy IS the problem?
Let's see what the diff for `origin/jules/accessor/modal-icon-semantics-recreate-16861156348524512791` looks like for `DepartmentFormModal.tsx`.
Oh wait! In `DepartmentFormModal.tsx`, we have:
```tsx
          <h3 id="dept-modal-title" className="text-base font-bold flex items-center gap-2" style={{ color: "var(--th-text-heading)" }}>
            <span className="text-lg" aria-hidden="true">{form.icon}</span>
            {isEdit ? tr("부서 정보 수정", "Edit Department") : tr("신규 부서 추가", "Add Department")}
          </h3>
```
And in `AgentFormModal.tsx`, we don't have the icon in the title.

Wait! I can check the exact branches mentioned:
```bash
git diff origin/jules/accessor/modal-accessibility-selectors-recreate-12648799679024778426
```
