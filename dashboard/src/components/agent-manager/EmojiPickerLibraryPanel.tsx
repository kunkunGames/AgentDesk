import { useEffect, useRef } from "react";
import EmojiPickerReact, {
  EmojiStyle,
  Theme,
  type EmojiClickData,
} from "emoji-picker-react";

interface EmojiPickerLibraryPanelProps {
  height: number;
  onSelect: (emoji: string) => void;
  width: number;
  value?: string;
}

export default function EmojiPickerLibraryPanel({
  height,
  onSelect,
  width,
  value,
}: EmojiPickerLibraryPanelProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  const handleEmojiClick = (emojiData: EmojiClickData) => {
    onSelect(emojiData.emoji);
  };

  useEffect(() => {
    if (!value || !containerRef.current) return;

    const normalizeEmoji = (e: string | null | undefined) => (e || "").replace(/\uFE0F/g, "");
    const normalizedValue = normalizeEmoji(value);

    const syncAriaCurrent = () => {
      const buttons = containerRef.current?.querySelectorAll("button.epr-emoji");
      buttons?.forEach((btn) => {
        // Find the emoji character element inside the button to verify against value
        const img = btn.querySelector("img");
        let isSelected = false;

        if (img && normalizeEmoji(img.alt) === normalizedValue) {
          isSelected = true;
        } else if (btn.textContent && normalizeEmoji(btn.textContent) === normalizedValue) {
          isSelected = true;
        } else if (btn.getAttribute("aria-label") && normalizeEmoji(btn.getAttribute("aria-label")) === normalizedValue) {
          isSelected = true;
        }

        if (isSelected) {
          btn.setAttribute("aria-current", "true");
        } else {
          btn.removeAttribute("aria-current");
        }
      });
    };

    // Initial sync
    syncAriaCurrent();

    const observer = new MutationObserver(() => {
      syncAriaCurrent();
    });
    observer.observe(containerRef.current, { childList: true, subtree: true });
    return () => observer.disconnect();
  }, [value]);

  return (
    <div ref={containerRef}>
      <EmojiPickerReact
        autoFocusSearch
        emojiStyle={EmojiStyle.NATIVE}
        height={height}
        lazyLoadEmojis
        onEmojiClick={handleEmojiClick}
        previewConfig={{ showPreview: false }}
        searchClearButtonLabel="Clear emoji search"
        searchPlaceholder="Search emoji"
        skinTonesDisabled={false}
        theme={Theme.DARK}
        width={width}
      />
    </div>
  );
}
