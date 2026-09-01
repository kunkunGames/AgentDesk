import { useEffect, useRef } from "react";
import EmojiPickerReact, {
  EmojiStyle,
  Theme,
  type EmojiClickData,
} from "emoji-picker-react";
import { useI18n } from "../../i18n";

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
  const { t: tr } = useI18n();

  const handleEmojiClick = (emojiData: EmojiClickData) => {
    onSelect(emojiData.emoji);
  };

  // emoji-picker-react (v4) renders each emoji as `button.epr-emoji` whose
  // visible text is the native emoji. The library exposes no selected state, so
  // we mark the current value with `aria-pressed="true"` for screen readers.
  // aria-pressed is used (not aria-selected, which is ignored on an implicit
  // role=button) so the toggle state of the button is actually announced.
  // The match is exact (ignoring the FE0F variation selector) so composed
  // sequences that merely contain the same codepoint are not tagged.
  useEffect(() => {
    const container = containerRef.current;
    if (!value || !container) return;

    const normalize = (text: string) => text.replace(/\uFE0F/g, "").trim();
    const target = normalize(value);

    const syncSelected = () => {
      container.querySelectorAll("button.epr-emoji").forEach((button) => {
        const text = button.textContent ?? "";
        if (normalize(text) === target) {
          button.setAttribute("aria-pressed", "true");
        } else {
          button.setAttribute("aria-pressed", "false");
        }
        if (text) {
          if (normalize(text) === target) {
            button.setAttribute("aria-label", tr({ ko: `선택된 아이콘: ${text}`, en: `Selected icon: ${text}` }));
          } else if (!button.hasAttribute("aria-label") || button.getAttribute("aria-label")?.startsWith("선택된 아이콘:") || button.getAttribute("aria-label")?.startsWith("Selected icon:")) {
            button.setAttribute("aria-label", tr({ ko: `아이콘 ${text}`, en: `Icon ${text}` }));
          }
        }
      });
    };

    // Apply once for the emojis already mounted (observers do not replay the
    // mutations that happened before observe()), then keep in sync as the
    // library mounts/unmounts buttons during search, scroll and lazy loading.
    syncSelected();
    const observer = new MutationObserver(syncSelected);
    observer.observe(container, { childList: true, subtree: true });
    return () => observer.disconnect();
  }, [tr, value]);

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
