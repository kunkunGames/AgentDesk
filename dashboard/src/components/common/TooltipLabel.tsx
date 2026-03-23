import { useEffect, useState } from "react";

interface TooltipLabelProps {
  text: string;
  tooltip: string;
  className?: string;
}

export default function TooltipLabel({ text, tooltip, className }: TooltipLabelProps) {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    if (!open) return;
    const t = setTimeout(() => setOpen(false), 1800);
    return () => clearTimeout(t);
  }, [open]);

  return (
    <span className={`relative inline-flex items-center gap-1 min-w-0 ${className || ""}`}>
      <button
        type="button"
        className="truncate text-left"
        title={tooltip}
        aria-label={tooltip}
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        onTouchStart={(e) => { e.stopPropagation(); setOpen((v) => !v); }}
        onClick={(e) => e.stopPropagation()}
      >
        {text}
      </button>
      <span
        className="text-[10px] shrink-0"
        style={{ color: "var(--th-text-muted)" }}
        title={tooltip}
      >
        ⓘ
      </span>

      {open && (
        <span
          className="absolute z-30 left-0 top-full mt-1 px-2 py-1 rounded-md text-[10px] whitespace-nowrap"
          style={{
            background: "rgba(15,23,42,0.95)",
            color: "#e2e8f0",
            border: "1px solid rgba(148,163,184,0.35)",
            boxShadow: "0 4px 14px rgba(0,0,0,0.25)",
          }}
        >
          {tooltip}
        </span>
      )}
    </span>
  );
}
