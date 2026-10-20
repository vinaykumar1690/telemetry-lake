import { useEffect, useRef, useState } from "react";

export interface Option {
  value: string;
  count?: number;
}

interface Props {
  label: string;
  options: Option[];
  selected: string[];
  onChange: (values: string[]) => void;
  loading?: boolean;
}

/** Dropdown of checkboxes with counts; typing a value not listed and pressing Enter adds it */
export function MultiSelect({ label, options, selected, onChange, loading }: Props) {
  const [open, setOpen] = useState(false);
  const [text, setText] = useState("");
  const root = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => {
      if (!root.current?.contains(e.target as Node)) setOpen(false);
    };
    const escape = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", escape);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", escape);
    };
  }, [open]);

  const toggle = (value: string) =>
    onChange(selected.includes(value) ? selected.filter((v) => v !== value) : [...selected, value]);

  const needle = text.toLowerCase();
  const known = new Set(options.map((o) => o.value));
  // Selected values that are not among the facets (e.g. typed, or outside the range) stay visible
  const all: Option[] = [...selected.filter((v) => !known.has(v)).map((value) => ({ value })), ...options];
  const shown = all.filter((o) => o.value.toLowerCase().includes(needle));

  return (
    <div className={`multiselect ${selected.length ? "has-value" : ""}`} ref={root}>
      <button type="button" className="multiselect-button" onClick={() => setOpen((o) => !o)} aria-expanded={open}>
        <span className="multiselect-label">{label}</span>
        {selected.length > 0 && <span className="badge">{selected.length === 1 ? selected[0] : selected.length}</span>}
        <span className="caret">▾</span>
      </button>
      {open && (
        <div className="multiselect-menu">
          <input
            autoFocus
            placeholder="Filter or type a value + Enter"
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && text.trim()) {
                if (!selected.includes(text.trim())) onChange([...selected, text.trim()]);
                setText("");
              }
            }}
          />
          <div className="multiselect-options">
            {loading && <div className="muted pad">Loading…</div>}
            {!loading && shown.length === 0 && <div className="muted pad">No values in this time range</div>}
            {shown.map((o) => (
              <label key={o.value} className="option">
                <input type="checkbox" checked={selected.includes(o.value)} onChange={() => toggle(o.value)} />
                <span className="option-value">{o.value}</span>
                {o.count !== undefined && <span className="muted">{o.count.toLocaleString()}</span>}
              </label>
            ))}
          </div>
          {selected.length > 0 && (
            <button type="button" className="ghost small" onClick={() => onChange([])}>
              Clear
            </button>
          )}
        </div>
      )}
    </div>
  );
}
