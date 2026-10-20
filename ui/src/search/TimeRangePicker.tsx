import { useEffect, useState } from "react";
import { fromLocalInput, toLocalInput } from "../time";
import { resolveRange } from "./buildRequest";
import type { TimeRange } from "./types";

const PRESETS = ["15m", "1h", "6h", "24h", "7d"];

interface Props {
  range: TimeRange;
  utc: boolean;
  onChange: (range: TimeRange) => void;
}

export function TimeRangePicker({ range, utc, onChange }: Props) {
  const [custom, setCustom] = useState(range.kind === "absolute");
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");

  // Seed the inputs from the current range (and keep them in sync when it changes elsewhere)
  useEffect(() => {
    const { startMs, endMs } = resolveRange(range);
    setStart(toLocalInput(startMs, utc));
    setEnd(toLocalInput(endMs, utc));
    if (range.kind === "absolute") setCustom(true);
  }, [range, utc]);

  const startMs = fromLocalInput(start, utc);
  const endMs = fromLocalInput(end, utc);
  const invalid = Number.isNaN(startMs) || Number.isNaN(endMs) || endMs <= startMs;

  return (
    <div className="time-range">
      <div className="segmented">
        {PRESETS.map((p) => (
          <button
            key={p}
            type="button"
            className={!custom && range.kind === "relative" && range.last === p ? "active" : ""}
            onClick={() => {
              setCustom(false);
              onChange({ kind: "relative", last: p });
            }}
          >
            {p}
          </button>
        ))}
        <button type="button" className={custom ? "active" : ""} onClick={() => setCustom(true)}>
          Custom
        </button>
      </div>
      {custom && (
        <div className="custom-range">
          <input type="datetime-local" step={1} value={start} onChange={(e) => setStart(e.target.value)} aria-label="Start" />
          <span className="muted">→</span>
          <input type="datetime-local" step={1} value={end} onChange={(e) => setEnd(e.target.value)} aria-label="End" />
          <button
            type="button"
            className="small"
            disabled={invalid}
            onClick={() =>
              onChange({ kind: "absolute", start: new Date(startMs).toISOString(), end: new Date(endMs).toISOString() })
            }
          >
            Apply
          </button>
        </div>
      )}
    </div>
  );
}
