import { useRef, useState } from "react";
import { formatShort } from "../time";

export interface Bucket {
  t: number; // bucket start, epoch ms
  n: number;
}

interface Props {
  buckets: Bucket[];
  startMs: number;
  endMs: number;
  bucketMs: number;
  utc: boolean;
  loading: boolean;
  error?: string;
  note?: string;
  onSelect: (startMs: number, endMs: number) => void;
}

const HEIGHT = 90;

/** Log volume over the range; drag across it to zoom the time range */
export function Histogram({ buckets, startMs, endMs, bucketMs, utc, loading, error, note, onSelect }: Props) {
  const svg = useRef<SVGSVGElement>(null);
  const [drag, setDrag] = useState<{ from: number; to: number } | null>(null);
  const [hover, setHover] = useState<Bucket | null>(null);
  const span = Math.max(1, endMs - startMs);
  const max = Math.max(1, ...buckets.map((b) => b.n));
  const total = buckets.reduce((sum, b) => sum + b.n, 0);
  const withDate = span > 20 * 3600_000;

  const fraction = (clientX: number) => {
    const rect = svg.current!.getBoundingClientRect();
    return Math.min(1, Math.max(0, (clientX - rect.left) / rect.width));
  };
  const timeAt = (f: number) => startMs + f * span;

  const ticks = Array.from({ length: 6 }, (_, i) => startMs + (span * i) / 5);

  return (
    <div className="histogram">
      <div className="histogram-head">
        <span>
          {loading ? "Counting…" : error ? <span className="error-text">{error}</span> : `${total.toLocaleString()} logs`}
          {hover && (
            <span className="muted">
              {" "}
              · {formatShort(hover.t, utc, true)}: {hover.n.toLocaleString()}
            </span>
          )}
        </span>
        <span className="muted small-text">{note ?? "drag to zoom"}</span>
      </div>
      <svg
        ref={svg}
        className="histogram-svg"
        viewBox={`0 0 1000 ${HEIGHT}`}
        preserveAspectRatio="none"
        onMouseDown={(e) => {
          const f = fraction(e.clientX);
          setDrag({ from: f, to: f });
        }}
        onMouseMove={(e) => {
          const f = fraction(e.clientX);
          if (drag) setDrag({ ...drag, to: f });
          const t = timeAt(f);
          setHover(buckets.find((b) => t >= b.t && t < b.t + bucketMs) ?? null);
        }}
        onMouseLeave={() => {
          setHover(null);
          setDrag(null);
        }}
        onMouseUp={() => {
          if (drag && Math.abs(drag.to - drag.from) > 0.005) {
            const a = timeAt(Math.min(drag.from, drag.to));
            const b = timeAt(Math.max(drag.from, drag.to));
            onSelect(Math.floor(a), Math.ceil(b));
          }
          setDrag(null);
        }}
      >
        {buckets.map((b) => {
          const x = ((b.t - startMs) / span) * 1000;
          const w = Math.max(1, (bucketMs / span) * 1000 - 1);
          const h = (b.n / max) * (HEIGHT - 4);
          return <rect key={b.t} className={hover === b ? "bar hover" : "bar"} x={x} y={HEIGHT - h} width={w} height={h} />;
        })}
        {drag && (
          <rect
            className="brush"
            x={Math.min(drag.from, drag.to) * 1000}
            y={0}
            width={Math.abs(drag.to - drag.from) * 1000}
            height={HEIGHT}
          />
        )}
      </svg>
      <div className="histogram-axis">
        {ticks.map((t) => (
          <span key={t}>{formatShort(t, utc, withDate)}</span>
        ))}
      </div>
    </div>
  );
}
