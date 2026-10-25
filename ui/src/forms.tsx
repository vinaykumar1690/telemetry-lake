// Form pieces shared by the Generate and Load test tabs
import { SEVERITIES, type ServiceSpec, type Severity } from "./generator/generate";

export function NumberInput({
  value,
  onChange,
  min = 0,
  max,
  step = 1,
  ariaLabel,
}: {
  value: number;
  onChange: (n: number) => void;
  min?: number;
  max?: number;
  step?: number;
  ariaLabel?: string;
}) {
  return (
    <input
      type="number"
      value={Number.isFinite(value) ? value : ""}
      min={min}
      max={max}
      step={step}
      aria-label={ariaLabel}
      onChange={(e) => onChange(e.target.valueAsNumber)}
    />
  );
}

export function ServicesEditor({
  services,
  setServices,
  maxPods = 100,
}: {
  services: ServiceSpec[];
  setServices: (update: (list: ServiceSpec[]) => ServiceSpec[]) => void;
  maxPods?: number;
}) {
  const setService = (i: number, patch: Partial<ServiceSpec>) =>
    setServices((list) => list.map((s, j) => (j === i ? { ...s, ...patch } : s)));
  return (
    <>
      <table className="services">
        <thead>
          <tr>
            <th>service.name</th>
            <th>Pods</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {services.map((s, i) => (
            <tr key={i}>
              <td>
                <input value={s.name} onChange={(e) => setService(i, { name: e.target.value })} />
              </td>
              <td>
                <NumberInput value={s.pods} min={1} max={maxPods} onChange={(pods) => setService(i, { pods })} />
              </td>
              <td>
                <button className="ghost small" aria-label="Remove service" onClick={() => setServices((l) => l.filter((_, j) => j !== i))}>
                  ✕
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <button className="ghost small" onClick={() => setServices((l) => [...l, { name: `service-${l.length + 1}`, pods: 2 }])}>
        + Service
      </button>
    </>
  );
}

export function SeverityWeightsEditor({
  weights,
  setWeights,
}: {
  weights: Record<Severity, number>;
  setWeights: (update: (w: Record<Severity, number>) => Record<Severity, number>) => void;
}) {
  const weightSum = SEVERITIES.reduce((n, s) => n + Math.max(0, weights[s] || 0), 0);
  return (
    <div className="weights">
      {SEVERITIES.map((s) => (
        <label key={s}>
          <span className={`sev sev-${s.toLowerCase()}`}>{s}</span>
          <NumberInput value={weights[s]} min={0} onChange={(n) => setWeights((w) => ({ ...w, [s]: n }))} />
          <span className="muted small-text">{weightSum ? Math.round(((weights[s] || 0) / weightSum) * 100) : 0}%</span>
        </label>
      ))}
    </div>
  );
}
