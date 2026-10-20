// Display and datetime-local conversion in UTC or the browser's zone

const pad = (n: number, width = 2) => String(n).padStart(width, "0");

/** "2026-10-19 07:18:39.894" from RFC 3339 (microseconds are kept when present) */
export function formatTimestamp(rfc3339: string, utc: boolean): string {
  const ms = Date.parse(rfc3339);
  if (Number.isNaN(ms)) return rfc3339;
  const fraction = /\.(\d+)/.exec(rfc3339)?.[1]?.padEnd(6, "0").slice(0, 6) ?? "000000";
  const d = new Date(ms);
  const parts = utc
    ? [d.getUTCFullYear(), d.getUTCMonth() + 1, d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds()]
    : [d.getFullYear(), d.getMonth() + 1, d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds()];
  return `${parts[0]}-${pad(parts[1])}-${pad(parts[2])} ${pad(parts[3])}:${pad(parts[4])}:${pad(parts[5])}.${fraction}`;
}

export function formatShort(ms: number, utc: boolean, withDate: boolean): string {
  const d = new Date(ms);
  const [mo, da, h, mi] = utc
    ? [d.getUTCMonth() + 1, d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes()]
    : [d.getMonth() + 1, d.getDate(), d.getHours(), d.getMinutes()];
  return withDate ? `${pad(mo)}-${pad(da)} ${pad(h)}:${pad(mi)}` : `${pad(h)}:${pad(mi)}`;
}

/** Value for <input type="datetime-local"> (seconds precision) */
export function toLocalInput(ms: number, utc: boolean): string {
  const d = new Date(ms);
  const p = utc
    ? [d.getUTCFullYear(), d.getUTCMonth() + 1, d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds()]
    : [d.getFullYear(), d.getMonth() + 1, d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds()];
  return `${p[0]}-${pad(p[1])}-${pad(p[2])}T${pad(p[3])}:${pad(p[4])}:${pad(p[5])}`;
}

export function fromLocalInput(value: string, utc: boolean): number {
  if (!value) return NaN;
  const withSeconds = value.length === 16 ? `${value}:00` : value;
  return utc ? Date.parse(`${withSeconds}Z`) : new Date(withSeconds).getTime();
}

export const zoneLabel = (utc: boolean) => (utc ? "UTC" : Intl.DateTimeFormat().resolvedOptions().timeZone);
