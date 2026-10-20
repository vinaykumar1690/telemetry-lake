import { useEffect, useState } from "react";
import { GeneratorPage } from "./generator/GeneratorPage";
import { SearchPage } from "./search/SearchPage";
import { DEFAULT_STATE, type SearchState } from "./search/types";
import { zoneLabel } from "./time";

type Tab = "search" | "generate";

const tabFromHash = (): Tab => (window.location.hash === "#generate" ? "generate" : "search");

function loadUtc(): boolean {
  try {
    return localStorage.getItem("tl.utc") !== "false";
  } catch {
    return true;
  }
}

function useStatus(url: string): boolean | null {
  const [up, setUp] = useState<boolean | null>(null);
  useEffect(() => {
    let stopped = false;
    const check = async () => {
      try {
        const response = await fetch(url);
        if (!stopped) setUp(response.ok);
      } catch {
        if (!stopped) setUp(false);
      }
    };
    check();
    const timer = setInterval(check, 10_000);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, [url]);
  return up;
}

function Status({ label, up }: { label: string; up: boolean | null }) {
  const text = up === null ? "checking" : up ? "ready" : "unavailable";
  return (
    <span className={`status ${up === null ? "" : up ? "up" : "down"}`} title={`${label}: ${text}`}>
      <span className="dot" /> {label}
    </span>
  );
}

export default function App() {
  const [tab, setTab] = useState<Tab>(tabFromHash);
  const [search, setSearch] = useState<SearchState>(DEFAULT_STATE);
  // Bumped to make the search page run the current state (e.g. from the generator)
  const [runToken, setRunToken] = useState(0);
  const [utc, setUtc] = useState(loadUtc);
  const queryUp = useStatus("/api/query/ready");
  const ingestUp = useStatus("/api/ingest/ready");

  useEffect(() => {
    const onHash = () => setTab(tabFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const go = (next: Tab) => {
    window.location.hash = next;
    setTab(next);
  };

  const toggleUtc = () => {
    setUtc((u) => {
      try {
        localStorage.setItem("tl.utc", String(!u));
      } catch {
        // per-viewer convenience only
      }
      return !u;
    });
  };

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">Telemetry Lake</div>
        <nav className="tabs" role="tablist">
          <button role="tab" aria-selected={tab === "search"} className={tab === "search" ? "active" : ""} onClick={() => go("search")}>
            Search
          </button>
          <button role="tab" aria-selected={tab === "generate"} className={tab === "generate" ? "active" : ""} onClick={() => go("generate")}>
            Generate logs
          </button>
        </nav>
        <div className="topbar-right">
          <Status label="query" up={queryUp} />
          <Status label="ingester" up={ingestUp} />
          <button className="ghost small" onClick={toggleUtc} title="Time zone used to show and enter times">
            {zoneLabel(utc)}
          </button>
        </div>
      </header>
      <main>
        <div hidden={tab !== "search"}>
          <SearchPage state={search} setState={setSearch} runToken={runToken} utc={utc} />
        </div>
        <div hidden={tab !== "generate"}>
          <GeneratorPage
            utc={utc}
            onSearch={(next) => {
              setSearch(next);
              setRunToken((t) => t + 1);
              go("search");
            }}
          />
        </div>
      </main>
    </div>
  );
}
