type ThemeMode = "system" | "dark" | "light";

interface SearchDocument {
  id: string;
  body: string;
}

const root = document.documentElement;
const themeButton = document.querySelector<HTMLButtonElement>("#theme");
const systemTheme = window.matchMedia("(prefers-color-scheme: dark)");
const searchInput = document.querySelector<HTMLInputElement>("#filter");
const searchResults = document.querySelector<HTMLDivElement>("#search-results");

function getThemeMode(): ThemeMode {
  const savedTheme = localStorage.getItem("aeron-theme");
  return savedTheme === "dark" || savedTheme === "light" ? savedTheme : "system";
}

function applyTheme(mode = getThemeMode()): void {
  const theme = mode === "system" ? (systemTheme.matches ? "dark" : "light") : mode;
  root.dataset.mode = mode;
  root.dataset.theme = theme;

  if (themeButton) {
    const label = mode === "system" ? `System (${theme})` : theme;
    themeButton.textContent = `Theme: ${label[0].toUpperCase()}${label.slice(1)}`;
  }
}

function setupThemeControl(): void {
  applyTheme();
  systemTheme.addEventListener("change", () => {
    if (getThemeMode() === "system") applyTheme();
  });

  themeButton?.addEventListener("click", () => {
    const nextMode: Record<ThemeMode, ThemeMode> = {
      system: "dark",
      dark: "light",
      light: "system",
    };
    const mode = nextMode[getThemeMode()];
    localStorage.setItem("aeron-theme", mode);
    applyTheme(mode);
  });
}

async function setupSearch(): Promise<void> {
  if (!searchInput || !searchResults) return;

  const response = await fetch("/search-index.json");
  const documents: SearchDocument[] = await response.json();

  searchInput.addEventListener("input", () => {
    const query = searchInput.value.trim().toLowerCase();
    searchResults.replaceChildren();

    if (query.length < 2) {
      searchResults.hidden = true;
      return;
    }

    for (const entry of documents.filter((candidate) => candidate.body.toLowerCase().includes(query)).slice(0, 8)) {
      const link = document.createElement("a");
      const heading = entry.body.match(/^#\s+(.+)$/m)?.[1] ?? entry.id;
      const route = entry.id === "index" ? "/" : `/${entry.id.replace(/\/index$/, "")}`;

      link.textContent = heading;
      link.href = route;
      link.setAttribute("role", "option");
      searchResults.append(link);
    }

    searchResults.hidden = searchResults.childElementCount === 0;
  });
}

function setupCodeCopy(): void {
  document.querySelectorAll<HTMLElement>('pre:not([data-language="mermaid"])').forEach((pre) => {
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = "Copy";
    button.setAttribute("aria-label", "Copy code");
    button.addEventListener("click", () => {
      const code = pre.querySelector("code");
      void navigator.clipboard.writeText(code?.textContent ?? pre.textContent ?? "");
    });
    pre.append(button);
  });
}

function setupMermaidDiagrams(): void {
  const diagrams = document.querySelectorAll<HTMLElement>('pre[data-language="mermaid"]');
  if (diagrams.length === 0) return;

  void import("mermaid").then(({ default: mermaid }) => {
    mermaid.initialize({ startOnLoad: false, theme: "neutral" });
    diagrams.forEach(async (diagram) => {
      const { svg } = await mermaid.render(`diagram-${Math.random().toString(36).slice(2)}`, diagram.textContent ?? "");
      diagram.outerHTML = svg;
    });
  });
}

setupThemeControl();
setupCodeCopy();
setupMermaidDiagrams();
void setupSearch();
