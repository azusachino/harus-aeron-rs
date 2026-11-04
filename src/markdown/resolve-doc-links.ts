import path from "node:path";

interface MarkdownNode {
  type: string;
  url?: string;
  children?: MarkdownNode[];
}

interface MarkdownFile {
  history: string[];
}

export function resolveDocLinks(docsDirectory: string) {
  return (tree: MarkdownNode, file: MarkdownFile): void => {
    const sourcePath = file.history[0];
    if (!sourcePath) return;

    const visit = (node: MarkdownNode): void => {
      if (node.type === "link" && node.url) {
        node.url = toAstroRoute(node.url, sourcePath, docsDirectory);
      }
      node.children?.forEach(visit);
    };

    visit(tree);
  };
}

function toAstroRoute(url: string, sourcePath: string, docsDirectory: string): string {
  if (/^(?:[a-z][a-z\d+.-]*:|\/\/|\/)/i.test(url)) return url;

  const suffixPosition = url.search(/[?#]/);
  const markdownPath = suffixPosition === -1 ? url : url.slice(0, suffixPosition);
  const suffix = suffixPosition === -1 ? "" : url.slice(suffixPosition);
  if (!markdownPath.endsWith(".md")) return url;

  const resolvedPath = path.resolve(path.dirname(sourcePath), markdownPath);
  const relativePath = path.relative(docsDirectory, resolvedPath);
  if (relativePath.startsWith("..") || path.isAbsolute(relativePath)) return url;

  let route = relativePath.replace(/\\/g, "/").replace(/\.md$/, "");
  if (route === "index") route = "";
  else route = route.replace(/\/index$/, "");

  return `/${route}${suffix}`;
}
