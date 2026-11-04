import { getCollection } from "astro:content";
export async function GET() {
  const documents = await getCollection("docs");
  return new Response(
    JSON.stringify(documents.map(({ id, body }) => ({ id, body }))),
    { headers: { "Content-Type": "application/json; charset=utf-8" } },
  );
}
