import { defineCollection, z } from 'astro:content';
import { glob } from 'astro/loaders';
export const collections = { docs: defineCollection({ loader: glob({ base: './docs', pattern: '**/*.md', generateId: ({ entry }) => entry.replace(/\.md$/, '') }), schema: z.object({ title: z.string().optional() }) }) };
