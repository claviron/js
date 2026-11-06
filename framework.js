import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { cwd } from "node:process";

const frameworks = new Map([
  ["hono", "Hono"],
  ["elysia", "Elysia"],
  ["express", "Express"],
  ["fastify", "Fastify"],
  ["koa", "Koa"],
  ["@nestjs/core", "NestJS"],
  ["@hapi/hapi", "Hapi"],
]);

/**
 * Discover management metadata once at join, without importing frameworks.
 * The nearest package above the application's working directory owns it.
 */
export async function discoverFrameworks() {
  for (let directory = cwd();;) {
    const path = join(directory, "package.json");
    let text;
    try {
      text = await readFile(path, "utf8");
    } catch (error) {
      if (error.code !== "ENOENT") {
        console.warn(`orbit: framework metadata could not be read from ${path}`, error);
        return [];
      }
    }

    if (text !== undefined) {
      let packageInfo;
      try {
        packageInfo = JSON.parse(text);
      } catch (error) {
        console.warn(`orbit: framework metadata could not be parsed from ${path}`, error);
        return [];
      }
      // Stop at this package, even when it declares no known framework.
      // Keep all distinct recognized names as informational tags.
      const detected = new Set();
      for (const section of ["dependencies", "peerDependencies"]) {
        const dependencies = packageInfo?.[section];
        if (!dependencies || typeof dependencies !== "object" || Array.isArray(dependencies)) {
          continue;
        }
        for (const name of Object.keys(dependencies)) {
          const framework = frameworks.get(name.toLowerCase());
          if (framework) detected.add(framework);
        }
      }
      return [...detected].sort();
    }

    const parent = dirname(directory);
    if (parent === directory) return [];
    directory = parent;
  }
}
