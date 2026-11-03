import { spawn, execFile } from "node:child_process";
import { once } from "node:events";
import { createWriteStream } from "node:fs";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { finished } from "node:stream/promises";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { deflateSync } from "node:zlib";
import { nativeTargets } from "../native.js";
import { readPrepared, releaseContext, sourceDigest, stagePrepared } from "./release.js";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const execute = promisify(execFile);
const quote = (value) => `'${value.replaceAll("'", "'\\''")}'`;

export function selectHosts(config, selected = []) {
  if (!config?.hosts || typeof config.hosts !== "object" || Array.isArray(config.hosts)) throw new Error(".hosts requires a hosts object");
  const entries = Object.entries(config.hosts);
  if (!entries.length) throw new Error(".hosts has no build hosts");
  for (const name of selected) if (!Object.hasOwn(config.hosts, name)) throw new Error(`Unknown host selection: ${name}`);
  const hosts = entries.filter(([name]) => !selected.length || selected.includes(name)).map(([name, host]) => {
    if (!/^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(name)) throw new Error("Invalid host label");
    if (!host || typeof host.ssh !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.@:[\]-]*$/.test(host.ssh)) throw new Error(`${name}: invalid SSH destination`);
    if (typeof host.repository !== "string" || !host.repository.startsWith("/") || /[\0\r\n]/.test(host.repository)) throw new Error(`${name}: repository must be an absolute remote path`);
    if (!Object.hasOwn(nativeTargets, host.target)) throw new Error(`${name}: unknown release target`);
    if (host.jobs !== undefined && (!Number.isSafeInteger(host.jobs) || host.jobs < 1)) throw new Error(`${name}: jobs must be a positive integer`);
    if (host.nice !== undefined && (!Number.isInteger(host.nice) || host.nice < -20 || host.nice > 19)) throw new Error(`${name}: nice must be between -20 and 19`);
    if (host.cache !== undefined && (typeof host.cache !== "string" || !host.cache.startsWith("/") || /[\0\r\n]/.test(host.cache))) throw new Error(`${name}: cache must be an absolute remote path`);
    return { ...host, name };
  });
  if (new Set(hosts.map((host) => host.target)).size !== hosts.length) throw new Error("Select only one builder per target");
  return hosts;
}

async function transfer(host, result, context, root, log) {
  const temporary = await mkdtemp(join(root, "prebuilds", `.build-${host.target}-`));
  try {
    const child = spawn("rsync", ["-rt", "--protect-args", "--include=/orbit.node", "--include=/release.json", "--exclude=*", "--",
      `${host.ssh}:${result.directory}/`, temporary + "/"], { cwd: root, stdio: ["ignore", log, log] });
    const [code, signal] = await once(child, "close");
    if (code !== 0) throw new Error(`Artifact transfer failed (${signal ?? code})`);
    const { bytes, receipt } = await readPrepared(host.target, temporary, context);
    if (result.sha256 !== receipt.sha256) throw new Error("Collected binary differs from the completed build");
    if (context.source !== await sourceDigest(root)) throw new Error("Local release inputs changed during the build");
    await stagePrepared(root, host.target, bytes, receipt);
  } finally { await rm(temporary, { recursive: true, force: true }); }
}

export async function buildAll(hosts, { root = repository, dryRun = false } = {}) {
  const { stdout } = await execute("git", ["rev-parse", "HEAD"], { cwd: root });
  const revision = stdout.trim();
  if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error("Cannot determine the source commit");
  const context = await releaseContext(root);
  for (const host of hosts) console.log(`${host.name}: ${host.ssh} -> ${host.target}, revision ${revision.slice(0, 8)}`);
  if (dryRun) return [];
  const dependencyLock = deflateSync(await readFile(join(root, "Cargo.lock"))).toString("base64");
  try { await execute("git", ["cat-file", "-e", `${revision}:scripts/build-worker.js`], { cwd: root }); }
  catch { throw new Error("Commit the builder scripts before starting remote builds"); }
  await mkdir(join(root, "target", "build-all"), { recursive: true });
  await mkdir(join(root, "prebuilds"), { recursive: true });
  const run = await mkdtemp(join(root, "target", "build-all", "run-"));
  const settled = await Promise.allSettled(hosts.map(async (host) => {
    const file = join(run, `${host.name}.log`);
    const log = createWriteStream(file, { mode: 0o600 });
    let started;
    try {
      const request = { repository: host.repository, target: host.target, jobs: host.jobs,
        nice: host.nice, cache: host.cache, revision, context, dependencyLock };
      const encoded = Buffer.from(JSON.stringify(request)).toString("base64");
      const git = `git -C ${quote(host.repository)}`;
      const bootstrap = `set -eu; set -o pipefail; ${git} cat-file -e ${quote(revision + "^{commit}")} 2>/dev/null || ${git} fetch --no-tags origin ${revision}; ` +
        `${git} show ${quote(revision + ":scripts/build-worker.js")} | node --input-type=module - ${encoded}`;
      const remote = `bash -l -c ${quote(bootstrap)}`;
      const child = spawn("ssh", ["-T", "-o", "BatchMode=yes", "--", host.ssh, remote], { cwd: root, stdio: ["pipe", "pipe", "pipe"] });
      const closed = once(child, "close");
      child.stdin.on("error", () => {});
      child.stdout.pipe(log, { end: false });
      child.stderr.pipe(log, { end: false });
      let result;
      const lines = createInterface({ input: child.stdout });
      lines.on("line", (line) => {
        try {
          if (line.startsWith("ORBIT_BUILD_STARTED ")) {
            started = JSON.parse(line.slice(20));
            console.log(`${host.name}: building; remote log ${started.job}/build.log`);
          } else if (line.startsWith("ORBIT_BUILD_RESULT ")) result = JSON.parse(line.slice(19));
        } catch {}
      });
      child.stdin.end();
      const [code, signal] = await closed;
      if (code !== 0 || !result?.success) throw new Error(result?.error ?? `SSH ended (${signal ?? code}); ${started ? `job: ${started.job}` : "see local log"}`);
      if (result.target !== host.target || result.revision !== revision || result.source !== context.source || result.version !== context.version ||
        typeof result.directory !== "string" || !isAbsolute(result.directory) || /[\0\r\n]/.test(result.directory)) throw new Error("Unexpected remote build result");
      await transfer(host, result, context, root, log);
      console.log(`${host.name}: collected and verified ${host.target} for ${context.version}`);
      return { host: host.name, target: host.target, success: true, job: result.job };
    } catch (error) {
      console.error(`${host.name}: ${error.message}; local log ${file}`);
      return { host: host.name, target: host.target, success: false, error: error.message, job: started?.job };
    } finally { log.end(); await finished(log); }
  }));
  const results = settled.map((entry, index) => entry.status === "fulfilled" ? entry.value :
    { host: hosts[index].name, target: hosts[index].target, success: false, error: entry.reason.message });
  await writeFile(join(run, "results.json"), JSON.stringify({ revision, context, results }, null, 2) + "\n");
  console.log(`Results: ${join(run, "results.json")}`);
  return results;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const args = process.argv.slice(2);
    if (args.includes("--help") || args.includes("-h")) {
      console.log("Usage: scripts/build-all.sh [--hosts FILE] [--host NAME ...] [--dry-run]\nDefault: .hosts (ignored). Build all configured hosts concurrently, or select named entries.\nOnly --prepare runs; verified binaries and receipts are collected. No publication.\nRemote jobs keep running if SSH disconnects; their log paths are printed.");
    } else {
      let config = ".hosts";
      let dryRun = false;
      const selected = [];
      for (let i = 0; i < args.length; i++) {
        if (args[i] === "--dry-run") dryRun = true;
        else if (["--hosts", "--host"].includes(args[i]) && args[i + 1] && !args[i + 1].startsWith("-")) {
          if (args[i] === "--hosts") config = args[++i]; else selected.push(args[++i]);
        } else throw new Error("Use --help for build-all arguments");
      }
      const hosts = selectHosts(JSON.parse(await readFile(resolve(repository, config), "utf8")), selected);
      const results = await buildAll(hosts, { dryRun });
      if (results.some((result) => !result.success)) process.exitCode = 1;
    }
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
