import { spawn } from "node:child_process";
import { once } from "node:events";
import { openSync, closeSync } from "node:fs";
import { mkdir, mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { inflateSync } from "node:zlib";

async function command(program, args, options = {}) {
  const child = spawn(program, args, { stdio: "inherit", ...options });
  const [code, signal] = await once(child, "close");
  if (code !== 0) throw new Error(`${program} failed (${signal ?? code})`);
}

async function build(job) {
  const request = JSON.parse(await readFile(join(job, "request.json"), "utf8"));
  const lock = join(request.cache, `prepare-${request.target}.lock`);
  let locked = false;
  let result;
  try {
    await mkdir(lock, { mode: 0o700 });
    locked = true;
    await writeFile(join(lock, "owner.json"), JSON.stringify({ pid: process.pid, job }));
    const source = join(job, "source");
    const release = await import(pathToFileURL(join(source, "scripts/release.js")));
    const { nativeTargets } = await import(pathToFileURL(join(source, "native.js")));
    const context = await release.releaseContext(source);
    for (const key of ["name", "version", "source"]) {
      if (context[key] !== request.context[key]) throw new Error(`Committed release ${key} differs from the requested build`);
    }
    const env = { ...process.env, CARGO_TARGET_DIR: join(request.cache, "target") };
    if (request.jobs !== undefined) env.CARGO_BUILD_JOBS = String(request.jobs);
    const args = ["scripts/publish.sh", "--prepare", "--target", nativeTargets[request.target]];
    if (request.nice === undefined) await command("bash", args, { cwd: source, env });
    else await command("nice", ["-n", String(request.nice), "bash", ...args], { cwd: source, env });
    const directory = join(source, "prebuilds", request.target);
    const { bytes, receipt } = await release.readPrepared(request.target, directory, context);
    await release.stagePrepared(request.cache, request.target, bytes, receipt);
    result = { success: true, target: request.target, directory, revision: request.revision,
      version: context.version, source: context.source, sha256: receipt.sha256 };
  } catch (error) {
    result = { success: false, error: error.message, target: request.target, job };
  } finally {
    if (locked) await rm(lock, { recursive: true });
  }
  await writeFile(join(job, "result.json"), JSON.stringify(result, null, 2) + "\n");
  if (!result.success) process.exitCode = 1;
}

async function launch(request) {
  const cache = request.cache ?? join(homedir(), ".cache", "claviron-js-builder");
  await mkdir(join(cache, "jobs"), { recursive: true, mode: 0o700 });
  const job = await mkdtemp(join(cache, "jobs", `${request.revision.slice(0, 8)}-${request.target}-`));
  const source = join(job, "source");
  await command("git", ["clone", "--quiet", "--no-hardlinks", "--no-checkout", request.repository, source]);
  await command("git", ["-C", source, "checkout", "--quiet", "--detach", request.revision]);
  // The dependency snapshot is local to the release, not stored in Git.
  await writeFile(join(source, "Cargo.lock"), inflateSync(Buffer.from(request.dependencyLock, "base64")));
  const worker = join(source, "scripts/build-worker.js");
  await writeFile(join(job, "request.json"), JSON.stringify({ ...request, cache }), { mode: 0o600 });
  const log = openSync(join(job, "build.log"), "a", 0o600);
  const child = spawn(process.execPath, [worker, "--run", job], {
    detached: true, stdio: ["ignore", log, log],
  });
  closeSync(log);
  console.log("ORBIT_BUILD_STARTED " + JSON.stringify({ job, pid: child.pid }));
  const [code, signal] = await once(child, "close");
  let result;
  try { result = JSON.parse(await readFile(join(job, "result.json"), "utf8")); }
  catch { result = { success: false, error: `Worker ended without a result (${signal ?? code})`, job }; }
  console.log("ORBIT_BUILD_RESULT " + JSON.stringify({ ...result, job }));
  if (code !== 0 || !result.success) process.exitCode = 1;
}

try {
  if (process.argv[1] === "-") await launch(JSON.parse(Buffer.from(process.argv[2], "base64").toString("utf8")));
  else if (process.argv[2] === "--run") await build(resolve(process.argv[3]));
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
