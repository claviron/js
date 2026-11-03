import { realpathSync } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { nativeTarget, nativeTargets } from "../native.js";
import { withPackage } from "./package.js";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
const cpuFor = (target) => target.includes("arm64") ? "generic" : "x86-64";

// Inspect architecture and shared-library shape without loading a foreign binary.
export function validateBinary(target, bytes) {
  let valid;
  if (target === "darwin-arm64") {
    valid = bytes.length >= 32 && bytes.readUInt32LE(0) === 0xfeedfacf &&
      bytes.readUInt32LE(4) === 0x0100000c && bytes.readUInt32LE(12) === 6;
  } else {
    const machine = target.includes("arm64") ? 183 : 62;
    // System V (0) is ambiguous: Linux addons commonly use it. Require the
    // FreeBSD ABI marker rather than accepting a Linux ELF as a cross build.
    const osabis = target.startsWith("freebsd-") ? [9] : [0, 3];
    valid = bytes.length >= 64 &&
      bytes.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46])) &&
      bytes[4] === 2 && bytes[5] === 1 && osabis.includes(bytes[7]) &&
      bytes.readUInt16LE(16) === 3 && bytes.readUInt16LE(18) === machine;
  }
  if (!valid) throw new Error("not a shared library for the declared platform/architecture");
}

// Bind collected artifacts to this package's source, not a host or an output path.
export async function sourceDigest(root = repository) {
  const hash = createHash("sha256");
  async function include(relative) {
    const bytes = await readFile(join(root, relative));
    hash.update(relative).update("\0").update(String(bytes.length)).update("\0").update(bytes);
  }
  async function directory(relative) {
    const entries = await readdir(join(root, relative), { withFileTypes: true });
    for (const entry of entries.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0)) {
      const path = `${relative}/${entry.name}`;
      if (entry.isDirectory()) await directory(path);
      else if (entry.isFile()) await include(path);
    }
  }
  for (const path of ["package.json", "Cargo.toml", "Cargo.lock", "addon/Cargo.toml",
    "addon/build.rs", "index.js", "binding.js", "invoke.js", "session.js", "framework.js", "native.js",
    "scripts/release.js", "scripts/package.js", "scripts/stage-native.js", "scripts/publish.sh",
    "scripts/prepare-freebsd.sh", "scripts/freebsd/Dockerfile",
    "scripts/freebsd/cc", "scripts/lib/cargo.sh"]) {
    await include(path);
  }
  await directory("src");
  await directory("addon/src");
  return hash.digest("hex");
}

export async function releaseContext(root = repository) {
  const { name, version } = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  return { name, version, source: await sourceDigest(root) };
}

export async function readPrepared(target, directory, context) {
  const bytes = await readFile(join(directory, "orbit.node"));
  validateBinary(target, bytes);
  const receipt = JSON.parse(await readFile(join(directory, "release.json"), "utf8"));
  if (receipt.format !== 1 || receipt.package !== context.name || receipt.version !== context.version ||
    receipt.target !== nativeTargets[target] || receipt.profile !== "release" ||
    receipt.cpu !== cpuFor(target) || receipt.napi !== 8 ||
    receipt.source !== context.source || receipt.sha256 !== digest(bytes)) {
    throw new Error(`preparation does not match ${context.name}@${context.version} and its sources/binary; run --prepare on its host`);
  }
  return { bytes, receipt };
}

export async function stagePrepared(root, target, bytes, receipt) {
  if (!Object.hasOwn(nativeTargets, target)) throw new Error(`Unknown target: ${target}`);
  const directory = join(root, "prebuilds", target);
  await mkdir(directory, { recursive: true });
  const suffix = `${process.pid}.${randomUUID()}.tmp`;
  const binaryTemporary = join(directory, `orbit.node.${suffix}`);
  const receiptTemporary = join(directory, `release.json.${suffix}`);
  try {
    await writeFile(binaryTemporary, bytes);
    await writeFile(receiptTemporary, `${JSON.stringify(receipt, null, 2)}\n`);
    // Replace inodes; a process may still have an earlier binary mapped.
    // A check between these renames fails closed on a mismatched checksum.
    await rename(binaryTemporary, join(directory, "orbit.node"));
    await rename(receiptTemporary, join(directory, "release.json"));
  } finally {
    await rm(binaryTemporary, { force: true });
    await rm(receiptTemporary, { force: true });
  }
}

export async function inspectPackage(root = repository) {
  const context = await releaseContext(root);
  return Promise.all(Object.keys(nativeTargets).map(async (target) => {
    try {
      await readPrepared(target, join(root, "prebuilds", target), context);
      return { target, ready: true };
    } catch (error) {
      return { target, ready: false, error: error.message };
    }
  }));
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { cwd: repository, stdio: "inherit", ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} failed (${result.signal ?? result.status})`);
}

async function prepare(tripleOverride) {
  const target = tripleOverride === undefined ? nativeTarget() :
    Object.keys(nativeTargets).find((key) => nativeTargets[key] === tripleOverride);
  if (!target) throw new Error(`Unsupported preparation target: ${tripleOverride}`);
  const triple = nativeTargets[target];
  const cpu = cpuFor(target);
  const context = await releaseContext();
  console.log(`Preparing ${context.name}@${context.version} ${target}: release, target-cpu=${cpu} (local Rust flags are replaced).`);
  // Override developer CPU/feature flags for every Rust dependency as well as the addon.
  run("cargo", ["build", "--manifest-path", "addon/Cargo.toml", "--locked", "--release", "--target", triple], {
    env: { ...process.env, CARGO_ENCODED_RUSTFLAGS: `-C\x1ftarget-cpu=${cpu}` },
  });
  if (context.source !== await sourceDigest()) throw new Error("sources changed during preparation; run --prepare again");
  const library = target === "darwin-arm64" ? "liborbit_js_addon.dylib" : "liborbit_js_addon.so";
  const artifact = join(process.env.CARGO_TARGET_DIR || "target", triple, "release", library);
  const bytes = await readFile(resolve(repository, artifact));
  validateBinary(target, bytes);
  const receipt = { format: 1, package: context.name, version: context.version, target: triple,
    profile: "release", cpu, napi: 8, source: context.source, sha256: digest(bytes) };
  await stagePrepared(repository, target, bytes, receipt);
  console.log(`Prepared prebuilds/${target}/ for ${context.version}; native/ is unchanged. No package was published.`);
}

export async function checkPackage(root = repository) {
  const results = await inspectPackage(root);
  for (const result of results) {
    if (result.ready) console.log(`${result.target}: ready`);
    else console.error(`${result.target}: ${result.error}`);
  }
  return results.every((result) => result.ready);
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [action, ...extra] = process.argv.slice(2);
    const cross = action === "--prepare" && extra.length === 2 && extra[0] === "--target";
    if ((!cross && extra.length) || !["--prepare", "--publish", "--dry-run"].includes(action)) {
      throw new Error("Use scripts/publish.sh --prepare [--target RUST_TARGET] | --dry-run | --publish");
    }
    if (action === "--prepare") await prepare(cross ? extra[1] : undefined);
    else if (!await checkPackage()) process.exitCode = 1;
    else await withPackage(repository, (cwd) => {
      run("npm", ["publish", "--access", "public", ...(action === "--dry-run" ? ["--dry-run"] : [])], { cwd });
    });
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
