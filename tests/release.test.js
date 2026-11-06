import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { chmod, copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { nativeTarget, nativeTargets } from "../native.js";
import { inspectPackage, sourceDigest, validateBinary } from "../scripts/release.js";
import { withPackage } from "../scripts/package.js";

const repository = dirname(dirname(fileURLToPath(import.meta.url)));
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");

function binary(target) {
  const bytes = Buffer.alloc(64);
  if (target === "darwin-arm64") {
    bytes.writeUInt32LE(0xfeedfacf, 0);
    bytes.writeUInt32LE(0x0100000c, 4);
    bytes.writeUInt32LE(6, 12);
  } else {
    bytes.set([0x7f, 0x45, 0x4c, 0x46, 2, 1]);
    bytes[7] = target.startsWith("freebsd-") ? 9 : 0;
    bytes.writeUInt16LE(3, 16);
    bytes.writeUInt16LE(target.includes("arm64") ? 183 : 62, 18);
  }
  return bytes;
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "orbit-release-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  for (const directory of ["src", "addon/src", "scripts/lib", "scripts/freebsd", "npm"]) {
    await mkdir(join(root, directory), { recursive: true });
  }
  for (const file of ["native.js", "scripts/package.js", "scripts/release.js", "scripts/stage-native.js", "scripts/publish.sh", "scripts/sync.js", "scripts/lib/cargo.sh", "scripts/prepare-freebsd.sh", "scripts/freebsd/Dockerfile", "scripts/freebsd/cc"]) {
    await copyFile(join(repository, file), join(root, file));
  }
  for (const file of ["Cargo.toml", "Cargo.lock", "addon/Cargo.toml", "addon/build.rs", "src/lib.rs", "addon/src/lib.rs", "index.js", "binding.js", "invoke.js", "session.js", "framework.js"]) {
    await writeFile(join(root, file), "fixture\n");
  }
  const published = JSON.parse(await readFile(join(repository, "package.json"), "utf8"));
  await writeFile(join(root, "package.json"), JSON.stringify({ ...published, name: "example-addon", version: "1.0.0" }));
  await writeFile(join(root, "README.md"), "Repository build and customization guide\n");
  await writeFile(join(root, "LICENSE"), "Fixture license\n");
  await writeFile(join(root, "npm/README.md"), "Consumer installation guide\n");
  return root;
}

test("FreeBSD preparation rejects an otherwise matching Linux x64 ELF", () => {
  assert.throws(() => validateBinary("freebsd-x64", binary("linux-x64-gnu")), /platform/);
  validateBinary("freebsd-x64", binary("freebsd-x64"));
  assert.throws(() => validateBinary("linux-x64-gnu", binary("freebsd-x64")), /platform/);
});

async function prepared(root, target) {
  const bytes = binary(target);
  const directory = join(root, "prebuilds", target);
  await mkdir(directory, { recursive: true });
  await writeFile(join(directory, "orbit.node"), bytes);
  await writeFile(join(directory, "release.json"), JSON.stringify({
    format: 1, package: "example-addon", napi: 8, target: nativeTargets[target], version: "1.0.0", profile: "release",
    cpu: target.includes("arm64") ? "generic" : "x86-64",
    source: await sourceDigest(root), sha256: hash(bytes),
  }));
}

test("reports every missing target and a wrong architecture together", async (t) => {
  const root = await fixture(t);
  await prepared(root, "darwin-arm64");
  await prepared(root, "linux-arm64-gnu");
  await writeFile(join(root, "prebuilds/linux-arm64-gnu/orbit.node"), binary("linux-x64-gnu"));
  const results = await inspectPackage(root);
  assert.equal(results.length, 5);
  assert.deepEqual(results.filter((entry) => entry.ready).map((entry) => entry.target), ["darwin-arm64"]);
  assert.match(results.find((entry) => entry.target === "linux-arm64-gnu").error, /architecture/);
  assert.equal(results.filter((entry) => !entry.ready).length, 4);
});

test("rejects a replaced binary, nonportable receipt and stale sources", async (t) => {
  const root = await fixture(t);
  for (const target of Object.keys(nativeTargets)) await prepared(root, target);
  assert.ok((await inspectPackage(root)).every((entry) => entry.ready));
  await writeFile(join(root, "prebuilds/linux-x64-gnu/orbit.node"), Buffer.concat([binary("linux-x64-gnu"), Buffer.from("changed")]));
  const receiptPath = join(root, "prebuilds/freebsd-arm64/release.json");
  const receipt = JSON.parse(await readFile(receiptPath, "utf8"));
  await writeFile(receiptPath, JSON.stringify({ ...receipt, cpu: "native" }));
  assert.equal((await inspectPackage(root)).filter((entry) => !entry.ready).length, 2);
  await writeFile(join(root, "src/lib.rs"), "changed source\n");
  assert.ok((await inspectPackage(root)).every((entry) => !entry.ready));
});

test("an ignored dependency lock still binds artifacts to the release snapshot", async (t) => {
  const root = await fixture(t);
  await prepared(root, "linux-x64-gnu");
  assert.ok((await inspectPackage(root)).find((entry) => entry.target === "linux-x64-gnu").ready);
  await writeFile(join(root, "Cargo.lock"), "different dependency snapshot\n");
  assert.ok(!(await inspectPackage(root)).find((entry) => entry.target === "linux-x64-gnu").ready);
  await rm(join(root, "Cargo.lock"));
  await assert.rejects(sourceDigest(root), /cargo generate-lockfile before release commands/);
});

test("prepare overrides native flags; publication requires every prepared target and an explicit action", async (t) => {
  const root = await fixture(t);
  const commands = join(root, "commands");
  await mkdir(commands);
  const log = join(root, "commands.jsonl");
  const cargo = join(commands, "cargo");
  await writeFile(cargo, `#!${process.execPath}\n` + `
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
appendFileSync(process.env.COMMAND_LOG, JSON.stringify({ command: 'cargo', args: process.argv.slice(2), flags: process.env.CARGO_ENCODED_RUSTFLAGS }) + '\\n');
const target = process.argv[process.argv.indexOf('--target') + 1];
const directory = join(process.env.CARGO_TARGET_DIR, target, 'release');
mkdirSync(directory, { recursive: true });
const binaries = ${JSON.stringify(Object.fromEntries(Object.entries(nativeTargets).map(([key, triple]) => [triple, [...binary(key)]])))};
writeFileSync(join(directory, target.includes('apple') ? 'libclaviron_js_addon.dylib' : 'libclaviron_js_addon.so'), Buffer.from(binaries[target]));
`);
  const npm = join(commands, "npm");
  await writeFile(npm, `#!${process.execPath}\n` + `
import { appendFileSync, readFileSync } from 'node:fs';
const manifest = JSON.parse(readFileSync('package.json', 'utf8'));
appendFileSync(process.env.COMMAND_LOG, JSON.stringify({ command: 'npm', args: process.argv.slice(2), cwd: process.cwd(), readme: readFileSync('README.md', 'utf8'), scripts: manifest.scripts ?? null }) + '\\n');
if (process.env.FAIL_NPM) process.exit(1);
`);
  await chmod(cargo, 0o755);
  await chmod(npm, 0o755);
  const env = { ...process.env, PATH: `${commands}:${process.env.PATH}`, COMMAND_LOG: log,
    CARGO_TARGET_DIR: join(root, "output"), CARGO_ENCODED_RUSTFLAGS: "-C\x1ftarget-cpu=native" };
  const invoke = (...args) => spawnSync("bash", [join(root, "scripts/publish.sh"), ...args], { env, encoding: "utf8" });
  assert.equal(invoke().status, 0);
  assert.equal(invoke("--publish", "--prepare").status, 2);
  const local = join(root, "native", nativeTarget());
  await mkdir(local, { recursive: true });
  await writeFile(join(local, "orbit.node"), "local build stays untouched");
  const preparation = invoke("--prepare");
  assert.equal(preparation.status, 0, preparation.stderr);
  assert.equal(await readFile(join(local, "orbit.node"), "utf8"), "local build stays untouched");
  let calls = (await readFile(log, "utf8")).trim().split("\n").map(JSON.parse);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].command, "cargo");
  assert.equal(calls[0].flags, `-C\x1ftarget-cpu=${process.arch === "arm64" ? "generic" : "x86-64"}`);
  assert.ok(calls[0].args.includes("--release"));
  const cross = invoke("--prepare", "--target", "x86_64-unknown-freebsd");
  assert.equal(cross.status, 0, cross.stderr);
  const crossReceipt = JSON.parse(await readFile(join(root, "prebuilds/freebsd-x64/release.json"), "utf8"));
  assert.equal(crossReceipt.target, "x86_64-unknown-freebsd");
  assert.equal(crossReceipt.cpu, "x86-64");
  assert.equal(crossReceipt.source, await sourceDigest(root));
  assert.equal(invoke("--prepare", "--target", "unsupported").status, 1);
  assert.equal(invoke("--publish", "--target", "x86_64-unknown-freebsd").status, 2);
  assert.equal(invoke("--publish").status, 1);
  assert.equal(invoke("--dry-run").status, 1);
  for (const target of Object.keys(nativeTargets)) await prepared(root, target);
  assert.equal(invoke("--dry-run").status, 0);
  assert.equal(invoke("--publish").status, 0);
  calls = (await readFile(log, "utf8")).trim().split("\n").map(JSON.parse);
  assert.equal(calls[1].flags, "-C\x1ftarget-cpu=x86-64");
  assert.ok(calls[1].args.includes("x86_64-unknown-freebsd"));
  assert.deepEqual(calls.slice(2).map(({ command, args }) => ({ command, args })), [
    { command: "npm", args: ["publish", "--access", "public", "--dry-run"] },
    { command: "npm", args: ["publish", "--access", "public"] },
  ]);
  const failed = spawnSync("bash", [join(root, "scripts/publish.sh"), "--publish"], {
    env: { ...env, FAIL_NPM: "1" }, encoding: "utf8",
  });
  assert.equal(failed.status, 1);
  calls = (await readFile(log, "utf8")).trim().split("\n").map(JSON.parse);
  for (const call of calls.slice(2)) {
    assert.notEqual(call.cwd, root);
    assert.equal(call.readme, "Consumer installation guide\n");
    assert.equal(call.scripts, null);
    await assert.rejects(readFile(join(call.cwd, "README.md")), { code: "ENOENT" });
  }
  assert.equal(await readFile(join(root, "README.md"), "utf8"), "Repository build and customization guide\n");
});


test("sync verifies the active release, reports missing files and preserves rejected targets", async (t) => {
  const root = await fixture(t);
  const remote = await fixture(t);
  for (const target of ["linux-arm64-gnu", "freebsd-arm64"]) {
    await prepared(remote, target);
    await writeFile(join(remote, "prebuilds", target, "unrelated.txt"), "must not transfer");
  }
  await prepared(root, "freebsd-arm64");
  const receiptPath = join(remote, "prebuilds/freebsd-arm64/release.json");
  const receipt = JSON.parse(await readFile(receiptPath, "utf8"));
  await writeFile(receiptPath, JSON.stringify({ ...receipt, version: "0.9.0" }));
  const commands = join(root, "commands");
  await mkdir(commands);
  const rsync = join(commands, "rsync");
  const log = join(root, "rsync.jsonl");
  await writeFile(rsync, `#!${process.execPath}\n` + `
import { appendFileSync, copyFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
const args = process.argv.slice(2);
appendFileSync(process.env.COMMAND_LOG, JSON.stringify(args) + '\\n');
for (const file of ['orbit.node', 'release.json']) {
 const source = join(args.at(-2), file);
 if (existsSync(source)) copyFileSync(source, join(args.at(-1), file));
}
`);
  await chmod(rsync, 0o755);
  const targets = {};
  for (const target of ["linux-arm64-gnu", "freebsd-arm64", "linux-x64-gnu"]) {
    targets[target] = { source: join(remote, "prebuilds", target) };
  }
  const config = join(root, "sync.json");
  await writeFile(config, JSON.stringify({ targets }));
  const result = spawnSync(process.execPath, [join(root, "scripts/sync.js"), "--config", config], {
    encoding: "utf8", env: { ...process.env, PATH: `${commands}:${process.env.PATH}`, COMMAND_LOG: log },
  });
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stdout, /linux-arm64-gnu: collected and verified/);
  assert.match(result.stderr, /freebsd-arm64: preparation does not match/);
  assert.match(result.stderr, /linux-x64-gnu:.*ENOENT/);
  const inspected = await inspectPackage(root);
  assert.ok(inspected.find((entry) => entry.target === "linux-arm64-gnu").ready);
  assert.ok(inspected.find((entry) => entry.target === "freebsd-arm64").ready);
  await assert.rejects(readFile(join(root, "prebuilds/linux-arm64-gnu/unrelated.txt")), { code: "ENOENT" });
  const calls = (await readFile(log, "utf8")).trim().split("\n").map(JSON.parse);
  assert.equal(calls.length, 3);
  for (const args of calls) {
    assert.ok(args.includes("--include=/orbit.node"));
    assert.ok(args.includes("--include=/release.json"));
    assert.ok(args.includes("--exclude=*"));
  }
});

test("npm package contains portable prebuilds and excludes local native files and receipts", async (t) => {
  const root = await fixture(t);
  const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  const published = JSON.parse(await readFile(join(repository, "package.json"), "utf8"));
  await writeFile(join(root, "package.json"), JSON.stringify({ ...manifest, files: published.files }));
  for (const target of Object.keys(nativeTargets)) {
    await prepared(root, target);
    await mkdir(join(root, "native", target), { recursive: true });
    await writeFile(join(root, "native", target, "orbit.node"), "local native binary");
  }
  await mkdir(join(root, "prebuilds", "unrecognized"), { recursive: true });
  await writeFile(join(root, "prebuilds", "unrecognized", "orbit.node"), "must not publish");
  await writeFile(join(root, "ARCHITECTURE.local.md"), "Private operator notes\n");
  await withPackage(root, async (stage) => {
    const result = spawnSync("npm", ["pack", "--json", "--cache", join(root, "npm-cache")], {
      cwd: stage, encoding: "utf8",
    });
    assert.equal(result.status, 0, result.stderr);
    const packed = JSON.parse(result.stdout);
    // npm 12 keys JSON results by package name; older versions return an array.
    const entry = Object.values(packed)[0];
    const files = entry.files.map((file) => file.path);
    for (const target of Object.keys(nativeTargets)) assert.ok(files.includes(`prebuilds/${target}/orbit.node`));
    assert.ok(!files.some((file) => file.startsWith("native/")));
    assert.ok(!files.some((file) => file.endsWith("release.json")));
    assert.ok(!files.some((file) => file.includes("sync")));
    assert.ok(!files.some((file) => file.includes("unrecognized")));
    assert.ok(!files.some((file) => file.endsWith("local.md") || file.startsWith("scripts/") || file.startsWith("npm/")));
    assert.equal(files.filter((file) => file.endsWith(".node")).length, 5);
    const extracted = spawnSync("tar", ["-xOf", join(stage, entry.filename), "package/README.md"], { encoding: "utf8" });
    assert.equal(extracted.status, 0, extracted.stderr);
    assert.equal(extracted.stdout, await readFile(join(root, "npm/README.md"), "utf8"));
  });
  assert.equal(await readFile(join(root, "README.md"), "utf8"), "Repository build and customization guide\n");
});

test("direct source packaging refuses to publish the repository README", async (t) => {
  const root = await fixture(t);
  const result = spawnSync("npm", ["pack", "--dry-run", "--cache", join(root, "npm-cache")], {
    cwd: root, encoding: "utf8",
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Use scripts\/publish.sh/);
});

test("staging failures clean up and leave the repository README intact", async (t) => {
  const root = await fixture(t);
  for (const target of Object.keys(nativeTargets)) await prepared(root, target);
  const readme = await readFile(join(root, "README.md"), "utf8");
  let directory;
  await assert.rejects(withPackage(root, async (stage) => {
    directory = stage;
    throw new Error("registry unavailable");
  }), /registry unavailable/);
  await assert.rejects(readFile(join(directory, "README.md")), { code: "ENOENT" });
  assert.equal(await readFile(join(root, "README.md"), "utf8"), readme);
});
