import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { chmod, copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { buildAll, selectHosts } from "../scripts/build-all.js";
import { readPrepared, releaseContext } from "../scripts/release.js";

const repository = dirname(dirname(fileURLToPath(import.meta.url)));

test("host selection rejects SSH options and duplicate target builders", () => {
  const host = { ssh: "build.example.test", repository: "/srv/source/orbit", target: "linux-x64-gnu" };
  assert.equal(selectHosts({ hosts: { linux: host } }, ["linux"])[0].ssh, host.ssh);
  assert.throws(() => selectHosts({ hosts: { linux: host } }, ["missing"]), /Unknown host/);
  assert.throws(() => selectHosts({ hosts: { linux: { ...host, ssh: "-oProxyCommand=command" } } }), /SSH destination/);
  assert.throws(() => selectHosts({ hosts: { first: host, second: host } }), /one builder per target/);
});

test("concurrent builders finish independently and collect only matching artifacts", { timeout: 10000 }, async (t) => {
  const temporary = await mkdtemp(join(tmpdir(), "orbit-build-all-"));
  const root = join(temporary, "source with spaces");
  const commands = join(temporary, "commands");
  const events = join(temporary, "events");
  await mkdir(commands);
  await writeFile(events, "");
  execFileSync("git", ["clone", "--quiet", "--no-hardlinks", repository, root]);
  for (const file of [".gitignore", "scripts/build-worker.js", "scripts/release.js", "invoke.js"]) {
    await copyFile(join(repository, file), join(root, file));
  }
  // A clean builder clone has no lockfile; only the release checkout owns it.
  execFileSync("git", ["rm", "--cached", "--ignore-unmatch", "Cargo.lock"], { cwd: root });
  const dependencyLock = "fixture dependency snapshot\n";
  await writeFile(join(root, "Cargo.lock"), dependencyLock);
  await writeFile(join(root, "scripts/publish.sh"), "#!/usr/bin/env bash\nexec node scripts/fixture-prepare.mjs\n");
  await writeFile(join(root, "scripts/fixture-prepare.mjs"), `
import { createHash } from 'node:crypto';
import { releaseContext, stagePrepared } from './release.js';
const context=await releaseContext();
const bytes=Buffer.alloc(64);
bytes.set([0x7f,0x45,0x4c,0x46,2,1]); bytes.writeUInt16LE(3,16); bytes.writeUInt16LE(62,18);
await stagePrepared(process.cwd(),'linux-x64-gnu',bytes,{format:1,package:context.name,version:context.version,
target:'x86_64-unknown-linux-gnu',profile:'release',cpu:'x86-64',napi:8,source:context.source,
sha256:createHash('sha256').update(bytes).digest('hex')});
`);
  execFileSync("git", ["add", ".gitignore", "scripts/publish.sh", "scripts/fixture-prepare.mjs", "scripts/build-worker.js", "scripts/release.js", "invoke.js"], { cwd: root });
  execFileSync("git", ["-c", "commit.gpgsign=false", "-c", "user.name=Example", "-c", "user.email=build@example.test",
    "commit", "--quiet", "-m", "Prepare isolated build fixture"], { cwd: root });
  const ssh = join(commands, "ssh");
  await writeFile(ssh, `#!${process.execPath}\n` + `
const {appendFile,readFile}=require('node:fs/promises');
const {watch}=require('node:fs');
const {spawn}=require('node:child_process');
(async()=>{
const host=process.argv.at(-2); const file=process.env.ORBIT_TEST_EVENTS;
await appendFile(file,JSON.stringify({host,pid:process.pid})+'\\n');
await new Promise((resolve,reject)=>{
  const watcher=watch(file,check);
  async function check(){try{
    if((await readFile(file,'utf8')).trim().split('\\n').length===2){watcher.close();resolve();}
  }catch(e){watcher.close();reject(e);}}
  check();
});
if(host==='refused')return process.exit(17);
const child=spawn('bash',['-c',process.argv.at(-1)],{stdio:'inherit'});
child.on('error',()=>process.exit(1));child.on('close',code=>process.exit(code??1));
})().catch(e=>{console.error(e);process.exit(1)});
`);
  await chmod(ssh, 0o755);
  const rsync = join(commands, "rsync");
  await writeFile(rsync, `#!${process.execPath}\n` + `
const{copyFileSync}=require('node:fs'); const{join}=require('node:path');
const source=process.argv.at(-2).split(':').slice(1).join(':');
for(const name of ['orbit.node','release.json'])copyFileSync(join(source,name),join(process.argv.at(-1),name));
`);
  await chmod(rsync, 0o755);
  const originalPath = process.env.PATH;
  const originalEvents = process.env.ORBIT_TEST_EVENTS;
  t.after(async () => {
    process.env.PATH = originalPath;
    if (originalEvents === undefined) delete process.env.ORBIT_TEST_EVENTS; else process.env.ORBIT_TEST_EVENTS = originalEvents;
    for (const line of (await readFile(events, "utf8")).trim().split("\n").filter(Boolean)) {
      try { process.kill(JSON.parse(line).pid); } catch {}
    }
    await rm(temporary, { recursive: true, force: true });
  });
  process.env.PATH = `${commands}:${originalPath}`;
  process.env.ORBIT_TEST_EVENTS = events;
  const config = { hosts: {
    refused: { ssh: "refused", repository: root, target: "freebsd-x64", cache: join(temporary, "refused") },
    accepted: { ssh: "accepted", repository: root, target: "linux-x64-gnu", cache: join(temporary, "cache") },
  } };
  const results = await buildAll(selectHosts(config), { root });
  assert.deepEqual(results.map((result) => result.success), [false, true]);
  const { receipt } = await readPrepared("linux-x64-gnu", join(root, "prebuilds/linux-x64-gnu"), await releaseContext(root));
  assert.equal(receipt.target, "x86_64-unknown-linux-gnu");
  assert.equal(await readFile(join(root, "Cargo.lock"), "utf8"), dependencyLock);
  assert.equal(execFileSync("git", ["ls-files", "Cargo.lock"], { cwd: root, encoding: "utf8" }), "");
  assert.deepEqual((await readFile(events, "utf8")).trim().split("\n").map((line) => JSON.parse(line).host).sort(), ["accepted", "refused"]);
  assert.deepEqual(execFileSync("git", ["status", "--porcelain"], { cwd: root, encoding: "utf8" }).trim(), "");
});
