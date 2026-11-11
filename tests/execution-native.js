// Explicit acceptance check after building/staging the ready-to-load addon.
// Each child must terminate cleanly, joining the owned runtime thread.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
const script = `import {runtimeInfo} from './index.js'; console.log(JSON.stringify(runtimeInfo()));`;
for (const [execution, threads] of [["current", "1"], ["multi", "1"], ["multi", "2"]]) {
  const child = spawnSync(process.execPath, ["--input-type=module", "-e", script], {
    cwd: new URL("..", import.meta.url), encoding: "utf8",
    env: {...process.env, ORBIT_JS_EXECUTION: execution, ORBIT_JS_WORKER_THREADS: threads},
  });
  assert.equal(child.status, 0, child.stderr);
  assert.deepEqual(JSON.parse(child.stdout), {execution, workerThreads: Number(threads)});
}
const invalid = spawnSync(process.execPath, ["--input-type=module", "-e", script], {
  cwd: new URL("..", import.meta.url), encoding: "utf8",
  env: {...process.env, ORBIT_JS_EXECUTION: "current", ORBIT_JS_WORKER_THREADS: "2"},
});
assert.notEqual(invalid.status, 0);
assert.match(invalid.stderr, /current has one thread/);
console.log("native executor flavor, validation and process cleanup passed");
