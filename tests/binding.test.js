import assert from "node:assert/strict";
import { test } from "node:test";
import { createOrbit, Link } from "../binding.js";

test("custom binding is injected without loading the platform addon", async () => {
  const calls = [];
  const binding = {
    async join(options) {
      calls.push(options);
      return { lane: 7, running: true, async close() { calls.push("closed"); } };
    },
  };
  const orbit = createOrbit(binding);
  const link = await orbit.join({ fleet: "example", name: "custom", frameworks: [] });
  assert.ok(link instanceof Link);
  assert.equal(link.lane, 7);
  assert.equal(link.running, true);
  await link.close();
  assert.deepEqual(calls, [{ fleet: "example", name: "custom", frameworks: [] }, "closed"]);
});

test("independent custom bindings do not share native state", async () => {
  const first = createOrbit({ join: async () => ({ lane: 1 }) });
  const second = createOrbit({ join: async () => ({ lane: 2 }) });
  const options = { fleet: "example", name: "custom", frameworks: [] };
  assert.equal((await first.join(options)).lane, 1);
  assert.equal((await second.join(options)).lane, 2);
});

test("invalid custom binding fails before joining", () => {
  for (const binding of [undefined, null, {}, { join: true }]) {
    assert.throws(() => createOrbit(binding), /must export join/);
  }
});

test("custom binding carries streamed requests and replies through the shared adapter", async () => {
  const finished = Promise.withResolvers();
  const terminal = Promise.withResolvers();
  const chunks = [Buffer.from("from Rust")];
  const written = [];
  let head;
  const exchange = {
    method: "POST", url: "http://example.test/echo", headers: [], hasBody: true,
    async read() { return chunks.shift() ?? null; },
    respond(status, headers) { head = { status, headers }; },
    async write(chunk) { written.push(Buffer.from(chunk)); },
    async waitClosed() { return terminal.promise; },
    async end() { terminal.resolve(false); finished.resolve(); },
    async abort() { finished.reject(new Error("Unexpected abort")); },
  };
  let incoming = exchange;
  const orbit = createOrbit({
    async join() {
      return {
        async next() { const next = incoming; incoming = null; return next; },
        async nextControl() { return null; },
      };
    },
  });
  await orbit.serve({ fleet: "example", name: "custom", frameworks: [] }, async (request) => {
    assert.equal(new URL(request.url).pathname, "/echo");
    return new Response(await request.text(), { status: 201 });
  });
  await finished.promise;
  assert.equal(head.status, 201);
  assert.equal(Buffer.concat(written).toString(), "from Rust");
});

test("admission is explicit, validated before join and passed without inventing limits", async () => {
  const calls = [];
  const orbit = createOrbit({ async join(options) { calls.push(options); return {}; } });
  for (const capacity of [
    { maxInFlight: 0, maxQueued: 2 },
    { maxInFlight: 1.5, maxQueued: 2 },
    { maxInFlight: 3, maxQueued: -1 },
    { maxInFlight: 1 },
  ]) await assert.rejects(orbit.join({ fleet: "example", name: "app", frameworks: [], ...capacity }));
  assert.equal(calls.length, 0);
  await orbit.join({ fleet: "example", name: "app", frameworks: [], maxInFlight: 2, maxQueued: 0 });
  assert.equal(calls[0].maxInFlight, 2);
  assert.equal(calls[0].maxQueued, 0);
  await assert.rejects(
    orbit.join({ fleet: "example", name: "app", maxWaiting: 3 }),
    /maxWaiting was replaced by maxQueued/,
  );
  await orbit.join({ fleet: "example", name: "app", frameworks: [] });
  assert.equal(calls[1].maxInFlight, undefined);
});
