import assert from "node:assert/strict";
import { test } from "node:test";
import { createOrbit } from "../binding.js";

function fixture(overrides = {}) {
  const terminal = Promise.withResolvers();
  const finished = Promise.withResolvers();
  const first = Promise.withResolvers();
  const calls = { writes: [], observers: 0, ends: 0 };
  const exchange = {
    method: "GET", url: "https://example.test/events", headers: [], hasBody: false,
    async waitClosed() {
      calls.observers++;
      try { return await terminal.promise; }
      finally { calls.observers--; }
    },
    respond() {},
    async write(value) { calls.writes.push(value); first.resolve(); },
    async end() { calls.ends++; terminal.resolve(false); finished.resolve(); },
    async abort() { terminal.resolve(true); finished.resolve(); },
    ...overrides,
  };
  let pending = exchange;
  const orbit = createOrbit({ async join() { return {
    async next() { const next = pending; pending = null; return next; },
    async nextControl() { return null; },
  }; } });
  return {
    calls, exchange, terminal, finished: finished.promise, first: first.promise,
    serve: (handler) => orbit.serve({ fleet: "example", name: "events", frameworks: [] }, handler),
  };
}

for (const firstRecord of [false, true]) {
  test(`peer departure cancels an idle body ${firstRecord ? "after one record" : "before its first record"}`, async () => {
    const f = fixture();
    const started = Promise.withResolvers();
    let signal;
    let cancellations = 0;
    const serving = f.serve((request) => {
      signal = request.signal;
      return new Response(new ReadableStream({
        start(controller) {
          if (firstRecord) controller.enqueue(new TextEncoder().encode("data: one\n\n"));
          started.resolve();
        },
        cancel() { cancellations++; },
      }), { headers: { "content-type": "text/event-stream" } });
    });
    await started.promise;
    if (firstRecord) await f.first;
    f.terminal.resolve(true);
    await f.finished;
    await serving;
    assert.ok(signal.aborted);
    assert.equal(cancellations, 1);
    assert.equal(f.calls.observers, 0);
    assert.equal(f.calls.ends, 0);
  });
}

test("peer departure ends a pending handler and cancels its late body", async () => {
  const f = fixture();
  const started = Promise.withResolvers();
  const response = Promise.withResolvers();
  const cancelled = Promise.withResolvers();
  let signal;
  const serving = f.serve((request) => {
    signal = request.signal; started.resolve(); return response.promise;
  });
  await started.promise;
  f.terminal.resolve(true);
  await f.finished;
  await serving;
  assert.ok(signal.aborted);
  response.resolve(new Response(new ReadableStream({ cancel: cancelled.resolve })));
  await cancelled.promise;
  assert.equal(f.calls.observers, 0);
});

test("departure already observed at subscription does not retain a handler", async () => {
  const f = fixture();
  f.terminal.resolve(true);
  await f.serve(() => new Promise(() => {}));
  await f.finished;
  assert.equal(f.calls.observers, 0);
});

for (const body of [null, "complete"]) {
  test(`normal ${body === null ? "empty" : "finite"} response ends without request abort`, async () => {
    const f = fixture();
    let signal;
    await f.serve((request) => { signal = request.signal; return new Response(body); });
    await f.finished;
    assert.equal(signal.aborted, false);
    assert.equal(f.calls.ends, 1);
    assert.equal(f.calls.observers, 0);
  });
}

test("request upload EOF leaves a valid response usable", async () => {
  const f = fixture({ method: "POST", hasBody: true, async read() { return null; } });
  let signal;
  await f.serve(async (request) => {
    signal = request.signal;
    assert.equal(await request.text(), "");
    return new Response("still valid");
  });
  await f.finished;
  assert.equal(signal.aborted, false);
  assert.equal(f.calls.ends, 1);
});

test("failed response construction retires the exchange and its observer", async () => {
  const f = fixture();
  await f.serve(() => ({ body: { getReader() { throw new Error("invalid body"); } } }));
  await f.finished;
  assert.equal(f.calls.observers, 0);
});

test("body output waits for native reply-head publication", async () => {
  const entered = Promise.withResolvers();
  const released = Promise.withResolvers();
  const ctx = fixture({
    async respond() {
      entered.resolve();
      await released.promise;
    },
  });
  const serving = ctx.serve(() => new Response("ok"));
  await entered.promise;
  assert.deepEqual(ctx.calls.writes, []);
  assert.equal(ctx.calls.ends, 0);
  released.resolve();
  await ctx.finished;
  await serving;
  assert.equal(Buffer.concat(ctx.calls.writes).toString(), "ok");
  assert.equal(ctx.calls.ends, 1);
});
