import assert from "node:assert/strict";
import { test } from "node:test";
import { Invocations, InvokeCall, InvokeReply, invokeOptions } from "../invoke.js";
import { Link } from "../binding.js";

const options = { fleet: "invokes", service: "worker", pendingCalls: 2, acceptedCalls: 1,
  operationBytes: 32, requestBytes: 256, responseBytes: 1024, routes: { calculator: "math" } };
test("invoke options require every bound and convert only explicit logical routes", () => {
  assert.deepEqual(invokeOptions(options).routes, [{ target: "calculator", service: "math" }]);
  for (const field of ["pendingCalls", "acceptedCalls", "operationBytes", "requestBytes", "responseBytes"]) {
    const invalid = { ...options }; delete invalid[field];
    assert.throws(() => invokeOptions(invalid), /explicit bounded integer/);
  }
  assert.throws(() => invokeOptions({ ...options, routes: { math: "" } }), /nonempty/);
});
function fixture() {
  const terminal = Promise.withResolvers(), incoming = Promise.withResolvers(), accepted = Promise.withResolvers();
  let first = true, replied, aborted = false;
  const native = { operation: "echo", payload: new Uint8Array([0, 255]), generation: 1n, cancelled: false,
    waitClosed: () => terminal.promise,
    reply(code, payload) { replied = { code, payload }; terminal.resolve(false); },
    abort() { aborted = true; terminal.resolve(true); } };
  const link = new Invocations({
    async next() { if (first) { first = false; accepted.resolve(); return native; } return incoming.promise; },
    withdraw() { incoming.resolve(null); }, async close() {},
  });
  return { link, native, terminal, accepted: accepted.promise, replied: () => replied, aborted: () => aborted };
}
test("invoke close drains an accepted response in the same JS event loop", async () => {
  const f = fixture(), entered = Promise.withResolvers(), reply = Promise.withResolvers();
  const serving = f.link.serve((request) => { assert.equal(request.generation, 1n); entered.resolve(); return reply.promise; });
  await entered.promise;
  let closed = false; const closing = f.link.close().then(() => { closed = true; });
  assert.equal(closed, false);
  reply.resolve(InvokeReply.success(new Uint8Array([0, 255])));
  await closing; await serving;
  assert.deepEqual(f.replied(), { code: 0, payload: new Uint8Array([0, 255]) });
  assert.equal(f.aborted(), false);
});
test("generation retirement ends the reply scope without waiting for an ignored handler", async () => {
  const f = fixture(), entered = Promise.withResolvers(), late = Promise.withResolvers(); let signal;
  const serving = f.link.serve((request) => { signal = request.signal; entered.resolve(); return late.promise; });
  await entered.promise; f.terminal.resolve(true);
  await f.link.close(); await serving;
  assert.equal(signal.aborted, true); assert.equal(f.aborted(), true);
  late.resolve(InvokeReply.success(new Uint8Array([1])));
  await late.promise;
  assert.equal(f.replied(), undefined);
});
test("caller AbortSignal cancels native interest and detaches on terminal completion", async () => {
  const controller = new AbortController(), done = Promise.withResolvers(); let cancelled = 0;
  const call = new InvokeCall({ generation: 2n, wait: () => done.promise,
    cancel() { cancelled++; done.reject({ code: "Cancelled" }); return true; } }, controller.signal);
  const rejection = assert.rejects(call.completion, { name: "AbortError" }); controller.abort();
  await rejection; assert.equal(cancelled, 1); assert.equal(call.generation, 2n);
});
test("a pre-aborted call never publishes to the native connection", async () => {
  let calls = 0; const controller = new AbortController(); controller.abort();
  const invokes = new Invocations({ call() { calls++; }, withdraw() {}, async close() {} });
  await assert.rejects(invokes.call("math", "sum", new Uint8Array(), { signal: controller.signal }).completion, { name: "AbortError" });
  assert.equal(calls, 0);
});
test("one application owns invoke children without creating a second JS runtime", async () => {
  const next = Promise.withResolvers(), accepted = Promise.withResolvers(); let nativeClosed = false;
  const link = new Link({ nextControl: () => next.promise,
    async invocations(value) { assert.equal(value.fleet, "invokes"); return { withdraw() {}, async close() { await accepted.promise; } }; },
    async close() { nativeClosed = true; next.resolve(null); } });
  await link.invocations(options);
  let closed = false; const closing = link.close().then(() => { closed = true; });
  assert.equal(nativeClosed, true); assert.equal(closed, false);
  accepted.resolve(); await closing;
});

test("parent close also drains a child whose opening promise resolves late", async () => {
  const opening = Promise.withResolvers(), control = Promise.withResolvers(); let drained = false;
  const link = new Link({ nextControl: () => control.promise,
    invocations: () => opening.promise,
    async close() { control.resolve(null); } });
  const pending = assert.rejects(link.invocations(options), /closed while opening/);
  await link.close();
  opening.resolve({ withdraw() {}, async close() { drained = true; } });
  await pending;
  assert.equal(drained, true);
});
