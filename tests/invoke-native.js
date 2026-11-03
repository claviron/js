// Real native invoke correctness fixture: one JS process serves HTTP and invokes.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { createInterface } from "node:readline";
import { createOrbit, InvokeReply } from "../binding.js";

const [addon, fleet, invokeFleet, responseBound] = process.argv.slice(2);
const bound = Number(responseBound);
const orbit = createOrbit(createRequire(import.meta.url)(resolve(addon)));
const link = await orbit.join({ fleet, name: "demo", frameworks: [] });
const options = {
  fleet: invokeFleet, service: "js", pendingCalls: 4, acceptedCalls: 1,
  operationBytes: 32, requestBytes: 1024, responseBytes: bound,
  routes: { native: "rust", missing: "absent", ambiguous: "dupe" },
};
await assert.rejects(link.invocations({ ...options, fleet }), /must differ/);
const invokes = await link.invocations(options);
const signal = (message) => console.log(message);
const payload = (...values) => new Uint8Array(values);
const code = (result, expected) => { assert.equal(result.code, expected); return result.payload; };
let held, heldRequest, outgoing, generation = 1;
let running = true;
link.onLifecycle((action) => {
  if (action === "restart" || (action === "start" && !running)) generation++;
  running = action !== "stop";
  signal(`CONTROL ${action.toUpperCase()} ${generation}`);
});
const servingInvokes = invokes.serve(async (request) => {
  switch (request.operation) {
    case "echo": case "echo-λ": return InvokeReply.success(request.payload);
    case "generation": return InvokeReply.success(new TextEncoder().encode(String(request.generation)));
    case "reject": return InvokeReply.rejected(payload(0, 255, 7));
    case "fail": throw new Error("Fixture handler failure");
    case "too-big": return InvokeReply.success(new Uint8Array(bound + 1));
    case "hold":
      held = Promise.withResolvers(); heldRequest = request;
      signal(`INBOUND_HELD ${request.generation}`); return held.promise;
    case "drain": {
      const result = await invokes.call("native", "gate", request.payload).completion;
      return InvokeReply.success(code(result, "SUCCESS"));
    }
    default: return InvokeReply.unsupported();
  }
});
const servingHttp = link.serve(() => new Response("http-ready"));
signal(`INVOKE_READY ${process.pid}`);
const input = createInterface({ input: process.stdin });
try {
  for await (const command of input) {
    if (command === "stop") break;
    switch (command) {
      case "outgoing-basic": {
        const submitted = payload(0, 255, 9);
        const call = invokes.call("native", "echo", submitted); submitted.fill(99);
        assert.equal(call.generation, 1n);
        assert.deepEqual([...code(await call.completion, "SUCCESS")], [0, 255, 9]);
        code(await invokes.call("unknown", "echo", payload()).completion, "UNKNOWN_TARGET");
        code(await invokes.call("missing", "echo", payload()).completion, "UNAVAILABLE");
        code(await invokes.call("ambiguous", "echo", payload()).completion, "AMBIGUOUS_TARGET");
        code(await invokes.call("native", "echo", new Uint8Array(1025)).completion, "PAYLOAD_TOO_LARGE");
        assert.deepEqual([...code(await invokes.call("native", "reject", payload()).completion, "REJECTED")], [0, 255]);
        signal("OUTGOING_BASIC_PASSED"); break;
      }
      case "finish-hold":
        assert.equal(heldRequest.signal.aborted, false, "Caller cancellation is not remote rollback");
        held.resolve(InvokeReply.success(payload(7))); signal("HOLD_FINISHED"); break;
      case "finish-late":
        assert.equal(heldRequest.signal.aborted, true);
        held.resolve(InvokeReply.success(payload(8))); signal("LATE_DROPPED"); break;
      case "call-gate":
        outgoing = invokes.call("native", "gate", payload(0, 255));
        // Observe rejection immediately; the command loop verifies the outcome later.
        outgoing.completion.catch(() => {});
        signal("OUTGOING_STARTED"); break;
      case "cancel-outgoing":
        assert.equal(outgoing.cancel(), true);
        await assert.rejects(outgoing.completion, { name: "AbortError" });
        signal("OUTGOING_CANCELLED"); break;
      case "assert-cancelled":
        await assert.rejects(outgoing.completion, { name: "AbortError" });
        signal("GENERATION_CANCELLED"); break;
      case "close-application": {
        const closing = link.close(); signal("APP_WITHDRAWN");
        await closing; await Promise.all([servingHttp, servingInvokes]);
        signal("APP_CLOSED"); break;
      }
      default: throw new Error(`Unknown fixture command: ${command}`);
    }
  }
} finally {
  input.close(); await link.close();
  await Promise.all([servingHttp, servingInvokes]);
}
signal("INVOKE_DRAINED");
