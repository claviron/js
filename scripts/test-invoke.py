#!/usr/bin/env python3
"""Real bidirectional invokes on an isolated Link, alongside a framework-free JavaScript HTTP application."""
import argparse
import datetime
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import shutil

ROOT=Path(__file__).resolve().parents[1]

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build",action="store_true",help="reuse a completed workspace all-features/all-targets build")
    args=parser.parse_args()
    if not args.skip_build:
        subprocess.run(["cargo","build","--workspace","--all-features","--all-targets"],cwd=ROOT,check=True)
    target=Path(os.environ.get("CARGO_TARGET_DIR",ROOT/"target"))
    if not target.is_absolute(): target=ROOT/target
    library="liborbit_js_addon.dylib" if sys.platform=="darwin" else "liborbit_js_addon.so"
    fleet="i"+secrets.token_hex(3); invoke_fleet=fleet+"v"
    for name in (fleet,invoke_fleet):
        if any(Path("/dev/shm").glob("orbit-"+name+"-*")): raise RuntimeError("Acceptance namespace already exists")
    record=ROOT/".local"/("invoke-"+fleet);record.mkdir(parents=True)
    addon=record/"orbit.node"
    shutil.copyfile(target/"debug"/library,addon)
    children=[];logs=[]
    def start(label,command):
        stderr=(record/(label+".stderr")).open("w");logs.append(stderr)
        child=subprocess.Popen(command,cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True,bufsize=1)
        children.append(child);(record/"children.json").write_text(json.dumps([child.pid for child in children])+"\n")
        return child
    def line(child):
        value=child.stdout.readline().strip()
        if not value: raise RuntimeError(f"Acceptance process closed output; inspect {record}")
        print(value,flush=True);return value
    def expect(child,expected):
        value=line(child)
        if value!=expected: raise RuntimeError(f"Expected {expected}, got {value}; inspect {record}")
    def send(child,value): child.stdin.write(value+"\n");child.stdin.flush()
    try:
        peer=start("peer",[str(target/"debug/examples/invoke"),fleet,invoke_fleet])
        ready=line(peer).split()
        if ready[0]!="PEER_READY":raise RuntimeError("Missing peer geometry")
        app=start("js",["node","tests/invoke-native.js",str(addon),fleet,invoke_fleet,ready[1]])
        expect(app,f"INVOKE_READY {app.pid}")
        send(peer,"run");expect(peer,"BASIC_CHECKS_PASSED")
        send(app,"outgoing-basic");expect(app,"OUTGOING_BASIC_PASSED")
        send(peer,"outgoing-done");expect(peer,"INBOUND_SENT");expect(app,"INBOUND_HELD 1")
        send(peer,"cancel-inbound");expect(peer,"CALLER_CANCELLED")
        send(app,"finish-hold");expect(app,"HOLD_FINISHED");send(peer,"inbound-finished")
        expect(peer,"GENERATION_INBOUND_SENT");expect(app,"INBOUND_HELD 1")
        send(peer,"stop-application");expect(peer,"STOP_CHECKS_PASSED");expect(app,"CONTROL STOP 1")
        send(peer,"start-application");expect(peer,"START_SENT");expect(app,"CONTROL START 2")
        send(peer,"start-observed");expect(peer,"START_CHECKS_PASSED")
        send(app,"finish-late");expect(app,"LATE_DROPPED")
        send(peer,"outgoing-cancel");send(app,"call-gate");expect(app,"OUTGOING_STARTED");expect(peer,"NATIVE_GATE_HELD")
        send(app,"cancel-outgoing");expect(app,"OUTGOING_CANCELLED")
        send(peer,"finish-gate");expect(peer,"NATIVE_GATE_FINISHED")
        send(peer,"outgoing-generation");send(app,"call-gate");expect(app,"OUTGOING_STARTED");expect(peer,"NATIVE_GATE_HELD")
        send(peer,"restart-application");expect(peer,"RESTART_SENT");expect(app,"CONTROL RESTART 3")
        send(app,"assert-cancelled");expect(app,"GENERATION_CANCELLED")
        send(peer,"finish-gate");expect(peer,"NATIVE_GATE_FINISHED");expect(peer,"GENERATION_CHECKS_PASSED")
        send(peer,"drain");expect(peer,"NATIVE_GATE_HELD")
        send(app,"close-application");expect(app,"APP_WITHDRAWN")
        send(peer,"release-drain");expect(peer,"INVOKE_CHECKS_PASSED");expect(app,"APP_CLOSED")
        send(app,"stop");expect(app,"INVOKE_DRAINED")
        if app.wait()!=0: raise RuntimeError("JavaScript drain failed")
        send(peer,"stop");expect(peer,"PEER_DRAINED")
        if peer.wait()!=0: raise RuntimeError("Peer drain failed")
    finally:
        for child in children:
            if child.poll() is None:child.terminate()
        for child in children:child.wait()
        for log in logs:log.close()
        for name in (fleet,invoke_fleet):
            with (record/(name+".cleanup")).open("w") as cleanup:
                subprocess.run(["orbit","clear",name,"--all","--yes"],stdout=cleanup,check=True)
    (record/"result.json").write_text(json.dumps({"passed":True,"application_fleet":fleet,"invoke_fleet":invoke_fleet,
        "checks":["bidirectional JavaScript/Rust calls", "binary payload/operation preservation", "typed protocol errors and rejections",
                  "accepted call and response bounds", "caller cancellation is not remote rollback", "generation cancellation in both directions",
                  "ignored late reply", "HTTP progresses while invoke is saturated", "stable invoke endpoint across generations",
                  "graceful nested outgoing/incoming drain", "all owned processes exit"],
        "completed_utc":datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+"\n")
    print("INVOKE_ACCEPTANCE_PASSED "+str(record),flush=True)

if __name__=="__main__":main()
