#!/usr/bin/env python3
"""Exercise plugin discovery, HTTP execution/lifecycle, shutdown and deterministic replay.
Use --mixed to include IS and Passive. Synthetic passive orders intentionally never fill.
"""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

root = Path(__file__).resolve().parents[1]
launcher = root / "engine/build/install/engine/bin/engine"
mixed = "--mixed" in sys.argv
with tempfile.TemporaryDirectory(prefix="strategy-smoke-") as directory:
    work = Path(directory)
    plugins = work / "plugins"
    plugins.mkdir()
    for module in (["example-twap", "example-is", "example-passive"] if mixed else ["example-twap"]):
        jar = root / module / "build/libs" / (module + "-1.0-SNAPSHOT.jar")
        (plugins / jar.name).write_bytes(jar.read_bytes())
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    token = secrets.token_hex(24)
    environment = dict(os.environ, STRATEGY_ENGINE_TOKEN=token)
    with (work / "server.log").open("w+") as log:
        server = subprocess.Popen([str(launcher), "demo", str(work / "journal"), str(plugins), str(port)], env=environment, stdout=log, stderr=log)
        def request(path, form=None):
            headers = {"Authorization": "Bearer " + token}
            data = None
            if form is not None:
                data = urllib.parse.urlencode(form).encode()
                headers.update({"Content-Type": "application/x-www-form-urlencoded", "Idempotency-Key": secrets.token_hex(16)})
            with urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}" + path, data=data, headers=headers), timeout=3) as response:
                return json.load(response)
        def specification(parent_id, strategy, now):
            form = {"id": parent_id, "account": 1, "instrument": 1, "side": "BUY", "quantity": 1000, "limit": 110010, "startNanos": now, "endNanos": now + 5_000_000_000, "strategy": strategy, "param.aheadSeconds": 5}
            if strategy == "twap":
                form["param.mode"] = "aggressive"
            elif strategy == "is":
                form.update({"param.referenceDestinations": "1", "param.behindSeconds": 0})
            return form
        try:
            for _ in range(100):
                try:
                    state = request("/api/state")
                    if state["ready"]:
                        break
                except (OSError, ValueError):
                    pass
                if server.poll() is not None:
                    raise RuntimeError("Demo exited during startup")
                time.sleep(0.1)
            else:
                raise RuntimeError("Demo did not become ready")
            strategies = ["twap", "is", "passive"] if mixed else ["twap"]
            available = request("/api/strategies")
            assert all(name in available for name in strategies), available
            for name in strategies:
                assert request("/api/strategies/" + name)["parameters"]
            for parent_id, strategy in enumerate(strategies, 1):
                state = request("/api/state")
                request("/api/parents", specification(parent_id, strategy, state["logicalNanos"]))
                recoveries = 0
                for _ in range(300):
                    state = request("/api/state")
                    parent = next((p for p in state["parents"] if p["id"] == parent_id), None)
                    expected = "CANCELLED" if strategy == "passive" else "FILLED"
                    if parent and parent["state"] == expected:
                        break
                    if parent and parent["state"] == "PAUSED" and parent["reserved"] == 0 and parent["children"] == 0 and parent["diagnostic"] == "Callback latency limit exceeded" and recoveries < 3:
                        recoveries += 1
                        version = parent["version"]
                        form = specification(parent_id, strategy, state["logicalNanos"])
                        form["version"] = version
                        request(f"/api/parents/{parent_id}/amend", form)
                        for _ in range(30):
                            updated = next(p for p in request("/api/state")["parents"] if p["id"] == parent_id)
                            if updated["version"] > version:
                                break
                            time.sleep(0.1)
                        else:
                            raise RuntimeError("Amendment did not complete")
                        request(f"/api/parents/{parent_id}/resume", {"version": updated["version"]})
                        print(strategy, "resumed callback safety pause", recoveries, flush=True)
                        time.sleep(0.2)
                    time.sleep(0.1)
                else:
                    raise RuntimeError("Unexpected strategy outcome: " + json.dumps(state))
                if strategy == "is":
                    assert parent["benchmark"] is not None
                    assert parent["executionCosts"]["venueShortfall"] is not None
                if strategy == "passive":
                    assert parent["venueFilled"] == 0 and parent["reserved"] == 0
                    assert parent["strategyState"] == "EXPIRED_INCOMPLETE"
                    orders = [e["command"] for e in request("/api/events") if e["parentId"] == parent_id and e["type"] == "COMMAND" and e["command"]["type"] == "NEW"]
                    assert orders and all(c["postOnly"] and c["timeInForce"] in ["DAY", "GTC"] for c in orders)
                print(strategy, "HTTP smoke:", expected, flush=True)
        finally:
            server.terminate()
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
                raise RuntimeError("Graceful shutdown timed out")
        subprocess.run([str(launcher), "replay", str(work / "journal"), str(plugins)], check=True)
