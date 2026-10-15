"""Isolated native Campaign HTTP/scheduler/restart check; invoked by the Mac canary.

The only operation is a local JS fixture. This does not certify a model or the
business effects of production jobs. All SQL runs in the caller's disposable DB.
"""
import datetime
import json
import time
import urllib.error
import urllib.request

from probe_test_phase_lifecycle import require


def check(service, sql, root, report):
    def get():
        status, body = service.request("campaigns/native-operation-canary")
        require(status == 200, "Campaign read failed")
        return body["campaign"]

    def update(state):
        current = get()
        current.update(status=state, expected_revision=current["revision"])
        request = urllib.request.Request(
            f"http://127.0.0.1:{service.port}/api/campaigns/native-operation-canary",
            data=json.dumps(current).encode(), method="PUT",
            headers={"Authorization": "Bearer " + service.token,
                     "Content-Type": "application/json"})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=10) as response:
            require(response.status == 200, "Campaign CAS failed")

    def count():
        return int(sql("SELECT count(*) FROM routine_runs WHERE campaign_id='native-operation-canary'"))

    def wait_node(expected):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            current = get()
            if current["nodes"][0]["status"] == expected:
                return current
            time.sleep(0.5)
        raise RuntimeError("Campaign node did not reach " + expected)

    sql("INSERT INTO agents(id,name,provider) VALUES('native-canary','Native canary','qwen')")
    sql("INSERT INTO routines(id,agent_id,script_ref,name,status,execution_strategy,schedule,timeout_secs,max_retries) "
        "VALUES('native-source','native-canary','probe.js','Canary source','paused','fresh','0 0 * * *',60,0)")
    updated = json.loads(sql("SELECT to_jsonb(updated_at) FROM routines WHERE id='native-source'"))
    payload = dict(id="native-operation-canary", title="Native operation canary", status="paused", round=1,
                   nodes=[dict(id="operation", title="Local fixture", stage="operate", status="pending",
                               round=1, assignee="native-canary", provider="qwen", details="Local fixture only")])
    require(service.request("campaigns", payload)[0] == 201, "Campaign creation failed")
    adoption = dict(routine_id="native-source", node_id="operation", expected_revision=1,
                    expected_routine_updated_at=updated,
                    next_due_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    path = "campaigns/native-operation-canary/operations"
    require(service.request(path, adoption, authenticated=False)[0] == 401, "adoption accepted unauthenticated caller")
    require(service.request(path, adoption)[0] == 200, "atomic adoption failed")
    require(service.request(path, adoption)[0] == 409, "duplicate adoption accepted")
    require(count() == 0, "paused Campaign admitted work")
    update("active")
    wait_node("completed")
    require(count() == 1, "Campaign did not admit exactly one occurrence")
    require(sql("SELECT status FROM routines WHERE id='native-source'") == "paused", "original Routine resumed")
    update("paused")
    sql("UPDATE routines SET next_due_at=NOW()-INTERVAL '1 second' WHERE id='native-source'")
    service.stop()
    service.start()
    time.sleep(3)
    require(count() == 1, "restart replayed a paused Campaign")
    update("active")
    deadline = time.monotonic() + 30
    while count() < 2 and time.monotonic() < deadline:
        time.sleep(0.5)
    wait_node("completed")
    require(count() == 2, "second Campaign occurrence was lost or duplicated")
    update("paused")
    (root / "routines/probe.js").write_text(
        'agentdesk.routines.register({name:"Campaign canary",tick(){throw new Error("owned fixture failure");}});')
    sql("UPDATE routines SET next_due_at=NOW()-INTERVAL '1 second' WHERE id='native-source'")
    service.stop()
    service.start()
    update("active")
    failed = wait_node("blocked")
    require(failed["status"] == "paused" and count() == 3, "failure was not held")
    service.stop()
    service.start()
    time.sleep(3)
    require(count() == 3, "restart replayed a failed operation")
    status, operations = service.request(path)
    require(status == 200 and operations["definition"]["source_status"] == "paused", "binding lost after restart")
    report["native_operations"] = {"profile": operations["profile"], "runs": operations["runs"],
                                   "source_status": "paused", "model_executed": False}
    report["checks"].extend(["campaign_adoption_auth_and_duplicate_guard", "real_scheduler_admission",
                             "campaign_pause_survives_restart", "recurring_execution_preserves_history",
                             "failed_operation_holds_across_restart"])
