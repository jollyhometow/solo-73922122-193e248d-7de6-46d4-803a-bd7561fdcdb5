"""运行批次 API 集成测试与并发/持久化测试。"""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.runroutes import get_store
from app.runstore import RunStore

client = TestClient(app)

RUN = "RUN-2026-0456"


@pytest.fixture
def store(tmp_path):
    """每个测试用独立的 SQLite 文件，通过依赖覆盖注入。"""
    s = RunStore(str(tmp_path / "runs.db"))
    app.dependency_overrides[get_store] = lambda: s
    yield s
    app.dependency_overrides.clear()
    s.close()


def rec(rid, barcode="BC-100", ts="2026-11-14T08:00:00Z", **results):
    return {
        "id": rid,
        "barcode": barcode,
        "timestamp": ts,
        "results": results or {"GLU": 5.2},
    }


def submit(run_id, idx, total, records):
    return client.put(
        f"/runs/{run_id}/segments/{idx}",
        json={"total_segments": total, "records": records},
    )


def publish(run_id):
    return client.post(f"/runs/{run_id}/publish")


# ---------------------------------------------------------------- 基础提交流程


def test_submit_segment_and_query_status(store):
    resp = submit(RUN, 0, 2, [rec("R-1"), rec("R-2")])
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == RUN
    assert body["segment_index"] == 0
    assert body["total_segments"] == 2
    assert body["state"] == "open"
    assert body["replay"] is False
    assert body["received_segments"] == [0]
    assert body["missing_segments"] == [1]

    status = client.get(f"/runs/{RUN}").json()
    assert status["state"] == "open"
    assert status["received_segments"] == [0]
    assert status["missing_segments"] == [1]
    assert status["received_record_count"] == 2
    assert status["snapshot"] is None


def test_out_of_order_submission_and_publish_ordering(store):
    assert submit(RUN, 1, 2, [rec("R-3", ts="2026-11-14T08:00:30Z")]).status_code == 200
    assert submit(RUN, 0, 2, [rec("R-1"), rec("R-2")]).status_code == 200

    resp = publish(RUN)
    assert resp.status_code == 200
    snap = resp.json()
    assert snap["state"] == "published"
    assert snap["total_segments"] == 2
    assert snap["record_count"] == 3
    assert [s["segment_index"] for s in snap["segments"]] == [0, 1]
    flat = [r["id"] for s in snap["segments"] for r in s["records"]]
    assert flat == ["R-1", "R-2", "R-3"]


def test_empty_segment_counts_as_received(store):
    assert submit(RUN, 0, 2, []).status_code == 200
    assert submit(RUN, 1, 2, [rec("R-1")]).status_code == 200
    resp = publish(RUN)
    assert resp.status_code == 200
    assert resp.json()["record_count"] == 1


# ---------------------------------------------------------------- 幂等与拒收


def test_same_segment_same_content_replay_is_idempotent(store):
    first = submit(RUN, 0, 2, [rec("R-1"), rec("R-2")])
    second = submit(RUN, 0, 2, [rec("R-1"), rec("R-2")])
    assert first.status_code == second.status_code == 200
    assert first.json()["replay"] is False
    assert second.json()["replay"] is True
    assert client.get(f"/runs/{RUN}").json()["received_record_count"] == 2


def test_replay_with_equivalent_content_is_idempotent(store):
    """键序不同、时区写法不同但逻辑相同的内容仍判为同内容。"""
    submit(RUN, 0, 2, [rec("R-1", ts="2026-11-14T08:00:00Z", GLU=5.2, K=4.1)])
    equivalent = {
        "id": "R-1",
        "barcode": "BC-100",
        "timestamp": "2026-11-14T16:00:00+08:00",  # 同一时刻
        "results": {"K": 4.1, "GLU": 5.2},  # 键序不同
    }
    resp = submit(RUN, 0, 2, [equivalent])
    assert resp.status_code == 200
    assert resp.json()["replay"] is True


def test_same_segment_different_content_rejected_and_data_unchanged(store):
    submit(RUN, 0, 2, [rec("R-1"), rec("R-2")])
    resp = submit(RUN, 0, 2, [rec("R-1"), rec("R-9")])
    assert resp.status_code == 409
    assert resp.json()["error"] == "segment_conflict"
    # 已收数据不变：仍是 R-1、R-2
    assert submit(RUN, 1, 2, []).status_code == 200
    snap = publish(RUN).json()
    flat = [r["id"] for s in snap["segments"] for r in s["records"]]
    assert flat == ["R-1", "R-2"]


def test_cross_segment_duplicate_record_id_rejected(store):
    submit(RUN, 0, 2, [rec("R-1"), rec("R-2")])
    resp = submit(RUN, 1, 2, [rec("R-3"), rec("R-1")])
    assert resp.status_code == 409
    assert resp.json()["error"] == "duplicate_record_id"
    assert resp.json()["duplicate_ids"] == ["R-1"]
    # 第 1 段未入库，仍可续传
    status = client.get(f"/runs/{RUN}").json()
    assert status["missing_segments"] == [1]


def test_total_segments_mismatch_rejected(store):
    submit(RUN, 0, 2, [rec("R-1")])
    resp = submit(RUN, 1, 3, [rec("R-2")])
    assert resp.status_code == 409
    assert resp.json()["error"] == "total_segments_mismatch"
    assert resp.json()["established_total_segments"] == 2


def test_submit_after_publish_rejected(store):
    submit(RUN, 0, 1, [rec("R-1")])
    assert publish(RUN).status_code == 200
    # 同内容重传也被拒：运行已封存
    resp = submit(RUN, 0, 1, [rec("R-1")])
    assert resp.status_code == 409
    assert resp.json()["error"] == "run_already_published"


# ---------------------------------------------------------------- 发布语义


def test_publish_with_missing_segments_returns_missing_and_stays_open(store):
    submit(RUN, 0, 3, [rec("R-1")])
    submit(RUN, 2, 3, [rec("R-3")])
    resp = publish(RUN)
    assert resp.status_code == 409
    assert resp.json()["error"] == "run_incomplete"
    assert resp.json()["missing_segments"] == [1]
    # 保留续传资格：补上缺段后发布成功
    assert submit(RUN, 1, 3, [rec("R-2")]).status_code == 200
    assert publish(RUN).status_code == 200


def test_publish_retry_returns_original_snapshot(store):
    submit(RUN, 0, 2, [rec("R-1")])
    submit(RUN, 1, 2, [rec("R-2")])
    first = publish(RUN)
    second = publish(RUN)
    assert first.status_code == second.status_code == 200
    assert first.content == second.content  # 逐字节一致

    # 已发布运行的状态查询也返回同一快照
    status = client.get(f"/runs/{RUN}").json()
    assert status["state"] == "published"
    assert status["snapshot"] == first.json()


def test_publish_unknown_run_404(store):
    resp = publish("NOPE")
    assert resp.status_code == 404
    assert resp.json()["error"] == "run_not_found"


def test_get_unknown_run_404(store):
    resp = client.get("/runs/NOPE")
    assert resp.status_code == 404
    assert resp.json()["error"] == "run_not_found"


# ---------------------------------------------------------------- 请求校验


def test_segment_index_must_be_below_total(store):
    resp = submit(RUN, 2, 2, [rec("R-1")])
    assert resp.status_code == 422
    assert resp.json()["error"] == "segment_index_out_of_range"


def test_segment_index_and_total_bounds(store):
    assert submit(RUN, 8, 8, []).status_code == 422  # 段号最大 7
    assert submit(RUN, 0, 9, []).status_code == 422  # 总段数最大 8
    assert submit(RUN, 0, 0, []).status_code == 422  # 总段数最小 1
    assert submit(RUN, -1, 2, []).status_code == 422


def test_more_than_12_records_rejected(store):
    records = [rec(f"R-{i}") for i in range(13)]
    assert submit(RUN, 0, 1, records).status_code == 422


def test_duplicate_ids_within_segment_rejected(store):
    assert submit(RUN, 0, 1, [rec("R-1"), rec("R-1")]).status_code == 422


def test_non_finite_values_rejected(store):
    for bad in ("NaN", "Infinity", "-Infinity"):
        payload = (
            '{"total_segments": 1, "records": [{"id": "R-1", "barcode": "B",'
            ' "timestamp": "2026-11-14T08:00:00Z", "results": {"GLU": '
            + bad
            + "}}]}"
        )
        resp = client.put(
            f"/runs/{RUN}/segments/0",
            content=payload,
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 422, bad


def test_naive_timestamp_and_empty_fields_rejected(store):
    bad_ts = rec("R-1", ts="2026-11-14T08:00:00")
    assert submit(RUN, 0, 1, [bad_ts]).status_code == 422
    assert submit(RUN, 0, 1, [rec("")]).status_code == 422
    assert submit(RUN, 0, 1, [rec("R-1", barcode="")]).status_code == 422
    no_results = rec("R-1")
    no_results["results"] = {}
    assert submit(RUN, 0, 1, [no_results]).status_code == 422


def test_unknown_fields_rejected(store):
    payload = {"total_segments": 1, "records": [rec("R-1")], "operator": "x"}
    resp = client.put(f"/runs/{RUN}/segments/0", json=payload)
    assert resp.status_code == 422
    bad_rec = rec("R-1")
    bad_rec["note"] = "x"
    assert submit(RUN, 0, 1, [bad_rec]).status_code == 422


def test_run_id_charset_rejected(store):
    assert submit("bad id", 0, 1, []).status_code == 422
    assert submit("中文运行", 0, 1, []).status_code == 422
    assert client.get("/runs/bad id").status_code == 422


# ---------------------------------------------------------------- 持久化与并发


def test_persistence_across_store_restart(tmp_path):
    db = str(tmp_path / "runs.db")
    s1 = RunStore(db)
    s1.submit_segment(RUN, 0, 2, [{"id": "R-1", "barcode": "B", "timestamp": "t",
                                   "results": {"GLU": 5.0}}])
    s1.close()

    s2 = RunStore(db)
    view = s2.get_run(RUN)
    assert view is not None
    assert view["received_segments"] == [0]
    assert view["missing_segments"] == [1]
    s2.close()


def test_concurrent_submit_and_publish_never_exposes_partial(store):
    """并发补传与发布：任何成功的发布都必须包含全部 4 段。"""
    total = 4
    records = {i: [rec(f"R-{i}-1"), rec(f"R-{i}-2")] for i in range(total)}
    results = {"submit": [], "publish": []}
    lock = threading.Lock()

    def do_submit(i):
        resp = submit(RUN, i, total, records[i])
        with lock:
            results["submit"].append(resp.status_code)

    def do_publish():
        resp = publish(RUN)
        with lock:
            results["publish"].append(resp)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(do_submit, i) for i in range(total)]
        futures += [pool.submit(do_publish) for _ in range(3)]
        for f in futures:
            f.result()

    # 每个段只由一个线程提交，发布要成功必须先齐 4 段 => 提交全部 200
    assert sorted(results["submit"]) == [200] * total
    # 并发期间的发布要么 409 缺段，要么 200 且含全部记录；成功的快照必须一致
    oks = [r for r in results["publish"] if r.status_code == 200]
    for r in results["publish"]:
        assert r.status_code in (200, 409)
        if r.status_code == 409:
            assert r.json()["missing_segments"]
    for r in oks:
        assert r.json()["record_count"] == 8
        assert r.content == oks[0].content
    # 段已齐，最终发布必定成功且快照完整
    final_publish = publish(RUN)
    assert final_publish.status_code == 200
    assert final_publish.json()["record_count"] == 8
    for r in oks:
        assert r.content == final_publish.content
    # 终态：已发布，快照完整
    final = client.get(f"/runs/{RUN}").json()
    assert final["state"] == "published"
    assert final["snapshot"]["record_count"] == 8


def test_concurrent_identical_replays_all_succeed(store):
    def do_submit(_):
        return submit(RUN, 0, 2, [rec("R-1"), rec("R-2")])

    with ThreadPoolExecutor(max_workers=6) as pool:
        resps = list(pool.map(do_submit, range(6)))
    assert all(r.status_code == 200 for r in resps)
    assert sum(1 for r in resps if r.json()["replay"]) == 5
    assert client.get(f"/runs/{RUN}").json()["received_record_count"] == 2
