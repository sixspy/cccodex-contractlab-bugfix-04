# -*- coding: utf-8 -*-
"""停用切断失败测：先红后绿。

口径（停用切断簿·甲路径）：校验前条件更新检查启用态，停用则拒绝零写入；
停用与入站撞车须排队，后到只记停用争用；偏离切断簿则拒绝并记切断偏了；
历史批次禁止删除；版本目录停用态与入站列表、对账账册一致。
"""
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

import app as app_module
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _client():
    c = TestClient(app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return c, {"Authorization": f"Bearer {tok}"}


def _counts():
    with app_module.connect() as c:
        b = c.execute("SELECT COUNT(*) n FROM ingress_batches").fetchone()["n"]
        u = c.execute("SELECT COUNT(*) n FROM buffers").fetchone()["n"]
    return b, u


def _events(c, h):
    r = c.get("/api/cutoff/events", headers=h)
    assert r.status_code == 200
    return r.json()["items"]


def test_seeded_disabled_formal_rejected_zero_writes():
    """种子停用正式版 v2：入站拒绝且零写入。"""
    c, h = _client()
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "库存入站", "payload_fields": "sku,qty"})
    assert r.status_code == 409 and "停用" in r.json()["detail"]
    assert _counts() == (0, 0)


def test_disable_then_ingress_cutoff_zero_writes():
    """停用正式版后：再入站一律拒绝零写入，目录/对账一致，历史批次保留。"""
    c, h = _client()
    ok = c.post("/api/ingress/validate", headers=h,
                json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert ok.json()["result"] == "通过"
    before = _counts()
    d = c.post("/api/contracts/1/disable", headers=h)
    assert d.status_code == 200 and d.json()["status"] == "停用"
    watermark = d.json()["watermark"]
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert r.status_code == 409 and "停用" in r.json()["detail"]
    assert _counts() == before  # 零写入
    items = c.get("/api/ingress", headers=h).json()["items"]
    assert [b["id"] for b in items] == [ok.json()["batch_id"]]  # 历史批次禁止删除
    assert all(b["id"] <= watermark for b in items if b["contract_version"] == "v3")
    ctr = {x["version"]: x["status"] for x in c.get("/api/contracts", headers=h).json()["items"]}
    assert ctr["v3"] == "停用"  # 版本目录停用态
    rec = c.get("/api/reconcile", headers=h).json()
    assert rec["ingress_total"] == before[0] and rec["aligned"] is True
    kinds = [e["kind"] for e in _events(c, h)]
    assert "停用切断" in kinds


def test_disable_is_conditional_update():
    """启用态条件更新：仅启用→停用一次命中；重复/非正式/不存在都不得放行。"""
    c, h = _client()
    assert c.post("/api/contracts/1/disable", headers=h).status_code == 200
    again = c.post("/api/contracts/1/disable", headers=h)
    assert again.status_code == 409  # 已停用，条件更新零命中
    shadow = c.post("/api/contracts/2/disable", headers=h)
    assert shadow.status_code == 409  # 影子轨不走正式切断
    missing = c.post("/api/contracts/999/disable", headers=h)
    assert missing.status_code == 404
    with app_module.connect() as conn:
        shadow_status = conn.execute("SELECT status FROM contracts WHERE id=2").fetchone()["status"]
    assert shadow_status == "试跑"


def test_contention_latecomer_only_records():
    """停用与入站撞车：后到只记停用争用，不写本操作结果。"""
    c, h = _client()
    assert app_module.CUTOFF_RW.acquire_write()  # 模拟停用值班在办
    try:
        r = c.post("/api/ingress/validate", headers=h,
                   json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
        assert r.status_code == 409 and "停用争用" in r.json()["detail"]
    finally:
        app_module.CUTOFF_RW.release_write()
    assert _counts() == (0, 0)  # 争用零写入
    assert app_module.CUTOFF_RW.acquire_read()
    try:
        d = c.post("/api/contracts/1/disable", headers=h)
        assert d.status_code == 409 and "停用争用" in d.json()["detail"]
    finally:
        app_module.CUTOFF_RW.release_read()
    kinds = [e["kind"] for e in _events(c, h)]
    assert kinds.count("停用争用") >= 2


def test_ledger_deviation_rejected_and_recorded(tmp_path, monkeypatch):
    """偏离停用切断簿：拒绝并记切断偏了。"""
    c, h = _client()
    bad = tmp_path / "ledger.md"
    bad.write_text("# 停用切断簿\n路径：乙\n", encoding="utf-8")
    monkeypatch.setattr("app.CUTOFF_LEDGER", bad)
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert r.status_code == 409 and "切断偏了" in r.json()["detail"]
    d = c.post("/api/contracts/1/disable", headers=h)
    assert d.status_code == 409 and "切断偏了" in d.json()["detail"]
    monkeypatch.setattr("app.CUTOFF_LEDGER", tmp_path / "missing.md")
    r2 = c.post("/api/ingress/validate", headers=h,
                json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert r2.status_code == 409
    assert _counts() == (0, 0)
    kinds = [e["kind"] for e in _events(c, h)]
    assert kinds.count("切断偏了") >= 2


def test_disable_ingress_race_final_consistency():
    """停用与入站并发：无论谁先到，水位后零通过，不变量可复现。"""
    c, h = _client()
    results = []

    def hammer(n):
        cc, hh = _client()
        for _ in range(n):
            resp = cc.post("/api/ingress/validate", headers=hh,
                           json={"subject": "订单入站",
                                 "payload_fields": "order_id,amount,currency"})
            results.append((resp.status_code, resp.json()))

    threads = [threading.Thread(target=hammer, args=(25,)) for _ in range(3)]
    for t in threads:
        t.start()
    d = c.post("/api/contracts/1/disable", headers=h)
    for t in threads:
        t.join()
    assert d.status_code in (200, 409)
    if d.status_code == 200:
        watermark = d.json()["watermark"]
    else:  # 撞车被排队：停用争用，随后补停
        assert "停用争用" in d.json()["detail"]
        watermark = c.post("/api/contracts/1/disable", headers=h).json()["watermark"]
    for status, body in results:
        assert status in (200, 409)
        if status == 200:
            assert body["result"] == "通过" and body["batch_id"] <= watermark
        else:
            assert "停用" in body["detail"] or "争用" in body["detail"]
    with app_module.connect() as conn:
        bad = conn.execute(
            "SELECT COUNT(*) n FROM ingress_batches WHERE result='通过' AND contract_version='v3' AND id>?",
            (watermark,)).fetchone()["n"]
    assert bad == 0
    rec = c.get("/api/reconcile", headers=h).json()
    assert rec["aligned"] is True
    assert any(e["kind"] == "停用切断" for e in _events(c, h))
