# -*- coding: utf-8 -*-
"""停用切断复现脚本（修复前后同一脚本，可核顺序）。

用法：/tmp/venv/bin/python scripts/repro_disable_cutoff.py
- 静态探针：对已停用正式版（种子 v2/库存入站）发起入站，看是否放行。
- 并发探针：入站校验压测途中停用正式版 v3（有停用接口走接口，没有则模拟值班主任直接改库），
  随后核对不变量：停用水位之后不得再出现 v3 的「通过」批次。
修复前：并发窗口内停用正式版仍被裁「通过」→ 退出码 1。
修复后：停用切断，后到只记「停用争用」，水位后零通过 → 退出码 0。
"""
import sqlite3, sys, tempfile, threading, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
import app as app_module  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

TMP = Path(tempfile.mkdtemp(prefix="cutoff-repro-"))
app_module.DB_PATH = TMP / "repro.db"
app_module.init_db()
DB = str(app_module.DB_PATH)

def login(c):
    r = c.post("/api/login", json={"username": "admin", "password": "123456"})
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['token']}"}

def sql(sql_text, args=()):
    conn = sqlite3.connect(DB)
    try:
        cur = conn.execute(sql_text, args)
        conn.commit()
        return cur.fetchall()
    finally:
        conn.close()

def main():
    admin = TestClient(app_module.app)
    h = login(admin)
    print(f"[repro] 临时库 {DB}（不动 data/app.db）")

    # ---- 静态探针：对已停用正式版发起入站 ----
    r = admin.post("/api/ingress/validate", headers=h,
                   json={"subject": "库存入站", "payload_fields": "sku,qty"})
    print(f"[静态] 停用正式版 v2 入站 -> HTTP {r.status_code} {r.json()}")
    n = sql("SELECT COUNT(*) FROM ingress_batches")[0][0]
    print(f"[静态] 入站批次总数={n}（拒绝须零写入）")

    v3_id = sql("SELECT id FROM contracts WHERE version='v3'")[0][0]

    # ---- 并发探针：停用与入站撞车 ----
    rounds, per_thread, threads_n = 40, 30, 4
    violations, contentions = [], 0
    for rd in range(1, rounds + 1):
        sql("UPDATE contracts SET status='启用' WHERE version='v3'")  # 值班重置，便于反复撞车
        results, stop = [], False

        def hammer():
            c = TestClient(app_module.app)
            hh = login(c)
            for _ in range(per_thread):
                if stop:
                    return
                resp = c.post("/api/ingress/validate", headers=hh,
                              json={"subject": "订单入站",
                                    "payload_fields": "order_id,amount,currency"})
                body = resp.json()
                results.append((resp.status_code, body.get("result"),
                                body.get("batch_id"), body.get("detail")))

        threads = [threading.Thread(target=hammer) for _ in range(threads_n)]
        for t in threads: t.start()
        time.sleep(0.005)  # 让入站先跑起来，再撞停用
        # 停用正式版：有接口走接口，没有接口模拟值班主任直接改库
        d = admin.post(f"/api/contracts/{v3_id}/disable", headers=h)
        if d.status_code == 404:
            sql("UPDATE contracts SET status='停用' WHERE version='v3'")
            watermark = sql("SELECT COALESCE(MAX(id),0) FROM ingress_batches")[0][0]
            how = "改库"
        else:
            watermark = d.json().get("watermark")
            how = "接口"
        stop = True
        for t in threads: t.join()
        passed_after = [b for b in results if b[1] == "通过" and watermark is not None and b[2] and b[2] > watermark]
        contentions += sum(1 for b in results if b[3] and "争用" in str(b[3]))
        if passed_after:
            violations.append((rd, how, watermark, passed_after[:3]))
            print(f"[并发] 第{rd}轮：停用（{how}）水位={watermark} 后仍裁通过 {len(passed_after)} 笔，如 {passed_after[0]}")
            break
    else:
        print(f"[并发] {rounds}轮撞车未发现水位后通过")

    # ---- 汇总 ----
    bad = sql("SELECT COUNT(*) FROM ingress_batches i WHERE i.result='通过' AND i.contract_version='v3' "
              "AND (SELECT status FROM contracts WHERE version='v3')='停用' AND i.id>?", (watermark,))[0][0]
    try:
        ev = admin.get("/api/cutoff/events", headers=h)
        events = ev.json().get("items", []) if ev.status_code == 200 else []
    except Exception:
        events = []
    print(f"[汇总] 停用后水位越界通过={bad}；停用争用记录={contentions}；切断事件={len(events)}")
    if violations or bad:
        print("[结论] 复现成功：停用正式版仍被选中裁通过（停用态与入站结果拧着）")
        return 1
    print("[结论] 停用切断生效：水位后零通过，撞车只记停用争用")
    return 0

if __name__ == "__main__":
    sys.exit(main())
