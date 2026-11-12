"""pipeline.py 的 pytest 测试。

覆盖：正常入库、行级隔离、批级失败停跑、断点续跑（修数后从失败批继续）、
重复运行幂等、规则文件驱动行为。
"""

import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pipeline

RULES = {
    "规则清单": [
        {"规则名": "金额为正", "字段": "amount", "要求": "严格大于", "界限值": 0},
        {"规则名": "币种在支持列表", "字段": "currency", "要求": "取值属于",
         "允许值": ["CNY", "USD"]},
        {"规则名": "账户格式合规", "字段": "account",
         "首字符": "英文字母", "后续字符": "数字", "后续位数": 10},
        {"规则名": "批内交易号唯一", "字段": "txn_id", "要求": "批内不重复"},
        {"规则名": "单批合计不超风控上限", "字段": "amount",
         "要求": "合计不超过", "上限": 10000},
    ]
}

HEADER = "txn_id,account,amount,currency,txn_date,batch_id"


def make_row(txn_id, amount="100.00", account="A1234567890",
             currency="CNY", date="2026-03-01", batch="b1"):
    return f"{txn_id},{account},{amount},{currency},{date},{batch}"


def write_batch(batches_dir, name, rows):
    (batches_dir / f"{name}.csv").write_text(
        HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8"
    )


@pytest.fixture()
def env(tmp_path):
    """构造独立的运行环境（batches/rules/db/quarantine/report 全在 tmp_path）。"""
    batches_dir = tmp_path / "batches"
    batches_dir.mkdir()
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(yaml.safe_dump(RULES, allow_unicode=True), encoding="utf-8")

    def run():
        return pipeline.run_pipeline(
            batches_dir=batches_dir,
            rules_path=rules_path,
            db_path=tmp_path / "pipeline.db",
            quarantine_dir=tmp_path / "quarantine",
            report_path=tmp_path / "quality_report.txt",
        )

    def db_rows():
        conn = sqlite3.connect(tmp_path / "pipeline.db")
        try:
            txns = conn.execute(
                "SELECT batch_id, txn_id FROM transactions ORDER BY batch_id, txn_id"
            ).fetchall()
            batches = dict(conn.execute("SELECT batch_id, status FROM batches").fetchall())
        finally:
            conn.close()
        return txns, batches

    return tmp_path, batches_dir, rules_path, run, db_rows


def test_happy_path_all_loaded(env):
    _, batches_dir, _, run, db_rows = env
    write_batch(batches_dir, "batch_a", [make_row("T1"), make_row("T2", batch="a")])
    write_batch(batches_dir, "batch_b", [make_row("T3", batch="b")])

    result = run()
    assert not result.halted
    txns, batches = db_rows()
    assert batches == {"batch_a": "loaded", "batch_b": "loaded"}
    assert len(txns) == 3


def test_row_level_quarantine(env):
    tmp_path, batches_dir, _, run, db_rows = env
    write_batch(batches_dir, "batch_a", [
        make_row("T1"),                                  # 合格
        make_row("T2", amount="-5.00"),                  # 金额非正
        make_row("T3", amount="abc"),                    # 金额非数值
        make_row("T4", currency="SGD"),                  # 币种越界
        make_row("T5", account="12345678901"),           # 账户首字符非字母
        make_row("T6", account="A123"),                  # 账户位数不足
        make_row("T1", amount="50.00"),                  # 批内重复 txn_id
    ])
    result = run()
    assert not result.halted

    txns, _ = db_rows()
    assert [t[1] for t in txns] == ["T1"]  # 仅首行合格入库，重复行被隔离

    qfile = tmp_path / "quarantine" / "batch_a.csv"
    assert qfile.exists()
    content = qfile.read_text(encoding="utf-8")
    assert "不合格原因" in content
    for bad_id in ["T2", "T3", "T4", "T5", "T6"]:
        assert bad_id in content
    assert content.count("T1") == 1  # 首行入库不进隔离文件，仅重复行在隔离区出现一次

    report = (tmp_path / "quality_report.txt").read_text(encoding="utf-8")
    assert "合格入库行数: 1" in report
    assert "隔离行数: 6" in report
    assert "批内交易号唯一: 1" in report


def test_batch_level_failure_halts_and_nothing_ingested(env):
    tmp_path, batches_dir, _, run, db_rows = env
    write_batch(batches_dir, "batch_a", [make_row("T1")])
    # 合计 20000 超上限 10000，且夹一行坏行：整批失败时什么都不入
    write_batch(batches_dir, "batch_b", [
        make_row("T2", amount="15000", batch="b"),
        make_row("T3", amount="5000", currency="SGD", batch="b"),
    ])
    write_batch(batches_dir, "batch_c", [make_row("T4", batch="c")])

    result = run()
    assert result.halted

    txns, batches = db_rows()
    assert batches == {"batch_a": "loaded", "batch_b": "failed"}
    assert [t[1] for t in txns] == ["T1"]          # batch_b 一行未入
    assert not (tmp_path / "quarantine" / "batch_b.csv").exists()  # 整批待定，不写隔离
    # batch_c 排在失败批之后，本轮不处理
    assert "batch_c" not in batches

    report = (tmp_path / "quality_report.txt").read_text(encoding="utf-8")
    assert "单批合计不超风控上限（批级）: 命中批次 1 个，命中行数 2 行" in report


def test_resume_after_fix(env):
    """断点续跑：修数后只处理失败批及之后的批次，已入库批次不受影响。"""
    _, batches_dir, _, run, db_rows = env
    write_batch(batches_dir, "batch_a", [make_row("T1")])
    write_batch(batches_dir, "batch_b", [make_row("T2", amount="99999", batch="b")])
    write_batch(batches_dir, "batch_c", [make_row("T3", batch="c")])

    first = run()
    assert first.halted
    txns_before, _ = db_rows()
    assert [t[1] for t in txns_before] == ["T1"]

    # 财务修数：把超限金额改回合规值
    write_batch(batches_dir, "batch_b", [make_row("T2", amount="500", batch="b")])

    second = run()
    assert not second.halted
    processed_ids = [r.batch_id for r in second.processed]
    assert processed_ids == ["batch_b", "batch_c"]   # 从失败的那批继续
    skipped_ids = [r.batch_id for r in second.results if r.status == "skipped"]
    assert skipped_ids == ["batch_a"]                # 已通过的批次不重来

    txns, batches = db_rows()
    assert batches == {"batch_a": "loaded", "batch_b": "loaded", "batch_c": "loaded"}
    assert sorted(t[1] for t in txns) == ["T1", "T2", "T3"]  # 无重复、无丢失


def test_rerun_is_idempotent(env):
    _, batches_dir, _, run, db_rows = env
    write_batch(batches_dir, "batch_a", [make_row("T1"), make_row("T2", batch="a")])
    run()
    txns_first, _ = db_rows()

    second = run()
    assert not second.halted
    assert second.processed == []                    # 没有需要处理的批次
    txns_second, _ = db_rows()
    assert txns_first == txns_second                 # 已入库数据不受影响


def test_failed_batch_retry_does_not_duplicate_rows(env):
    """失败批反复重跑（未修数）不应产生重复行或状态错乱。"""
    _, batches_dir, _, run, db_rows = env
    write_batch(batches_dir, "batch_a", [make_row("T1", amount="99999")])
    for _ in range(3):
        assert run().halted
    txns, batches = db_rows()
    assert txns == []
    assert batches == {"batch_a": "failed"}


def test_rules_file_drives_behavior(env):
    """检查口径随 rules.yaml：放宽上限后原本失败的批次应通过。"""
    _, batches_dir, rules_path, run, db_rows = env
    write_batch(batches_dir, "batch_a", [make_row("T1", amount="5000")])
    assert not run().halted

    # 收紧上限到 100，新批次失败
    rules = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    rules["规则清单"][4]["上限"] = 100
    rules_path.write_text(yaml.safe_dump(rules, allow_unicode=True), encoding="utf-8")
    write_batch(batches_dir, "batch_b", [make_row("T2", amount="5000", batch="b")])
    assert run().halted
    _, batches = db_rows()
    assert batches["batch_b"] == "failed"


def test_report_has_two_views(env):
    tmp_path, batches_dir, _, run, _ = env
    write_batch(batches_dir, "batch_a", [
        make_row("T1"),
        make_row("T2", amount="-1"),
    ])
    run()
    report = (tmp_path / "quality_report.txt").read_text(encoding="utf-8")
    assert "== 行视角 ==" in report
    assert "== 规则视角 ==" in report
    assert "合格入库行数: 1" in report
    assert "隔离行数: 1" in report
    assert "金额为正: 1" in report
    # 规则视角：每条规则都有批次数和行数
    assert "金额为正（行级）: 命中批次 1 个，命中行数 1 行" in report
    assert "批内交易号唯一（行级）: 命中批次 0 个，命中行数 0 行" in report
