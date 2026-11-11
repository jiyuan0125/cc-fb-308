"""pipeline.py 的行为测试：逐行隔离、整批拦截、断点续跑与规则驱动。"""

from __future__ import annotations

import csv
import logging
import shutil
import sqlite3
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

import pipeline

REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_COLUMNS = ("txn_id", "account", "amount", "currency", "txn_date", "batch_id")

# 样例 12 个批次的形状：301-306 干净，307/308/309 各有 2 行行级问题，
# 310/312 批内交易号重复，311 单批合计超上限。
SAMPLE_TOTALS = {
    "batch_20260301.csv": 40,
    "batch_20260302.csv": 45,
    "batch_20260303.csv": 38,
    "batch_20260304.csv": 42,
    "batch_20260305.csv": 36,
    "batch_20260306.csv": 44,
    "batch_20260307.csv": 48,
    "batch_20260308.csv": 40,
    "batch_20260309.csv": 42,
    "batch_20260310.csv": 35,
    "batch_20260311.csv": 30,
    "batch_20260312.csv": 33,
}


def sample_files() -> list[str]:
    return sorted(path.name for path in (REPO_ROOT / "batches").glob("*.csv"))


def build_config(
    tmp_path: Path, *, batch_names: list[str] | None = None, rules_text: str | None = None
) -> pipeline.PipelineConfig:
    batches_dir = tmp_path / "batches"
    batches_dir.mkdir(exist_ok=True)
    names = sample_files() if batch_names is None else batch_names
    for name in names:
        shutil.copy(REPO_ROOT / "batches" / name, batches_dir / name)

    rules_path = tmp_path / "rules.yaml"
    if rules_text is None:
        shutil.copy(REPO_ROOT / "rules.yaml", rules_path)
    else:
        rules_path.write_text(rules_text, encoding="utf-8")

    return pipeline.PipelineConfig(
        rules_path=rules_path,
        batches_dir=batches_dir,
        db_path=tmp_path / "warehouse.db",
        report_path=tmp_path / "quality_report.txt",
        quarantine_dir=tmp_path / "quarantine",
    )


def run(config: pipeline.PipelineConfig, *, log_level: str = "WARNING") -> int:
    logging.getLogger("pipeline").setLevel(log_level)
    return pipeline.main(
        [
            "--rules",
            str(config.rules_path),
            "--batches",
            str(config.batches_dir),
            "--db",
            str(config.db_path),
            "--report",
            str(config.report_path),
            "--quarantine",
            str(config.quarantine_dir),
            "--log-level",
            log_level,
        ]
    )


def query(config: pipeline.PipelineConfig, sql: str) -> list[tuple]:
    connection = sqlite3.connect(config.db_path)
    try:
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


def statuses(config: pipeline.PipelineConfig) -> dict[str, str]:
    return dict(query(config, "SELECT batch_id, status FROM batches"))


def report_text(config: pipeline.PipelineConfig) -> str:
    return Path(config.report_path).read_text(encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(BASE_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def rewrite(path: Path, mutate: Callable[[pd.DataFrame], None]) -> None:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    mutate(frame)
    frame.to_csv(path, index=False)


def read_quarantine(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_first_run_stops_at_whole_batch_failure(tmp_path: Path) -> None:
    config = build_config(tmp_path)

    assert run(config) == pipeline.EXIT_HALTED

    state = statuses(config)
    assert [state[f"2026030{number}"] for number in range(1, 10)] == ["passed"] * 9
    assert state["20260310"] == "failed"
    assert "20260311" not in state and "20260312" not in state

    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 369
    assert query(config, "SELECT COUNT(*) FROM quarantined_rows")[0][0] == 6

    report = report_text(config)
    assert "合格数（已入库行数）：369" in report
    assert "隔离数：6" in report
    assert "整批失败未入库行数：35" in report
    assert "金额为正 | 逐行 | 1 | 2" in report
    assert "币种在支持列表 | 逐行 | 1 | 2" in report
    assert "账户格式合规 | 逐行 | 1 | 2" in report
    assert "批内交易号唯一 | 整批 | 1 | 2" in report
    assert "单批合计不超风控上限 | 整批 | 0 | 0" in report
    assert "未处理批次：2（batch_20260311.csv、batch_20260312.csv）" in report
    assert "已中断（整批不合格）" in report

    quarantined_307 = read_quarantine(config.quarantine_dir / "batch_20260307.csv")
    assert len(quarantined_307) == 2
    assert {row["txn_id"] for row in quarantined_307} == {"T10000251", "T10000276"}
    assert {row["违规规则"] for row in quarantined_307} == {"金额为正"}

    quarantined_308 = read_quarantine(config.quarantine_dir / "batch_20260308.csv")
    assert {row["txn_id"] for row in quarantined_308} == {"T10000301", "T10000316"}
    assert {row["违规规则"] for row in quarantined_308} == {"币种在支持列表"}

    quarantined_309 = read_quarantine(config.quarantine_dir / "batch_20260309.csv")
    assert {row["txn_id"] for row in quarantined_309} == {"T10000343", "T10000365"}
    assert {row["违规规则"] for row in quarantined_309} == {"账户格式合规"}

    # 整批失败的批次不写隔离文件，整批都不入库。
    assert not (config.quarantine_dir / "batch_20260310.csv").exists()
    assert query(config, "SELECT COUNT(*) FROM transactions WHERE batch_id='20260310'")[0][0] == 0


def test_duplicate_txn_id_is_whole_batch_failure_not_row_quarantine(tmp_path: Path) -> None:
    config = build_config(tmp_path, batch_names=["batch_20260310.csv"])

    assert run(config) == pipeline.EXIT_HALTED

    assert statuses(config) == {"20260310": "failed"}
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 0
    assert query(config, "SELECT COUNT(*) FROM quarantined_rows")[0][0] == 0
    assert not (config.quarantine_dir / "batch_20260310.csv").exists()

    report = report_text(config)
    assert "批内交易号唯一 | 整批 | 1 | 2" in report
    assert "整批失败未入库行数：35" in report


def test_batch_sum_limit_blocks_entire_batch(tmp_path: Path) -> None:
    config = build_config(tmp_path, batch_names=["batch_20260311.csv"])

    assert run(config) == pipeline.EXIT_HALTED

    assert statuses(config) == {"20260311": "failed"}
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 0
    assert not list(config.quarantine_dir.glob("*.csv"))

    report = report_text(config)
    assert "单批合计不超风控上限 | 整批 | 1 | 30" in report
    assert "369542.13" in report
    assert "整批失败未入库行数：30" in report


@pytest.mark.parametrize(
    "file_name, total_rows, quarantined, rule_name, txn_ids",
    [
        ("batch_20260307.csv", 48, 2, "金额为正", {"T10000251", "T10000276"}),
        ("batch_20260308.csv", 40, 2, "币种在支持列表", {"T10000301", "T10000316"}),
        ("batch_20260309.csv", 42, 2, "账户格式合规", {"T10000343", "T10000365"}),
    ],
)
def test_row_level_issues_are_quarantined_without_blocking_batch(
    tmp_path: Path,
    file_name: str,
    total_rows: int,
    quarantined: int,
    rule_name: str,
    txn_ids: set[str],
) -> None:
    config = build_config(tmp_path, batch_names=[file_name])

    assert run(config) == pipeline.EXIT_OK

    batch_id = file_name.removeprefix("batch_").removesuffix(".csv")
    assert statuses(config) == {batch_id: "passed"}
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == total_rows - quarantined
    assert query(config, "SELECT COUNT(*) FROM quarantined_rows")[0][0] == quarantined

    quarantine_rows = read_quarantine(config.quarantine_dir / file_name)
    assert {row["txn_id"] for row in quarantine_rows} == txn_ids
    assert {row["违规规则"] for row in quarantine_rows} == {rule_name}

    report = report_text(config)
    assert f"合格数（已入库行数）：{total_rows - quarantined}" in report
    assert f"隔离数：{quarantined}" in report
    assert f"{rule_name}：{quarantined}" in report


def _fix_duplicate_txn_id(frame: pd.DataFrame) -> None:
    frame.loc[frame["txn_id"].duplicated(keep="first"), "txn_id"] = "T99999999"


def _shrink_amounts(frame: pd.DataFrame) -> None:
    frame["amount"] = "100.00"


def test_resume_after_fixing_batches_skips_ingested_ones(tmp_path: Path) -> None:
    config = build_config(tmp_path)

    # 第一轮：停在 20260310（批内交易号重复）。
    assert run(config) == pipeline.EXIT_HALTED
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 369

    # 修好 20260310 后重跑：已入库的 9 个批跳过，处理 20260310，停在 20260311。
    rewrite(config.batches_dir / "batch_20260310.csv", _fix_duplicate_txn_id)
    assert run(config) == pipeline.EXIT_HALTED
    assert statuses(config)["20260310"] == "passed"
    assert "20260311" in statuses(config) and statuses(config)["20260311"] == "failed"
    assert "20260312" not in statuses(config)
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 404
    assert "跳过（已入库）批次：9" in report_text(config)

    # 修好 20260311 的合计超限后重跑：处理 20260311，停在 20260312。
    rewrite(config.batches_dir / "batch_20260311.csv", _shrink_amounts)
    assert run(config) == pipeline.EXIT_HALTED
    assert statuses(config)["20260311"] == "passed"
    assert statuses(config)["20260312"] == "failed"
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 434

    # 修好 20260312 后重跑：全部通过，行数 = 473 - 6 隔离行。
    rewrite(config.batches_dir / "batch_20260312.csv", _fix_duplicate_txn_id)
    assert run(config) == pipeline.EXIT_OK
    assert set(statuses(config).values()) == {"passed"}
    assert len(statuses(config)) == 12
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 467
    assert query(config, "SELECT COUNT(*) FROM quarantined_rows")[0][0] == 6
    assert (
        query(
            config,
            "SELECT batch_id, txn_id, COUNT(*) FROM transactions"
            " GROUP BY batch_id, txn_id HAVING COUNT(*) > 1",
        )
        == []
    )

    # 再跑一轮：零处理、零重复入库。
    assert run(config) == pipeline.EXIT_OK
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 467
    final_report = report_text(config)
    assert "跳过（已入库）批次：12" in final_report
    assert "处理批次数：0" in final_report
    assert "已处理批次：12（通过 12，失败 0）" in final_report


def test_rerun_rebuilds_quarantine_files_from_database(tmp_path: Path) -> None:
    config = build_config(tmp_path, batch_names=["batch_20260307.csv"])

    assert run(config) == pipeline.EXIT_OK
    shutil.rmtree(config.quarantine_dir)
    assert run(config) == pipeline.EXIT_OK

    rows = read_quarantine(config.quarantine_dir / "batch_20260307.csv")
    assert len(rows) == 2


def test_passed_batch_is_not_reprocessed_even_if_file_changes(tmp_path: Path) -> None:
    config = build_config(tmp_path, batch_names=["batch_20260301.csv"])

    assert run(config) == pipeline.EXIT_OK
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 40

    # 事后篡改已入库批次：追加一条坏行和一条重复交易号。
    path = config.batches_dir / "batch_20260301.csv"
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    extra = frame.iloc[[0]].copy()
    extra.loc[:, "txn_id"] = frame.iloc[0]["txn_id"]
    extra.loc[:, "amount"] = "-1.00"
    extra.to_csv(path, mode="a", header=False, index=False)

    assert run(config) == pipeline.EXIT_OK
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 40
    assert statuses(config) == {"20260301": "passed"}
    assert "跳过（已入库）批次：1" in report_text(config)


def _custom_rules(sum_limit: str) -> str:
    return f"""规则清单:
  - 规则名: 金额为正
    字段: amount
    要求: 严格大于
    界限值: 0
  - 规则名: 币种仅人民币
    字段: currency
    要求: 取值属于
    允许值: [CNY]
  - 规则名: 账户格式合规
    字段: account
    首字符: 英文字母
    后续字符: 数字
    后续位数: 10
  - 规则名: 批内交易号唯一
    字段: txn_id
    要求: 批内不重复
  - 规则名: 单批合计不超风控上限
    字段: amount
    要求: 合计不超过
    上限: {sum_limit}
"""


CUSTOM_ROWS = [
    {"txn_id": "T1", "account": "A1234567890", "amount": "100.00", "currency": "CNY", "txn_date": "2026-01-01", "batch_id": "20990101"},
    {"txn_id": "T2", "account": "A1234567890", "amount": "-5.00", "currency": "CNY", "txn_date": "2026-01-01", "batch_id": "20990101"},
    {"txn_id": "T3", "account": "A1234567890", "amount": "0.00", "currency": "CNY", "txn_date": "2026-01-01", "batch_id": "20990101"},
    {"txn_id": "T4", "account": "A1234567890", "amount": "abc", "currency": "CNY", "txn_date": "2026-01-01", "batch_id": "20990101"},
    {"txn_id": "T5", "account": "B1234567890", "amount": "300.00", "currency": "USD", "txn_date": "2026-01-01", "batch_id": "20990101"},
    {"txn_id": "T6", "account": "1234567890", "amount": "50.00", "currency": "CNY", "txn_date": "2026-01-01", "batch_id": "20990101"},
    {"txn_id": "T7", "account": "C1234567890", "amount": "20.00", "currency": "CNY", "txn_date": "2026-01-01", "batch_id": "20990101"},
    {"txn_id": "T8", "account": "C1234567890", "amount": "2000.00", "currency": "CNY", "txn_date": "2026-01-01", "batch_id": "20990101"},
]


def test_rules_yaml_drives_checks(tmp_path: Path) -> None:
    config = build_config(tmp_path, batch_names=[], rules_text=_custom_rules("5000"))
    write_csv(config.batches_dir / "batch_20990101.csv", CUSTOM_ROWS)

    assert run(config) == pipeline.EXIT_OK

    quarantined = read_quarantine(config.quarantine_dir / "batch_20990101.csv")
    assert {row["txn_id"] for row in quarantined} == {"T2", "T3", "T4", "T5", "T6"}
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 3

    report = report_text(config)
    assert "合格数（已入库行数）：3" in report
    assert "隔离数：5" in report
    assert "金额为正：3" in report
    assert "币种仅人民币：1" in report
    assert "账户格式合规：1" in report
    assert "批内交易号唯一 | 整批 | 0 | 0" in report
    assert "单批合计不超风控上限 | 整批 | 0 | 0" in report


def test_rules_yaml_upper_bound_change_blocks_batch(tmp_path: Path) -> None:
    config = build_config(tmp_path, batch_names=[], rules_text=_custom_rules("2000"))
    write_csv(config.batches_dir / "batch_20990101.csv", CUSTOM_ROWS)

    assert run(config) == pipeline.EXIT_HALTED
    assert statuses(config) == {"20990101": "failed"}
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 0
    assert "单批合计不超风控上限 | 整批 | 1 | 8" in report_text(config)


def test_unsupported_rule_fails_fast(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    rules_text = """规则清单:
  - 规则名: 未知判据
    字段: amount
    要求: 不小于
    界限值: 0
"""
    config = build_config(tmp_path, batch_names=["batch_20260301.csv"], rules_text=rules_text)

    with caplog.at_level(logging.ERROR, logger="pipeline"):
        assert run(config) == pipeline.EXIT_ERROR

    assert any("暂不支持" in record.message for record in caplog.records)
    assert not config.db_path.exists()


@pytest.mark.parametrize(
    "content",
    [
        "",
        "txn_id,account,amount,currency,txn_date,batch_id\n",
        "txn_id,account,amount,txn_date,batch_id\nT1,A1234567890,10.00,2026-01-01,20990101\n",
        (
            "txn_id,account,amount,currency,txn_date,batch_id\n"
            "T1,A1234567890,10.00,CNY,2026-01-01,20990101\n"
            "T2,A1234567890,10.00,CNY,2026-01-01,20990102\n"
        ),
        (
            "txn_id,account,amount,currency,txn_date,batch_id\n"
            ",A1234567890,10.00,CNY,2026-01-01,20990101\n"
        ),
    ],
)
def test_structural_issues_reject_whole_batch(tmp_path: Path, content: str) -> None:
    config = build_config(tmp_path, batch_names=[])
    (config.batches_dir / "batch_20990101.csv").write_text(content, encoding="utf-8")

    assert run(config) == pipeline.EXIT_HALTED
    assert statuses(config) == {"20990101": "failed"}
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 0
    assert "整批失败" in report_text(config)


def test_interrupted_run_resumes_without_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
    config = build_config(
        tmp_path,
        batch_names=["batch_20260301.csv", "batch_20260302.csv", "batch_20260303.csv"],
    )
    real_process_batch = pipeline.process_batch
    calls = {"count": 0}

    def flaky_process_batch(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 3:
            raise KeyboardInterrupt
        return real_process_batch(*args, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(pipeline, "process_batch", flaky_process_batch)
        assert run(config) == pipeline.EXIT_INTERRUPTED

    assert statuses(config) == {"20260301": "passed", "20260302": "passed"}
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 40 + 45
    assert "已中断（进程被中断）" in report_text(config)

    assert run(config) == pipeline.EXIT_OK
    assert set(statuses(config)) == {"20260301", "20260302", "20260303"}
    assert query(config, "SELECT COUNT(*) FROM transactions")[0][0] == 40 + 45 + 38
