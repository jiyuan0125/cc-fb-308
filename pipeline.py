#!/usr/bin/env python3
"""交易明细质量管道。

从 batches/ 按文件名顺序逐批读取交易明细 CSV，按 rules.yaml 中的规则
做数据质量检查：

- 行级规则（金额为正 / 币种在支持列表 / 账户格式合规 / 批内交易号唯一）：
  不合格的行写入 quarantine/ 目录，不连累整批。
- 批级规则（单批合计不超风控上限）：命中说明整批不可信，本批全部不入库，
  标记为 failed 并停止本轮运行，等人修数后再次运行从该批继续。

已入库（loaded）的批次跳过不重跑；failed 或未见过的批次会被（重新）处理。
每轮运行结束输出 quality_report.txt，含行视角与规则视角两份统计。

运行方式：在仓库根目录执行 python3 pipeline.py
"""

from __future__ import annotations

import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

BATCHES_DIR = Path("batches")
RULES_PATH = Path("rules.yaml")
DB_PATH = Path("pipeline.db")
QUARANTINE_DIR = Path("quarantine")
REPORT_PATH = Path("quality_report.txt")

REQUIRED_COLUMNS = ["txn_id", "account", "amount", "currency", "txn_date", "batch_id"]


def _now() -> str:
    """带本地时区偏移的当前时间戳。"""
    return datetime.now().astimezone().isoformat(timespec="seconds")


# ---------------------------------------------------------------- 规则引擎

@dataclass
class Rule:
    """rules.yaml 中一条规则的解析结果。level 为 row 或 batch。"""

    name: str
    level: str
    kind: str
    raw: dict


def load_rules(rules_path: Path) -> list[Rule]:
    """读取 rules.yaml，按规则语义归类为行级 / 批级。"""
    with open(rules_path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    rules = []
    for item in doc["规则清单"]:
        name = item["规则名"]
        requirement = item.get("要求", "")
        if requirement == "合计不超过":
            rules.append(Rule(name, "batch", "sum_max", item))
        elif requirement == "批内不重复":
            rules.append(Rule(name, "row", "unique_in_batch", item))
        elif requirement == "严格大于":
            rules.append(Rule(name, "row", "gt", item))
        elif requirement == "取值属于":
            rules.append(Rule(name, "row", "in_list", item))
        elif "首字符" in item and "后续字符" in item:
            rules.append(Rule(name, "row", "account_format", item))
        else:
            raise ValueError(f"无法识别的规则，请检查 rules.yaml: {item!r}")
    return rules


def _account_pattern(rule: Rule) -> re.Pattern:
    """按规则里的首字符/后续字符/后续位数拼出账户格式（封闭固定格式，可用正则）。"""
    first_classes = {"英文字母": "[A-Za-z]", "数字": r"\d"}
    first = first_classes[rule.raw["首字符"]]
    rest = first_classes[rule.raw["后续字符"]]
    count = int(rule.raw["后续位数"])
    return re.compile(first + rest + "{" + str(count) + "}")


def check_row(rule: Rule, row: dict, seen_ids: set) -> bool:
    """行级规则单条检查，返回 True 表示合格。"""
    value = row.get(rule.raw.get("字段", ""), "")
    if rule.kind == "gt":
        try:
            return float(value) > float(rule.raw["界限值"])
        except (TypeError, ValueError):
            return False
    if rule.kind == "in_list":
        return value in set(rule.raw["允许值"])
    if rule.kind == "account_format":
        return bool(_account_pattern(rule).fullmatch(value or ""))
    if rule.kind == "unique_in_batch":
        return value not in seen_ids
    raise ValueError(f"未知行级规则类型: {rule.kind}")


def check_batch(rule: Rule, rows: list[dict]) -> bool:
    """批级规则检查，返回 True 表示整批可信。"""
    if rule.kind == "sum_max":
        total = 0.0
        for row in rows:
            try:
                total += float(row.get(rule.raw["字段"], ""))
            except (TypeError, ValueError):
                continue  # 非数值金额已被行级规则隔离，不参与合计
        return total <= float(rule.raw["上限"])
    raise ValueError(f"未知批级规则类型: {rule.kind}")


# ---------------------------------------------------------------- 状态存储

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,              -- loaded / failed
    rows_total INTEGER NOT NULL DEFAULT 0,
    rows_loaded INTEGER NOT NULL DEFAULT 0,
    rows_quarantined INTEGER NOT NULL DEFAULT 0,
    detail TEXT NOT NULL DEFAULT '',
    processed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS transactions (
    batch_id TEXT NOT NULL,
    txn_id TEXT NOT NULL,
    account TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    txn_date TEXT NOT NULL,
    PRIMARY KEY (batch_id, txn_id)
);
"""


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn


def loaded_batch_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT batch_id FROM batches WHERE status = 'loaded'").fetchall()
    return {r[0] for r in rows}


# ---------------------------------------------------------------- 管道主体

@dataclass
class BatchResult:
    batch_id: str
    status: str  # loaded / failed / skipped
    rows_total: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    # 规则名 -> 命中行数（批级规则命中时记整批行数）
    rule_hits: dict = field(default_factory=dict)


@dataclass
class RunResult:
    results: list = field(default_factory=list)  # list[BatchResult]
    halted: bool = False
    report_path: Path = REPORT_PATH

    @property
    def processed(self) -> list:
        return [r for r in self.results if r.status != "skipped"]


def read_batch(csv_path: Path) -> list[dict]:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{csv_path} 缺少字段: {missing}")
    return df.to_dict("records")


def process_batch(
    conn: sqlite3.Connection,
    csv_path: Path,
    rules: list[Rule],
    quarantine_dir: Path,
) -> BatchResult:
    """处理单个批次。行级不合格进 quarantine，批级不合格整批拒绝。"""
    batch_id = csv_path.stem
    rows = read_batch(csv_path)
    result = BatchResult(batch_id=batch_id, status="loaded", rows_total=len(rows))

    row_rules = [r for r in rules if r.level == "row"]
    batch_rules = [r for r in rules if r.level == "batch"]

    # 批级检查：整批不可信则全部不入库，本批不写隔离文件（整批等待修数）
    failed_rules = []
    for rule in batch_rules:
        if not check_batch(rule, rows):
            failed_rules.append(rule.name)
            result.rule_hits[rule.name] = len(rows)
    if failed_rules:
        result.status = "failed"
        now = _now()
        with conn:
            conn.execute(
                "INSERT INTO batches VALUES (?, 'failed', ?, 0, 0, ?, ?) "
                "ON CONFLICT(batch_id) DO UPDATE SET "
                "status='failed', rows_total=excluded.rows_total, "
                "rows_loaded=0, rows_quarantined=0, "
                "detail=excluded.detail, processed_at=excluded.processed_at",
                (batch_id, len(rows), "整批不可信: " + "; ".join(failed_rules), now),
            )
        return result

    # 行级检查：坏行进 quarantine，好行入库
    seen_ids: set = set()
    good_rows, bad_rows = [], []
    for row in rows:
        reasons = []
        for rule in row_rules:
            if not check_row(rule, row, seen_ids):
                reasons.append(rule.name)
                result.rule_hits[rule.name] = result.rule_hits.get(rule.name, 0) + 1
        if reasons:
            bad_rows.append((row, reasons))
        else:
            good_rows.append(row)
        # 首次出现即登记，后续同号视为重复（无论首行本身是否因别的原因被隔离）
        seen_ids.add(row.get("txn_id", ""))

    if bad_rows:
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        qdf = pd.DataFrame(
            [{**row, "不合格原因": "; ".join(reasons)} for row, reasons in bad_rows],
            columns=REQUIRED_COLUMNS + ["不合格原因"],
        )
        qdf.to_csv(quarantine_dir / f"{batch_id}.csv", index=False, encoding="utf-8")
    else:
        # 本批本次无隔离行，清掉上一轮可能遗留的隔离文件，避免口径漂移
        stale = quarantine_dir / f"{batch_id}.csv"
        if stale.exists():
            stale.unlink()

    now = _now()
    result.rows_loaded = len(good_rows)
    result.rows_quarantined = len(bad_rows)
    with conn:
        # 防御性清理：同一批次重跑时先删旧行，保证不重复入库
        conn.execute("DELETE FROM transactions WHERE batch_id = ?", (batch_id,))
        conn.executemany(
            "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    batch_id,
                    row["txn_id"],
                    row["account"],
                    float(row["amount"]),
                    row["currency"],
                    row["txn_date"],
                )
                for row in good_rows
            ],
        )
        conn.execute(
            "INSERT INTO batches VALUES (?, 'loaded', ?, ?, ?, '', ?) "
            "ON CONFLICT(batch_id) DO UPDATE SET "
            "status='loaded', rows_total=excluded.rows_total, "
            "rows_loaded=excluded.rows_loaded, "
            "rows_quarantined=excluded.rows_quarantined, "
            "detail='', processed_at=excluded.processed_at",
            (batch_id, len(rows), len(good_rows), len(bad_rows), now),
        )
    return result


def run_pipeline(
    batches_dir: Path = BATCHES_DIR,
    rules_path: Path = RULES_PATH,
    db_path: Path = DB_PATH,
    quarantine_dir: Path = QUARANTINE_DIR,
    report_path: Path = REPORT_PATH,
) -> RunResult:
    """跑一轮管道：只处理未通过的批次，批级失败即停，最后写质量报告。"""
    rules = load_rules(rules_path)
    conn = open_db(db_path)
    try:
        done = loaded_batch_ids(conn)
        run = RunResult(report_path=report_path)
        csv_files = sorted(batches_dir.glob("*.csv"))
        for csv_path in csv_files:
            batch_id = csv_path.stem
            if batch_id in done:
                run.results.append(BatchResult(batch_id=batch_id, status="skipped"))
                continue
            result = process_batch(conn, csv_path, rules, quarantine_dir)
            run.results.append(result)
            if result.status == "failed":
                # 整批不可信：停下来等人修数，后续批次本轮不再处理
                run.halted = True
                break
        write_report(run, rules, report_path)
        return run
    finally:
        conn.close()


# ---------------------------------------------------------------- 质量报告

def write_report(run: RunResult, rules: list[Rule], report_path: Path) -> None:
    processed = run.processed
    skipped = [r for r in run.results if r.status == "skipped"]
    loaded = [r for r in processed if r.status == "loaded"]
    failed = [r for r in processed if r.status == "failed"]

    rows_loaded = sum(r.rows_loaded for r in processed)
    rows_quarantined = sum(r.rows_quarantined for r in processed)

    lines = [
        "交易明细数据质量报告",
        f"生成时间: {_now()}",
        "",
        (
            f"本轮处理批次: {len(processed)} 个（入库 {len(loaded)}，整批失败 {len(failed)}）"
            f"，跳过已入库批次: {len(skipped)} 个"
        ),
    ]
    if run.halted:
        lines.append(
            "运行状态: 已在整批失败的批次处停止（"
            + ", ".join(r.batch_id for r in failed)
            + "），待修数后重新运行将从该批继续"
        )
    else:
        lines.append("运行状态: 正常完成")

    lines += [
        "",
        "== 行视角 ==",
        f"合格入库行数: {rows_loaded}",
        f"隔离行数: {rows_quarantined}",
        "各原因隔离计数:",
    ]
    # 行视角只统计行级规则的隔离原因；批级规则命中见规则视角
    reason_counts: dict[str, int] = {}
    row_rule_names = [r.name for r in rules if r.level == "row"]
    for r in processed:
        for name in row_rule_names:
            if name in r.rule_hits:
                reason_counts[name] = reason_counts.get(name, 0) + r.rule_hits[name]
    for name in row_rule_names:
        lines.append(f"  {name}: {reason_counts.get(name, 0)}")
    if not reason_counts:
        lines.append("  （本轮无不合格行）")

    lines += ["", "== 规则视角 =="]
    for rule in rules:
        hit_batches = [r for r in processed if rule.name in r.rule_hits]
        hit_rows = sum(r.rule_hits[rule.name] for r in hit_batches)
        level = "行级" if rule.level == "row" else "批级"
        lines.append(
            f"  {rule.name}（{level}）: 命中批次 {len(hit_batches)} 个，命中行数 {hit_rows} 行"
        )

    lines += ["", "== 批次明细 =="]
    for r in run.results:
        if r.status == "skipped":
            lines.append(f"  {r.batch_id}: 跳过（此前已入库）")
        elif r.status == "failed":
            lines.append(f"  {r.batch_id}: 整批失败，未入库（共 {r.rows_total} 行）")
        else:
            lines.append(
                f"  {r.batch_id}: 入库 {r.rows_loaded} 行，隔离 {r.rows_quarantined} 行"
            )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    run = run_pipeline()
    print(REPORT_PATH.read_text(encoding="utf-8"))
    return 1 if run.halted else 0


if __name__ == "__main__":
    sys.exit(main())
