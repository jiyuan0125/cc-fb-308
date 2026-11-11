#!/usr/bin/env python3
"""交易明细质量管道：逐行隔离、整批拦截、断点续跑。

质量规则从 rules.yaml 读取，按判据本身的语义分成两类执行：

* 逐行规则：判据只依赖单行的字段值（金额为正、币种在允许值内、账户格式）。
  不合格行写入 quarantine/，同批其余行照常入库，单行的毛病不连累整批。
* 整批规则：判据依赖批内多行或全批汇总（批内交易号唯一、单批合计上限）。
  任一命中即整批不入库，运行在当前批次停下，等人工修数后重跑。

入库数据、隔离行、规则命中和批次状态在同一个 SQLite 事务里提交，因此进程
中断或整批失败都不会留下半批数据；重跑时已通过的批次直接跳过。

在仓库根目录运行:

    python3 pipeline.py

默认读取 batches/ 与 rules.yaml，写入 warehouse.db、quality_report.txt 与
quarantine/。
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import os
import re
import sqlite3
import sys
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

LOG = logging.getLogger("pipeline")

BASE_COLUMNS: tuple[str, ...] = (
    "txn_id",
    "account",
    "amount",
    "currency",
    "txn_date",
    "batch_id",
)

ROW_SCOPE = "row"
BATCH_SCOPE = "batch"
SCOPE_LABELS = {ROW_SCOPE: "逐行", BATCH_SCOPE: "整批"}

STATUS_PASSED = "passed"
STATUS_FAILED = "failed"

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_HALTED = 2
EXIT_INTERRUPTED = 130

# 规则类型到检查层级（逐行/整批）的映射，归类依据是判据语义，而不是规则名。
_KIND_SCOPE: dict[str, str] = {
    "strictly_greater": ROW_SCOPE,
    "in_set": ROW_SCOPE,
    "format_pattern": ROW_SCOPE,
    "unique_in_batch": BATCH_SCOPE,
    "batch_sum_limit": BATCH_SCOPE,
}

# 格式类规则支持的字符类别（封闭枚举，不做开放文本猜测）。
_FIRST_CHAR_CLASSES: dict[str, str] = {
    "英文字母": "A-Za-z",
    "字母": "A-Za-z",
    "数字": "0-9",
    "字母或数字": "A-Za-z0-9",
}
_FOLLOW_CHAR_CLASSES: dict[str, str] = {
    "数字": "0-9",
    "英文字母": "A-Za-z",
    "字母": "A-Za-z",
    "字母或数字": "A-Za-z0-9",
}


class PipelineConfigError(Exception):
    """运行环境或规则配置错误（退出码 1，不会处理任何批次）。"""


class RuleConfigError(PipelineConfigError):
    """rules.yaml 的内容无法解释成可执行的规则。"""


class BatchRejected(Exception):
    """批次在读取阶段就被判定为整批不可信（结构性校验失败）。"""


@dataclass(frozen=True)
class Rule:
    """一条已解析的质量规则。"""

    name: str
    field: str
    scope: str
    kind: str
    params: Mapping[str, Any]

    @property
    def scope_label(self) -> str:
        return SCOPE_LABELS[self.scope]


@dataclass(frozen=True)
class BatchRuleHit:
    """整批规则的一次命中。"""

    rule_name: str
    hit_rows: int
    detail: str


@dataclass(frozen=True)
class QuarantineRow:
    """一条被隔离的行（保留原始字段值）。"""

    row_no: int
    values: Mapping[str, str]
    reasons: tuple[str, ...]


@dataclass
class BatchOutcome:
    """一个批次在本次运行中的处理结果。"""

    batch_id: str
    source_file: str
    status: str
    total_rows: int = 0
    ingested_rows: int = 0
    quarantined_rows: int = 0
    quarantine_reason_counts: dict[str, int] = field(default_factory=dict)
    row_rule_hits: dict[str, int] = field(default_factory=dict)
    batch_rule_hits: dict[str, int] = field(default_factory=dict)
    failure_reason: str | None = None
    skipped: bool = False


@dataclass(frozen=True)
class PipelineConfig:
    """一次运行的路径配置。"""

    rules_path: Path = Path("rules.yaml")
    batches_dir: Path = Path("batches")
    db_path: Path = Path("warehouse.db")
    report_path: Path = Path("quality_report.txt")
    quarantine_dir: Path = Path("quarantine")


@dataclass
class RunSummary:
    """一次运行的汇总，供报告渲染使用。"""

    rules: list[Rule]
    config: PipelineConfig
    processed: list[BatchOutcome] = field(default_factory=list)
    skipped: list[BatchOutcome] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    halted: bool = False
    interrupted: bool = False
    started_at: str = ""
    finished_at: str = ""


def _now() -> str:
    return datetime.now(tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _parse_decimal(raw: str) -> Decimal | None:
    """把 CSV 里的字符串解析成有限 Decimal；无法解析返回 None。"""
    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError):
        return None
    return value if value.is_finite() else None


def _rule_decimal(value: Any, where: str, key: str) -> Decimal:
    if value is None or isinstance(value, bool):
        raise RuleConfigError(f"{where} 缺少有效的「{key}」")
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise RuleConfigError(f"{where} 的「{key}」不是数值：{value!r}") from exc
    if not number.is_finite():
        raise RuleConfigError(f"{where} 的「{key}」不是有限数值：{value!r}")
    return number


def load_rules(path: Path) -> list[Rule]:
    """读取并解析 rules.yaml；任何解释不了的规则都直接报错，不静默跳过。"""
    if not path.exists():
        raise RuleConfigError(f"规则文件不存在：{path}")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuleConfigError(f"规则文件无法读取：{path}（{exc}）") from exc
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise RuleConfigError(f"规则文件 YAML 解析失败：{path}（{exc}）") from exc
    if not isinstance(data, Mapping):
        raise RuleConfigError(f"规则文件顶层应为键值结构：{path}")

    entries = data.get("规则清单")
    if not isinstance(entries, list) or not entries:
        raise RuleConfigError(f"规则文件缺少非空的「规则清单」：{path}")

    rules: list[Rule] = []
    names: set[str] = set()
    for index, entry in enumerate(entries, start=1):
        rule = _parse_rule(index, entry, path)
        if rule.name in names:
            raise RuleConfigError(f"{path} 第 {index} 条规则与前面的规则重名：{rule.name}")
        names.add(rule.name)
        rules.append(rule)
    return rules


def _parse_rule(index: int, entry: Any, path: Path) -> Rule:
    where = f"{path} 第 {index} 条规则"
    if not isinstance(entry, Mapping):
        raise RuleConfigError(f"{where} 不是键值结构")

    name = entry.get("规则名")
    if not isinstance(name, str) or not name.strip():
        raise RuleConfigError(f"{where} 缺少「规则名」")
    field_name = entry.get("字段")
    if not isinstance(field_name, str) or not field_name.strip():
        raise RuleConfigError(f"{where}（{name}）缺少「字段」")

    requirement = entry.get("要求")
    if requirement is not None:
        kind, params = _parse_requirement(str(requirement).strip(), entry, where, name)
    elif any(key in entry for key in ("首字符", "后续字符", "后续位数")):
        kind, params = "format_pattern", _parse_format(entry, where, name)
    else:
        raise RuleConfigError(
            f"{where}（{name}）既没有「要求」也没有格式规则字段，无法归类执行"
        )

    return Rule(
        name=name.strip(),
        field=field_name.strip(),
        scope=_KIND_SCOPE[kind],
        kind=kind,
        params=params,
    )


def _parse_requirement(
    requirement: str, entry: Mapping[str, Any], where: str, name: str
) -> tuple[str, Mapping[str, Any]]:
    if requirement == "严格大于":
        return "strictly_greater", {"limit": _rule_decimal(entry.get("界限值"), where, "界限值")}
    if requirement == "合计不超过":
        return "batch_sum_limit", {"limit": _rule_decimal(entry.get("上限"), where, "上限")}
    if requirement == "取值属于":
        allowed = entry.get("允许值")
        if not isinstance(allowed, list) or not allowed:
            raise RuleConfigError(f"{where}（{name}）的「允许值」应为非空列表")
        if any(isinstance(value, (list, Mapping)) for value in allowed):
            raise RuleConfigError(f"{where}（{name}）的「允许值」只支持标量取值")
        return "in_set", {"allowed": tuple(str(value) for value in allowed)}
    if requirement == "批内不重复":
        return "unique_in_batch", {}
    raise RuleConfigError(
        f"{where}（{name}）的「要求」暂不支持：{requirement}；"
        "已支持：严格大于、取值属于、批内不重复、合计不超过，以及首字符/后续字符/后续位数格式规则"
    )


def _parse_format(entry: Mapping[str, Any], where: str, name: str) -> Mapping[str, Any]:
    missing = [key for key in ("首字符", "后续字符", "后续位数") if key not in entry]
    if missing:
        raise RuleConfigError(f"{where}（{name}）的格式规则缺少：{'、'.join(missing)}")

    first = entry["首字符"]
    follow = entry["后续字符"]
    digits = entry["后续位数"]
    if not isinstance(first, str) or first not in _FIRST_CHAR_CLASSES:
        raise RuleConfigError(
            f"{where}（{name}）的「首字符」只支持：{'、'.join(_FIRST_CHAR_CLASSES)}"
        )
    if not isinstance(follow, str) or follow not in _FOLLOW_CHAR_CLASSES:
        raise RuleConfigError(
            f"{where}（{name}）的「后续字符」只支持：{'、'.join(_FOLLOW_CHAR_CLASSES)}"
        )
    if isinstance(digits, bool) or not isinstance(digits, int) or digits < 0:
        raise RuleConfigError(f"{where}（{name}）的「后续位数」应为非负整数")

    tail = f"[{_FOLLOW_CHAR_CLASSES[follow]}]{{{digits}}}" if digits else ""
    pattern = re.compile(f"[{_FIRST_CHAR_CLASSES[first]}]{tail}")
    return {"pattern": pattern}


@dataclass
class _BatchLoad:
    """批次读取结果；error 非空表示结构性问题（整批不可信）。"""

    batch_id: str
    frame: pd.DataFrame | None = None
    error: str | None = None


def _filename_batch_id(path: Path) -> str:
    """文件名兜底批号：batch_xxx.csv 取 xxx，其余取文件名主干。"""
    match = re.fullmatch(r"batch_(.+)", path.stem)
    return match.group(1) if match else path.stem


def _load_batch(path: Path, rules: Iterable[Rule]) -> _BatchLoad:
    """读取 CSV 并做结构性校验，不涉及业务规则。"""
    fallback_id = _filename_batch_id(path)
    try:
        frame = pd.read_csv(
            path, dtype=str, keep_default_na=False, encoding="utf-8-sig"
        )
    except pd.errors.EmptyDataError:
        return _BatchLoad(fallback_id, error="文件为空（没有表头与数据）")
    except (pd.errors.ParserError, UnicodeDecodeError, OSError) as exc:
        return _BatchLoad(fallback_id, error=f"CSV 无法解析：{exc}")

    frame = frame.reset_index(drop=True)
    if frame.columns.duplicated().any():
        duplicated = sorted({str(col) for col in frame.columns[frame.columns.duplicated()]})
        return _BatchLoad(fallback_id, error=f"表头存在重复字段：{'、'.join(duplicated)}")

    required = list(BASE_COLUMNS)
    required.extend(rule.field for rule in rules if rule.field not in required)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        return _BatchLoad(fallback_id, error=f"缺少必需字段：{'、'.join(missing)}")

    if len(frame) == 0:
        return _BatchLoad(fallback_id, error="批次没有任何数据行")

    raw_ids = [str(value) for value in frame["batch_id"].tolist()]
    ids = sorted({value.strip() for value in raw_ids if value.strip()})
    if len(ids) != 1:
        shown = "全部为空" if not ids else "、".join(ids)
        return _BatchLoad(
            fallback_id,
            error=f"batch_id 列应只含一个非空批号，实际为：{shown}",
        )
    batch_id = ids[0]

    blank_rows = [
        position + 1
        for position, value in enumerate(frame["txn_id"].tolist())
        if str(value).strip() == ""
    ]
    if blank_rows:
        shown = "、".join(str(row) for row in blank_rows[:5])
        more = " 等" if len(blank_rows) > 5 else ""
        return _BatchLoad(
            batch_id,
            error=f"交易号为空，无法唯一标识行（数据行：{shown}{more}）",
        )

    return _BatchLoad(batch_id, frame=frame)


def _row_failures(rule: Rule, frame: pd.DataFrame) -> dict[int, str]:
    """逐行规则的判定，返回 行序号(从 0 起) -> 失败说明。"""
    values = [str(value) for value in frame[rule.field].tolist()]
    failures: dict[int, str] = {}
    if rule.kind == "strictly_greater":
        limit: Decimal = rule.params["limit"]
        for position, raw in enumerate(values):
            number = _parse_decimal(raw)
            if number is None or number <= limit:
                failures[position] = f"取值 {raw!r} 未严格大于 {limit}"
    elif rule.kind == "in_set":
        allowed = set(rule.params["allowed"])
        for position, raw in enumerate(values):
            if raw not in allowed:
                failures[position] = f"取值 {raw!r} 不在允许列表"
    elif rule.kind == "format_pattern":
        pattern: re.Pattern[str] = rule.params["pattern"]
        for position, raw in enumerate(values):
            if pattern.fullmatch(raw) is None:
                failures[position] = f"取值 {raw!r} 不符合格式"
    else:  # pragma: no cover - 解析阶段已经挡住未知类型
        raise PipelineConfigError(f"未知的逐行规则类型：{rule.kind}")
    return failures


def _evaluate_row_rules(rules: Iterable[Rule], frame: pd.DataFrame) -> dict[int, list[str]]:
    """执行所有逐行规则，返回 行序号 -> 命中的规则名（按规则清单顺序）。"""
    failures: dict[int, list[str]] = {}
    for rule in rules:
        if rule.scope != ROW_SCOPE:
            continue
        for position in _row_failures(rule, frame):
            failures.setdefault(position, []).append(rule.name)
    return failures


def _batch_rule_hit(rule: Rule, frame: pd.DataFrame, context: str) -> BatchRuleHit | None:
    if rule.kind == "unique_in_batch":
        values = [str(value) for value in frame[rule.field].tolist()]
        counts = Counter(values)
        duplicated = sorted(value for value, count in counts.items() if count > 1)
        if not duplicated:
            return None
        hit_rows = sum(1 for value in values if counts[value] > 1)
        detail = "交易号重复：" + "、".join(
            f"{value} 出现 {counts[value]} 次" for value in duplicated
        )
        return BatchRuleHit(rule.name, hit_rows, detail)

    if rule.kind == "batch_sum_limit":
        total = Decimal(0)
        unparsable = 0
        for raw in frame[rule.field].tolist():
            number = _parse_decimal(str(raw))
            if number is None:
                unparsable += 1
            else:
                total += number
        if unparsable:
            LOG.warning(
                "%s：%d 行金额无法解析，批内合计只按可解析行计算",
                context,
                unparsable,
            )
        limit: Decimal = rule.params["limit"]
        if total > limit:
            return BatchRuleHit(rule.name, len(frame), f"批内合计 {total} 超过上限 {limit}")
        return None

    raise PipelineConfigError(f"未知的整批规则类型：{rule.kind}")  # pragma: no cover


def _evaluate_batch_rules(
    rules: Iterable[Rule], frame: pd.DataFrame, context: str
) -> list[BatchRuleHit]:
    hits: list[BatchRuleHit] = []
    for rule in rules:
        if rule.scope != BATCH_SCOPE:
            continue
        hit = _batch_rule_hit(rule, frame, context)
        if hit is not None:
            hits.append(hit)
    return hits


_SCHEMA: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS batches (
        batch_id TEXT PRIMARY KEY,
        source_file TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('passed', 'failed')),
        total_rows INTEGER NOT NULL,
        ingested_rows INTEGER NOT NULL,
        quarantined_rows INTEGER NOT NULL,
        failure_reason TEXT,
        content_sha256 TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS transactions (
        row_no INTEGER NOT NULL,
        txn_id TEXT NOT NULL,
        account TEXT NOT NULL,
        amount TEXT NOT NULL,
        currency TEXT NOT NULL,
        txn_date TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        ingested_at TEXT NOT NULL,
        PRIMARY KEY (batch_id, row_no)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quarantined_rows (
        batch_id TEXT NOT NULL,
        row_no INTEGER NOT NULL,
        txn_id TEXT NOT NULL,
        account TEXT NOT NULL,
        amount TEXT NOT NULL,
        currency TEXT NOT NULL,
        txn_date TEXT NOT NULL,
        PRIMARY KEY (batch_id, row_no)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS row_rule_hits (
        batch_id TEXT NOT NULL,
        row_no INTEGER NOT NULL,
        rule_name TEXT NOT NULL,
        PRIMARY KEY (batch_id, row_no, rule_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batch_rule_hits (
        batch_id TEXT NOT NULL,
        rule_name TEXT NOT NULL,
        hit_rows INTEGER NOT NULL,
        PRIMARY KEY (batch_id, rule_name)
    )
    """,
)


def _connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    for statement in _SCHEMA:
        connection.execute(statement)
    connection.commit()
    return connection


def _fetch_batch(connection: sqlite3.Connection, batch_id: str) -> sqlite3.Row | None:
    cursor = connection.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,))
    return cursor.fetchone()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_outcome(
    connection: sqlite3.Connection,
    *,
    outcome: BatchOutcome,
    content_sha256: str,
    timestamp: str,
    ingested_frame: pd.DataFrame | None,
    quarantine_rows: Sequence[QuarantineRow],
    row_rule_hit_rows: Sequence[tuple[int, str]],
    batch_rule_hit_rows: Sequence[tuple[str, int]],
) -> None:
    """把批次结果整体写进数据库；删除与写入同一个事务，重试不会叠加数据。"""
    with connection:
        connection.execute("DELETE FROM transactions WHERE batch_id = ?", (outcome.batch_id,))
        connection.execute(
            "DELETE FROM quarantined_rows WHERE batch_id = ?", (outcome.batch_id,)
        )
        connection.execute("DELETE FROM row_rule_hits WHERE batch_id = ?", (outcome.batch_id,))
        connection.execute(
            "DELETE FROM batch_rule_hits WHERE batch_id = ?", (outcome.batch_id,)
        )

        if ingested_frame is not None and len(ingested_frame) > 0:
            records = [
                (
                    int(row_index) + 1,
                    str(txn_id),
                    str(account),
                    str(amount),
                    str(currency),
                    str(txn_date),
                    outcome.batch_id,
                    timestamp,
                )
                for row_index, (txn_id, account, amount, currency, txn_date) in zip(
                    ingested_frame.index,
                    ingested_frame[list(BASE_COLUMNS[:-1])].itertuples(
                        index=False, name=None
                    ),
                )
            ]
            connection.executemany(
                "INSERT INTO transactions (row_no, txn_id, account, amount, currency,"
                " txn_date, batch_id, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                records,
            )

        if quarantine_rows:
            connection.executemany(
                "INSERT INTO quarantined_rows (batch_id, row_no, txn_id, account,"
                " amount, currency, txn_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        outcome.batch_id,
                        row.row_no,
                        str(row.values["txn_id"]),
                        str(row.values["account"]),
                        str(row.values["amount"]),
                        str(row.values["currency"]),
                        str(row.values["txn_date"]),
                    )
                    for row in quarantine_rows
                ],
            )

        if row_rule_hit_rows:
            connection.executemany(
                "INSERT INTO row_rule_hits (batch_id, row_no, rule_name) VALUES (?, ?, ?)",
                [
                    (outcome.batch_id, row_no, rule_name)
                    for row_no, rule_name in row_rule_hit_rows
                ],
            )

        if batch_rule_hit_rows:
            connection.executemany(
                "INSERT INTO batch_rule_hits (batch_id, rule_name, hit_rows)"
                " VALUES (?, ?, ?)",
                [
                    (outcome.batch_id, rule_name, hit_rows)
                    for rule_name, hit_rows in batch_rule_hit_rows
                ],
            )

        connection.execute(
            """
            INSERT INTO batches (batch_id, source_file, status, total_rows, ingested_rows,
                quarantined_rows, failure_reason, content_sha256, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(batch_id) DO UPDATE SET
                source_file = excluded.source_file,
                status = excluded.status,
                total_rows = excluded.total_rows,
                ingested_rows = excluded.ingested_rows,
                quarantined_rows = excluded.quarantined_rows,
                failure_reason = excluded.failure_reason,
                content_sha256 = excluded.content_sha256,
                updated_at = excluded.updated_at
            """,
            (
                outcome.batch_id,
                outcome.source_file,
                outcome.status,
                outcome.total_rows,
                outcome.ingested_rows,
                outcome.quarantined_rows,
                outcome.failure_reason,
                content_sha256,
                timestamp,
            ),
        )


def process_batch(
    connection: sqlite3.Connection,
    rules: Sequence[Rule],
    path: Path,
    content_sha256: str,
    timestamp: str,
) -> BatchOutcome:
    """处理单个批次文件：已通过的跳过，未通过的重新判定并落库。"""
    load = _load_batch(path, rules)
    batch_id = load.batch_id

    existing = _fetch_batch(connection, batch_id)
    if existing is not None and existing["source_file"] != path.name:
        raise PipelineConfigError(
            f"批号 {batch_id} 已由 {existing['source_file']} 处理，"
            f"又与 {path.name} 冲突；请先人工确认并清理数据库"
        )
    if existing is not None and existing["status"] == STATUS_PASSED:
        if existing["content_sha256"] != content_sha256:
            LOG.warning(
                "批次 %s（%s）已入库，但文件内容与入库时不同；按约定不重跑",
                batch_id,
                path.name,
            )
        LOG.info("跳过已入库批次 %s（%s）", batch_id, path.name)
        return BatchOutcome(
            batch_id=batch_id,
            source_file=path.name,
            status=STATUS_PASSED,
            skipped=True,
        )

    if load.error is not None:
        LOG.error("批次 %s（%s）整批不合格：%s", batch_id, path.name, load.error)
        outcome = BatchOutcome(
            batch_id=batch_id,
            source_file=path.name,
            status=STATUS_FAILED,
            failure_reason=load.error,
        )
        _record_outcome(
            connection,
            outcome=outcome,
            content_sha256=content_sha256,
            timestamp=timestamp,
            ingested_frame=None,
            quarantine_rows=(),
            row_rule_hit_rows=(),
            batch_rule_hit_rows=(),
        )
        return outcome

    frame = load.frame
    if frame is None:  # pragma: no cover - _load_batch 的不变量
        raise PipelineConfigError(f"批次 {batch_id} 读取结果缺少数据：{path}")

    row_failures = _evaluate_row_rules(rules, frame)
    row_rule_hits = Counter(
        rule_name for names in row_failures.values() for rule_name in names
    )
    row_rule_hit_rows = [
        (position + 1, rule_name)
        for position, names in row_failures.items()
        for rule_name in names
    ]
    batch_hits = _evaluate_batch_rules(rules, frame, f"批次 {batch_id}（{path.name}）")

    if batch_hits:
        reason = "；".join(f"{hit.rule_name}：{hit.detail}" for hit in batch_hits)
        LOG.error("批次 %s（%s）整批不合格：%s", batch_id, path.name, reason)
        outcome = BatchOutcome(
            batch_id=batch_id,
            source_file=path.name,
            status=STATUS_FAILED,
            total_rows=len(frame),
            row_rule_hits=dict(row_rule_hits),
            batch_rule_hits={hit.rule_name: hit.hit_rows for hit in batch_hits},
            failure_reason=reason,
        )
        _record_outcome(
            connection,
            outcome=outcome,
            content_sha256=content_sha256,
            timestamp=timestamp,
            ingested_frame=None,
            quarantine_rows=(),
            row_rule_hit_rows=row_rule_hit_rows,
            batch_rule_hit_rows=[(hit.rule_name, hit.hit_rows) for hit in batch_hits],
        )
        return outcome

    quarantined_positions = sorted(row_failures)
    quarantine_rows = [
        QuarantineRow(
            row_no=position + 1,
            values={column: str(frame.at[position, column]) for column in BASE_COLUMNS},
            reasons=tuple(row_failures[position]),
        )
        for position in quarantined_positions
    ]
    ingested = (
        frame.drop(index=quarantined_positions) if quarantined_positions else frame
    )
    outcome = BatchOutcome(
        batch_id=batch_id,
        source_file=path.name,
        status=STATUS_PASSED,
        total_rows=len(frame),
        ingested_rows=len(ingested),
        quarantined_rows=len(quarantine_rows),
        quarantine_reason_counts=dict(row_rule_hits),
        row_rule_hits=dict(row_rule_hits),
    )
    _record_outcome(
        connection,
        outcome=outcome,
        content_sha256=content_sha256,
        timestamp=timestamp,
        ingested_frame=ingested,
        quarantine_rows=quarantine_rows,
        row_rule_hit_rows=row_rule_hit_rows,
        batch_rule_hit_rows=(),
    )
    LOG.info(
        "批次 %s（%s）：读取 %d 行，入库 %d 行，隔离 %d 行",
        batch_id,
        path.name,
        outcome.total_rows,
        outcome.ingested_rows,
        outcome.quarantined_rows,
    )
    return outcome


def _discover_batch_files(batches_dir: Path) -> list[Path]:
    if not batches_dir.exists():
        raise PipelineConfigError(f"批次目录不存在：{batches_dir}")
    if not batches_dir.is_dir():
        raise PipelineConfigError(f"批次路径不是目录：{batches_dir}")
    files = sorted(
        (
            path
            for path in batches_dir.iterdir()
            if path.is_file() and path.suffix.lower() == ".csv"
        ),
        key=lambda path: path.name,
    )
    return files


def _sync_quarantine_files(
    connection: sqlite3.Connection, config: PipelineConfig, rules: Sequence[Rule]
) -> None:
    """按数据库中的隔离行重写 quarantine 文件，保证文件与库内状态一致。"""
    config.quarantine_dir.mkdir(parents=True, exist_ok=True)
    rows = connection.execute(
        """
        SELECT q.batch_id, q.row_no, q.txn_id, q.account, q.amount, q.currency,
               q.txn_date, b.source_file
        FROM quarantined_rows q
        JOIN batches b ON b.batch_id = q.batch_id
        ORDER BY q.batch_id, q.row_no
        """
    ).fetchall()
    if not rows:
        return

    reason_rows = connection.execute(
        "SELECT batch_id, row_no, rule_name FROM row_rule_hits"
        " ORDER BY batch_id, row_no, rule_name"
    ).fetchall()
    reasons: dict[tuple[str, int], list[str]] = {}
    for row in reason_rows:
        reasons.setdefault((row["batch_id"], row["row_no"]), []).append(row["rule_name"])

    by_file: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        by_file.setdefault(row["source_file"], []).append(row)

    order = {rule.name: index for index, rule in enumerate(rules)}
    for source_file, file_rows in by_file.items():
        target = config.quarantine_dir / source_file
        tmp_path = target.with_name(target.name + ".tmp")
        try:
            with tmp_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow([*BASE_COLUMNS, "违规规则", "数据行号"])
                for row in file_rows:
                    names = reasons.get((row["batch_id"], row["row_no"]), [])
                    ordered = sorted(names, key=lambda name: order.get(name, len(order)))
                    writer.writerow(
                        [
                            row["txn_id"],
                            row["account"],
                            row["amount"],
                            row["currency"],
                            row["txn_date"],
                            row["batch_id"],
                            "; ".join(ordered),
                            row["row_no"],
                        ]
                    )
            os.replace(tmp_path, target)
        except OSError:
            LOG.exception("写入隔离文件失败：%s", target)
            tmp_path.unlink(missing_ok=True)
            raise
        LOG.info("隔离文件已写入 %s（%d 行）", target, len(file_rows))


def run_pipeline(config: PipelineConfig) -> RunSummary:
    """跑一轮管道：按文件名顺序处理，遇到整批失败就停在当前批次。"""
    started_at = _now()
    rules = load_rules(config.rules_path)
    files = _discover_batch_files(config.batches_dir)
    LOG.info("规则 %d 条，批次文件 %d 个", len(rules), len(files))

    connection = _connect_db(config.db_path)
    summary = RunSummary(rules=rules, config=config, started_at=started_at)
    current_index = 0
    try:
        for current_index, path in enumerate(files):
            outcome = process_batch(
                connection,
                rules,
                path,
                _hash_file(path),
                _now(),
            )
            if outcome.skipped:
                summary.skipped.append(outcome)
                continue
            summary.processed.append(outcome)
            if outcome.status == STATUS_FAILED:
                summary.halted = True
                summary.pending = [item.name for item in files[current_index + 1 :]]
                LOG.error(
                    "批次 %s 整批不合格，运行在此中断；修数后重跑会从该批继续",
                    outcome.batch_id,
                )
                break
    except KeyboardInterrupt:
        summary.interrupted = True
        summary.pending = [item.name for item in files[current_index:]]
        LOG.error("运行被中断；已提交批次保持入库状态，重跑从未通过批次继续")
    finally:
        summary.finished_at = _now()
        try:
            _sync_quarantine_files(connection, config, rules)
        except (OSError, sqlite3.Error):
            LOG.exception("同步 quarantine 目录失败")
        try:
            write_report(connection, summary)
            LOG.info("质量报告已写入 %s", config.report_path)
        except (OSError, sqlite3.Error):
            LOG.exception("写入质量报告失败")
        connection.close()
    return summary


@dataclass
class CumulativeStats:
    """数据库里全部批次的累计统计。"""

    passed_batches: int = 0
    failed_batches: int = 0
    ingested_rows: int = 0
    quarantined_rows: int = 0
    failed_rows: int = 0
    reason_counts: dict[str, int] = field(default_factory=dict)
    rule_batches: dict[str, int] = field(default_factory=dict)
    rule_rows: dict[str, int] = field(default_factory=dict)


def _cumulative_stats(connection: sqlite3.Connection) -> CumulativeStats:
    stats = CumulativeStats()
    for row in connection.execute(
        """
        SELECT status, COUNT(*) AS batch_count, COALESCE(SUM(total_rows), 0) AS total,
               COALESCE(SUM(ingested_rows), 0) AS ingested,
               COALESCE(SUM(quarantined_rows), 0) AS quarantined
        FROM batches GROUP BY status
        """
    ).fetchall():
        if row["status"] == STATUS_PASSED:
            stats.passed_batches = int(row["batch_count"])
            stats.ingested_rows = int(row["ingested"])
            stats.quarantined_rows = int(row["quarantined"])
        else:
            stats.failed_batches = int(row["batch_count"])
            stats.failed_rows = int(row["total"])

    for row in connection.execute(
        """
        SELECT h.rule_name, COUNT(*) AS hit_rows
        FROM row_rule_hits h
        JOIN quarantined_rows q ON q.batch_id = h.batch_id AND q.row_no = h.row_no
        GROUP BY h.rule_name
        """
    ).fetchall():
        stats.reason_counts[row["rule_name"]] = int(row["hit_rows"])

    for row in connection.execute(
        """
        SELECT rule_name, COUNT(DISTINCT batch_id) AS batches, SUM(hit_rows) AS rows
        FROM (
            SELECT batch_id, rule_name, COUNT(*) AS hit_rows
            FROM row_rule_hits GROUP BY batch_id, rule_name
            UNION ALL
            SELECT batch_id, rule_name, hit_rows FROM batch_rule_hits
        )
        GROUP BY rule_name
        """
    ).fetchall():
        stats.rule_batches[row["rule_name"]] = int(row["batches"])
        stats.rule_rows[row["rule_name"]] = int(row["rows"])
    return stats


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    try:
        tmp_path.write_text(text, encoding="utf-8")
        os.replace(tmp_path, path)
    except OSError:
        tmp_path.unlink(missing_ok=True)
        raise


def render_report(summary: RunSummary, cumulative: CumulativeStats) -> str:
    """生成 quality_report.txt 的正文。"""
    processed = summary.processed
    passed = [item for item in processed if item.status == STATUS_PASSED]
    failed = [item for item in processed if item.status == STATUS_FAILED]

    rows_read = sum(item.total_rows for item in processed)
    ingested_rows = sum(item.ingested_rows for item in passed)
    quarantined_rows = sum(item.quarantined_rows for item in passed)
    failed_rows = sum(item.total_rows for item in failed)

    reason_counts: Counter[str] = Counter()
    for item in passed:
        reason_counts.update(item.quarantine_reason_counts)

    rule_batches: Counter[str] = Counter()
    rule_rows: Counter[str] = Counter()
    for item in processed:
        for rule_name, hit_rows in item.row_rule_hits.items():
            if hit_rows:
                rule_batches[rule_name] += 1
                rule_rows[rule_name] += hit_rows
        for rule_name, hit_rows in item.batch_rule_hits.items():
            if hit_rows:
                rule_batches[rule_name] += 1
                rule_rows[rule_name] += hit_rows

    if summary.interrupted:
        run_state = "已中断（进程被中断），重跑从未通过的批次继续"
    elif summary.halted:
        run_state = "已中断（整批不合格），等待修数后重跑"
    else:
        run_state = "已完成"

    config = summary.config
    lines: list[str] = [
        "交易明细质量管道 质量报告",
        "=" * 60,
        f"生成时间：{summary.finished_at or _now()}",
        f"本轮开始：{summary.started_at}",
        f"规则文件：{config.rules_path}",
        f"批次目录：{config.batches_dir}",
        f"数据库：{config.db_path}",
        f"隔离目录：{config.quarantine_dir}",
        "",
        "一、本轮运行概况",
        f"  处理批次数：{len(processed)}（整批通过 {len(passed)}，整批失败 {len(failed)}）",
        f"  跳过（已入库）批次：{len(summary.skipped)}"
        + (f"（{'、'.join(item.batch_id for item in summary.skipped)}）" if summary.skipped else ""),
        f"  未处理批次：{len(summary.pending)}"
        + (f"（{'、'.join(summary.pending)}）" if summary.pending else ""),
        f"  运行状态：{run_state}",
    ]
    if failed:
        last = failed[-1]
        lines.append(f"  中断原因：批次 {last.batch_id} 整批不合格（{last.failure_reason}）")
    lines.append("")

    lines.extend(
        [
            "二、按行视角（本轮）",
            f"  读取行数：{rows_read}",
            f"  合格数（已入库行数）：{ingested_rows}",
            f"  隔离数：{quarantined_rows}",
            f"  整批失败未入库行数：{failed_rows}",
            "  各原因计数（隔离行按命中规则计，一条行可计多个原因）：",
        ]
    )
    for rule in summary.rules:
        if rule.scope == ROW_SCOPE:
            lines.append(f"    {rule.name}：{reason_counts.get(rule.name, 0)}")
    lines.append("")

    lines.append("三、按规则视角（本轮）")
    lines.append("  规则名 | 类型 | 命中批次数 | 命中行数")
    for rule in summary.rules:
        lines.append(
            f"  {rule.name} | {rule.scope_label} |"
            f" {rule_batches.get(rule.name, 0)} | {rule_rows.get(rule.name, 0)}"
        )
    lines.append(
        "  注：整批规则的命中行数取该规则涉及的行数（交易号重复取涉重复行，"
        "合计超限取整批行数）；"
    )
    lines.append(
        "      规则视角包含整批失败批次中已检出的行级问题，行视角的隔离数只算"
        "整批通过批次中被隔离的行。"
    )
    lines.append("")

    lines.append("四、批次明细（本轮）")
    if processed:
        lines.append(
            "  批次号 | 文件 | 状态 | 读取行数 | 入库行数 | 隔离行数 | 失败原因"
        )
        for item in processed:
            status = "整批通过" if item.status == STATUS_PASSED else "整批失败"
            lines.append(
                f"  {item.batch_id} | {item.source_file} | {status} |"
                f" {item.total_rows} | {item.ingested_rows} | {item.quarantined_rows} |"
                f" {item.failure_reason or '-'}"
            )
    else:
        lines.append("  本轮没有处理任何批次")
    lines.append("")

    lines.extend(
        [
            "五、累计视图（数据库内全部批次）",
            (
                f"  已处理批次：{cumulative.passed_batches + cumulative.failed_batches}"
                f"（通过 {cumulative.passed_batches}，失败 {cumulative.failed_batches}）"
            ),
            f"  已入库行数：{cumulative.ingested_rows}",
            f"  隔离行数：{cumulative.quarantined_rows}",
            f"  整批失败未入库行数：{cumulative.failed_rows}",
            "  各原因计数：",
        ]
    )
    for rule in summary.rules:
        if rule.scope == ROW_SCOPE:
            lines.append(f"    {rule.name}：{cumulative.reason_counts.get(rule.name, 0)}")
    lines.append("  规则命中累计：")
    lines.append("  规则名 | 类型 | 命中批次数 | 命中行数")
    for rule in summary.rules:
        lines.append(
            f"  {rule.name} | {rule.scope_label} |"
            f" {cumulative.rule_batches.get(rule.name, 0)} |"
            f" {cumulative.rule_rows.get(rule.name, 0)}"
        )
    lines.append("")
    return "\n".join(lines)


def write_report(connection: sqlite3.Connection, summary: RunSummary) -> None:
    cumulative = _cumulative_stats(connection)
    _atomic_write_text(
        summary.config.report_path,
        render_report(summary, cumulative),
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="交易明细质量管道：逐行隔离、整批拦截、断点续跑"
    )
    parser.add_argument("--rules", default="rules.yaml", help="规则文件路径")
    parser.add_argument("--batches", default="batches", help="批次 CSV 目录")
    parser.add_argument("--db", default="warehouse.db", help="SQLite 数据库路径")
    parser.add_argument("--report", default="quality_report.txt", help="质量报告输出路径")
    parser.add_argument("--quarantine", default="quarantine", help="隔离目录")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="日志级别",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    config = PipelineConfig(
        rules_path=Path(args.rules),
        batches_dir=Path(args.batches),
        db_path=Path(args.db),
        report_path=Path(args.report),
        quarantine_dir=Path(args.quarantine),
    )
    try:
        summary = run_pipeline(config)
    except PipelineConfigError as exc:
        LOG.error("%s", exc)
        return EXIT_ERROR
    except KeyboardInterrupt:
        LOG.error("运行被中断；已提交批次保持入库状态，重跑从未通过批次继续")
        return EXIT_INTERRUPTED

    if summary.interrupted:
        return EXIT_INTERRUPTED
    if summary.halted:
        return EXIT_HALTED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
