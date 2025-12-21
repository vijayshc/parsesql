from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, Sequence
import csv

REQUIRED_COLUMNS: Sequence[str] = (
    "source_table",
    "source_column",
    "expression",
    "target_column",
    "target_table",
    "file_name",
)


@dataclass(frozen=True)
class Node:
    """Graph node representing a column, result, expression, or grouping bucket."""

    id: str
    label: str
    table: str
    column: str
    file: str
    role: str
    expression: str
    parent: str | None


@dataclass(frozen=True)
class Edge:
    """Directed edge describing lineage from source -> target."""

    id: str
    source: str
    target: str
    expression: str
    file: str


@dataclass(frozen=True)
class LineageGraph:
    """Lineage graph composed of nodes and edges plus optional warnings."""

    nodes: Sequence[Node]
    edges: Sequence[Edge]
    warnings: Sequence[str]


def load_lineage_csv(path: str | Path, *, encoding: str = "utf-8") -> LineageGraph:
    """Load lineage CSV from disk and transform into a graph representation."""

    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    with csv_path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        normalized_rows = [
            {(_sanitize_key(key)): value for key, value in row.items() if key is not None}
            for row in reader
        ]

    return transform_records(normalized_rows)


def transform_records(records: Iterable[Mapping[str, str]]) -> LineageGraph:
    """Convert raw lineage rows into graph nodes and edges suitable for visualisation."""

    node_map: MutableMapping[str, Node] = {}
    edge_keys: set[str] = set()
    edges: list[Edge] = []
    warnings: list[str] = []

    for index, raw in enumerate(records, start=1):
        # Filter for Trace_level 0 (summary row) if Trace_level exists in CSV
        if raw.get("trace_level") and raw.get("trace_level") != "0":
            continue
        # Only process SELECT lineage for the main visualization
        if raw.get("lineage_type") and raw.get("lineage_type") != "SELECT":
            continue

        row = _normalise_row(raw)

        if not row["target_column"]:
            warnings.append(f"Row {index}: target column missing; skipping entry.")
            continue

        target_id = _ensure_node(
            node_map,
            table=row["target_table"],
            column=row["target_column"],
            file=row["file"],
            expression=row["expression"],
            role="table" if row["target_table"] else "result",
        )

        def add_edge(source_id: str) -> None:
            key = f"{source_id}->{target_id}->{row['expression']}"
            if key in edge_keys:
                return
            edge_keys.add(key)
            edges.append(
                Edge(
                    id=f"e{len(edge_keys)}",
                    source=source_id,
                    target=target_id,
                    expression=row["expression"],
                    file=row["file"],
                )
            )

        if row["source_table"] and row["source_column"]:
            source_id = _ensure_node(
                node_map,
                table=row["source_table"],
                column=row["source_column"],
                file=row["file"],
                expression=row["expression"],
                role="table",
            )
            add_edge(source_id)
        elif row["source_column"]:
            source_id = _ensure_node(
                node_map,
                table=row["source_table"],
                column=row["source_column"],
                file=row["file"],
                expression=row["expression"],
                role="expression",
            )
            add_edge(source_id)
        elif row["expression"]:
            source_id = _ensure_node(
                node_map,
                table="",
                column="",
                file=row["file"],
                expression=row["expression"],
                role="expression",
            )
            add_edge(source_id)

    return LineageGraph(nodes=list(node_map.values()), edges=edges, warnings=warnings)


def _ensure_node(
    node_map: MutableMapping[str, Node],
    *,
    table: str,
    column: str,
    file: str,
    expression: str,
    role: str,
) -> str:
    node_id = _build_node_id(table=table, column=column, file=file, expression=expression, role=role)

    parent_info = _resolve_parent_info(table=table, role=role, file=file)
    parent_id = parent_info["id"] if parent_info else None

    if parent_info and parent_id not in node_map:
        node_map[parent_id] = Node(
            id=parent_id,
            label=parent_info["label"],
            table=parent_info.get("table", ""),
            column="",
            file=parent_info.get("file", ""),
            role=parent_info["role"],
            expression="",
            parent=None,
        )

    if node_id not in node_map:
        node_map[node_id] = Node(
            id=node_id,
            label=_build_label(
                table=table,
                column=column,
                file=file,
                expression=expression,
                role=role,
                node_id=node_id,
            ),
            table=table,
            column=column,
            file=file,
            role=_resolve_role(table=table, column=column, role=role),
            expression=expression,
            parent=parent_id,
        )

    return node_id


def _normalise_row(row: Mapping[str, str]) -> dict[str, str]:
    lookup = {(_sanitize_key(key)): value for key, value in row.items() if key is not None}
    return {
        "source_table": _normalise_cell(lookup.get("source_table")),
        "source_column": _normalise_cell(lookup.get("source_column")),
        "expression": _normalise_cell(lookup.get("expression")),
        "target_column": _normalise_cell(lookup.get("target_column")),
        "target_table": _normalise_cell(lookup.get("target_table")),
        "file": _normalise_cell(lookup.get("file_name")),
    }


def _normalise_cell(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _sanitize_key(key: str | None) -> str:
    if key is None:
        return ""
    return str(key).strip().lower()


def _build_node_id(*, table: str, column: str, file: str, expression: str, role: str) -> str:
    table_value = table.lower().strip() if table else ""
    column_value = column.lower().strip() if column else ""
    file_value = file.lower().strip() if file else ""

    if table_value and column_value:
        return f"table::{table_value}::{column_value}"

    if role == "result" and column_value:
        scoped = file_value or "result"
        return f"result::{scoped}::{column_value}"

    basis = expression or f"{role}:{column_value or 'anonymous'}"
    return f"expr::{_hash_identifier(basis)}"


def _build_label(*, table: str, column: str, file: str, expression: str, role: str, node_id: str) -> str:
    table_value = _normalise_cell(table)
    column_value = _normalise_cell(column)

    if table_value and column_value:
        return f"{table_value}.{column_value}"

    if role == "result" and column_value:
        label_file = _truncate_middle(_normalise_cell(file) or "derived", 28)
        return f"{column_value} ({label_file})"

    if expression:
        return f"Expr: {_truncate_middle(expression, 42)}"

    return node_id.split("::", 1)[-1]


def _resolve_role(*, table: str, column: str, role: str) -> str:
    if role in {"result", "table"}:
        return role
    if table and column:
        return "table"
    if table or column:
        return "derived"
    return "expression"


def _resolve_parent_info(*, table: str, role: str, file: str) -> dict[str, str] | None:
    table_value = _normalise_cell(table)
    file_value = _normalise_cell(file)

    if table_value:
        parent_id = f"group::table::{table_value.lower()}"
        return {
            "id": parent_id,
            "label": f"Table: {table_value}",
            "role": "group-table",
            "table": table_value,
            "file": "",
        }

    if role == "result":
        key = file_value.lower() if file_value else "result"
        return {
            "id": f"group::result::{key}",
            "label": f"Result set ({file_value or 'Derived'})",
            "role": "group-result",
            "table": "",
            "file": file_value,
        }

    group_label_file = file_value or "Global"
    key = file_value.lower() if file_value else "global"
    return {
        "id": f"group::expression::{key}",
        "label": f"Expressions ({group_label_file})",
        "role": "group-expression",
        "table": "",
        "file": file_value,
    }


def _hash_identifier(value: str) -> str:
    hash_value = 0
    for character in value:
        hash_value = (hash_value << 5) - hash_value + ord(character)
        hash_value &= 0xFFFFFFFF
    hash_value = abs(hash_value)
    return f"h{hash_value}"


def _truncate_middle(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    half = max((limit - 3) // 2, 1)
    return f"{value[:half]}…{value[-half:]}"
