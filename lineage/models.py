from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


from .core.origin import TraceStep

@dataclass(frozen=True)
class LineageRecord:
    source_table: Optional[str]
    source_column: Optional[str]
    trace: tuple[TraceStep, ...]  # Was expression: str
    target_column: Optional[str]
    target_table: Optional[str]
    file: Optional[str] = None
    engine: Optional[str] = None
    source_path: tuple[str, ...] = ()  # Path of CTEs/Tables traversed
    lineage_type: str = "SELECT"  # 'SELECT' or 'WHERE'

    def as_csv_rows(self) -> List[List[str]]:
        rows = []
        # If trace is empty, emit at least one row? 
        # Usually trace has at least one step if it's a valid lineage. 
        # But if not, we should probably output blank expression/alias/level?
        if not self.trace:
            return [[
                self.source_table or "",
                self.source_column or "",
                "", # expression
                "1", # trace_level
                self.target_column or "",
                self.target_table or "",
                self.file or "",
                self.lineage_type,
            ]]
        
        # Add summary row at level 0
        summary_parts = []
        for step in self.trace:
            expr = step.expression or ""
            if step.alias:
                expr = f"{expr} as {step.alias}"
            summary_parts.append(expr)
        summary_expr = "~".join(summary_parts)
        rows.append([
            self.source_table or "",
            self.source_column or "",
            summary_expr,
            "0",          # trace_level
            self.target_column or "",
            self.target_table or "",
            self.file or "",
            self.lineage_type,
        ])
        
        for i, step in enumerate(self.trace):
            expr = step.expression or ""
            if step.alias:
                expr = f"{expr} as {step.alias}"
            
            rows.append([
                self.source_table or "",
                self.source_column or "",
                expr,
                str(len(self.trace) - i), # trace_level, descending (1 = final step)
                self.target_column or "",
                self.target_table or "",
                self.file or "",
                self.lineage_type,
            ])
        return rows


@dataclass(frozen=True)
class JoinConditionRecord:
    """Represents a JOIN condition affecting a specific target column."""
    target_column: Optional[str]  # The output column affected by this JOIN
    left_table: Optional[str]
    right_table: Optional[str]
    join_type: str  # INNER, LEFT, RIGHT, FULL, CROSS
    condition_expression: str
    file: Optional[str] = None
    query_level: int = 0  # Track nesting depth for recursive queries
    source_cte: Optional[str] = None  # Track which CTE/subquery this join belongs to

    def as_csv_row(self) -> List[str]:
        return [
            self.target_column or "",
            self.left_table or "",
            self.right_table or "",
            self.join_type or "",
            self.condition_expression or "",
            self.file or "",
            str(self.query_level),
            self.source_cte or "",
        ]


CSV_HEADER = [
    "source_table",
    "source_column",
    "expression",
    "trace_level",
    "target_column",
    "target_table",
    "file",
    "lineage_type",
]

JOIN_CSV_HEADER = [
    "target_column",
    "left_table",
    "right_table",
    "join_type",
    "condition_expression",
    "file",
    "query_level",
    "source_cte",
]
