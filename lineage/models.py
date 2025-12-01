from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class LineageRecord:
    source_table: Optional[str]
    source_column: Optional[str]
    expression: str
    target_column: Optional[str]
    target_table: Optional[str]
    file: Optional[str] = None
    engine: Optional[str] = None
    source_path: tuple[str, ...] = ()  # Path of CTEs/Tables traversed

    def as_csv_row(self) -> List[str]:
        return [
            self.source_table or "",
            self.source_column or "",
            self.expression or "",
            self.target_column or "",
            self.target_table or "",
            self.file or "",
        ]


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
    "target_column",
    "target_table",
    "file",
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
