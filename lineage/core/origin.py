from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ColumnOrigin:
    """Represents a physical source table & column for lineage."""
    table: Optional[str]
    column: Optional[str]

    def as_key(self) -> tuple:
        return (self.table, self.column)


@dataclass(frozen=True)
class ExpressionLineage:
    """Mapping between a projection expression and its resolved origins."""
    expression_sql: str
    output_column: Optional[str]  # alias / resulting column name (if determinable)
    origins: tuple[ColumnOrigin, ...]
