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

    def as_csv_row(self) -> List[str]:
        return [
            self.source_table or "",
            self.source_column or "",
            self.expression or "",
            self.target_column or "",
            self.target_table or "",
            self.file or "",
        ]


CSV_HEADER = [
    "source_table",
    "source_column",
    "expression",
    "target_column",
    "target_table",
    "file",
]
