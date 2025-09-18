from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


def _norm(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    return name.strip().strip('`').strip('"').lower()


@dataclass
class Schema:
    """Lightweight schema abstraction supporting lookups & registration.

    Accepts flattened mapping: table -> list[column]. Nested dicts will be
    flattened by dotted path externally (handled by caller of LineageExtractor).
    """

    tables: Dict[str, List[str]]

    def has_table(self, table: str) -> bool:
        return _norm(table) in self.tables

    def columns(self, table: str) -> List[str]:
        return self.tables.get(_norm(table), [])

    def has_column(self, table: str, column: str) -> bool:
        return _norm(column) in self.columns(table)

    def candidate_tables_for_column(self, column: str, tables: Iterable[str]) -> List[str]:
        c = _norm(column)
        out = []
        for t in tables:
            if c in self.columns(t):
                out.append(t)
        return out
