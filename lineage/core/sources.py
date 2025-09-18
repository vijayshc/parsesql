from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from sqlglot import expressions as exp

from .origin import ColumnOrigin
from .schema import Schema, _norm


@dataclass
class SourceBase:
    """Abstract base for any selectable source in a FROM clause (table, CTE, subquery)."""
    def output_columns(self) -> List[str]:  # names presented by this source in SELECT * context
        raise NotImplementedError

    def resolve_column(self, name: str) -> List[ColumnOrigin]:
        """Resolve a (possibly projected) column name to ultimate physical origins."""
        raise NotImplementedError


@dataclass
class TableSource(SourceBase):
    table_name: str
    schema: Schema

    def output_columns(self) -> List[str]:
        cols = self.schema.columns(self.table_name)
        return list(cols) if cols else ['*']

    def resolve_column(self, name: str) -> List[ColumnOrigin]:
        # Even if schema doesn't list the column we still attribute to table
        return [ColumnOrigin(table=self.table_name, column=_norm(name))]


@dataclass
class SelectSource(SourceBase):
    """Represents a SELECT (CTE or subquery) as a source; performs lazy lineage analysis of its projections."""
    select: exp.Expression  # Select or Union
    env: 'AnalysisEnvironment'  # environment for nested CTE resolution
    schema: Schema
    _outputs_cache: Optional[List[str]] = field(default=None, init=False)
    _lineage_index: Optional[Dict[str, List[ColumnOrigin]]] = field(default=None, init=False)

    def _analyze_if_needed(self):
        if self._outputs_cache is not None and self._lineage_index is not None:
            return
        from .analyzer import SelectAnalyzer
        analyzer = SelectAnalyzer(self.select, self.env, self.schema)
        expr_lineages = analyzer.analyze()
        outputs: List[str] = []
        idx: Dict[str, List[ColumnOrigin]] = {}
        for el in expr_lineages:
            out_name = el.output_column
            if out_name:
                outputs.append(out_name)
                # Only set lineage if not already present (first occurrence wins)
                if out_name not in idx:
                    idx[out_name] = list(el.origins)
            # If expression is star expansion with origins but no output_column (due to star) add origin columns
            if not out_name and el.expression_sql in ('*',) and el.origins:
                for o in el.origins:
                    if o.column and o.column != '*':
                        outputs.append(o.column)
                        if o.column not in idx:
                            idx[o.column] = [o]
            # For unnamed direct column expressions (e.g., table.col) add the column name
            if not out_name and el.expression_sql and '.' in el.expression_sql and len(el.origins) == 1:
                origin = el.origins[0]
                if origin.column and origin.column not in outputs:
                    outputs.append(origin.column)
                    idx.setdefault(origin.column, []).append(origin)
        # preserve order; remove duplicates
        seen = set()
        ordered = []
        for o in outputs:
            if o not in seen:
                seen.add(o)
                ordered.append(o)
        self._outputs_cache = ordered
        self._lineage_index = idx

    def output_columns(self) -> List[str]:
        self._analyze_if_needed()
        return list(self._outputs_cache or [])

    def resolve_column(self, name: str) -> List[ColumnOrigin]:
        self._analyze_if_needed()
        n = _norm(name)
        return list(self._lineage_index.get(n, []))


@dataclass
class AnalysisEnvironment:
    """Holds named sources (CTEs) available during analysis."""
    ctes: Dict[str, SelectSource]

    def get(self, name: str) -> Optional[SelectSource]:
        return self.ctes.get(_norm(name))

    def register(self, name: str, source: SelectSource):
        self.ctes[_norm(name)] = source

    def list_cte_names(self) -> Sequence[str]:
        return list(self.ctes.keys())
