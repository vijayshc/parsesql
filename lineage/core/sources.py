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
        return [ColumnOrigin(table=self.table_name, column=_norm(name), expression_chain=_norm(name))]


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
        
        # Track expressions without output column names to generate synthetic names
        unnamed_count = 0
        
        for el in expr_lineages:
            out_name = el.output_column
            
                        # Generate synthetic column name for unnamed expressions
            if not out_name and el.expression_sql and el.expression_sql not in ('*',):
                # Extract the expression type or function name generically
                expr_str = el.expression_sql.strip()
                
                # Try to extract function name from function calls like func(args)
                import re
                func_match = re.match(r'^(\w+)\s*\(', expr_str.lower())
                if func_match:
                    func_name = func_match.group(1)
                    out_name = f'{func_name}_{unnamed_count}'
                else:
                    # For non-function expressions, use a generic name based on content
                    # Remove special characters and spaces to create a valid identifier
                    clean_expr = re.sub(r'[^\w]', '_', expr_str.lower())[:20]  # Truncate to reasonable length
                    if clean_expr and clean_expr != '_':
                        out_name = f'{clean_expr}_{unnamed_count}'
                    else:
                        out_name = f'col_{unnamed_count}'
                unnamed_count += 1
            
            if out_name:
                outputs.append(out_name)
                # Only set lineage if not already present (first occurrence wins)
                if out_name not in idx:
                    # Preserve expression chains as-is from the analyzer
                    idx[out_name] = list(el.origins)
            # If expression is star expansion with origins but no output_column (due to star) add origin columns
            elif not out_name and el.expression_sql in ('*',) and el.origins:
                for o in el.origins:
                    if o.column and o.column != '*':
                        outputs.append(o.column)
                        if o.column not in idx:
                            # For star expansion, preserve existing expression chains
                            idx[o.column] = [o]
            # For unnamed direct column expressions (e.g., table.col) add the column name
            elif not out_name and el.expression_sql and '.' in el.expression_sql and len(el.origins) == 1:
                origin = el.origins[0]
                if origin.column and origin.column not in outputs:
                    outputs.append(origin.column)
                    # Preserve expression chains as-is from the analyzer
                    idx.setdefault(origin.column, []).append(origin)
        
        # If no outputs were generated, create a synthetic column for star expansion fallback
        if not outputs and expr_lineages:
            outputs = ['col_0']
            if expr_lineages:
                idx['col_0'] = list(expr_lineages[0].origins) if expr_lineages[0].origins else []
        
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
