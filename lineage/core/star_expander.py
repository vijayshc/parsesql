"""
Star expansion functionality.

This module handles expanding * expressions into explicit column lists
based on schema information and CTE analysis.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import sqlglot
from sqlglot import expressions as exp

from ..utils import normalize_identifier, table_name_of, alias_to_str


class StarExpander:
    """Handles star (*) expansion in SELECT statements."""
    
    def __init__(self, schema: Dict[str, List[str]], logger: Optional[logging.Logger] = None):
        self.schema = schema
        self.logger = logger or logging.getLogger(__name__)
    
    def expand_star(self, scope: exp.Select) -> List[Tuple[str, exp.Expression]]:
        """Expand * into columns when selecting from subqueries/CTEs with known outputs."""
        out: List[Tuple[str, exp.Expression]] = []
        from_ = scope.args.get("from")
        if not from_:
            return out
        
        # For each subquery source, flatten its outputs
        for sub in from_.find_all(exp.Subquery):
            a = normalize_identifier(alias_to_str(getattr(sub, "alias", None))) or ""
            inner = sub.this if isinstance(sub.this, exp.Select) else None
            if not inner:
                continue
            
            # Get projected columns from subquery
            inner_selects = self._get_select_parts(inner)
            for inner_select in inner_selects:
                for proj in inner_select.expressions:
                    if isinstance(proj, exp.Alias):
                        alias_name = normalize_identifier(proj.alias)
                        col_expr = exp.Column(this=exp.Identifier(this=alias_name))
                        if a:
                            col_expr = col_expr.replace(exp.Column(table=exp.Identifier(this=a)))
                        out.append((alias_name, col_expr))
                    elif isinstance(proj, exp.Column) and not isinstance(proj, exp.Star):
                        col_name = normalize_identifier(proj.name)
                        col_expr = exp.Column(this=exp.Identifier(this=col_name))
                        if a:
                            col_expr = col_expr.replace(exp.Column(table=exp.Identifier(this=a)))
                        out.append((col_name, col_expr))
        
        return out

    def flatten_projection(
        self, 
        select_expr: exp.Select, 
        proj_map: Dict[str, exp.Expression]
    ) -> List[Tuple[Optional[str], exp.Expression]]:
        """Flatten projections, handling star expansion and alias resolution."""
        result: List[Tuple[Optional[str], exp.Expression]] = []
        
        for proj in select_expr.expressions:
            alias = None
            expr = proj
            
            if isinstance(proj, exp.Alias):
                alias = normalize_identifier(proj.alias)
                expr = proj.this
            elif isinstance(proj, exp.Star):
                # Expand * if possible; if not, keep a synthetic '*'
                expanded = self.expand_star(select_expr)
                if expanded:
                    result.extend(expanded)
                else:
                    # Keep a placeholder to be resolved later
                    star_col = exp.Column(this=exp.Identifier(this='*'))
                    result.append((None, star_col))
                continue
            else:
                # derive alias from column name if simple column
                if isinstance(proj, exp.Column):
                    alias = normalize_identifier(proj.name)

            # Resolve alias if expr is a column reference to an alias
            if isinstance(expr, exp.Column) and not expr.table:
                name = normalize_identifier(expr.name)
                if name in proj_map:
                    resolved_expr = proj_map.get(name)
                    if resolved_expr:
                        expr = resolved_expr

            result.append((alias or None, expr))
        
        return result

    def _get_select_parts(self, query: exp.Expression) -> List[exp.Select]:
        """Recursively get all SELECT parts from a query, handling UNIONs."""
        parts = []
        if isinstance(query, exp.Union):
            parts.extend(self._get_select_parts(query.left))
            parts.extend(self._get_select_parts(query.right))
        elif isinstance(query, exp.Select):
            parts.append(query)
        return parts