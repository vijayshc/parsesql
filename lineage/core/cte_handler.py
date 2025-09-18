"""
CTE (Common Table Expression) handling functionality.

This module contains all CTE-related logic including:
- CTE output enumeration  
- CTE resolution and unwrapping
- Union handling for CTEs
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple, Set

import sqlglot
from sqlglot import expressions as exp

from ..utils import normalize_identifier, table_name_of


class CTEHandler:
    """Handles CTE resolution, enumeration, and unwrapping."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
    
    def enumerate_cte_output_sources(
        self, 
        cte_name: str, 
        cte_map: Dict[str, exp.Expression], 
        visited: Set[str],
        column_resolver: 'ColumnResolver'  # Forward reference
    ) -> List[Tuple[str, str]]:
        """Return list of (source_table, source_column) for the visible output columns of a CTE.

        This walks through chains of CTEs when the CTE body is a simple SELECT * from another CTE,
        but STOPS when:
          * Projection lists explicit columns (with or without aliases)
          * Multiple base tables (joins) appear
          * UNION appears

        For explicit projections, each projected column's lineage is resolved via existing column resolution logic.
        """
        if cte_name in visited:
            return []
        visited.add(cte_name)
        node = cte_map.get(cte_name)
        if not node:
            return []
        inner = node.this if isinstance(node, exp.With) else node
        if not isinstance(inner, (exp.Select, exp.Union)):
            return []
        
        # UNION root: merge branch outputs and dedupe
        if isinstance(inner, exp.Union):
            merged: List[Tuple[str, str]] = []
            for part in self._get_select_parts(inner):
                if isinstance(part, exp.Select):
                    merged.extend(self.enumerate_select_output_sources(part, cte_map, visited, column_resolver))
            uniq: List[Tuple[str, str]] = []
            seen = set()
            for p in merged:
                if p not in seen:
                    seen.add(p)
                    uniq.append(p)
            return uniq
        
        pairs = self.enumerate_select_output_sources(inner, cte_map, visited, column_resolver)
        if not pairs:
            # Fallback: attempt to resolve projection aliases explicitly
            proj_map_inner = self._projection_name_to_expr(inner)
            fallback: List[Tuple[str, str]] = []
            for alias_name, ex in proj_map_inner.items():
                for c in ex.find_all(exp.Column):
                    for st, sc in column_resolver.resolve_column_sources(inner, c, cte_map, proj_map_inner):
                        if st and sc:
                            fallback.append((st, sc))
            if fallback:
                pairs = fallback
        
        # If all pairs reference the CTE itself (or another intermediate CTE), attempt to unwrap to base physical tables
        unwrapped: List[Tuple[str, str]] = []
        for st, sc in pairs:
            if st in cte_map:
                # For explicit column references, resolve the specific column through the CTE chain
                # instead of enumerating all columns from the CTE
                if sc != '*':  # Not a star placeholder
                    # Create a column reference and resolve it specifically
                    col_ref = exp.Column(this=exp.Identifier(this=sc), table=exp.Identifier(this=st))
                    # Use a simple scope for resolution - we just need to trace through the CTE
                    temp_scope = exp.Select(expressions=[col_ref], from_=exp.From(this=exp.Table(this=st)))
                    resolved = column_resolver.resolve_column_sources(temp_scope, col_ref, cte_map, {})
                    unwrapped.extend(resolved or [(st, sc)])
                else:
                    # For star placeholders, enumerate all columns
                    deeper = self.enumerate_cte_output_sources(st, cte_map, visited, column_resolver)
                    unwrapped.extend(deeper or [(st, sc)])
            else:
                unwrapped.append((st, sc))
        
        # Normalize: if any physical table names present, drop intermediate CTE table-only star placeholders
        phys = [p for p in unwrapped if p[0] not in cte_map]
        return phys or unwrapped

    def enumerate_select_output_sources(
        self, 
        select_expr: exp.Select, 
        cte_map: Dict[str, exp.Expression], 
        visited: Set[str],
        column_resolver: 'ColumnResolver'
    ) -> List[Tuple[str, str]]:
        """Enumerate output sources for a SELECT expression."""
        out: List[Tuple[str, str]] = []
        
        # If projection is STAR only and single table source that is a CTE -> recurse
        star_only = all(isinstance(p, (exp.Star,)) for p in select_expr.expressions)
        from_ = select_expr.args.get("from")
        joins = select_expr.args.get("joins") or []
        base_tables = []
        if from_:
            for t in from_.find_all(exp.Table):
                tn = table_name_of(t)
                if tn:
                    base_tables.append(tn)
        
        # Include explicit join tables
        for j in joins:
            jt = j.this
            if isinstance(jt, exp.Table):
                tn = table_name_of(jt)
                if tn and tn not in base_tables:
                    base_tables.append(tn)
        
        # Simple chain case
        if star_only and len(base_tables) == 1 and not joins:
            base = base_tables[0]
            if base in cte_map:
                return self.enumerate_cte_output_sources(base, cte_map, visited, column_resolver)
            # Base physical table
            cols = column_resolver.schema.get(base, [])
            if cols:
                return [(base, normalize_identifier(c)) for c in cols]
            return [(base, '*')]
        
        # STAR over multi-table join: enumerate each side's visible columns
        if star_only and len(base_tables) > 1:
            enumerated: List[Tuple[str, str]] = []
            for bt in base_tables:
                if bt in cte_map:
                    enumerated.extend(self.enumerate_cte_output_sources(bt, cte_map, visited, column_resolver))
                else:
                    cols = column_resolver.schema.get(bt, [])
                    if cols:
                        enumerated.extend([(bt, normalize_identifier(c)) for c in cols])
                    else:
                        enumerated.append((bt, '*'))
            return enumerated
        
        # Explicit projections: resolve each column expression lineage
        proj_map = self._projection_name_to_expr(select_expr)
        for proj in select_expr.expressions:
            if isinstance(proj, exp.Star):
                # STAR among explicit columns: expand relative to single table if possible else skip
                if len(base_tables) == 1 and not joins:
                    base = base_tables[0]
                    if base in cte_map:
                        out.extend(self.enumerate_cte_output_sources(base, cte_map, visited, column_resolver))
                    else:
                        cols = column_resolver.schema.get(base, [])
                        if cols:
                            out.extend([(base, normalize_identifier(c)) for c in cols])
                        else:
                            out.append((base, '*'))
                continue
            
            expr = proj.this if isinstance(proj, exp.Alias) else proj
            alias = normalize_identifier(proj.alias) if isinstance(proj, exp.Alias) else None
            
            # Handle qualified stars (e.g., c3.*)
            if isinstance(expr, exp.Column) and expr.name == '*' and expr.table:
                table_name = normalize_identifier(expr.table)
                if table_name in cte_map:
                    out.extend(self.enumerate_cte_output_sources(table_name, cte_map, visited, column_resolver))
                else:
                    cols = column_resolver.schema.get(table_name, [])
                    if cols:
                        out.extend([(table_name, normalize_identifier(c)) for c in cols])
                    else:
                        out.append((table_name, '*'))
                continue
            
            # Collect columns inside expr
            cols = list(expr.find_all(exp.Column))
            if not cols:
                # If expression is an alias of another alias (e.g., wrapping) try to find inner projection
                if isinstance(expr, exp.Identifier) and alias and expr.this in proj_map:
                    inner_expr = proj_map[expr.this]
                    inner_cols = list(inner_expr.find_all(exp.Column))
                    for c in inner_cols:
                        for st, sc in column_resolver.resolve_column_sources(select_expr, c, cte_map, proj_map):
                            if st and sc:
                                out.append((st, sc))
                continue
            
            temp_select = select_expr  # reuse scope for resolution
            for c in cols:
                for st, sc in column_resolver.resolve_column_sources(temp_select, c, cte_map, proj_map):
                    if st and sc:
                        out.append((st, sc))
        
        # Deduplicate preserving order
        seen_pairs = set()
        dedup: List[Tuple[str, str]] = []
        for p in out:
            if p not in seen_pairs:
                seen_pairs.add(p)
                dedup.append(p)
        return dedup

    def resolve_cte_base(self, cte_name: str, cte_map: Dict[str, exp.Expression]) -> str:
        """Resolve a CTE to its ultimate base table name."""
        visited = set()
        current = cte_name
        while current in cte_map and current not in visited:
            visited.add(current)
            node = cte_map[current]
            inner = node.this if isinstance(node, exp.With) else node
            if not isinstance(inner, exp.Select):
                break
            from_ = inner.args.get("from")
            if not from_:
                break
            tables = [table_name_of(t) for t in from_.find_all(exp.Table)]
            if len(tables) == 1:
                current = tables[0]
            else:
                break
        return current

    def _get_select_parts(self, query: exp.Expression) -> List[exp.Select]:
        """Recursively get all SELECT parts from a query, handling UNIONs."""
        parts = []
        if isinstance(query, exp.Union):
            parts.extend(self._get_select_parts(query.left))
            parts.extend(self._get_select_parts(query.right))
        elif isinstance(query, exp.Select):
            parts.append(query)
        return parts

    def _projection_name_to_expr(self, select_expr: exp.Select) -> Dict[str, exp.Expression]:
        """Build mapping from projection alias names to their expressions."""
        proj_map: Dict[str, exp.Expression] = {}
        for projection in select_expr.expressions:
            if isinstance(projection, exp.Alias):
                name = normalize_identifier(projection.alias)
                proj_map[name] = projection.this
            elif isinstance(projection, exp.Column):
                name = normalize_identifier(projection.name)
                proj_map[name] = projection
        return proj_map