"""
Column resolution functionality.

This module handles resolving column references to their ultimate source tables and columns,
including traversing through CTEs, subqueries, and aliases.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple, Set

import sqlglot
from sqlglot import expressions as exp

from ..utils import normalize_identifier, table_name_of, collect_cte_aliases


class ColumnResolver:
    """Handles column resolution through CTEs, subqueries, and aliases."""
    
    def __init__(self, schema: Dict[str, List[str]], logger: Optional[logging.Logger] = None):
        self.schema = schema
        self.logger = logger or logging.getLogger(__name__)
    
    def resolve_column_sources(
        self,
        scope: exp.Select,
        column: exp.Column,
        cte_map: Dict[str, exp.Expression],
        proj_map: Dict[str, exp.Expression],
        _visited: Optional[set] = None,
    ) -> List[Tuple[Optional[str], Optional[str]]]:
        """
        Resolve a column to ultimate source tables/columns, traversing CTEs and subqueries.
        Returns list of (source_table, source_column) pairs.
        """
        results: List[Tuple[Optional[str], Optional[str]]] = []
        _visited = _visited or set()

        tbl = normalize_identifier(column.table) if column.table else None
        name = normalize_identifier(column.name)

        key = (id(scope), name, tbl)  # Include table in the key to avoid conflicts
        if key in _visited:
            return []  # Avoid infinite recursion
        _visited.add(key)

        # Resolve alias if column is an alias in current scope
        if not tbl and name in proj_map:
            resolved_expr = proj_map.get(name)
            if resolved_expr:
                inner_cols = list(resolved_expr.find_all(exp.Column))
                if inner_cols:
                    results = []
                    for c in inner_cols:
                        sub_key = (id(scope), normalize_identifier(c.name), normalize_identifier(c.table) if c.table else None)
                        if sub_key not in _visited:  # Prevent recursive loops
                            results.extend(self.resolve_column_sources(scope, c, cte_map, proj_map, _visited))
                    return results

        # Determine alias via inference if missing
        alias = tbl or self.alias_or_table_for_column(scope, column)
        if not alias:
            # If there's exactly one source table in this scope, and it corresponds to a CTE or a subquery,
            # resolve through it instead of treating it as a base table name.
            one_alias = self.single_source_alias(scope)
            if one_alias:
                alias = one_alias
            else:
                # Check if single table is a CTE
                from_ = scope.args.get("from")
                if from_:
                    tables = [t for t in from_.find_all(exp.Table)]
                    if len(tables) == 1 and not (scope.args.get("joins") or []):
                        tname = table_name_of(tables[0])
                        if tname and tname in cte_map:
                            alias = tname

        # CTE resolution
        self.logger.debug(f"Checking CTE for {name}, alias {alias}, in cte_map {alias in cte_map if alias else False}")
        if alias and alias in cte_map:
            self.logger.debug(f"CTE branch for {name}, alias {alias}")
            inner = cte_map[alias]
            inner_ctes = collect_cte_aliases(inner) if isinstance(inner, exp.With) else {}
            inner_select = inner.this if isinstance(inner, exp.With) else inner
            # Support UNION by exploring both sides
            from ..core.cte_handler import CTEHandler
            parts = CTEHandler()._get_select_parts(inner_select)
            for part in parts:
                if not isinstance(part, exp.Select):
                    continue
                proj_map = self._projection_name_to_expr(part)
                expr = proj_map.get(name)
                if expr is None:
                    # Check if name is a direct column in the projection
                    for proj in part.expressions:
                        if isinstance(proj, exp.Column) and normalize_identifier(proj.name) == name:
                            expr = proj
                            break
                if expr is None:
                    # If projection includes STAR, attribute to single inner base table when unambiguous
                    has_star = any(isinstance(p, exp.Star) or (isinstance(p, exp.Column) and p.name == '*') for p in part.expressions)
                    if has_star:
                        from_ = part.args.get("from")
                        joins = part.args.get("joins") or []
                        if from_ and not joins:
                            base_tables = [table_name_of(t) for t in from_.find_all(exp.Table)]
                            base_tables = [t for t in base_tables if t]
                            if len(base_tables) == 1:
                                return [(base_tables[0], name)]
                    continue
                inner_cols = list(expr.find_all(exp.Column))
                self.logger.debug(f"CTE expr for {name}: {expr}, inner_cols: {inner_cols}")
                if not inner_cols:
                    # Try to guess from inner base tables when expression is literal
                    candidates = []
                    from_ = part.args.get("from")
                    if from_:
                        for t in from_.find_all(exp.Table):
                            tname = table_name_of(t)
                            if tname:
                                candidates.append(tname)
                    if len(candidates) == 1:
                        results.append((candidates[0], name))
                    else:
                        results.append((None, name))
                    continue
                
                for c in inner_cols:
                    deeper_key = (id(part), normalize_identifier(c.name), normalize_identifier(c.table) if c.table else None)
                    if deeper_key not in _visited:  # Prevent recursive loops
                        deeper_results = self.resolve_column_sources(part, c, {**cte_map, **inner_ctes}, proj_map, _visited)
                        results.extend(deeper_results)
                break  # Found in this part, don't check others
            return results

        # Subquery resolution
        if alias:
            subquery = self.get_subquery_by_alias(scope, alias)
            if subquery:
                inner_select = subquery.this if isinstance(subquery, (exp.Subquery, exp.CTE)) else subquery
                if inner_select and isinstance(inner_select, exp.Select):
                    # Look for the column in subquery's projections
                    inner_proj_map = self._projection_name_to_expr(inner_select)
                    expr = inner_proj_map.get(name)
                    if expr:
                        inner_cols = list(expr.find_all(exp.Column))
                        if inner_cols:
                            for c in inner_cols:
                                sub_key = (id(inner_select), normalize_identifier(c.name), normalize_identifier(c.table) if c.table else None)
                                if sub_key not in _visited:  # Prevent recursive loops
                                    results.extend(self.resolve_column_sources(inner_select, c, cte_map, inner_proj_map, _visited))
                        else:
                            # Expression without columns - might be literal
                            from_ = inner_select.args.get("from")
                            if from_:
                                base_tables = [table_name_of(t) for t in from_.find_all(exp.Table)]
                                if len(base_tables) == 1:
                                    results.append((base_tables[0], name))
                                else:
                                    results.append((None, name))
                        return results
                    
                    # Check if subquery has star projections
                    has_star = any(isinstance(p, exp.Star) for p in inner_select.expressions)
                    if has_star:
                        from_ = inner_select.args.get("from")
                        if from_:
                            base_tables = [table_name_of(t) for t in from_.find_all(exp.Table)]
                            if len(base_tables) == 1:
                                return [(base_tables[0], name)]

        # Regular table resolution
        if alias:
            # Check if it's a known table name or alias
            resolved_table = self.resolve_table_name(scope, alias)
            return [(resolved_table, name)]

        return results

    def alias_or_table_for_column(self, scope: exp.Select, column: exp.Column) -> Optional[str]:
        """Determine the table alias or name for a column reference."""
        from ..utils import alias_or_table_for_column
        return alias_or_table_for_column(scope, column)

    def single_source_alias(self, scope: exp.Select) -> Optional[str]:
        """Get the single source alias if there's exactly one table/subquery in the scope."""
        from_ = scope.args.get("from")
        joins = scope.args.get("joins") or []
        
        if from_ and not joins:
            # Single table or subquery
            if hasattr(from_, 'this'):
                source = from_.this
                if isinstance(source, exp.Table):
                    alias = getattr(source, 'alias', None)
                    if alias:
                        return normalize_identifier(alias)
                    return table_name_of(source)
                elif isinstance(source, exp.Subquery):
                    alias = getattr(source, 'alias', None)
                    if alias:
                        return normalize_identifier(alias)
        return None

    def get_subquery_by_alias(self, scope: exp.Select, alias: str) -> Optional[exp.Expression]:
        """Find a subquery in the scope by its alias."""
        from_ = scope.args.get("from")
        if from_ and hasattr(from_, 'this'):
            source = from_.this
            if isinstance(source, exp.Subquery):
                source_alias = getattr(source, 'alias', None)
                if source_alias and normalize_identifier(source_alias) == alias:
                    return source
        
        # Check joins
        joins = scope.args.get("joins") or []
        for join in joins:
            join_source = join.this
            if isinstance(join_source, exp.Subquery):
                join_alias = getattr(join_source, 'alias', None)
                if join_alias and normalize_identifier(join_alias) == alias:
                    return join_source
        
        return None

    def resolve_table_name(self, scope: exp.Select, alias_or_table: str) -> str:
        """Resolve a table name or alias to the actual table name."""
        # Build alias map
        alias_map = self.build_alias_map(scope)
        
        # Check if it's an alias first
        if alias_or_table in alias_map:
            return alias_map[alias_or_table]
        
        # Otherwise assume it's a table name
        return alias_or_table

    def build_alias_map(self, scope: exp.Select) -> Dict[str, str]:
        """Build mapping from table aliases to table names."""
        alias_map: Dict[str, str] = {}
        
        # Process FROM clause
        from_ = scope.args.get("from")
        if from_ and hasattr(from_, 'this'):
            source = from_.this
            if isinstance(source, exp.Table):
                table_name = table_name_of(source)
                alias = getattr(source, 'alias', None)
                if alias and table_name:
                    alias_map[normalize_identifier(alias)] = table_name
        
        # Process JOINs
        joins = scope.args.get("joins") or []
        for join in joins:
            join_source = join.this
            if isinstance(join_source, exp.Table):
                table_name = table_name_of(join_source)
                alias = getattr(join_source, 'alias', None)
                if alias and table_name:
                    alias_map[normalize_identifier(alias)] = table_name
        
        return alias_map

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