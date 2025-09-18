"""
Main lineage extractor using modular components.

This is a refactored version of the original extractor.py that uses
separate modules for different functionality areas.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, Iterable, List, Optional, Tuple

import sqlglot
from sqlglot import expressions as exp

from .logger import get_logger
from .models import LineageRecord
from .utils import (
    collect_cte_aliases,
    infer_target_table,
    is_insert,
    is_select,
    is_with,
    normalize_identifier,
)
from .core.cte_handler import CTEHandler
from .core.column_resolver import ColumnResolver
from .core.star_expander import StarExpander


class LineageExtractor:
    """
    Extract lineage records from SQL files using sqlglot with modular architecture.

    Output fields per record:
    - source_table
    - source_column
    - expression
    - target_column
    - target_table
    """

    def __init__(
        self,
        engine: str = "spark",
        schema: Optional[Dict[str, List[str]]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.engine = engine.lower()
        # Normalize schema: flatten nested dicts like {a:{b:{t2:{c2:'int'}}}} -> {'a.b.t2': ['c2']}
        self.schema = self._normalize_schema(schema or {})
        self.logger = logger or get_logger(level=os.getenv("LOG_LEVEL", "INFO"))
        
        # Initialize modular components
        self.column_resolver = ColumnResolver(self.schema, self.logger)
        self.cte_handler = CTEHandler(self.logger)
        self.star_expander = StarExpander(self.schema, self.logger)

    def _normalize_schema(self, schema: Dict) -> Dict[str, List[str]]:
        """Normalize nested schema structure to flat dictionary."""
        flat: Dict[str, List[str]] = {}
        def walk(prefix: str, node):
            if isinstance(node, dict):
                # If values are primitive types or all strings (columns), collect as columns
                if node and all(isinstance(v, (str, int, float, bool)) for v in node.values()):
                    cols = [normalize_identifier(k) for k in node.keys()]
                    flat[prefix] = cols
                else:
                    for k, v in node.items():
                        key = k.lower()
                        new_prefix = f"{prefix}.{key}" if prefix else key
                        walk(new_prefix, v)
            elif isinstance(node, list):
                flat[prefix] = [normalize_identifier(x) for x in node]
        for k, v in schema.items():
            walk(k.lower(), v)
        return flat

    def parse_sql(self, sql_text: str) -> List[exp.Expression]:
        """Parse SQL text into AST expressions."""
        try:
            return sqlglot.parse(sql_text, read=self.engine)
        except Exception as e:
            self.logger.error(f"Failed to parse SQL for engine={self.engine}: {e}")
            raise

    def extract_from_file(self, path: str) -> List[LineageRecord]:
        """Extract lineage records from a SQL file."""
        with open(path, "r", encoding="utf-8") as f:
            sql_text = f.read()
        stmts = self.parse_sql(sql_text)
        all_records: List[LineageRecord] = []
        for stmt in stmts:
            all_records.extend(self._extract_from_statement(stmt, file=path))
        return all_records

    def _extract_from_statement(
        self, stmt: exp.Expression, file: Optional[str] = None
    ) -> List[LineageRecord]:
        """Extract lineage from a single SQL statement."""
        self.logger.debug(f"stmt type: {type(stmt)}, is_with: {is_with(stmt)}")
        # Debug: log raw SQL for statement heads
        try:
            self.logger.debug(f"Statement SQL head: {stmt.sql(dialect=self.engine)[:200]}")
        except Exception:
            pass
        
        # Normalize WITH wrapper
        if is_with(stmt):
            inner = stmt.this
        else:
            inner = stmt

        target_table = infer_target_table(inner)
        target_cols: List[str] = []

        # Determine select part
        select_expr: Optional[exp.Select] = None
        if isinstance(inner, exp.Insert):
            # INSERT ... SELECT ...
            select_expr = inner.args.get("expression")
            if isinstance(select_expr, exp.Select):
                # capture target columns from insert if specified
                cols = inner.args.get("columns") or []
                if cols:
                    target_cols = [normalize_identifier(c.name) for c in cols if c]
                else:
                    target_cols = []
            elif isinstance(select_expr, exp.Union):
                # handle union: no explicit target columns available
                target_cols = []
        elif isinstance(inner, exp.Select):
            select_expr = inner
        elif isinstance(inner, exp.Union):
            # For UNION, process each side and merge
            left = inner.left
            right = inner.right
            recs = []
            recs.extend(self._extract_from_statement(left, file))
            recs.extend(self._extract_from_statement(right, file))
            return recs
        else:
            # Not a SELECT/INSERT we handle explicitly
            return []

        if not select_expr:
            return []

        # Collect CTE context
        with_expr = stmt if is_with(stmt) else None
        cte_map: Dict[str, exp.Expression] = {}
        if with_expr:
            cte_map = collect_cte_aliases(with_expr)
        elif isinstance(stmt, exp.Select) and getattr(stmt, 'ctes', None):
            for cte in getattr(stmt, 'ctes'):
                name = normalize_identifier(cte.alias)
                cte_map[name] = cte.this
        self.logger.debug(f"with_expr: {with_expr}, cte_map: {cte_map}")

        # Build source mapping
        return self._extract_from_select(
            select_expr,
            target_table=target_table,
            target_cols=target_cols,
            cte_map=cte_map,
            file=file,
        )

    def _extract_from_select(
        self,
        select_expr: exp.Select,
        target_table: Optional[str],
        target_cols: List[str],
        cte_map: Dict[str, exp.Expression],
        file: Optional[str],
    ) -> List[LineageRecord]:
        """Extract lineage from a SELECT expression."""
        self.logger.info(f"cte_map in _extract_from_select: {list(cte_map.keys())}")
        recs: List[LineageRecord] = []

        # Build projection map for alias resolution
        proj_map = self._projection_name_to_expr(select_expr)
        
        # Process each projection using star expander
        flattened = self.star_expander.flatten_projection(select_expr, proj_map)
        
        for i, (alias, expr) in enumerate(flattened):
            target_col = target_cols[i] if i < len(target_cols) else alias
            expr_sql = self._expr_display(expr)
            
            # Handle star expansion using CTE handler
            if isinstance(expr, exp.Column) and expr.name == '*':
                # Star expansion
                table_name = normalize_identifier(expr.table) if expr.table else None
                if table_name:
                    # Qualified star (table.*)
                    if table_name in cte_map:
                        for st, sc in self.cte_handler.enumerate_cte_output_sources(table_name, cte_map, set(), self.column_resolver):
                            recs.append(LineageRecord(
                                source_table=st,
                                source_column=sc,
                                expression=expr_sql,
                                target_column=target_col,
                                target_table=target_table,
                                file=file,
                                engine=self.engine,
                            ))
                    else:
                        # Physical table star
                        cols = self.schema.get(table_name, [])
                        if cols:
                            for col in cols:
                                recs.append(LineageRecord(
                                    source_table=table_name,
                                    source_column=normalize_identifier(col),
                                    expression=expr_sql,
                                    target_column=target_col,
                                    target_table=target_table,
                                    file=file,
                                    engine=self.engine,
                                ))
                        else:
                            recs.append(LineageRecord(
                                source_table=table_name,
                                source_column='*',
                                expression=expr_sql,
                                target_column=target_col,
                                target_table=target_table,
                                file=file,
                                engine=self.engine,
                            ))
                else:
                    # Unqualified star - determine from scope
                    from_ = select_expr.args.get("from")
                    joins = select_expr.args.get("joins") or []
                    
                    # Single table case
                    if from_ and not joins:
                        tables = [t for t in from_.find_all(exp.Table)]
                        if len(tables) == 1:
                            single_name = normalize_identifier(tables[0].name)
                            if single_name in cte_map:
                                for st, sc in self.cte_handler.enumerate_cte_output_sources(single_name, cte_map, set(), self.column_resolver):
                                    recs.append(LineageRecord(
                                        source_table=st,
                                        source_column=sc,
                                        expression=expr_sql,
                                        target_column=target_col,
                                        target_table=target_table,
                                        file=file,
                                        engine=self.engine,
                                    ))
                            else:
                                cols = self.schema.get(single_name, [])
                                if cols:
                                    for col in cols:
                                        recs.append(LineageRecord(
                                            source_table=single_name,
                                            source_column=normalize_identifier(col),
                                            expression=expr_sql,
                                            target_column=target_col,
                                            target_table=target_table,
                                            file=file,
                                            engine=self.engine,
                                        ))
                                else:
                                    recs.append(LineageRecord(
                                        source_table=single_name,
                                        source_column='*',
                                        expression=expr_sql,
                                        target_column=target_col,
                                        target_table=target_table,
                                        file=file,
                                        engine=self.engine,
                                    ))
                    # Multi-table case (joins)
                    else:
                        all_tables = []
                        if from_:
                            for t in from_.find_all(exp.Table):
                                tname = normalize_identifier(t.name)
                                if tname:
                                    all_tables.append(tname)
                        
                        for join in joins:
                            if isinstance(join.this, exp.Table):
                                tname = normalize_identifier(join.this.name)
                                if tname and tname not in all_tables:
                                    all_tables.append(tname)
                        
                        for table_name in all_tables:
                            if table_name in cte_map:
                                for st, sc in self.cte_handler.enumerate_cte_output_sources(table_name, cte_map, set(), self.column_resolver):
                                    recs.append(LineageRecord(
                                        source_table=st,
                                        source_column=sc,
                                        expression=expr_sql,
                                        target_column=target_col,
                                        target_table=target_table,
                                        file=file,
                                        engine=self.engine,
                                    ))
                            else:
                                cols = self.schema.get(table_name, [])
                                if cols:
                                    for col in cols:
                                        recs.append(LineageRecord(
                                            source_table=table_name,
                                            source_column=normalize_identifier(col),
                                            expression=expr_sql,
                                            target_column=target_col,
                                            target_table=target_table,
                                            file=file,
                                            engine=self.engine,
                                        ))
                                else:
                                    recs.append(LineageRecord(
                                        source_table=table_name,
                                        source_column='*',
                                        expression=expr_sql,
                                        target_column=target_col,
                                        target_table=target_table,
                                        file=file,
                                        engine=self.engine,
                                    ))
            else:
                # Regular column expression
                columns = list(expr.find_all(exp.Column))
                if not columns:
                    # Literal expression
                    recs.append(LineageRecord(
                        source_table=None,
                        source_column=None,
                        expression=expr_sql,
                        target_column=target_col,
                        target_table=target_table,
                        file=file,
                        engine=self.engine,
                    ))
                else:
                    # Resolve each column
                    for col in columns:
                        resolved = self.column_resolver.resolve_column_sources(
                            select_expr, col, cte_map, proj_map
                        )
                        for source_table, source_column in resolved:
                            recs.append(LineageRecord(
                                source_table=source_table,
                                source_column=source_column,
                                expression=expr_sql,
                                target_column=target_col,
                                target_table=target_table,
                                file=file,
                                engine=self.engine,
                            ))

        return recs

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

    def _expr_display(self, expr: exp.Expression) -> str:
        """Get display string for an expression."""
        try:
            return expr.sql(dialect=self.engine)
        except Exception:
            return str(expr)