from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

import sqlglot
from sqlglot import expressions as exp

from .logger import get_logger
from .models import LineageRecord
from .core.schema import Schema, _norm
from .core.sources import AnalysisEnvironment, SelectSource, TableSource
from .core.analyzer import SelectAnalyzer, expr_sql


def _flatten_schema(raw: Dict) -> Dict[str, List[str]]:
    """Flatten nested schema dicts into a table -> [columns] mapping.

    Input may look like { db: { table: { col: type }}} or { table: [col1, col2] }.
    We keep dotted naming for multi-level keys (db.table).
    """
    flat: Dict[str, List[str]] = {}

    def walk(prefix: str, node):
        if isinstance(node, list):
            flat[_norm(prefix)] = [_norm(c) for c in node]
        elif isinstance(node, dict):
            # If dict values are primitives treat keys as column names
            if node and all(not isinstance(v, (dict, list)) for v in node.values()):
                flat[_norm(prefix)] = [_norm(k) for k in node.keys()]
            else:
                for k, v in node.items():
                    key = f"{prefix}.{k}" if prefix else k
                    walk(key, v)

    for k, v in raw.items():
        walk(k, v)
    # remove empty keys
    return {k: v for k, v in flat.items() if k}


class LineageExtractor:
    """New modular lineage extractor built from first principles.

    Responsibilities:
    1. Parse SQL statements using sqlglot
    2. Build CTE environment (AnalysisEnvironment)
    3. For each top-level statement, gather lineage rows via SelectAnalyzer
       (INSERT targets supported by attaching target table/columns)
    4. Emit LineageRecord rows
    """

    def __init__(self, engine: str = "spark", schema: Optional[Dict[str, List[str]]] = None, logger: Optional[logging.Logger] = None):
        self.engine = engine.lower()
        self.schema = Schema(_flatten_schema(schema or {}))
        self.logger = logger or get_logger(level=os.getenv("LOG_LEVEL", "INFO"))

    # --------------- public API ---------------
    def extract_from_file(self, path: str) -> List[LineageRecord]:
        with open(path, 'r', encoding='utf-8') as f:
            sql_text = f.read()
        statements = self._parse(sql_text)
        all_rows: List[LineageRecord] = []
        for stmt in statements:
            all_rows.extend(self._extract_statement(stmt, file=path))
        return all_rows

    # --------------- internal ---------------
    def _parse(self, sql_text: str) -> List[exp.Expression]:
        try:
            return sqlglot.parse(sql_text, read=self.engine)
        except Exception as e:  # pragma: no cover
            self.logger.error(f"Parse error: {e}")
            return []

    def _extract_statement(self, stmt: exp.Expression, file: Optional[str]) -> List[LineageRecord]:
        # WITH wrapper (support WITH on any statement type including INSERT/UPDATE/MERGE)
        env = AnalysisEnvironment(ctes={})
        core_stmt = stmt
        with_obj = None
        if isinstance(stmt, exp.With):
            with_obj = stmt
        else:
            w = stmt.args.get('with') if hasattr(stmt, 'args') else None
            if w:
                with_obj = w
        if with_obj is not None:
            for cte in with_obj.find_all(exp.CTE):
                name = _norm(str(cte.alias))
                if not name:
                    continue
                inner = cte.this
                if isinstance(inner, (exp.Select, exp.Union)):
                    env.register(name, SelectSource(inner, env, self.schema))
            if isinstance(stmt, exp.With):
                core_stmt = stmt.this
            else:
                core_stmt = stmt

        # INSERT target handling
        target_table = None
        target_cols: List[str] = []
        select_part: Optional[exp.Expression] = None
        if isinstance(core_stmt, exp.Insert):
            # Target
            table = core_stmt.this
            if isinstance(table, exp.Schema):
                t = table.this
                if isinstance(t, exp.Table):
                    target_table = self._table_name(t)
                # capture target column identifiers
                cols = table.expressions or []
                target_cols = [_norm(getattr(c, 'name', getattr(c, 'this', None))) for c in cols if c]
            elif isinstance(table, exp.Table):
                target_table = self._table_name(table)
            # Optional target columns
            cols = core_stmt.args.get('columns') or []
            for c in cols:
                if hasattr(c, 'name'):
                    target_cols.append(_norm(c.name))
            select_part = core_stmt.args.get('expression')
        else:
            select_part = core_stmt

        # UPDATE handling (no select_part path)
        if isinstance(core_stmt, exp.Update):
            return self._extract_update(core_stmt, env, file)
        # MERGE handling
        if isinstance(core_stmt, exp.Merge):
            return self._extract_merge(core_stmt, env, file)

        if not isinstance(select_part, (exp.Select, exp.Union)):
            return []

        analyzer = SelectAnalyzer(select_part, env, self.schema, self.engine)
        expr_lineages = analyzer.analyze()
        # If INSERT without explicit target columns: apply positional or name-based mapping using target schema
        if target_table and not target_cols:
            target_schema_cols = list(self.schema.columns(target_table))
            # Build output column list from analyzed expressions
            produced = [el.output_column for el in expr_lineages]
            # If star expansion produced None output_column entries (handled separately), infer from origins
            inferred = []
            for el in expr_lineages:
                if el.output_column:
                    inferred.append(el.output_column)
                else:
                    # Use first origin column when available
                    if el.origins and el.origins[0].column:
                        inferred.append(el.origins[0].column)
            produced = [c for c in inferred if c]
            # Retain only produced columns that exactly match target schema names and preserve produced order (positional safety)
            ordered_schema_set = {c for c in target_schema_cols}
            clean_produced = [c for c in produced if c in ordered_schema_set]
            if clean_produced:
                # Truncate to min of produced & schema intersection
                target_cols = clean_produced[:len(clean_produced)]
        rows: List[LineageRecord] = []
        for idx, el in enumerate(expr_lineages):
            # Determine target column if INSERT and user-specified target column list present
            target_col = None
            if target_cols and idx < len(target_cols):
                # Only map if expression's primary origin column name matches the target column OR names identical
                candidate = target_cols[idx]
                origin_first = el.origins[0].column if el.origins else None
                if origin_first == candidate or el.output_column == candidate:
                    target_col = candidate
            elif el.output_column:
                target_col = el.output_column
            for origin in el.origins:
                # Prefer real physical column name if alias-only (e.g., CTE projected 'a' from 'customer_id')
                source_column = origin.column
                # Fallback: if this is an INSERT context and target_col not set but el.output_column matches one of target_cols
                if target_table and not target_col and el.output_column in target_cols:
                    target_col = el.output_column
                rows.append(
                    LineageRecord(
                        source_table=origin.table,
                        source_column=source_column,
                        expression=el.expression_sql,
                        target_column=target_col,
                        target_table=target_table,
                        file=file,
                        engine=self.engine,
                    )
                )
        # Deduplicate identical full lineage rows (source_table, source_column, target_table, target_column, expression)
        seen_target = set()
        deduped: List[LineageRecord] = []
        for r in rows:
            key = (r.source_table, r.source_column, r.target_table, r.target_column, r.expression)
            if key in seen_target:
                continue
            seen_target.add(key)
            deduped.append(r)
        # Remove placeholder None origins when concrete origin for same target/expression exists
        grouped = {}
        for r in deduped:
            gkey = (r.target_table, r.target_column, r.expression)
            grouped.setdefault(gkey, []).append(r)
        filtered: List[LineageRecord] = []
        for gkey, items in grouped.items():
            concrete = [i for i in items if i.source_table or i.source_column]
            if concrete:
                # remove placeholders (None,None)
                filtered.extend([c for c in concrete if c.source_table or c.source_column])
            else:
                # Keep constants only if there is no other row for that target/expression (already known here); acceptable
                # But if expression is a NULL/constant and target name already appears elsewhere (handled after grouping) it will be filtered.
                filtered.extend(items)
        # Final pass: if multiple rows share target_column and any have concrete origin, drop constant-only rows
        by_target = {}
        for r in filtered:
            by_target.setdefault(r.target_column, []).append(r)
        final_filtered: List[LineageRecord] = []
        for tcol, items in by_target.items():
            concretes = [i for i in items if i.source_table or i.source_column]
            if concretes:
                final_filtered.extend(concretes)
            else:
                final_filtered.extend(items)
        # Secondary collapse: for duplicate target_column mapping to same source_table/source_column keep first
        final: List[LineageRecord] = []
        collapse_seen = set()
        for r in final_filtered:
            if r.target_column:
                ckey = (r.source_table, r.source_column, r.target_table, r.target_column)
                if ckey in collapse_seen:
                    continue
                collapse_seen.add(ckey)
            final.append(r)
        return final

    # unreachable due to return above

    # --------------- non-select lineage helpers ---------------
    def _analyze_expression_origins(self, expression: exp.Expression, sources: List[TableSource | SelectSource]) -> List[LineageRecord]:
        from .core.analyzer import SelectAnalyzer
        # Reuse analyzer by creating a synthetic SELECT expression list
        fake_select = exp.select(expression)
        # Build temporary environment for sources already passed (no CTE resolution here)
        env = AnalysisEnvironment(ctes={})
        analyzer = SelectAnalyzer(fake_select, env, self.schema, self.engine)
        # Monkey patch _build_sources to supply provided sources
        def _custom_build(_self, _select):
            out = []
            for s in sources:
                if isinstance(s, TableSource):
                    out.append((s.table_name.split('.')[-1], s))
                else:
                    out.append(('_sub', s))
            return out
        analyzer._build_sources = _custom_build.__get__(analyzer, SelectAnalyzer)  # type: ignore
        expr_lineages = analyzer.analyze()
        rows: List[LineageRecord] = []
        for el in expr_lineages:
            for origin in el.origins:
                if origin.table or origin.column:
                    rows.append(LineageRecord(
                        source_table=origin.table,
                        source_column=origin.column,
                        expression=el.expression_sql,
                        target_column=None,
                        target_table=None,
                        file=None,
                        engine=self.engine,
                    ))
        return rows

    def _extract_update(self, update: exp.Update, env: AnalysisEnvironment, file: Optional[str]) -> List[LineageRecord]:
        target_table = None
        if isinstance(update.this, exp.Table):
            target_table = self._table_name(update.this)
        # sources: target plus any tables referenced in FROM / USING style joins (simplified)
        sources: List[TableSource | SelectSource] = []
        if target_table:
            sources.append(TableSource(target_table, self.schema))
        from_clause = update.args.get('from')
        if from_clause and hasattr(from_clause, 'this'):
            fc_this = from_clause.this
            if isinstance(fc_this, exp.Table):
                sources.append(TableSource(self._table_name(fc_this), self.schema))
            elif isinstance(fc_this, exp.Subquery) and isinstance(fc_this.this, exp.Select):
                # treat subquery as select source
                sources.append(SelectSource(fc_this.this, env, self.schema))
        rows: List[LineageRecord] = []
        assignments = update.args.get('expressions') or []
        for assign in assignments:
            # Assignment nodes often are EQ with left Column and right expression
            if isinstance(assign, exp.EQ):
                left = assign.left
                right = assign.right
            else:
                left = getattr(assign, 'this', None)
                right = getattr(assign, 'expression', None)
            if not isinstance(left, exp.Column) or right is None:
                continue
            target_col = _norm(left.name)
            # Extract origins of right expression
            origins = []
            for col in right.find_all(exp.Column):
                name = _norm(col.name)
                tbl_alias = _norm(col.table) if col.table else None
                phys_table = None
                if tbl_alias:
                    # Attempt to resolve alias to target_table (simple heuristic: if alias isn't a registered table name but target table exists)
                    if target_table and tbl_alias != target_table.split('.')[-1]:
                        # If any source table matches alias, use its physical name
                        for s in sources:
                            if isinstance(s, TableSource) and (s.table_name.split('.')[-1] == tbl_alias or s.table_name == tbl_alias):
                                phys_table = s.table_name
                                break
                    if not phys_table:
                        for s in sources:
                            if isinstance(s, TableSource) and (s.table_name.split('.')[-1] == tbl_alias or s.table_name == tbl_alias):
                                phys_table = s.table_name
                                break
                if not phys_table:
                    # unqualified or unresolved alias: search in sources for column membership
                    for s in sources:
                        if isinstance(s, TableSource) and name in self.schema.columns(s.table_name):
                            phys_table = s.table_name
                            break
                if not phys_table and target_table and name in self.schema.columns(target_table):
                    phys_table = target_table
                origins.append((phys_table, name))
            if not origins:
                origins.append((None, None))
            for tbl, col in origins:
                rows.append(LineageRecord(
                    source_table=tbl,
                    source_column=col,
                    expression=expr_sql(right, self.engine),
                    target_column=target_col,
                    target_table=target_table,
                    file=file,
                    engine=self.engine,
                ))
        return rows

    def _extract_merge(self, merge: exp.Merge, env: AnalysisEnvironment, file: Optional[str]) -> List[LineageRecord]:
        rows: List[LineageRecord] = []
        target_table = None
        if merge.this and isinstance(merge.this, exp.Table):
            target_table = self._table_name(merge.this)
        source_table = None
        using = merge.args.get('using')
        if using and isinstance(using.this, exp.Table):
            source_table = self._table_name(using.this)
        using_subquery_select = None
        if using and isinstance(using.this, exp.Subquery) and isinstance(using.this.this, exp.Select):
            using_subquery_select = using.this.this
        # Process merge clauses
        whens = merge.args.get('whens')
        if whens:
            for clause in whens.expressions if hasattr(whens, 'expressions') else []:
                if isinstance(clause, exp.When):
                    then_expr = clause.args.get('then')
                    # UPDATE clause lineage (direct handling to resolve alias src)
                    if isinstance(then_expr, exp.Update) and target_table:
                        assignments = then_expr.args.get('expressions') or []
                        for assign in assignments:
                            if isinstance(assign, exp.EQ):
                                left = assign.left
                                right = assign.right
                            else:
                                left = getattr(assign, 'this', None)
                                right = getattr(assign, 'expression', None)
                            if not isinstance(left, exp.Column) or right is None:
                                continue
                            tgt_col = _norm(left.name)
                            # collect column origins in right expression
                            origin_cols = []
                            for col in right.find_all(exp.Column):
                                col_name = _norm(col.name)
                                alias = _norm(col.table) if col.table else None
                                using_alias_name = None
                                if using and getattr(using, 'alias', None):
                                    ua = getattr(using, 'alias')
                                    using_alias_name = _norm(getattr(ua, 'this', getattr(ua, 'name', None)))
                                if alias and using and isinstance(using.this, exp.Subquery) and using_alias_name and alias == using_alias_name:
                                    origin_cols.append((source_table, col_name))
                                elif alias and source_table and alias in (source_table.split('.')[-1], source_table):
                                    origin_cols.append((source_table, col_name))
                                elif source_table and col_name in self.schema.columns(source_table):
                                    origin_cols.append((source_table, col_name))
                                else:
                                    origin_cols.append((None, col_name))
                            if not origin_cols:
                                origin_cols.append((None, None))
                            for ot, oc in origin_cols:
                                resolved_table = ot
                                if resolved_table is None and target_table and oc and oc in self.schema.columns(target_table):
                                    resolved_table = target_table
                                rows.append(LineageRecord(
                                    source_table=resolved_table,
                                    source_column=oc,
                                    expression=expr_sql(right, self.engine),
                                    target_column=tgt_col,
                                    target_table=target_table,
                                    file=file,
                                    engine=self.engine,
                                ))
                    # INSERT clause lineage (VALUES columns) mapping index-wise
                    elif isinstance(then_expr, exp.Insert) and target_table:
                        insert = then_expr
                        # columns in INSERT statement (this tuple) correspond to target order
                        target_cols = []
                        if isinstance(insert.this, exp.Tuple):
                            for c in insert.this.expressions:
                                if isinstance(c, exp.Column):
                                    target_cols.append(_norm(c.name))
                        value_exprs = []
                        expr_tuple = insert.args.get('expression')
                        if isinstance(expr_tuple, exp.Tuple):
                            value_exprs = expr_tuple.expressions
                        for idx, ve in enumerate(value_exprs):
                            tgt_col = target_cols[idx] if idx < len(target_cols) else None
                            if not tgt_col:
                                continue
                            origins = []
                            for col in ve.find_all(exp.Column):
                                col_name = _norm(col.name)
                                alias = _norm(col.table) if col.table else None
                                if alias and source_table and alias in (source_table.split('.')[-1], source_table):
                                    origins.append((source_table, col_name))
                                elif source_table and col_name in self.schema.columns(source_table):
                                    origins.append((source_table, col_name))
                                else:
                                    origins.append((None, col_name))
                            if not origins:
                                origins.append((None, None))
                            for ot, oc in origins:
                                resolved_table = ot
                                if resolved_table is None and target_table and oc and oc in self.schema.columns(target_table):
                                    resolved_table = target_table
                                rows.append(LineageRecord(
                                    source_table=resolved_table,
                                    source_column=oc,
                                    expression=expr_sql(ve, self.engine),
                                    target_column=tgt_col,
                                    target_table=target_table,
                                    file=file,
                                    engine=self.engine,
                                ))
        # For insert mapping ensure target_table set
        # Rebuild rows with target_table set (immutable dataclass)
        updated: List[LineageRecord] = []
        for r in rows:
            updated.append(LineageRecord(
                source_table=r.source_table,
                source_column=r.source_column,
                expression=r.expression,
                target_column=r.target_column,
                target_table=r.target_table or target_table,
                file=file,
                engine=self.engine,
            ))
        rows = updated
        return rows

    # --------------- helpers ---------------
    def _table_name(self, table: exp.Table) -> Optional[str]:
        parts = []
        if table.args.get('catalog'):
            parts.append(str(table.catalog))
        if table.args.get('db'):
            parts.append(str(table.db))
        this = table.this
        if hasattr(this, 'name'):
            parts.append(str(this.name))
        else:
            parts.append(str(this))
        return _norm(".".join(p for p in parts if p))

