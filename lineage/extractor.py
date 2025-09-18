from __future__ import annotations

import logging
import os
from typing import Dict, Iterable, List, Optional, Tuple

import sqlglot
from sqlglot import expressions as exp

from .logger import get_logger
from .models import LineageRecord
from .utils import (
    alias_or_table_for_column,
    alias_to_str,
    collect_cte_aliases,
    infer_target_table,
    is_insert,
    is_select,
    is_with,
    normalize_identifier,
    table_name_of,
    guess_table_from_prefix,
)


class LineageExtractor:
    """
    Extract lineage records from SQL files using sqlglot.

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

    def _normalize_schema(self, schema: Dict) -> Dict[str, List[str]]:
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
        try:
            return sqlglot.parse(sql_text, read=self.engine)
        except Exception as e:
            self.logger.error(f"Failed to parse SQL for engine={self.engine}: {e}")
            raise

    def extract_from_file(self, path: str) -> List[LineageRecord]:
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
        self.logger.debug(f"stmt type: {type(stmt)}, is_with: {is_with(stmt)}")
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

    def _flatten_projection(self, select_expr: exp.Select, proj_map: Dict[str, exp.Expression]) -> List[Tuple[Optional[str], exp.Expression]]:
        result: List[Tuple[Optional[str], exp.Expression]] = []
        for proj in select_expr.expressions:
            alias = None
            expr = proj
            if isinstance(proj, exp.Alias):
                alias = normalize_identifier(proj.alias)
                expr = proj.this
            elif isinstance(proj, exp.Star):
                # Expand * if possible; if not, keep a synthetic '*'
                expanded = self._expand_star(select_expr)
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

    def _extract_from_select(
        self,
        select_expr: exp.Select,
        target_table: Optional[str],
        target_cols: List[str],
        cte_map: Dict[str, exp.Expression],
        file: Optional[str],
    ) -> List[LineageRecord]:
        self.logger.info(f"cte_map in _extract_from_select: {list(cte_map.keys())}")
        recs: List[LineageRecord] = []

        # Build projection map for alias resolution
        proj_map = self._projection_name_to_expr(select_expr)

        # Determine potential target column names when not explicitly provided
        proj = self._flatten_projection(select_expr, proj_map)
        derived_targets: List[str] = []
        for alias, expr in proj:
            derived_targets.append(alias or self._expr_display(expr))

        # Iterate projection expressions
        # Pre-compute explicitly projected simple column names (excluding stars) to avoid star duplication
        explicit_simple_cols: set[str] = set()
        for p in select_expr.expressions:
            if isinstance(p, exp.Column):
                explicit_simple_cols.add(normalize_identifier(p.name))
            elif isinstance(p, exp.Alias) and isinstance(p.this, exp.Column):
                explicit_simple_cols.add(normalize_identifier(p.this.name))

        for idx, (alias, expr) in enumerate(proj):
            target_col = None
            if target_cols and idx < len(target_cols):
                target_col = target_cols[idx]
            else:
                target_col = alias or None

            # Find source columns inside expression
            source_cols = list(expr.find_all(exp.Column))
            # Handle SELECT * specially: no Column nodes inside Star, so produce mappings
            if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Identifier) and expr.this.this == '*':
                # Improved STAR expansion: respect intermediate CTE projections (do NOT blindly expand to base schema)
                from_ = select_expr.args.get("from")
                if not from_:
                    continue
                # Detect undefined references
                for t in from_.find_all(exp.Table):
                    tname = table_name_of(t)
                    if tname and tname not in self.schema and tname not in cte_map:
                        try:
                            self.logger.warning(f"Potential undefined table/CTE reference: {tname}")
                        except Exception:
                            pass
                # Gather all tables (FROM + JOINs)
                multi_tables: List[str] = []
                for t in from_.find_all(exp.Table):
                    tn = table_name_of(t)
                    if tn:
                        multi_tables.append(tn)
                for j in (select_expr.args.get("joins") or []):
                    jt = j.this
                    if isinstance(jt, exp.Table):
                        tn = table_name_of(jt)
                        if tn:
                            multi_tables.append(tn)
                # If more than one base table and no single CTE indirection, use schema-based expansion per table
                if len(set(multi_tables)) > 1:
                    emitted = False
                    for bt in multi_tables:
                        cols = self.schema.get(bt, [])
                        if cols:
                            for c in cols:
                                if normalize_identifier(c) in explicit_simple_cols:
                                    continue
                                recs.append(LineageRecord(
                                    source_table=bt,
                                    source_column=normalize_identifier(c),
                                    expression='*',
                                    target_column=None,
                                    target_table=target_table,
                                    file=file,
                                    engine=self.engine,
                                ))
                                emitted = True
                    if emitted:
                        continue
                # Single source table or CTE
                tables = [t for t in from_.find_all(exp.Table)]
                if len(tables) != 1:
                    # Fall back – unresolved star
                    recs.append(LineageRecord(
                        source_table=None,
                        source_column='*',
                        expression='*',
                        target_column=None,
                        target_table=target_table,
                        file=file,
                        engine=self.engine,
                    ))
                    continue
                single_name = table_name_of(tables[0])
                if not single_name:
                    continue
                # If it's a CTE, enumerate its effective output columns honoring projection pruning/aliasing
                if single_name in cte_map:
                    for st, sc in self._enumerate_cte_output_sources(single_name, cte_map, set()):
                        if sc in explicit_simple_cols:
                            continue
                        recs.append(LineageRecord(
                            source_table=st,
                            source_column=sc,
                            expression='*',
                            target_column=None,
                            target_table=target_table,
                            file=file,
                            engine=self.engine,
                        ))
                    continue
                # Base table: expand via schema if available
                cols = self.schema.get(single_name, [])
                if cols:
                    for c in cols:
                        if normalize_identifier(c) in explicit_simple_cols:
                            continue
                        recs.append(LineageRecord(
                            source_table=single_name,
                            source_column=normalize_identifier(c),
                            expression='*',
                            target_column=None,
                            target_table=target_table,
                            file=file,
                            engine=self.engine,
                        ))
                else:
                    recs.append(LineageRecord(
                        source_table=single_name,
                        source_column='*',
                        expression='*',
                        target_column=None,
                        target_table=target_table,
                        file=file,
                        engine=self.engine,
                    ))
                continue
            if not source_cols:
                # Expression without column reference (e.g., literal)
                recs.append(
                    LineageRecord(
                        source_table=None,
                        source_column=None,
                        expression=self._expr_display(expr),
                        target_column=target_col,
                        target_table=target_table,
                        file=file,
                        engine=self.engine,
                    )
                )
                continue

            for col in source_cols:
                for tbl, scol in self._resolve_column_sources(select_expr, col, cte_map, proj_map):
                    recs.append(
                        LineageRecord(
                            source_table=tbl,
                            source_column=scol,
                            expression=self._expr_display(expr),
                            target_column=target_col,
                            target_table=target_table,
                            file=file,
                            engine=self.engine,
                        )
                    )

        return recs

    def _resolve_table_name(
        self,
        scope: exp.Select,
        alias: Optional[str],
        cte_map: Dict[str, exp.Expression],
    ) -> Optional[str]:
        alias_map = self._build_alias_map(scope)
        if alias and alias in alias_map:
            return alias_map[alias]
        if not alias:
            # Try to infer from single-source select
            from_ = scope.args.get("from")
            if from_:
                tables = [t for t in from_.find_all(exp.Table)]
                if len(tables) == 1:
                    return table_name_of(tables[0])
            return None

        # If alias is a CTE name
        if alias in cte_map:
            inner = cte_map[alias]
            # If CTE is a select from table(s), and only one base table, return it
            from_ = inner.args.get("from") if isinstance(inner, exp.Select) else None
            if from_:
                base_tables = [table_name_of(t) for t in from_.find_all(exp.Table)]
                base_tables = [t for t in base_tables if t]
                if len(base_tables) == 1:
                    return base_tables[0]
            return alias

        # Fallback: return alias as unknown table name
        return alias

    def _expr_display(self, expr: exp.Expression) -> str:
        try:
            return expr.sql(dialect=self.engine)
        except Exception:
            return str(expr)

    def _build_alias_map(self, scope: exp.Select) -> Dict[str, str]:
        """
        Build a mapping of alias -> base table name for the current select scope.
        Also include short base table names to full names to help disambiguate.
        """
        from .utils import alias_to_str
        result: Dict[str, str] = {}
        # Traverse all tables in the SELECT scope (FROM + JOINs)
        for t in scope.find_all(exp.Table):
            base = table_name_of(t)
            if not base:
                continue
            # map alias -> base
            a = normalize_identifier(alias_to_str(getattr(t, "alias", None)))
            try:
                self.logger.debug(
                    f"Table node: base={base}, raw={t.sql(dialect=self.engine)}, alias_obj={getattr(t,'alias',None)}, alias={a}"
                )
            except Exception:
                pass
            if a:
                result[a] = base
            # map short base name -> base
            short = base.split(".")[-1]
            result.setdefault(short, base)
        try:
            self.logger.debug(f"Alias map: {result}")
        except Exception:
            pass
        return result

    def _get_subquery_by_alias(self, scope: exp.Select, alias: str) -> Optional[exp.Expression]:
        for sub in scope.find_all(exp.Subquery):
            # Check if sub has alias
            a = normalize_identifier(alias_to_str(getattr(sub, "alias", None)))
            try:
                self.logger.debug(
                    f"Inspecting subquery alias: sub_sql={sub.sql(dialect=self.engine)}, sub.alias={getattr(sub, 'alias', None)}, norm_alias={a}, target_alias={alias}"
                )
            except Exception:
                pass
            if a == alias:
                return sub.this
            # Check if parent From has alias
            parent = sub.parent
            if isinstance(parent, exp.From) and parent.alias:
                a = normalize_identifier(alias_to_str(parent.alias))
                try:
                    self.logger.debug(
                        f"Inspecting parent FROM alias: parent.alias={parent.alias}, norm_alias={a}, target_alias={alias}"
                    )
                except Exception:
                    pass
                if a == alias:
                    return sub.this
        return None

    def _alias_or_table_for_column(self, scope: exp.Select, column: exp.Column) -> Optional[str]:
        tbl = column.table
        if tbl:
            return normalize_identifier(tbl)
        name = normalize_identifier(column.name)
        # Check alias map
        alias_map = self._build_alias_map(scope)
        for alias, base in alias_map.items():
            if alias == name or base.split(".")[-1] == name:
                return alias
        # Check subqueries
        for sub in scope.find_all(exp.Subquery):
            a = normalize_identifier(alias_to_str(getattr(sub, "alias", None)))
            if a:
                query = sub.this
                parts = self._get_select_parts(query)
                for part in parts:
                    if name in self._projection_name_to_expr(part):
                        return a
        return None

    def _get_select_parts(self, query: exp.Expression) -> List[exp.Select]:
        """Recursively get all SELECT parts from a query, handling UNIONs."""
        parts = []
        if isinstance(query, exp.Union):
            parts.extend(self._get_select_parts(query.left))
            parts.extend(self._get_select_parts(query.right))
        elif isinstance(query, exp.Select):
            parts.append(query)
        return parts

    def _expand_star(self, scope: exp.Select) -> List[Tuple[str, exp.Expression]]:
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
            inner_proj = self._flatten_projection(inner, self._projection_name_to_expr(inner))
            for alias, _ in inner_proj:
                col_name = alias or ""
                if not col_name:
                    continue
                col_expr = exp.Column(this=exp.Identifier(this=col_name))
                if a:
                    col_expr.set("table", exp.Identifier(this=a))
                out.append((col_name, col_expr))
        return out

    def _resolve_column_sources(
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

        key = (id(scope), name)
        _visited.add(key)

        # Resolve alias if column is an alias in current scope
        if not tbl and name in proj_map:
            resolved_expr = proj_map.get(name)
            if resolved_expr:
                inner_cols = list(resolved_expr.find_all(exp.Column))
                if inner_cols:
                    results = []
                    for c in inner_cols:
                        results.extend(self._resolve_column_sources(scope, c, cte_map, proj_map, _visited))
                    return results

        # Determine alias via inference if missing
        alias = tbl or self._alias_or_table_for_column(scope, column)
        if not alias:
            # If there's exactly one source table in this scope, and it corresponds to a CTE or a subquery,
            # resolve through it instead of treating it as a base table name.
            one_alias = self._single_source_alias(scope)
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
            parts = self._get_select_parts(inner_select)
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
                    has_star = any(isinstance(p, exp.Star) for p in part.expressions)
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
                    guessed = guess_table_from_prefix(name, candidates)
                    if guessed:
                        results.append((guessed, name))
                    else:
                        results.append((None, None))
                else:
                    for c in inner_cols:
                        results.extend(
                            self._resolve_column_sources(part, c, {**cte_map, **inner_ctes}, proj_map, _visited)
                        )
            if (not results or all(a is None for a, _ in results)) and isinstance(inner_select, exp.Select):
                # Guess from inner base tables when projection wasn't found
                candidates = []
                from_ = inner_select.args.get("from")
                if from_:
                    for t in from_.find_all(exp.Table):
                        tname = table_name_of(t)
                        if tname:
                            candidates.append(tname)
                guessed = guess_table_from_prefix(name, candidates)
                if guessed:
                    return [(guessed, name)]
            self.logger.debug(f"CTE results for {name}: {results}")
            return [(a, b) for a, b in results if a is not None] or [(None, name)]

        # Subquery resolution within current scope
        if alias:
            sub = self._get_subquery_by_alias(scope, alias)
            if sub:
                # Prevent infinite recursion: we'll track visited per select part below
                # Unwrap WITH if present to collect inner CTEs
                inner_ctes: Dict[str, exp.Expression] = collect_cte_aliases(sub)
                inner_select = sub.this if isinstance(sub, exp.With) else sub
                # Handle UNION
                parts = self._get_select_parts(inner_select)
                for part in parts:
                    if not isinstance(part, exp.Select):
                        continue
                    proj_map = self._projection_name_to_expr(part)
                    try:
                        self.logger.debug(
                            f"Subquery resolution: alias={alias}, name={name}, part_sql={part.sql(dialect=self.engine)}, proj_map_keys={list(proj_map.keys())}"
                        )
                    except Exception:
                        pass
                    part_key = (id(part), name)
                    if part_key not in _visited:
                        _visited.add(part_key)
                        expr = proj_map.get(name)
                        if expr is None:
                            try:
                                self.logger.debug(
                                    f"Subquery resolution: expr for name={name} not found in proj_map"
                                )
                            except Exception:
                                pass
                            # Fallback 1: find alias node whose alias name matches
                            for proj in part.expressions:
                                if isinstance(proj, exp.Alias):
                                    alias_name = normalize_identifier(proj.alias)
                                    if alias_name == name:
                                        expr = proj.this
                                        break
                            # Fallback 2: if only one projection column, use it
                            if expr is None and len(part.expressions) == 1:
                                only_proj = part.expressions[0]
                                if isinstance(only_proj, exp.Alias):
                                    expr = only_proj.this
                                elif isinstance(only_proj, exp.Column):
                                    expr = only_proj
                        if expr is not None:
                            inner_cols = list(expr.find_all(exp.Column))
                            try:
                                self.logger.debug(
                                    f"Subquery resolution: expr={self._expr_display(expr)}, inner_cols={inner_cols}"
                                )
                            except Exception:
                                pass
                            if not inner_cols:
                                # Try to guess from subquery's base tables
                                candidates = []
                                from_ = part.args.get("from")
                                if from_:
                                    for t in from_.find_all(exp.Table):
                                        tname = table_name_of(t)
                                        if tname:
                                            candidates.append(tname)
                                guessed = guess_table_from_prefix(name, candidates)
                                if guessed:
                                    return [(guessed, name)]
                                return [(None, None)]
                            for c in inner_cols:
                                results.extend(self._resolve_column_sources(part, c, {**cte_map, **inner_ctes}, proj_map, _visited))
                            return [(a, b) for a, b in results if a is not None] or [(None, name)]        # Base table
        if alias:
            src_table = self._resolve_table_name(scope, alias, cte_map)
            self.logger.debug(f"Resolving {name} with explicit/derived alias {alias} -> {src_table}")
            return [(src_table, name)]

        # Unqualified column disambiguation using schema
        candidate_tables: List[str] = []
        from_ = scope.args.get("from")
        if from_:
            for t in from_.find_all(exp.Table):
                tname = table_name_of(t)
                if tname:
                    candidate_tables.append(tname)
        joins = scope.args.get("joins") or []
        for join in joins:
            jt = join.this
            if isinstance(jt, exp.Table):
                tname = table_name_of(jt)
                if tname:
                    candidate_tables.append(tname)

        schema_hits = [t for t in candidate_tables if t in self.schema and name in self.schema[t]]
        if len(schema_hits) == 1:
            self.logger.debug(f"Schema disambiguated column {name} -> {schema_hits[0]}")
            return [(schema_hits[0], name)]
        if len(schema_hits) > 1:
            self.logger.debug(f"Ambiguous column {name} across {schema_hits}; returning all")
            return [(t, name) for t in schema_hits]

        guessed = guess_table_from_prefix(name, candidate_tables)
        if guessed:
            return [(guessed, name)]

        src_table = self._resolve_table_name(scope, None, cte_map)
        self.logger.debug(f"Fallback single-table inference for {name} -> {src_table}")
        return [(src_table, name)]

    def _single_source_alias(self, scope: exp.Select) -> Optional[str]:
        tables = []
        from_ = scope.args.get("from")
        if from_:
            for t in from_.find_all(exp.Table):
                tables.append(t)
        subqueries = []
        if from_:
            for s in from_.find_all(exp.Subquery):
                subqueries.append(s)
        joins = scope.args.get("joins") or []
        if joins:
            return None  # Multiple sources, don't infer
        if len(tables) == 1 and not subqueries:
            t = tables[0]
            # Prefer alias if present, otherwise table name
            a = normalize_identifier(alias_to_str(getattr(t, "alias", None)))
            if a:
                return a
            name = table_name_of(t)
            return name.split(".")[-1] if name else None
        if len(subqueries) == 1 and not tables:
            s = subqueries[0]
            return normalize_identifier(alias_to_str(getattr(s, "alias", None)))
        return None

    def _projection_name_to_expr(self, select_expr: exp.Select) -> Dict[str, exp.Expression]:
        mapping: Dict[str, exp.Expression] = {}
        for proj in select_expr.expressions:
            if isinstance(proj, exp.Alias):
                alias = normalize_identifier(proj.alias)
                if alias:
                    mapping[alias] = proj.this
        return mapping

    def _resolve_cte_base(self, cte_name: str, cte_map: Dict[str, exp.Expression]) -> str:
        """Walk a chain of CTEs to find an ultimate base table name.

        If the chain ends in a SELECT with exactly one table in FROM (no joins) and no further CTE indirections, return that table name.
        Otherwise return the last resolvable name (may be the original CTE name).
        """
        seen: set[str] = set()
        current = cte_name
        while current in cte_map and current not in seen:
            seen.add(current)
            node = cte_map[current]
            # Unwrap WITH inside CTE definition
            inner = node.this if isinstance(node, exp.With) else node
            if not isinstance(inner, exp.Select):
                break
            from_ = inner.args.get("from")
            if not from_:
                break
            tables = [table_name_of(t) for t in from_.find_all(exp.Table)]
            tables = [t for t in tables if t]
            joins = inner.args.get("joins") or []
            # If single table and no joins, attempt to follow if that table is itself a CTE
            if len(tables) == 1 and not joins:
                nxt = tables[0]
                if nxt in cte_map and nxt not in seen:
                    current = nxt
                    continue
                # Reached a base table
                return nxt
            break
        return current

    def _enumerate_cte_output_sources(self, cte_name: str, cte_map: Dict[str, exp.Expression], visited: set) -> List[Tuple[str, str]]:
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
        if not isinstance(inner, exp.Select):
            return []
        # If UNION inside, gather both sides separately (treat as ambiguous set of sources)
        if isinstance(inner, exp.Union):
            out: List[Tuple[str, str]] = []
            for part in self._get_select_parts(inner):
                if isinstance(part, exp.Select):
                    out.extend(self._enumerate_select_output_sources(part, cte_map, visited))
            return out
        return self._enumerate_select_output_sources(inner, cte_map, visited)

    def _enumerate_select_output_sources(self, select_expr: exp.Select, cte_map: Dict[str, exp.Expression], visited: set) -> List[Tuple[str, str]]:
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
        # Simple chain case
        if star_only and len(base_tables) == 1 and not joins:
            base = base_tables[0]
            if base in cte_map:
                return self._enumerate_cte_output_sources(base, cte_map, visited)
            # Base physical table
            cols = self.schema.get(base, [])
            if cols:
                return [(base, normalize_identifier(c)) for c in cols]
            return [(base, '*')]
        # Explicit projections: resolve each column expression lineage
        proj_map = self._projection_name_to_expr(select_expr)
        for proj in select_expr.expressions:
            if isinstance(proj, exp.Star):
                # STAR among explicit columns: expand relative to single table if possible else skip (handled earlier)
                if len(base_tables) == 1 and not joins:
                    base = base_tables[0]
                    if base in cte_map:
                        out.extend(self._enumerate_cte_output_sources(base, cte_map, visited))
                    else:
                        cols = self.schema.get(base, [])
                        if cols:
                            out.extend([(base, normalize_identifier(c)) for c in cols])
                        else:
                            out.append((base, '*'))
                continue
            expr = proj.this if isinstance(proj, exp.Alias) else proj
            alias = normalize_identifier(proj.alias) if isinstance(proj, exp.Alias) else None
            # Collect columns inside expr
            cols = list(expr.find_all(exp.Column))
            if not cols:
                continue
            temp_select = select_expr  # reuse scope for resolution
            for c in cols:
                for st, sc in self._resolve_column_sources(temp_select, c, cte_map, proj_map):
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
