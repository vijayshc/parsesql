from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from sqlglot import expressions as exp

from .origin import ColumnOrigin, ExpressionLineage
from .schema import Schema, _norm
from .sources import AnalysisEnvironment, SourceBase, TableSource, SelectSource


def expr_sql(e: exp.Expression, dialect: str) -> str:
    try:
        return e.sql(dialect=dialect)
    except Exception:
        return str(e)


@dataclass
class SelectAnalyzer:
    """Analyze a SELECT or UNION expression to produce expression-level lineage.

    Algorithm (first principles simplification):
    1. Build source list from FROM + JOIN clauses (order preserved for star expansion)
    2. For each projection expression in order:
       a. If star -> expand according to qualification and enumerate underlying physical origins
       b. Else gather Column nodes referenced; resolve each to base origins
       c. Determine output column name (alias, else simple column name, else None)
       d. Emit ExpressionLineage with all resolved origins
    3. Resolution rules for a Column:
       - Qualified: look up alias/base in source map; delegate to that source
       - Unqualified with single source: delegate
       - Unqualified with multiple sources: disambiguate via schema unique match; if ambiguous -> all candidates
    4. CTE and subquery handling: represented as SelectSource; lazy analysis ensures recursion termination.
    """

    select: exp.Expression  # Select or Union
    env: AnalysisEnvironment
    schema: Schema
    dialect: str = "spark"

    def analyze(self) -> List[ExpressionLineage]:
        if isinstance(self.select, exp.Union):
            # Merge branch outputs; treat each branch individually but we don't know output ordering if different.
            out: List[ExpressionLineage] = []
            for part in self._select_parts(self.select):
                sa = SelectAnalyzer(part, self.env, self.schema, self.dialect)
                out.extend(sa.analyze())
            return out
        if not isinstance(self.select, exp.Select):
            return []
        sources = self._build_sources(self.select)
        lineages: List[ExpressionLineage] = []
        for proj in self.select.expressions:
            if isinstance(proj, exp.Star):
                lineages.extend(self._expand_unqualified_star(sources, proj))
                continue
            # Qualified star like alias.* represented as Column with this=Star
            if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
                lineages.extend(self._expand_qualified_star(sources, proj))
                continue
            # Alias wrapper
            expr = proj.this if isinstance(proj, exp.Alias) else proj
            out_col = None
            if isinstance(proj, exp.Alias):
                out_col = _norm(str(proj.alias))
            elif isinstance(expr, exp.Column):
                out_col = _norm(expr.name)
            origins = self._origins_for_expression(expr, sources)
            # If column expression with no resolved origins, attach placeholder
            if isinstance(expr, exp.Column) and not any(o.table or o.column for o in origins):
                origins = [ColumnOrigin(table=None, column=None)]
            lineages.append(ExpressionLineage(expression_sql=expr_sql(expr, self.dialect), output_column=out_col, origins=tuple(origins)))
        return lineages

    # ------- helpers ---------
    def _select_parts(self, query: exp.Expression) -> List[exp.Select]:
        parts: List[exp.Select] = []
        if isinstance(query, exp.Union):
            parts.extend(self._select_parts(query.left))
            parts.extend(self._select_parts(query.right))
        elif isinstance(query, exp.Select):
            parts.append(query)
        return parts

    def _build_sources(self, select: exp.Select) -> List[Tuple[str, SourceBase]]:
        sources: List[Tuple[str, SourceBase]] = []  # (alias_or_name, source)
        from_ = select.args.get("from")
        if from_:
            self._collect_source_term(from_.this, sources)
        for j in select.args.get("joins") or []:
            self._collect_source_term(j.this, sources)
        return sources

    def _collect_source_term(self, term: exp.Expression, out: List[Tuple[str, SourceBase]]):
        # Handle pivot applied directly to a table or subquery: PIVOT node wraps underlying source
        from sqlglot import expressions as _e
        if isinstance(term, _e.Pivot):
            base = term.this
            # Recursively collect underlying base
            self._collect_source_term(base, out)
            # Wrap last collected source with pivot mapping
            if out:
                alias, src = out.pop()
                src = self._wrap_pivots(src, [term])
                out.append((alias, src))
            return
        if isinstance(term, exp.Subquery):
            alias = _norm(self._alias_of(term))
            inner = term.this
            if isinstance(inner, (exp.Select, exp.Union)):
                    src = SelectSource(inner, self.env, self.schema)
                    # If the subquery node itself has pivots, wrap the produced source
                    pivots = term.args.get('pivots') or []
                    if pivots:
                        src = self._wrap_pivots(src, pivots)
                    out.append((alias or '_q_derived', src))
        elif isinstance(term, exp.Table):
            name = self._table_name(term)
            alias = _norm(self._alias_of(term)) or (name.split('.')[-1] if name else None)
            if name:
                # If table name OR alias matches a registered CTE, use that SelectSource
                cte_src = self.env.get(name) or (alias and self.env.get(alias))
                if cte_src:
                    base_src: SourceBase = cte_src
                else:
                    base_src = TableSource(name, self.schema)
                # Wrap with pivot if present
                pivots = term.args.get('pivots') or []
                if pivots:
                    base_src = self._wrap_pivots(base_src, pivots)
                out.append((_norm(alias or name), base_src))
        elif isinstance(term, exp.Values):
            # VALUES clause with optional alias + column list
            alias = _norm(self._alias_of(term)) or '_values'
            # Synthesize a source with columns from alias specification or positional indices
            cols = []
            alias_obj = getattr(term, 'alias', None)
            if alias_obj and getattr(alias_obj, 'args', {}).get('columns'):
                for c in alias_obj.args['columns']:
                    if hasattr(c, 'name'):
                        cols.append(_norm(c.name))
            if not cols and term.expressions:
                width = len(term.expressions[0].expressions)
                cols = [f'col{i+1}' for i in range(width)]
            # Simple inline source class
            class _ValuesSource(SourceBase):
                def output_columns(self_inner):
                    return cols
                def resolve_column(self_inner, name: str):
                    return [ColumnOrigin(table=None, column=name)]
            out.append((alias, _ValuesSource()))

    def _alias_of(self, node: exp.Expression) -> Optional[str]:
        alias = getattr(node, 'alias', None)
        if not alias:
            return None
        if isinstance(alias, exp.TableAlias):
            ident = alias.this
            if isinstance(ident, exp.Identifier):
                return ident.name or ident.this
            return str(ident)
        if isinstance(alias, exp.Identifier):
            return alias.name or alias.this
        name = getattr(alias, 'name', None)
        if name:
            return name
        return str(alias)

    def _table_name(self, table: exp.Table) -> Optional[str]:
        parts = []
        if table.args.get('catalog'):
            parts.append(str(table.catalog))
        if table.args.get('db'):
            parts.append(str(table.db))
        this = table.this
        if isinstance(this, exp.Identifier):
            parts.append(this.name or this.this)
        else:
            parts.append(str(this))
        return _norm(".".join(p for p in parts if p))

    def _expand_unqualified_star(self, sources: List[Tuple[str, SourceBase]], proj: exp.Star | exp.Expression) -> List[ExpressionLineage]:
        origins: List[ExpressionLineage] = []
        # For * we enumerate every column from each source in order
        for alias, src in sources:
            for col in src.output_columns():
                col_name = _norm(col)
                # Skip placeholder '*' from sources with unknown schema
                if col_name == '*':
                    continue
                col_origins = src.resolve_column(col_name)
                for o in col_origins:
                    origins.append(ExpressionLineage(expression_sql='*', output_column=col_name, origins=(o,)))
        return origins

    def _expand_qualified_star(self, sources: List[Tuple[str, SourceBase]], proj: exp.Column) -> List[ExpressionLineage]:
        alias = _norm(proj.table) if proj.table else None
        if not alias:
            return []
        # Find matching source by alias
        matches = [src for a, src in sources if a == alias]
        out: List[ExpressionLineage] = []
        for src in matches:
            for col in src.output_columns():
                col_name = _norm(col)
                if col_name == '*':
                    continue
                col_origins = src.resolve_column(col_name)
                for o in col_origins:
                    out.append(ExpressionLineage(expression_sql=f"{alias}.*", output_column=col_name, origins=(o,)))
        return out

    def _origins_for_expression(self, expr: exp.Expression, sources: List[Tuple[str, SourceBase]]) -> List[ColumnOrigin]:
        out: List[ColumnOrigin] = []
        # Scalar subqueries inside an expression
        for subq in expr.find_all(exp.Select):
            if subq is self.select:
                continue
            sa = SelectAnalyzer(subq, self.env, self.schema, self.dialect)
            for el in sa.analyze():
                # propagate real origins only (skip placeholder None/None rows)
                for o in el.origins:
                    if o.table or o.column:
                        out.append(o)
        # Explicit Subquery nodes (some constructs may not surface inner Select via find_all depending on wrapping)
        for sq in expr.find_all(exp.Subquery):
            inner = sq.this
            if isinstance(inner, (exp.Select, exp.Union)):
                sa = SelectAnalyzer(inner, self.env, self.schema, self.dialect)
                for el in sa.analyze():
                    for o in el.origins:
                        if o.table or o.column:
                            out.append(o)
        cols = list(expr.find_all(exp.Column))
        # Include window ORDER BY columns explicitly (some dialects may not surface via find_all depending on node structure)
        for win in expr.find_all(exp.Window):
            order = win.args.get('order')
            if order and hasattr(order, 'expressions'):
                for oe in order.expressions:
                    if isinstance(oe, exp.Ordered):
                        inner = oe.this
                        if isinstance(inner, exp.Column):
                            cols.append(inner)
        if not cols and not out:
            return [ColumnOrigin(table=None, column=None)]
        for c in cols:
            resolved = self._resolve_column(c, sources)
            # If resolution returns placeholder only and we have multiple sources with entirely unknown schemas (all '*'), keep placeholder
            out.extend(resolved)
        # Deduplicate
        seen = set()
        dedup: List[ColumnOrigin] = []
        for o in out:
            k = o.as_key()
            if k not in seen:
                seen.add(k)
                dedup.append(o)
        # If we obtained at least one concrete origin, drop generic None/None placeholders
        if any(o.table or o.column for o in dedup):
            # remove placeholder rows lacking table when a concrete table origin for same column exists
            by_col: Dict[str, List[ColumnOrigin]] = {}
            for o in dedup:
                by_col.setdefault(o.column or '__none__', []).append(o)
            cleaned: List[ColumnOrigin] = []
            for col, items in by_col.items():
                concrete = [i for i in items if i.table]
                if concrete:
                    # keep only concrete
                    # deduplicate tables
                    seen_tbl = set()
                    for c in concrete:
                        if c.table not in seen_tbl:
                            seen_tbl.add(c.table)
                            cleaned.append(c)
                else:
                    cleaned.extend(items)
            dedup = cleaned
        # If no origins resolved at all (unknown schema or star-only upstream), return a single placeholder origin
        if not dedup:
            return [ColumnOrigin(table=None, column=None)]
        return dedup

    def _wrap_pivots(self, base_src: SourceBase, pivot_nodes: List[exp.Pivot]) -> SourceBase:
        mapping: Dict[str, List[ColumnOrigin]] = {}
        for pivot in pivot_nodes:
            exprs = pivot.args.get('expressions') or []
            expr_cols: List[exp.Column] = []
            for ag in exprs:
                expr_cols.extend(list(ag.find_all(exp.Column)))
            origins: List[ColumnOrigin] = []
            for c in expr_cols:
                for o in base_src.resolve_column(_norm(c.name)):
                    if o not in origins:
                        origins.append(o)
            # If no direct columns (e.g., COUNT(*) scenario), fall back to all base columns
            if not origins:
                for col in base_src.output_columns():
                    for o in base_src.resolve_column(_norm(col)):
                        if o not in origins:
                            origins.append(o)
            pivot_cols = pivot.args.get('columns') or []
            for pc in pivot_cols:
                raw = getattr(pc, 'name', None) or getattr(pc, 'this', None)
                n = _norm(str(raw)) if raw else None
                if n:
                    mapping[n] = origins or [ColumnOrigin(table=None, column=None)]
        if not mapping:
            return base_src
        class _PivotSource(SourceBase):
            def output_columns(self_inner):
                return list({*base_src.output_columns(), *mapping.keys()})
            def resolve_column(self_inner, name: str):
                n = _norm(name)
                if n in mapping:
                    return mapping[n]
                return base_src.resolve_column(n)
        return _PivotSource()

    def _resolve_column(self, col: exp.Column, sources: List[Tuple[str, SourceBase]]) -> List[ColumnOrigin]:
        name = _norm(col.name)
        if col.table:  # qualified
            alias = _norm(col.table)
            for a, src in sources:
                if a == alias:
                    r = src.resolve_column(name)
                    return r or [ColumnOrigin(table=None, column=name)]
            return [ColumnOrigin(table=None, column=name)]
        # unqualified
        if len(sources) == 1:
            return sources[0][1].resolve_column(name)
        # multi-source disambiguation via schema
        candidate_tables = []
        for _, src in sources:
            if isinstance(src, TableSource):
                candidate_tables.append(src.table_name)
        unique_hits = [t for t in candidate_tables if name in self.schema.columns(t)]
        if len(unique_hits) == 1:
            return [ColumnOrigin(table=unique_hits[0], column=name)]
        # Prefix-based inference (TPC-DS style)
        prefix_map = {
            'ss_': 'store_sales',
            'sr_': 'store_returns',
            'cs_': 'catalog_sales',
            'ws_': 'web_sales',
            'i_': 'item',
            'd_': 'date_dim',
            'c_': 'customer',
            'ca_': 'customer_address',
            's_': 'store',
        }
        for pref, table in sorted(prefix_map.items(), key=lambda x: -len(x[0])):
            if name.startswith(pref) and any(t.endswith(table) or t == table for t in candidate_tables):
                # Accept prefix inference only if table participates in sources
                return [ColumnOrigin(table=table, column=name)]
        # Fallback: attribute to every source that reports the column
        results: List[ColumnOrigin] = []
        for a, src in sources:
            r = src.resolve_column(name)
            if r:
                results.extend(r)
        # If results contain the same column from different tables and column appears in schema uniquely for one, keep only that
        tables = {o.table for o in results if o.table}
        if len(tables) > 1:
            unique_hits = [t for t in tables if name in self.schema.columns(t)]
            if len(unique_hits) == 1:
                results = [o for o in results if o.table == unique_hits[0]]
            else:
                # Deterministic choice: keep origins from the first source that reported the column
                order = []
                for a, src in sources:
                    if isinstance(src, TableSource):
                        order.append(src.table_name)
                first = None
                for o in results:
                    if o.table in order:
                        first = o.table
                        break
                if first:
                    results = [o for o in results if o.table == first]
        return results or [ColumnOrigin(table=None, column=name)]
