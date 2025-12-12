from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from sqlglot import expressions as exp

from .origin import ColumnOrigin, ExpressionLineage, TraceStep
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
            # Handle UNION semantics: column names from first SELECT, data from all branches by position
            parts = self._select_parts(self.select)
            if not parts:
                return []
            
            # Analyze each branch
            branch_lineages = []
            for part in parts:
                sa = SelectAnalyzer(part, self.env, self.schema, self.dialect)
                branch_lineages.append(sa.analyze())
            
            if not branch_lineages:
                return []
            
            # Merge by position: column names from first branch, origins from all branches
            first_branch = branch_lineages[0]
            merged_lineages = []
            
            for i, first_el in enumerate(first_branch):
                # Start with the first branch's lineage
                merged_el = ExpressionLineage(
                    expression_sql=first_el.expression_sql,
                    output_column=first_el.output_column,
                    origins=list(first_el.origins)
                )
                
                # Add origins from corresponding positions in other branches
                for branch in branch_lineages[1:]:
                    if i < len(branch):
                        branch_el = branch[i]
                        merged_el.origins.extend(branch_el.origins)
                
                merged_lineages.append(merged_el)
            
            return merged_lineages
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
            
            # Update expression chains to include current expression
            current_expr_sql = expr_sql(expr, self.dialect)
            # Capture alias if present
            local_alias = _norm(str(proj.alias)) if isinstance(proj, exp.Alias) else None
            
            enhanced_origins = []
            for origin in origins:
                # Build expression chain (source -> target flow)
                new_step = TraceStep(expression=current_expr_sql, alias=local_alias)
                # Avoid duplicating the last step if it's identical (e.g. simple pass-through)
                # But here we are projecting, so it's a new layer.
                # However, if current_expr_sql is same as last step expression and alias is None, maybe skip?
                # User complaint: "expressions traces are delimited by ~. Instead, want to capture each of the exceptions"
                # If we have `SELECT col AS col`, we might have step `col` then step `col` (alias col).
                # Only if strictly identical?
                # For now, just append. But existing logic had:
                # if origin.expression_chain and current_expr_sql != origin.expression_chain: ...
                
                # Check if this step is redundant (same expression as last step, no new alias)
                if origin.trace:
                    last = origin.trace[-1]
                    if last.expression == current_expr_sql and not local_alias:
                        new_trace = origin.trace
                    else:
                        new_trace = origin.trace + (new_step,)
                else:
                    new_trace = (new_step,)
                
                enhanced_origins.append(ColumnOrigin(
                    table=origin.table,
                    column=origin.column,
                    trace=new_trace,
                    path=origin.path
                ))
            
            lineages.append(ExpressionLineage(expression_sql=current_expr_sql, output_column=out_col, origins=tuple(enhanced_origins)))
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
        seen_columns = set()  # Track columns we've already seen to avoid duplicates
        # For * we enumerate every column from each source in order
        for alias, src in sources:
            for col in src.output_columns():
                col_name = _norm(col)
                # Handle placeholder '*' from sources with unknown schema
                if col_name == '*':
                    # Try to resolve '*' from the source directly first to preserve lineage
                    resolved_star = src.resolve_column('*')
                    if resolved_star:
                         origins.append(ExpressionLineage(
                            expression_sql='*',
                            output_column=None,
                            origins=tuple(resolved_star)
                         ))
                         continue

                    # Create a demand-responsive lineage entry for unknown schema tables
                    # This allows the SelectSource to handle column requests dynamically
                    table_name = getattr(src, 'table_name', None)
                    if not table_name:
                         table_name = getattr(src, 'name', None) or alias
                    
                    origins.append(ExpressionLineage(
                        expression_sql='*', 
                        output_column=None,  # No specific output column - demand-driven
                        origins=(ColumnOrigin(table=table_name, column='*', trace=(TraceStep('*'),)),)
                    ))
                    continue
                # Skip columns we've already seen (first occurrence wins)
                if col_name in seen_columns:
                    continue
                seen_columns.add(col_name)
                col_origins = src.resolve_column(col_name)
                # Create one lineage entry with all origins for this column
                if col_origins:
                    origins.append(ExpressionLineage(expression_sql='*', output_column=col_name, origins=tuple(col_origins)))
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
                # Create one lineage entry with all origins for this column
                if col_origins:
                    out.append(ExpressionLineage(expression_sql=f"{alias}.*", output_column=col_name, origins=tuple(col_origins)))
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
                    cleaned.extend(concrete)
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
            
            # Fallback: Reference to undefined alias (possible typo or copy-paste error)
            # Try to resolve against available sources ignoring the invalid alias
            loose_matches = []
            seen_match = set()
            for a, src in sources:
                r = src.resolve_column(name)
                # Only consider if we found actual lineage
                concrete = [o for o in r if o.table or o.column]
                for o in concrete:
                    k = o.as_key()
                    if k not in seen_match:
                        seen_match.add(k)
                        loose_matches.append(o)
            
            if loose_matches:
                return loose_matches

            return [ColumnOrigin(table=None, column=name)]
        # unqualified
        # unqualified
        if len(sources) == 1:
            res = sources[0][1].resolve_column(name)
            if not res:
                # Fallback for single source: even if schema validation fails (column not in schema),
                # force attribution to this source. This handles incomplete schemas or implied columns.
                src = sources[0][1]
                table_name = getattr(src, 'table_name', None)
                if table_name:
                    return [ColumnOrigin(table=table_name, column=name, trace=(TraceStep(name),), path=(table_name,))]
            return res
        # multi-source disambiguation via schema
        candidate_tables = []
        for _, src in sources:
            if isinstance(src, TableSource):
                candidate_tables.append(src.table_name)
        unique_hits = [t for t in candidate_tables if name in self.schema.columns(t)]
        if len(unique_hits) == 1:
            return [ColumnOrigin(table=unique_hits[0], column=name, trace=(TraceStep(name),))]
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
                return [ColumnOrigin(table=table, column=name, trace=(TraceStep(name),))]
        # Enhanced scope-aware resolution with strict unknown table handling
        # Priority order:
        # 1. Explicit column definitions (subqueries that specifically select the column)
        # 2. FROM clause (main source) 
        # 3. JOIN sources with schema-defined columns  
        # 4. JOIN sources without schema (only if no better options exist)
        
        explicit_results: List[ColumnOrigin] = []
        from_source_results: List[ColumnOrigin] = []
        schema_join_results: List[ColumnOrigin] = []
        unknown_join_results: List[ColumnOrigin] = []
        
        for i, (alias, src) in enumerate(sources):
            r = src.resolve_column(name)
            if r:
                # Check if this source explicitly defines the column
                if self._source_explicitly_defines_column(src, name):
                    explicit_results.extend(r)
                elif i == 0:  # First source is the FROM clause
                    from_source_results.extend(r)
                else:  # JOIN sources
                    # Check if this is a schema-validated source
                    if isinstance(src, TableSource) and src.schema.columns(src.table_name):
                        schema_join_results.extend(r)
                    else:
                        # Unknown source - be very conservative
                        # Only include if the result has concrete table/column information
                        concrete_results = [o for o in r if o.table and o.column and o.column != '*']
                        if concrete_results:
                            unknown_join_results.extend(concrete_results)
        
        # Apply priority resolution with stricter unknown table handling:
        if explicit_results:
            concrete = [o for o in explicit_results if o.table or o.column]
            return concrete if concrete else explicit_results
        elif from_source_results:
            # Apply process of elimination logic to FROM sources with JOINs
            if len(sources) > 1:
                # We have FROM + JOINs - apply elimination logic
                all_results = []
                sources_definitely_not_having_column = []
                
                for i, (alias, src) in enumerate(sources):
                    result = src.resolve_column(name)
                    if result and any(o.table for o in result):
                        all_results.append((alias, src, result))
                    
                    # Check if we can definitively say this source does NOT have the column
                    if isinstance(src, SelectSource):
                        output_cols = src.output_columns()
                        normalized_name = _norm(name)
                        if (output_cols and 
                            '*' not in output_cols and
                            not any(_norm(col) == normalized_name for col in output_cols) and
                            not any(col.startswith('col_') for col in output_cols)):
                            sources_definitely_not_having_column.append((alias, src))
                
                # Filter out sources that definitely don't have the column
                remaining_sources = []
                for alias, src, result in all_results:
                    if (alias, src) not in sources_definitely_not_having_column:
                        remaining_sources.append((alias, src, result))
                
                # If elimination leaves exactly one source, use it
                if len(remaining_sources) == 1:
                    _, _, result = remaining_sources[0]
                    return result
                elif len(remaining_sources) == 0:
                    return [ColumnOrigin(table=None, column=name, trace=(TraceStep(name),))]
            
            # Apply conservative logic to FROM sources when they're unknown tables
            first_alias, first_src = sources[0] if sources else (None, None)
            if (first_src and isinstance(first_src, TableSource) and 
                not first_src.schema.columns(first_src.table_name)):
                # FROM source is unknown table - be conservative if no elimination helped
                return [ColumnOrigin(table=None, column=name, trace=(TraceStep(name),))]
            
            # FROM source has schema or is SelectSource - trust it
            concrete = [o for o in from_source_results if o.table or o.column]
            return concrete if concrete else from_source_results
        elif schema_join_results:
            concrete = [o for o in schema_join_results if o.table or o.column]
            return concrete if concrete else schema_join_results
        elif unknown_join_results:
            # Enhanced logic: Process of elimination for unknown sources
            # When we have unknown sources claiming to have a column,
            # check if we can eliminate other sources that definitely DON'T have it
            
            # Collect all sources and check which ones definitely DON'T have the column
            all_sources = []
            sources_claiming_column = []
            sources_definitely_not_having_column = []
            
            for i, (alias, src) in enumerate(sources):
                all_sources.append((alias, src))
                
                # Check if this source claims to have the column
                result = src.resolve_column(name)
                if result and any(o.table for o in result):
                    sources_claiming_column.append((alias, src, result))
                
                # Check if we can definitively say this source does NOT have the column
                if isinstance(src, SelectSource):
                    output_cols = src.output_columns()
                    normalized_name = _norm(name)
                    if (output_cols and 
                        '*' not in output_cols and
                        not any(_norm(col) == normalized_name for col in output_cols) and
                        not any(col.startswith('col_') for col in output_cols)):  # Not placeholder columns
                        sources_definitely_not_having_column.append((alias, src))
            
            # Apply process of elimination
            if len(sources_claiming_column) >= 1:
                # One or more sources claim to have the column
                # Filter out sources that we know definitely don't have it
                remaining_sources = []
                for alias, src, result in sources_claiming_column:
                    if (alias, src) not in sources_definitely_not_having_column:
                        remaining_sources.append((alias, src, result))
                
                # If elimination leaves us with exactly one source, use it
                if len(remaining_sources) == 1:
                    _, _, result = remaining_sources[0]
                    return result
                else:
                    # Multiple sources remain or none remain - be conservative
                    # For JOINs with multiple unknown tables, we can't definitively determine the source
                    return [ColumnOrigin(table=None, column=name, trace=(TraceStep(name),))]
            
            # If no sources claim to have the column, fall back to conservative approach
            return [ColumnOrigin(table=None, column=name, trace=(TraceStep(name),))]
        else:
            return [ColumnOrigin(table=None, column=name, trace=(TraceStep(name),))]
    
    def _source_explicitly_defines_column(self, src: SourceBase, column_name: str) -> bool:
        """Check if a source explicitly defines a column (vs having it implicitly available)."""
        if isinstance(src, SelectSource):
            # For SelectSource, check if the column is in the explicit output
            output_cols = src.output_columns()
            normalized_name = _norm(column_name)
            
            # If the column is explicitly in output, it's explicit
            if any(_norm(col) == normalized_name for col in output_cols):
                return True
                
            # Check if this SelectSource was created from a query that explicitly selects this column
            # This requires analyzing the SELECT expressions  
            analyzer = SelectAnalyzer(src.select, src.env, src.schema)
            lineages = analyzer.analyze()
            
            for el in lineages:
                if el.output_column and _norm(el.output_column) == normalized_name:
                    # This column was explicitly selected (not from *)
                    if el.expression_sql != '*':
                        return True
            
            # Special case: If this is a * expansion without schema support,
            # do NOT consider it as explicitly defining any specific column
            return False
            
        elif isinstance(src, TableSource):
            # For TableSource, only consider columns explicit if they're in the schema
            # Tables without schema information cannot explicitly define specific columns
            table_columns = src.schema.columns(src.table_name)
            if table_columns:
                normalized_name = _norm(column_name)
                return normalized_name in [_norm(col) for col in table_columns]
            else:
                # No schema means we can't explicitly define any specific columns
                return False
        
        return False
    
    def extract_join_conditions(self, query_level: int = 0, source_cte: Optional[str] = None) -> List[Tuple]:
        """
        Extract JOIN conditions from the SELECT statement.
        Returns list of (target_column, left_table, right_table, join_type, expression)
        One record per target column that depends on joined tables.
        """
        from ..models import JoinConditionRecord
        
        if not isinstance(self.select, exp.Select):
            # Handle UNION by recursively extracting from each branch
            if isinstance(self.select, exp.Union):
                results = []
                parts = self._select_parts(self.select)
                for part in parts:
                    analyzer = SelectAnalyzer(part, self.env, self.schema, self.dialect)
                    results.extend(analyzer.extract_join_conditions(query_level, source_cte))
                return results
            return []
        
        sources = self._build_sources(self.select)
        joins_list = self.select.args.get("joins") or []
        
        if not joins_list:
            return []
        
        # Get lineage to understand which target columns exist
        lineages = self.analyze()
        
        # Find all unique tables referenced in the output
        all_tables_in_output = set()
        for el in lineages:
            for origin in el.origins:
                if origin.table:
                    all_tables_in_output.add(origin.table)
        
        # If no tables or no JOINs, no meaningful JOINs to report
        # Note: We keep this even for single table (self-joins)
        if len(all_tables_in_output) == 0:
            return []
        
        join_records = []
        
        # Process each JOIN
        for join_node in joins_list:
            # Determine join type
            join_type = "INNER"
            if isinstance(join_node, exp.Join):
                if join_node.args.get("side"):
                    join_type = str(join_node.args["side"]).upper()
                if join_node.args.get("kind"):
                    kind = str(join_node.args["kind"]).upper()
                    if kind in ("LEFT", "RIGHT", "FULL", "CROSS"):
                        join_type = kind
            
            # Get the ON condition
            on_condition = join_node.args.get("on")
            if not on_condition:
                continue
            
            # Get the full condition expression as SQL
            condition_sql = expr_sql(on_condition, self.dialect)
            
            # Find tables involved in this JOIN condition
            columns_in_condition = list(on_condition.find_all(exp.Column))
            join_tables = set()
            
            for col in columns_in_condition:
                if col.table:
                    alias = _norm(col.table)
                    for src_alias, src in sources:
                        if src_alias == alias:
                            if isinstance(src, TableSource):
                                join_tables.add(src.table_name)
                            elif isinstance(src, SelectSource):
                                # Resolve to CTE name or base table
                                cte_name = None
                                for env_cte_name, env_cte_src in self.env.ctes.items():
                                    if env_cte_src is src:
                                        cte_name = env_cte_name
                                        break
                                
                                if cte_name:
                                    join_tables.add(cte_name)
                                else:
                                    # Try to resolve to base table
                                    try:
                                        sub_analyzer = SelectAnalyzer(src.select, src.env, self.schema, self.dialect)
                                        sub_sources = sub_analyzer._build_sources(src.select if isinstance(src.select, exp.Select) else src.select)
                                        base_tables = []
                                        for _, sub_src in sub_sources:
                                            if isinstance(sub_src, TableSource):
                                                base_tables.append(sub_src.table_name)
                                        if len(base_tables) == 1:
                                            join_tables.add(base_tables[0])
                                        else:
                                            join_tables.add(alias)
                                    except:
                                        join_tables.add(alias)
                            break
            
            # For each output column that comes from any table in this JOIN
            # NOTE: For aggregated columns (COUNT, SUM, etc.) the lineage may show None.None
            # but they still depend on the tables in the FROM/JOIN clauses
            for el in lineages:
                target_col = el.output_column
                if not target_col:
                    continue
                
                # Check if this column uses any table involved in this JOIN
                column_tables = set()
                for origin in el.origins:
                    if origin.table:
                        column_tables.add(origin.table)
                    if origin.path:
                        column_tables.update(origin.path)
                
               # For aggregated columns with no table reference (None.None),
                # they still depend on ALL tables in the FROM/JOIN
                has_table_ref = len(column_tables) > 0
                
                # Create JOIN record if:
                # 1. Column references tables in this JOIN, OR
                # 2. Column has no table reference (aggregated) and this is a JOIN query
                should_record = False
                if has_table_ref:
                    # Regular column - check if it uses tables from this JOIN
                    if join_tables.intersection(column_tables):
                        should_record = True
                else:
                    # Aggregated column (None.None) - it depends on all joined tables
                    should_record = True
                
                if should_record:
# print(f"DEBUG: Found join in {source_cte or 'top'}: {join_tables} for target {target_col}")
                    join_table_list = sorted(list(join_tables))
                    if len(join_table_list) >= 2:
                        # Normal join with 2 different tables
                        join_records.append((
                            target_col,
                            join_table_list[0],
                            join_table_list[1],
                            join_type,
                            condition_sql
                        ))
                    elif len(join_table_list) == 1 and len(joins_list) > 0:
                        # Self-join: same table on both sides
                        table = join_table_list[0]
                        join_records.append((
                            target_col,
                            table,
                            table,
                            join_type,
                            condition_sql
                        ))
        
        return join_records
    
    def analyze_where(self) -> List[ExpressionLineage]:
        """
        Analyze WHERE clause expressions to produce lineage for columns used in filtering.
        Returns list of ExpressionLineage objects, treating WHERE clause columns as targets.
        """
        if isinstance(self.select, exp.Union):
            # Handle UNION by recursively extracting from each branch
            results = []
            parts = self._select_parts(self.select)
            for part in parts:
                analyzer = SelectAnalyzer(part, self.env, self.schema, self.dialect)
                results.extend(analyzer.analyze_where())
            return results
            
        if not isinstance(self.select, exp.Select):
            return []
        
        where_clause = self.select.args.get("where")
        if not where_clause:
            return []
            
        sources = self._build_sources(self.select)
        lineages: List[ExpressionLineage] = []
        
        # Extract all columns from the WHERE clause
        columns_in_where = list(where_clause.find_all(exp.Column))
        
        # Deduplicate columns by name to avoid redundant analysis
        seen_columns = set()
        unique_columns = []
        for col in columns_in_where:
            col_name = _norm(col.name)
            # Use full column representation for uniqueness (table.col)
            full_name = f"{col.table}.{col.name}" if col.table else col_name
            if full_name not in seen_columns:
                seen_columns.add(full_name)
                unique_columns.append(col)
        
        for col in unique_columns:
            col_name = _norm(col.name)
            origins = self._origins_for_expression(col, sources)
            
            # If column expression with no resolved origins, attach placeholder
            if not any(o.table or o.column for o in origins):
                origins = [ColumnOrigin(table=None, column=None)]
            
            # Find the condition expression in the WHERE clause
            condition_expr = col
            while condition_expr.parent and not isinstance(condition_expr.parent, (exp.Where, exp.And, exp.Or)):
                 condition_expr = condition_expr.parent
            condition_sql = expr_sql(condition_expr, self.dialect)

            # Update expression chains
            current_expr_sql = expr_sql(col, self.dialect)
            enhanced_origins = []
            for origin in origins:
                new_step = TraceStep(expression=current_expr_sql)
                if origin.trace:
                     last = origin.trace[-1]
                     if last.expression == current_expr_sql:
                         new_trace = origin.trace
                     else:
                         new_trace = origin.trace + (new_step,)
                else:
                    new_trace = (new_step,)
                
                # Append condition_sql if different from current chain end
                if condition_sql and condition_sql != current_expr_sql:
                     # Add condition as a separate trace step
                     cond_step = TraceStep(expression=condition_sql)
                     # Check redundancy
                     if new_trace and new_trace[-1].expression == condition_sql:
                         pass
                     else:
                         new_trace = new_trace + (cond_step,)
                
                enhanced_origins.append(ColumnOrigin(
                    table=origin.table,
                    column=origin.column,
                    trace=new_trace,
                    path=origin.path
                ))
            
            # Output column is the column name itself
            lineages.append(ExpressionLineage(
                expression_sql=condition_sql or current_expr_sql, 
                output_column=col_name, 
                origins=tuple(enhanced_origins)
            ))
            
        # Also check for subqueries in WHERE (nested)
        subqueries_in_where = list(where_clause.find_all(exp.Select))
        for subq in subqueries_in_where:
            # Recursively extract WHERE conditions from subqueries
            sub_analyzer = SelectAnalyzer(subq, self.env, self.schema, self.dialect)
            lineages.extend(sub_analyzer.analyze_where())
            
        return lineages
