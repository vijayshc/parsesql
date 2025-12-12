from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from sqlglot import expressions as exp

from .origin import ColumnOrigin, TraceStep
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
        # Check if table is in schema
        table_columns = self.schema.columns(self.table_name)
        normalized_name = _norm(name)
        
        if table_columns:
            # Table is in schema - only return column if it exists in schema
            if normalized_name in [_norm(col) for col in table_columns]:
                return [ColumnOrigin(table=self.table_name, column=normalized_name, trace=(TraceStep(normalized_name),), path=(self.table_name,))]
            else:
                # Table exists in schema but column doesn't - return empty
                return []
        else:
            # Table not in schema - conservatively assume it might have the column
            # This preserves backward compatibility but allows for better disambiguation
            return [ColumnOrigin(table=self.table_name, column=normalized_name, trace=(TraceStep(normalized_name),), path=(self.table_name,))]


@dataclass
class SelectSource(SourceBase):
    """Represents a SELECT (CTE or subquery) as a source; performs lazy lineage analysis of its projections."""
    select: exp.Expression  # Select or Union
    env: 'AnalysisEnvironment'  # environment for nested CTE resolution
    schema: Schema
    name: Optional[str] = None
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
                # Accumulate all lineage for the same column (important for UNION cases)
                if out_name not in idx:
                    idx[out_name] = list(el.origins)
                else:
                    # Add new origins to existing ones (for UNION, multiple CTEs, etc.)
                    idx[out_name].extend(el.origins)
            # If expression is star expansion with origins but no output_column (due to star) add origin columns
            elif not out_name and el.expression_sql in ('*',) and el.origins:
                for o in el.origins:
                    if o.column and o.column != '*':
                        if o.column not in outputs:  # First occurrence wins for star expansion
                            outputs.append(o.column)
                            idx[o.column] = [o]
            # For unnamed direct column expressions (e.g., table.col) add the column name
            elif not out_name and el.expression_sql and '.' in el.expression_sql and len(el.origins) == 1:
                origin = el.origins[0]
                if origin.column and origin.column not in outputs:
                    outputs.append(origin.column)
                    idx[origin.column] = [origin]
        
        # If no outputs were generated, create a synthetic column for star expansion fallback
        # If no outputs were generated, check if we have a star expansion
        if not outputs and expr_lineages:
            # Check for unresolved star expansion
            star_lineage = next((el for el in expr_lineages if el.expression_sql == '*' and not el.output_column), None)
            if star_lineage:
                outputs = ['*']
                idx['*'] = list(star_lineage.origins)
            else:
                # Fallback to synthetic column
                outputs = ['col_0']
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
        
        # First try: check if column exists in the SelectSource output
        result = list(self._lineage_index.get(n, []))
        
        if not result:
            # Demand-driven resolution for star expansion from unknown schema tables
            # When a SelectSource contains a * from an unknown table and a specific column is requested,
            # we can infer that the column comes from the star expansion if it's the only source
            
            # Re-analyze to check for demand-responsive star expansions
            from .analyzer import SelectAnalyzer
            analyzer = SelectAnalyzer(self.select, self.env, self.schema)
            expr_lineages = analyzer.analyze()
            
            # Look for star expansion lineages that can handle this column request
            for el in expr_lineages:
                if (el.expression_sql == '*' and 
                    not el.output_column and  # Demand-driven (no specific output)
                    el.origins and 
                    len(el.origins) == 1 and 
                    el.origins[0].column == '*'):
                    
                    # This is a demand-responsive star expansion
                    # We can infer the requested column comes from this source
                    table_name = el.origins[0].table
                    if table_name:
                        result = [ColumnOrigin(table=table_name, column=name, trace=(TraceStep(name),), path=(table_name,))]
                        break
        
        if result and self.name:
            # Prepend current CTE name to path
            new_result = []
            for o in result:
                new_path = (self.name,) + o.path
                new_result.append(ColumnOrigin(
                    table=o.table,
                    column=o.column,
                    trace=o.trace,
                    path=new_path
                ))
            return new_result
            
        return result
    
    def _has_star_expansion(self) -> bool:
        """Check if this SelectSource has a * expansion that couldn't be resolved."""
        # Re-analyze to get the raw lineages
        from .analyzer import SelectAnalyzer
        analyzer = SelectAnalyzer(self.select, self.env, self.schema)
        expr_lineages = analyzer.analyze()
        
        # Look for * expressions that didn't generate output columns
        for el in expr_lineages:
            if el.expression_sql == '*' and not el.output_column:
                return True
        return False
    
    def _get_star_sources(self) -> List[str]:
        """Get table names from * expansion origins."""
        from .analyzer import SelectAnalyzer
        analyzer = SelectAnalyzer(self.select, self.env, self.schema)
        expr_lineages = analyzer.analyze()
        
        tables = []
        for el in expr_lineages:
            if el.expression_sql == '*':
                for origin in el.origins:
                    if origin.table and origin.table not in tables:
                        tables.append(origin.table)
        return tables


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
