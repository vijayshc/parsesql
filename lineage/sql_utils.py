"""
SQL utilities for lineage extraction.

Contains helper functions for working with SQL AST nodes,
table names, column references, and expression handling.
"""

from typing import Dict, List, Optional
from sqlglot import expressions as exp
from .utils import normalize_identifier, table_name_of


class SQLUtils:
    """Utility functions for SQL AST manipulation."""
    
    @staticmethod
    def get_select_parts(query: exp.Expression) -> List[exp.Select]:
        """Recursively get all SELECT parts from a query, handling UNIONs."""
        parts = []
        if isinstance(query, exp.Union):
            parts.extend(SQLUtils.get_select_parts(query.left))
            parts.extend(SQLUtils.get_select_parts(query.right))
        elif isinstance(query, exp.Select):
            parts.append(query)
        return parts

    @staticmethod
    def build_projection_map(select_expr: exp.Select) -> Dict[str, exp.Expression]:
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

    @staticmethod
    def build_alias_map(scope: exp.Select) -> Dict[str, str]:
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

    @staticmethod
    def get_base_tables(scope: exp.Select) -> List[str]:
        """Get all base table names from FROM and JOIN clauses."""
        base_tables = []
        
        # FROM clause
        from_ = scope.args.get("from")
        if from_:
            for t in from_.find_all(exp.Table):
                tn = table_name_of(t)
                if tn:
                    base_tables.append(tn)
        
        # JOIN clauses
        joins = scope.args.get("joins") or []
        for j in joins:
            jt = j.this
            if isinstance(jt, exp.Table):
                tn = table_name_of(jt)
                if tn and tn not in base_tables:
                    base_tables.append(tn)
        
        return base_tables

    @staticmethod
    def is_star_only(select_expr: exp.Select) -> bool:
        """Check if a SELECT has only star projections."""
        return all(isinstance(p, (exp.Star,)) for p in select_expr.expressions)

    @staticmethod
    def is_qualified_star(projection: exp.Expression) -> bool:
        """Check if a projection is a qualified star (e.g., table.*)."""
        return (isinstance(projection, exp.Column) and 
                projection.name == '*' and 
                projection.table is not None)

    @staticmethod
    def get_subquery_by_alias(scope: exp.Select, alias: str) -> Optional[exp.Expression]:
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

    @staticmethod
    def expr_display(expr: exp.Expression, engine: str = "spark") -> str:
        """Get display string for an expression."""
        try:
            return expr.sql(dialect=engine)
        except Exception:
            return str(expr)