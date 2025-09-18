from __future__ import annotations

from typing import Dict, Iterable, Optional, Sequence

from sqlglot import expressions as exp


def normalize_identifier(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    return str(name).strip().strip("`").strip('"').lower()


def table_name_of(node: exp.Expression) -> Optional[str]:
    if isinstance(node, exp.Table):
        def get_name(part):
            if isinstance(part, str):
                return part
            elif hasattr(part, 'name'):
                return part.name
            else:
                return str(part) if part else None
        
        if node.args.get("catalog"):
            parts = [get_name(node.catalog), get_name(node.db), get_name(node.this)]
            return ".".join(p for p in parts if p).lower()
        if node.args.get("db"):
            parts = [get_name(node.db), get_name(node.this)]
            return ".".join(p for p in parts if p).lower()
        this = node.this
        if isinstance(this, str):
            return this.lower()
        elif hasattr(this, 'name'):
            return this.name.lower()
        else:
            return str(this).lower() if this else None
    return None


def collect_cte_aliases(select: exp.Expression) -> Dict[str, exp.Expression]:
    ctes: Dict[str, exp.Expression] = {}
    for cte in select.find_all(exp.CTE):
        alias_expr = getattr(cte, "alias", None)
        alias = alias_to_str(alias_expr)
        alias = normalize_identifier(alias)
        if alias:
            ctes[alias] = cte.this
    return ctes


def iter_tables(expr: exp.Expression) -> Iterable[exp.Table]:
    for t in expr.find_all(exp.Table):
        yield t


def is_insert(stmt: exp.Expression) -> bool:
    return isinstance(stmt, exp.Insert)


def is_select(stmt: exp.Expression) -> bool:
    return isinstance(stmt, (exp.Select, exp.Union, exp.With))


def is_with(expr: exp.Expression) -> bool:
    return isinstance(expr, exp.With)


def infer_target_table(stmt: exp.Expression) -> Optional[str]:
    if isinstance(stmt, exp.Insert):
        t = stmt.this
        if isinstance(t, exp.Table):
            return table_name_of(t)
    return None


def alias_or_table_for_column(scope: exp.Select, column: exp.Column) -> Optional[str]:
    # Prefer explicit table/alias on column
    if column.table:
        return normalize_identifier(column.table)
    # Walk FROM tables and JOINs for potential single-source inference
    sources = []
    from_ = scope.args.get("from")
    if from_:
        for src in from_.find_all(exp.Table):
            if src.alias:
                sources.append(normalize_identifier(alias_to_str(src.alias)))
            else:
                tname = table_name_of(src)
                if tname:
                    sources.append(tname.split(".")[-1])
    # If just one source, attribute to it
    if len(sources) == 1:
        return sources[0]
    return None


def alias_to_str(alias_expr) -> Optional[str]:
    if not alias_expr:
        return None
    # TableAlias or Identifier
    try:
        if isinstance(alias_expr, exp.TableAlias):
            ident = alias_expr.this
            if isinstance(ident, exp.Identifier):
                return ident.name or ident.this
            return str(ident)
        if isinstance(alias_expr, exp.Identifier):
            return alias_expr.name or alias_expr.this
        # Some nodes may expose .name
        name = getattr(alias_expr, "name", None)
        if name:
            return name
        this = getattr(alias_expr, "this", None)
        if isinstance(this, str):
            return this
        if hasattr(alias_expr, "sql"):
            return alias_expr.sql(dialect="")
        return str(alias_expr)
    except Exception:
        return str(alias_expr)


TPCDS_PREFIX_TABLES = {
    "ss_": "store_sales",
    "sr_": "store_returns",
    "cs_": "catalog_sales",
    "ws_": "web_sales",
    "i_": "item",
    "d_": "date_dim",
    "c_": "customer",
    "ca_": "customer_address",
    "s_": "store",
}

GENERIC_PREFIX_TABLES = {
    "o_": "orders",
    "oi_": "order_items",
    "p_": "products",
}


def guess_table_from_prefix(column_name: Optional[str], candidate_tables: Sequence[str]) -> Optional[str]:
    if not column_name:
        return None
    col = normalize_identifier(column_name) or ""
    pref_map = {**TPCDS_PREFIX_TABLES, **GENERIC_PREFIX_TABLES}
    # Prefer longer prefixes first (e.g., 'oi_' before 'o_')
    for pref, table in sorted(pref_map.items(), key=lambda x: -len(x[0])):
        if col.startswith(pref) and any(ct.endswith(table) or ct == table for ct in candidate_tables):
            return table
    return None
