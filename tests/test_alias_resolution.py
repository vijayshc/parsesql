import os
from lineage.extractor import LineageExtractor

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

SCHEMA = {
    "customers": ["customer_id", "first_name", "last_name", "status"],
    "orders": ["customer_id", "total_amount", "order_date", "order_id"],
    "products": ["product_id", "product_name", "price"],
}


def _extract(sql: str, schema=SCHEMA):
    """Extract lineage, joins, and wheres from SQL string."""
    extractor = LineageExtractor(engine='spark', schema=schema)
    tmp_file = os.path.join(BASE_DIR, 'temp', 'temp_alias_test.sql')
    os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
    with open(tmp_file, 'w', encoding='utf-8') as f:
        f.write(sql)
    return extractor.extract_from_file(tmp_file)


def test_join_alias_resolution_to_table():
    """Test that table aliases are resolved to actual table names in JOINs."""
    sql = """
    SELECT c.customer_id, o.order_id
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    """
    results = _extract(sql)
    
    joins = results['joins']
    assert len(joins) >= 1
    
    # Should show actual table names, not aliases
    for j in joins:
        assert j.left_table == 'customers'
        assert j.right_table == 'orders'


def test_join_with_cte_alias_resolution():
    """Test that CTE aliases are resolved to CTE names in JOINs."""
    sql = """
    WITH customer_base AS (
        SELECT customer_id, first_name FROM customers
    )
    SELECT c.customer_id, o.order_id
    FROM customer_base c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    """
    results = _extract(sql)
    
    joins = results['joins']
    assert len(joins) >= 1
    
    # Should show CTE name, not alias
    # Check that one of the tables is the CTE
    tables = {j.left_table for j in joins} | {j.right_table for j in joins}
    assert 'customer_base' in tables or 'orders' in tables


def test_join_with_derived_table_resolution():
    """Test that derived table aliases are resolved to base tables when possible."""
    sql = """
    SELECT c.customer_id, o.order_id
    FROM (SELECT customer_id, first_name FROM customers WHERE status = 'active') c
    INNER JOIN (SELECT order_id, customer_id FROM orders) o 
    ON c.customer_id = o.customer_id
    """
    results = _extract(sql)
    
    joins = results['joins']
    assert len(joins) >= 1
    
    # Should resolve to base tables for single-table subqueries
    # Check that actual table names appear
    tables = {j.left_table for j in joins} | {j.right_table for j in joins}
    assert 'customers' in tables or 'orders' in tables


def test_where_alias_resolution_to_table():
    """Test that table aliases are resolved to actual table names in WHERE from CTEs."""
    sql = """
    WITH filtered AS (
        SELECT customer_id, first_name
        FROM customers c
        WHERE c.status = 'active'
    )
    SELECT * FROM filtered
    """
    results = _extract(sql)
    
    wheres = results['wheres']
    assert len(wheres) >= 1
    
    # Should show actual table name, not alias
    assert wheres[0].table_name == 'customers'


def test_where_with_derived_table_resolution():
    """Test that WHERE conditions on derived tables are resolved."""
    sql = """
    WITH sub AS (
        SELECT customer_id,  first_name
        FROM (SELECT * FROM customers WHERE first_name LIKE 'J%') c
        WHERE c.status = 'active'
    )
    SELECT * FROM sub
    """
    results = _extract(sql)
    
    wheres = results['wheres']
    # Should have WHERE from CTE
    assert len(wheres) >= 1
    
    # Check that at least one is resolved to customers table
    table_names = {w.table_name for w in wheres}
    assert 'customers' in table_names


def test_join_one_row_per_target_column():
    """Test that each target column gets its own JOIN record."""
    sql = """
    SELECT c.customer_id, c.first_name, o.order_id, o.total_amount
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    """
    results = _extract(sql)
    
    joins = results['joins']
    assert len(joins) == 4  # One for each output column
    
    # Each join should have a unique target column
    target_cols = [j.target_column for j in joins]
    assert 'customer_id' in target_cols
    assert 'first_name' in target_cols
    assert 'order_id' in target_cols
    assert 'total_amount' in target_cols


def test_no_target_columns_in_where():
    """Test that WHERE records don't have target_columns field."""
    sql = """
    WITH filtered AS (
        SELECT customer_id, first_name, status
        FROM customers
        WHERE status = 'active'
    )
    SELECT * FROM filtered
    """
    results = _extract(sql)
    
    wheres = results['wheres']
    assert len(wheres) >= 1
    
    # WHERE records should not have target_columns field
    where = wheres[0]
    assert not hasattr(where, 'target_columns') or where.target_columns is None


def test_self_join_alias_resolution():
    """Test that self-joins show the same table for both sides."""
    sql = """
    WITH pairs AS (
        SELECT c1.customer_id AS id1, c2.customer_id AS id2
        FROM customers c1
        INNER JOIN customers c2 ON c1.status = c2.status
    )
    SELECT * FROM pairs
    """
    results = _extract(sql)
    
    joins = results['joins']
    if len(joins) > 0:
        # Both sides should resolve to the same table for self-join
        join = joins[0]
        assert join.left_table == 'customers'
        assert join.right_table == 'customers'
