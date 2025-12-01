import os
from lineage.extractor import LineageExtractor

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

SCHEMA = {
    "customers": ["customer_id", "first_name", "last_name", "status"],
    "orders": ["customer_id", "total_amount", "order_date", "order_id"],
    "products": ["product_id", "product_name", "price"],
    "order_items": ["order_id", "product_id", "quantity"],
}


def _extract(sql: str, schema=SCHEMA):
    """Extract lineage, joins, and wheres from SQL string."""
    extractor = LineageExtractor(engine='spark', schema=schema)
    tmp_file = os.path.join(BASE_DIR, 'temp', 'temp_join_where.sql')
    os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
    with open(tmp_file, 'w', encoding='utf-8') as f:
        f.write(sql)
    return extractor.extract_from_file(tmp_file)


def test_simple_join():
    """Test basic INNER JOIN extraction - one row per target column."""
    sql = """
    SELECT c.customer_id, o.order_id
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    """
    results = _extract(sql)
    
    # Check lineage
    lineage = results['lineage']
    assert len(lineage) == 2
    
    # Check JOIN conditions - should have one row per output column
    joins = results['joins']
    assert len(joins) == 2  # customer_id and order_id
    
    # Check that all joins reference the correct tables
    for j in joins:
        assert j.left_table == 'customers'
        assert j.right_table == 'orders'
        assert j.join_type == 'INNER'
        assert j.target_column in ['customer_id', 'order_id']


def test_multiple_joins():
    """Test multiple JOINs in one query."""
    sql = """
    SELECT c.customer_id, o.order_id, p.product_name
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN products p ON oi.product_id = p.product_id
    """
    results = _extract(sql)
    
    joins = results['joins']
    # Should have multiple join records
    assert len(joins) >= 3  # At least one for each target column
    
    # Check join types are present
    join_types = {j.join_type for j in joins}
    assert 'INNER' in join_types or 'LEFT' in join_types


def test_simple_where():
    """Test WHERE clause extraction - only from CTEs/subqueries."""
    sql = """
    SELECT customer_id, first_name
    FROM customers
    WHERE status = 'active'
    """
    results = _extract(sql)
    
    wheres = results['wheres']
    # Top-level WHERE should NOT be extracted
    assert len(wheres) == 0


def test_where_in_cte():
    """Test WHERE clause from CTE is extracted."""
    sql = """
    WITH active_customers AS (
        SELECT customer_id, first_name
        FROM customers
        WHERE status = 'active'
    )
    SELECT * FROM active_customers
    """
    results = _extract(sql)
    
    wheres = results['wheres']
    # CTE WHERE should be extracted
    assert len(wheres) >= 1
    assert wheres[0].table_name == 'customers'
    assert wheres[0].column_name == 'status'


def test_where_with_subquery():
    """Test WHERE from inline subquery in FROM clause."""
    sql = """
    SELECT customer_id, first_name
    FROM (SELECT * FROM customers WHERE status = 'active') c
    """
    results = _extract(sql)
    
    wheres = results['wheres']
    # Inline subquery WHERE should now be extracted
    assert len(wheres) >= 1
    assert wheres[0].table_name == 'customers'
    assert wheres[0].column_name == 'status'


def test_join_and_where_combined():
    """Test query with both JOIN and WHERE in CTE."""
    sql = """
    WITH filtered AS (
        SELECT c.customer_id, o.order_id
        FROM customers c
        INNER JOIN orders o ON c.customer_id = o.customer_id
        WHERE c.status = 'active'
    )
    SELECT * FROM filtered
    """
    results = _extract(sql)
    
    # Check JOINs - should be from the CTE
    joins = results['joins']
    assert len(joins) >= 1
    
    # Check WHERE from CTE
    wheres = results['wheres']
    assert len(wheres) >= 1
    assert wheres[0].source_cte == 'filtered'


def test_join_target_column_tracking():
    """Test that JOIN records track individual target columns."""
    sql = """
    SELECT c.customer_id, c.first_name, o.order_id
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    """
    results = _extract(sql)
    
    joins = results['joins']
    assert len(joins) == 3  # One for each target column
    
    # Each join should have a target column
    target_columns = {j.target_column for j in joins}
    assert 'customer_id' in target_columns
    assert 'first_name' in target_columns
    assert 'order_id' in target_columns


def test_where_no_target_columns():
    """Test that WHERE records do NOT have target_columns field."""
    sql = """
    WITH base AS (
        SELECT customer_id FROM customers WHERE status = 'active'
    )
    SELECT * FROM base
    """
    results = _extract(sql)
    
    wheres = results['wheres']
    assert len(wheres) >= 1
    
    # WHERE records should not have target_columns
    where = wheres[0]
    assert not hasattr(where, 'target_columns') or where.target_columns is None


def test_alias_resolution_in_joins():
    """Test that aliases are resolved to actual table names."""
    sql = """
    SELECT c.customer_id, o.order_id
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    """
    results = _extract(sql)
    
    joins = results['joins']
    # All joins should show actual table names, not aliases
    for j in joins:
        assert j.left_table == 'customers'
        assert j.right_table == 'orders'


def test_no_left_right_column_fields():
    """Test that JOIN records don't have left_column/right_column."""
    sql = """
    SELECT c.customer_id, o.order_id
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    """
    results = _extract(sql)
    
    joins = results['joins']
    assert len(joins) >= 1
    
    # Should not have left_column or right_column
    join = joins[0]
    assert not hasattr(join, 'left_column')
    assert not hasattr(join, 'right_column')
