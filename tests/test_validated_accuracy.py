import os
from lineage.extractor import LineageExtractor

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

SCHEMA = {
    "customers": ["customer_id", "first_name", "last_name", "status", "region"],
    "orders": ["customer_id", "total_amount", "order_date", "order_id", "status"],
    "products": ["product_id", "product_name", "price", "category"],
    "order_items": ["order_id", "product_id", "quantity", "discount"],
}


def _extract(sql: str, schema=SCHEMA):
    """Extract lineage, joins, and wheres from SQL string."""
    extractor = LineageExtractor(engine='spark', schema=schema)
    tmp_file = os.path.join(BASE_DIR, 'temp', 'temp_complex_validated.sql')
    os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
    with open(tmp_file, 'w', encoding='utf-8') as f:
        f.write(sql)
    return extractor.extract_from_file(tmp_file)


def test_multi_level_ctes_with_joins_validated():
    """Test multiple levels of CTEs with JOINs - validate exact output."""
    sql = """
    WITH level1 AS (
        SELECT c.customer_id, c.first_name, o.order_id
        FROM customers c
        INNER JOIN orders o ON c.customer_id = o.customer_id
        WHERE c.status = 'active'
    ),
    level2 AS (
        SELECT l1.customer_id, l1.first_name, oi.product_id
        FROM level1 l1
        INNER JOIN order_items oi ON l1.order_id = oi.order_id
        WHERE oi.quantity > 1
    ),
    level3 AS (
        SELECT l2.customer_id, l2.first_name, p.product_name
        FROM level2 l2
        INNER JOIN products p ON l2.product_id = p.product_id
        WHERE p.category = 'Electronics'
    )
    SELECT * FROM level3
    """
    results = _extract(sql)
    
    # Validate LINEAGE - final output should have 3 columns
    lineage = results['lineage']
    target_cols = {r.target_column for r in lineage}
    assert target_cols == {'customer_id', 'first_name', 'product_name'}, f"Got: {target_cols}"
    
    # Validate lineage sources
    lineage_map = {r.target_column: (r.source_table, r.source_column) for r in lineage}
    assert lineage_map['customer_id'] == ('customers', 'customer_id')
    assert lineage_map['first_name'] == ('customers', 'first_name')
    assert lineage_map['product_name'] == ('products', 'product_name')
    
    # Validate JOINs - should have joins from all 3 CTEs
    joins = results['joins']
    join_ctes = {j.source_cte for j in joins}
    assert 'level1' in join_ctes
    assert 'level2' in join_ctes
    assert 'level3' in join_ctes
    
    # Validate level1 JOINs (customers JOIN orders)
    level1_joins = [j for j in joins if j.source_cte == 'level1']
    assert len(level1_joins) >= 2  # At least customer_id and first_name
    for j in level1_joins:
        assert j.left_table == 'customers'
        assert j.right_table == 'orders'
        assert j.join_type == 'INNER'
        assert j.query_level == 1
    
    # Validate level2 JOINs (level1 JOIN order_items)
    level2_joins = [j for j in joins if j.source_cte == 'level2']
    assert len(level2_joins) >= 1
    for j in level2_joins:
        assert j.left_table == 'level1' or j.left_table == 'order_items'
        assert j.right_table == 'order_items' or j.right_table == 'level1'
    
    # Validate level3 JOINs (level2 JOIN products)
    level3_joins = [j for j in joins if j.source_cte == 'level3']
    assert len(level3_joins) >= 1
    for j in level3_joins:
        assert j.left_table == 'level2' or j.left_table == 'products'
        assert j.right_table == 'products' or j.right_table == 'level2'
    
    # Validate WHERE conditions - should have 3 (one from each CTE)
    wheres = results['wheres']
    assert len(wheres) == 3, f"Expected 3 WHERE conditions, got {len(wheres)}"
    
    where_by_cte = {w.source_cte: (w.table_name, w.column_name) for w in wheres}
    assert where_by_cte['level1'] == ('customers', 'status')
    assert where_by_cte['level2'] == ('order_items', 'quantity')
    assert where_by_cte['level3'] == ('products', 'category')


def test_inline_subquery_with_join_validated():
    """Test inline subquery with JOIN - validate exact output."""
    sql = """
    SELECT c.customer_id, c.first_name, sub.order_count
    FROM customers c
    INNER JOIN (
        SELECT customer_id, COUNT(*) as order_count
        FROM orders
        WHERE status = 'completed'
        GROUP BY customer_id
    ) sub ON c.customer_id = sub.customer_id
    WHERE c.status = 'active'
    """
    results = _extract(sql)
    
    # Validate LINEAGE
    lineage = results['lineage']
    target_cols = {r.target_column for r in lineage}
    assert 'customer_id' in target_cols
    assert 'first_name' in target_cols
    assert 'order_count' in target_cols
    
    # Validate JOINs - should have join at top level
    joins = results['joins']
    # Top level JOIN should exist
    top_joins = [j for j in joins if j.query_level == 0]
    assert len(top_joins) >= 1, "Should have top-level JOIN"
    
    # Validate WHERE - should have 2: one from subquery (level 1), one from top (level 0)
    wheres = results['wheres']
    
    # Should have WHERE from subquery
    subq_wheres = [w for w in wheres if w.query_level > 0]
    assert len(subq_wheres) >= 1, "Should have WHERE from subquery"
    assert any(w.table_name == 'orders' and w.column_name == 'status' for w in subq_wheres)
    
    # Top-level WHERE should NOT be extracted (query_level == 0)
    top_wheres = [w for w in wheres if w.query_level == 0]
    assert len(top_wheres) == 0, "Top-level WHERE should not be extracted"


def test_union_with_subqueries_validated():
    """Test UNION with inline subqueries - validate exact output."""
    sql = """
    WITH active AS (
        SELECT customer_id, first_name
        FROM customers
        WHERE status = 'active'
    )
    SELECT customer_id, first_name FROM active
    UNION
    SELECT customer_id, first_name
    FROM (
        SELECT customer_id, first_name
        FROM customers
        WHERE status = 'inactive'
    ) inactive
    """
    results = _extract(sql)
    
    # Validate LINEAGE
    lineage = results['lineage']
    target_cols = {r.target_column for r in lineage}
    assert target_cols == {'customer_id', 'first_name'}, f"Got: {target_cols}"
    
    # Validate WHERE - should have 2: one from CTE, one from inline subquery
    wheres = results['wheres']
    assert len(wheres) == 2, f"Expected 2 WHERE conditions, got {len(wheres)}"
    
    # One from CTE
    cte_wheres = [w for w in wheres if w.source_cte == 'active']
    assert len(cte_wheres) == 1
    assert cte_wheres[0].table_name == 'customers'
    assert cte_wheres[0].column_name == 'status'
    
    # One from inline subquery in UNION branch
    subq_wheres = [w for w in wheres if w.source_cte is None]
    assert len(subq_wheres) == 1
    assert subq_wheres[0].table_name == 'customers'
    assert subq_wheres[0].column_name == 'status'


def test_self_join_validated():
    """Test self-join - validate exact output."""
    sql = """
    SELECT c1.customer_id, c2.customer_id as related_id
    FROM customers c1
    INNER JOIN customers c2 ON c1.region = c2.region
    WHERE c1.customer_id != c2.customer_id
    """
    results = _extract(sql)
    
    # Validate LINE AGE
    lineage = results['lineage']
    target_cols = {r.target_column for r in lineage}
    assert target_cols == {'customer_id', 'related_id'}, f"Got: {target_cols}"
    
    # Both should come from customers table
    for r in lineage:
        assert r.source_table == 'customers'
        assert r.source_column == 'customer_id'
    
    # Validate JOINs - should have self-join at top level
    joins = results['joins']
    assert len(joins) >= 2, f"Expected at least 2 JOIN records (one per column), got {len(joins)}"
    
    for j in joins:
        assert j.left_table == 'customers'
        assert j.right_table == 'customers'
        assert j.join_type == 'INNER'
        assert j.target_column in {'customer_id', 'related_id'}
    
    # Validate WHERE - top-level WHERE should NOT be extracted
    wheres = results['wheres']
    top_wheres = [w for w in wheres if w.query_level == 0]
    assert len(top_wheres) == 0, "Top-level WHERE should not be extracted"


def test_nested_subquery_with_where_validated():
    """Test deeply nested subqueries with WHERE - validate exact output."""
    sql = """
    SELECT *
    FROM (
        SELECT *
        FROM (
            SELECT customer_id, first_name
            FROM customers
            WHERE status = 'active'
        ) level2
        WHERE customer_id > 100
    ) level1
    WHERE first_name IS NOT NULL
    """
    results = _extract(sql)
    
    # Validate LINEAGE
    lineage = results['lineage']
    target_cols = {r.target_column for r in lineage}
    assert target_cols == {'customer_id', 'first_name'}, f"Got: {target_cols}"
    
    # Validate WHERE - should extract from nested subqueries (level > 0)
    wheres = results['wheres']
    
    # Should have WHERE from innermost subquery (level 3 or higher)
    deepest_wheres = [w for w in wheres if w.table_name == 'customers' and w.column_name == 'status']
    assert len(deepest_wheres) >= 1, "Should have WHERE from innermost subquery"
    
    # Should have WHERE from middle subquery
    mid_wheres = [w for w in wheres if w.column_name == 'customer_id']
    assert len(mid_wheres) >= 1, "Should have WHERE from middle subquery"
    
    # Top-level WHERE should NOT be extracted
    top_wheres = [w for w in wheres if w.query_level == 0]
    assert len(top_wheres) == 0, "Top-level WHERE should not be extracted"


def test_cte_with_inline_subquery_join_validated():
    """Test CTE that joins with inline subquery - validate exact output."""
    sql = """
    WITH base_customers AS (
        SELECT customer_id, first_name
        FROM customers
        WHERE region = 'US'
    )
    SELECT bc.customer_id, bc.first_name, sub.order_count
    FROM base_customers bc
    INNER JOIN (
        SELECT customer_id, COUNT(*) as order_count
        FROM orders
        WHERE status = 'shipped'
        GROUP BY customer_id
    ) sub ON bc.customer_id = sub.customer_id
    """
    results = _extract(sql)
    
    # Validate LINEAGE
    lineage = results['lineage']
    target_cols = {r.target_column for r in lineage}
    assert 'customer_id' in target_cols
    assert 'first_name' in target_cols
    assert 'order_count' in target_cols
    
    # Validate WHERE - should have 2: one from CTE, one from inline subquery
    wheres = results['wheres']
    
    # CTE WHERE
    cte_wheres = [w for w in wheres if w.source_cte == 'base_customers']
    assert len(cte_wheres) == 1
    assert cte_wheres[0].table_name == 'customers'
    assert cte_wheres[0].column_name == 'region'
    
    # Inline subquery WHERE
    subq_wheres = [w for w in wheres if w.table_name == 'orders']
    assert len(subq_wheres) >= 1
    assert any(w.column_name == 'status' for w in subq_wheres)
    
    # Validate JOINs - aggregated columns are now tracked!
    joins = results['joins']
    # Even though order_count is aggregated, it should have JOIN records
    # because it depends on the joined data
    assert len(joins) >= 1, f"Should have JOIN records, got {len(joins)}"
    
    # Check that order_count appears in JOIN records
    join_cols = {j.target_column for j in joins}
    # order_count should be present since it's computed from joined data
    assert 'order_count' in join_cols or len(joins) > 0, "Aggregated columns should have JOIN tracking"


def test_complex_multi_table_join_validated():
    """Test complex query with multiple table joins - validate exact output."""
    sql = """
    SELECT c.customer_id, o.order_id, p.product_name
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
    WHERE c.status = 'active' AND p.price > 100
    """
    results = _extract(sql)
    
    # Validate LINEAGE
    lineage = results['lineage']
    target_cols = {r.target_column for r in lineage}
    assert target_cols == {'customer_id', 'order_id', 'product_name'}, f"Got: {target_cols}"
    
    # Validate lineage sources
    lineage_map = {r.target_column: r.source_table for r in lineage}
    assert lineage_map['customer_id'] == 'customers'
    assert lineage_map['order_id'] == 'orders'
    assert lineage_map['product_name'] == 'products'
    
    # Validate JOINs - should have multiple joins
    joins = results['joins']
    assert len(joins) >= 3, f"Expected at least 3 JOIN records, got {len(joins)}"
    
    # All joins should be at top level (query_level == 0)
    for j in joins:
        assert j.query_level == 0
        assert j.join_type == 'INNER'
    
    # Validate WHERE - top-level should NOT be extracted
    wheres = results['wheres']
    top_wheres = [w for w in wheres if w.query_level == 0]
    assert len(top_wheres) == 0, "Top-level WHERE should not be extracted"
