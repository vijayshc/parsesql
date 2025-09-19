import os

from lineage.extractor import LineageExtractor

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

SCHEMA = {
    "customers": ["customer_id", "first_name", "last_name", "status"],
    "orders": ["customer_id", "total_amount", "order_date", "order_id"],
    "z": ["a", "b"],
    "x": ["a"],
    "a.b.t2": ["c2"],
    "sample_data": ["value", "category", "price"],
    "quarterly_sales": ["empid", "amount", "quarter"],
    "loan_ledger": ["product_type", "month", "loan_id"],
    # TPC-DS like tables
    "store_sales": ["ss_sold_date_sk", "ss_store_sk", "ss_customer_sk", "ss_item_sk", "ss_ext_sales_price", "ss_quantity"],
    "date_dim": ["d_date_sk", "d_date"],
    "customer": ["c_customer_sk", "c_first_name"],
}


def _extract(sql: str, schema=SCHEMA):
    extractor = LineageExtractor(engine='spark', schema=schema)
    # write to temp file for unified path-based interface
    tmp_file = os.path.join(BASE_DIR, 'temp', 'temp_case.sql')
    os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
    with open(tmp_file, 'w', encoding='utf-8') as f:
        f.write(sql)
    return extractor.extract_from_file(tmp_file)


def _triples(recs):
    """Return set of (target_column, source_table, source_column) including constants (None origins)."""
    out = set()
    for r in recs:
        if r.target_column:
            out.add((r.target_column, r.source_table, r.source_column))
    return out


def _quads(recs):
    """Return set of (target_column, source_table, source_column, target_table) for DML operations."""
    out = set()
    for r in recs:
        if r.target_column:
            out.add((r.target_column, r.source_table, r.source_column, r.target_table))
    return out


def test_cte_star_expansion_customers_orders():
    path = os.path.join(BASE_DIR, 'sql', 'test_new.sql')
    extractor = LineageExtractor(engine='spark', schema=SCHEMA)
    records = extractor.extract_from_file(path)
    
    # The SQL uses unknown base tables (customersx, orders1) so origins will be unresolved
    # But we should validate the target table mapping (INSERT INTO orders)
    target_records = [r for r in records if r.target_table == 'orders']
    assert len(target_records) > 0, "Should have records targeting orders table"
    
    # Validate complete quads (target_column, source_table, source_column, target_table)
    quads = _quads(records)
    expected_quads = {
        ('customer_id', None, None, 'orders'),  # unknown origins due to missing base tables
        ('order_id', None, None, 'orders'),
    }
    insert_quads = {q for q in quads if q[3] == 'orders'}
    assert insert_quads == expected_quads, f"Expected {expected_quads}, got {insert_quads}"


def test_insert_star_into_orders():
    """Validate INSERT without explicit column list maps schema-aligned columns only."""
    path = os.path.join(BASE_DIR, 'sql', 'test_new.sql')
    with open(path, 'r') as f:
        sql_content = f.read()
    recs = _extract(sql_content)
    
    # Filter records for INSERT INTO orders
    insert_records = [r for r in recs if r.target_table == 'orders']
    assert len(insert_records) > 0, "Should have INSERT records"
    
    # Validate complete quads (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {
        ('customer_id', None, None, 'orders'),  # unknown origins due to missing base tables
        ('order_id', None, None, 'orders'),
    }
    insert_quads = {q for q in quads if q[3] == 'orders'}
    assert insert_quads == expected_quads, f"Expected {expected_quads}, got {insert_quads}"


def test_update_simple():
    sql = """
    UPDATE orders SET total_amount = o2.total_amount
    FROM orders o2
    WHERE orders.order_id = o2.order_id
    """
    schema = {"orders":["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    
    # Should have exactly one UPDATE lineage record
    update_records = [r for r in recs if r.target_table == 'orders']
    assert len(update_records) == 1, f"Expected 1 UPDATE record, got {len(update_records)}"
    
    # Validate complete quad (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {('total_amount', 'orders', 'total_amount', 'orders')}
    assert quads == expected_quads, f"Expected {expected_quads}, got {quads}"


def test_update_constant_and_expression():
    sql = """
    UPDATE orders SET total_amount = total_amount * 1.1, order_date = '2024-01-01'
    """
    schema = {"orders":["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    
    # Should have exactly 2 UPDATE lineage records
    update_records = [r for r in recs if r.target_table == 'orders']
    assert len(update_records) == 2, f"Expected 2 UPDATE records, got {len(update_records)}"
    
    # Validate complete quads (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {
        ('total_amount', 'orders', 'total_amount', 'orders'),
        ('order_date', None, None, 'orders'),  # constant has None source
    }
    assert quads == expected_quads, f"Expected {expected_quads}, got {quads}"


def test_merge_update_insert():
    sql = """
    MERGE INTO orders tgt
    USING (SELECT order_id, total_amount FROM orders) src
    ON tgt.order_id = src.order_id
    WHEN MATCHED THEN UPDATE SET total_amount = src.total_amount
    WHEN NOT MATCHED THEN INSERT (order_id, total_amount) VALUES (src.order_id, src.total_amount)
    """
    schema = {"orders":["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    
    # Should have exactly 3 MERGE lineage records (UPDATE total_amount + INSERT order_id + INSERT total_amount)
    merge_records = [r for r in recs if r.target_table == 'orders']
    assert len(merge_records) == 3, f"Expected 3 MERGE records, got {len(merge_records)}"
    
    # Validate complete quads (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {
        ('total_amount', 'orders', 'total_amount', 'orders'),  # UPDATE
        ('total_amount', 'orders', 'total_amount', 'orders'),  # INSERT (duplicate will be deduped to 1)
        ('order_id', 'orders', 'order_id', 'orders'),         # INSERT
    }
    # Due to deduplication, we might have fewer records, but let's check the actual patterns
    total_amount_quads = [q for q in quads if q[0] == 'total_amount']
    order_id_quads = [q for q in quads if q[0] == 'order_id']
    
    assert len(total_amount_quads) >= 1, "Should have at least 1 total_amount mapping"
    assert len(order_id_quads) == 1, "Should have exactly 1 order_id mapping"
    assert ('total_amount', 'orders', 'total_amount', 'orders') in quads
    assert ('order_id', 'orders', 'order_id', 'orders') in quads


def test_merge_update_only():
    sql = """
    MERGE INTO orders t
    USING (SELECT order_id, total_amount FROM orders) s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET total_amount = s.total_amount
    """
    schema = {"orders":["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    
    # Should have exactly 1 MERGE UPDATE record
    merge_records = [r for r in recs if r.target_table == 'orders']
    assert len(merge_records) == 1, f"Expected 1 MERGE record, got {len(merge_records)}"
    
    # Validate complete quad (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {('total_amount', 'orders', 'total_amount', 'orders')}
    assert quads == expected_quads, f"Expected {expected_quads}, got {quads}"

def test_merge_insert_only():
    sql = """
    MERGE INTO orders t
    USING (SELECT order_id, total_amount FROM orders) s
    ON t.order_id = s.order_id
    WHEN NOT MATCHED THEN INSERT (order_id,total_amount) VALUES (s.order_id,s.total_amount)
    """
    schema = {"orders":["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    
    # Should have exactly 2 MERGE INSERT records
    merge_records = [r for r in recs if r.target_table == 'orders']
    assert len(merge_records) == 2, f"Expected 2 MERGE records, got {len(merge_records)}"
    
    # Validate complete quads (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {
        ('order_id', 'orders', 'order_id', 'orders'),
        ('total_amount', 'orders', 'total_amount', 'orders'),
    }
    assert quads == expected_quads, f"Expected {expected_quads}, got {quads}"

def test_merge_update_expression():
    sql = """
    MERGE INTO orders t
    USING (SELECT order_id, total_amount FROM orders) s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET total_amount = s.total_amount * 1.05
    """
    schema = {"orders":["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    
    # Should have exactly 1 MERGE UPDATE record
    merge_records = [r for r in recs if r.target_table == 'orders']
    assert len(merge_records) == 1, f"Expected 1 MERGE record, got {len(merge_records)}"
    
    # Validate complete quad (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {('total_amount', 'orders', 'total_amount', 'orders')}
    assert quads == expected_quads, f"Expected {expected_quads}, got {quads}"
    
    # Also verify expression contains multiplication
    record = merge_records[0]
    assert '*' in record.expression or 'MULTIPLY' in record.expression.upper(), f"Expression should contain multiplication: {record.expression}"


def test_insert_with_cte_chain_unknown_sources():
    sql = """
    WITH customers1 AS (
        SELECT a.*, b.* FROM customersx a
        INNER JOIN orders1 b ON a.customer_id = b.customer_id
    ), customers2 AS (
      SELECT customer_name, customer_id, order_id FROM customers1 WHERE a.status = 'active'
    )
    INSERT INTO orders
    SELECT * FROM customers2
    """
    # Provide only target schema so intermediate sources are unknown
    schema = {"orders": ["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    
    # Filter records for INSERT INTO orders
    insert_records = [r for r in recs if r.target_table == 'orders']
    assert len(insert_records) > 0, "Should have INSERT records"
    
    # Validate complete quads (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {
        ('customer_id', None, None, 'orders'),  # unknown origins due to missing base tables
        ('order_id', None, None, 'orders'),
    }
    insert_quads = {q for q in quads if q[3] == 'orders'}
    assert insert_quads == expected_quads, f"Expected {expected_quads}, got {insert_quads}"


def test_simple_select_single_table():
    recs = _extract("SELECT customer_id, first_name FROM customers")
    
    # Should have exactly 2 lineage records
    assert len(recs) == 2, f"Expected 2 records, got {len(recs)}"
    
    # Validate source pairs
    pairs = {(r.source_table, r.source_column) for r in recs}
    expected_pairs = {('customers','customer_id'), ('customers','first_name')}
    assert pairs == expected_pairs, f"Expected {expected_pairs}, got {pairs}"
    
    # Validate target mappings
    triples = _triples(recs)
    expected_triples = {
        ('customer_id','customers','customer_id'),
        ('first_name','customers','first_name'),
    }
    assert triples == expected_triples, f"Expected {expected_triples}, got {triples}"
    
    # Validate no target_table for SELECT statements
    for record in recs:
        assert record.target_table == '' or record.target_table is None


def test_union_basic():
    recs = _extract("SELECT a FROM (SELECT a FROM x UNION SELECT a FROM x) u")
    pairs = {(r.source_table, r.source_column) for r in recs}
    assert pairs == {('x','a')}
    assert _triples(recs) == {('a','x','a')}


def test_nested_cte_chain():
    local_schema = {"base": ["a"]}
    sql = "WITH t1 AS (SELECT a FROM base), t2 AS (SELECT a FROM t1) SELECT a FROM t2"
    recs = _extract(sql, schema=local_schema)
    assert {(r.source_table, r.source_column) for r in recs} == {('base','a')}
    assert _triples(recs) == {('a','base','a')}


def test_values_source():
    recs = _extract("SELECT a FROM (VALUES (1),(2)) AS t(a)")
    # Should produce at least one lineage row (may have no table origin)
    # VALUES origin is synthetic (no table); expect None source
    assert _triples(recs) == {('a', None, 'a')}


def test_subquery_scalar():
    recs = _extract("SELECT (SELECT max(o.order_id) FROM orders o) AS mx")
    srcs = {(r.source_table, r.source_column) for r in recs if r.source_column}
    assert srcs == {('orders','order_id')}
    assert _triples(recs) == {('mx','orders','order_id')}


def test_join_star_and_column():
    sql = "SELECT c.first_name, o.order_id FROM customers c JOIN orders o ON c.customer_id = o.customer_id"
    recs = _extract(sql)
    
    # Validate we have at least the expected output columns
    output_columns = {r.target_column for r in recs}
    expected_outputs = {'first_name', 'order_id'}
    assert expected_outputs.issubset(output_columns), f"Expected outputs {expected_outputs} not found in {output_columns}"
    
    # Validate source pairs include expected mappings
    pairs = {(r.source_table, r.source_column) for r in recs}
    expected_core_pairs = {('customers','first_name'), ('orders','order_id')}
    assert expected_core_pairs.issubset(pairs), f"Expected core pairs {expected_core_pairs} not found in {pairs}"
    
    # Validate target mappings (allowing for additional join predicate columns)
    triples = _triples(recs)
    expected_core_triples = {
        ('first_name','customers','first_name'),
        ('order_id','orders','order_id'),
    }
    assert expected_core_triples.issubset(triples), f"Expected core triples {expected_core_triples} not found in {triples}"


def test_pivot_like():
    sql = "SELECT empid FROM quarterly_sales"
    recs = _extract(sql)
    assert {(r.source_table, r.source_column) for r in recs} == {('quarterly_sales','empid')}
    assert _triples(recs) == {('empid','quarterly_sales','empid')}


def test_double_nested_subquery():
    sql = "SELECT * FROM (SELECT * FROM (SELECT order_id FROM orders) q1) q2"
    recs = _extract(sql)
    assert {(r.source_table, r.source_column) for r in recs} == {('orders','order_id')}
    assert _triples(recs) == {('order_id','orders','order_id')}


def test_scalar_subquery_aggregation():
    sql = "SELECT (SELECT max(o.order_id) FROM orders o) + 1 AS val FROM customers"
    recs = _extract(sql)
    assert {(r.source_table, r.source_column) for r in recs if r.source_column} == {('orders','order_id')}
    assert _triples(recs) == {('val','orders','order_id')}


def test_union_three_way():
    sql = "SELECT a FROM (SELECT a FROM x UNION SELECT a FROM x UNION SELECT a FROM x) u"
    recs = _extract(sql, schema={"x":["a"]})
    assert {(r.source_table, r.source_column) for r in recs} == {('x','a')}
    assert _triples(recs) == {('a','x','a')}


def test_cte_union_dataset():
    sql = """
    WITH dataset AS (
      SELECT * FROM customers
      UNION
      SELECT * FROM customers
    )
    SELECT customer_id FROM dataset
    """
    recs = _extract(sql)
    assert {(r.source_table, r.source_column) for r in recs if r.source_column} == {('customers','customer_id')}
    assert _triples(recs) == {('customer_id','customers','customer_id')}


def test_cte_name_appears_in_schema():
    schema = {"a.b.t2":["c2"], "t1":["c2"]}
    sql = "WITH t1 AS (SELECT * FROM a.b.t2), inter AS (SELECT * FROM t1) SELECT * FROM inter"
    recs = _extract(sql, schema=schema)
    assert {(r.source_table, r.source_column) for r in recs if r.source_column} == {('a.b.t2','c2')}
    assert _triples(recs) == {('c2','a.b.t2','c2')}


def test_insert_ddl_lineage():
    sql = """
    INSERT INTO target (x, y)
    SELECT t.x, t.y FROM (
        SELECT o.order_id AS x, 1 AS y FROM orders o
    ) t
    """
    schema = {"orders": ["order_id"]}
    recs = _extract(sql, schema=schema)
    # Expect order_id lineage and target mapping
    assert any(r.target_table == 'target' and r.target_column == 'x' for r in recs), recs
    assert ('orders','order_id') in {(r.source_table, r.source_column) for r in recs}
    triples = _triples(recs)
    # x comes from orders.order_id, y is constant
    assert ('x','orders','order_id') in triples
    assert ('y', None, None) in triples


def test_derived_table_alias():
    sql = "SELECT a FROM (SELECT customer_id AS a FROM customers) subq"
    recs = _extract(sql)
    assert {(r.source_table, r.source_column) for r in recs} == {('customers','customer_id')}
    assert _triples(recs) == {('a','customers','customer_id')}


def test_derived_table_no_alias():
    sql = "SELECT a FROM (SELECT customer_id AS a FROM customers)"
    recs = _extract(sql)
    assert {(r.source_table, r.source_column) for r in recs} == {('customers','customer_id')}
    assert _triples(recs) == {('a','customers','customer_id')}


def test_join_using_style():
    sql = "WITH y AS (SELECT * FROM customers) SELECT customer_id FROM y JOIN orders USING (customer_id)"
    recs = _extract(sql)
    pairs = {(r.source_table, r.source_column) for r in recs}
    assert ('customers','customer_id') in pairs or ('orders','customer_id') in pairs
    # final target column
    triples = _triples(recs)
    # Disambiguation picks orders.customer_id given resolution logic
    assert triples == {('customer_id','orders','customer_id')}


def test_pivot_basic():
    sql = "SELECT empid FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN ('2023_Q1','2023_Q2'))"
    recs = _extract(sql)
    assert {(r.source_table, r.source_column) for r in recs} == {('quarterly_sales','empid')}
    assert _triples(recs) == {('empid','quarterly_sales','empid')}


def test_pivot_with_subquery_chain():
    sql = """
    WITH cte AS (
      SELECT * FROM (
         SELECT product_type, month, loan_id FROM loan_ledger
      ) PIVOT(COUNT(loan_id) FOR month IN ('2024-10','2024-11'))
    )
    SELECT product_type, `2024-10`, `2024-11` FROM cte
    """
    recs = _extract(sql)
    cols = {(r.source_table, r.source_column) for r in recs if r.source_column}
    # All pivot output columns derive from loan_ledger.loan_id (the aggregated column) and product_type passes through
    assert ('loan_ledger','product_type') in cols
    assert ('loan_ledger','loan_id') in cols  # aggregation source
    triples = _triples(recs)
    expected = {
        ('product_type','loan_ledger','product_type'),
        ('2024-10','loan_ledger','loan_id'),
        ('2024-11','loan_ledger','loan_id'),
    }
    assert triples == expected


def test_star_with_nested_subqueries():
    sql = "SELECT * FROM (SELECT * FROM (SELECT * FROM customers) c1) c2"
    recs = _extract(sql)
    assert ('customers','customer_id') in {(r.source_table, r.source_column) for r in recs}
    # All customer columns should appear through star propagation
    expected = {
        ('customer_id','customers','customer_id'),
        ('first_name','customers','first_name'),
        ('last_name','customers','last_name'),
        ('status','customers','status'),
    }
    assert _triples(recs) == expected


# --- Tests derived from local sql/ directory scenarios ---

def test_sql_cte_alias_chain_prune():
    sql = (BASE_DIR + '/sql/test_cte_alias_chain_prune.sql')
    with open(sql,'r') as f: text=f.read()
    recs = _extract(text)
    # final should only include customer_id column lineage
    cols = {(r.source_table, r.source_column) for r in recs}
    # Only customer_id survives pruning chain
    assert ('customers','customer_id') in cols
    assert all(c[1] != 'first_name' for c in cols)
    # Target column set should only contain customer_id
    assert _triples(recs) == {('customer_id','customers','customer_id')}

def test_sql_cte_complex_chain():
    with open(BASE_DIR + '/sql/test_cte_complex.sql') as f: text=f.read()
    recs = _extract(text)
    cols = {(r.source_table, r.source_column) for r in recs}
    # All base customer columns should appear
    # Deep star chain shouldn't invent columns; expects all base columns
    expected = {'customer_id','first_name','last_name','status'}
    assert {c for t,c in cols if t=='customers'} == expected
    assert _triples(recs) == {
        ('customer_id','customers','customer_id'),
        ('first_name','customers','first_name'),
        ('last_name','customers','last_name'),
        ('status','customers','status'),
    }

def test_sql_cte_expression_alias():
    with open(BASE_DIR + '/sql/test_cte_expression_alias.sql') as f: text=f.read()
    recs = _extract(text)
    cols = {(r.source_table, r.source_column) for r in recs}
    # Ensure derived columns (tax, adjusted) trace back to total_amount
    ta_sources = [r for r in recs if r.target_column in ('tax','adjusted')]
    assert any(r.source_column == 'total_amount' for r in ta_sources)
    assert ('orders','customer_id') in cols
    assert _triples(recs) == {
        ('customer_id','orders','customer_id'),
        ('tax','orders','total_amount'),
        ('adjusted','orders','total_amount'),
    }

def test_sql_cte_join_star():
    with open(BASE_DIR + '/sql/test_cte_join_star.sql') as f: text=f.read()
    recs = _extract(text)
    cols = {(r.source_table, r.source_column) for r in recs}
    assert ('orders','order_id') in cols
    assert ('customers','first_name') in cols
    assert _triples(recs) == {
        ('order_id','orders','order_id'),
        ('customer_id','orders','customer_id'),
        ('first_name','customers','first_name'),
    }

def test_sql_cte_join_star_prune():
    with open(BASE_DIR + '/sql/test_cte_join_star_prune.sql') as f: text=f.read()
    recs = _extract(text)
    cols = {(r.source_table, r.source_column) for r in recs}
    assert ('customers','customer_id') in cols
    assert ('orders','order_id') not in cols  # pruned away
    assert _triples(recs) == {('customer_id','customers','customer_id')}

def test_sql_cte_mixed_star_explicit():
    with open(BASE_DIR + '/sql/test_cte_mixed_star_explicit.sql') as f: text=f.read()
    recs = _extract(text)
    # should not duplicate first_name
    assert _triples(recs) == {
        ('customer_id','customers','customer_id'),
        ('first_name','customers','first_name'),
    }

def test_sql_cte_nested_subquery():
    with open(BASE_DIR + '/sql/test_cte_nested_subquery.sql') as f: text=f.read()
    recs = _extract(text)
    cols = {(r.source_table, r.source_column) for r in recs}
    assert ('customers','customer_id') in cols
    assert _triples(recs) == {
        ('customer_id','customers','customer_id'),
        ('first_name','customers','first_name'),
    }

def test_sql_cte_projection_prune():
    with open(BASE_DIR + '/sql/test_cte_projection_prune.sql') as f: text=f.read()
    recs = _extract(text)
    cols = {(r.source_table, r.source_column) for r in recs}
    assert ('customers','customer_id') in cols
    assert ('customers','last_name') not in cols
    assert ('customers','status') not in cols
    assert _triples(recs) == {
        ('customer_id','customers','customer_id'),
        ('first_name','customers','first_name'),
    }

def test_sql_cte_union_star():
    with open(BASE_DIR + '/sql/test_cte_union_star.sql') as f: text=f.read()
    recs = _extract(text)
    cols = {(r.source_table, r.source_column) for r in recs}
    assert ('customers','customer_id') in cols
    assert _triples(recs) == {
        ('customer_id','customers','customer_id'),
        ('first_name','customers','first_name'),
    }

def test_sql_plain_complex():
    with open(BASE_DIR + '/sql/test_plain_complex.sql') as f: text=f.read()
    recs = _extract(text)
    
    # This SQL has a UNION ALL with 6 output columns each branch
    # First branch: customer_id, first_name_upper, order_id, gross_amount, size_flag, rank_in_group  
    # Second branch: customer_id, first_name_upper, order_id, gross_amount, size_flag, rank_in_group
    # So we expect lineage for all these target columns
    
    target_columns = {r.target_column for r in recs if r.target_column}
    expected_target_columns = {'customer_id', 'first_name_upper', 'order_id', 'gross_amount', 'size_flag', 'rank_in_group'}
    assert target_columns == expected_target_columns, f"Expected {expected_target_columns}, got {target_columns}"
    
    # Validate source mappings for key columns
    cols = {(r.source_table, r.source_column) for r in recs if r.source_column}
    expected_source_columns = {('customers','customer_id'), ('customers','first_name'), ('orders','order_id'), ('orders','total_amount')}
    assert expected_source_columns.issubset(cols), f"Expected source columns {expected_source_columns} not found in {cols}"
    
    # Validate specific target-source mappings
    triples = _triples(recs)
    
    # Must have mappings for all derived columns
    customer_id_mappings = [t for t in triples if t[0] == 'customer_id']
    assert len(customer_id_mappings) >= 1, "Missing customer_id mappings"
    assert any(t[1] == 'customers' and t[2] == 'customer_id' for t in customer_id_mappings), "Missing customers.customer_id mapping"
    
    first_name_mappings = [t for t in triples if t[0] == 'first_name_upper']
    assert len(first_name_mappings) >= 1, "Missing first_name_upper mappings"
    assert any(t[1] == 'customers' and t[2] == 'first_name' for t in first_name_mappings), "Missing customers.first_name mapping"
    
    order_id_mappings = [t for t in triples if t[0] == 'order_id']
    assert len(order_id_mappings) >= 1, "Missing order_id mappings"
    # order_id can be orders.order_id or None (from second branch NULL)
    
    gross_amount_mappings = [t for t in triples if t[0] == 'gross_amount']
    assert len(gross_amount_mappings) >= 1, "Missing gross_amount mappings"
    assert any(t[1] == 'orders' and t[2] == 'total_amount' for t in gross_amount_mappings), "Missing orders.total_amount mapping for gross_amount"
    
    size_flag_mappings = [t for t in triples if t[0] == 'size_flag']
    assert len(size_flag_mappings) >= 1, "Missing size_flag mappings"
    
    rank_mappings = [t for t in triples if t[0] == 'rank_in_group']
    assert len(rank_mappings) >= 1, "Missing rank_in_group mappings"

# --- TPC-DS representative tests ---

def test_tpcds_fact_dim_join_prefix_inference():
    sql = """
    SELECT ss_quantity, ss_ext_sales_price, d_date
    FROM store_sales ss
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE ss_ext_sales_price > 0
    """
    recs = _extract(sql)
    cols = {(r.source_table, r.source_column) for r in recs}
    # prefix inference for ss_* columns
    assert ('store_sales','ss_ext_sales_price') in cols
    assert any(c[0]=='date_dim' for c in cols)
    triples = _triples(recs)
    expected = {
        ('ss_quantity','store_sales','ss_quantity'),
        ('ss_ext_sales_price','store_sales','ss_ext_sales_price'),
        ('d_date','date_dim','d_date'),
    }
    assert expected.issubset(triples)

def test_tpcds_cte_aggregate():
    sql = """
    WITH sales AS (
      SELECT ss_store_sk, SUM(ss_ext_sales_price) AS revenue
      FROM store_sales
      GROUP BY ss_store_sk
    )
    SELECT revenue FROM sales
    """
    recs = _extract(sql)
    cols = {(r.source_table, r.source_column) for r in recs}
    assert ('store_sales','ss_ext_sales_price') in cols
    assert _triples(recs) == {('revenue','store_sales','ss_ext_sales_price')}

def test_tpcds_window_function():
    sql = "SELECT ss_customer_sk, ROW_NUMBER() OVER (PARTITION BY ss_store_sk ORDER BY ss_ext_sales_price DESC) AS rn FROM store_sales"
    recs = _extract(sql)
    
    # Should have exactly 2 target columns
    target_columns = {r.target_column for r in recs if r.target_column}
    expected_target_columns = {'ss_customer_sk', 'rn'}
    assert target_columns == expected_target_columns, f"Expected {expected_target_columns}, got {target_columns}"
    
    # Validate source columns
    cols = {(r.source_table, r.source_column) for r in recs if r.source_column}
    expected_source_columns = {('store_sales','ss_customer_sk'), ('store_sales','ss_ext_sales_price'), ('store_sales','ss_store_sk')}
    assert expected_source_columns.issubset(cols), f"Expected {expected_source_columns}, got {cols}"
    
    # Validate mappings
    triples = _triples(recs)
    
    # ss_customer_sk should map directly
    assert ('ss_customer_sk','store_sales','ss_customer_sk') in triples
    
    # rn should have multiple origins (partition by ss_store_sk, order by ss_ext_sales_price)
    rn_triples = [t for t in triples if t[0] == 'rn']
    assert len(rn_triples) >= 2, f"Expected at least 2 origins for rn, got {rn_triples}"
    assert ('rn','store_sales','ss_ext_sales_price') in triples
    assert ('rn','store_sales','ss_store_sk') in triples


def test_ctas_basic_subset():
    sql = """
    CREATE TABLE tgt_basic AS
    SELECT customer_id, first_name FROM customers
    """
    recs = _extract(sql)
    
    # Should have exactly 2 CTAS lineage records
    ctas_records = [r for r in recs if r.target_table == 'tgt_basic']
    assert len(ctas_records) == 2, f"Expected 2 CTAS records, got {len(ctas_records)}"
    
    # Validate complete quads (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {
        ('customer_id', 'customers', 'customer_id', 'tgt_basic'),
        ('first_name', 'customers', 'first_name', 'tgt_basic'),
    }
    ctas_quads = {q for q in quads if q[3] == 'tgt_basic'}
    assert ctas_quads == expected_quads, f"Expected {expected_quads}, got {ctas_quads}"

def test_ctas_star():
    sql = "CREATE TABLE tgt_star AS SELECT * FROM customers"
    recs = _extract(sql)
    
    # Should have exactly 4 CTAS lineage records (all customer columns)
    ctas_records = [r for r in recs if r.target_table == 'tgt_star']
    assert len(ctas_records) == 4, f"Expected 4 CTAS records, got {len(ctas_records)}"
    
    # Validate complete quads (target_column, source_table, source_column, target_table)
    quads = _quads(recs)
    expected_quads = {
        ('customer_id', 'customers', 'customer_id', 'tgt_star'),
        ('first_name', 'customers', 'first_name', 'tgt_star'),
        ('last_name', 'customers', 'last_name', 'tgt_star'),
        ('status', 'customers', 'status', 'tgt_star'),
    }
    ctas_quads = {q for q in quads if q[3] == 'tgt_star'}
    assert ctas_quads == expected_quads, f"Expected {expected_quads}, got {ctas_quads}"

def test_ctas_join_alias_expression():
    sql = """
    CREATE TABLE tgt_join AS
    SELECT c.customer_id, o.order_id, o.total_amount * 1.1 AS gross
    FROM customers c JOIN orders o ON c.customer_id = o.customer_id
    """
    schema = {
        'customers': ['customer_id','first_name','last_name','status'],
        'orders': ['customer_id','total_amount','order_date','order_id']
    }
    recs = _extract(sql, schema=schema)
    triples = _triples(recs)
    assert ('customer_id','customers','customer_id') in triples
    assert ('order_id','orders','order_id') in triples
    assert ('gross','orders','total_amount') in triples

def test_ctas_with_cte_chain():
    sql = """
    WITH base AS (SELECT * FROM customers), j AS (SELECT customer_id FROM base)
    CREATE TABLE tgt_chain AS SELECT customer_id FROM j
    """
    recs = _extract(sql)
    triples = _triples(recs)
    assert ('customer_id','customers','customer_id') in triples
    assert any(r.target_table=='tgt_chain' for r in recs)

def test_ctas_constants_and_multi_origin():
    sql = """
    CREATE TABLE tgt_expr AS
    SELECT c.customer_id, o.order_id, o.total_amount + 5 AS adjusted
    FROM customers c JOIN orders o ON c.customer_id = o.customer_id
    """
    schema = {
        'customers': ['customer_id','first_name','last_name','status'],
        'orders': ['customer_id','total_amount','order_date','order_id']
    }
    recs = _extract(sql, schema=schema)
    triples = _triples(recs)
    assert ('adjusted','orders','total_amount') in triples
    assert ('customer_id','customers','customer_id') in triples
    assert ('order_id','orders','order_id') in triples

def test_ctas_create_or_replace():
    sql = """
    CREATE OR REPLACE TABLE tgt_replace AS
    SELECT customer_id FROM customers
    """
    recs = _extract(sql)
    triples = _triples(recs)
    assert ('customer_id','customers','customer_id') in triples


def test_multistmt_ctas_then_select():
    sql = """
    CREATE TABLE tmp_orders AS SELECT order_id, customer_id, total_amount FROM orders;
    SELECT customer_id, order_id FROM tmp_orders;
    """
    schema = {"orders": ["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    # Expect lineage for CTAS target tmp_orders sourced from orders, plus second statement projecting from tmp_orders without target table
    src_pairs = {(r.source_table, r.source_column) for r in recs if r.source_table}
    assert ('orders','order_id') in src_pairs and ('orders','customer_id') in src_pairs and ('orders','total_amount') in src_pairs
    # Ensure tmp_orders registered so second select produces lineage rows referencing original orders columns, not lost
    assert any(r.source_table=='orders' and r.target_column=='customer_id' for r in recs)

def test_multistmt_ctas_chain_and_insert():
    sql = """
    CREATE TABLE t_stage AS SELECT customer_id, total_amount FROM orders;
    CREATE TABLE t_stage2 AS SELECT customer_id FROM t_stage;
    INSERT INTO target (customer_id) SELECT customer_id FROM t_stage2;
    """
    schema = {"orders": ["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    triples = _triples(recs)
    # Final insert should trace back to orders.customer_id
    assert ('customer_id','orders','customer_id') in triples
    assert any(r.target_table=='target' and r.target_column=='customer_id' for r in recs)

def test_multistmt_ctas_then_update():
    sql = """
    CREATE TABLE t_upd AS SELECT order_id, total_amount FROM orders;
    UPDATE t_upd SET total_amount = total_amount * 1.2;
    """
    schema = {"orders": ["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    triples = _triples(recs)
    # Update lineage should still map to t_upd total_amount with origin orders.total_amount
    assert ('total_amount','orders','total_amount') in triples

def test_multistmt_ctas_then_merge():
    sql = """
    CREATE TABLE t_merge_src AS SELECT order_id, total_amount FROM orders;
    MERGE INTO orders tgt USING t_merge_src s ON tgt.order_id = s.order_id WHEN MATCHED THEN UPDATE SET total_amount = s.total_amount;
    """
    schema = {"orders": ["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    triples = _triples(recs)
    assert ('total_amount','orders','total_amount') in triples

def test_multistmt_ctas_star_then_select_with_schema():
    sql = """
    CREATE TABLE t_ctas_star AS SELECT * FROM orders;
    SELECT customer_id, order_id, total_amount FROM t_ctas_star;
    """
    schema = {"orders": ["customer_id","total_amount","order_date","order_id"]}
    recs = _extract(sql, schema=schema)
    
    # Validate CTAS records using quads
    ctas_quads = {q for q in _quads(recs) if q[3] == 't_ctas_star'}
    expected_ctas_quads = {
        ('customer_id', 'orders', 'customer_id', 't_ctas_star'),
        ('total_amount', 'orders', 'total_amount', 't_ctas_star'),
        ('order_date', 'orders', 'order_date', 't_ctas_star'),
        ('order_id', 'orders', 'order_id', 't_ctas_star'),
    }
    assert ctas_quads == expected_ctas_quads, f"Expected {expected_ctas_quads}, got {ctas_quads}"
    
    # Validate SELECT records using triples (no target_table for SELECT)
    select_triples = {t for t in _triples(recs) if not any(r.target_table == 't_ctas_star' and r.target_column == t[0] for r in recs)}
    expected_select_triples = {
        ('customer_id', 't_ctas_star', 'customer_id'),
        ('order_id', 't_ctas_star', 'order_id'),
        ('total_amount', 't_ctas_star', 'total_amount'),
    }
    
    # Alternative approach: filter SELECT records by target_table being None or empty
    select_records = [r for r in recs if not r.target_table or r.target_table == '']
    assert len(select_records) == 3, f"Expected 3 SELECT records, got {len(select_records)}"
    
    select_triples_direct = {(r.target_column, r.source_table, r.source_column) for r in select_records}
    assert select_triples_direct == expected_select_triples, f"Expected {expected_select_triples}, got {select_triples_direct}"