import os
from lineage.extractor import LineageExtractor

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

SCHEMA = {
    "customers": ["customer_id", "first_name", "last_name", "status"],
    "orders": ["customer_id", "total_amount", "order_date", "order_id"],
    "known_table": ["col1", "col2", "col3"],
    "test_table": ["col1", "col2", "col3"],
}

def _extract(sql: str):
    extractor = LineageExtractor(engine='spark', schema=SCHEMA)
    # write to temp file for unified path-based interface
    tmp_file = os.path.join(BASE_DIR, 'temp', 'temp_join_test.sql')
    os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
    with open(tmp_file, 'w', encoding='utf-8') as f:
        f.write(sql)
    return extractor.extract_from_file(tmp_file)

def _triples(recs):
    return {(r.target_column, r.source_table, r.source_column) for r in recs}

def test_join_elimination_cte_with_unknown_table():
    """Test that elimination logic works when joining CTE with unknown table."""
    sql = """
    with known_cte as (
        select col1, col2, col3 from test_table
    )
    select t_message, col1 from known_cte f 
    inner join unknown_table on 1=2
    """
    recs = _extract(sql)
    triples = _triples(recs)
    
    # t_message should resolve to unknown_table (since known_cte doesn't have it)
    # col1 should resolve to test_table (via known_cte)
    expected = {
        ('t_message', 'unknown_table', 't_message'),
        ('col1', 'test_table', 'col1')
    }
    assert triples == expected, f"Expected {expected}, got {triples}"

def test_join_elimination_known_table_with_unknown_table():
    """Test elimination with known table schema joining unknown table."""
    sql = """
    select unknown_col, customer_id 
    from customers c 
    inner join unknown_table u on c.customer_id = u.some_id
    """
    recs = _extract(sql)
    triples = _triples(recs)
    
    # unknown_col should resolve to unknown_table (customers doesn't have it)
    # customer_id should resolve to customers (customers has it)
    expected = {
        ('unknown_col', 'unknown_table', 'unknown_col'),
        ('customer_id', 'customers', 'customer_id')
    }
    assert triples == expected, f"Expected {expected}, got {triples}"

def test_join_elimination_multiple_unknown_tables():
    """Test elimination with multiple unknown tables - should be conservative."""
    sql = """
    with precise_cte as (
        select col1, col2 from test_table
    )
    select mystery_col, col1 
    from precise_cte p
    inner join mystery_table1 m1 on 1=1
    inner join mystery_table2 m2 on 1=1
    """
    recs = _extract(sql)
    triples = _triples(recs)
    
    # col1 should resolve to test_table (via precise_cte)
    # mystery_col can't be resolved definitively between mystery_table1/mystery_table2
    # so it should return None (conservative approach)
    
    col1_results = [r for r in recs if r.target_column == 'col1']
    mystery_col_results = [r for r in recs if r.target_column == 'mystery_col']
    
    assert len(col1_results) == 1
    assert col1_results[0].source_table == 'test_table'
    assert col1_results[0].source_column == 'col1'
    
    # For mystery_col, since both unknown tables could have it, should be conservative
    assert len(mystery_col_results) == 1
    assert mystery_col_results[0].source_table is None  # Conservative approach

def test_join_elimination_nested_cte_scenario():
    """Test the original problematic scenario with nested CTEs and JOINs."""
    sql = """
    create table test_result as
    with fmf_int as (
        select col1, col2, col3 from test_table
    ),
    fmf as (
        select f.* from fmf_int f
        inner join (select test_col from some_other_table) as a on 1=2
    ),
    msg_rank as (
        select t_message, col1 from fmf f 
        inner join table2 on 1=2
    ),
    msg as (
        select t_message from msg_rank
    )
    select * from msg
    """
    recs = _extract(sql)
    
    # t_message should resolve to table2 (since fmf only has col1,col2,col3)
    t_message_results = [r for r in recs if r.target_column == 't_message']
    assert len(t_message_results) == 1
    assert t_message_results[0].source_table == 'table2'
    assert t_message_results[0].source_column == 't_message'

def test_join_elimination_ambiguous_case():
    """Test case where column could come from multiple unknown sources."""
    sql = """
    select ambiguous_col 
    from unknown_table1 u1
    inner join unknown_table2 u2 on 1=1
    """
    recs = _extract(sql)
    
    # Since both tables are unknown and could have ambiguous_col,
    # the result should be conservative (None)
    assert len(recs) == 1
    assert recs[0].source_table is None

def test_join_elimination_single_unknown_after_elimination():
    """Test that single unknown table works after elimination."""
    sql = """
    with known_cte as (
        select col1, col2, col3 from test_table
    )
    select unknown_col, col1 from known_cte
    inner join single_unknown_table on 1=1
    """
    recs = _extract(sql)
    triples = _triples(recs)
    
    # col1 should resolve to test_table (via known_cte)
    # unknown_col should resolve to single_unknown_table (only unknown source left)
    expected = {
        ('col1', 'test_table', 'col1'),
        ('unknown_col', 'single_unknown_table', 'unknown_col')
    }
    assert triples == expected, f"Expected {expected}, got {triples}"

def test_join_elimination_with_qualified_columns():
    """Test that qualified column references work correctly."""
    sql = """
    with known_cte as (
        select col1, col2 from test_table
    )
    select k.col1, u.unknown_col 
    from known_cte k
    inner join unknown_table u on 1=1
    """
    recs = _extract(sql)
    triples = _triples(recs)
    
    # Qualified references should work precisely
    expected = {
        ('col1', 'test_table', 'col1'),
        ('unknown_col', 'unknown_table', 'unknown_col')
    }
    assert triples == expected, f"Expected {expected}, got {triples}"

def test_join_elimination_star_expansion():
    """Test elimination with star expansion in JOINs."""
    sql = """
    with limited_cte as (
        select col1, col2 from test_table  
    )
    select * from limited_cte l
    inner join unknown_table u on 1=1
    """
    recs = _extract(sql)
    
    # Star should expand to include columns from both sources
    # But duplicate column names should follow "first occurrence wins"
    target_columns = {r.target_column for r in recs}
    
    # Should at least include the known CTE columns
    assert 'col1' in target_columns
    assert 'col2' in target_columns
    
    # col1, col2 should come from test_table (via limited_cte)
    col1_sources = [r for r in recs if r.target_column == 'col1']
    col2_sources = [r for r in recs if r.target_column == 'col2']
    
    assert len(col1_sources) == 1
    assert col1_sources[0].source_table == 'test_table'
    assert len(col2_sources) == 1  
    assert col2_sources[0].source_table == 'test_table'