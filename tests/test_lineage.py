import os
from lineage.extractor import LineageExtractor

BASE = os.path.dirname(os.path.dirname(__file__))
SQL_DIR = os.path.join(BASE, "sql")
TEST_SQL = os.path.join(SQL_DIR, "test.sql")


def test_basic_lineage_on_test_sql():
    ext = LineageExtractor(engine="spark")
    records = ext.extract_from_file(TEST_SQL)

    def has_record(source_table=None, source_column=None, target_column=None, expr_contains=None):
        for r in records:
            if source_table is not None and r.source_table != source_table:
                continue
            if source_column is not None and r.source_column != source_column:
                continue
            if target_column is not None and r.target_column != target_column:
                continue
            if expr_contains is not None and expr_contains not in r.expression:
                continue
            return True
        return False

    assert has_record("customers", "customer_id", "customer_id")
    assert has_record("orders", "order_date", "order_date")
    assert has_record("products", "product_name", "product_name")
    assert has_record(expr_contains="COUNT(*)", target_column="order_count")
    assert has_record(expr_contains="ROW_NUMBER() OVER", target_column="customer_rank")
