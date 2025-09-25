from __future__ import annotations

from pathlib import Path

import pytest

from lineage_visualizer import LineageGraph, load_lineage_csv, transform_records


@pytest.fixture(scope="module")
def sample_graph() -> LineageGraph:
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "output.csv"
    graph = load_lineage_csv(csv_path)
    if not graph.nodes or not graph.edges:
        pytest.skip("Sample lineage CSV did not produce any graph content.")
    return graph


def test_load_lineage_csv_builds_expected_nodes(sample_graph: LineageGraph) -> None:
    node_lookup = {node.id: node for node in sample_graph.nodes}

    customers_column = node_lookup["table::customers::customer_id"]
    assert customers_column.parent == "group::table::customers"
    assert "group::table::customers" in node_lookup

    assert "table::test1::customer_id" in node_lookup

    expression_nodes = [node for node in sample_graph.nodes if node.role == "expression"]
    assert any(node.expression == "total_amount * 1.1" for node in expression_nodes)


def test_lineage_edges_are_unique(sample_graph: LineageGraph) -> None:
    edge_signatures = {(edge.source, edge.target, edge.expression) for edge in sample_graph.edges}
    assert len(edge_signatures) == len(sample_graph.edges)


def test_transform_records_warns_on_missing_target() -> None:
    graph = transform_records(
        [
            {
                "source_table": "orders",
                "source_column": "order_id",
                "expression": "order_id",
                "target_column": "",
                "target_table": "",
                "file": "demo.sql",
            }
        ]
    )
    assert graph.nodes == []
    assert graph.edges == []
    assert graph.warnings == ["Row 1: target column missing; skipping entry."]


def test_lineage_contains_expected_edge(sample_graph: LineageGraph) -> None:
    target_id = "table::test1::customer_id"
    incoming_sources = {edge.source for edge in sample_graph.edges if edge.target == target_id}
    assert "table::customers::customer_id" in incoming_sources

    parent_groups = {node.id for node in sample_graph.nodes if node.role.startswith("group-")}
    assert "group::table::test1" in parent_groups
