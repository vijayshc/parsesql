# SQL Lineage Parser (Modular Refactor)

Modular, first‑principles implementation for column‑level SQL lineage built on top of `sqlglot`. The refactored architecture emphasizes clarity, testability, and extensibility while correctly handling real‑world SQL patterns (CTEs, nested subqueries, joins, star expansion, unions, etc.).

## Core Architecture Overview

| Layer | Module | Responsibility |
|-------|--------|----------------|
| Parsing | `sqlglot` | Produces AST for each statement |
| Schema Abstraction | `lineage.core.schema.Schema` | Normalized table → columns mapping, convenience lookups |
| Source Modeling | `lineage.core.sources` | Represents tables, CTEs, subqueries as lazy-resolving sources |
| Expression Analysis | `lineage.core.analyzer.SelectAnalyzer` | Walks projections & resolves column origins per expression |
| Lineage Assembly | `lineage.extractor.LineageExtractor` | Orchestrates parse → analyze → emit `LineageRecord` rows |

### Why This Design?
1. **Deterministic Resolution Flow** – Each projection independently resolves its physical origins; no large stateful inference is required.
2. **Lazy CTE Evaluation** – CTE/subquery outputs are only analyzed if referenced; avoids redundant traversals.
3. **Clear Extension Points** – New resolution strategies (e.g., function provenance, data type propagation) plug into analyzer/origin modeling without rewriting core logic.
4. **Predictable Star Semantics** – `*` and `alias.*` expansions enumerate only visible columns, respecting intermediate pruning.

### Key Improvements vs Legacy Version
- Removed deeply recursive bespoke resolution paths; replaced with uniform `SourceBase.resolve_column()` contract.
- Unified star handling (unqualified & qualified) with consistent output column naming.
- Added explicit origin objects (`ColumnOrigin`) making downstream enrichment (e.g., classification, sensitivity tags) straightforward.
- Simplified schema flattening logic and isolated it from extractor internals.

## Installation

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
python sql_lineage_parser.py --sql-folder sql --output lineage.csv
```

### With Schema Support

Provide schema as JSON string:
```bash
python sql_lineage_parser.py --sql-folder sql --output lineage.csv --schema '{"orders": ["customer_id", "total_amount"]}'
```

Or from JSON file:
```bash
python sql_lineage_parser.py --sql-folder sql --output lineage.csv --schema-file schema.json
```

### Options

- `--sql-folder`: Folder containing .sql files (default: sql)
- `--engines`: Comma-separated list of SQL engines (default: spark,hive)
- `--output`: Output CSV file path (default: output.csv)
- `--schema`: Inline JSON schema dict
- `--schema-file`: Path to JSON schema file
- `--log-level`: Logging level (default: INFO)

## Schema Format

Schema should be a JSON object mapping table names to lists of column names:

```json
{
  "orders": ["customer_id", "total_amount", "order_date"],
  "customers": ["customer_id", "first_name", "last_name"]
}
```

## CSV Schema (Alternative Input)

Instead of JSON, you can provide a CSV via `--schema-csv` with header (case-insensitive):

```
database,table,column,data_type
sales,orders,order_id,int
sales,orders,order_date,date
core,customers,customer_id,int
core,customers,status,string
```

Usage:
```bash
python sql_lineage_parser.py --sql-folder sql --schema-csv schema.csv --engines spark --output lineage.csv
```

Precedence (highest first):
1. `--schema-csv`
2. `--schema-file`
3. `--schema` inline JSON
4. Auto-detected `schema.json` (if present in cwd)
5. Per-file `<name>_schema.json` merged over whichever global schema loaded above

### Behavior with CSV
* Builds nested structure: `db -> table -> {column: type}` (db optional)
* Column names normalized to lowercase
* Data types kept (lowercased) for potential future enrichment (currently lineage uses only names)

### Column Disambiguation
If an unqualified column appears in multiple joined tables and schema says each table has that column, one lineage row per table is emitted (explicit ambiguity). If exactly one table matches, it's used. If none match, prefix heuristics then single-source inference are applied.

### Star Expansion (Refactored)
`SELECT *` now yields one lineage row per physically resolvable column when schema/CTE projection info is available. If a source’s schema is unknown, it is skipped for enumeration (no misleading `*` column synthesis) unless nothing can be resolved – then a single placeholder record is emitted.

### Nested / Qualified Tables
Nested JSON schemas (e.g., `{"a": {"b": {"t2": {"c2": "int"}}}}`) are flattened internally to `a.b.t2` so star expansions include `c2`.


## Output Format

CSV with columns:
- `source_table`: Source table name
- `source_column`: Source column name
- `expression`: SQL expression
- `target_column`: Target column name
- `target_table`: Target table name
- `file`: Source file path
- `engine`: SQL engine used

## Examples

### Input SQL
```sql
SELECT o.customer_id, first_name, total_amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
```

### Output
```
source_table,source_column,expression,target_column,target_table
orders,customer_id,o.customer_id,customer_id,
customers,first_name,first_name,first_name,
orders,total_amount,total_amount,total_amount,
```

## Supported SQL Constructs

- SELECT queries with aliases
- JOINs (INNER, LEFT, RIGHT, FULL)
- CTEs (WITH clauses)
- Subqueries
- UNION/UNION ALL
- Aggregate functions (COUNT, SUM, AVG, etc.)
- Window functions (ROW_NUMBER, RANK, etc.)
- WHERE, GROUP BY, ORDER BY clauses

## Deep CTE Lineage & Star Semantics (Summary)

The new engine walks only what it must: CTE bodies are lazily analyzed, star expansion respects pruning boundaries, joins enumerate visible columns, and unions merge per-branch projections without double counting.

### Practical Examples

| Scenario | Input Pattern | Output Behavior |
|----------|---------------|-----------------|
| CTE pruning | `base -> lvl1(project subset) -> SELECT *` | Only subset columns appear |
| Expression alias | `metrics AS (amount, amount*0.1 tax)` then `SELECT * FROM metrics` | Underlying physical column `amount` traced; no phantom columns |
| Join inside CTE | `joined AS (o JOIN c)` then `SELECT * FROM joined` | Columns attributed to each side (`orders_base.*`, `cust_base.*`) not inflated beyond projection |
| Mixed star & explicit | `SELECT id, *, name` | Single lineage rows for `id` and `name` (star suppressed duplicates) |

This approach maximizes accuracy while preventing over-reporting columns that are not actually part of the user-visible result set.

## Accuracy and Limitations

- Resolves unqualified columns using schema when available
- Handles CTEs and subqueries by traversing AST
- For literal-only expressions, source may be unresolved (None)
- Assumes accurate schema for best resolution
- `impala` maps to `hive` dialect internally

test_basic_lineage_on_test_sql()
## Tests

Run all tests:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=. pytest -q
```

