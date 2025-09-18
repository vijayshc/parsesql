# SQL Lineage Parser

A Python tool for extracting column-level lineage from SQL queries using sqlglot. Supports multiple SQL engines and provides accurate lineage tracing for complex constructs like CTEs, subqueries, joins, unions, aggregates, and window functions.

## Features

- **Multi-Engine Support**: Parses SQL for Spark, Hive, and other sqlglot-supported dialects
- **Schema-Aware Resolution**: Resolves unqualified column references using table schemas
- **Complex SQL Handling**: Supports CTEs, subqueries, joins, unions, aggregates, window functions
- **CSV Output**: Generates detailed lineage reports in CSV format

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

### Star Expansion
`SELECT *` expands using schema:
* Single table / chained CTE: emits each column.
* Joins: emits each column from every joined table (one row per source column).
* Missing schema: falls back to a single `*` record.

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

## Deep CTE Lineage & Star Semantics

The extractor performs multi-level CTE analysis to ensure `SELECT *` only expands to the columns that are actually visible at the final query boundary:

1. CTE Chain Unwinding: Chains like `lvl5 -> lvl4 -> lvl3 -> lvl2 -> lvl1 -> base` are walked top‑down. Expansion stops when an intermediate CTE introduces an explicit projection (column list, aliases, expressions) or a join/multi-source pattern.
2. Projection Pruning Preservation: Columns dropped in any intermediate CTE are never reintroduced during later `*` expansion. Example: If `last_name` and `status` are removed at `lvl2`, a later `SELECT * FROM lvl5` will not emit them.
3. Join Awareness: When a `SELECT *` sits atop a CTE whose body joins multiple sources, expansion emits (when schema is available) the union of projected columns from each underlying side, not the full base table schemas.
4. Expression Aliases: For derived columns (e.g., `total_amount * 0.1 AS tax`), a downstream `SELECT *` over a CTE that already projected `tax` contributes only the physical source columns actually required (here: `orders.total_amount`). It does not duplicate the derived alias as a “source column”; instead it traces back to the physical origin(s).
5. Duplicate Suppression with Mixed Projections: In patterns like `SELECT col_a, *, col_a FROM some_cte`, the `*` expansion will suppress `col_a` because it is already explicitly projected, avoiding redundant lineage rows.
6. Ambiguity Handling: If a `*` spans multiple tables and schema metadata is present, the expansion enumerates concrete columns per table. Without schema, a conservative single `*` placeholder record is emitted.
7. Undefined Reference Warning: If a final `FROM` references a name that is neither a known table (per schema) nor a defined CTE, a warning is logged to aid in catching typos early.

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

## Tests

```bash
python -c "
from tests.test_lineage import test_basic_lineage_on_test_sql
test_basic_lineage_on_test_sql()
print('OK')
"
```
