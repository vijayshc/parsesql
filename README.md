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
