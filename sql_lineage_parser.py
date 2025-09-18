import argparse
import csv
import glob
import json
import os
import sys
from typing import List

from lineage import LineageExtractor
from lineage.logger import get_logger
from lineage.models import CSV_HEADER, LineageRecord


def find_sql_files(folder: str) -> List[str]:
	pattern = os.path.join(folder, "**", "*.sql")
	return sorted(glob.glob(pattern, recursive=True))


def write_csv(path: str, rows: List[LineageRecord]) -> None:
	with open(path, "w", encoding="utf-8", newline="") as f:
		writer = csv.writer(f)
		writer.writerow(CSV_HEADER)
		for r in rows:
			writer.writerow(r.as_csv_row())


def main() -> int:
	parser = argparse.ArgumentParser(description="SQL Lineage Parser using sqlglot")
	parser.add_argument("--sql-folder", default="sql", help="Folder containing .sql files")
	parser.add_argument(
		"--engines",
		default="spark,hive",
		help="Comma-separated engines to parse (e.g., spark,hive)",
	)
	parser.add_argument(
		"--output",
		default="output.csv",
		help="Path to output CSV file",
	)
	parser.add_argument(
		"--log-level", default=os.getenv("LOG_LEVEL", "INFO"), help="Logging level"
	)
	parser.add_argument(
		"--schema",
		default=None,
		help="JSON string of schema dict, e.g., '{\"table\": [\"col1\", \"col2\"]}'",
	)
	parser.add_argument(
		"--schema-file",
		default=None,
		help="Path to JSON file containing schema dict",
	)
	parser.add_argument(
		"--schema-csv",
		default=None,
		help="Path to CSV file with columns: database,table,column,data_type (Hive types)",
	)

	args = parser.parse_args()
	logger = get_logger(level=args.log_level)

	schema = None
	# 1. CSV schema has highest precedence
	if args.schema_csv:
		csv_path = args.schema_csv
		if not os.path.exists(csv_path):
			logger.error(f"Schema CSV not found: {csv_path}")
			return 1
		try:
			import csv as _csv
			csv_schema = {}
			with open(csv_path, 'r', encoding='utf-8') as cf:
				reader = _csv.reader(cf)
				headers = next(reader, None)
				# Flexible header handling
				# Expected: database, table, column, data_type (case-insensitive)
				if headers:
					headers = [h.strip().lower() for h in headers]
				for row in reader:
					if not row or all(not c.strip() for c in row):
						continue
					# Pad or trim row length
					if len(row) < 4:
						row = row + ["" for _ in range(4 - len(row))]
					db, tbl, col, dtype = [c.strip() for c in row[:4]]
					if not tbl or not col:
						continue
					db_key = db.lower() if db else None
					tbl_key = tbl.lower()
					col_key = col.lower()
					ref = csv_schema
					if db_key:
						ref = ref.setdefault(db_key, {})
					table_ref = ref.setdefault(tbl_key, {})
					table_ref[col_key] = dtype.lower() if dtype else "unknown"
			schema = csv_schema
			logger.info(f"Loaded schema from CSV {csv_path} with {len(schema)} top-level entries")
		except Exception as e:
			logger.error(f"Failed to parse schema CSV {csv_path}: {e}")
			return 1
	elif args.schema_file:
		try:
			with open(args.schema_file, 'r') as f:
				schema = json.load(f)
		except (json.JSONDecodeError, FileNotFoundError) as e:
			logger.error(f"Error loading schema from {args.schema_file}: {e}")
			return 1
	elif args.schema:
		try:
			schema = json.loads(args.schema)
		except json.JSONDecodeError as e:
			logger.error(f"Invalid schema JSON: {e}")
			return 1
	else:
		# Auto-load global schema.json if present
		default_schema_path = os.path.join(os.getcwd(), "schema.json")
		if os.path.exists(default_schema_path):
			try:
				with open(default_schema_path, 'r') as f:
					schema = json.load(f)
				logger.info(f"Loaded global schema.json with {len(schema)} top-level entries")
			except Exception as e:
				logger.warning(f"Could not load global schema.json: {e}")

	sql_files = find_sql_files(args.sql_folder)
	if not sql_files:
		logger.error(f"No SQL files found in {args.sql_folder}")
		return 2

	engines = [e.strip() for e in args.engines.split(",") if e.strip()]
	all_records: List[LineageRecord] = []

	for path in sql_files:
		# Load schema for this file if exists
		schema_file = path.replace('.sql', '_schema.json')
		# Merge file-specific schema (if any) over global/base schema
		file_schema = schema or {}
		if os.path.exists(schema_file):
			try:
				with open(schema_file, 'r') as f:
					local_schema = json.load(f)
				# Shallow merge (file entries override global)
				if isinstance(local_schema, dict):
					merged = dict(file_schema)
					for k, v in local_schema.items():
						merged[k] = v
					file_schema = merged
			except (json.JSONDecodeError, FileNotFoundError) as e:
				logger.debug(f"Error loading schema from {schema_file}: {e}")
		
		parsed = False
		for eng in engines:
			try:
				extractor = LineageExtractor(engine=eng, schema=file_schema, logger=logger)
				records = extractor.extract_from_file(path)
				for r in records:
					all_records.append(r)
				logger.info(f"Successfully parsed {path} with engine {eng}")
				parsed = True
				break
			except Exception as e:
				logger.debug(f"Failed to parse {path} with engine {eng}: {e}")
				continue
		if not parsed:
			logger.error(f"Failed to parse {path} with any engine")

	# Print and write results
	for r in all_records:
		print(
			f"source_table={r.source_table}, source_column={r.source_column}, "
			f"expression={r.expression}, target_column={r.target_column}, target_table={r.target_table}, file={r.file}"
		)

	write_csv(args.output, all_records)
	logger.info(f"Wrote lineage to {args.output} with {len(all_records)} rows")
	return 0


if __name__ == "__main__":
	sys.exit(main())
