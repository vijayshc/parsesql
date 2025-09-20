# Lineage Chain Processor

## Overview
This post-processing tool reads the lineage CSV output from the SQL lineage parser and constructs complete lineage chains, linking dependencies between scripts to show the full data flow from ultimate sources to final targets.

## Features
- **Complete Lineage Chains**: Traces columns from ultimate source tables to final target columns
- **Level Numbering**: Provides both top-to-bottom and bottom-to-top level numbering
- **Cycle Detection**: Detects and handles recursive dependencies safely
- **Multiple Paths**: Handles cases where a column has multiple lineage paths
- **Comprehensive Testing**: Validated with 12+ test scenarios including edge cases

## Output Format
The processor outputs a CSV with the following columns:
- `source_table`: Direct source table for this step
- `source_column`: Direct source column for this step  
- `target_table`: Direct target table for this step
- `target_column`: Direct target column for this step
- `ultimate_source_table`: The root source table in the complete chain
- `ultimate_source_column`: The root source column in the complete chain
- `ultimate_target_table`: The final target table in the complete chain
- `ultimate_target_column`: The final target column in the complete chain
- `level_top_to_bottom`: Level number from ultimate source (1-based)
- `level_bottom_to_top`: Level number from ultimate target (1-based, reverse)

## Usage

### Basic Usage
```bash
python lineage_chain_processor.py input.csv output.csv
```

### Example
```bash
# First generate lineage from SQL files
python sql_lineage_parser.py --sql-folder sql --output output.csv --engines spark

# Then process the lineage chains
python lineage_chain_processor.py output.csv lineage_chains.csv
```

## Example Output
```csv
source_table,source_column,target_table,target_column,ultimate_source_table,ultimate_source_column,ultimate_target_table,ultimate_target_column,level_top_to_bottom,level_bottom_to_top
customers,customer_id,test1,customer_id,customers,customer_id,,customer_id,1,2
test1,customer_id,,customer_id,customers,customer_id,,customer_id,2,1
customers,first_name,,first_name_upper,customers,first_name,,first_name_upper,1,1
```

This shows:
1. A 2-level chain: `customers.customer_id` → `test1.customer_id` → final `customer_id`
2. A 1-level direct mapping: `customers.first_name` → `first_name_upper`

## Key Features

### Cycle Detection
The processor automatically detects and handles recursive dependencies:
```
WARNING: Detected 1 cycles in lineage graph:
  Cycle 1: table_a.col1 -> table_b.col1 -> table_c.col1 -> table_a.col1
Continuing with cycle-safe processing...
```

### Multi-Path Handling
When a column has multiple lineage paths (e.g., both direct and indirect), all paths are captured:
- Direct: `source.col` → `target.col` (level 1/1)
- Indirect: `source.col` → `intermediate.col` → `target.col` (levels 1/2, 2/1)

### Level Numbering
- `level_top_to_bottom`: Position in chain from ultimate source (1 = closest to source)
- `level_bottom_to_top`: Position in chain from ultimate target (1 = closest to target)

## Testing
The processor includes comprehensive testing:
```bash
# Run comprehensive test suite with 12 scenarios
python comprehensive_test_suite.py

# Run validation tests on actual data
python test_lineage_processor.py
```

## Test Scenarios Covered
1. Simple linear chains
2. Multiple sources to single target
3. Single source to multiple targets
4. Complex branching patterns
5. Deep chains (5+ levels)
6. Recursive dependency cycles
7. Diamond dependency patterns
8. Constants and derived columns
9. Mixed lineage patterns
10. Empty/null value handling
11. Large scale performance (200+ steps)
12. Self-referencing tables

## Error Handling
- **Cycles**: Detected and broken gracefully with warnings
- **Missing Data**: Empty or null values are handled safely
- **Invalid Chains**: Malformed lineage data is skipped with warnings
- **Performance**: Optimized for large datasets with deduplication

## Output Analysis
The processor provides summary statistics:
- Number of unique lineage chains processed
- Ultimate source and target column counts
- Multi-level chain identification
- Sample lineage paths for verification

This tool ensures complete traceability of data lineage across complex SQL transformations and multi-script workflows.