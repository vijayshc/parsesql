#!/usr/bin/env python3
"""
Post-processing script to construct lineage chains from the CSV output.
Links lineage between scripts to show the complete data flow from ultimate sources.
Output format: source_table,source_column,target_table,target_column,ultimate_source_table,ultimate_source_column,ultimate_target_table,ultimate_target_column,level_top_to_bottom,level_bottom_to_top
"""

import csv
import sys
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class LineageRecord:
    """Represents a single lineage record from the CSV."""
    source_table: str
    source_column: str
    expression: str
    target_column: str
    target_table: str
    file: str


@dataclass
class LineageStep:
    """Represents a single step in a lineage chain."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    ultimate_source_table: str
    ultimate_source_column: str
    ultimate_target_table: str
    ultimate_target_column: str
    level_top_to_bottom: int
    level_bottom_to_top: int
    
    def to_csv_row(self) -> str:
        """Convert to CSV row format."""
        return f"{self.source_table},{self.source_column},{self.target_table},{self.target_column},{self.ultimate_source_table},{self.ultimate_source_column},{self.ultimate_target_table},{self.ultimate_target_column},{self.level_top_to_bottom},{self.level_bottom_to_top}"


class LineageChainProcessor:
    """Processes lineage CSV to construct complete lineage chains."""
    
    def __init__(self, csv_file: str):
        self.csv_file = csv_file
        self.records: List[LineageRecord] = []
        # Map (table, column) -> List[(source_table, source_column)]
        self.lineage_map: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
        self.created_tables: Set[str] = set()  # Tables created by scripts
        self.base_tables: Set[str] = set()  # Tables that are never created
        
    def load_lineage_data(self) -> None:
        """Load lineage records from CSV file."""
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                record = LineageRecord(
                    source_table=row['source_table'].strip() if row['source_table'] else '',
                    source_column=row['source_column'].strip() if row['source_column'] else '',
                    expression=row['expression'].strip() if row['expression'] else '',
                    target_column=row['target_column'].strip() if row['target_column'] else '',
                    target_table=row['target_table'].strip() if row['target_table'] else '',
                    file=row['file'].strip() if row['file'] else ''
                )
                self.records.append(record)
    
    def build_lineage_map(self) -> None:
        """Build the lineage mapping from the records."""
        # First pass: identify created tables
        for record in self.records:
            if record.target_table:
                self.created_tables.add(record.target_table)
        
        # Second pass: build lineage mapping and identify base tables
        for record in self.records:
            if not record.target_column:
                continue
                
            # Map target to its sources
            target_key = (record.target_table, record.target_column)
            source_key = (record.source_table, record.source_column)
            
            # Only add non-empty sources
            if record.source_table or record.source_column:
                self.lineage_map[target_key].append(source_key)
            
            # Track base tables (tables that are used as sources but never created)
            if record.source_table and record.source_table not in self.created_tables:
                self.base_tables.add(record.source_table)
    
    def trace_lineage_chain(self, target_table: str, target_column: str, visited: Optional[Set[Tuple[str, str]]] = None) -> List[List[Tuple[str, str]]]:
        """
        Trace lineage chain for a target column.
        Returns list of complete paths from base tables to the target.
        Uses robust cycle detection to prevent infinite recursion.
        """
        if visited is None:
            visited = set()
        
        target_key = (target_table, target_column)
        
        # Cycle detection: if we've already visited this node in the current path, we have a cycle
        if target_key in visited:
            print(f"WARNING: Cycle detected at {target_key}, breaking cycle")
            return []
        
        visited.add(target_key)
        
        # Get sources for this target
        sources = self.lineage_map.get(target_key, [])
        
        if not sources:
            # No sources found - this might be a base table or final output
            visited.remove(target_key)
            if not target_table or target_table in self.base_tables:
                return [[target_key]]
            return []
        
        all_paths = []
        for source_table, source_column in sources:
            # Skip completely empty sources
            if not source_table and not source_column:
                continue
                
            source_key = (source_table, source_column)
            
            # Additional cycle check: don't revisit nodes we've seen in current path
            if source_key in visited:
                print(f"WARNING: Potential cycle from {target_key} to {source_key}, skipping")
                continue
                
            if source_table in self.base_tables:
                # Found base table - this is the start of the chain
                path = [source_key, target_key]
                all_paths.append(path)
            else:
                # Recursively trace upstream with current visited set
                upstream_paths = self.trace_lineage_chain(source_table, source_column, visited.copy())
                for upstream_path in upstream_paths:
                    # Extend path with current target
                    complete_path = upstream_path + [target_key]
                    all_paths.append(complete_path)
        
        visited.remove(target_key)
        return all_paths
    
    def get_final_outputs(self) -> List[Tuple[str, str]]:
        """
        Get final output columns.
        """
        final_outputs = []
        
        # Get all targets and track which ones are used as sources
        all_targets = {}  # (table, column) -> file
        used_as_source = set()  # (table, column)
        
        for record in self.records:
            if record.target_column:
                target_key = (record.target_table, record.target_column)
                all_targets[target_key] = record.file
                
                # Check if this record's source is from a created table
                if record.source_table in self.created_tables:
                    source_key = (record.source_table, record.source_column)
                    used_as_source.add(source_key)
        
        # Identify final outputs
        for (target_table, target_column), file in all_targets.items():
            target_key = (target_table, target_column)
            
            # SELECT statement outputs (no target_table)
            if not target_table:
                final_outputs.append(target_key)
            # Created table columns that are not used as sources for other created tables
            elif target_table in self.created_tables and target_key not in used_as_source:
                final_outputs.append(target_key)
        
        return final_outputs
    
    def create_lineage_steps(self, chain: List[Tuple[str, str]]) -> List[LineageStep]:
        """
        Convert a lineage chain into LineageStep objects with proper level numbering.
        """
        if len(chain) < 2:
            return []
        
        steps = []
        total_levels = len(chain)
        ultimate_source_table, ultimate_source_column = chain[0]
        ultimate_target_table, ultimate_target_column = chain[-1]
        
        # Create steps for each adjacent pair in the chain
        for i in range(len(chain) - 1):
            source_table, source_column = chain[i]
            target_table, target_column = chain[i + 1]
            
            step = LineageStep(
                source_table=source_table,
                source_column=source_column,
                target_table=target_table,
                target_column=target_column,
                ultimate_source_table=ultimate_source_table,
                ultimate_source_column=ultimate_source_column,
                ultimate_target_table=ultimate_target_table,
                ultimate_target_column=ultimate_target_column,
                level_top_to_bottom=i + 1,  # 1-based indexing from top
                level_bottom_to_top=total_levels - i - 1  # Count from bottom
            )
            steps.append(step)
        
        return steps
    
    def detect_cycles(self) -> List[List[Tuple[str, str]]]:
        """
        Detect cycles in the lineage graph using DFS.
        Returns list of cycles found.
        """
        def dfs_cycle_detection(node, visited, rec_stack, path, cycles):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in self.lineage_map.get(node, []):
                neighbor_key = neighbor
                if neighbor_key not in visited:
                    dfs_cycle_detection(neighbor_key, visited, rec_stack, path, cycles)
                elif neighbor_key in rec_stack:
                    # Found a cycle
                    cycle_start_idx = path.index(neighbor_key)
                    cycle = path[cycle_start_idx:] + [neighbor_key]
                    cycles.append(cycle)
            
            path.pop()
            rec_stack.remove(node)
        
        visited = set()
        cycles = []
        
        # Check all nodes for cycles
        all_nodes = set(self.lineage_map.keys())
        for source_list in self.lineage_map.values():
            all_nodes.update(source_list)
        
        for node in all_nodes:
            if node not in visited:
                dfs_cycle_detection(node, visited, set(), [], cycles)
        
        return cycles
    
    def process_lineage(self) -> List[LineageStep]:
        """Process the lineage data and return all lineage steps."""
        self.load_lineage_data()
        self.build_lineage_map()
        
        # Check for cycles before processing
        cycles = self.detect_cycles()
        if cycles:
            print(f"WARNING: Detected {len(cycles)} cycles in lineage graph:")
            for i, cycle in enumerate(cycles):
                cycle_str = " -> ".join([f"{t}.{c}" for t, c in cycle])
                print(f"  Cycle {i+1}: {cycle_str}")
            print("Continuing with cycle-safe processing...")
        
        all_steps = []
        final_outputs = self.get_final_outputs()
        
        # Trace lineage for each final output
        for target_table, target_column in final_outputs:
            chains = self.trace_lineage_chain(target_table, target_column)
            for chain in chains:
                if len(chain) >= 2:  # Only process chains with at least 2 nodes (source -> target)
                    steps = self.create_lineage_steps(chain)
                    all_steps.extend(steps)
        
        # Remove duplicates
        unique_steps = []
        seen_steps = set()
        for step in all_steps:
            step_key = step.to_csv_row()
            if step_key not in seen_steps:
                seen_steps.add(step_key)
                unique_steps.append(step)
        
        return unique_steps
    
    def write_steps_to_file(self, output_file: str, steps: List[LineageStep]) -> None:
        """Write lineage steps to output file."""
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write header
            f.write("source_table,source_column,target_table,target_column,ultimate_source_table,ultimate_source_column,ultimate_target_table,ultimate_target_column,level_top_to_bottom,level_bottom_to_top\n")
            
            # Write steps
            for step in steps:
                f.write(f"{step.to_csv_row()}\n")


def main():
    """Main function to process lineage chains."""
    if len(sys.argv) != 3:
        print("Usage: python lineage_chain_processor.py <input_csv> <output_csv>")
        sys.exit(1)
    
    input_csv = sys.argv[1]
    output_csv = sys.argv[2]
    
    processor = LineageChainProcessor(input_csv)
    steps = processor.process_lineage()
    
    if not steps:
        print("No lineage steps found.")
        return
    
    processor.write_steps_to_file(output_csv, steps)
    
    print(f"Processed {len(steps)} unique lineage steps")
    print(f"Output written to {output_csv}")
    
    # Print sample steps for verification
    print("\nSample lineage steps:")
    for i, step in enumerate(steps[:10]):  # Show first 10 steps
        print(f"Step {i+1}: {step.source_table}.{step.source_column} -> {step.target_table}.{step.target_column} (Level {step.level_top_to_bottom})")


if __name__ == "__main__":
    main()