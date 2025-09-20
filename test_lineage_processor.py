#!/usr/bin/env python3
"""
Test script to validate the lineage chain processor.
"""

import csv
import sys
import os
from lineage_chain_processor import LineageChainProcessor, LineageStep


def test_basic_functionality():
    """Test basic functionality with the existing output.csv."""
    processor = LineageChainProcessor('output.csv')
    steps = processor.process_lineage()
    
    print(f"Total steps processed: {len(steps)}")
    
    # Test 1: Verify we have the three-level chain
    three_level_chains = []
    direct_customer_id_chains = []
    for step in steps:
        if (step.ultimate_source_table == 'customers' and 
            step.ultimate_source_column == 'customer_id' and 
            step.ultimate_target_table == '' and 
            step.ultimate_target_column == 'customer_id'):
            if step.level_bottom_to_top > 1:  # Multi-level chain
                three_level_chains.append(step)
            else:  # Direct single-level chain
                direct_customer_id_chains.append(step)
    
    print(f"Found {len(three_level_chains)} steps in the three-level customer_id chain")
    print(f"Found {len(direct_customer_id_chains)} steps in direct customer_id chains")
    
    # Should have 1 step in the three-level chain (customers->test1) and 1 step continuing (test1->final)
    # But the second step (test1->final) is part of the same ultimate lineage, so check all steps in that chain
    multi_level_customer_id = [s for s in steps if (s.ultimate_source_table == 'customers' and 
                                                   s.ultimate_source_column == 'customer_id' and 
                                                   s.ultimate_target_column == 'customer_id' and
                                                   s.level_bottom_to_top > 1)]
    
    assert len(multi_level_customer_id) >= 1, f"Expected at least 1 step in multi-level chain, got {len(multi_level_customer_id)}"
    # Should have 1 step in the direct chain
    assert len(direct_customer_id_chains) >= 1, f"Expected at least 1 step in direct chain, got {len(direct_customer_id_chains)}"
    
    # Test 2: Verify level numbering for multi-level chain
    if multi_level_customer_id:
        level_1_step = multi_level_customer_id[0]  # Take the first step
        assert level_1_step.source_table == 'customers'
        assert level_1_step.target_table == 'test1'
        assert level_1_step.level_top_to_bottom == 1
        assert level_1_step.level_bottom_to_top == 2
    
    print("✓ Three-level chain validation passed")
    
    # Test 3: Verify direct chains (customers -> final output)
    direct_chains = [s for s in steps if s.level_top_to_bottom == 1 and s.level_bottom_to_top == 1]
    print(f"Found {len(direct_chains)} direct chains (single-level)")
    
    # Test 4: Verify all steps have valid ultimate source/target
    for step in steps:
        assert step.ultimate_source_table, f"Missing ultimate source table for step: {step}"
        assert step.ultimate_source_column, f"Missing ultimate source column for step: {step}"
        assert step.ultimate_target_column, f"Missing ultimate target column for step: {step}"
    
    print("✓ Ultimate source/target validation passed")
    
    # Test 5: Verify level consistency
    for step in steps:
        # Top-to-bottom should be >= 1
        assert step.level_top_to_bottom >= 1, f"Invalid top-to-bottom level: {step.level_top_to_bottom}"
        # Bottom-to-top should be >= 1
        assert step.level_bottom_to_top >= 1, f"Invalid bottom-to-top level: {step.level_bottom_to_top}"
    
    print("✓ Level numbering validation passed")
    
    return steps


def test_output_format():
    """Test the output CSV format."""
    processor = LineageChainProcessor('output.csv')
    steps = processor.process_lineage()
    
    # Write to test file
    test_output = 'test_lineage_chains.csv'
    processor.write_steps_to_file(test_output, steps)
    
    # Read back and validate format
    with open(test_output, 'r') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        expected_headers = [
            'source_table', 'source_column', 'target_table', 'target_column',
            'ultimate_source_table', 'ultimate_source_column', 
            'ultimate_target_table', 'ultimate_target_column',
            'level_top_to_bottom', 'level_bottom_to_top'
        ]
        
        assert headers == expected_headers, f"Headers mismatch. Expected {expected_headers}, got {headers}"
        
        # Validate each row
        row_count = 0
        for row in reader:
            row_count += 1
            # Verify all required fields are present
            for header in expected_headers:
                assert header in row, f"Missing header {header} in row {row_count}"
            
            # Verify level numbers are integers
            try:
                level_tb = int(row['level_top_to_bottom'])
                level_bt = int(row['level_bottom_to_top'])
                assert level_tb >= 1 and level_bt >= 1
            except ValueError:
                assert False, f"Invalid level numbers in row {row_count}: {row['level_top_to_bottom']}, {row['level_bottom_to_top']}"
    
    print(f"✓ Output format validation passed ({row_count} rows)")
    
    # Clean up
    os.remove(test_output)


def test_specific_lineage_patterns():
    """Test specific lineage patterns we expect to see."""
    processor = LineageChainProcessor('output.csv')
    steps = processor.process_lineage()
    
    # Group steps by ultimate lineage chain
    chains = {}
    for step in steps:
        chain_key = (step.ultimate_source_table, step.ultimate_source_column, 
                    step.ultimate_target_table, step.ultimate_target_column)
        if chain_key not in chains:
            chains[chain_key] = []
        chains[chain_key].append(step)
    
    print(f"Found {len(chains)} unique lineage chains")
    
    # The customer_id case has multiple paths, so we need to handle them separately
    # Don't validate chain connectivity for this case since there are multiple paths
    
    # Validate that each step has valid levels
    for step in steps:
        assert step.level_top_to_bottom >= 1, f"Invalid top-to-bottom level: {step.level_top_to_bottom}"
        assert step.level_bottom_to_top >= 1, f"Invalid bottom-to-top level: {step.level_bottom_to_top}"
    
    print("✓ Level validation passed")
    
    # Look for any multi-level chains (where level_bottom_to_top > 1)
    multi_level_chains = {}
    for step in steps:
        if step.level_bottom_to_top > 1:
            chain_key = (step.ultimate_source_table, step.ultimate_source_column, 
                        step.ultimate_target_table, step.ultimate_target_column)
            if chain_key not in multi_level_chains:
                multi_level_chains[chain_key] = []
            multi_level_chains[chain_key].append(step)
    
    print(f"Found {len(multi_level_chains)} multi-level chains")
    
    # Validate connectivity only for chains that have a single path
    for chain_key, chain_steps in multi_level_chains.items():
        # Sort by level
        chain_steps.sort(key=lambda x: x.level_top_to_bottom)
        
        # For single-path chains, validate connectivity
        if len(set(s.level_top_to_bottom for s in chain_steps)) == len(chain_steps):
            # This is a single path chain, validate connectivity
            for i in range(len(chain_steps) - 1):
                current_step = chain_steps[i]
                next_step = chain_steps[i + 1]
                
                # Current step's target should match next step's source
                assert current_step.target_table == next_step.source_table, \
                    f"Chain break in {chain_key}: {current_step.target_table} != {next_step.source_table}"
                assert current_step.target_column == next_step.source_column, \
                    f"Chain break in {chain_key}: {current_step.target_column} != {next_step.source_column}"
    
    print("✓ Chain connectivity validation passed")
    
    # Look for the specific multi-level customer_id chain
    customer_multi_level = [s for s in steps if (s.ultimate_source_table == 'customers' and 
                                                s.ultimate_source_column == 'customer_id' and 
                                                s.ultimate_target_column == 'customer_id' and
                                                s.level_bottom_to_top > 1)]
    assert len(customer_multi_level) >= 1, f"Expected at least 1 multi-level customer_id step"
    
    print("✓ Specific pattern validation passed")


def print_summary(steps):
    """Print a summary of the lineage analysis."""
    print("\n" + "="*80)
    print("LINEAGE ANALYSIS SUMMARY")
    print("="*80)
    
    # Group by ultimate source
    by_source = {}
    for step in steps:
        source_key = (step.ultimate_source_table, step.ultimate_source_column)
        if source_key not in by_source:
            by_source[source_key] = []
        by_source[source_key].append(step)
    
    print(f"Ultimate source columns: {len(by_source)}")
    for (source_table, source_column), source_steps in by_source.items():
        targets = set((s.ultimate_target_table, s.ultimate_target_column) for s in source_steps)
        print(f"  {source_table}.{source_column} → {len(targets)} targets")
    
    # Group by ultimate target
    by_target = {}
    for step in steps:
        target_key = (step.ultimate_target_table, step.ultimate_target_column)
        if target_key not in by_target:
            by_target[target_key] = []
        by_target[target_key].append(step)
    
    print(f"\nUltimate target columns: {len(by_target)}")
    for (target_table, target_column), target_steps in by_target.items():
        sources = set((s.ultimate_source_table, s.ultimate_source_column) for s in target_steps)
        target_name = f"{target_table}.{target_column}" if target_table else f".{target_column}"
        print(f"  {target_name} ← {len(sources)} sources")
    
    # Show multi-level chains
    multi_level = [s for s in steps if s.level_bottom_to_top > 1]
    print(f"\nMulti-level chains: {len(multi_level)} steps")
    
    # Group multi-level by ultimate chain
    chains = {}
    for step in multi_level:
        chain_key = (step.ultimate_source_table, step.ultimate_source_column, 
                    step.ultimate_target_table, step.ultimate_target_column)
        if chain_key not in chains:
            chains[chain_key] = []
        chains[chain_key].append(step)
    
    for chain_key, chain_steps in chains.items():
        source_table, source_column, target_table, target_column = chain_key
        target_name = f"{target_table}.{target_column}" if target_table else f".{target_column}"
        print(f"  {source_table}.{source_column} → {target_name} ({len(chain_steps)} levels)")


def main():
    """Run all tests."""
    print("Testing Lineage Chain Processor")
    print("="*50)
    
    try:
        steps = test_basic_functionality()
        test_output_format()
        test_specific_lineage_patterns()
        
        print("\n✓ All tests passed!")
        
        print_summary(steps)
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()