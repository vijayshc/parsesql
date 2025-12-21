#!/usr/bin/env python3
"""
Comprehensive test suite for lineage chain processor with multiple test scenarios.
"""

import csv
import os
import sys
from lineage_chain_processor import LineageChainProcessor


def create_test_csv(filename: str, content: str):
    """Helper to create test CSV files."""
    with open(filename, 'w') as f:
        f.write("File_name,Target_table,Target_Column,Source_Table,Source_column,Expression,Trace_level,Lineage_type\n")
        f.write(content)


def test_scenario_1_simple_linear_chain():
    """Test 1: Simple linear chain A -> B -> C"""
    print("Test 1: Simple linear chain")
    content = """test1.sql,table_b,col1,source_a,col1,col1,0,SELECT
test1.sql,table_c,col1,table_b,col1,col1,0,SELECT
test1.sql,,final_col,table_c,col1,col1,0,SELECT"""
    
    create_test_csv('test1.csv', content)
    processor = LineageChainProcessor('test1.csv')
    steps = processor.process_lineage()
    
    # Should have 3 steps in the 4-level chain (source_a -> table_b -> table_c -> final_col)
    assert len(steps) == 3, f"Expected 3 steps, got {len(steps)}"
    
    # Verify the chain connectivity
    level1 = next(s for s in steps if s.level_top_to_bottom == 1)
    level2 = next(s for s in steps if s.level_top_to_bottom == 2)
    level3 = next(s for s in steps if s.level_top_to_bottom == 3)
    
    assert level1.source_table == 'source_a' and level1.target_table == 'table_b'
    assert level2.source_table == 'table_b' and level2.target_table == 'table_c'
    assert level3.source_table == 'table_c' and level3.target_table == ''
    assert level1.ultimate_source_table == 'source_a' and level3.ultimate_target_column == 'final_col'
    
    print("✓ Test 1 passed")
    os.remove('test1.csv')


def test_scenario_2_multiple_sources_single_target():
    """Test 2: Multiple sources feeding into single target"""
    print("Test 2: Multiple sources to single target")
    content = """test2.sql,,merged_col,source_a,col1,col1,0,SELECT
test2.sql,,merged_col,source_b,col2,col2,0,SELECT
test2.sql,,merged_col,source_c,col3,col3,0,SELECT"""
    
    create_test_csv('test2.csv', content)
    processor = LineageChainProcessor('test2.csv')
    steps = processor.process_lineage()
    
    # Should have 3 steps (one for each source)
    assert len(steps) == 3, f"Expected 3 steps, got {len(steps)}"
    
    # All should be single-level chains
    for step in steps:
        assert step.level_top_to_bottom == 1 and step.level_bottom_to_top == 1
        assert step.ultimate_target_column == 'merged_col'
    
    sources = {step.ultimate_source_table for step in steps}
    assert sources == {'source_a', 'source_b', 'source_c'}
    
    print("✓ Test 2 passed")
    os.remove('test2.csv')


def test_scenario_3_single_source_multiple_targets():
    """Test 3: Single source feeding multiple targets"""
    print("Test 3: Single source to multiple targets")
    content = """test3.sql,,target1,source_table,base_col,base_col,0,SELECT
test3.sql,,target2,source_table,base_col,base_col,0,SELECT
test3.sql,,target3,source_table,base_col,UPPER(base_col),0,SELECT"""
    
    create_test_csv('test3.csv', content)
    processor = LineageChainProcessor('test3.csv')
    steps = processor.process_lineage()
    
    # Should have 3 steps (one for each target)
    assert len(steps) == 3, f"Expected 3 steps, got {len(steps)}"
    
    # All should originate from same source
    for step in steps:
        assert step.ultimate_source_table == 'source_table'
        assert step.ultimate_source_column == 'base_col'
    
    targets = {step.ultimate_target_column for step in steps}
    assert targets == {'target1', 'target2', 'target3'}
    
    print("✓ Test 3 passed")
    os.remove('test3.csv')


def test_scenario_4_complex_branching():
    """Test 4: Complex branching - tree structure"""
    print("Test 4: Complex branching tree")
    content = """test4.sql,branch_a,col1,root_table,root_col,root_col,0,SELECT
test4.sql,branch_b,col1,root_table,root_col,root_col,0,SELECT
test4.sql,,final_a,branch_a,col1,col1,0,SELECT
test4.sql,,final_a_transformed,branch_a,col1,TRANSFORM(col1),0,SELECT
test4.sql,,final_b,branch_b,col1,col1,0,SELECT"""
    
    create_test_csv('test4.csv', content)
    processor = LineageChainProcessor('test4.csv')
    steps = processor.process_lineage()
    
    # Should have 6 steps total (2 for branch creation + 4 for final outputs)
    assert len(steps) == 6, f"Expected 6 steps, got {len(steps)}"
    
    # Check that all ultimate sources trace back to root_table
    for step in steps:
        assert step.ultimate_source_table == 'root_table'
        assert step.ultimate_source_column == 'root_col'
    
    print("✓ Test 4 passed")
    os.remove('test4.csv')


def test_scenario_5_deep_chain():
    """Test 5: Deep chain (5 levels)"""
    print("Test 5: Deep chain (5 levels)")
    content = """test5.sql,level2,col,level1,col,col,0,SELECT
test5.sql,level3,col,level2,col,col,0,SELECT
test5.sql,level4,col,level3,col,col,0,SELECT
test5.sql,level5,col,level4,col,col,0,SELECT
test5.sql,,final_col,level5,col,col,0,SELECT"""
    
    create_test_csv('test5.csv', content)
    processor = LineageChainProcessor('test5.csv')
    steps = processor.process_lineage()
    
    # Should have 5 steps in the 6-level chain
    assert len(steps) == 5, f"Expected 5 steps, got {len(steps)}"
    
    # Verify levels are numbered correctly
    steps_by_level = {step.level_top_to_bottom: step for step in steps}
    assert len(steps_by_level) == 5, "Should have 5 distinct levels"
    
    # Check level progression
    for i in range(1, 6):
        assert i in steps_by_level, f"Missing level {i}"
        step = steps_by_level[i]
        assert step.level_bottom_to_top == 6 - i, f"Wrong bottom-to-top level for level {i}"
    
    print("✓ Test 5 passed")
    os.remove('test5.csv')


def test_scenario_6_cycle_detection():
    """Test 6: Recursive dependency cycle"""
    print("Test 6: Cycle detection")
    content = """test6.sql,table_b,col1,table_a,col1,col1,0,SELECT
test6.sql,table_c,col1,table_b,col1,col1,0,SELECT
test6.sql,table_a,col1,table_c,col1,col1,0,SELECT
test6.sql,,col2,independent,col2,col2,0,SELECT"""
    
    create_test_csv('test6.csv', content)
    processor = LineageChainProcessor('test6.csv')
    steps = processor.process_lineage()
    
    # Should only process the independent column, cycles should be skipped
    assert len(steps) == 1, f"Expected 1 step (non-cyclic), got {len(steps)}"
    assert steps[0].ultimate_source_table == 'independent'
    
    print("✓ Test 6 passed")
    os.remove('test6.csv')


def test_scenario_7_diamond_pattern():
    """Test 7: Diamond dependency pattern"""
    print("Test 7: Diamond pattern")
    content = """test7.sql,left_table,left_col,source,col,col,0,SELECT
test7.sql,right_table,right_col,source,col,col,0,SELECT
test7.sql,,final_col,left_table,left_col,left_col,0,SELECT
test7.sql,,final_col,right_table,right_col,right_col,0,SELECT"""
    
    create_test_csv('test7.csv', content)
    processor = LineageChainProcessor('test7.csv')
    steps = processor.process_lineage()
    
    # Should have 4 steps: 2 for intermediate tables + 2 for final convergence
    assert len(steps) == 4, f"Expected 4 steps, got {len(steps)}"
    
    # All should trace back to same source
    for step in steps:
        assert step.ultimate_source_table == 'source'
        assert step.ultimate_source_column == 'col'
    
    # Should have both 1-level and 2-level paths to final_col
    final_steps = [s for s in steps if s.ultimate_target_column == 'final_col']
    assert len(final_steps) == 4, "Should have 4 steps leading to final_col (2 paths x 2 levels each)"
    
    print("✓ Test 7 passed")
    os.remove('test7.csv')


def test_scenario_8_no_ultimate_source():
    """Test 8: Columns with no clear ultimate source (constants)"""
    print("Test 8: Constants and derived columns")
    content = """test8.sql,,const_col,,,'constant_value',0,SELECT
test8.sql,,calculated_col,derived_table,col1,col1 + 100,0,SELECT"""
    
    create_test_csv('test8.csv', content)
    processor = LineageChainProcessor('test8.csv')
    steps = processor.process_lineage()
    
    # Should handle the derived column, constants might be skipped
    derived_steps = [s for s in steps if s.ultimate_target_column == 'calculated_col']
    assert len(derived_steps) >= 1, "Should have lineage for calculated column"
    
    print("✓ Test 8 passed")
    os.remove('test8.csv')


def test_scenario_9_mixed_patterns():
    """Test 9: Mixed patterns in same dataset"""
    print("Test 9: Mixed patterns")
    content = """test9.sql,intermediate,col1,base1,col1,col1,0,SELECT
test9.sql,intermediate,col2,base2,col2,col2,0,SELECT
test9.sql,,final1,intermediate,col1,col1,0,SELECT
test9.sql,,final2,intermediate,col2,col2,0,SELECT
test9.sql,,direct_final,base3,col3,col3,0,SELECT"""
    
    create_test_csv('test9.csv', content)
    processor = LineageChainProcessor('test9.csv')
    steps = processor.process_lineage()
    
    # Should have multiple patterns: 2-level chains and 1-level direct
    assert len(steps) >= 5, f"Expected at least 5 steps, got {len(steps)}"
    
    # Check we have both direct and indirect paths
    levels = {step.level_top_to_bottom for step in steps}
    assert 1 in levels and 2 in levels, "Should have both 1-level and 2-level paths"
    
    print("✓ Test 9 passed")
    os.remove('test9.csv')


def test_scenario_10_empty_values():
    """Test 10: Empty/null values handling"""
    print("Test 10: Empty values handling")
    content = """test10.sql,,target1,source_table,col1,col1,0,SELECT
test10.sql,,target2,,col2,col2,0,SELECT
test10.sql,,target3,source_table,,expression,0,SELECT"""
    
    create_test_csv('test10.csv', content)
    processor = LineageChainProcessor('test10.csv')
    steps = processor.process_lineage()
    
    # Should handle valid entries and skip invalid ones gracefully
    valid_steps = [s for s in steps if s.ultimate_source_table and s.ultimate_source_column]
    assert len(valid_steps) >= 1, "Should have at least one valid lineage step"
    
    print("✓ Test 10 passed")
    os.remove('test10.csv')


def test_scenario_11_large_scale():
    """Test 11: Large scale test (performance and correctness)"""
    print("Test 11: Large scale test")
    
    # Generate a large test case
    lines = []
    # Create a fan-out pattern: 1 source -> 10 intermediate -> 100 final
    for i in range(10):
        lines.append(f"test11.sql,intermediate{i},col{i},source,base_col,base_col,0,SELECT")
    
    for i in range(10):
        for j in range(10):
            final_idx = i * 10 + j
            lines.append(f"test11.sql,,final{final_idx},intermediate{i},col{i},col{i},0,SELECT")
    
    content = "\n".join(lines)
    create_test_csv('test11.csv', content)
    
    processor = LineageChainProcessor('test11.csv')
    steps = processor.process_lineage()
    
    # Should have 200 steps total: 100 level-1 (source->intermediate) + 100 level-2 (intermediate->final)
    # This is correct because each final output has a 2-step lineage chain
    assert len(steps) == 200, f"Expected 200 steps, got {len(steps)}"
    
    # All should trace back to same source
    for step in steps:
        assert step.ultimate_source_table == 'source'
        assert step.ultimate_source_column == 'base_col'
    
    # Should have both level 1 and level 2 steps
    levels = {step.level_top_to_bottom for step in steps}
    assert levels == {1, 2}, f"Expected levels 1 and 2, got {levels}"
    
    print("✓ Test 11 passed")
    os.remove('test11.csv')


def test_scenario_12_self_reference():
    """Test 12: Self-referencing table (should be handled gracefully)"""
    print("Test 12: Self-reference handling")
    content = """test12.sql,self_ref_table,col1,base_table,col1,col1,0,SELECT
test12.sql,self_ref_table,col1,self_ref_table,col1,col1 + 1,0,SELECT
test12.sql,,final_col,self_ref_table,col1,col1,0,SELECT"""
    
    create_test_csv('test12.csv', content)
    processor = LineageChainProcessor('test12.csv')
    steps = processor.process_lineage()
    
    # Should detect self-reference cycle and handle gracefully
    # Final output should still trace back to base_table
    final_steps = [s for s in steps if s.ultimate_target_column == 'final_col']
    assert len(final_steps) >= 1, "Should have lineage for final column"
    
    print("✓ Test 12 passed")
    os.remove('test12.csv')


def run_all_tests():
    """Run all test scenarios."""
    print("Running Comprehensive Lineage Processor Test Suite")
    print("=" * 60)
    
    test_functions = [
        test_scenario_1_simple_linear_chain,
        test_scenario_2_multiple_sources_single_target,
        test_scenario_3_single_source_multiple_targets,
        test_scenario_4_complex_branching,
        test_scenario_5_deep_chain,
        test_scenario_6_cycle_detection,
        test_scenario_7_diamond_pattern,
        test_scenario_8_no_ultimate_source,
        test_scenario_9_mixed_patterns,
        test_scenario_10_empty_values,
        test_scenario_11_large_scale,
        test_scenario_12_self_reference,
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_functions:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"✗ {test_func.__name__} failed: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Test Results: {passed} passed, {failed} failed")
    
    if failed > 0:
        print("Some tests failed. Please review the issues above.")
        sys.exit(1)
    else:
        print("🎉 All tests passed! The lineage processor is working correctly.")


if __name__ == "__main__":
    run_all_tests()