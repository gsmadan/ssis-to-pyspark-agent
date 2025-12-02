#!/usr/bin/env python3
"""
Validation script to compare outputs with and without schema mapping.

This script helps verify that schema mapping is working correctly by:
1. Converting a package without schema mapping
2. Converting the same package with schema mapping
3. Showing the differences between the two outputs
"""

import sys
from pathlib import Path
from ssis_to_pyspark_app import SSISToPySparkApp
import difflib

def compare_outputs():
    """Compare outputs with and without schema mapping."""
    
    test_package = "input-sample packages/Sample_Simple_Package.dtsx"
    test_mapping = "test_schema_mapping.json"
    
    # Check if files exist
    if not Path(test_package).exists():
        print(f"❌ Test package not found: {test_package}")
        return
    
    if not Path(test_mapping).exists():
        print(f"❌ Schema mapping file not found: {test_mapping}")
        return
    
    print("="*60)
    print("Schema Mapping Comparison Test")
    print("="*60)
    
    # Convert without schema mapping
    print("\n1. Converting WITHOUT schema mapping...")
    try:
        app_no_mapping = SSISToPySparkApp(databricks_mode=True, schema_mapping_file=None)
        result_no_mapping = app_no_mapping.convert_single_package(test_package)
        
        if not result_no_mapping['success']:
            print(f"   ❌ Conversion failed: {result_no_mapping.get('error', 'Unknown error')}")
            return
        print(f"   ✅ Conversion successful: {result_no_mapping['pyspark_code']}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return
    
    # Convert with schema mapping
    print("\n2. Converting WITH schema mapping...")
    try:
        app_with_mapping = SSISToPySparkApp(databricks_mode=True, schema_mapping_file=test_mapping)
        result_with_mapping = app_with_mapping.convert_single_package(test_package)
        
        if not result_with_mapping['success']:
            print(f"   ❌ Conversion failed: {result_with_mapping.get('error', 'Unknown error')}")
            return
        print(f"   ✅ Conversion successful: {result_with_mapping['pyspark_code']}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return
    
    # Read both files
    print("\n3. Comparing outputs...")
    try:
        with open(result_no_mapping['pyspark_code'], 'r', encoding='utf-8') as f:
            code_no_mapping = f.read().splitlines()
        
        with open(result_with_mapping['pyspark_code'], 'r', encoding='utf-8') as f:
            code_with_mapping = f.read().splitlines()
        
        # Show differences
        print("-"*60)
        diff = difflib.unified_diff(
            code_no_mapping,
            code_with_mapping,
            fromfile='Without Mapping',
            tofile='With Mapping',
            lineterm='',
            n=3  # Show 3 lines of context
        )
        
        diff_lines = list(diff)
        changes_found = False
        
        if len(diff_lines) > 2:
            for line in diff_lines[2:]:  # Skip header
                if line.startswith('+') and not line.startswith('+++'):
                    print(f"  ✅ Added: {line[1:].strip()}")
                    changes_found = True
                elif line.startswith('-') and not line.startswith('---'):
                    print(f"  ❌ Removed: {line[1:].strip()}")
                    changes_found = True
                elif line.startswith('@'):
                    print(f"\n  📍 {line}")
        else:
            print("  ⚠️  No differences found (schema mapping may not have been applied)")
        
        if changes_found:
            print("\n" + "="*60)
            print("✅ Schema mapping differences detected!")
            print("   The generated code has been modified by schema mapping.")
        else:
            print("\n" + "="*60)
            print("⚠️  No differences found")
            print("   Schema mapping may not have been applied, or")
            print("   the package doesn't use the mapped connections/tables.")
        
        print("="*60)
        
    except Exception as e:
        print(f"   ❌ Error reading files: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    print("\n🔍 Schema Mapping Validation Test\n")
    compare_outputs()
    print("\n✅ Validation completed!\n")

