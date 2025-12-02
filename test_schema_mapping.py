#!/usr/bin/env python3
"""
Quick test script for schema mapping functionality.

This script tests the schema mapping feature by:
1. Loading a schema mapping file
2. Converting a sample SSIS package
3. Verifying that schema mappings are applied in the generated code
"""

import sys
from pathlib import Path
from ssis_to_pyspark_app import SSISToPySparkApp

def test_schema_mapping():
    """Test schema mapping functionality."""
    
    # Test files
    test_package = "input-sample packages/Sample_Simple_Package.dtsx"
    test_mapping = "test_schema_mapping.json"
    
    # Check if files exist
    if not Path(test_package).exists():
        print(f"❌ Test package not found: {test_package}")
        print("   Please ensure the file exists in the correct location")
        return False
    
    if not Path(test_mapping).exists():
        print(f"❌ Schema mapping file not found: {test_mapping}")
        print("   Please create the test_schema_mapping.json file first")
        return False
    
    print("="*60)
    print("Schema Mapping Test")
    print("="*60)
    
    # Initialize with schema mapping
    print(f"\n1. Initializing converter with schema mapping...")
    app = SSISToPySparkApp(
        databricks_mode=True,
        schema_mapping_file=test_mapping
    )
    
    # Check if mapping loaded
    if app.schema_mapper and app.schema_mapper.is_active():
        print(f"   ✅ Schema mapping loaded successfully!")
        print(f"   📊 Mapped connections: {len(app.schema_mapper.connection_mappings)}")
        for conn_name, mapping in app.schema_mapper.connection_mappings.items():
            schema = mapping.get('databricks_schema', 'N/A')
            table_count = len(mapping.get('table_mappings', {}))
            print(f"      - {conn_name} -> {schema} ({table_count} table(s))")
    else:
        print("   ❌ Schema mapping not loaded")
        return False
    
    # Convert package
    print(f"\n2. Converting package: {Path(test_package).name}")
    try:
        result = app.convert_single_package(test_package)
        
        if result['success']:
            print(f"   ✅ Conversion successful!")
            print(f"   📄 Output file: {result['pyspark_code']}")
            
            # Check if schema mapping was applied
            print(f"\n3. Verifying schema mapping in generated code...")
            with open(result['pyspark_code'], 'r', encoding='utf-8') as f:
                code = f.read()
                
            # Check for mapped schema names
            expected_schemas = ['test_bronze', 'test_silver']
            found_schemas = [s for s in expected_schemas if s in code]
            
            # Check for original schema names (should be replaced)
            original_schemas = ['dbo.SRC_InputTable', 'dbo.SRC_GenericTable']
            found_original = [s for s in original_schemas if s in code]
            
            if found_schemas:
                print(f"   ✅ Found mapped schemas: {', '.join(found_schemas)}")
            else:
                print(f"   ⚠️  No mapped schemas found in code")
            
            if found_original:
                print(f"   ⚠️  Found original schema references: {', '.join(found_original)}")
                print(f"      (These should have been replaced by schema mapping)")
            else:
                print(f"   ✅ Original schema references replaced")
            
            # Show sample of mapped code
            print(f"\n4. Sample of generated code:")
            lines = code.split('\n')
            mapped_lines = [line for line in lines if any(s in line for s in expected_schemas)]
            if mapped_lines:
                for line in mapped_lines[:5]:  # Show first 5 matches
                    print(f"      {line.strip()}")
            else:
                print(f"      (No mapped schema references found)")
            
            print(f"\n" + "="*60)
            print("Test Summary:")
            print(f"  ✅ Schema mapping loaded: {len(app.schema_mapper.connection_mappings)} connections")
            print(f"  ✅ Package converted successfully")
            print(f"  {'✅' if found_schemas and not found_original else '⚠️ '} Schema mapping applied in code")
            print("="*60)
            
            return True
        else:
            print(f"   ❌ Conversion failed: {result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_medium_package():
    """Test schema mapping with medium complexity package."""
    
    test_package = "input-sample packages/Sample_Medium_Package.dtsx"
    test_mapping = "test_schema_mapping.json"
    
    if not Path(test_package).exists():
        print(f"\n⚠️  Medium package not found: {test_package}")
        print("   Skipping medium package test")
        return
    
    print("\n" + "="*60)
    print("Testing Medium Package with Schema Mapping")
    print("="*60)
    
    app = SSISToPySparkApp(
        databricks_mode=True,
        schema_mapping_file=test_mapping
    )
    
    print(f"\nConverting: {Path(test_package).name}")
    try:
        result = app.convert_single_package(test_package)
        
        if result['success']:
            print(f"✅ Conversion successful!")
            
            # Check for lookup table mappings
            with open(result['pyspark_code'], 'r', encoding='utf-8') as f:
                code = f.read()
            
            # Check for lookup table mappings
            lookup_tables = ['lkp_category_table', 'lkp_country_table', 'lkp_local_category_table']
            found_lookups = [t for t in lookup_tables if t in code]
            
            if found_lookups:
                print(f"✅ Found mapped lookup tables: {', '.join(found_lookups)}")
            else:
                print(f"⚠️  Lookup table mappings not found")
        else:
            print(f"❌ Conversion failed: {result.get('error', 'Unknown error')}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == '__main__':
    print("\n🧪 Schema Mapping Test Suite\n")
    
    # Test simple package
    success = test_schema_mapping()
    
    # Test medium package if simple package test passed
    if success:
        test_medium_package()
    
    print("\n✅ Test completed!\n")

