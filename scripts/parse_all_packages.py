"""
Parse all SSIS packages in the input directory using the Data Engineering Parser.
Outputs are named based on the input file names.
"""
import json
import os
from pathlib import Path
from parsing.data_engineering_parser import DataEngineeringParser


def parse_package(input_file: str, output_dir: str = "output"):
    """Parse a single package and save the output."""
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get input file name without extension
    input_path = Path(input_file)
    base_name = input_path.stem
    
    # Create output file name
    output_file = os.path.join(output_dir, f"{base_name}_data_engineering.json")
    
    print(f"\n{'='*80}")
    print(f"Processing: {input_file}")
    print(f"{'='*80}")
    
    try:
        # Parse the package
        parser = DataEngineeringParser()
        result = parser.parse_dtsx_file(input_file)
        
        # Save to JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, default=str)
        
        # Display summary
        print(f"\n✓ Package: {result['package_name']}")
        print(f"  Connections: {len(result['connections'])}")
        print(f"  SQL Tasks: {len(result['sql_tasks'])}")
        print(f"  Data Flows: {len(result['data_flows'])}")
        
        # Show data flow details
        for i, df in enumerate(result['data_flows'], 1):
            print(f"\n  Data Flow {i}: {df['name']}")
            print(f"    Sources: {len(df['sources'])}")
            for src in df['sources']:
                if src.get('table'):
                    table_ref = f"{src.get('schema', '')}.{src['table']}" if src.get('schema') else src['table']
                    print(f"      • {src['name']}: {table_ref}")
                elif src.get('table_name'):
                    print(f"      • {src['name']}: {src['table_name']}")
            
            print(f"    Transformations: {len(df['transformations'])}")
            for trans in df['transformations']:
                print(f"      • {trans['name']} ({trans['type']})")
            
            print(f"    Destinations: {len(df['destinations'])}")
            for dest in df['destinations']:
                if dest.get('table'):
                    table_ref = f"{dest.get('schema', '')}.{dest['table']}" if dest.get('schema') else dest['table']
                    print(f"      • {dest['name']}: {table_ref}")
                elif dest.get('target_name'):
                    print(f"      • {dest['name']}: {dest['target_name']}")
        
        print(f"\n✓ Output saved to: {output_file}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error parsing {input_file}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Parse all DTSX files in the input directory."""
    
    input_dir = "input"
    output_dir = "output"
    
    print("="*80)
    print("DATA ENGINEERING PARSER - BATCH PROCESSING")
    print("="*80)
    
    # Find all .dtsx files
    dtsx_files = list(Path(input_dir).glob("*.dtsx"))
    
    if not dtsx_files:
        print(f"\n✗ No .dtsx files found in {input_dir}/")
        return
    
    print(f"\nFound {len(dtsx_files)} package(s) to process:")
    for f in dtsx_files:
        print(f"  • {f.name}")
    
    # Process each file
    success_count = 0
    for dtsx_file in dtsx_files:
        if parse_package(str(dtsx_file), output_dir):
            success_count += 1
    
    # Summary
    print("\n" + "="*80)
    print("BATCH PROCESSING COMPLETE")
    print("="*80)
    print(f"\nSuccessfully processed: {success_count}/{len(dtsx_files)} packages")
    print(f"Output directory: {output_dir}/")
    print("\nGenerated files:")
    
    for dtsx_file in dtsx_files:
        output_file = f"{dtsx_file.stem}_data_engineering.json"
        output_path = Path(output_dir) / output_file
        if output_path.exists():
            size = output_path.stat().st_size
            print(f"  ✓ {output_file} ({size} bytes)")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()

