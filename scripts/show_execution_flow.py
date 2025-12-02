"""
Script to display execution flow for all SSIS packages.
Shows both the parsed control flow and the generated PySpark step order.
"""
import os
import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parsing.data_engineering_parser import DataEngineeringParser


def show_execution_flow(dtsx_file):
    """Display execution flow for a single SSIS package."""
    filename = os.path.basename(dtsx_file)
    print(f"\n{'='*100}")
    print(f"FILE: {filename}")
    print(f"{'='*100}")
    
    try:
        # Parse DTSX
        parser = DataEngineeringParser()
        parsed_data = parser.parse_dtsx_file(dtsx_file)
        
        # Display package info
        print(f"\nPackage Name: {parsed_data['package_name']}")
        print(f"SQL Tasks: {len(parsed_data['sql_tasks'])}")
        print(f"Data Flows: {len(parsed_data['data_flows'])}")
        print(f"Precedence Constraints: {len(parsed_data['execution_order'])}")
        
        # Display all tasks with their IDs
        print(f"\n{'-'*100}")
        print("ALL TASKS (with Task IDs):")
        print(f"{'-'*100}")
        
        print("\nSQL TASKS:")
        if parsed_data['sql_tasks']:
            for i, task in enumerate(parsed_data['sql_tasks'], 1):
                task_id = task.get('task_id', 'NO_ID')
                task_name = task['name']
                purpose = task.get('purpose', 'UNKNOWN')
                print(f"  {i}. ID: {task_id}")
                print(f"     Name: {task_name}")
                print(f"     Purpose: {purpose}")
        else:
            print("  (None)")
        
        print("\nDATA FLOW TASKS:")
        if parsed_data['data_flows']:
            for i, df in enumerate(parsed_data['data_flows'], 1):
                task_id = df.get('task_id', 'NO_ID')
                task_name = df['name']
                sources = len(df.get('sources', []))
                transforms = len(df.get('transformations', []))
                dests = len(df.get('destinations', []))
                print(f"  {i}. ID: {task_id}")
                print(f"     Name: {task_name}")
                print(f"     Components: {sources} source(s), {transforms} transformation(s), {dests} destination(s)")
        else:
            print("  (None)")
        
        # Display execution order from SSIS
        print(f"\n{'-'*100}")
        print("SSIS CONTROL FLOW (Precedence Constraints):")
        print(f"{'-'*100}")
        
        if parsed_data['execution_order']:
            for i, flow in enumerate(parsed_data['execution_order'], 1):
                from_name = flow['from']
                to_name = flow['to']
                from_id = flow['from_id']
                to_id = flow['to_id']
                
                print(f"\n  Constraint {i}:")
                print(f"    FROM: {from_name}")
                print(f"          ID: {from_id}")
                print(f"    TO:   {to_name}")
                print(f"          ID: {to_id}")
        else:
            print("\n  (No precedence constraints - tasks may run in parallel)")
        
        # Build expected execution order
        print(f"\n{'-'*100}")
        print("EXPECTED EXECUTION ORDER:")
        print(f"{'-'*100}")
        
        if parsed_data['execution_order']:
            expected_order = []
            processed = set()
            
            for flow in parsed_data['execution_order']:
                if flow['from'] not in processed:
                    expected_order.append(flow['from'])
                    processed.add(flow['from'])
                if flow['to'] not in processed:
                    expected_order.append(flow['to'])
                    processed.add(flow['to'])
            
            # Add any tasks not in execution flow
            for task in parsed_data['sql_tasks']:
                if task['name'] not in processed:
                    expected_order.append(task['name'])
                    processed.add(task['name'])
            
            for df in parsed_data['data_flows']:
                if df['name'] not in processed:
                    expected_order.append(df['name'])
                    processed.add(df['name'])
            
            for i, task_name in enumerate(expected_order, 1):
                print(f"  {i}. {task_name}")
        else:
            print("\n  (No specific order - all tasks independent)")
        
        # Check generated PySpark file (try both naming conventions)
        pyspark_file = f"output/pyspark_code/{Path(dtsx_file).stem}_pyspark_mapped.py"
        if not os.path.exists(pyspark_file):
            pyspark_file = f"output/pyspark_code/{Path(dtsx_file).stem}_pyspark.py"
        
        if os.path.exists(pyspark_file):
            print(f"\n{'-'*100}")
            print("GENERATED PYSPARK EXECUTION ORDER:")
            print(f"{'-'*100}")
            
            with open(pyspark_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            generated_steps = []
            for line in lines:
                if line.strip().startswith('# Step') and ':' in line:
                    # Extract step number and task name
                    step_line = line.strip()
                    generated_steps.append(step_line)
            
            if generated_steps:
                for step in generated_steps:
                    print(f"  {step}")
            else:
                print("\n  (No steps found - might be using legacy format)")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR]: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Show execution flow for all SSIS packages."""
    input_dir = Path("input")
    dtsx_files = sorted(list(input_dir.glob("*.dtsx")))
    
    print("\n" + "="*100)
    print(f"EXECUTION FLOW ANALYSIS FOR {len(dtsx_files)} SSIS PACKAGES")
    print("="*100)
    
    for dtsx_file in dtsx_files:
        show_execution_flow(str(dtsx_file))
    
    print("\n" + "="*100)
    print("ANALYSIS COMPLETE")
    print("="*100)
    print("\nPlease validate each execution flow against the SSIS Designer control flow view.")
    print("="*100 + "\n")


if __name__ == '__main__':
    main()



