System:
You are an expert Microsoft SSIS package developer who deeply understands data integration, ETL design, and DTSX XML schema. You can analyze existing packages to infer their control flow, data flow, and transformation logic, and then recreate functionally equivalent anonymized packages that maintain structure but remove all client-specific details.

User:
Task: Build two sample SSIS packages (.dtsx files).

Purpose:
We need these packages to demonstrate the capabilities of our SSIS → PySpark converter, using files that structurally resemble the client’s “BrokerGroupings” (simple) and “OECD” (medium) packages, but contain no client-specific information.

Instructions:
1. Analyze the reference .dtsx files inside the `input-packages` folder (BrokerGroupings and OECD).  
   - Understand their control flow, data flow, and logical data movement.  
   - Do not copy any sensitive names, table names, or connection details.

2. Based on your analysis, generate two new .dtsx files:
   - **Package 1 – Simple Complexity**
     - Control Flow:
       1. Execute SQL Task – TRUNCATE statement
       2. Data Flow Task – Load Data
       3. Execute SQL Task – Add defaults
     - Data Flow:
       1. Source: OLE_SRC → SQL SELECT
       2. Transformation: RC Insert
       3. Destination: Write to a Databricks table (use placeholder connection name)
     - Use generic names for tables, variables, and connections (e.g., `SRC_GenericTable`, `DFT_Load`, `DBX_Output`).

   - **Package 2 – Medium Complexity**
     - Control Flow: Follow the same structure as the OECD reference (or, if unclear, ask for clarification from Deepti).
     - Data Flow (in order):
       1. SQL Check Regulatory RowCount
       2. DFT Load
       3. SQL Set variable via stored procedure
       4. OLE_SRC (multiple sources)
       5. RC Select
       6. LKP_1, LKP_2, LKP_3
       7. Sort
       8. Derived Column
       9. Checksum
       10. Row Count SELECT
       11. Merge Join
       12. RC Intermediate
       13. Derived Column 1 + 2
       14. Conditional Split
       15. Row Count INSERT
       16. RC Update
       17. OLE DB Command (SP call)
       18. RC Delete
       19. OLE DB Command (SP call)
       20. RC Intermediate + Trash Destinations
       21. Final Destination: Write to a Databricks table
     - All object names must be generic (no client-specific references).

3. Output both .dtsx files in the `/output` folder:
   - `Sample_Simple_Package.dtsx`
   - `Sample_Medium_Package.dtsx`

Rules:
- ❌ No client names, table names, schema names, or environment-specific variables.
- ✅ Use neutral placeholders like `SRC_InputTable`, `LKP_CustomerRef`, `VAR_RowCount`, etc.
- ✅ Keep internal SSIS XML syntax valid for direct import into Visual Studio / SQL Data Tools.
- ✅ Maintain logical and structural fidelity with the original reference packages.

Goal:
The generated `.dtsx` packages should open correctly in SSIS Designer and demonstrate realistic ETL complexity for the SSIS → PySpark converter demo.

