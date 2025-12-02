# Databricks Testing Guide

## Overview

This guide helps you test the generated PySpark code in your Databricks environment and compare results with SSIS execution.

---

## 📋 Prerequisites

### Databricks Environment
- Azure Databricks workspace access
- Databricks cluster (DBR 10.4+ recommended)
- Appropriate permissions to create notebooks/jobs

### Data Sources
- Access to the same data sources as SSIS
- JDBC drivers installed (for SQL Server connections)
- Credentials for data sources

### SSIS Baseline
- SSIS package execution results (for comparison)
- Expected row counts
- Sample output data (CSV or table)

---

## 🚀 Step-by-Step Testing Process

### Step 1: Prepare the Generated PySpark Code

1. **Locate the generated code**:
   ```
   output/pyspark_code/<YourPackage>_pyspark.py
   ```

2. **Review the code**:
   - Check connection configurations
   - Identify TODOs
   - Note any join conditions to specify

3. **Update placeholders**:
   ```python
   # BEFORE:
   username = "username"  # TODO: Set actual username
   password = "password"  # TODO: Set actual password
   
   # AFTER (using Databricks secrets - recommended):
   username = dbutils.secrets.get(scope="my_scope", key="sql_username")
   password = dbutils.secrets.get(scope="my_scope", key="sql_password")
   
   # OR (for testing only):
   username = "actual_user"
   password = "actual_password"
   ```

### Step 2: Upload to Databricks

#### Option A: Create Databricks Notebook

1. Go to Databricks workspace
2. Click "Create" → "Notebook"
3. Name: `<YourPackage>_Converted`
4. Copy-paste the generated PySpark code
5. Language: Python

#### Option B: Import as File

1. In Databricks workspace
2. Click "Workspace" → "Import"
3. Select the generated `.py` file
4. Databricks will create a notebook

#### Option C: Upload via Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure
databricks configure --token

# Upload notebook
databricks workspace import output/pyspark_code/package_pyspark.py \
  /Users/your.email@company.com/converted_packages/package_pyspark \
  --language PYTHON
```

### Step 3: Configure Databricks Cluster

**Recommended Configuration:**
```
Cluster Mode: Standard
Databricks Runtime: 11.3 LTS or higher
Worker Type: Standard_DS3_v2 (or similar)
Workers: 2-4 (adjust based on data size)
Auto-scaling: Enabled
```

**Required Libraries:**
```
# If not already installed on cluster:
# Most PySpark functions are built-in
# Only add if your package uses specific connectors
```

**For SQL Server Connections:**
```
# Install JDBC driver
# Maven coordinates: com.microsoft.sqlserver:mssql-jdbc:9.4.0.jre8
```

### Step 4: Update Connection Details

**SQL Server Connections:**
```python
# Update JDBC URLs
DESKTOP_HREK7MN_ASS3DB_url = "jdbc:sqlserver://DESKTOP-HREK7MN:1433;databaseName=ASS3DB"

# Change to your actual server (if different):
production_server_url = "jdbc:sqlserver://your-server.database.windows.net:1433;databaseName=YourDB"

# Use Databricks secrets for credentials:
username = dbutils.secrets.get(scope="sql_scope", key="username")
password = dbutils.secrets.get(scope="sql_scope", key="password")
```

**File Paths:**
```python
# If SSIS used local files, update to Azure storage:
# BEFORE:
file_path = "C:\\data\\input.csv"

# AFTER:
file_path = "/mnt/datalake/input.csv"
# or
file_path = "abfss://container@storage.dfs.core.windows.net/input.csv"
```

### Step 5: Run and Test

#### First Test Run (Dry Run)

1. **Comment out write operations** initially:
   ```python
   # df.write.mode("overwrite").saveAsTable("target_table")  # Commented for test
   
   # Instead, just show results:
   display(df)
   print(f"Row count: {df.count()}")
   df.printSchema()
   ```

2. **Run the notebook**:
   - Click "Run All"
   - Watch for errors
   - Check DataFrame outputs

3. **Verify data**:
   - Check row counts
   - Inspect sample rows
   - Verify column names and types

#### Second Test Run (With Writes)

1. **Uncomment write operations** (or write to test location):
   ```python
   # Write to test table first
   df.write.mode("overwrite").saveAsTable("test_schema.test_table")
   ```

2. **Run and verify**:
   ```sql
   SELECT COUNT(*) FROM test_schema.test_table
   SELECT * FROM test_schema.test_table LIMIT 10
   ```

---

## 🔍 Comparison Testing

### Test 1: Row Count Comparison

**In SSIS (SQL Server):**
```sql
-- Get row counts from SSIS results
SELECT COUNT(*) as ssis_count FROM YourTargetTable
```

**In Databricks:**
```python
# Get row count from PySpark execution
databricks_count = spark.sql("SELECT COUNT(*) FROM YourTargetTable").collect()[0][0]
print(f"Databricks count: {databricks_count}")
```

**Compare:**
```python
# Using the comparison framework
from test_results_comparison import ResultsComparator

comparator = ResultsComparator()
comparator.compare_row_counts_only(
    ssis_count=1000,
    databricks_count=1000,
    test_name="YourTable Comparison"
)
```

### Test 2: Data Sample Comparison

**Export from SSIS:**
```sql
-- Export SSIS results to CSV
SELECT TOP 1000 * FROM YourTargetTable
ORDER BY id
-- Save as ssis_results.csv
```

**Export from Databricks:**
```python
# Export Databricks results to CSV
df.orderBy("id").limit(1000).toPandas().to_csv("/dbfs/databricks_results.csv", index=False)
```

**Compare:**
```python
comparator.compare_csv_results(
    ssis_csv="ssis_results.csv",
    databricks_csv="databricks_results.csv",
    test_name="Data Content Comparison"
)
```

### Test 3: Aggregation Comparison

**If your package includes aggregations:**

```sql
-- SSIS aggregation results
SELECT 
    category,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM YourTable
GROUP BY category
```

```python
# Databricks equivalent
spark.sql("""
SELECT 
    category,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM YourTable
GROUP BY category
""").show()
```

Compare the results manually or export both to CSV for automated comparison.

---

## 🔧 Common Adjustments Needed

### 1. Join Conditions

**Generated code:**
```python
# TODO: Specify join condition based on lookup column mappings
result_df = df.join(
    lookup_df,
    # JOIN_CONDITION_HERE,  # e.g., df.id == lookup_df.id
    "left"
)
```

**Your update:**
```python
result_df = df.join(
    lookup_df,
    df["EmployeeID"] == lookup_df["ID"],  # Actual join condition
    "left"
)
```

### 2. Date/Time Functions

**SSIS `GETDATE()` is translated to:**
```python
df = df.withColumn("timestamp", current_timestamp())
```

**If you need specific timezone:**
```python
from pyspark.sql.functions import current_timestamp
df = df.withColumn("timestamp", current_timestamp())  # UTC by default

# Or with timezone:
df = df.withColumn("timestamp", 
    from_utc_timestamp(current_timestamp(), "America/New_York"))
```

### 3. Data Type Conversions

**If SSIS had specific data type conversions:**
```python
# Check generated code for data conversions
# Verify they match SSIS expectations

# Example:
df = df.withColumn("amount", col("amount").cast("decimal(10,2)"))
df = df.withColumn("date", col("date").cast("date"))
```

---

## 📊 Validation Checklist

### Pre-Execution Validation
- [ ] Reviewed generated PySpark code
- [ ] Updated all connection credentials
- [ ] Specified all join conditions
- [ ] Updated file paths for Azure storage
- [ ] Commented out writes for first test

### Execution Validation
- [ ] Code runs without errors
- [ ] All DataFrames created successfully
- [ ] Transformations produce expected results
- [ ] No null/missing data issues

### Results Validation
- [ ] Row counts match SSIS (+/- acceptable margin)
- [ ] Column names match
- [ ] Data types appropriate
- [ ] Sample data comparison passes
- [ ] Aggregations match

### Performance Validation
- [ ] Execution time acceptable
- [ ] Memory usage reasonable
- [ ] No OOM errors
- [ ] Scalable to production data volumes

---

## 🐛 Troubleshooting

### Issue: JDBC Connection Fails

**Symptoms:**
```
java.sql.SQLException: No suitable driver found
```

**Solution:**
```python
# Ensure JDBC driver is installed on cluster
# Cluster > Libraries > Install New
# Maven: com.microsoft.sqlserver:mssql-jdbc:9.4.0.jre8
```

### Issue: Row Count Mismatch

**Possible Causes:**
1. Different filter conditions
2. Timezone differences in date comparisons
3. NULL handling differences
4. Data in source changed between runs

**Investigation:**
```python
# Check intermediate DataFrames
df.show(10)  # See sample
df.count()   # Check count at each step
df.printSchema()  # Verify structure
```

### Issue: Column Not Found

**Symptoms:**
```
AnalysisException: Column 'xyz' not found
```

**Solutions:**
1. Check column name case sensitivity
2. Verify column exists in source
3. Check if column is derived correctly
4. Review transformation logic

### Issue: Performance Slow

**Solutions:**
```python
# Add caching for reused DataFrames
df.cache()

# Optimize joins
df.repartition("key_column")

# Use broadcast for small lookup tables
from pyspark.sql.functions import broadcast
result = df.join(broadcast(small_lookup), "key")
```

---

## 📈 Performance Optimization Tips

### For Large Data Volumes

**1. Partitioning:**
```python
# Read with partitioning
df = spark.read \
    .format("jdbc") \
    .option("partitionColumn", "id") \
    .option("lowerBound", 1) \
    .option("upperBound", 1000000) \
    .option("numPartitions", 10) \
    .load()
```

**2. Caching:**
```python
# Cache frequently accessed DataFrames
lookup_df.cache()
```

**3. Broadcast Joins:**
```python
# For small dimension tables
from pyspark.sql.functions import broadcast
fact_df.join(broadcast(dim_df), "key")
```

---

## 📝 Testing Template

Use this template for documenting your tests:

```markdown
# Test: [Package Name]

## SSIS Results
- Execution Time: ___ minutes
- Rows Processed: ___ rows
- Output Row Count: ___ rows
- Issues: None / [List any issues]

## Databricks Results
- Execution Time: ___ minutes
- Rows Processed: ___ rows
- Output Row Count: ___ rows
- Issues: None / [List any issues]

## Comparison
- Row Count Match: Yes / No (Diff: ___)
- Data Sample Match: Yes / No
- Performance: Faster / Slower / Similar
- Notes: ___

## Status
- [ ] Passed
- [ ] Failed (requires investigation)
- [ ] Passed with minor differences (acceptable)
```

---

## 🎯 Success Criteria

### Conversion Success
✅ Exe builds successfully
✅ Exe converts package without errors
✅ Generated code has valid syntax
✅ All components mapped

### Execution Success
✅ Code runs in Databricks without errors
✅ All transformations execute
✅ Connections established
✅ Data flows through pipeline

### Validation Success
✅ Row counts match (within tolerance)
✅ Data samples match
✅ Aggregations match
✅ Business logic preserved

---

## 💡 Tips for Success

1. **Test Incrementally**
   - Start with simple packages
   - Test one transformation at a time
   - Build confidence before complex packages

2. **Use Databricks Display**
   ```python
   # Use display() to see DataFrames
   display(df.limit(100))
   ```

3. **Check Each Step**
   ```python
   # Add counts after each transformation
   print(f"After source: {df.count()}")
   print(f"After filter: {df.count()}")
   print(f"After join: {df.count()}")
   ```

4. **Save Intermediate Results**
   ```python
   # Save checkpoints for debugging
   df.write.mode("overwrite").parquet("/tmp/checkpoint_1")
   ```

5. **Use Explain Plans**
   ```python
   # See execution plan
   df.explain()
   ```

---

## 📞 Quick Reference

### Test Single Package in Databricks
```python
# 1. Upload generated .py as notebook
# 2. Update credentials
# 3. Run cell by cell
# 4. Verify each transformation
# 5. Check final output
```

### Compare with SSIS
```python
# 1. Export SSIS results
# 2. Export Databricks results  
# 3. Use test_results_comparison.py
# 4. Review differences
# 5. Investigate discrepancies
```

### Monitor Execution
```python
# Check Spark UI
# Monitor cluster metrics
# Review logs
# Track execution time
```

---

## 🎊 Ready to Test!

You now have:
- ✅ Built .exe file
- ✅ Generated PySpark code
- ✅ Testing framework
- ✅ Comparison tools
- ✅ This testing guide

**Next**: Upload to Databricks and run your first test!

---

**Good luck with your Databricks testing!** 🚀

If you encounter any issues, check the troubleshooting section or review the generated code comments.




