$ErrorActionPreference = "Stop"

$ProjectRoot = "C:\Users\gurdain.singh.madan\ssis-to-pyspark-agent"
$OutputPath = "$ProjectRoot\dist\ssis_converter_databricks.zip"

Write-Host "Packaging project for Databricks..."
Write-Host "Source: $ProjectRoot"
Write-Host "Destination: $OutputPath"

# Create dist directory if not exists
if (-not (Test-Path "$ProjectRoot\dist")) {
    New-Item -ItemType Directory -Path "$ProjectRoot\dist" | Out-Null
}

# Get files to zip, excluding specific folders
# Note: Get-ChildItem -Exclude works on the immediate children names
$ItemsToZip = Get-ChildItem -Path $ProjectRoot -Exclude "dist", ".git", ".venv", "venv", "__pycache__", "output", "build", "*.egg-info", ".env", "tests", "*.pyc"

# Compress
Compress-Archive -Path $ItemsToZip.FullName -DestinationPath $OutputPath -Force -CompressionLevel Optimal

Write-Host "Successfully created deployment package: $OutputPath"
Write-Host "You can now upload this zip to Databricks DBFS or Workspace."
