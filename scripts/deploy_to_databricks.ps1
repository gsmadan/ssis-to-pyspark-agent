param (
    [Parameter(Mandatory=$true)]
    [string]$FilePath,
    
    [string]$RemotePath = "dbfs:/FileStore/ssis_conversion/"
)

# Check if Databricks CLI is installed
if (-not (Get-Command "databricks" -ErrorAction SilentlyContinue)) {
    Write-Error "Databricks CLI is not installed or not in PATH."
    exit 1
}

# Check if file exists
if (-not (Test-Path $FilePath)) {
    Write-Error "File not found: $FilePath"
    exit 1
}

$FileName = Split-Path $FilePath -Leaf
$RemoteFullPath = "$RemotePath$FileName"

Write-Host "Deploying $FileName to $RemotePath..."

# Upload file
try {
    databricks fs cp "$FilePath" "$RemoteFullPath" --overwrite
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Successfully uploaded to $RemoteFullPath"
        Write-Host "You can now create a Databricks Job pointing to this file."
    } else {
        Write-Error "Upload failed. Please check your Databricks CLI configuration."
        exit $LASTEXITCODE
    }
} catch {
    Write-Error "An error occurred during deployment: $_"
    exit 1
}
