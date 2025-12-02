# Quick Command Reference for GitHub Setup

## Prerequisites
✅ Git has been installed
⚠️ **IMPORTANT**: Close and reopen PowerShell/Terminal after Git installation

## Manual Setup Commands

### 1. Configure Git (First Time Only)
```powershell
git config --global user.name "gsmadan"
git config --global user.email "your.email@example.com"
```

### 2. Initialize Repository
```powershell
cd "c:\Users\gurdain.singh.madan\ssis-to-pyspark-agent"
git init
```

### 3. Add Files
```powershell
git add .
git status  # Review what will be committed
```

### 4. Create Initial Commit
```powershell
git commit -m "Initial commit: SSIS to PySpark converter"
```

### 5. Create GitHub Repository
1. Go to https://github.com/new
2. Repository name: `ssis-to-pyspark-converter`
3. **DO NOT** check "Initialize with README"
4. Click "Create repository"
5. Copy the repository URL

### 6. Connect and Push
```powershell
# Replace with your actual repository URL
git remote add origin https://github.com/gsmadan/ssis-to-pyspark-converter.git
git branch -M main
git push -u origin main
```

## Authentication
When prompted for password, use a **Personal Access Token**:
1. GitHub → Settings → Developer settings → Personal access tokens
2. Generate new token (classic)
3. Select scope: `repo`
4. Copy token and use as password

## Future Updates
```powershell
git add .
git commit -m "Your commit message"
git push
```

## Automated Setup
Run the setup script in a **new PowerShell window**:
```powershell
cd "c:\Users\gurdain.singh.madan\ssis-to-pyspark-agent"
.\setup_github.ps1
```
