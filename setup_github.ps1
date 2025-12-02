# Quick GitHub Setup Script
# Run this in a NEW PowerShell window after Git installation

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  SSIS to PySpark - GitHub Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Navigate to project directory
$projectDir = "c:\Users\gurdain.singh.madan\ssis-to-pyspark-agent"
Set-Location $projectDir

Write-Host "📁 Project Directory: $projectDir" -ForegroundColor Green
Write-Host ""

# Step 1: Configure Git
Write-Host "Step 1: Configuring Git..." -ForegroundColor Yellow
git config --global user.name "gsmadan"
Write-Host "   ✓ Username set to: gsmadan" -ForegroundColor Green

$email = Read-Host "Enter your GitHub email address"
git config --global user.email $email
Write-Host "   ✓ Email set to: $email" -ForegroundColor Green
Write-Host ""

# Step 2: Initialize repository
Write-Host "Step 2: Initializing Git repository..." -ForegroundColor Yellow
git init
Write-Host "   ✓ Repository initialized" -ForegroundColor Green
Write-Host ""

# Step 3: Add files
Write-Host "Step 3: Adding files to Git..." -ForegroundColor Yellow
git add .
Write-Host "   ✓ Files added (respecting .gitignore)" -ForegroundColor Green
Write-Host ""

# Show what will be committed
Write-Host "Files to be committed:" -ForegroundColor Cyan
git status --short
Write-Host ""

# Step 4: Create initial commit
Write-Host "Step 4: Creating initial commit..." -ForegroundColor Yellow
git commit -m "Initial commit: SSIS to PySpark converter with Databricks integration"
Write-Host "   ✓ Initial commit created" -ForegroundColor Green
Write-Host ""

# Step 5: Get GitHub repository URL
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Step 5: Connect to GitHub" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Please create a repository on GitHub:" -ForegroundColor White
Write-Host "1. Go to https://github.com/new" -ForegroundColor White
Write-Host "2. Repository name: ssis-to-pyspark-converter" -ForegroundColor White
Write-Host "3. DO NOT initialize with README or .gitignore" -ForegroundColor Red
Write-Host "4. Click 'Create repository'" -ForegroundColor White
Write-Host ""

$repoUrl = Read-Host "Enter your GitHub repository URL (e.g., https://github.com/gsmadan/ssis-to-pyspark-converter.git)"

# Step 6: Add remote and push
Write-Host ""
Write-Host "Step 6: Connecting to GitHub and pushing..." -ForegroundColor Yellow
git remote add origin $repoUrl
git branch -M main

Write-Host ""
Write-Host "Pushing to GitHub..." -ForegroundColor Yellow
Write-Host "NOTE: You may be prompted for authentication" -ForegroundColor Cyan
Write-Host "      Use your Personal Access Token as the password" -ForegroundColor Cyan
Write-Host ""

git push -u origin main

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  ✓ Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Your code is now on GitHub at:" -ForegroundColor White
Write-Host $repoUrl -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Visit your repository on GitHub" -ForegroundColor White
Write-Host "2. Replace README.md with README_GITHUB.md" -ForegroundColor White
Write-Host "3. Add a LICENSE file if needed" -ForegroundColor White
Write-Host ""
