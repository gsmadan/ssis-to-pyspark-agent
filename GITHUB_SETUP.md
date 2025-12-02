# GitHub Setup Guide for SSIS to PySpark Converter

This guide will help you push your SSIS to PySpark conversion tool to GitHub.

## Prerequisites

✅ Git has been installed on your system
⚠️ You need to **restart your terminal/PowerShell** for Git to be available in PATH

## Step 1: Configure Git (First Time Only)

Open a **new PowerShell window** and run:

```powershell
# Set your Git username
git config --global user.name "gsmadan"

# Set your Git email (use your GitHub email)
git config --global user.email "gsmadan97@gmail.com"

# Verify configuration
git config --global --list
```

## Step 2: Create a GitHub Repository

1. Go to [GitHub](https://github.com) and sign in
2. Click the **"+"** icon in the top right → **"New repository"**
3. Fill in the details:
   - **Repository name**: `ssis-to-pyspark-converter`
   - **Description**: `Automated SSIS to PySpark conversion tool with Databricks integration`
   - **Visibility**: Choose **Public** or **Private**
   - **DO NOT** initialize with README, .gitignore, or license (we already have these)
4. Click **"Create repository"**
5. Copy the repository URL (e.g., `https://github.com/gsmadan/ssis-to-pyspark-converter.git`)

## Step 3: Initialize Git Repository Locally

In a **new PowerShell window**, navigate to your project and run:

```powershell
# Navigate to project directory
cd "c:\Users\gurdain.singh.madan\ssis-to-pyspark-agent"

# Initialize Git repository
git init

# Add all files (respecting .gitignore)
git add .

# Check what will be committed
git status

# Create initial commit
git commit -m "Initial commit: SSIS to PySpark converter with Databricks integration"
```

## Step 4: Connect to GitHub and Push

```powershell
# Add GitHub repository as remote (replace with your actual URL)
git remote add origin https://github.com/gsmadan/ssis-to-pyspark-converter.git

# Rename branch to main (if needed)
git branch -M main

# Push to GitHub
git push -u origin main
```

### Authentication Options

When you push, GitHub will ask for authentication:

**Option A: Personal Access Token (Recommended)**
1. Go to GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Give it a name: `SSIS Converter`
4. Select scopes: `repo` (full control of private repositories)
5. Click "Generate token"
6. **Copy the token** (you won't see it again!)
7. When prompted for password during `git push`, paste the token

**Option B: GitHub CLI**
```powershell
# Install GitHub CLI
winget install --id GitHub.cli

# Authenticate
gh auth login
```

## Step 5: Verify Upload

1. Go to your GitHub repository URL
2. Refresh the page
3. You should see all your files!

## Important Files to Review Before Pushing

### ✅ Already Protected (in .gitignore)
- `.env` - Your API keys and secrets
- `__pycache__/` - Python cache files
- `*.log` - Log files
- `build/` and `dist/` - Build artifacts

### 📝 Files That WILL Be Pushed
- All Python source code
- `requirements.txt`
- Documentation files
- `.gitignore` itself
- Sample packages (if you want to exclude these, add to .gitignore)

## Optional: Add Large Files to .gitignore

If you have large files you don't want to push:

```bash
# Edit .gitignore and add:
*.dtsx
*.zip
Harsh-KT.zip
testing/
input-sample packages/
output-sample-packages/
```

## Future Updates

After the initial push, to update your repository:

```powershell
# Check status
git status

# Add changed files
git add .

# Commit changes
git commit -m "Description of your changes"

# Push to GitHub
git push
```

## Troubleshooting

### "git is not recognized"
- **Solution**: Close and reopen PowerShell/Terminal after Git installation

### "Permission denied"
- **Solution**: Use Personal Access Token instead of password

### "Large files detected"
- **Solution**: Add them to `.gitignore` or use Git LFS

### "Repository already exists"
- **Solution**: Use `git remote set-url origin <new-url>` to change the remote URL

## Next Steps After GitHub Upload

1. **Add a comprehensive README.md** (see README_TEMPLATE.md)
2. **Create a LICENSE file** (MIT, Apache 2.0, etc.)
3. **Set up GitHub Actions** for CI/CD (optional)
4. **Add badges** to README (build status, license, etc.)
5. **Create releases** for version management

## Security Checklist

Before pushing, ensure:
- [ ] No API keys in code
- [ ] No passwords in code
- [ ] `.env` is in `.gitignore`
- [ ] No sensitive customer data
- [ ] No proprietary SSIS packages (or add to .gitignore)

---

**Need Help?** Check the [GitHub Docs](https://docs.github.com/en/get-started/quickstart/create-a-repo)
