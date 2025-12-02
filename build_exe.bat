@echo off
setlocal ENABLEEXTENSIONS
set "script_dir=%~dp0"
pushd "%script_dir%"
REM ========================================
REM SSIS to PySpark Converter - Build Script
REM Build standalone .exe applications (GUI or CLI)
REM With API Key Setup Support (colocated .env in dist\)
REM ========================================

set "choice=%~1"
set "skipkeys=%~2"

echo.
echo ============================================================
echo SSIS to PySpark Converter - Build Script
echo ============================================================
echo Usage: build_exe.bat [1^|2^|3] [--skip-keys]
echo   1 = GUI   2 = CLI   3 = Both
echo   --skip-keys = skip API key setup prompt
echo ============================================================
echo.

if "%choice%"=="" (
    echo Select build type:
    echo   1. GUI Application (Graphical Interface)
    echo   2. CLI Application (Command Line)
    echo   3. Both
    echo.
    set /p choice="Enter your choice (1/2/3): "
    echo.
) else (
    echo Selected build option: %choice%
    echo.
)

set "skip_api_keys="
if /I "%skipkeys%"=="--skip-keys" (
    set "skip_api_keys=1"
)

REM [1/5] Python
echo [1/5] Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH
    echo Please install Python from https://www.python.org/
    pause
    exit /b 1
)
python --version
echo [OK] Python found
echo.

REM [2/5] PyInstaller
echo [2/5] Checking PyInstaller...
python -c "import PyInstaller" >nul 2>&1
if errorlevel 1 (
    echo [WARNING] PyInstaller not found, installing...
    python -m pip install pyinstaller
    if errorlevel 1 (
        echo [ERROR] Failed to install PyInstaller
        pause
        exit /b 1
    )
)
echo [OK] PyInstaller ready
echo.

REM [3/5] Dependencies
echo [3/5] Installing/updating required packages...
python -m pip install --upgrade -r requirements.txt >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Some dependencies may have failed to install
    echo Continuing with build...
)
echo [OK] Dependencies checked
echo.

REM Optional API Key Setup in project root (for local test builds)
echo [INFO] API key configuration (project root)...
if not defined skip_api_keys (
    if exist "%script_dir%.env" (
        echo   [OK] .env found in project root (used when running scripts)
    ) else (
        echo   [INFO] No .env in project root. Skipping local API key setup.
        echo         Run setup_api_keys.py manually later if you want to store keys.
    )
) else (
    echo   [INFO] Skipping local API key setup (--skip-keys flag).
)
echo.

REM [4/5] Clean
echo [4/5] Cleaning previous build artifacts...
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"
echo [OK] Cleanup complete
echo.

REM Build based on user choice
if "%choice%"=="1" goto build_gui
if "%choice%"=="2" goto build_cli
if "%choice%"=="3" goto build_both
echo [ERROR] Invalid choice. Please select 1, 2, or 3.
pause
exit /b 1

:build_gui
echo ============================================================
echo Building GUI Application...
echo ============================================================
echo.
if not exist "ssis_to_pyspark_gui.py" (
    echo [ERROR] ssis_to_pyspark_gui.py not found
    pause
    exit /b 1
)
python -m PyInstaller ssis_to_pyspark_gui.spec --clean --noconfirm
if errorlevel 1 goto build_error
echo [OK] GUI build complete
goto after_build

:build_cli
echo ============================================================
echo Building CLI Application...
echo ============================================================
echo.
if not exist "ssis_to_pyspark_app.py" (
    echo [ERROR] ssis_to_pyspark_app.py not found
    pause
    exit /b 1
)
python -m PyInstaller ssis_to_pyspark.spec --clean --noconfirm
if errorlevel 1 goto build_error
echo [OK] CLI build complete
goto after_build

:build_both
echo ============================================================
echo Building Both Applications...
echo ============================================================
echo.
echo Building GUI Application...
if exist "ssis_to_pyspark_gui.py" (
    python -m PyInstaller ssis_to_pyspark_gui.spec --clean --noconfirm
    if errorlevel 1 goto build_error
    echo [OK] GUI build complete
    echo.
) else (
    echo [WARNING] ssis_to_pyspark_gui.py not found, skipping GUI build
    echo.
)

echo Building CLI Application...
if exist "ssis_to_pyspark_app.py" (
    python -m PyInstaller ssis_to_pyspark.spec --clean --noconfirm
    if errorlevel 1 goto build_error
    echo [OK] CLI build complete
) else (
    echo [WARNING] ssis_to_pyspark_app.py not found, skipping CLI build
)
goto after_build

:build_error
echo.
echo [ERROR] Build failed!
pause
exit /b 1

:after_build
REM Copy helpers into dist
if exist "%script_dir%setup_api_keys.py" (
    copy /Y "%script_dir%setup_api_keys.py" "dist\setup_api_keys.py" >nul
)

echo.
echo [5/5] Build completed successfully!
echo.
echo Built executables in dist\ folder:
dir /b dist\*.exe 2>nul
echo.
echo Usage:
echo   GUI: Double-click ssis_to_pyspark_converter.exe
echo   CLI: ssis_to_pyspark_converter.exe input\package.dtsx
echo        ssis_to_pyspark_converter.exe input\folder\
echo.
echo API Keys (for EXE):
echo   - Place .env next to the exe in dist\   (or run setup_api_keys.py in dist\)
echo     For Gemini:
echo       GEMINI_API_KEY=your_key
echo       DEFAULT_LLM_PROVIDER=gemini
echo     For OpenAI:
echo       OPENAI_API_KEY=your_key
echo       DEFAULT_LLM_PROVIDER=openai
echo.
echo Notes:
echo   - The exe reads ONLY dist\.env (colocated) or OS environment variables.
echo   - If no keys are present, the exe runs in rule-based mode and logs "Skipping LLM validation".
echo.
popd
endlocal
pause