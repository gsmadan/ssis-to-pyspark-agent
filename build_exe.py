"""
Build script for creating standalone .exe file using PyInstaller.
"""
import subprocess
import sys
import shutil
from pathlib import Path

def clean_build_artifacts():
    """Clean previous build artifacts."""
    print("Cleaning previous build artifacts...")
    
    folders_to_remove = ['build', 'dist', '__pycache__']
    for folder in folders_to_remove:
        if Path(folder).exists():
            shutil.rmtree(folder)
            print(f"  Removed: {folder}/")
    
    # Remove spec file artifacts
    spec_files = list(Path('.').glob('*.spec'))
    for spec_file in spec_files:
        if spec_file.name != 'ssis_to_pyspark.spec':
            spec_file.unlink()
            print(f"  Removed: {spec_file}")
    
    print("[OK] Cleanup complete\n")


def build_exe():
    """Build the .exe file using PyInstaller."""
    print("="*60)
    print("Building SSIS to PySpark Converter .exe")
    print("="*60 + "\n")
    
    # Clean previous builds
    clean_build_artifacts()
    
    # Build using spec file
    print("Running PyInstaller...")
    print("-"*60)
    
    cmd = [
        sys.executable,
        '-m', 'PyInstaller',
        'ssis_to_pyspark.spec',
        '--clean',
        '--noconfirm'
    ]
    
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode == 0:
        print("\n" + "="*60)
        print("[SUCCESS] Build completed!")
        print("="*60)
        
        exe_path = Path('dist/ssis_to_pyspark_converter.exe')
        if exe_path.exists():
            size_mb = exe_path.stat().st_size / (1024 * 1024)
            print(f"\nExecutable created: {exe_path}")
            print(f"Size: {size_mb:.2f} MB")
            print("\nYou can now distribute this .exe file to client environments.")
            print("\nUsage:")
            print("  ssis_to_pyspark_converter.exe input/package.dtsx")
            print("  ssis_to_pyspark_converter.exe input/")
        else:
            print("\n[WARNING] Executable not found in expected location")
            print(f"Expected: {exe_path}")
    else:
        print("\n[ERROR] Build failed!")
        print(f"Exit code: {result.returncode}")
        return False
    
    return True


def test_exe():
    """Test the built .exe file."""
    print("\n" + "="*60)
    print("Testing built .exe")
    print("="*60 + "\n")
    
    exe_path = Path('dist/ssis_to_pyspark_converter.exe')
    
    if not exe_path.exists():
        print(f"[ERROR] Executable not found: {exe_path}")
        return False
    
    # Test with --help
    print("Testing with --help flag...")
    result = subprocess.run(
        [str(exe_path), '--help'],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("[OK] Help command works")
        print("\nHelp output:")
        print("-"*60)
        print(result.stdout)
    else:
        print(f"[ERROR] Help command failed with exit code: {result.returncode}")
        if result.stderr:
            print("Error output:")
            print(result.stderr)
        return False
    
    return True


if __name__ == '__main__':
    print("\nSSIS to PySpark Converter - Build Script")
    print("="*60 + "\n")
    
    # Check if PyInstaller is installed
    try:
        import PyInstaller
        print(f"[OK] PyInstaller version: {PyInstaller.__version__}\n")
    except ImportError:
        print("[ERROR] PyInstaller not installed!")
        print("Install with: pip install pyinstaller")
        sys.exit(1)
    
    # Build the exe
    if not build_exe():
        sys.exit(1)
    
    # Test the exe
    if not test_exe():
        print("\n[WARNING] Exe tests failed")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("[SUCCESS] Build and test completed!")
    print("="*60)
    print("\nYour exe file is ready for deployment:")
    print("  dist/ssis_to_pyspark_converter.exe")
    print("\n")

