# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for SSIS to PySpark Converter GUI
This file defines how to build the standalone GUI .exe application
"""

block_cipher = None

# Collect all Python files from parsing, mapping, and code_generation
a = Analysis(
    ['ssis_to_pyspark_gui.py'],
    pathex=[],
    binaries=[],
    datas=[
        # Include the models.py file
        ('models.py', '.'),
        # Include config.py if it exists
        ('config.py', '.'),
    ],
    hiddenimports=[
        # Parsing modules
        'parsing.data_engineering_parser',
        'parsing.enhanced_xml_processor',
        
        # Mapping modules
        'mapping.enhanced_json_mapper',
        'mapping.expression_translator',
        'mapping.component_mapper',
        'mapping.data_flow_mapper',
        'mapping.control_flow_mapper',
        'mapping.ssis_to_pyspark_mapper',
        
        # Code generation (LLM fallback)
        'code_generation.llm_code_generator',
        'code_generation.llm_code_validator',
        
        # Models and config
        'models',
        'config',
        
        # Standard library modules that might be missed
        'xml.etree.ElementTree',
        'json',
        'pathlib',
        'logging',
        're',
        
        # GUI modules
        'tkinter',
        'tkinter.ttk',
        'tkinter.filedialog',
        'tkinter.messagebox',
        'tkinter.scrolledtext',
        
        # Threading for async processing
        'threading',
        
        # Pydantic and dependencies
        'pydantic',
        'pydantic.fields',
        'pydantic.main',
        'pydantic.types',
        
        # Typing
        'typing',
        'typing_extensions',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Exclude unnecessary modules to reduce size
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'PIL',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='ssis_to_pyspark_converter',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # Windowed GUI application (no console window)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # Add icon path if you have one
)



