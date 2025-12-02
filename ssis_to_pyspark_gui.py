#!/usr/bin/env python3
"""
SSIS to PySpark Converter - GUI Application
User-friendly graphical interface for converting SSIS packages to PySpark code.
"""

import sys
import os
import json
import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
from pathlib import Path
from datetime import datetime
import threading
import logging
import re
from io import StringIO

# Add current directory to path for bundled exe
if getattr(sys, 'frozen', False):
    # Running as compiled exe
    application_path = sys._MEIPASS
else:
    # Running as script
    application_path = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, application_path)

from parsing.data_engineering_parser import DataEngineeringParser
from mapping.enhanced_json_mapper import EnhancedJSONMapper
from config import detect_available_provider, save_api_key_to_env
from code_generation.llm_code_validator import LLMCodeValidator


class TextHandler(logging.Handler):
    """Custom logging handler to redirect logs to GUI text widget."""
    
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
        
    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + '\n')
            self.text_widget.configure(state='disabled')
            self.text_widget.see(tk.END)
        self.text_widget.after(0, append)


class APIKeySetupDialog:
    """Dialog for setting up API keys."""
    
    def __init__(self, parent):
        self.parent = parent
        self.result = None
        self.setup_dialog()
    
    def setup_dialog(self):
        """Show API key setup dialog."""
        self.dialog = tk.Toplevel(self.parent)
        dialog = self.dialog
        dialog.title("API Key Setup")
        dialog.geometry("560x520")
        dialog.resizable(True, True)
        
        # Center the dialog
        dialog.transient(self.parent)
        dialog.grab_set()
        
        # Main frame
        main_frame = tk.Frame(dialog, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = tk.Label(main_frame, text="🔑 API Key Setup", 
                              font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Description
        desc_text = """This converter uses AI for advanced code validation and refinement.

You can use either OpenAI or Google Gemini (or both).

Choose your preferred AI provider:"""
        desc_label = tk.Label(main_frame, text=desc_text, justify=tk.LEFT)
        desc_label.pack(pady=(0, 20))
        
        # Provider selection
        self.provider_var = tk.StringVar(value="gemini")
        
        openai_radio = tk.Radiobutton(main_frame, text="OpenAI (GPT-4)", 
                                     variable=self.provider_var, value="openai")
        openai_radio.pack(anchor=tk.W, pady=5)
        
        gemini_radio = tk.Radiobutton(main_frame, text="Google Gemini (Free tier available)", 
                                     variable=self.provider_var, value="gemini")
        gemini_radio.pack(anchor=tk.W, pady=5)
        
        skip_radio = tk.Radiobutton(main_frame, text="Skip (Rule-based conversion only)", 
                                   variable=self.provider_var, value="skip")
        skip_radio.pack(anchor=tk.W, pady=5)
        
        # API Key input
        api_frame = tk.Frame(main_frame)
        api_frame.pack(fill=tk.X, pady=(20, 0))
        
        api_label = tk.Label(api_frame, text="API Key:")
        api_label.pack(anchor=tk.W)
        
        self.api_key_entry = tk.Entry(api_frame, show="*", width=50)
        self.api_key_entry.pack(fill=tk.X, pady=(5, 0))
        # Bind Enter to submit
        self.api_key_entry.bind("<Return>", lambda e: self.save_api_key())
        
        # Help text
        help_text = """Get your API key from:
• OpenAI: https://platform.openai.com/api-keys
• Gemini: https://makersuite.google.com/app/apikey"""
        help_label = tk.Label(
            main_frame,
            text=help_text,
            font=("Arial", 8),
            fg="gray",
            justify=tk.LEFT,
            anchor="w",
            wraplength=520
        )
        help_label.pack(anchor=tk.W, pady=(10, 0))

        # Small hint below the entry
        hint_label = tk.Label(
            main_frame,
            text="Press Enter or click Save",
            font=("Arial", 8),
            fg="gray"
        )
        hint_label.pack(anchor=tk.W, pady=(4, 0))
        
        # Buttons
        button_frame = tk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(20, 0))
        
        tk.Button(button_frame, text="Skip", 
                 command=lambda: self.set_result("skip", ""),
                 width=10).pack(side=tk.RIGHT, padx=(5, 0))
        
        save_btn = tk.Button(button_frame, text="Save", 
                 command=self.save_api_key,
                 width=10)
        save_btn.pack(side=tk.RIGHT)
        
        # Focus on API key entry
        self.api_key_entry.focus()
        # Also bind Enter at dialog level
        dialog.bind("<Return>", lambda e: self.save_api_key())
        
        # Handle window close (treat as skip)
        dialog.protocol("WM_DELETE_WINDOW", lambda: self.set_result("skip", ""))

        # Wait for dialog to close
        dialog.wait_window()
    
    def save_api_key(self):
        """Save the API key."""
        provider = self.provider_var.get()
        api_key = self.api_key_entry.get().strip()
        
        if provider == "skip":
            self.set_result("skip", "")
        elif not api_key:
            messagebox.showerror("Error", "Please enter an API key or select 'Skip'")
        else:
            self.set_result(provider, api_key)
    
    def set_result(self, provider, api_key):
        """Set the result and close dialog."""
        self.result = (provider, api_key)
        # Close only the dialog, keep main window
        try:
            if hasattr(self, 'dialog') and self.dialog and self.dialog.winfo_exists():
                self.dialog.destroy()
        except Exception:
            pass
        self.parent.focus_set()


class SSISToPySparkGUI:
    """GUI application for SSIS to PySpark conversion."""
    
    def __init__(self, root):
        self.root = root
        self.root.title("SSIS to PySpark Converter")
        self.root.geometry("1200x820")
        self.root.minsize(1100, 760)
        try:
            self.root.state('zoomed')
        except Exception:
            pass
        
        # Set icon if available
        try:
            if getattr(sys, 'frozen', False):
                icon_path = os.path.join(sys._MEIPASS, 'icon.ico')
                if os.path.exists(icon_path):
                    self.root.iconbitmap(icon_path)
        except:
            pass
        
        # Variables
        self.input_path = tk.StringVar()
        self.output_path = tk.StringVar(value="output")
        self.schema_mapping_path = tk.StringVar()
        self.is_processing = False
        
        # Initialize converters
        self.parser = None
        self.mapper = None
        self.validator = None
        
        # Setup API keys first
        self.setup_api_keys()
        # Initialize LLM after dialog outcome (.env saved or skipped)
        self.init_llm()
        
        # Setup GUI
        self.setup_gui()
        
        # Setup logging
        self.setup_logging()
    
    def setup_api_keys(self):
        """Setup API keys for the GUI application."""
        from config import settings
        
        provider = detect_available_provider()
        
        if provider == "none":
            # Show API key setup dialog
            dialog = APIKeySetupDialog(self.root)
            
            if dialog.result:
                provider, api_key = dialog.result
                
                if provider != "skip" and api_key:
                    # Update settings dynamically
                    if provider == "openai":
                        settings.openai_api_key = api_key
                        settings.default_llm_provider = "openai"
                    else:
                        settings.gemini_api_key = api_key
                        settings.default_llm_provider = "gemini"
                    
                    # Save to .env file
                    save_api_key_to_env(provider, api_key)
                    messagebox.showinfo("Success", 
                                      f"✅ {provider.upper()} API key saved!\n"
                                      f"AI features are now enabled.")
                else:
                    messagebox.showinfo("Info", 
                                      "⚠️ No API key configured.\n"
                                      "Using rule-based conversion only.")
        else:
            # Provider already configured - show info
            messagebox.showinfo("Info", 
                              f"✅ {provider.upper()} API key already configured.\n"
                              f"AI features are enabled.")
        
    def init_llm(self):
        """Initialize LLM validator after API key setup/decision."""
        try:
            self.validator = LLMCodeValidator()
            provider = getattr(self.validator, 'primary_provider', 'none')
            client_available = bool(getattr(self.validator, 'client', None))
            logging.info(f"LLM setup: provider={provider}, client={'available' if client_available else 'not available'}")
        except Exception as e:
            logging.warning(f"LLM initialization failed: {e}")
            self.validator = None
        
    def setup_gui(self):
        """Setup the GUI components."""
        
        # Main frame with padding
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(4, weight=1)
        
        # Title
        title_label = ttk.Label(
            main_frame, 
            text="SSIS to PySpark Converter",
            font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 10))
        
        subtitle_label = ttk.Label(
            main_frame,
            text="Convert SSIS packages (.dtsx) to PySpark code",
            font=("Arial", 9)
        )
        subtitle_label.grid(row=1, column=0, columnspan=3, pady=(0, 20))
        
        # Input section
        input_frame = ttk.LabelFrame(main_frame, text="Input Selection", padding="10")
        input_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        input_frame.columnconfigure(1, weight=1)
        
        ttk.Label(input_frame, text="Input:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        
        input_entry = ttk.Entry(input_frame, textvariable=self.input_path, width=50)
        input_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        
        btn_file = ttk.Button(input_frame, text="Select File", command=self.select_file, width=12)
        btn_file.grid(row=0, column=2, padx=(0, 5))
        
        btn_folder = ttk.Button(input_frame, text="Select Folder", command=self.select_folder, width=12)
        btn_folder.grid(row=0, column=3)
        
        # Help text
        help_text = ttk.Label(
            input_frame,
            text="Select a single .dtsx file or a folder containing multiple .dtsx files",
            font=("Arial", 8),
            foreground="gray"
        )
        help_text.grid(row=1, column=0, columnspan=4, sticky=tk.W, pady=(5, 0))
        
        # Output section
        output_frame = ttk.LabelFrame(main_frame, text="Output Options", padding="10")
        output_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        output_frame.columnconfigure(1, weight=1)
        
        ttk.Label(output_frame, text="Output Folder:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        
        output_entry = ttk.Entry(output_frame, textvariable=self.output_path, width=50)
        output_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        
        btn_output = ttk.Button(output_frame, text="Browse", command=self.select_output, width=12)
        btn_output.grid(row=0, column=2)
        
        # Schema Mapping section
        ttk.Label(output_frame, text="Schema Mapping:").grid(row=1, column=0, sticky=tk.W, padx=(0, 10), pady=(10, 0))
        
        schema_entry = ttk.Entry(output_frame, textvariable=self.schema_mapping_path, width=50)
        schema_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), padx=(0, 10), pady=(10, 0))
        
        btn_schema = ttk.Button(output_frame, text="Browse", command=self.select_schema_mapping, width=12)
        btn_schema.grid(row=1, column=2, pady=(10, 0))
        
        # Help text for schema mapping
        schema_help = ttk.Label(
            output_frame,
            text="Optional: JSON file mapping SSIS connections to Databricks schemas",
            font=("Arial", 8),
            foreground="gray"
        )
        schema_help.grid(row=2, column=0, columnspan=3, sticky=tk.W, pady=(5, 0))
        
        # Progress section
        progress_frame = ttk.LabelFrame(main_frame, text="Conversion Log", padding="10")
        progress_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        progress_frame.columnconfigure(0, weight=1)
        progress_frame.rowconfigure(1, weight=1)
        
        # Progress bar
        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress_bar.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Log text area
        self.log_text = scrolledtext.ScrolledText(
            progress_frame,
            height=15,
            width=80,
            state='disabled',
            wrap=tk.WORD,
            font=("Consolas", 9)
        )
        self.log_text.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Buttons section
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=5, column=0, columnspan=3, pady=(0, 0))
        
        self.btn_convert = ttk.Button(
            button_frame,
            text="▶ Convert",
            command=self.start_conversion,
            width=20
        )
        self.btn_convert.grid(row=0, column=0, padx=5)
        
        btn_clear = ttk.Button(
            button_frame,
            text="Clear Log",
            command=self.clear_log,
            width=15
        )
        btn_clear.grid(row=0, column=1, padx=5)
        
        btn_open_output = ttk.Button(
            button_frame,
            text="Open Output Folder",
            command=self.open_output_folder,
            width=20
        )
        btn_open_output.grid(row=0, column=2, padx=5)
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(
            main_frame,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W
        )
        status_bar.grid(row=6, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(10, 0))
        
    def setup_logging(self):
        """Setup logging to redirect to GUI."""
        # Create logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Add GUI text handler
        text_handler = TextHandler(self.log_text)
        text_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))
        self.logger.addHandler(text_handler)
        
        # Also add file handler
        try:
            file_handler = logging.FileHandler('ssis_converter.log')
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(file_handler)
        except:
            pass
    
    def select_file(self):
        """Open file dialog to select a single .dtsx file."""
        filename = filedialog.askopenfilename(
            title="Select SSIS Package",
            filetypes=[("SSIS Package", "*.dtsx"), ("All Files", "*.*")]
        )
        if filename:
            self.input_path.set(filename)
            self.status_var.set(f"Selected: {os.path.basename(filename)}")
    
    def select_folder(self):
        """Open folder dialog to select a folder containing .dtsx files."""
        foldername = filedialog.askdirectory(
            title="Select Folder Containing SSIS Packages"
        )
        if foldername:
            self.input_path.set(foldername)
            # Count .dtsx files
            dtsx_count = len(list(Path(foldername).glob('*.dtsx')))
            self.status_var.set(f"Selected folder: {dtsx_count} .dtsx file(s) found")
    
    def select_output(self):
        """Open folder dialog to select output folder."""
        foldername = filedialog.askdirectory(
            title="Select Output Folder"
        )
        if foldername:
            self.output_path.set(foldername)
    
    def select_schema_mapping(self):
        """Open file dialog to select schema mapping JSON file."""
        filename = filedialog.askopenfilename(
            title="Select Schema Mapping File",
            filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")]
        )
        if filename:
            self.schema_mapping_path.set(filename)
            self.status_var.set(f"Schema mapping: {os.path.basename(filename)}")
    
    def clear_log(self):
        """Clear the log text area."""
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')
    
    def open_output_folder(self):
        """Open the output folder in file explorer."""
        output_dir = self.output_path.get()
        if os.path.exists(output_dir):
            os.startfile(output_dir)
        else:
            messagebox.showwarning("Folder Not Found", f"Output folder does not exist:\n{output_dir}")
    
    def start_conversion(self):
        """Start the conversion process in a separate thread."""
        if self.is_processing:
            messagebox.showwarning("Processing", "Conversion is already in progress!")
            return
        
        input_path = self.input_path.get().strip()
        if not input_path:
            messagebox.showerror("Error", "Please select an input file or folder!")
            return
        
        if not os.path.exists(input_path):
            messagebox.showerror("Error", f"Input path does not exist:\n{input_path}")
            return
        
        # Disable convert button and start progress
        self.btn_convert.config(state='disabled')
        self.progress_bar.start(10)
        self.is_processing = True
        self.status_var.set("Converting...")
        
        # Run conversion in separate thread
        thread = threading.Thread(target=self.run_conversion, daemon=True)
        thread.start()
    
    def run_conversion(self):
        """Run the actual conversion process."""
        try:
            # Initialize converters
            logging.info("="*60)
            logging.info("SSIS to PySpark Converter - Starting")
            logging.info("="*60)
            
            from mapping.schema_mapper import SchemaMapper
            
            self.parser = DataEngineeringParser()
            
            # Load schema mapping if provided
            schema_mapping_file = self.schema_mapping_path.get().strip()
            schema_mapper = None
            if schema_mapping_file:
                schema_mapper = SchemaMapper(schema_mapping_file)
                if schema_mapper.is_active():
                    logging.info(f"Schema mapping loaded: {len(schema_mapper.connection_mappings)} connection(s) mapped")
                else:
                    logging.warning(f"Schema mapping file provided but not loaded: {schema_mapping_file}")
            
            self.mapper = EnhancedJSONMapper(
                use_llm_fallback=False,
                databricks_mode=True,
                schema_mapper=schema_mapper
            )
            
            # Create output directories
            output_base = Path(self.output_path.get())
            (output_base / 'parsed_json').mkdir(parents=True, exist_ok=True)
            (output_base / 'pyspark_code').mkdir(parents=True, exist_ok=True)
            (output_base / 'mapping_details').mkdir(parents=True, exist_ok=True)
            (output_base / 'analysis').mkdir(parents=True, exist_ok=True)
            
            input_path = Path(self.input_path.get())
            results = []
            
            # Determine if input is file or folder
            if input_path.is_file():
                logging.info(f"Mode: Single file conversion")
                logging.info(f"Input: {input_path.name}")
                result = self.convert_single_package(input_path, output_base)
                results.append(result)
            
            elif input_path.is_dir():
                logging.info(f"Mode: Batch folder conversion")
                dtsx_files = list(input_path.glob('*.dtsx'))
                
                if not dtsx_files:
                    logging.warning(f"No .dtsx files found in: {input_path}")
                else:
                    logging.info(f"Found {len(dtsx_files)} SSIS package(s)")
                    logging.info("="*60)
                    
                    for i, dtsx_file in enumerate(dtsx_files, 1):
                        logging.info(f"\n[{i}/{len(dtsx_files)}] Processing: {dtsx_file.name}")
                        logging.info("-"*60)
                        
                        try:
                            result = self.convert_single_package(dtsx_file, output_base)
                            results.append(result)
                            logging.info(f"[SUCCESS] {dtsx_file.name}")
                        except Exception as e:
                            logging.error(f"[FAILED] {dtsx_file.name} - {e}")
                            results.append({
                                'success': False,
                                'package_file': dtsx_file.name,
                                'error': str(e)
                            })
            
            # Print summary
            self.print_summary(results, output_base)
            
            # Save summary report
            summary_file = output_base / 'analysis' / 'conversion_summary.json'
            with open(summary_file, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'input': str(input_path),
                    'results': results,
                    'summary': {
                        'total': len(results),
                        'successful': sum(1 for r in results if r.get('success', False)),
                        'failed': sum(1 for r in results if not r.get('success', False))
                    }
                }, f, indent=2)
            
            logging.info(f"\n[SAVED] Summary report: {summary_file}")
            
            # Show completion message
            successful = sum(1 for r in results if r.get('success', False))
            self.root.after(0, lambda: messagebox.showinfo(
                "Conversion Complete",
                f"Successfully converted {successful}/{len(results)} package(s)!\n\n"
                f"Output saved to: {output_base}"
            ))
            
            self.root.after(0, lambda: self.status_var.set(
                f"Complete: {successful}/{len(results)} successful"
            ))
            
        except Exception as e:
            logging.error(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()
            self.root.after(0, lambda: messagebox.showerror("Error", f"Conversion failed:\n{str(e)}"))
            self.root.after(0, lambda: self.status_var.set("Error occurred"))
        
        finally:
            # Re-enable button and stop progress
            self.root.after(0, lambda: self.btn_convert.config(state='normal'))
            self.root.after(0, lambda: self.progress_bar.stop())
            self.is_processing = False
    
    def convert_single_package(self, dtsx_path: Path, output_base: Path) -> dict:
        """Convert a single SSIS package to PySpark."""
        logging.info(f"Converting: {dtsx_path.name}")
        
        # Step 1: Parse SSIS package
        logging.info("Step 1/4: Parsing SSIS package...")
        parsed_data = self.parser.parse_dtsx_file(str(dtsx_path))
        package_name = parsed_data.get('package_name', 'Unknown')
        
        logging.info(f"  Package: {package_name}")
        logging.info(f"    - Connections: {len(parsed_data.get('connections', {}))}")
        logging.info(f"    - SQL Tasks: {len(parsed_data.get('sql_tasks', []))}")
        logging.info(f"    - Data Flows: {len(parsed_data.get('data_flows', []))}")
        
        # Save parsed JSON
        json_filename = dtsx_path.stem + "_data_engineering.json"
        json_path = output_base / 'parsed_json' / json_filename
        
        with open(json_path, 'w') as f:
            json.dump(parsed_data, f, indent=2)
        logging.info(f"  Parsed JSON: {json_path.name}")
        
        # Step 2: Map to PySpark
        logging.info("Step 2/4: Mapping to PySpark...")
        mapping_result = self.mapper.map_json_package(str(json_path))
        
        stats = mapping_result['statistics']
        logging.info(f"  [OK] Mapping Rate: {stats['overall_mapping_rate']}%")
        logging.info(f"       - Sources: {stats['mapped_sources']}/{stats['total_sources']}")
        logging.info(f"       - Transformations: {stats['mapped_transformations']}/{stats['total_transformations']}")
        logging.info(f"       - Destinations: {stats['mapped_destinations']}/{stats['total_destinations']}")
        
        # Save PySpark code
        pyspark_filename = dtsx_path.stem + "_pyspark.py"
        pyspark_path = output_base / 'pyspark_code' / pyspark_filename
        
        with open(pyspark_path, 'w') as f:
            f.write(mapping_result['pyspark_code'])
        logging.info(f"  [SAVED] Rule-based PySpark Code: {pyspark_path}")

        refined_code = mapping_result['pyspark_code']

        # Step 3: Validate and refine code with LLM (only when heuristics pass)
        use_llm_validation = (
            bool(getattr(self.validator, 'client', None)) and
            self._should_use_llm_validation(parsed_data, stats)
        )

        if use_llm_validation:
            logging.info("Step 3/4: Validating and refining PySpark code with LLM...")
            logging.info("  [INFO] Package complexity detected - using LLM validation")
            try:
                refined_code = self.validator.validate_and_refine_code(
                    pyspark_code=mapping_result['pyspark_code'],
                    package_name=package_name,
                    parsed_json=parsed_data,
                    mapping_details=mapping_result,
                    databricks_mode=True
                )
                with open(pyspark_path, 'w') as f:
                    f.write(refined_code)
                logging.info("  [OK] LLM validation and refinement: COMPLETED")
            except Exception as e:
                logging.warning(f"  [WARNING] LLM validation failed: {e}")
                logging.warning("  [FALLBACK] Using original rule-based code (already saved)")
                refined_code = mapping_result['pyspark_code']
        else:
            logging.info("Step 3/4: Skipping LLM validation - simple package detected")
            logging.info(f"  [INFO] Package is simple (transformations: {stats.get('total_transformations', 0)}, "
                         f"complex logic: {self._has_complex_logic(parsed_data)})")
            refined_code = mapping_result['pyspark_code']
        
        # Save mapping details
        mapping_filename = dtsx_path.stem + "_mapping.json"
        mapping_path = output_base / 'mapping_details' / mapping_filename
        
        result_copy = mapping_result.copy()
        result_copy.pop('pyspark_code', None)
        
        with open(mapping_path, 'w') as f:
            json.dump(result_copy, f, indent=2)
        logging.info(f"  Mapping Details: {mapping_path.name}")
        
        # Step 3: Validate syntax
        logging.info("Step 4/4: Validating Python syntax...")
        import ast
        import re
        
        with open(pyspark_path, 'r') as f:
            code = f.read()
        
        syntax_valid = False
        syntax_errors = []
        code_analysis = self._analyze_final_code(code, stats)
        
        try:
            ast.parse(code)
            syntax_valid = True
            logging.info("  [OK] Syntax validation: PASSED")
        except SyntaxError as e:
            syntax_errors.append({
                'line': e.lineno,
                'message': e.msg,
                'text': e.text
            })
            logging.warning(f"  [WARNING] Syntax validation: FAILED at line {e.lineno}")
        
        return {
            'success': True,
            'package_name': package_name,
            'package_file': dtsx_path.name,
            'parsed_json': str(json_path),
            'pyspark_code': str(pyspark_path),
            'mapping_details': str(mapping_path),
            'mapping_rate': stats['overall_mapping_rate'],
            'syntax_valid': syntax_valid,
            'syntax_errors': syntax_errors,
            'statistics': stats,
            'code_analysis': code_analysis,
            'llm_validated': refined_code != mapping_result['pyspark_code']
        }
    
    def print_summary(self, results: list, output_base: Path):
        """Print conversion summary."""
        if not results:
            logging.info("\nNo files processed.")
            return
        
        successful = [r for r in results if r.get('success', False)]
        failed = [r for r in results if not r.get('success', False)]
        
        logging.info("\n" + "="*60)
        logging.info("CONVERSION SUMMARY")
        logging.info("="*60)
        logging.info(f"Total Packages: {len(results)}")
        logging.info(f"Successful: {len(successful)}")
        logging.info(f"Failed: {len(failed)}")
        
        if successful:
            avg_mapping = sum(r['mapping_rate'] for r in successful) / len(successful)
            syntax_valid = sum(1 for r in successful if r['syntax_valid'])
            logging.info(f"\nAverage Mapping Rate: {avg_mapping:.1f}%")
            logging.info(f"Syntax Valid: {syntax_valid}/{len(successful)}")
        
        logging.info(f"\nGenerated Files:")
        logging.info(f"  Parsed JSON: {output_base}/parsed_json/")
        logging.info(f"  PySpark Code: {output_base}/pyspark_code/")
        logging.info(f"  Mapping Details: {output_base}/mapping_details/")
        logging.info("="*60)

    def _should_use_llm_validation(self, parsed_data: dict, stats: dict) -> bool:
        """Determine if LLM validation should be used."""
        if self._has_complex_logic(parsed_data):
            return True
        return self._count_significant_transformations(parsed_data) > 3

    def _count_significant_transformations(self, parsed_data: dict) -> int:
        """Count transformations excluding pure row-count components."""
        skip_prefixes = ("RC ", "TD ")
        skip_types = {"ROW_COUNT", "RC_INSERT", "RC_UPDATE", "RC_DELETE", "RC_INTERMEDIATE", "TRASH_DESTINATION"}

        count = 0
        for data_flow in parsed_data.get('data_flows', []):
            for transformation in data_flow.get('transformations', []):
                name = transformation.get('name', '')
                transf_type = transformation.get('type', '').upper()

                if name.startswith(skip_prefixes):
                    continue
                if transf_type in skip_types:
                    continue
                count += 1

        return count

    def _has_complex_logic(self, parsed_data: dict) -> bool:
        """Check if package contains complex logic."""
        for data_flow in parsed_data.get('data_flows', []):
            for transformation in data_flow.get('transformations', []):
                transf_type = transformation.get('type', '').upper()

                if transf_type in {'DERIVED_COLUMN', 'LOOKUP', 'MERGE', 'MERGE_JOIN', 'OLEDB_COMMAND', 'SORT'}:
                    return True
                elif transf_type == 'CONDITIONAL_SPLIT':
                    conditions = transformation.get('conditions', [])
                    if len(conditions) > 2:
                        return True
                elif transf_type in ['MERGE', 'MERGE_JOIN']:
                    if transformation.get('join_conditions') or transformation.get('join_keys'):
                        return True

        return False

    def _analyze_final_code(self, code: str, stats: dict) -> dict:
        """Analyze the final PySpark code after LLM validation."""
        todos = re.findall(r'# TODO[^\n]*', code)
        transformations = re.findall(r'# Transformation: ([^\n]+)', code)

        has_llm_patterns = bool(re.search(r'(lit\(|when\(|otherwise\(|col\(["\']|withColumn\(|filter\(|join\(|groupBy\()', code))
        code_lines = [line for line in code.split('\n') if line.strip() and not line.strip().startswith('#')]

        trans_types = {}
        for trans_line in transformations:
            if '(' in trans_line and ')' in trans_line:
                match = re.search(r'\(([^)]+)\)', trans_line)
                if match:
                    trans_type = match.group(1)
                    trans_types[trans_type] = trans_types.get(trans_type, 0) + 1

        completion_rate = 100
        if transformations:
            completion_ratio = 1 - (len(todos) / len(transformations))
            completion_rate = round(max(0.0, min(1.0, completion_ratio)) * 100, 2)

        return {
            'total_lines': len(code.split('\n')),
            'effective_lines': len(code_lines),
            'todo_count': len(todos),
            'transformation_count': len(transformations),
            'transformation_types': trans_types,
            'has_llm_patterns': has_llm_patterns,
            'completion_rate': completion_rate
        }


def main():
    """Main entry point for GUI application."""
    root = tk.Tk()
    app = SSISToPySparkGUI(root)
    root.mainloop()


if __name__ == '__main__':
    main()


