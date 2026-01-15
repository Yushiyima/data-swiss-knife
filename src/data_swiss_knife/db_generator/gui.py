"""GUI for PostgreSQL Table Generator using tkinter."""

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path

import pandas as pd

from .file_reader import read_file, get_sample_data
from .type_detector import analyze_dataframe, PG_TYPES, DATE_FORMATS
from .database import test_connection, create_table, insert_data_copy, get_schemas


class DBGeneratorApp:
    """Main application window for DB Table Generator."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Data Swiss Knife - DB Table Generator")
        self.root.geometry("1000x700")
        self.root.minsize(900, 600)

        # State
        self.df: pd.DataFrame | None = None
        self.file_path: str | None = None
        self.column_analysis: dict = {}
        self.column_widgets: dict = {}
        self.conn_str: str | None = None

        self._create_widgets()

    def _create_widgets(self):
        """Create all GUI widgets."""
        # Main container with padding
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        # === File Selection Section ===
        file_frame = ttk.LabelFrame(main_frame, text="1. Select File", padding="5")
        file_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 10))

        self.file_label = ttk.Label(file_frame, text="No file selected")
        self.file_label.grid(row=0, column=0, sticky="w", padx=5)

        ttk.Button(file_frame, text="Browse...", command=self._browse_file).grid(
            row=0, column=1, padx=5
        )

        self.file_info_label = ttk.Label(file_frame, text="")
        self.file_info_label.grid(row=1, column=0, columnspan=2, sticky="w", padx=5)

        # === Database Connection Section ===
        db_frame = ttk.LabelFrame(main_frame, text="2. PostgreSQL Connection", padding="5")
        db_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 10))

        # Connection fields
        fields = [("Host:", "host", "localhost"), ("Port:", "port", "5432"),
                  ("Database:", "database", ""), ("User:", "user", "postgres"),
                  ("Password:", "password", "")]

        self.db_entries = {}
        for i, (label, key, default) in enumerate(fields):
            ttk.Label(db_frame, text=label).grid(row=0, column=i*2, sticky="e", padx=2)
            entry = ttk.Entry(db_frame, width=12)
            if key == "password":
                entry.config(show="*")
            entry.insert(0, default)
            entry.grid(row=0, column=i*2+1, sticky="w", padx=2)
            self.db_entries[key] = entry

        ttk.Button(db_frame, text="Test Connection", command=self._test_connection).grid(
            row=0, column=10, padx=10
        )

        self.conn_status = ttk.Label(db_frame, text="Not connected", foreground="gray")
        self.conn_status.grid(row=1, column=0, columnspan=11, sticky="w", pady=5)

        # Schema and table name
        ttk.Label(db_frame, text="Schema:").grid(row=2, column=0, sticky="e", padx=2)
        self.schema_combo = ttk.Combobox(db_frame, width=15, values=["public"])
        self.schema_combo.set("public")
        self.schema_combo.grid(row=2, column=1, columnspan=2, sticky="w", padx=2)

        ttk.Label(db_frame, text="Table Name:").grid(row=2, column=3, sticky="e", padx=2)
        self.table_entry = ttk.Entry(db_frame, width=20)
        self.table_entry.grid(row=2, column=4, columnspan=2, sticky="w", padx=2)

        # === Column Configuration Section ===
        col_frame = ttk.LabelFrame(main_frame, text="3. Column Configuration", padding="5")
        col_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(0, 10))
        main_frame.rowconfigure(2, weight=1)

        # Create scrollable frame for columns
        canvas = tk.Canvas(col_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(col_frame, orient="vertical", command=canvas.yview)
        self.columns_frame = ttk.Frame(canvas)

        self.columns_frame.bind(
            "<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=self.columns_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        # Header row
        headers = ["Column Name", "Data Type", "Date Format", "Primary Key", "Index", "Sample Values"]
        for i, header in enumerate(headers):
            ttk.Label(self.columns_frame, text=header, font=("", 9, "bold")).grid(
                row=0, column=i, padx=5, pady=5, sticky="w"
            )

        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        col_frame.columnconfigure(0, weight=1)
        col_frame.rowconfigure(0, weight=1)

        # Mouse wheel scrolling
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # === Action Buttons ===
        action_frame = ttk.Frame(main_frame)
        action_frame.grid(row=3, column=0, columnspan=2, sticky="ew", pady=10)

        ttk.Button(action_frame, text="Create Table Only", command=self._create_table).grid(
            row=0, column=0, padx=5
        )

        ttk.Button(action_frame, text="Create Table & Insert Data",
                   command=self._create_and_insert).grid(row=0, column=1, padx=5)

        self.progress = ttk.Progressbar(action_frame, mode="indeterminate", length=200)
        self.progress.grid(row=0, column=2, padx=20)

        self.status_label = ttk.Label(action_frame, text="Ready")
        self.status_label.grid(row=0, column=3, padx=5)

    def _browse_file(self):
        """Open file browser to select CSV or Excel file."""
        filetypes = [
            ("Data files", "*.csv *.xlsx *.xls"),
            ("CSV files", "*.csv"),
            ("Excel files", "*.xlsx *.xls"),
        ]
        filepath = filedialog.askopenfilename(filetypes=filetypes)
        if filepath:
            self._load_file(filepath)

    def _load_file(self, filepath: str):
        """Load and analyze the selected file."""
        try:
            self.file_path = filepath
            self.file_label.config(text=Path(filepath).name)

            # Read file
            self.df = read_file(filepath)
            rows, cols = self.df.shape
            self.file_info_label.config(text=f"Loaded: {rows:,} rows, {cols} columns")

            # Set default table name from filename
            table_name = Path(filepath).stem.lower().replace(" ", "_").replace("-", "_")
            self.table_entry.delete(0, tk.END)
            self.table_entry.insert(0, table_name)

            # Analyze columns
            self.column_analysis = analyze_dataframe(self.df)
            self._populate_column_config()

            self.status_label.config(text="File loaded successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load file: {e}")

    def _populate_column_config(self):
        """Populate column configuration widgets."""
        # Clear existing widgets
        for widget in self.columns_frame.winfo_children():
            if int(widget.grid_info().get("row", 0)) > 0:
                widget.destroy()

        self.column_widgets = {}
        pg_type_options = list(PG_TYPES.values())

        for i, (col_name, info) in enumerate(self.column_analysis.items(), start=1):
            row_widgets = {}

            # Column name
            ttk.Label(self.columns_frame, text=col_name).grid(
                row=i, column=0, padx=5, pady=2, sticky="w"
            )

            # Data type dropdown
            type_combo = ttk.Combobox(self.columns_frame, values=pg_type_options, width=18)
            type_combo.set(info["pg_type"])
            type_combo.grid(row=i, column=1, padx=5, pady=2)
            row_widgets["type"] = type_combo

            # Date format dropdown (only for date columns)
            date_combo = ttk.Combobox(self.columns_frame, values=[""] + DATE_FORMATS, width=18)
            if info["date_format"]:
                date_combo.set(info["date_format"])
            date_combo.grid(row=i, column=2, padx=5, pady=2)
            row_widgets["date_format"] = date_combo

            # Primary key checkbox
            pk_var = tk.BooleanVar()
            pk_check = ttk.Checkbutton(self.columns_frame, variable=pk_var)
            pk_check.grid(row=i, column=3, padx=5, pady=2)
            row_widgets["pk"] = pk_var

            # Index checkbox
            idx_var = tk.BooleanVar()
            idx_check = ttk.Checkbutton(self.columns_frame, variable=idx_var)
            idx_check.grid(row=i, column=4, padx=5, pady=2)
            row_widgets["index"] = idx_var

            # Sample values
            samples = ", ".join(str(v)[:20] for v in info["sample_values"][:3])
            ttk.Label(self.columns_frame, text=samples, foreground="gray").grid(
                row=i, column=5, padx=5, pady=2, sticky="w"
            )

            self.column_widgets[col_name] = row_widgets

    def _test_connection(self):
        """Test the database connection."""
        success, message = test_connection(
            host=self.db_entries["host"].get(),
            port=int(self.db_entries["port"].get()),
            database=self.db_entries["database"].get(),
            user=self.db_entries["user"].get(),
            password=self.db_entries["password"].get(),
        )

        if success:
            self.conn_status.config(text=message, foreground="green")
            self.conn_str = (
                f"host={self.db_entries['host'].get()} "
                f"port={self.db_entries['port'].get()} "
                f"dbname={self.db_entries['database'].get()} "
                f"user={self.db_entries['user'].get()} "
                f"password={self.db_entries['password'].get()}"
            )
            # Update schema list
            schemas = get_schemas(self.conn_str)
            self.schema_combo["values"] = schemas
        else:
            self.conn_status.config(text=f"Failed: {message}", foreground="red")
            self.conn_str = None

    def _get_column_config(self) -> tuple[list[dict], str | None, list[str], dict[str, str]]:
        """Get current column configuration from widgets."""
        columns = []
        primary_key = None
        indexes = []
        date_formats = {}

        for col_name, widgets in self.column_widgets.items():
            col_config = {
                "name": col_name,
                "pg_type": widgets["type"].get(),
            }
            columns.append(col_config)

            if widgets["pk"].get():
                primary_key = col_name

            if widgets["index"].get():
                indexes.append(col_name)

            date_fmt = widgets["date_format"].get()
            if date_fmt:
                date_formats[col_name] = date_fmt

        return columns, primary_key, indexes, date_formats

    def _create_table(self):
        """Create the table without inserting data."""
        if not self._validate():
            return

        columns, primary_key, indexes, _ = self._get_column_config()

        self.progress.start()
        self.status_label.config(text="Creating table...")
        self.root.update()

        success, message = create_table(
            conn_str=self.conn_str,
            schema=self.schema_combo.get(),
            table_name=self.table_entry.get(),
            columns=columns,
            primary_key=primary_key,
            indexes=indexes,
        )

        self.progress.stop()

        if success:
            self.status_label.config(text=message)
            messagebox.showinfo("Success", message)
        else:
            self.status_label.config(text="Failed")
            messagebox.showerror("Error", message)

    def _create_and_insert(self):
        """Create table and insert all data."""
        if not self._validate():
            return

        columns, primary_key, indexes, date_formats = self._get_column_config()

        self.progress.start()
        self.status_label.config(text="Creating table...")
        self.root.update()

        # Create table first
        success, message = create_table(
            conn_str=self.conn_str,
            schema=self.schema_combo.get(),
            table_name=self.table_entry.get(),
            columns=columns,
            primary_key=primary_key,
            indexes=indexes,
        )

        if not success:
            self.progress.stop()
            self.status_label.config(text="Failed")
            messagebox.showerror("Error", f"Failed to create table: {message}")
            return

        # Insert data
        self.status_label.config(text="Inserting data (using COPY)...")
        self.root.update()

        success, message, row_count = insert_data_copy(
            conn_str=self.conn_str,
            schema=self.schema_combo.get(),
            table_name=self.table_entry.get(),
            df=self.df,
            date_formats=date_formats,
        )

        self.progress.stop()

        if success:
            self.status_label.config(text=f"Inserted {row_count:,} rows")
            messagebox.showinfo("Success", f"Table created and {row_count:,} rows inserted!")
        else:
            self.status_label.config(text="Insert failed")
            messagebox.showerror("Error", f"Failed to insert data: {message}")

    def _validate(self) -> bool:
        """Validate inputs before operation."""
        if self.df is None:
            messagebox.showwarning("Warning", "Please select a file first")
            return False

        if not self.conn_str:
            messagebox.showwarning("Warning", "Please test database connection first")
            return False

        if not self.table_entry.get().strip():
            messagebox.showwarning("Warning", "Please enter a table name")
            return False

        return True


def run_app():
    """Launch the DB Generator GUI application."""
    root = tk.Tk()
    app = DBGeneratorApp(root)
    root.mainloop()


if __name__ == "__main__":
    run_app()
