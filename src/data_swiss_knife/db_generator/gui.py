"""Modern GUI for PostgreSQL Table Generator using customtkinter."""

import json
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path

import customtkinter as ctk
import pandas as pd

from .file_reader import read_file
from .type_detector import analyze_dataframe, PG_TYPES, DATE_FORMATS
from .database import test_connection, create_table, insert_data_copy, get_schemas

# Set appearance
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

# Config file for saved connections
CONFIG_FILE = Path.home() / ".data_swiss_knife" / "connections.json"


def load_saved_connections() -> dict:
    """Load saved connections from config file."""
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_connection(name: str, conn_data: dict):
    """Save a connection to config file."""
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    connections = load_saved_connections()
    connections[name] = conn_data
    CONFIG_FILE.write_text(json.dumps(connections, indent=2))


def delete_connection(name: str):
    """Delete a saved connection."""
    connections = load_saved_connections()
    if name in connections:
        del connections[name]
        CONFIG_FILE.write_text(json.dumps(connections, indent=2))


class ModernCard(ctk.CTkFrame):
    """A modern card component with title."""

    def __init__(self, master, title: str, **kwargs):
        super().__init__(master, corner_radius=12, **kwargs)

        self.title_label = ctk.CTkLabel(
            self,
            text=title,
            font=ctk.CTkFont(size=14, weight="bold"),
            anchor="w",
        )
        self.title_label.pack(fill="x", padx=16, pady=(16, 8))

        self.content = ctk.CTkFrame(self, fg_color="transparent")
        self.content.pack(fill="both", expand=True, padx=16, pady=(0, 16))


class ColumnRow(ctk.CTkFrame):
    """A row for column configuration with proper alignment."""

    # Column widths for alignment
    COL_WIDTHS = [180, 150, 170, 50, 60, 150]

    def __init__(self, master, col_name: str, col_info: dict, **kwargs):
        super().__init__(master, fg_color="transparent", height=40, **kwargs)

        self.col_name = col_name
        self.col_info = col_info
        self.is_date_type = col_info["detected_type"] in ("DATE", "TIMESTAMP")

        # Use grid for proper alignment
        for i in range(6):
            self.grid_columnconfigure(i, minsize=self.COL_WIDTHS[i])

        # Column name (col 0)
        self.name_label = ctk.CTkLabel(
            self,
            text=col_name,
            font=ctk.CTkFont(size=13),
            anchor="w",
        )
        self.name_label.grid(row=0, column=0, sticky="w", padx=(0, 5))

        # Data type dropdown (col 1)
        self.type_var = ctk.StringVar(value=col_info["pg_type"])
        self.type_menu = ctk.CTkOptionMenu(
            self,
            values=list(PG_TYPES.values()),
            variable=self.type_var,
            width=140,
            height=30,
            font=ctk.CTkFont(size=12),
            command=self._on_type_change,
        )
        self.type_menu.grid(row=0, column=1, sticky="w", padx=5)

        # Date format dropdown (col 2) - only for date types
        self.date_var = ctk.StringVar(value=col_info.get("date_format") or "Auto")
        self.date_menu = ctk.CTkOptionMenu(
            self,
            values=["Auto"] + DATE_FORMATS,
            variable=self.date_var,
            width=160,
            height=30,
            font=ctk.CTkFont(size=12),
        )
        self.date_menu.grid(row=0, column=2, sticky="w", padx=5)

        # Show/hide date format based on type
        if not self.is_date_type:
            self.date_menu.configure(state="disabled", fg_color="gray30")

        # Primary key checkbox (col 3)
        self.pk_var = ctk.BooleanVar(value=False)
        self.pk_check = ctk.CTkCheckBox(
            self,
            text="",
            variable=self.pk_var,
            width=30,
            checkbox_width=20,
            checkbox_height=20,
        )
        self.pk_check.grid(row=0, column=3, padx=5)

        # Index checkbox (col 4)
        self.idx_var = ctk.BooleanVar(value=False)
        self.idx_check = ctk.CTkCheckBox(
            self,
            text="",
            variable=self.idx_var,
            width=30,
            checkbox_width=20,
            checkbox_height=20,
        )
        self.idx_check.grid(row=0, column=4, padx=5)

        # Sample values (col 5)
        samples = ", ".join(str(v)[:20] for v in col_info.get("sample_values", [])[:2])
        self.sample_label = ctk.CTkLabel(
            self,
            text=samples if samples else "-",
            font=ctk.CTkFont(size=11),
            text_color="gray",
            anchor="w",
        )
        self.sample_label.grid(row=0, column=5, sticky="w", padx=5)

    def _on_type_change(self, value: str):
        """Handle data type change - enable/disable date format."""
        is_date = value in ("DATE", "TIMESTAMP")
        if is_date:
            self.date_menu.configure(state="normal", fg_color=["#3B8ED0", "#1F6AA5"])
        else:
            self.date_menu.configure(state="disabled", fg_color="gray30")
            self.date_var.set("Auto")


class DBGeneratorApp(ctk.CTk):
    """Modern DB Table Generator application."""

    def __init__(self):
        super().__init__()

        self.title("Data Swiss Knife - DB Table Generator")
        self.geometry("1100x750")
        self.minsize(1000, 650)

        # State
        self.df: pd.DataFrame | None = None
        self.file_path: str | None = None
        self.column_analysis: dict = {}
        self.column_rows: list[ColumnRow] = []
        self.conn_str: str | None = None

        self._create_ui()
        self._load_saved_connections()

    def _create_ui(self):
        """Create the modern UI."""
        # Configure grid
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)

        # Header
        header = ctk.CTkFrame(self, fg_color="transparent", height=60)
        header.grid(row=0, column=0, sticky="ew", padx=20, pady=(20, 10))

        title = ctk.CTkLabel(
            header,
            text="PostgreSQL Table Generator",
            font=ctk.CTkFont(size=24, weight="bold"),
        )
        title.pack(side="left")

        # Theme toggle
        theme_menu = ctk.CTkSegmentedButton(
            header,
            values=["Light", "Dark"],
            command=self._toggle_theme,
            font=ctk.CTkFont(size=12),
        )
        theme_menu.set("Dark")
        theme_menu.pack(side="right")

        # Main container with two columns
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.grid(row=1, column=0, sticky="ew", padx=20)
        main.grid_columnconfigure((0, 1), weight=1)

        # Left column - File
        left_col = ctk.CTkFrame(main, fg_color="transparent")
        left_col.grid(row=0, column=0, sticky="nsew", padx=(0, 10))

        # File selection card
        file_card = ModernCard(left_col, "1. Select Data File")
        file_card.pack(fill="x", pady=(0, 10))

        file_row = ctk.CTkFrame(file_card.content, fg_color="transparent")
        file_row.pack(fill="x")

        self.file_label = ctk.CTkLabel(
            file_row,
            text="No file selected",
            font=ctk.CTkFont(size=13),
            text_color="gray",
        )
        self.file_label.pack(side="left", fill="x", expand=True)

        self.browse_btn = ctk.CTkButton(
            file_row,
            text="Browse",
            command=self._browse_file,
            width=100,
            height=36,
            font=ctk.CTkFont(size=13),
        )
        self.browse_btn.pack(side="right")

        self.file_info = ctk.CTkLabel(
            file_card.content,
            text="",
            font=ctk.CTkFont(size=12),
            text_color="#4CAF50",
        )
        self.file_info.pack(anchor="w", pady=(8, 0))

        # Right column - Connection
        right_col = ctk.CTkFrame(main, fg_color="transparent")
        right_col.grid(row=0, column=1, sticky="nsew", padx=(10, 0))

        # Database connection card
        db_card = ModernCard(right_col, "2. Database Connection")
        db_card.pack(fill="x")

        # Saved connections row
        saved_row = ctk.CTkFrame(db_card.content, fg_color="transparent")
        saved_row.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(
            saved_row,
            text="Saved:",
            font=ctk.CTkFont(size=12),
        ).pack(side="left")

        self.saved_conn_menu = ctk.CTkOptionMenu(
            saved_row,
            values=["(Select)"],
            width=150,
            height=30,
            font=ctk.CTkFont(size=12),
            command=self._load_connection,
        )
        self.saved_conn_menu.pack(side="left", padx=(8, 5))

        self.save_conn_btn = ctk.CTkButton(
            saved_row,
            text="Save",
            width=60,
            height=30,
            font=ctk.CTkFont(size=12),
            command=self._save_current_connection,
        )
        self.save_conn_btn.pack(side="left", padx=2)

        self.delete_conn_btn = ctk.CTkButton(
            saved_row,
            text="Delete",
            width=60,
            height=30,
            font=ctk.CTkFont(size=12),
            fg_color="#F44336",
            hover_color="#D32F2F",
            command=self._delete_current_connection,
        )
        self.delete_conn_btn.pack(side="left", padx=2)

        # Connection fields in grid
        conn_grid = ctk.CTkFrame(db_card.content, fg_color="transparent")
        conn_grid.pack(fill="x")
        conn_grid.grid_columnconfigure((1, 3), weight=1)

        fields = [
            ("Host", "host", "localhost", 0, 0),
            ("Port", "port", "5432", 0, 2),
            ("Database", "database", "", 1, 0),
            ("User", "user", "postgres", 1, 2),
            ("Password", "password", "", 2, 0),
        ]

        self.db_entries = {}
        for label, key, default, row, col in fields:
            ctk.CTkLabel(
                conn_grid,
                text=label,
                font=ctk.CTkFont(size=12),
            ).grid(row=row, column=col, sticky="e", padx=(0, 8), pady=4)

            entry = ctk.CTkEntry(
                conn_grid,
                height=32,
                font=ctk.CTkFont(size=12),
                show="*" if key == "password" else "",
            )
            entry.insert(0, default)
            entry.grid(row=row, column=col + 1, sticky="ew", pady=4, padx=(0, 10))
            self.db_entries[key] = entry

        # Connect button and status
        conn_row = ctk.CTkFrame(db_card.content, fg_color="transparent")
        conn_row.pack(fill="x", pady=(12, 0))

        self.connect_btn = ctk.CTkButton(
            conn_row,
            text="Test Connection",
            command=self._test_connection,
            width=140,
            height=36,
            font=ctk.CTkFont(size=13),
        )
        self.connect_btn.pack(side="left")

        self.conn_status = ctk.CTkLabel(
            conn_row,
            text="Not connected",
            font=ctk.CTkFont(size=12),
            text_color="gray",
        )
        self.conn_status.pack(side="left", padx=15)

        # Columns section (full width)
        columns_card = ModernCard(self, "3. Configure Columns")
        columns_card.grid(row=2, column=0, sticky="nsew", padx=20, pady=10)

        # Table settings row
        table_row = ctk.CTkFrame(columns_card.content, fg_color="transparent")
        table_row.pack(fill="x", pady=(0, 12))

        ctk.CTkLabel(
            table_row,
            text="Schema:",
            font=ctk.CTkFont(size=12),
        ).pack(side="left")

        self.schema_menu = ctk.CTkOptionMenu(
            table_row,
            values=["public"],
            width=140,
            height=32,
            font=ctk.CTkFont(size=12),
        )
        self.schema_menu.pack(side="left", padx=(8, 5))

        # Refresh schemas button
        self.refresh_schema_btn = ctk.CTkButton(
            table_row,
            text="Refresh",
            width=70,
            height=32,
            font=ctk.CTkFont(size=12),
            command=self._refresh_schemas,
        )
        self.refresh_schema_btn.pack(side="left", padx=(0, 20))

        ctk.CTkLabel(
            table_row,
            text="Table Name:",
            font=ctk.CTkFont(size=12),
        ).pack(side="left")

        self.table_entry = ctk.CTkEntry(
            table_row,
            width=200,
            height=32,
            font=ctk.CTkFont(size=12),
            placeholder_text="Enter table name",
        )
        self.table_entry.pack(side="left", padx=8)

        # Column headers with proper alignment
        header_frame = ctk.CTkFrame(columns_card.content, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 8))

        headers = [
            ("Column Name", 180),
            ("Data Type", 150),
            ("Date Format", 170),
            ("PK", 50),
            ("Index", 60),
            ("Sample Values", 150),
        ]

        for i, (text, width) in enumerate(headers):
            header_frame.grid_columnconfigure(i, minsize=width)
            ctk.CTkLabel(
                header_frame,
                text=text,
                font=ctk.CTkFont(size=12, weight="bold"),
                anchor="w",
            ).grid(row=0, column=i, sticky="w", padx=5)

        # Scrollable columns area
        self.columns_scroll = ctk.CTkScrollableFrame(
            columns_card.content,
            fg_color="transparent",
        )
        self.columns_scroll.pack(fill="both", expand=True)

        # Placeholder text
        self.placeholder = ctk.CTkLabel(
            self.columns_scroll,
            text="Select a CSV or Excel file to configure columns",
            font=ctk.CTkFont(size=14),
            text_color="gray",
        )
        self.placeholder.pack(pady=40)

        # Action bar
        action_bar = ctk.CTkFrame(self, fg_color="transparent", height=70)
        action_bar.grid(row=3, column=0, sticky="ew", padx=20, pady=(0, 20))

        self.create_btn = ctk.CTkButton(
            action_bar,
            text="Create Table",
            command=self._create_table,
            width=140,
            height=42,
            font=ctk.CTkFont(size=14),
            fg_color="#2196F3",
            hover_color="#1976D2",
        )
        self.create_btn.pack(side="left", padx=(0, 10))

        self.insert_btn = ctk.CTkButton(
            action_bar,
            text="Create & Insert Data",
            command=self._create_and_insert,
            width=180,
            height=42,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#4CAF50",
            hover_color="#388E3C",
        )
        self.insert_btn.pack(side="left")

        self.progress = ctk.CTkProgressBar(action_bar, width=200, height=8)
        self.progress.pack(side="left", padx=30)
        self.progress.set(0)

        self.status_label = ctk.CTkLabel(
            action_bar,
            text="Ready",
            font=ctk.CTkFont(size=13),
        )
        self.status_label.pack(side="left")

    def _toggle_theme(self, value: str):
        """Toggle between light and dark theme."""
        ctk.set_appearance_mode(value.lower())

    def _load_saved_connections(self):
        """Load saved connections into dropdown."""
        connections = load_saved_connections()
        if connections:
            self.saved_conn_menu.configure(values=["(Select)"] + list(connections.keys()))
        else:
            self.saved_conn_menu.configure(values=["(Select)"])

    def _load_connection(self, name: str):
        """Load a saved connection into fields."""
        if name == "(Select)":
            return

        connections = load_saved_connections()
        if name in connections:
            conn = connections[name]
            for key, entry in self.db_entries.items():
                entry.delete(0, "end")
                entry.insert(0, conn.get(key, ""))

    def _save_current_connection(self):
        """Save current connection settings."""
        dialog = ctk.CTkInputDialog(
            text="Enter a name for this connection:",
            title="Save Connection",
        )
        name = dialog.get_input()

        if name and name.strip():
            conn_data = {key: entry.get() for key, entry in self.db_entries.items()}
            save_connection(name.strip(), conn_data)
            self._load_saved_connections()
            self.saved_conn_menu.set(name.strip())
            messagebox.showinfo("Saved", f"Connection '{name}' saved successfully!")

    def _delete_current_connection(self):
        """Delete the currently selected saved connection."""
        name = self.saved_conn_menu.get()
        if name == "(Select)":
            messagebox.showwarning("Warning", "Please select a connection to delete")
            return

        if messagebox.askyesno("Confirm", f"Delete connection '{name}'?"):
            delete_connection(name)
            self._load_saved_connections()
            self.saved_conn_menu.set("(Select)")
            messagebox.showinfo("Deleted", f"Connection '{name}' deleted")

    def _refresh_schemas(self):
        """Refresh the schema list from database."""
        if not self.conn_str:
            messagebox.showwarning("Warning", "Please test connection first")
            return

        schemas = get_schemas(self.conn_str)
        self.schema_menu.configure(values=schemas)
        self.status_label.configure(text=f"Loaded {len(schemas)} schemas")

    def _browse_file(self):
        """Open file browser."""
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
            filename = Path(filepath).name
            self.file_label.configure(text=filename, text_color=("gray10", "gray90"))

            # Read file
            self.df = read_file(filepath)
            rows, cols = self.df.shape
            self.file_info.configure(text=f"{rows:,} rows x {cols} columns")

            # Set default table name
            table_name = Path(filepath).stem.lower().replace(" ", "_").replace("-", "_")
            self.table_entry.delete(0, "end")
            self.table_entry.insert(0, table_name)

            # Analyze and populate columns
            self.column_analysis = analyze_dataframe(self.df)
            self._populate_columns()

            self.status_label.configure(text="File loaded successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load file: {e}")

    def _populate_columns(self):
        """Populate column configuration rows."""
        # Clear existing
        self.placeholder.pack_forget()
        for row in self.column_rows:
            row.destroy()
        self.column_rows.clear()

        # Create rows
        for col_name, col_info in self.column_analysis.items():
            row = ColumnRow(self.columns_scroll, col_name, col_info)
            row.pack(fill="x", pady=4)
            self.column_rows.append(row)

    def _test_connection(self):
        """Test the database connection."""
        self.connect_btn.configure(state="disabled", text="Testing...")
        self.update()

        success, message = test_connection(
            host=self.db_entries["host"].get(),
            port=int(self.db_entries["port"].get() or 5432),
            database=self.db_entries["database"].get(),
            user=self.db_entries["user"].get(),
            password=self.db_entries["password"].get(),
        )

        self.connect_btn.configure(state="normal", text="Test Connection")

        if success:
            self.conn_status.configure(text="Connected", text_color="#4CAF50")
            self.conn_str = (
                f"host={self.db_entries['host'].get()} "
                f"port={self.db_entries['port'].get()} "
                f"dbname={self.db_entries['database'].get()} "
                f"user={self.db_entries['user'].get()} "
                f"password={self.db_entries['password'].get()}"
            )
            # Update schemas
            schemas = get_schemas(self.conn_str)
            self.schema_menu.configure(values=schemas)
        else:
            self.conn_status.configure(text=f"Failed: {message[:40]}", text_color="#F44336")
            self.conn_str = None

    def _get_column_config(self):
        """Get column configuration from rows."""
        columns = []
        primary_key = None
        indexes = []
        date_formats = {}

        for row in self.column_rows:
            columns.append({
                "name": row.col_name,
                "pg_type": row.type_var.get(),
            })

            if row.pk_var.get():
                primary_key = row.col_name

            if row.idx_var.get():
                indexes.append(row.col_name)

            date_fmt = row.date_var.get()
            if date_fmt and date_fmt != "Auto":
                date_formats[row.col_name] = date_fmt

        return columns, primary_key, indexes, date_formats

    def _validate(self) -> bool:
        """Validate inputs."""
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

    def _create_table(self):
        """Create table without data."""
        if not self._validate():
            return

        columns, primary_key, indexes, _ = self._get_column_config()

        self.progress.set(0.3)
        self.status_label.configure(text="Creating table...")
        self.update()

        success, message = create_table(
            conn_str=self.conn_str,
            schema=self.schema_menu.get(),
            table_name=self.table_entry.get(),
            columns=columns,
            primary_key=primary_key,
            indexes=indexes,
        )

        self.progress.set(1.0 if success else 0)

        if success:
            self.status_label.configure(text="Table created!")
            messagebox.showinfo("Success", message)
        else:
            self.status_label.configure(text="Failed")
            messagebox.showerror("Error", message)

    def _create_and_insert(self):
        """Create table and insert data."""
        if not self._validate():
            return

        columns, primary_key, indexes, date_formats = self._get_column_config()

        self.progress.set(0.2)
        self.status_label.configure(text="Creating table...")
        self.update()

        # Create table
        success, message = create_table(
            conn_str=self.conn_str,
            schema=self.schema_menu.get(),
            table_name=self.table_entry.get(),
            columns=columns,
            primary_key=primary_key,
            indexes=indexes,
        )

        if not success:
            self.progress.set(0)
            self.status_label.configure(text="Failed")
            messagebox.showerror("Error", f"Failed to create table: {message}")
            return

        # Insert data
        self.progress.set(0.5)
        self.status_label.configure(text="Inserting data...")
        self.update()

        success, message, row_count = insert_data_copy(
            conn_str=self.conn_str,
            schema=self.schema_menu.get(),
            table_name=self.table_entry.get(),
            df=self.df,
            date_formats=date_formats,
        )

        self.progress.set(1.0 if success else 0)

        if success:
            self.status_label.configure(text=f"Inserted {row_count:,} rows")
            messagebox.showinfo("Success", f"Table created and {row_count:,} rows inserted!")
        else:
            self.status_label.configure(text="Insert failed")
            messagebox.showerror("Error", f"Failed to insert data: {message}")


def run_app():
    """Launch the DB Generator GUI application."""
    app = DBGeneratorApp()
    app.mainloop()


if __name__ == "__main__":
    run_app()
