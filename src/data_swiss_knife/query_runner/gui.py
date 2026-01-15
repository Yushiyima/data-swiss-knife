"""Modern GUI for Parametric Query Runner using customtkinter."""

import threading
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path

import customtkinter as ctk
import pandas as pd

from ..db_generator.gui import load_saved_connections, save_connection, ModernCard
from ..db_generator.database import get_schemas, test_connection
from .executor import ThreadedQueryExecutor, extract_parameters, execute_param_query, ExecutionStats
from .parameters import ParameterManager, PARAM_TYPES, DATE_FORMATS
from .output import export_to_csv, export_to_excel, insert_to_table, create_and_insert

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


class ParameterPanel(ctk.CTkFrame):
    """Panel for managing a single parameter with type support."""

    def __init__(self, master, param_name: str, manager: ParameterManager,
                 conn_str_getter, on_delete, on_update, **kwargs):
        super().__init__(master, corner_radius=8, **kwargs)

        self.param_name = param_name
        self.manager = manager
        self.conn_str_getter = conn_str_getter
        self.on_delete = on_delete
        self.on_update = on_update

        self.grid_columnconfigure(1, weight=1)

        # Header: name, type, delete
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.grid(row=0, column=0, columnspan=3, sticky="ew", padx=10, pady=(10, 5))

        ctk.CTkLabel(
            header,
            text=f":{param_name}",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#4CAF50",
        ).pack(side="left")

        # Type selector
        ctk.CTkLabel(header, text="Type:", font=ctk.CTkFont(size=11)).pack(side="left", padx=(20, 5))

        self.type_var = ctk.StringVar(value="text")
        self.type_menu = ctk.CTkOptionMenu(
            header,
            values=PARAM_TYPES,
            variable=self.type_var,
            width=80,
            height=28,
            command=self._on_type_change,
        )
        self.type_menu.pack(side="left")

        # Date format (only visible for date type)
        self.date_format_label = ctk.CTkLabel(header, text="Format:", font=ctk.CTkFont(size=11))
        self.date_format_var = ctk.StringVar(value="%Y-%m-%d")
        self.date_format_menu = ctk.CTkOptionMenu(
            header,
            values=DATE_FORMATS,
            variable=self.date_format_var,
            width=100,
            height=28,
        )

        ctk.CTkButton(
            header,
            text="X",
            width=30,
            height=28,
            fg_color="#F44336",
            hover_color="#D32F2F",
            command=lambda: self.on_delete(param_name),
        ).pack(side="right")

        # Source type tabs
        source_frame = ctk.CTkFrame(self, fg_color="transparent")
        source_frame.grid(row=1, column=0, columnspan=3, sticky="ew", padx=10, pady=5)

        self.source_var = ctk.StringVar(value="manual")

        ctk.CTkRadioButton(
            source_frame, text="Manual", variable=self.source_var,
            value="manual", command=self._on_source_change,
        ).pack(side="left", padx=(0, 15))

        ctk.CTkRadioButton(
            source_frame, text="Range", variable=self.source_var,
            value="range", command=self._on_source_change,
        ).pack(side="left", padx=(0, 15))

        ctk.CTkRadioButton(
            source_frame, text="From Query", variable=self.source_var,
            value="query", command=self._on_source_change,
        ).pack(side="left")

        # Manual input frame
        self.manual_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.manual_frame.grid(row=2, column=0, columnspan=3, sticky="ew", padx=10, pady=5)

        self.values_entry = ctk.CTkEntry(
            self.manual_frame,
            placeholder_text="Enter values separated by comma",
            height=32,
        )
        self.values_entry.pack(side="left", fill="x", expand=True, padx=(0, 5))

        ctk.CTkButton(
            self.manual_frame, text="Set", width=60, height=32,
            command=self._set_manual_values,
        ).pack(side="left")

        # Range input frame
        self.range_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.range_frame.grid(row=3, column=0, columnspan=3, sticky="ew", padx=10, pady=5)

        ctk.CTkLabel(self.range_frame, text="From:").pack(side="left")
        self.range_start = ctk.CTkEntry(self.range_frame, width=120, height=32,
                                         placeholder_text="Start (e.g. 2024-01-01)")
        self.range_start.pack(side="left", padx=(5, 15))

        ctk.CTkLabel(self.range_frame, text="To:").pack(side="left")
        self.range_end = ctk.CTkEntry(self.range_frame, width=120, height=32,
                                       placeholder_text="End (e.g. 2024-12-31)")
        self.range_end.pack(side="left", padx=(5, 10))

        ctk.CTkButton(
            self.range_frame, text="Generate", width=80, height=32,
            command=self._generate_range,
        ).pack(side="left")

        self.range_frame.grid_remove()

        # Query input frame
        self.query_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.query_frame.grid(row=4, column=0, columnspan=3, sticky="ew", padx=10, pady=5)

        self.query_text = ctk.CTkTextbox(self.query_frame, height=60, font=ctk.CTkFont(size=12))
        self.query_text.pack(side="left", fill="x", expand=True, padx=(0, 5))

        ctk.CTkButton(
            self.query_frame, text="Run", width=60, height=32,
            command=self._run_query,
        ).pack(side="left")

        self.query_frame.grid_remove()

        # Values display
        self.values_label = ctk.CTkLabel(
            self, text="Values: (none)",
            font=ctk.CTkFont(size=11), text_color="gray",
        )
        self.values_label.grid(row=5, column=0, columnspan=3, sticky="w", padx=10, pady=(5, 10))

    def _on_type_change(self, value: str):
        """Handle parameter type change."""
        param = self.manager.get_parameter(self.param_name)
        if param:
            param.param_type = value
            param.clear_values()
            self._update_values_display()

        # Show/hide date format
        if value == "date":
            self.date_format_label.pack(side="left", padx=(15, 5))
            self.date_format_menu.pack(side="left")
        else:
            self.date_format_label.pack_forget()
            self.date_format_menu.pack_forget()

        self.on_update()

    def _on_source_change(self):
        """Handle source type change."""
        source = self.source_var.get()
        self.manual_frame.grid_remove()
        self.range_frame.grid_remove()
        self.query_frame.grid_remove()

        if source == "manual":
            self.manual_frame.grid()
        elif source == "range":
            self.range_frame.grid()
        elif source == "query":
            self.query_frame.grid()

    def _set_manual_values(self):
        """Set values from manual input."""
        text = self.values_entry.get().strip()
        if text:
            values = [v.strip() for v in text.split(",") if v.strip()]
            param = self.manager.get_parameter(self.param_name)
            if param:
                param.date_format = self.date_format_var.get()
                param.set_values(values)
                param.source_type = "manual"
                self._update_values_display()
        self.on_update()

    def _generate_range(self):
        """Generate values from range."""
        start = self.range_start.get().strip()
        end = self.range_end.get().strip()

        if not start or not end:
            messagebox.showwarning("Warning", "Please enter start and end values")
            return

        param = self.manager.get_parameter(self.param_name)
        if param:
            param.date_format = self.date_format_var.get()
            param.set_from_range(start, end)
            self._update_values_display()

            if param.values:
                messagebox.showinfo("Success", f"Generated {len(param.values)} values")
            else:
                messagebox.showwarning("Warning", "Could not generate range. Check format.")
        self.on_update()

    def _run_query(self):
        """Run query to get parameter values."""
        conn_str = self.conn_str_getter()
        if not conn_str:
            messagebox.showwarning("Warning", "Please connect to database first")
            return

        query = self.query_text.get("1.0", "end").strip()
        if not query:
            messagebox.showwarning("Warning", "Please enter a query")
            return

        try:
            df = execute_param_query(conn_str, query, {})
            if df.empty:
                messagebox.showinfo("Info", "Query returned no results")
                return

            values = df.iloc[:, 0].dropna().unique().tolist()
            param = self.manager.get_parameter(self.param_name)
            if param:
                param.set_values_raw(values)
                param.source_type = "query"
                param.source_query = query
                self._update_values_display()
                messagebox.showinfo("Success", f"Loaded {len(values)} values")

        except Exception as e:
            messagebox.showerror("Error", str(e))
        self.on_update()

    def _update_values_display(self):
        """Update the values display label."""
        param = self.manager.get_parameter(self.param_name)
        if param:
            display = param.get_display_values(5)
            self.values_label.configure(text=f"Values ({len(param.values)}): {display}")
        else:
            self.values_label.configure(text="Values: (none)")


class QueryRunnerApp(ctk.CTk):
    """Parametric Query Runner application."""

    def __init__(self):
        super().__init__()

        self.title("Data Swiss Knife - Query Runner")
        self.geometry("1200x900")
        self.minsize(1100, 800)

        # State
        self.conn_str: str | None = None
        self.param_manager = ParameterManager()
        self.param_panels: dict[str, ParameterPanel] = {}
        self.results_df: pd.DataFrame | None = None
        self.is_running = False

        self._create_ui()
        self._load_saved_connections()

    def _create_ui(self):
        """Create the UI."""
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # Header
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.grid(row=0, column=0, sticky="ew", padx=20, pady=(20, 10))

        ctk.CTkLabel(
            header, text="Parametric Query Runner",
            font=ctk.CTkFont(size=24, weight="bold"),
        ).pack(side="left")

        theme_menu = ctk.CTkSegmentedButton(
            header, values=["Light", "Dark"],
            command=lambda v: ctk.set_appearance_mode(v.lower()),
        )
        theme_menu.set("Dark")
        theme_menu.pack(side="right")

        # Main content
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.grid(row=1, column=0, sticky="nsew", padx=20, pady=10)
        main.grid_columnconfigure(0, weight=1)
        main.grid_columnconfigure(1, weight=1)
        main.grid_rowconfigure(0, weight=1)

        # Left column
        left_col = ctk.CTkFrame(main, fg_color="transparent")
        left_col.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        left_col.grid_rowconfigure(1, weight=1)

        # Connection card
        conn_card = ModernCard(left_col, "1. Database Connection")
        conn_card.pack(fill="x", pady=(0, 10))

        saved_row = ctk.CTkFrame(conn_card.content, fg_color="transparent")
        saved_row.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(saved_row, text="Saved:").pack(side="left")
        self.saved_conn_menu = ctk.CTkOptionMenu(
            saved_row, values=["(Select)"], width=200,
            command=self._load_connection,
        )
        self.saved_conn_menu.pack(side="left", padx=8)

        conn_grid = ctk.CTkFrame(conn_card.content, fg_color="transparent")
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
            ctk.CTkLabel(conn_grid, text=label).grid(row=row, column=col, sticky="e", padx=(0, 8), pady=4)
            entry = ctk.CTkEntry(conn_grid, height=32, show="*" if key == "password" else "")
            entry.insert(0, default)
            entry.grid(row=row, column=col + 1, sticky="ew", pady=4, padx=(0, 10))
            self.db_entries[key] = entry

        conn_btn_row = ctk.CTkFrame(conn_card.content, fg_color="transparent")
        conn_btn_row.pack(fill="x", pady=(10, 0))

        ctk.CTkButton(conn_btn_row, text="Connect", width=100, command=self._test_connection).pack(side="left")
        self.conn_status = ctk.CTkLabel(conn_btn_row, text="Not connected", text_color="gray")
        self.conn_status.pack(side="left", padx=15)

        # Query card
        query_card = ModernCard(left_col, "2. SQL Query")
        query_card.pack(fill="both", expand=True, pady=(0, 10))

        ctk.CTkLabel(
            query_card.content, text="Use :param_name for parameters (e.g. :start_date, :user_id)",
            font=ctk.CTkFont(size=11), text_color="gray",
        ).pack(anchor="w")

        self.query_text = ctk.CTkTextbox(
            query_card.content, font=ctk.CTkFont(family="Consolas", size=13),
        )
        self.query_text.pack(fill="both", expand=True, pady=(5, 10))
        self.query_text.insert("1.0", "SELECT * FROM orders\nWHERE order_date = :order_date\n  AND status = :status")

        ctk.CTkButton(query_card.content, text="Detect Parameters", command=self._detect_parameters).pack(anchor="w")

        # Right column
        right_col = ctk.CTkFrame(main, fg_color="transparent")
        right_col.grid(row=0, column=1, sticky="nsew", padx=(10, 0))
        right_col.grid_rowconfigure(0, weight=1)

        # Parameters card
        params_card = ModernCard(right_col, "3. Parameters")
        params_card.pack(fill="both", expand=True, pady=(0, 10))

        self.params_scroll = ctk.CTkScrollableFrame(params_card.content, fg_color="transparent")
        self.params_scroll.pack(fill="both", expand=True)

        self.params_placeholder = ctk.CTkLabel(
            self.params_scroll, text="Click 'Detect Parameters' to find parameters in query",
            text_color="gray",
        )
        self.params_placeholder.pack(pady=20)

        # Execution card
        exec_card = ModernCard(right_col, "4. Execute")
        exec_card.pack(fill="x")

        # Thread count
        thread_row = ctk.CTkFrame(exec_card.content, fg_color="transparent")
        thread_row.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(thread_row, text="Threads:").pack(side="left")
        self.thread_slider = ctk.CTkSlider(thread_row, from_=1, to=16, number_of_steps=15, width=150)
        self.thread_slider.set(4)
        self.thread_slider.pack(side="left", padx=10)
        self.thread_label = ctk.CTkLabel(thread_row, text="4")
        self.thread_label.pack(side="left")
        self.thread_slider.configure(command=lambda v: self.thread_label.configure(text=str(int(v))))

        # Output options
        output_row = ctk.CTkFrame(exec_card.content, fg_color="transparent")
        output_row.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(output_row, text="Output:").pack(side="left")
        self.output_var = ctk.StringVar(value="preview")
        for text, value in [("Preview", "preview"), ("CSV", "csv"), ("Excel", "excel"),
                            ("Insert", "insert"), ("Create+Insert", "create_insert")]:
            ctk.CTkRadioButton(output_row, text=text, variable=self.output_var, value=value).pack(side="left", padx=8)

        # Schema/Table for DB output
        db_row = ctk.CTkFrame(exec_card.content, fg_color="transparent")
        db_row.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(db_row, text="Schema:").pack(side="left")
        self.out_schema = ctk.CTkOptionMenu(db_row, values=["public"], width=120)
        self.out_schema.pack(side="left", padx=(5, 15))

        ctk.CTkLabel(db_row, text="Table:").pack(side="left")
        self.out_table = ctk.CTkEntry(db_row, width=150, placeholder_text="output_table")
        self.out_table.pack(side="left", padx=5)

        ctk.CTkButton(db_row, text="Refresh", width=70, command=self._refresh_schemas).pack(side="left", padx=5)

        # Run section with ETA
        run_row = ctk.CTkFrame(exec_card.content, fg_color="transparent")
        run_row.pack(fill="x", pady=(10, 0))

        self.combo_label = ctk.CTkLabel(run_row, text="Combinations: 0", font=ctk.CTkFont(size=12))
        self.combo_label.pack(side="left")

        self.run_btn = ctk.CTkButton(
            run_row, text="Run Query", width=120, height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#4CAF50", hover_color="#388E3C",
            command=self._run_queries,
        )
        self.run_btn.pack(side="right")

        self.progress = ctk.CTkProgressBar(run_row, width=120)
        self.progress.pack(side="right", padx=10)
        self.progress.set(0)

        # ETA display
        self.eta_label = ctk.CTkLabel(run_row, text="", font=ctk.CTkFont(size=11))
        self.eta_label.pack(side="right", padx=10)

        # Results section
        results_card = ModernCard(self, "5. Results")
        results_card.grid(row=2, column=0, sticky="ew", padx=20, pady=(0, 20))

        self.results_info = ctk.CTkLabel(results_card.content, text="No results yet", font=ctk.CTkFont(size=12))
        self.results_info.pack(anchor="w")

    def _load_saved_connections(self):
        connections = load_saved_connections()
        if connections:
            self.saved_conn_menu.configure(values=["(Select)"] + list(connections.keys()))

    def _load_connection(self, name: str):
        if name == "(Select)":
            return
        connections = load_saved_connections()
        if name in connections:
            conn = connections[name]
            for key, entry in self.db_entries.items():
                entry.delete(0, "end")
                entry.insert(0, conn.get(key, ""))

    def _test_connection(self):
        success, message = test_connection(
            host=self.db_entries["host"].get(),
            port=int(self.db_entries["port"].get() or 5432),
            database=self.db_entries["database"].get(),
            user=self.db_entries["user"].get(),
            password=self.db_entries["password"].get(),
        )

        if success:
            self.conn_status.configure(text="Connected", text_color="#4CAF50")
            self.conn_str = (
                f"host={self.db_entries['host'].get()} "
                f"port={self.db_entries['port'].get()} "
                f"dbname={self.db_entries['database'].get()} "
                f"user={self.db_entries['user'].get()} "
                f"password={self.db_entries['password'].get()}"
            )
            schemas = get_schemas(self.conn_str)
            self.out_schema.configure(values=schemas)
        else:
            self.conn_status.configure(text=f"Failed: {message[:30]}", text_color="#F44336")
            self.conn_str = None

    def _refresh_schemas(self):
        if self.conn_str:
            schemas = get_schemas(self.conn_str)
            self.out_schema.configure(values=schemas)

    def _detect_parameters(self):
        query = self.query_text.get("1.0", "end")
        params = extract_parameters(query)

        for panel in self.param_panels.values():
            panel.destroy()
        self.param_panels.clear()
        self.param_manager.clear_all()

        if not params:
            self.params_placeholder.pack(pady=20)
            self.combo_label.configure(text="Combinations: 0")
            return

        self.params_placeholder.pack_forget()

        for param_name in sorted(params):
            self.param_manager.add_parameter(param_name)
            panel = ParameterPanel(
                self.params_scroll, param_name, self.param_manager,
                lambda: self.conn_str, self._remove_parameter, self._update_combo_count,
            )
            panel.pack(fill="x", pady=5)
            self.param_panels[param_name] = panel

        self._update_combo_count()

    def _remove_parameter(self, name: str):
        if name in self.param_panels:
            self.param_panels[name].destroy()
            del self.param_panels[name]
            self.param_manager.remove_parameter(name)
            self._update_combo_count()

    def _update_combo_count(self):
        count = self.param_manager.get_combination_count()
        self.combo_label.configure(text=f"Combinations: {count:,}")

    def _run_queries(self):
        if self.is_running:
            return

        if not self.conn_str:
            messagebox.showwarning("Warning", "Please connect to database first")
            return

        query = self.query_text.get("1.0", "end").strip()
        if not query:
            messagebox.showwarning("Warning", "Please enter a query")
            return

        self._update_combo_count()
        combinations = self.param_manager.generate_combinations()
        if not combinations or combinations == [{}]:
            combinations = [{}]

        thread_count = int(self.thread_slider.get())

        self.is_running = True
        self.run_btn.configure(state="disabled", text="Running...")
        self.progress.set(0)
        self.eta_label.configure(text="Calculating...")

        def run():
            executor = ThreadedQueryExecutor(self.conn_str, max_workers=thread_count)

            def on_progress(stats: ExecutionStats):
                progress_val = stats.completed / stats.total if stats.total > 0 else 0
                elapsed = stats.format_time(stats.elapsed_time)
                eta = stats.format_time(stats.estimated_remaining)

                self.after(0, lambda: self.progress.set(progress_val))
                self.after(0, lambda: self.eta_label.configure(
                    text=f"Elapsed: {elapsed} | ETA: {eta}"
                ))
                self.after(0, lambda: self.results_info.configure(
                    text=f"Running: {stats.completed}/{stats.total} ({stats.success} ok, {stats.errors} errors)"
                ))

            executor.set_progress_callback(on_progress)
            executor.execute(query, combinations)
            self.results_df = executor.get_combined_results()

            self.after(0, lambda: self._on_queries_complete(executor))

        threading.Thread(target=run, daemon=True).start()

    def _on_queries_complete(self, executor: ThreadedQueryExecutor):
        self.is_running = False
        self.run_btn.configure(state="normal", text="Run Query")

        stats = executor.stats
        elapsed = stats.format_time(stats.elapsed_time)

        self.results_info.configure(
            text=f"Completed: {stats.success} success, {stats.errors} errors, "
                 f"{len(self.results_df)} rows | Time: {elapsed}"
        )
        self.eta_label.configure(text=f"Done in {elapsed}")

        output_type = self.output_var.get()

        if output_type == "preview":
            if self.results_df is not None and not self.results_df.empty:
                preview = self.results_df.head(10).to_string()
                messagebox.showinfo("Preview (first 10 rows)", preview[:2000])
            else:
                messagebox.showinfo("Preview", "No data returned")

        elif output_type == "csv":
            file_path = filedialog.asksaveasfilename(
                defaultextension=".csv", filetypes=[("CSV files", "*.csv")],
            )
            if file_path:
                success, msg = export_to_csv(self.results_df, file_path)
                messagebox.showinfo("Export", msg)

        elif output_type == "excel":
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")],
            )
            if file_path:
                success, msg = export_to_excel(self.results_df, file_path)
                messagebox.showinfo("Export", msg)

        elif output_type == "insert":
            table = self.out_table.get().strip()
            if not table:
                messagebox.showwarning("Warning", "Please enter table name")
                return
            success, msg, count = insert_to_table(
                self.conn_str, self.results_df, self.out_schema.get(), table
            )
            messagebox.showinfo("Insert", msg)

        elif output_type == "create_insert":
            table = self.out_table.get().strip()
            if not table:
                messagebox.showwarning("Warning", "Please enter table name")
                return
            success, msg, count = create_and_insert(
                self.conn_str, self.results_df, self.out_schema.get(), table
            )
            messagebox.showinfo("Create & Insert", msg)


def run_app():
    """Launch the Query Runner GUI."""
    app = QueryRunnerApp()
    app.mainloop()


if __name__ == "__main__":
    run_app()
