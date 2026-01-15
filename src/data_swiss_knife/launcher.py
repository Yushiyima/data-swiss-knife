"""Main launcher GUI for Data Swiss Knife tools."""

import customtkinter as ctk

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


class ToolCard(ctk.CTkFrame):
    """A card for launching a tool."""

    def __init__(self, master, title: str, description: str, command, color: str, **kwargs):
        super().__init__(master, corner_radius=16, **kwargs)

        self.configure(cursor="hand2")
        self.bind("<Button-1>", lambda e: command())

        # Make all children also trigger the click
        def bind_children(widget):
            widget.bind("<Button-1>", lambda e: command())
            for child in widget.winfo_children():
                bind_children(child)

        content = ctk.CTkFrame(self, fg_color="transparent")
        content.pack(fill="both", expand=True, padx=30, pady=30)

        # Icon placeholder (colored circle)
        icon_frame = ctk.CTkFrame(content, width=60, height=60, corner_radius=30, fg_color=color)
        icon_frame.pack(pady=(0, 20))
        icon_frame.pack_propagate(False)

        title_label = ctk.CTkLabel(
            content,
            text=title,
            font=ctk.CTkFont(size=20, weight="bold"),
        )
        title_label.pack(pady=(0, 10))

        desc_label = ctk.CTkLabel(
            content,
            text=description,
            font=ctk.CTkFont(size=13),
            text_color="gray",
            wraplength=250,
        )
        desc_label.pack()

        bind_children(self)

        # Hover effect
        def on_enter(e):
            self.configure(fg_color=("gray80", "gray25"))

        def on_leave(e):
            self.configure(fg_color=("gray90", "gray17"))

        self.bind("<Enter>", on_enter)
        self.bind("<Leave>", on_leave)


class LauncherApp(ctk.CTk):
    """Main launcher for Data Swiss Knife tools."""

    def __init__(self):
        super().__init__()

        self.title("Data Swiss Knife")
        self.geometry("700x500")
        self.minsize(600, 450)

        self._create_ui()

    def _create_ui(self):
        """Create the launcher UI."""
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # Header
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.grid(row=0, column=0, sticky="ew", padx=30, pady=(30, 20))

        ctk.CTkLabel(
            header,
            text="Data Swiss Knife",
            font=ctk.CTkFont(size=32, weight="bold"),
        ).pack(side="left")

        theme_menu = ctk.CTkSegmentedButton(
            header,
            values=["Light", "Dark"],
            command=lambda v: ctk.set_appearance_mode(v.lower()),
        )
        theme_menu.set("Dark")
        theme_menu.pack(side="right")

        ctk.CTkLabel(
            self,
            text="Select a tool to launch",
            font=ctk.CTkFont(size=14),
            text_color="gray",
        ).grid(row=1, column=0, sticky="n")

        # Tools grid
        tools_frame = ctk.CTkFrame(self, fg_color="transparent")
        tools_frame.grid(row=2, column=0, sticky="nsew", padx=30, pady=20)
        tools_frame.grid_columnconfigure((0, 1), weight=1)

        # DB Generator card
        db_card = ToolCard(
            tools_frame,
            title="DB Table Generator",
            description="Create PostgreSQL tables from CSV/Excel files with auto type detection",
            command=self._launch_db_generator,
            color="#4CAF50",
        )
        db_card.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        # Query Runner card
        query_card = ToolCard(
            tools_frame,
            title="Query Runner",
            description="Execute parameterized SQL queries with threading and multiple output options",
            command=self._launch_query_runner,
            color="#2196F3",
        )
        query_card.grid(row=0, column=1, sticky="nsew", padx=10, pady=10)

        # Footer
        footer = ctk.CTkLabel(
            self,
            text="v0.1.0 | github.com/Yushiyima/data-swiss-knife",
            font=ctk.CTkFont(size=11),
            text_color="gray",
        )
        footer.grid(row=3, column=0, pady=(0, 20))

    def _launch_db_generator(self):
        """Launch DB Generator in new window."""
        self.withdraw()  # Hide launcher
        from .db_generator.gui import DBGeneratorApp

        app = DBGeneratorApp()
        app.protocol("WM_DELETE_WINDOW", lambda: self._on_tool_close(app))
        app.mainloop()

    def _launch_query_runner(self):
        """Launch Query Runner in new window."""
        self.withdraw()  # Hide launcher
        from .query_runner.gui import QueryRunnerApp

        app = QueryRunnerApp()
        app.protocol("WM_DELETE_WINDOW", lambda: self._on_tool_close(app))
        app.mainloop()

    def _on_tool_close(self, tool_window):
        """Handle tool window close - show launcher again."""
        tool_window.destroy()
        self.deiconify()  # Show launcher again


def run_launcher():
    """Launch the main launcher."""
    app = LauncherApp()
    app.mainloop()


if __name__ == "__main__":
    run_launcher()
