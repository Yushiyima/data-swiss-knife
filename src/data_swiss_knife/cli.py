"""Command-line interface for Data Swiss Knife."""

import argparse


def main() -> None:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="data-swiss-knife",
        description="A versatile data processing toolkit",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Launcher (default)
    subparsers.add_parser(
        "launch",
        help="Launch the main GUI with all tools (default)",
    )

    # DB Generator command
    subparsers.add_parser(
        "db-generator",
        help="Launch the PostgreSQL table generator GUI",
    )

    # Query Runner command
    subparsers.add_parser(
        "query-runner",
        help="Launch the parametric query runner GUI",
    )

    args = parser.parse_args()

    if args.command == "db-generator":
        from .db_generator import run_app
        run_app()
    elif args.command == "query-runner":
        from .query_runner import run_app
        run_app()
    elif args.command == "launch" or args.command is None:
        # Default: launch the main GUI
        from .launcher import run_launcher
        run_launcher()


if __name__ == "__main__":
    main()
