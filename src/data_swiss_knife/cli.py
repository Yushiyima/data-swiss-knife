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

    # DB Generator command
    db_parser = subparsers.add_parser(
        "db-generator",
        help="Launch the PostgreSQL table generator GUI",
    )

    args = parser.parse_args()

    if args.command == "db-generator":
        from .db_generator import run_app
        run_app()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
