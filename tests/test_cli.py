"""Tests for the CLI module."""

from data_swiss_knife import __version__


def test_version():
    """Test that version is defined."""
    assert __version__ == "0.1.0"
