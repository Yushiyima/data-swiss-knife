"""Parameter management for query runner."""

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from itertools import product
from typing import Any


# Supported parameter types
PARAM_TYPES = ["text", "number", "date"]

# Common date formats
DATE_FORMATS = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%Y.%m.%d",
]


def parse_value(value: str, param_type: str, date_format: str = "%Y-%m-%d") -> Any:
    """Parse a string value to the appropriate type."""
    value = value.strip()

    if param_type == "number":
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    elif param_type == "date":
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            # Try common formats
            for fmt in DATE_FORMATS:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
            return value

    # Default: text
    return value


def generate_date_range(start: str, end: str, date_format: str = "%Y-%m-%d") -> list[date]:
    """Generate a list of dates between start and end (inclusive)."""
    try:
        start_date = datetime.strptime(start, date_format).date()
        end_date = datetime.strptime(end, date_format).date()

        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates
    except ValueError:
        return []


@dataclass
class Parameter:
    """A query parameter with its values."""
    name: str
    param_type: str = "text"  # "text", "number", "date"
    date_format: str = "%Y-%m-%d"
    values: list[Any] = field(default_factory=list)
    source_type: str = "manual"  # "manual", "query", "range"
    source_query: str = ""

    def add_value(self, value: Any):
        """Add a value to the parameter."""
        parsed = self._parse(value)
        if parsed not in self.values:
            self.values.append(parsed)

    def remove_value(self, value: Any):
        """Remove a value from the parameter."""
        if value in self.values:
            self.values.remove(value)

    def clear_values(self):
        """Clear all values."""
        self.values = []

    def set_values(self, values: list[Any]):
        """Set all values at once (with parsing)."""
        self.values = [self._parse(v) for v in values]

    def set_values_raw(self, values: list[Any]):
        """Set values without parsing (already correct type)."""
        self.values = list(values)

    def set_from_range(self, start: str, end: str):
        """Set values from a date range (only for date type)."""
        if self.param_type == "date":
            dates = generate_date_range(start, end, self.date_format)
            self.values = dates
            self.source_type = "range"
        elif self.param_type == "number":
            try:
                start_num = int(start)
                end_num = int(end)
                self.values = list(range(start_num, end_num + 1))
                self.source_type = "range"
            except ValueError:
                pass

    def _parse(self, value: Any) -> Any:
        """Parse a value to the parameter's type."""
        if isinstance(value, str):
            return parse_value(value, self.param_type, self.date_format)
        return value

    def get_display_values(self, max_count: int = 5) -> str:
        """Get values formatted for display."""
        if not self.values:
            return "(none)"

        formatted = []
        for v in self.values[:max_count]:
            if isinstance(v, date):
                formatted.append(v.strftime(self.date_format))
            else:
                formatted.append(str(v))

        result = ", ".join(formatted)
        if len(self.values) > max_count:
            result += f"... (+{len(self.values) - max_count} more)"

        return result


class ParameterManager:
    """Manage multiple parameters and generate combinations."""

    def __init__(self):
        self.parameters: dict[str, Parameter] = {}

    def add_parameter(self, name: str, param_type: str = "text") -> Parameter:
        """Add a new parameter."""
        if name not in self.parameters:
            self.parameters[name] = Parameter(name=name, param_type=param_type)
        return self.parameters[name]

    def remove_parameter(self, name: str):
        """Remove a parameter."""
        if name in self.parameters:
            del self.parameters[name]

    def get_parameter(self, name: str) -> Parameter | None:
        """Get a parameter by name."""
        return self.parameters.get(name)

    def get_all_parameters(self) -> list[Parameter]:
        """Get all parameters."""
        return list(self.parameters.values())

    def generate_combinations(self) -> list[dict[str, Any]]:
        """Generate all combinations of parameter values (cartesian product)."""
        if not self.parameters:
            return [{}]

        # Filter parameters that have values
        params_with_values = {
            name: param.values
            for name, param in self.parameters.items()
            if param.values
        }

        if not params_with_values:
            return [{}]

        # Generate cartesian product
        names = list(params_with_values.keys())
        value_lists = [params_with_values[name] for name in names]

        combinations = []
        for combo in product(*value_lists):
            combinations.append(dict(zip(names, combo)))

        return combinations

    def get_combination_count(self) -> int:
        """Get the total number of combinations."""
        count = 1
        for param in self.parameters.values():
            if param.values:
                count *= len(param.values)
        return count if self.parameters else 0

    def clear_all(self):
        """Clear all parameters."""
        self.parameters = {}
