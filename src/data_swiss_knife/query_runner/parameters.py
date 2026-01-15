"""Parameter management for query runner."""

from dataclasses import dataclass, field
from itertools import product
from typing import Any


@dataclass
class Parameter:
    """A query parameter with its values."""
    name: str
    values: list[Any] = field(default_factory=list)
    source_type: str = "manual"  # "manual" or "query"
    source_query: str = ""

    def add_value(self, value: Any):
        """Add a value to the parameter."""
        if value not in self.values:
            self.values.append(value)

    def remove_value(self, value: Any):
        """Remove a value from the parameter."""
        if value in self.values:
            self.values.remove(value)

    def clear_values(self):
        """Clear all values."""
        self.values = []

    def set_values(self, values: list[Any]):
        """Set all values at once."""
        self.values = list(values)


class ParameterManager:
    """Manage multiple parameters and generate combinations."""

    def __init__(self):
        self.parameters: dict[str, Parameter] = {}

    def add_parameter(self, name: str) -> Parameter:
        """Add a new parameter."""
        if name not in self.parameters:
            self.parameters[name] = Parameter(name=name)
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
