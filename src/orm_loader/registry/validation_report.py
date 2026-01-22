from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Optional
import json


"""
Validation Reporting
====================

This module defines structured representations for validation outcomes.

It provides:
- severity levels
- individual validation issues
- an aggregate validation report

The report format is designed to be:
- human-readable
- machine-readable
- CI/CD friendly

No validation logic is implemented here.
"""


class SeverityLevel(Enum):
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"

@dataclass
class ValidationIssue:
    """
    Represents a single validation finding.

    Validation issues are immutable data records describing a deviation
    between an ORM model and an external specification.

    Attributes
    ----------
    table
        Name of the table where the issue was found.
    level
        Severity level of the issue.
    message
        Machine-readable issue identifier.
    field
        Optional field/column name associated with the issue.
    expected
        Optional expected value or condition.
    actual
        Optional actual value or condition.
    hint
        Optional human-readable guidance.
    """
    table: str
    level: SeverityLevel 
    message: str
    field: Optional[str] = None
    expected: Optional[str] = None
    actual: Optional[str] = None
    hint: Optional[str] = None

@dataclass
class ValidationReport:
    """
    Aggregate report of validation outcomes.

    A ValidationReport collects all issues produced during validation
    and provides multiple representations suitable for:
    - interactive inspection
    - text reporting
    - JSON serialisation
    - CI/CD pipeline integration
    """
    def __init__(self, *, model_version: str, model_name: Optional[str] = None):
        """
        Initialise a new validation report.

        Parameters
        ----------
        model_version
            Version identifier for the validated model set.
        model_name
            Optional human-readable model name.
        """
        self.model_version = model_version
        self.model_name = model_name
        self.issues: list[ValidationIssue] = []
    
    def add(self, issue: ValidationIssue) -> None:
        """
        Add a validation issue to the report.

        Parameters
        ----------
        issue
            Validation issue to record.
        """
        self.issues.append(issue)
        
    def is_valid(self) -> bool:
        """
        Return whether the report contains no issues.

        Returns
        -------
        bool
            True if no issues are present, otherwise False.
        """
        return not self.issues

    def summary(self) -> str:

        """
        Return a one-line human-readable summary.

        Returns
        -------
        str
            Summary string including error, warning, and info counts.
        """
        by = {SeverityLevel.ERROR: 0, SeverityLevel.WARN: 0, SeverityLevel.INFO: 0}
        for i in self.issues:
            by[i.level] += 1
        model = self.model_name.upper() if self.model_name else "MODEL"
        return f"{model} v{self.model_version}: {by[SeverityLevel.ERROR]} error(s), {by[SeverityLevel.WARN]} warning(s), {by[SeverityLevel.INFO]} info"
    
    def render_text_report(self) -> str:
        """
        Render a human-readable multi-line text report.

        Issues are grouped by table and rendered with simple visual
        indicators for severity.

        Returns
        -------
        str
            Formatted text report.
        """
        lines = []
        by_table = defaultdict(list)

        for issue in self.issues:
            by_table[issue.table].append(issue)

        for table, issues in sorted(by_table.items()):
            lines.append(f"\n📦 {table}")
            for i in issues:
                icon = "❌" if i.level == SeverityLevel.ERROR else "⚠️"
                hint = f" Hint: {i.hint}" if i.hint else ""
                field = f" (field: {i.field})" if i.field else ""
                lines.append(f"  {icon} {i.message}{field}{hint}")

        return "\n".join(lines)
    
    def to_dict(self) -> dict:
        """
        Return a structured dictionary representation of the report.

        Intended for use in CI/CD pipelines and programmatic consumers.

        Returns
        -------
        dict
            Dictionary representation of the validation report.
        """
        return {
            "model_version": self.model_version,
            "summary": {
                "error": sum(i.level == SeverityLevel.ERROR for i in self.issues),
                "warn": sum(i.level == SeverityLevel.WARN for i in self.issues),
                "info": sum(i.level == SeverityLevel.INFO for i in self.issues),
            },
            "issues": [
                {
                    "table": i.table,
                    "level": i.level.value,
                    "message": i.message,
                    "field": i.field,
                    "expected": i.expected,
                    "actual": i.actual,
                    "hint": i.hint,
                }
                for i in self.issues
            ],
        }
    
    def to_json(self) -> str:
        """
        Serialise the report to a JSON string.

        Returns
        -------
        str
            JSON-formatted validation report.
        """
        return json.dumps(self.to_dict(), indent=2)
    
    def exit_code(self) -> int:
        """
        Return a process exit code suitable for CI/CD pipelines.

        Returns
        -------
        int
            1 if any ERROR-level issues are present, otherwise 0.
        """
        return 1 if any(i.level == SeverityLevel.ERROR for i in self.issues) else 0