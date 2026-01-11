from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Optional
import json

class SeverityLevel(Enum):
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"

@dataclass
class ValidationIssue:
    table: str
    level: SeverityLevel 
    message: str
    field: Optional[str] = None
    expected: Optional[str] = None
    actual: Optional[str] = None
    hint: Optional[str] = None

@dataclass
class ValidationReport:

    def __init__(self, *, model_version: str, model_name: Optional[str] = None):
        self.model_version = model_version
        self.model_name = model_name
        self.issues: list[ValidationIssue] = []
    
    def add(self, issue: ValidationIssue) -> None:
        self.issues.append(issue)
        
    def is_valid(self) -> bool:
        return not self.issues

    def summary(self) -> str:

        by = {SeverityLevel.ERROR: 0, SeverityLevel.WARN: 0, SeverityLevel.INFO: 0}
        for i in self.issues:
            by[i.level] += 1
        model = self.model_name.upper() if self.model_name else "MODEL"
        return f"{model} v{self.model_version}: {by[SeverityLevel.ERROR]} error(s), {by[SeverityLevel.WARN]} warning(s), {by[SeverityLevel.INFO]} info"
    
    def render_text_report(self) -> str:
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
    
    # so that this can be used in CI/CD pipelines easily
    def to_dict(self) -> dict:
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
        return json.dumps(self.to_dict(), indent=2)
    
    def exit_code(self) -> int:
        return 1 if any(i.level == SeverityLevel.ERROR for i in self.issues) else 0