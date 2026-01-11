from .validation_report import ValidationReport, SeverityLevel
from .registry import ModelRegistry
from .validation import Validator

class ValidationRunner:
    def __init__(self, validators: list[Validator], fail_fast: bool = False):
        self.validators = validators
        self.fail_fast = fail_fast


    def run(self, registry: ModelRegistry) -> ValidationReport:
        report = ValidationReport(
            model_version=registry.model_version
        )

        for table_name, desc in registry.models().items():
            table_spec = registry._table_specs.get(table_name)
            field_specs = registry._field_specs.get(table_name)

            for validator in self.validators:
                issues = validator.validate(
                    model=desc,
                    spec=table_spec,
                    fields=field_specs,
                )

                for issue in issues:
                    report.add(issue)

                    if (
                        self.fail_fast
                        and issue.level == SeverityLevel.ERROR
                    ):
                        return report

        return report
