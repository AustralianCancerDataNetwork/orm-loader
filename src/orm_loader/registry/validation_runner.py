from .validation_report import ValidationReport, SeverityLevel
from .registry import ModelRegistry
from .validation import Validator


"""
Validation Runner
=================

This module coordinates execution of validation rules against a
registered set of ORM models.

It is responsible for:
- iterating over registered models
- invoking validators
- collecting validation issues
- supporting optional fail-fast behaviour
"""

class ValidationRunner:
    """
    Coordinates execution of validators against a model registry.

    The runner itself is deliberately simple and does not embed any
    validation logic. All rules are provided via Validator instances.
    """
    def __init__(self, validators: list[Validator], fail_fast: bool = False):
        """
        Initialise a validation runner.

        Parameters
        ----------
        validators
            List of validator implementations to execute.
        fail_fast
            Whether to stop execution on the first ERROR-level issue.
        """
        self.validators = validators
        self.fail_fast = fail_fast


    def run(self, registry: ModelRegistry) -> ValidationReport:
        """
        Run validation across all registered models.

        Parameters
        ----------
        registry
            Model registry containing models and specifications.

        Returns
        -------
        ValidationReport
            Aggregated validation report.
        """
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
