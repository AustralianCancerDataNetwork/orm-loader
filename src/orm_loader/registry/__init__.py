from .registry import FieldSpec, ModelDescriptor, ModelRegistry, TableSpec
from .validation import Validator
from .validation_presets import always_on_validators
from .validation_report import SeverityLevel, ValidationIssue
from .validation_runner import ValidationRunner

__all__ = [
    "FieldSpec",
    "ModelDescriptor",
    "ModelRegistry",
    "SeverityLevel",
    "TableSpec",
    "ValidationIssue",
    "ValidationRunner",
    "Validator",
    "always_on_validators",
]


"""
Usage example:

registry = ModelRegistry(
    model_version="5.4",
    model_name="OMOP",
)

registry.load_table_specs(
    table_csv=Path("specs/CDM_TABLE.csv"),
    field_csv=Path("specs/CDM_FIELD.csv"),
)

registry.register_models([
    Person,
    VisitOccurrence,
])

OR 

registry.discover_models('omop_alchemy.cdm.model')

class MyCustomValidator:
    def validate(self, *, model, spec=None, fields=None) -> list[ValidationIssue
        # Custom validation logic here
        return []

validators = always_on_validators() + [MyCustomValidator()]
runner = ValidationRunner(
    validators=validators,
    fail_fast=False,
)

report = runner.run(registry)

### locally inspect report ###
print(report.summary())

if not report.is_valid():
    print(report.render_text_report())

    
### use in CI/CD pipeline ###

print(report.to_json())
sys.exit(report.exit_code())

"""
