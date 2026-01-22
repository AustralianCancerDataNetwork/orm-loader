from __future__ import annotations
from typing import Protocol
from .validation_report import ValidationIssue, SeverityLevel
from .registry import ModelDescriptor, TableSpec, FieldSpec

class Validator(Protocol):
    def validate(
        self,
        *,
        model: ModelDescriptor,
        spec: TableSpec | None = None,
        fields: dict[str, FieldSpec] | None = None,
    ) -> list[ValidationIssue]:
        ...


class ColumnNullabilityValidator:

    """
    Ensures required columns are NOT NULL in the ORM model.
    """

    def validate(
        self,
        *,
        model: ModelDescriptor,
        spec: TableSpec | None = None,
        fields: dict[str, FieldSpec] | None = None,
    ) -> list[ValidationIssue]:
        if not fields:
            return []

        issues: list[ValidationIssue] = []

        for field_name, field_spec in fields.items():
            if not field_spec.is_required:
                continue

            col = model.columns.get(field_name)
            if col is None:
                # Presence handled elsewhere
                continue

            if col.nullable:
                issues.append(
                    ValidationIssue(
                        table=model.table_name,
                        level=SeverityLevel.ERROR,
                        field=field_name,
                        message="REQUIRED_COLUMN_NULLABLE",
                        expected="NOT NULL",
                        actual="NULL",
                        hint="Specification marks column as required",
                    )
                )

        return issues
        
class ColumnPresenceValidator:

    """
    Ensures all specified columns are present on the ORM model.
    """

    def validate(
        self,
        *,
        model: ModelDescriptor,
        spec: TableSpec | None = None,
        fields: dict[str, FieldSpec] | None = None,
    ) -> list[ValidationIssue]:
        if not fields:
            return []

        issues: list[ValidationIssue] = []
        model_cols = set(model.columns.keys())

        for field_name, field_spec in fields.items():
            if field_name not in model_cols:
                issues.append(
                    ValidationIssue(
                        table=model.table_name,
                        level=SeverityLevel.ERROR,
                        field=field_name,
                        message="COLUMN_MISSING",
                        hint="Column defined in specification but not implemented in ORM model",
                    )
                )

        return issues

class PrimaryKeyValidator:
    """
    Validates primary key presence and correctness.

    Always-on:
    - PK must exist
    - PK columns must be NOT NULL

    Spec-aware (if FieldSpec provided):
    - Spec PKs must exist in ORM
    - ORM PKs should be declared in spec
    """

    def validate(
        self,
        *,
        model: ModelDescriptor,
        spec: TableSpec | None = None,
        fields: dict[str, FieldSpec] | None = None,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        pk_cols = model.primary_keys

        if not pk_cols:
            issues.append(
                ValidationIssue(
                    table=model.table_name,
                    level=SeverityLevel.ERROR,
                    message="TABLE_HAS_NO_PRIMARY_KEY",
                    hint="Every table must define a primary key",
                )
            )
            return issues  # nothing else to check

        for pk in pk_cols:
            col = model.columns.get(pk)
            if col is None:
                continue  # should not happen, but presence validator handles this

            if col.nullable:
                issues.append(
                    ValidationIssue(
                        table=model.table_name,
                        level=SeverityLevel.ERROR,
                        field=pk,
                        message="PRIMARY_KEY_COLUMN_NULLABLE",
                        expected="NOT NULL",
                        actual="NULL",
                    )
                )

        if fields:
            spec_pks = {
                name
                for name, fs in fields.items()
                if fs.is_primary_key
            }

            for missing in sorted(spec_pks - pk_cols):
                issues.append(
                    ValidationIssue(
                        table=model.table_name,
                        level=SeverityLevel.ERROR,
                        field=missing,
                        message="PRIMARY_KEY_MISSING_FROM_MODEL",
                        hint="Column marked as primary key in specification",
                    )
                )

            for extra in sorted(pk_cols - spec_pks):
                issues.append(
                    ValidationIssue(
                        table=model.table_name,
                        level=SeverityLevel.WARN,
                        field=extra,
                        message="PRIMARY_KEY_NOT_DECLARED_IN_SPEC",
                        hint="ORM primary key not marked as primary key in specification",
                    )
                )

        if len(pk_cols) > 1:
            issues.append(
                ValidationIssue(
                    table=model.table_name,
                    level=SeverityLevel.INFO,
                    message="COMPOSITE_PRIMARY_KEY",
                    expected=", ".join(sorted(pk_cols)),
                    hint="Composite primary key detected",
                )
            )

        return issues

    
class ForeignKeyShapeValidator:
    
    """
    Validates structural correctness of foreign key definitions.
    """

    def validate(
        self,
        *,
        model: ModelDescriptor,
        spec: TableSpec | None = None,
        fields: dict[str, FieldSpec] | None = None,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        for col_name, (fk_table, fk_field) in model.foreign_keys.items():
            if not fk_table or not fk_field:
                issues.append(
                    ValidationIssue(
                        table=model.table_name,
                        level=SeverityLevel.ERROR,
                        field=col_name,
                        message="FOREIGN_KEY_INCOMPLETE",
                        hint="Foreign key must reference table and column",
                    )
                )
                continue

            # If field specs are available, cross-check
            if fields:
                field_spec = fields.get(col_name)
                if field_spec and not field_spec.is_foreign_key:
                    issues.append(
                        ValidationIssue(
                            table=model.table_name,
                            level=SeverityLevel.WARN,
                            field=col_name,
                            message="FOREIGN_KEY_NOT_IN_SPEC",
                            hint="ORM defines FK but specification does not",
                        )
                    )

        return issues