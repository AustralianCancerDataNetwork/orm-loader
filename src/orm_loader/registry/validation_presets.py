from .validation import (
    ColumnNullabilityValidator,
    ColumnPresenceValidator,
    ForeignKeyShapeValidator,
    PrimaryKeyValidator,
)


def always_on_validators():
    return [
        ColumnPresenceValidator(),
        ColumnNullabilityValidator(),
        PrimaryKeyValidator(),
        ForeignKeyShapeValidator(),
    ]
