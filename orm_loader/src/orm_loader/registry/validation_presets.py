from .validation import (
    ColumnNullabilityValidator, 
    ColumnPresenceValidator, 
    PrimaryKeyValidator, 
    ForeignKeyShapeValidator,
)

def always_on_validators():
    return [
        ColumnPresenceValidator(),
        ColumnNullabilityValidator(),
        PrimaryKeyValidator(),
        ForeignKeyShapeValidator(),
    ]
