# Validators

Validators implement **focused, composable validation rules** that
compare ORM models against specifications.

Each validator:
- operates on a single model
- produces structured validation issues
- is independent of execution order

---

## Validator Protocol

::: orm_loader.registry.validation.Validator

---

## ColumnNullabilityValidator

Ensures required columns are not nullable.

::: orm_loader.registry.validation.ColumnNullabilityValidator

---

## ColumnPresenceValidator

Ensures all specified columns exist on the ORM model.

::: orm_loader.registry.validation.ColumnPresenceValidator

---

## PrimaryKeyValidator

Validates primary key presence, nullability, and alignment with specs.

::: orm_loader.registry.validation.PrimaryKeyValidator

---

## ForeignKeyShapeValidator

Validates structural correctness of foreign key definitions.

::: orm_loader.registry.validation.ForeignKeyShapeValidator

---

## `always_on_validators()`

Returns the standard bundle of all four validators above
(`ColumnPresenceValidator`, `ColumnNullabilityValidator`,
`PrimaryKeyValidator`, `ForeignKeyShapeValidator`) as a ready-made list, for
callers that want the default validation set without constructing each one
individually.

::: orm_loader.registry.validation_presets.always_on_validators
