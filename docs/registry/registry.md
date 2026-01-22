# ModelRegistry

The `ModelRegistry` is the central coordination object for model
registration and validation.

It holds:
- registered ORM models
- external table specifications
- external field specifications
- derived model descriptors

The registry does **not** perform validation itself — it provides the
context required for validators and runners.

---

## Responsibilities

- Load table and field specifications
- Register ORM model classes
- Track required vs implemented tables
- Provide descriptors for validation

---

## API

::: orm_loader.registry.registry.ModelRegistry
