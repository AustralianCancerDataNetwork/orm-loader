# Model & Specification Descriptors

Descriptor objects provide a **normalised, inspectable view** of both
ORM models and external specifications.

They are immutable, explicit, and independent of validation logic.

---

## TableSpec

Represents a table-level specification loaded from an external source
(e.g. OMOP CSVs).

::: orm_loader.registry.registry.TableSpec

---

## FieldSpec

Represents a field-level specification loaded from an external source.

::: orm_loader.registry.registry.FieldSpec

---

## ModelDescriptor

A derived, introspected representation of an ORM model class.

It captures:
- column objects
- primary keys
- foreign key relationships

::: orm_loader.registry.registry.ModelDescriptor
