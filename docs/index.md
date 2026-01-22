# orm-loader

A lightweight, reusable foundation for building and validating
SQLAlchemy-based data models.

`orm-loader` provides **infrastructure, not semantics**.

It focuses on:

- ORM table introspection
- safe bulk ingestion patterns
- file-based loading via staging tables
- model-agnostic validation scaffolding
- database-portable operational helpers

No domain logic is included.
No schema assumptions are enforced.

---

## Core Concepts

- **Tables are structural** — semantics live downstream
- **Mixins define capabilities**, not behaviour contracts
- **Protocols decouple infrastructure from implementations**
- **Ingestion is explicit and staged**

---

## API Reference

- [Tables](tables/index.md)
- [Registry & Validation](registry/index.md)
- [Loaders](loaders/index.md)

---

# Design Philosophy

`orm-loader` is intentionally conservative.

It provides:

- *mechanisms*, not policies
- *capabilities*, not workflows
- *structure*, not semantics

The library is designed to sit **below**:

- OMOP CDM (initial scope)
- extension to O3, MCODE, etc.
- custom clinical schemas
- research data marts

and **above**:

- raw SQLAlchemy
- database-specific ingestion scripts

---

## What this library does **not** do

- No domain validation
- No schema enforcement
- No migrations
- No concurrency guarantees

---

## Why mixins and protocols?

Mixins provide reusable behaviour.

Protocols provide:
- static typing
- runtime structural checks
- decoupling between infrastructure and models

This allows downstream libraries to:
- replace base classes
- mock implementations
- incrementally adopt features

