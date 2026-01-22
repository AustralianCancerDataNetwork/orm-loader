# Table Infrastructure

The `orm_loader.tables` module provides foundational building blocks
for defining, inspecting, serialising, and loading ORM tables.

All components are:
- model-agnostic
- database-portable
- safe to compose

---

## Table Base & Mixins

- [`ORMTableBase`](orm_table.md)
- [`SerialisableTableInterface`](serialisable_table.md)
- [`CSVLoadableTableInterface`](loadable_table.md)

---

## Supporting Utilities

- [`IdAllocator`](allocators.md)
- [Typing Protocols](typing.md)
