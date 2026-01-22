# IdAllocator

A simple in-process ID allocator for controlled ingestion contexts.

Used when:
- database sequences are unavailable
- ingestion is single-writer
- deterministic ID assignment is required

---

## API

::: orm_loader.tables.allocators.IdAllocator
