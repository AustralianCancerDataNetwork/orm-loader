class IdAllocator:
    """
    Simple in-process ID allocator.

    Intended for databases without sequence support (e.g. SQLite),
    or controlled single-writer ingestion contexts.

    Not safe for concurrent writers.
    """

    def __init__(self, start: int):
        self._next = start + 1

    def next(self) -> int:
        val = self._next
        self._next += 1
        return val

    def reserve(self, n: int) -> range:
        start = self._next
        self._next += n
        return range(start, start + n)
