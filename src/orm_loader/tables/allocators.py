class IdAllocator:
    """
    Simple in-process ID allocator.

    This utility provides monotonically increasing integer identifiers
    starting from a known baseline. It is intended for environments where
    database-managed sequences are unavailable or undesirable.

    Typical use cases include:
    - SQLite or other lightweight databases without sequences
    - controlled, single-writer ingestion pipelines
    - deterministic ID assignment during bulk loads

    This allocator is deliberately minimal and **not safe for concurrent
    writers or multi-process use**.
    """

    def __init__(self, start: int):
        self._next = start + 1

    def next(self) -> int:
        val = self._next
        self._next += 1
        return val

    def reserve(self, n: int) -> range:
        """
        Reserve a contiguous block of identifiers.

        This method advances the internal counter by ``n`` and returns
        a ``range`` representing the reserved identifiers.

        Parameters
        ----------
        n
            The number of identifiers to reserve.

        Returns
        -------
        range
            A range covering the reserved identifiers.
        """
        start = self._next
        self._next += n
        return range(start, start + n)
