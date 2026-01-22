from orm_loader.tables.allocators import IdAllocator

def test_allocator_sequence():
    alloc = IdAllocator(start=5)
    assert alloc.next() == 6
    assert alloc.next() == 7

def test_allocator_reserve():
    alloc = IdAllocator(start=0)
    r = alloc.reserve(3)
    assert list(r) == [1, 2, 3]
