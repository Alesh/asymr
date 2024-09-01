import asyncio
import sys
from datetime import datetime

if sys.version_info < (3, 11):
    from async_timeout import timeout

    asyncio.timeout = timeout

import pytest

from asymr.primitives import Source, Destination


@pytest.mark.asyncio
async def test_source_destination_destination():
    class FirstSource(Source[list[int]]):
        def __init__(self):
            async def source_iterator():
                for i in range(10):
                    yield [i]

            super().__init__(source_iterator())

    class FirstDestination(Destination[list[int]]):
        async def __anext__(self):
            first, = await super().__anext__()
            return [first, first * first]

    class SecondDestination(Destination[list[int]]):
        async def __anext__(self):
            first, second = await super().__anext__()
            if first == 5:
                raise StopAsyncIteration
            return [first, second, first * second]

    destination = FirstSource() >> FirstDestination() >> SecondDestination()
    result = [item async for item in destination]
    assert result == [[0, 0, 0], [1, 1, 1], [2, 4, 8], [3, 9, 27], [4, 16, 64]]

    assert not destination.closed
    asyncio.get_event_loop().call_later(0.1, lambda: destination.close())
    start = datetime.now()
    await destination
    assert (datetime.now() - start).microseconds > 100_000
    assert destination.closed


class Source5(Source[list[int]]):
    def __init__(self, max: int = None):

        async def source_iterator():
            nonlocal max
            cnt = 0
            max = max or 1_000_000
            while cnt < max:
                for m in range(5):
                    yield [i for i in range(m + 1)]
                    await asyncio.sleep(0.001)
                    cnt += 1
                    if cnt >= max:
                        break

        super().__init__(source_iterator())


@pytest.mark.asyncio
async def test_source_destination():
    # close from source
    result = []
    destination = Source5(9) >> Destination()
    async for item in destination:
        result.append(item)

    assert destination.closed
    await destination
    assert True

    assert result == [
        [0], [0, 1], [0, 1, 2], [0, 1, 2, 3], [0, 1, 2, 3, 4],
        [0], [0, 1], [0, 1, 2], [0, 1, 2, 3]
    ]

    # close from destination
    cnt = 0
    result = []
    destination = Source5() >> Destination()
    async for item in destination:
        result.append(item)
        cnt += 1
        if cnt == 12:
            destination.close()

    assert destination.closed
    await destination
    assert True

    assert result == [
        [0], [0, 1], [0, 1, 2], [0, 1, 2, 3], [0, 1, 2, 3, 4],
        [0], [0, 1], [0, 1, 2], [0, 1, 2, 3], [0, 1, 2, 3, 4],
        [0], [0, 1]
    ]


@pytest.mark.asyncio
async def test_with_timeout_and_continue_iteration():
    class SourceX(Source[int]):
        def __init__(self):
            async def source_iterator():
                cnt = 0
                while True:
                    cnt += 1
                    await asyncio.sleep(0.1)
                    yield cnt

            super().__init__(source_iterator())

    result = []
    destination = SourceX() >> Destination()
    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.35):
            async for item in destination:
                result.append(item)

    assert result == [1, 2, 3]

    assert 4 == await anext(destination)
    assert 5 == await anext(destination)
    destination.close()

    with pytest.raises(StopAsyncIteration):
        assert await anext(destination)
