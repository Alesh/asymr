import math
from typing import AsyncIterator

import pytest

from asymr.primitives import Source, Transform


@pytest.mark.asyncio
async def test_source_destination_destination():
    class FirstSource(Source[list[int]]):
        def __init__(self, *args, **kwargs):
            #
            async def source_it():
                for i in range(10):
                    yield [i]

            super().__init__(source_it(), *args, **kwargs)

    class FirstTransformator(Transform[list[int]]):
        def __init__(self, *args, **kwargs):
            #
            async def transform_it(incoming: AsyncIterator):
                async for first, in incoming:
                    yield [first, first * first]

            super().__init__(transform_it, *args, **kwargs)

    class SecondTransformator(Transform[list[int]]):
        def __init__(self, *args, **kwargs):
            #
            async def transform_it(incoming):
                async for first, second in incoming:
                    if first == 5:
                        break
                    yield [first, second, first * second]

            super().__init__(transform_it, *args, **kwargs)

    chain = FirstSource("one") >> FirstTransformator("two") >> SecondTransformator("three")
    assert chain.name == 'one:two:three'
    result = [item async for item in chain]
    assert result == [[0, 0, 0], [1, 1, 1], [2, 4, 8], [3, 9, 27], [4, 16, 64]]

    assert chain.closed


@pytest.mark.asyncio
async def test_source_map_reduce():
    async def source_it(max):
        for i in range(max):
            yield i

    async def power_map(incoming: AsyncIterator, y: int):
        async for x in incoming:
            yield int(math.pow(x, y))

    async def sum_odd_reduce(incoming: AsyncIterator):
        accum = []
        async for x in incoming:
            if x % 2:
                accum.append(x)
        yield sum(accum)

    r = [i async for i in (Source(source_it(12)) >> Transform(lambda ait: power_map(ait, 3)))]
    assert r == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729, 1000, 1331]

    r = [i async for i in (Source(source_it(12))
                           >> Transform(lambda ait: power_map(ait, 3))
                           >> Transform(sum_odd_reduce))]
    assert r == [2556]


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::RuntimeWarning")
async def test_bad_source():
    async def bad_source_it(max):
        for i in range(max):
            return i

    with pytest.raises(TypeError):
        Source(bad_source_it(12))


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::RuntimeWarning")
async def test_bad_transform():
    async def source_it(max):
        for i in range(max):
            yield i

    async def bad_power_map(incoming: AsyncIterator, y: int):
        async for x in incoming:
            return int(math.pow(x, y))

    dest = Source(source_it(12)) >> Transform(lambda ait: bad_power_map(ait, 3))
    with pytest.raises(TypeError):
        [i async for i in dest]
