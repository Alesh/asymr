import math
import pytest

import asymr


@pytest.mark.asyncio
async def test_map_reduce_deco():

    @asymr.source()
    async def source_it(max):
        for i in range(max):
            yield i

    r = [i async for i in source_it(12)]
    assert r == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]


    @asymr.transform()
    async def power(incoming, y):
        async for x in incoming:
            yield int(math.pow(x, y))

    r = [i async for i in (source_it(12) >> power(3))]
    assert r == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729, 1000, 1331]


    @asymr.transform()
    async def sum_odd_by(incoming, cnt):
        accum = []
        async for x in incoming:
            if x % 2:
                accum.append(x)
                if len(accum) == cnt:
                    yield sum(accum)
                    accum.clear()


    r = [i async for i in (source_it(12) >> power(3) >> sum_odd_by(2))]
    assert r == [28, 468, 2060]


    r = [i async for i in (source_it(500) >> power(5) >> sum_odd_by(50))]
    assert r == [83291672500, 5249375017500, 55413958362500, 280576041707500, 960734625052500]
