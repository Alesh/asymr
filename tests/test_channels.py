import asyncio
from datetime import datetime
from random import randint

import pytest
from redis.asyncio import from_url

from asymr.channels import Channel
from .test_primitives import Source5, Destination


@pytest.mark.asyncio
async def test_pubsub_close_open():
    # subscriber
    destination = Channel("TEST:PUBSUB", redis=from_url("redis://localhost")) >> Destination()
    assert not destination.closed

    asyncio.get_running_loop().call_later(0.1, lambda: destination.close())
    start = datetime.now()
    async for _ in destination:
        assert False, "Nothing should be received"
    assert (datetime.now() - start).microseconds > 100000
    await destination
    assert destination.closed

    # publisher
    channel = Source5() >> Channel("TEST:PUBSUB", redis=from_url("redis://localhost"))
    asyncio.get_running_loop().call_later(0.1, lambda: channel.close())
    start = datetime.now()
    await channel

    await asyncio.sleep(0.1)
    channel.close()
    assert (datetime.now() - start).microseconds > 100000
    assert channel.closed
    assert channel.source.closed

    # both
    result = []
    destination = Channel("TEST:PUBSUB", redis=from_url("redis://localhost")) >> Destination()

    async def destination_task():
        async for item in destination:
            result.append(item)

    _ = asyncio.create_task(destination_task())

    channel = Source5() >> Channel("TEST:PUBSUB", redis=from_url("redis://localhost"))
    await asyncio.sleep(0.1)
    channel.close()
    await channel

    destination.close()
    await destination

    assert channel.source.closed
    assert channel.closed
    assert destination.closed
    assert result


@pytest.mark.asyncio
async def test_pubsub():
    from redis.asyncio import from_url

    channel = Source5() >> Channel("TEST:PUBSUB", redis=from_url("redis://localhost"))
    await asyncio.sleep((randint(0, 50) + 1) / 1000)

    destination = Channel("TEST:PUBSUB", redis=from_url("redis://localhost")) >> Destination()

    a = 0
    b = 0
    while True:
        if b == 0:
            a += 1
            item = await anext(destination)
            if item != [0, 1, 2, 3, 4]:
                continue

        b += 1
        item = await anext(destination)
        if item == [0, 1, 2, 3, 4]:
            break

    destination.close()
    channel.close()
    await channel
    await destination

    assert a <= 5
    assert b == 5
