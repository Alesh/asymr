import asyncio
import json
from dataclasses import is_dataclass, asdict

from redis.asyncio import Redis

from asymr.primitives import Link, Named, JSONEncoder, D


class Channel(Link[D]):
    """ Generalized Channel class """

    def __new__(cls, name: str, redis: Redis = None, **kwargs):
        if redis:
            return _RedisChannel(name, redis, **kwargs)
        raise "Only Redis supported now"

    def __init__(self, name: str, redis: Redis = None, **kwargs):
        super().__init__()


class _RedisChannel(Named, Link[D]):

    def __init__(self, name: str, redis: Redis = None, **kwargs):
        self._redis = redis
        Named.__init__(self, name)
        Link.__init__(self)

        async def pubsub_task():
            async with self._redis:
                if self.source:
                    try:  # publisher mode
                        while True:
                            data = await anext(self.source)
                            if is_dataclass(data):
                                data = asdict(data)
                            await self._redis.publish(self.name, json.dumps(data, cls=JSONEncoder))
                    except (asyncio.CancelledError, StopAsyncIteration):
                        pass
                else:
                    try:  # subscriber mode
                        async with self._redis.pubsub() as pubsub:
                            await pubsub.subscribe(self.name)
                            while True:
                                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                                if message is not None:
                                    await self._queue.put(json.loads(message['data']))
                    except asyncio.CancelledError:
                        await self._queue.put(None)
                    except Exception as exc:
                        await self._queue.put(exc)

        self._source_tack.cancel()
        self._source_tack = asyncio.create_task(pubsub_task())

    def close(self):
        super().close()
