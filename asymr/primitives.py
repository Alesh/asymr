import asyncio
import json
import typing as t
from collections.abc import AsyncIterator
from dataclasses import is_dataclass, asdict, Field
from datetime import datetime, date
from decimal import Decimal


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if is_dataclass(obj):
            return asdict(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


@t.runtime_checkable
class Dataclass(t.Protocol):
    """ Any dataclass """
    __dataclass_fields__: t.ClassVar[dict[str, Field[t.Any]]]


D = t.TypeVar('D', Dataclass, dict[str, t.Any], list[t.Any])


class Named:
    def __init__(self, name: str = None):
        self.__name = name

    @property
    def name(self) -> str | None:
        """ Name """
        return self.__name


class Suffixed(Named):
    source: Named

    def __init__(self, suffix: str = None):
        super().__init__(suffix)

    @property
    def name(self) -> str | None:
        """ Name """
        name = self.source and self.source.name
        if name:
            if suffix := super().name:
                if suffix[0].isalnum():
                    name = f'{name}:{suffix}'
                else:
                    name = name + suffix
        return name


class Link(AsyncIterator[D]):
    """ Base data link """

    def __init__(self, source: AsyncIterator[D] = None):
        self.__source = source
        self._queue = asyncio.Queue()

        async def source_task():
            try:
                if not self.__source:
                    raise RuntimeError("Source is not set")
                while True:
                    data = await anext(self.__source)
                    await self._queue.put(data)
            except asyncio.CancelledError:
                await self._queue.put(None)
            except Exception as exc:
                await self._queue.put(exc)

        self._source_tack = asyncio.create_task(source_task())

    def __await__(self):
        if self._queue._unfinished_tasks * (None,) == tuple(self._queue._queue):
            self._clear_queue()
        sources = []
        destination = self
        while destination.source:
            sources.append(destination.source)
            destination = destination.source
        return asyncio.gather(self._queue.join(), self._source_tack, *sources).__await__()

    def __rshift__(self, destination: 'Link') -> 'Link':
        destination.source = self
        return destination

    async def __anext__(self) -> D:
        if not self.closed:
            item = await self._queue.get()
            self._queue.task_done()
            if item is not None:
                if not isinstance(item, Exception):
                    return item
                raise item
        raise StopAsyncIteration

    @property
    def source(self) -> t.Optional['Link']:
        """ Data source"""
        if isinstance(self.__source, Link):
            return self.__source

    @source.setter
    def source(self, source: t.Union[AsyncIterator[D], 'Link']) -> None:
        if source is not None:
            if self.__source is not None:
                raise RuntimeError("Source already set")
            self.__source = source

    @property
    def closed(self):
        """ True if source is closed """
        return self._source_tack.done() and self._queue.qsize() == 0

    def close(self):
        """ Closes source  """
        if self.source:
            self.source.close()
        if not self.closed:
            self._source_tack.cancel()
            self._clear_queue()

    def _clear_queue(self):
        while self._queue.qsize():
            self._queue.get_nowait()
            self._queue.task_done()


class Source(Link[D], Named):
    """ Data source """

    def __init__(self, source_it: AsyncIterator[D], name: str = None):
        if not isinstance(source_it, AsyncIterator):
            raise TypeError("First parameter must be an async iterator")
        Link.__init__(self, source_it)
        Named.__init__(self, name)

    @property
    def source(self):
        return None

    @source.setter
    def source(self, source):
        raise AttributeError("can't set attribute 'source'")


class Destination(Link[D], Suffixed):
    """ Data destination """

    def __init__(self, suffix: str = None):
        Link.__init__(self)
        Suffixed.__init__(self, suffix)


class Transform(Destination[D]):
    """ Data transformator """

    def __init__(self, transform_it: t.Callable[[AsyncIterator[D]], AsyncIterator[D]], suffix: str = None):
        Destination.__init__(self, suffix)

        async def transform_task():
            if self.source:
                aiter_ = transform_it(self.source)
                try:
                    if not isinstance(aiter_, AsyncIterator):
                        raise TypeError("First parameter must be an async iterator")
                    async for item in aiter_:
                        await self._queue.put(item)
                except asyncio.CancelledError:
                    await self._queue.put(None)
                except Exception as exc:
                    await self._queue.put(exc)
                finally:
                    self.source.close()

        self._source_tack.cancel()
        self._source_tack = asyncio.create_task(transform_task())