import typing as t
from collections.abc import AsyncIterator

from asymr.primitives import Source, Transform, D


def source(name: str | t.Callable[..., AsyncIterator[D]] = None):
    """ Source decorator """

    def make_source(source_ait, name) -> t.Type[Source[D]]:
        class _Source(Source[D]):
            def __init__(self, *args, **kwargs):
                super().__init__(source_ait(*args, **kwargs), name)

        return _Source

    if name and (not isinstance(name, str)):
        source_ait, name = name, None
        return make_source(source_ait, name)
    else:
        def wrapper(source_ait):
            return make_source(source_ait, name)

        return wrapper


def transform(suffix: str | t.Callable[..., AsyncIterator[D]] = None):
    """ Transformation decorator """

    def make_transform(transform_ait, name) -> t.Type[Transform[D]]:
        class _Transform(Transform[D]):
            def __init__(self, *args, **kwargs):
                super().__init__(lambda source_ait: transform_ait(source_ait, *args, **kwargs), name)

        return _Transform

    if suffix and (not isinstance(suffix, str)):
        transform_ait, suffix = suffix, None
        return make_transform(transform_ait, suffix)
    else:
        def wrapper(transform_ait):
            return make_transform(transform_ait, suffix)

        return wrapper
