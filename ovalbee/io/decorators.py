from __future__ import annotations

import asyncio
import functools


def sync_compatible(async_fn):
    @functools.wraps(async_fn)
    def wrapper(self, *args, **kwargs):
        coro = async_fn(self, *args, **kwargs)
        try:
            asyncio.get_running_loop()
            # We're in an async context, return the coroutine
            return coro
        except RuntimeError:
            return asyncio.run(coro)

    return wrapper
