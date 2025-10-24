from __future__ import annotations

import asyncio
import atexit
import functools
import threading
from queue import Queue
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional

# @TODO: Re-evaluate whether running a dedicated background event loop is worth the complexity

_bg_loop: Optional[asyncio.AbstractEventLoop] = None
_bg_thread: Optional[threading.Thread] = None
_bg_started = threading.Event()


def _loop_thread_target() -> None:
    global _bg_loop
    loop = asyncio.new_event_loop()
    _bg_loop = loop
    asyncio.set_event_loop(loop)
    _bg_started.set()
    try:
        loop.run_forever()
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()
        _bg_loop = None


def _ensure_bg_loop_started() -> None:
    global _bg_thread
    if _bg_loop is not None:
        return
    if _bg_thread is not None and _bg_thread.is_alive():
        return
    _bg_started.clear()
    _bg_thread = threading.Thread(target=_loop_thread_target, name="ovalbee-bg-loop", daemon=True)
    _bg_thread.start()
    _bg_started.wait()


def _bg_run(coro: Coroutine[Any, Any, Any], timeout: Optional[float] = None) -> Any:
    _ensure_bg_loop_started()
    assert _bg_loop is not None
    fut = asyncio.run_coroutine_threadsafe(coro, _bg_loop)
    return fut.result(timeout=timeout)


def _bg_submit(coro: Coroutine[Any, Any, Any]):
    _ensure_bg_loop_started()
    assert _bg_loop is not None
    return asyncio.run_coroutine_threadsafe(coro, _bg_loop)


def _stop_bg_loop() -> None:
    global _bg_loop, _bg_thread
    loop = _bg_loop
    if loop is None:
        return
    try:
        loop.call_soon_threadsafe(loop.stop)
    except Exception:
        pass
    if _bg_thread and _bg_thread.is_alive():
        _bg_thread.join(timeout=2.0)
    _bg_loop = None
    _bg_thread = None


atexit.register(_stop_bg_loop)


def sync_compatible(async_fn: Callable[..., Coroutine[Any, Any, Any]]):
    @functools.wraps(async_fn)
    def wrapper(self, *args, **kwargs):
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            return _bg_run(async_fn(self, *args, **kwargs))
        if current_loop is _bg_loop:
            return async_fn(self, *args, **kwargs)

        async def _await_bg():
            fut = _bg_submit(async_fn(self, *args, **kwargs))
            return await asyncio.wrap_future(fut)

        return _await_bg()

    return wrapper


def sync_compatible_generator(async_gen_fn: Callable[..., AsyncGenerator[Any, None]]):
    # Shared pump creator to fan-out items via a thread-safe Queue
    def _make_pump(self, *args, **kwargs):
        q: Queue = Queue(maxsize=1)
        sentinel = object()
        exc_holder: dict = {}

        async def _pump():
            try:
                async for item in async_gen_fn(self, *args, **kwargs):
                    await asyncio.to_thread(q.put, item)
            except Exception as e:
                exc_holder["exc"] = e
            finally:
                await asyncio.to_thread(q.put, sentinel)

        fut = _bg_submit(_pump())
        return q, sentinel, exc_holder, fut

    @functools.wraps(async_gen_fn)
    def sync_wrapper(self, *args, **kwargs) -> Iterator[Any]:
        q, sentinel, exc_holder, fut = _make_pump(self, *args, **kwargs)
        try:
            while True:
                item = q.get()
                if item is sentinel:
                    break
                yield item
            if "exc" in exc_holder:
                raise exc_holder["exc"]
        finally:
            try:
                fut.result(timeout=0.1)
            except Exception:
                pass

    @functools.wraps(async_gen_fn)
    def wrapper(self, *args, **kwargs):
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            return sync_wrapper(self, *args, **kwargs)

        if current_loop is _bg_loop:
            return async_gen_fn(self, *args, **kwargs)

        async def agen():
            q, sentinel, exc_holder, fut = _make_pump(self, *args, **kwargs)
            try:
                while True:
                    item = await asyncio.to_thread(q.get)
                    if item is sentinel:
                        break
                    yield item
                if "exc" in exc_holder:
                    raise exc_holder["exc"]
            finally:
                try:
                    await asyncio.wrap_future(fut)
                except Exception:
                    pass

        return agen()

    return wrapper
