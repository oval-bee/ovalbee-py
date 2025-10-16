# coding: utf-8
""""""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Dict,
    Generator,
    Iterable,
    Literal,
    Mapping,
    Optional,
    Union,
)

import httpx

from ovalbee.api.storage_api import StorageApi
from ovalbee.io.network_exceptions import (
    RetryableRequestException,
    process_requests_exception,
    process_unhandled_request,
)

OVALBEE_ENV_FILE = os.path.join(Path.home(), "ovalbee.env")

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Api:
    """
    Ovalbee API connection to the server which allows user to communicate with Ovalbee.
    """

    def __init__(
        self,
        retry_count: Optional[int] = 10,
        retry_sleep_sec: Optional[int] = None,
    ):
        self._token: Optional[str] = None
        self._server_address: Optional[str] = None
        self._headers: Dict[str, str] = {}
        self._httpx_client: Optional[httpx.AsyncClient] = None
        self._retry_count = retry_count
        if self._retry_count is None:
            self._retry_count = os.getenv("OVALBEE_API_RETRY_COUNT", 10)
        self._retry_sleep_sec = retry_sleep_sec
        if self._retry_sleep_sec is None:
            self._retry_sleep_sec = os.getenv("OVALBEE_API_RETRY_SLEEP_SEC", 1)

        self.storage = StorageApi()

    def _set_client(self):
        """
        Set async httpx client with HTTP/2 if it is not set yet.
        """
        if self._httpx_client is None:
            self._httpx_client = httpx.AsyncClient(http2=True)

    async def post(
        self,
        method: str,
        json: Dict = None,
        content: Union[str, bytes, Iterable[bytes], AsyncIterable[bytes]] = None,
        files: Union[Mapping] = None,
        params: Union[str, bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        raise_error: Optional[bool] = False,
        timeout: httpx._types.TimeoutTypes = 60,
    ) -> httpx.Response:
        """
        Performs POST request to server with given parameters using httpx.

        :param method: Method name.
        :type method: str
        :param json: Dictionary to send in the body of request.
        :type json: dict, optional
        :param content: Bytes with data content or dictionary with params.
        :type content: bytes or dict, optional
        :param files: Files to send in the body of request.
        :type files: dict, optional
        :param params: URL query parameters.
        :type params: str, bytes, optional
        :param headers: Custom headers to include in the request.
        :type headers: dict, optional
        :param retries: The number of attempts to connect to the server.
        :type retries: int, optional
        :param raise_error: Define, if you'd like to raise error if connection is failed.
        :type raise_error: bool, optional
        :param timeout: Overall timeout for the request.
        :type timeout: float, optional
        :return: Response object
        :rtype: :class:`httpx.Response`
        """
        self._set_client()

        if retries is None:
            retries = self._retry_count

        url = self._server_address + "/v1/" + method
        logger.trace(f"POST {url}")

        if headers is None:
            headers = self._headers.copy()
        else:
            headers = {**self._headers, **headers}

        for retry_idx in range(retries):
            response = None
            try:
                response = await self._httpx_client.post(
                    url,
                    content=content,
                    files=files,
                    json=json,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )
                if response.status_code != httpx.codes.OK:
                    Api._raise_for_status_httpx(response)
                return response
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                if (
                    isinstance(exc, httpx.HTTPStatusError)
                    and response.status_code == 400
                    and self._token is None
                ):
                    logger.warning("API_TOKEN env variable is undefined.")
                if raise_error:
                    raise exc
                else:
                    await process_requests_exception(
                        logger,
                        exc,
                        method,
                        url,
                        verbose=True,
                        swallow_exc=True,
                        sleep_sec=min(self._retry_sleep_sec * (2**retry_idx), 60),
                        response=response,
                        retry_info={"retry_idx": retry_idx + 1, "retry_limit": retries},
                    )
            except Exception as exc:
                process_unhandled_request(logger, exc)
        raise httpx.RequestError(
            f"Retry limit exceeded ({url})",
            request=getattr(response, "request", None),
        )

    async def stream(
        self,
        method: str,
        method_type: Literal["GET", "POST"],
        data: Union[bytes, Dict],
        headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
        chunk_size: int = 8192,
        timeout: httpx._types.TimeoutTypes = 60,
        **kwargs,
    ) -> AsyncGenerator:
        """
        Performs asynchronous streaming GET or POST request to server with given parameters.
        Yield chunks of data and hash of the whole content to check integrity of the data stream.

        :param method: Method name for the request.
        :type method: str
        :param method_type: Request type ('GET' or 'POST').
        :type method_type: str
        :param data: Bytes with data content or dictionary with params.
        :type data: bytes or dict
        :param headers: Custom headers to include in the request.
        :type headers: dict, optional
        :param retries: The number of retry attempts.
        :type retries: int, optional
        :param range_start: Start byte position for streaming.
        :type range_start: int, optional
        :param range_end: End byte position for streaming.
        :type range_end: int, optional
        :param chunk_size: Size of the chunk to read from the stream. Default is 8192.
        :type chunk_size: int, optional
        :param use_public_api: Define if public API should be used.
        :type use_public_api: bool, optional
        :param timeout: Overall timeout for the request.
        :type timeout: float, optional
        :return: Async generator object.
        :rtype: :class:`AsyncGenerator`
        """
        self._set_client()

        if retries is None:
            retries = self._retry_count

        url = self._server_address + "/v1/" + method
        logger.trace(f"{method_type} {url}")

        if headers is None:
            headers = self._headers.copy()
        else:
            headers = {**self._headers, **headers}

        params = kwargs.get("params", None)
        if "content" in kwargs or "json_body" in kwargs:
            content = kwargs.get("content", None)
            json_body = kwargs.get("json_body", None)
        else:
            if isinstance(data, (bytes, Generator)):
                content = data
                json_body = None
            elif isinstance(data, Dict):
                json_body = {**data}
                content = None
            else:
                raise ValueError("Data should be either bytes or dict")

        if range_start is not None or range_end is not None:
            headers["Range"] = f"bytes={range_start or ''}-{range_end or ''}"
            logger.debug(f"Setting Range header: {headers['Range']}")

        for retry_idx in range(retries):
            total_streamed = 0
            try:
                if method_type == "POST":
                    response = self._httpx_client.stream(
                        method_type,
                        url,
                        content=content,
                        json=json_body,
                        headers=headers,
                        timeout=timeout,
                        params=params,
                    )
                elif method_type == "GET":
                    response = self._httpx_client.stream(
                        method_type,
                        url,
                        content=content,
                        json=json_body,
                        headers=headers,
                        timeout=timeout,
                        params=params,
                    )
                else:
                    raise NotImplementedError(
                        f"Unsupported method type: {method_type}. Supported types: 'GET', 'POST'"
                    )

                async with response as resp:
                    expected_size = int(resp.headers.get("content-length", 0))
                    if resp.status_code not in [
                        httpx.codes.OK,
                        httpx.codes.PARTIAL_CONTENT,
                    ]:
                        Api._raise_for_status_httpx(resp)

                    # received hash of the content to check integrity of the data stream
                    hhash = resp.headers.get("x-content-checksum-sha256", None)
                    try:
                        async for chunk in resp.aiter_raw(chunk_size):
                            yield chunk, hhash
                            total_streamed += len(chunk)
                    except Exception as e:
                        raise RetryableRequestException(repr(e))

                    if expected_size != 0 and total_streamed != expected_size:
                        raise ValueError(
                            f"Streamed size does not match the expected: {total_streamed} != {expected_size}"
                        )
                    logger.trace(f"Streamed size: {total_streamed}, expected size: {expected_size}")
                    return
            except (httpx.RequestError, httpx.HTTPStatusError, RetryableRequestException) as e:
                if (
                    isinstance(e, httpx.HTTPStatusError)
                    and resp.status_code == 400
                    and self._token is None
                ):
                    logger.warning("API_TOKEN env variable is undefined.")
                retry_range_start = total_streamed + (range_start or 0)
                if total_streamed != 0:
                    retry_range_start += 1
                headers["Range"] = f"bytes={retry_range_start}-{range_end or ''}"
                logger.debug(f"Setting Range header {headers['Range']} for retry")
                await process_requests_exception(
                    logger,
                    e,
                    method,
                    url,
                    verbose=True,
                    swallow_exc=True,
                    sleep_sec=min(self._retry_sleep_sec * (2**retry_idx), 60),
                    response=locals().get("resp"),
                    retry_info={"retry_idx": retry_idx + 1, "retry_limit": retries},
                )
            except Exception as e:
                process_unhandled_request(logger, e)
        raise httpx.RequestError(
            message=f"Retry limit exceeded ({url})",
            request=resp.request if locals().get("resp") else None,
        )

    @staticmethod
    def _raise_for_status_httpx(response: httpx.Response):
        """
        Raise error and show message with error code if given response can not connect to server.
        :param response: Response class object
        """
        http_error_msg = ""

        if hasattr(response, "reason_phrase"):
            reason = response.reason_phrase
        else:
            reason = "Can't get reason"

        def decode_response_content(response: httpx.Response):
            try:
                return response.content.decode("utf-8")
            except Exception as e:
                if hasattr(response, "is_stream_consumed"):
                    return f"Stream is consumed. {e}"
                else:
                    return f"Can't decode response content: {e}"

        if 400 <= response.status_code < 500:
            http_error_msg = "%s Client Error: %s for url: %s (%s)" % (
                response.status_code,
                reason,
                response.url,
                decode_response_content(response),
            )

        elif 500 <= response.status_code < 600:
            http_error_msg = "%s Server Error: %s for url: %s (%s)" % (
                response.status_code,
                reason,
                response.url,
                decode_response_content(response),
            )

        if http_error_msg:
            raise httpx.HTTPStatusError(
                message=http_error_msg, response=response, request=response.request
            )
