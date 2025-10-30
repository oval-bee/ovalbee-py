# coding: utf-8
""""""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import (
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
import requests
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

from ovalbee.io.network_exceptions import (
    RetryableRequestException,
    process_requests_exception,
    process_requests_exception_async,
    process_unhandled_request,
)

API_VERSION = None  # "v1"
OVALBEE_ENV_FILE = os.path.join(Path.home(), "ovalbee.env")

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class _Api:
    """
    Ovalbee API connection to the server which allows user to communicate with Ovalbee.
    """

    _checked_servers = set()

    def __init__(
        self,
        server_address: Optional[str] = None,
        token: Optional[str] = None,
        retry_count: Optional[int] = 10,
        retry_sleep_sec: Optional[int] = None,
        external_client: Optional[bool] = True,  # True for external usage, False to use in ovalbee nodes
    ):
        # authorization
        self._token = token
        self._server_address = server_address
        self._headers = {"Authorization": self._token} if self._token else {}
        self._additional_headers = {}
        self._external_client = external_client

        # logger
        self.logger = logger

        # retry settings
        self._retry_count = retry_count
        if self._retry_count is None:
            self._retry_count = os.getenv("OVALBEE_API_RETRY_COUNT", 10)
        self._retry_sleep_sec = retry_sleep_sec
        if self._retry_sleep_sec is None:
            self._retry_sleep_sec = os.getenv("OVALBEE_API_RETRY_SLEEP_SEC", 1)

        # httpx clients
        self._async_httpx_client: httpx.AsyncClient = None
        self._httpx_client: httpx.Client = None
        self._semaphore: Optional[asyncio.Semaphore] = None

        # HTTPS redirect check
        self._skip_https_redirect_check = True  # TODO: change logic later
        self._require_https_redirect_check = (
            False
            if self._skip_https_redirect_check
            else not self._server_address.startswith("https://")
        )

        # additional settings
        self.context = {}
        self._additional_fields = {}

    def post(
        self,
        method: str,
        data: Dict,
        retries: Optional[int] = None,
        stream: Optional[bool] = False,
        raise_error: Optional[bool] = False,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """
        Performs POST request to server with given parameters.

        :param method: Method name.
        :type method: str
        :param data: Dictionary to send in the body of the :class:`Request`.
        :type data: dict
        :param retries: The number of attempts to connect to the server.
        :type retries: int, optional
        :param stream: Define, if you'd like to get the raw socket response from the server.
        :type stream: bool, optional
        :param raise_error: Define, if you'd like to raise error if connection is failed. Retries will be ignored.
        :type raise_error: bool, optional
        :return: Response object
        :rtype: :class:`Response<Response>`
        """
        if retries is None:
            retries = self._retry_count

        url = self._prepare_url(method)
        logger.info(f"POST {url}")
        if headers is not None:
            headers = {**self._headers, **self._additional_headers, **headers}
        else:
            headers = {**self._headers, **self._additional_headers}

        for retry_idx in range(retries):
            response = None
            try:
                if type(data) is bytes:
                    response = requests.post(url, data=data, headers=headers, stream=stream)
                elif type(data) is MultipartEncoderMonitor or type(data) is MultipartEncoder:
                    response = requests.post(
                        url,
                        data=data,
                        headers={"Content-Type": data.content_type, **headers},
                        stream=stream,
                    )
                else:
                    json_body = data
                    if type(data) is dict:
                        json_body = {**data, **self._additional_fields}
                    response = requests.post(url, json=json_body, headers=headers, stream=stream)

                if response.status_code != requests.codes.ok:  # pylint: disable=no-member
                    _Api._raise_for_status(response)
                return response
            except requests.RequestException as exc:
                if (
                    isinstance(exc, requests.exceptions.HTTPError)
                    and response.status_code == 400
                    and self._token is None
                ):
                    self.logger.info("API_TOKEN env variable is undefined.")
                if raise_error:
                    raise exc
                else:
                    process_requests_exception(
                        self.logger,
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
                process_unhandled_request(self.logger, exc)
        raise requests.exceptions.RetryError("Retry limit exceeded ({!r})".format(url))

    def get(
        self,
        method: str,
        params: Dict,
        retries: Optional[int] = None,
        stream: Optional[bool] = False,
        data: Optional[Dict] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """
        Performs GET request to server with given parameters.

        :param method:
        :type method: str
        :param params: Dictionary to send in the body of the :class:`Request`.
        :type params: dict
        :param retries: The number of attempts to connect to the server.
        :type retries: int, optional
        :param stream: Define, if you'd like to get the raw socket response from the server.
        :type stream: bool, optional
        :param data: Dictionary to send in the body of the :class:`Request`.
        :type data: dict, optional
        :return: Response object
        :rtype: :class:`Response<Response>`
        """
        if retries is None:
            retries = self._retry_count

        url = self._prepare_url(method)
        logger.info(f"GET {url}")
        if headers is not None:
            headers = {**self._headers, **self._additional_headers, **headers}
        else:
            headers = {**self._headers, **self._additional_headers}

        for retry_idx in range(retries):
            response = None
            try:
                json_body = params
                if type(params) is dict:
                    json_body = {**params, **self._additional_fields}
                response = requests.get(
                    url, params=json_body, data=data, headers=headers, stream=stream
                )

                if response.status_code != requests.codes.ok:  # pylint: disable=no-member
                    _Api._raise_for_status(response)
                return response
            except requests.RequestException as exc:
                if (
                    isinstance(exc, requests.exceptions.HTTPError)
                    and response.status_code == 400
                    and self._token is None
                ):
                    self.logger.info("API_TOKEN env variable is undefined.")
                process_requests_exception(
                    self.logger,
                    exc,
                    method,
                    url,
                    verbose=True,
                    swallow_exc=True,
                    sleep_sec=min(self._retry_sleep_sec * (2**retry_idx), 60),
                    response=response,
                    retry_info={"retry_idx": retry_idx + 2, "retry_limit": retries},
                )
            except Exception as exc:
                process_unhandled_request(self.logger, exc)

    def put(
        self,
        method: str,
        data: Dict,
        retries: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """
        Performs PUT request to server with given parameters.

        :param method:
        :type method: str
        :param data: Dictionary to send in the body of the :class:`Request`.
        :type data: dict
        :param retries: The number of attempts to connect to the server.
        :type retries: int, optional
        :return: Response object
        :rtype: :class:`Response<Response>`
        """
        if retries is None:
            retries = self._retry_count

        url = self._prepare_url(method)
        logger.info(f"PUT {url}")
        if headers is not None:
            headers = {**self._headers, **self._additional_headers, **headers}
        else:
            headers = {**self._headers, **self._additional_headers}

        for retry_idx in range(retries):
            response = None
            try:
                json_body = data
                if type(data) is dict:
                    json_body = {**data, **self._additional_fields}
                response = requests.put(url, json=json_body, headers=headers)

                if response.status_code != requests.codes.ok:  # pylint: disable=no-member
                    _Api._raise_for_status(response)
                return response
            except requests.RequestException as exc:
                if (
                    isinstance(exc, requests.exceptions.HTTPError)
                    and response.status_code == 400
                    and self._token is None
                ):
                    self.logger.info("API_TOKEN env variable is undefined.")
                process_requests_exception(
                    self.logger,
                    exc,
                    method,
                    url,
                    verbose=True,
                    swallow_exc=True,
                    sleep_sec=min(self._retry_sleep_sec * (2**retry_idx), 60),
                    response=response,
                    retry_info={"retry_idx": retry_idx + 2, "retry_limit": retries},
                )
            except Exception as exc:
                process_unhandled_request(self.logger, exc)

    def delete(
        self,
        method: str,
        retries: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Performs DELETE request to server with given parameters.

        :param method:
        :type method: str
        :param retries: The number of attempts to connect to the server.
        :type retries: int, optional
        """
        if retries is None:
            retries = self._retry_count

        url = self._prepare_url(method)
        logger.info(f"DELETE {url}")
        if headers is not None:
            headers = {**self._headers, **self._additional_headers, **headers}
        else:
            headers = {**self._headers, **self._additional_headers}

        for retry_idx in range(retries):
            response = None
            try:
                response = requests.delete(url, headers=headers)

                if response.status_code != requests.codes.ok:  # pylint: disable=no-member
                    _Api._raise_for_status(response)
                return
            except requests.RequestException as exc:
                if (
                    isinstance(exc, requests.exceptions.HTTPError)
                    and response.status_code == 400
                    and self._token is None
                ):
                    self.logger.info("API_TOKEN env variable is undefined.")
                process_requests_exception(
                    self.logger,
                    exc,
                    method,
                    url,
                    verbose=True,
                    swallow_exc=True,
                    sleep_sec=min(self._retry_sleep_sec * (2**retry_idx), 60),
                    response=response,
                    retry_info={"retry_idx": retry_idx + 2, "retry_limit": retries},
                )
            except Exception as exc:
                process_unhandled_request(self.logger, exc)

    def _prepare_url(self, method: str) -> str:
        """
        Prepares the API endpoint URL.
        """
        url = self.api_server_address

        if API_VERSION:
            url = os.path.join(url, API_VERSION)
        url = os.path.join(url, method)
        return url

    @staticmethod
    def _raise_for_status(response: requests.Response):
        """
        Raise error and show message with error code if given response can not connect to server.
        :param response: Request class object
        """
        http_error_msg = ""
        if isinstance(response.reason, bytes):
            try:
                reason = response.reason.decode("utf-8")
            except UnicodeDecodeError:
                reason = response.reason.decode("iso-8859-1")
        else:
            reason = response.reason

        if 400 <= response.status_code < 500:
            http_error_msg = "%s Client Error: %s for url: %s (%s)" % (
                response.status_code,
                reason,
                response.url,
                response.content.decode("utf-8"),
            )

        elif 500 <= response.status_code < 600:
            http_error_msg = "%s Server Error: %s for url: %s (%s)" % (
                response.status_code,
                reason,
                response.url,
                response.content.decode("utf-8"),
            )

        if http_error_msg:
            raise requests.exceptions.HTTPError(http_error_msg, response=response)

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

    @staticmethod
    def parse_error(
        response: requests.Response,
        default_error: Optional[str] = "Error",
        default_message: Optional[str] = "please, contact administrator",
    ):
        """
        Processes error from response.

        :param response: Request object.
        :type method: Request
        :param default_error: Error description.
        :type method: str, optional
        :param default_message: Message to user.
        :type method: str, optional
        :return: Number of error and message about curren connection mistake
        :rtype: :class:`int`, :class:`str`
        """
        ERROR_FIELD = "error"
        MESSAGE_FIELD = "message"
        DETAILS_FIELD = "details"

        try:
            data_str = response.content.decode("utf-8")
            data = json.loads(data_str)
            error = data.get(ERROR_FIELD, default_error)
            details = data.get(DETAILS_FIELD, {})
            if type(details) is dict:
                message = details.get(MESSAGE_FIELD, default_message)
            else:
                message = details[0].get(MESSAGE_FIELD, default_message)

            return error, message
        except Exception:
            return "", ""

    def pop_header(self, key: str) -> str:
        """ """
        if key not in self._headers:
            raise KeyError(f"Header {key!r} not found")
        return self._headers.pop(key)

    # def _check_https_redirect(self):
    #     """
    #     Check if HTTP server should be redirected to HTTPS.
    #     If the server has already been checked before (for any instance of this class),
    #     skip the check to avoid redundant network requests.
    #     """
    #     if self._require_https_redirect_check is True:
    #         if self._server_address in _Api._checked_servers:
    #             self._require_https_redirect_check = False
    #             return

    #         try:
    #             response = requests.get(
    #                 self._server_address.replace("http://", "https://"),
    #                 allow_redirects=False,
    #                 timeout=(5, 15),
    #             )
    #             response.raise_for_status()
    #             self._server_address = self._server_address.replace("http://", "https://")
    #             msg = (
    #                 "You're using HTTP server address while the server requires HTTPS. "
    #                 "Ovalbee automatically changed the server address to HTTPS for you. "
    #                 f"Consider updating your server address to {self._server_address}"
    #             )
    #             self.logger.info(msg)
    #         except:
    #             pass
    #         finally:
    #             _Api._checked_servers.add(self._server_address)
    #             self._require_https_redirect_check = False

    @property
    def api_server_address(self) -> str:
        """
        Get API server address.

        :return: API server address.
        :rtype: :class:`str`
        :Usage example:

         .. code-block:: python

            import ovalbee as ob

            api = ob.Api(server_address='https://app.ovalbee.com', token='4r47N...xaTatb')
            print(api.api_server_address)
            # Output:
            # 'https://app.ovalbee.com/public/api'
        """
        if not self._external_client:
            return self._server_address
        return f"{self._server_address}/api"

    def post_httpx(
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

        url = self._prepare_url(method)
        logger.info(f"POST {url}")

        if headers is None:
            headers = {**self._headers, **self._additional_headers}
        else:
            headers = {**self._headers, **self._additional_headers, **headers}

        for retry_idx in range(retries):
            response = None
            try:
                response = self._httpx_client.post(
                    url,
                    content=content,
                    files=files,
                    json=json,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )
                if response.status_code != httpx.codes.OK:
                    _Api._raise_for_status_httpx(response)
                return response
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                if (
                    isinstance(exc, httpx.HTTPStatusError)
                    and response.status_code == 400
                    and self._token is None
                ):
                    self.logger.info("API_TOKEN env variable is undefined.")
                if raise_error:
                    raise exc
                else:
                    process_requests_exception(
                        self.logger,
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
                process_unhandled_request(self.logger, exc)
        raise httpx.RequestError(
            f"Retry limit exceeded ({url})",
            request=getattr(response, "request", None),
        )

    def get_httpx(
        self,
        method: str,
        params: httpx._types.QueryParamTypes,
        retries: Optional[int] = None,
        timeout: httpx._types.TimeoutTypes = 60,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        Performs GET request to server with given parameters.

        :param method: Method name.
        :type method: str
        :param params: URL query parameters.
        :type params: httpx._types.QueryParamTypes
        :param retries: The number of attempts to connect to the server.
        :type retries: int, optional
        :param timeout: Overall timeout for the request.
        :type timeout: float, optional
        :return: Response object
        :rtype: :class:`Response<Response>`
        """
        self._set_client()

        if retries is None:
            retries = self._retry_count

        url = self._prepare_url(method)
        logger.info(f"GET {url}")
        if headers is None:
            headers = {**self._headers, **self._additional_headers}
        else:
            headers = {**self._headers, **self._additional_headers, **headers}

        if isinstance(params, Dict):
            request_params = {**params, **self._additional_fields}
        else:
            request_params = params

        for retry_idx in range(retries):
            response = None
            try:
                response = self._httpx_client.get(
                    url,
                    params=request_params,
                    headers=headers,
                    timeout=timeout,
                )
                if response.status_code != httpx.codes.OK:
                    _Api._raise_for_status_httpx(response)
                return response
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                if (
                    isinstance(exc, httpx.HTTPStatusError)
                    and response.status_code == 400
                    and self._token is None
                ):
                    self.logger.info("API_TOKEN env variable is undefined.")
                process_requests_exception(
                    self.logger,
                    exc,
                    method,
                    url,
                    verbose=True,
                    swallow_exc=True,
                    sleep_sec=min(self._retry_sleep_sec * (2**retry_idx), 60),
                    response=response,
                    retry_info={"retry_idx": retry_idx + 2, "retry_limit": retries},
                )
            except Exception as exc:
                process_unhandled_request(self.logger, exc)

    def stream(
        self,
        method: str,
        method_type: Literal["GET", "POST"],
        data: Union[bytes, Dict],
        headers: Optional[Dict[str, str]] = None,
        retries: Optional[int] = None,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
        raise_error: Optional[bool] = False,
        chunk_size: int = 8192,
        timeout: httpx._types.TimeoutTypes = 60,
    ) -> Generator:
        """
        Performs streaming GET or POST request to server with given parameters.
        Multipart is not supported.

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
        :param raise_error: If True, raise raw error if the request fails.
        :type raise_error: bool, optional
        :param chunk_size: Size of the chunks to stream.
        :type chunk_size: int, optional
        :param timeout: Overall timeout for the request.
        :type timeout: float, optional
        :return: Generator object.
        :rtype: :class:`Generator`
        """
        self._set_client()

        if retries is None:
            retries = self._retry_count

        url = self._prepare_url(method)

        if headers is None:
            headers = {**self._headers, **self._additional_headers}
        else:
            headers = {**self._headers, **self._additional_headers, **headers}

        logger.info(f"{method_type} {url}")

        if isinstance(data, (bytes, Generator)):
            content = data
            json_body = None
            params = None
        elif isinstance(data, Dict):
            json_body = {**data, **self._additional_fields}
            content = None
            params = None
        else:
            params = data
            content = None
            json_body = None

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
                        params=params,
                        headers=headers,
                        timeout=timeout,
                    )

                elif method_type == "GET":
                    response = self._httpx_client.stream(
                        method_type,
                        url,
                        params=json_body or params,
                        headers=headers,
                        timeout=timeout,
                    )
                else:
                    raise NotImplementedError(
                        f"Unsupported method type: {method_type}. Supported types: 'GET', 'POST'"
                    )

                with response as resp:
                    expected_size = int(resp.headers.get("content-length", 0))
                    if resp.status_code not in [
                        httpx.codes.OK,
                        httpx.codes.PARTIAL_CONTENT,
                    ]:
                        _Api._raise_for_status_httpx(resp)

                    hhash = resp.headers.get("x-content-checksum-sha256", None)
                    try:
                        for chunk in resp.iter_raw(chunk_size):
                            yield chunk, hhash
                            total_streamed += len(chunk)
                    except Exception as e:
                        raise RetryableRequestException(repr(e))

                    if expected_size != 0 and total_streamed != expected_size:
                        raise ValueError(
                            f"Streamed size does not match the expected: {total_streamed} != {expected_size}"
                        )
                    logger.info(f"Streamed size: {total_streamed}, expected size: {expected_size}")
                    return
            except (httpx.RequestError, httpx.HTTPStatusError, RetryableRequestException) as e:
                if (
                    isinstance(e, httpx.HTTPStatusError)
                    and resp.status_code == 400
                    and self._token is None
                ):
                    self.logger.info("API_TOKEN env variable is undefined.")
                retry_range_start = total_streamed + (range_start or 0)
                if total_streamed != 0:
                    retry_range_start += 1
                headers["Range"] = f"bytes={retry_range_start}-{range_end or ''}"
                logger.debug(f"Setting Range header {headers['Range']} for retry")
                if raise_error:
                    raise e
                else:
                    process_requests_exception(
                        self.logger,
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
                process_unhandled_request(self.logger, e)
        raise httpx.RequestError(
            message=f"Retry limit exceeded ({url})",
            request=resp.request if locals().get("resp") else None,
        )

    async def post_async(
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
        self._set_async_client()

        if retries is None:
            retries = self._retry_count

        url = self._prepare_url(method)
        logger.info(f"POST {url}")

        if headers is None:
            headers = {**self._headers, **self._additional_headers}
        else:
            headers = {**self._headers, **self._additional_headers, **headers}

        for retry_idx in range(retries):
            response = None
            try:
                response = await self._async_httpx_client.post(
                    url,
                    content=content,
                    files=files,
                    json=json,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )
                if response.status_code != httpx.codes.OK:
                    _Api._raise_for_status_httpx(response)
                return response
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                if (
                    isinstance(exc, httpx.HTTPStatusError)
                    and response.status_code == 400
                    and self._token is None
                ):
                    self.logger.info("API_TOKEN env variable is undefined. ")
                if raise_error:
                    raise exc
                else:
                    await process_requests_exception_async(
                        self.logger,
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
                process_unhandled_request(self.logger, exc)
        raise httpx.RequestError(
            f"Retry limit exceeded ({url})",
            request=getattr(response, "request", None),
        )

    async def stream_async(
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
        :param timeout: Overall timeout for the request.
        :type timeout: float, optional
        :return: Async generator object.
        :rtype: :class:`AsyncGenerator`
        """
        self._set_async_client()

        if retries is None:
            retries = self._retry_count

        url = self._prepare_url(method)
        logger.info(f"{method_type} {url}")

        if headers is None:
            headers = {**self._headers, **self._additional_headers}
        else:
            headers = {**self._headers, **self._additional_headers, **headers}

        params = kwargs.get("params", None)
        if "content" in kwargs or "json_body" in kwargs:
            content = kwargs.get("content", None)
            json_body = kwargs.get("json_body", None)
        else:
            if isinstance(data, (bytes, Generator)):
                content = data
                json_body = None
            elif isinstance(data, Dict):
                json_body = {**data, **self._additional_fields}
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
                    response = self._async_httpx_client.stream(
                        method_type,
                        url,
                        content=content,
                        json=json_body,
                        headers=headers,
                        timeout=timeout,
                        params=params,
                    )
                elif method_type == "GET":
                    response = self._async_httpx_client.stream(
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
                        _Api._raise_for_status_httpx(resp)

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
                    logger.info(f"Streamed size: {total_streamed}, expected size: {expected_size}")
                    return
            except (httpx.RequestError, httpx.HTTPStatusError, RetryableRequestException) as e:
                if (
                    isinstance(e, httpx.HTTPStatusError)
                    and resp.status_code == 400
                    and self._token is None
                ):
                    self.logger.info("API_TOKEN env variable is undefined.")
                retry_range_start = total_streamed + (range_start or 0)
                if total_streamed != 0:
                    retry_range_start += 1
                headers["Range"] = f"bytes={retry_range_start}-{range_end or ''}"
                logger.debug(f"Setting Range header {headers['Range']} for retry")
                await process_requests_exception_async(
                    self.logger,
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
                process_unhandled_request(self.logger, e)
        raise httpx.RequestError(
            message=f"Retry limit exceeded ({url})",
            request=resp.request if locals().get("resp") else None,
        )

    def _set_async_client(self):
        """
        Set async httpx client with HTTP/2 if it is not set yet.
        """
        if self._async_httpx_client is None:
            self._async_httpx_client = httpx.AsyncClient(http2=True)

    def _set_client(self):
        """
        Set sync httpx client with HTTP/2 if it is not set yet.
        """
        if self._httpx_client is None:
            self._httpx_client = httpx.Client(http2=True)

    def get_default_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._initialize_semaphore()
        return self._semaphore

    def _initialize_semaphore(self, semaphore_size: int = 10):
        self._semaphore = asyncio.Semaphore(semaphore_size)
        logger.debug(
            f"Setting global API semaphore size to {semaphore_size} from environment variable"
        )

    def set_semaphore_size(self, size: int = None):
        if size is not None:
            self._semaphore = asyncio.Semaphore(size)
        else:
            self._initialize_semaphore()

    @property
    def semaphore(self) -> asyncio.Semaphore:
        return self._semaphore
