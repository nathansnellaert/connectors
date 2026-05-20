import os
import httpx
import time
from . import debug

_client = None
_client_config = {
    'timeout': int(os.environ.get('HTTP_TIMEOUT', '30')),
    'headers': {'User-Agent': os.environ.get('HTTP_USER_AGENT', 'DataIntegrations/1.0')}
}


def _get_or_create_client() -> httpx.Client:
    global _client

    if _client is None:
        _client = httpx.Client(
            timeout=_client_config['timeout'],
            headers=_client_config['headers'],
            follow_redirects=True
        )

    return _client


def _logged_request(method: str, url: str, **kwargs) -> httpx.Response:
    """Execute HTTP request with logging if ENABLE_LOGGING is set."""
    client = _get_or_create_client()
    start = time.time()
    error = None
    status = None

    try:
        response = client.request(method, url, **kwargs)
        status = response.status_code
        return response
    except Exception as e:
        error = str(e)
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        debug.log_http_request(method, url, status, duration_ms=duration_ms, error=error)


def get(url: str, **kwargs) -> httpx.Response:
    return _logged_request("GET", url, **kwargs)


def post(url: str, **kwargs) -> httpx.Response:
    return _logged_request("POST", url, **kwargs)


def put(url: str, **kwargs) -> httpx.Response:
    return _logged_request("PUT", url, **kwargs)


def delete(url: str, **kwargs) -> httpx.Response:
    return _logged_request("DELETE", url, **kwargs)


def get_client() -> httpx.Client:
    return _get_or_create_client()


def configure_http(**config):
    global _client_config, _client
    _client_config.update(config)
    if _client:
        _client.close()
        _client = None
