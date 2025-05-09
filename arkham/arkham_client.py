import requests
import json
from .config import BASE_API_URL, DEFAULT_REQUEST_TIMEOUT, ArkhamAPIError, get_logger

logger = get_logger(__name__)

class ArkhamClient:
    """Handles communication with the Arkham Intelligence API."""

    def __init__(self, api_key: str, base_url: str = BASE_API_URL):
        if not api_key or api_key == 'YOUR_API_KEY_HERE':
            logger.error("API ключ Arkham не предоставлен или некорректен.")
            raise ValueError("API ключ не найден или некорректен")
        
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.headers = {"API-Key": self.api_key}

    def _request(self, endpoint: str, params: dict | None = None):
        """Makes a request to the specified API endpoint."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        request_params = params or {}

        logger.debug(f"Запрос к Arkham API: URL={url}, Params={request_params}")

        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=request_params,
                timeout=DEFAULT_REQUEST_TIMEOUT
            )
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

            logger.debug(f"Arkham API ответил статусом {response.status_code}")
            
            try:
                return response.json()
            except json.JSONDecodeError as json_err:
                logger.error(f"Ошибка декодирования JSON от Arkham API: {json_err}. Ответ: {response.text[:500]}")
                raise ArkhamAPIError(message=f"JSON Decode Error: {json_err}", status_code=response.status_code if response else None) from json_err

        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code
            response_text = http_err.response.text[:500] if http_err.response else "<no response text>"
            logger.error(f"HTTP ошибка {status_code} при запросе к {url}: {response_text}")
            
            if status_code == 401:
                raise ArkhamAPIError(message="Ошибка авторизации (401 Unauthorized). Проверьте API ключ.", status_code=status_code) from http_err
            elif status_code == 403:
                 # Basic rate limit check
                if 'throttled' in response_text.lower() or 'rate limit' in response_text.lower():
                    raise ArkhamAPIError(message="Превышен лимит запросов Arkham API (403 Forbidden/Throttled).", status_code=status_code) from http_err
                raise ArkhamAPIError(message=f"Доступ запрещен (403 Forbidden). Ответ: {response_text}", status_code=status_code) from http_err
            else:
                 # General HTTP error
                raise ArkhamAPIError(message=f"HTTP Error: {status_code}. Ответ: {response_text}", status_code=status_code) from http_err

        except requests.exceptions.RequestException as req_err:
            # Network errors, timeouts, etc.
            logger.error(f"Ошибка соединения с Arkham API ({url}): {req_err}")
            raise ArkhamAPIError(message=f"Ошибка соединения: {req_err}", status_code=None) from req_err

    def get_transfers(self, params: dict | None = None):
        """Fetches transfers from the Arkham API.
        
        Args:
            params: Dictionary of query parameters for the /transfers endpoint.
            
        Returns:
            Dictionary containing the API response.
            
        Raises:
            ArkhamAPIError: If an API or network error occurs.
        """
        return self._request('transfers', params=params) 