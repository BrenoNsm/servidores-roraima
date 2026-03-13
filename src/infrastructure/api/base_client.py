"""
Cliente HTTP base com tratamento de erros, retry e logging.
Segue o padrão Repository para isolar dependências externas.
"""
import logging
import time
from typing import Optional, Dict, Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import settings

logger = logging.getLogger(__name__)


class APIClientError(Exception):
    """Exceção base para erros de API."""
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[Dict] = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)


class BaseAPIClient:
    """
    Cliente HTTP reutilizável com:
    - Retry exponencial
    - Timeout configurável
    - Logging estruturado
    - Tratamento de erros padronizado
    """
    
    def __init__(self, base_url: str, timeout: int, api_name: str):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.api_name = api_name
        
        # Configura sessão com retry
        self.session = requests.Session()
        retry_strategy = Retry(
            total=settings.api.RETRY_ATTEMPTS,
            backoff_factor=settings.api.RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Executa requisição HTTP com tratamento padronizado.
        
        Raises:
            APIClientError: Em caso de falha na requisição.
        """
        url = urljoin(self.base_url, endpoint)
        
        logger.debug(f"[{self.api_name}] {method} {url} params={params}")
        
        try:
            # Rate limiting simples
            time.sleep(1 / settings.api.REQUESTS_PER_SECOND)
            
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                headers=headers or {},
                timeout=self.timeout,
            )
            response.raise_for_status()
            
            logger.debug(f"[{self.api_name}] Resposta: {response.status_code}")
            return response.json()
            
        except requests.exceptions.Timeout:
            msg = f"Timeout após {self.timeout}s em {endpoint}"
            logger.error(f"[{self.api_name}] {msg}")
            raise APIClientError(msg, status_code=None)
            
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else None
            msg = f"HTTP {status} em {endpoint}: {str(e)}"
            logger.error(f"[{self.api_name}] {msg}")
            raise APIClientError(msg, status_code=status)
            
        except requests.exceptions.RequestException as e:
            msg = f"Erro de conexão em {endpoint}: {str(e)}"
            logger.error(f"[{self.api_name}] {msg}")
            raise APIClientError(msg)
            
        except ValueError as e:
            msg = f"Erro ao parsear JSON da resposta: {str(e)}"
            logger.error(f"[{self.api_name}] {msg}")
            raise APIClientError(msg)
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Wrapper para requisições GET."""
        return self._make_request("GET", endpoint, params)
    
    def close(self) -> None:
        """Fecha a sessão HTTP (importante para cleanup)."""
        self.session.close()