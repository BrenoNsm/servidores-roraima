"""
Cliente específico para a API de busca de servidores do estado.
"""
import logging
from datetime import datetime
from typing import List, Optional

from src.domain.models.servidor import ServidorBasico
from src.infrastructure.api.base_client import BaseAPIClient, APIClientError
from src.config.settings import settings

logger = logging.getLogger(__name__)


class ServidoresAPIClient(BaseAPIClient):
    """Cliente para a API de lista de servidores de Roraima."""
    
    def __init__(self):
        super().__init__(
            base_url=settings.api.SERVIDORES_BASE_URL,
            timeout=settings.api.SERVIDORES_TIMEOUT,
            api_name="ServidoresAPI"
        )
    
    def buscar_servidores(
        self,
        data_inicio: str,
        data_fim: str,
        orgao: Optional[str] = None,
    ) -> List[ServidorBasico]:
        """
        Busca servidores no período especificado.
        
        Args:
            data_inicio: Data inicial no formato YYYY-MM-DD
            data_fim: Data final no formato YYYY-MM-DD
            orgao: Filtro opcional por órgão (ex: "SEED")
            
        Returns:
            Lista de objetos ServidorBasico
            
        Raises:
            APIClientError: Em caso de falha na API
        """
        params = {
            "de": data_inicio,
            "ate": data_fim,
        }
        if orgao:
            params["orgao"] = orgao
            
        try:
            response = self.get(settings.api.SERVIDORES_ENDPOINT, params=params)
            
            # Validação básica da estrutura da resposta
            if not isinstance(response, dict) or "listaServ" not in response:
                raise APIClientError(
                    f"Resposta inesperada da API: {type(response)}",
                    response=response
                )
            
            servidores_raw = response["listaServ"]
            logger.info(f"API retornou {len(servidores_raw)} servidores")
            
            return [self._parse_servidor(s) for s in servidores_raw if s]
            
        except APIClientError:
            # Re-lança erros já tratados
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao processar resposta: {e}")
            raise APIClientError(f"Erro interno: {str(e)}")
    
    def _parse_servidor(self, data: dict) -> ServidorBasico:
        """
        Transforma dados brutos da API em entidade de domínio.
        Método privado para manter encapsulamento.
        """
        def parse_date(date_str: Optional[str]) -> Optional[datetime]:
            if not date_str:
                return None
            # Remove timezone info se presente para simplificar
            if "T" in date_str:
                date_str = date_str.split("T")[0]
            return datetime.strptime(date_str, "%Y-%m-%d")
        
        return ServidorBasico(
            matricula=str(data.get("Matricula", "")).strip(),
            nome=str(data.get("Nome", "")).strip().upper(),
            orgao=str(data.get("Orgao", "")).strip(),
            cargo=str(data.get("Cargo", "")).strip(),
            lotacao=str(data.get("NmLotacao", "")).strip(),
            data_admissao=parse_date(data.get("dtadmissao")) or datetime.min,
            data_desligamento=parse_date(data.get("dtdesligamento")),
            carga_horaria=int(data.get("CargaHoraria", 0) or 0),
        )