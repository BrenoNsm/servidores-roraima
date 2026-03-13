"""
Módulo de configurações do pipeline.
Centraliza todas as variáveis de ambiente e constantes do sistema.
"""
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class APIConfig:
    """Configurações das APIs do governo de Roraima."""
    
    # API de Lista de Servidores
    SERVIDORES_BASE_URL: str = "https://ws-api.host-server.dev.br"
    SERVIDORES_ENDPOINT: str = "/busca_dados_serv"
    SERVIDORES_TIMEOUT: int = 30  # segundos
    
    # API de Detalhamento
    DETALHAMENTO_BASE_URL: str = "https://api.transparencia.rr.gov.br"
    DETALHAMENTO_ENDPOINT: str = "/api/v1/portal/transparencia/pesquisar-remuneracoes"
    DETALHAMENTO_TIMEOUT: int = 30
    
    # Rate limiting (respeitar limites das APIs)
    REQUESTS_PER_SECOND: float = 1.0
    RETRY_ATTEMPTS: int = 3
    RETRY_BACKOFF: float = 2.0  # fator exponencial


@dataclass(frozen=True)
class PipelineConfig:
    """Configurações do pipeline de coleta."""
    
    # Janela padrão de coleta (pode ser sobrescrita por parâmetro)
    DEFAULT_START_DATE: str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    DEFAULT_END_DATE: str = datetime.now().strftime("%Y-%m-%d")
    
    # Controle de paginação e batch
    BATCH_SIZE: int = 50  # Processar em lotes para não sobrecarregar
    MAX_WORKERS: int = 5  # Threads/Processos para paralelismo
    
    # Diretórios de saída
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "data/output")
    LOG_DIR: str = os.getenv("LOG_DIR", "data/logs")
    
    # Flags de execução
    DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"
    VERBOSE: bool = os.getenv("VERBOSE", "false").lower() == "true"


@dataclass(frozen=True)
class Config:
    """Configuração principal do sistema."""
    api: APIConfig = APIConfig()
    pipeline: PipelineConfig = PipelineConfig()
    
    @classmethod
    def load(cls) -> "Config":
        """Factory method para carregar configurações com validações."""
        # Aqui poderíamos adicionar validações adicionais
        return cls()


# Instância global para acesso fácil (em projetos pequenos)
settings = Config.load()