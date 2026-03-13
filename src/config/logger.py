"""
Configuração centralizada de logging para todo o projeto.
"""
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler


def setup_logging(
    level: int = logging.INFO,
    log_dir: str = "data/logs",
    log_file: str = "pipeline.log",
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
) -> None:
    """
    Configura handlers de console e arquivo com rotação.
    
    Args:
        level: Nível mínimo de log
        log_dir: Diretório para arquivos de log
        log_file: Nome do arquivo de log
        max_bytes: Tamanho máximo antes de rotacionar
        backup_count: Quantidade de backups a manter
    """
    # Cria diretório de logs se não existir
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Formatter padrão
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Limpa handlers existentes (evita duplicação em reloads)
    root_logger.handlers.clear()
    
    # Handler de console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)
    root_logger.addHandler(console_handler)
    
    # Handler de arquivo com rotação
    file_handler = RotatingFileHandler(
        Path(log_dir) / log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
        delay=True  # Só cria arquivo se houver logs
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)  # Sempre loga tudo no arquivo
    root_logger.addHandler(file_handler)
    
    # Silencia logs muito verbosos de bibliotecas externas
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    
    logging.info(f"Logging configurado: nível={logging.getLevelName(level)}, arquivo={log_dir}/{log_file}")