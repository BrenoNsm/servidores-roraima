"""
Serviço responsável por persistir os dados coletados.
Suporta múltiplos formatos (JSON, CSV) com validação e organização.
"""
import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

from src.domain.models.servidor import (
    ServidorBasico, 
    ServidorCompleto, 
    RemuneracaoServidor
)
from src.config.settings import settings

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Exceção para erros de persistência."""
    pass


class StorageService:
    """
    Serviço de persistência com suporte a múltiplos formatos.
    
    Responsabilidades:
    - Criar estrutura de diretórios organizada
    - Validar dados antes de salvar
    - Serializar entidades de domínio para formatos de saída
    - Garantir atomicidade nas operações de escrita
    """
    
    def __init__(
        self,
        output_dir: str,
        dry_run: bool = False,
        file_prefix: str = "roraima_servidores",
    ):
        """
        Inicializa o serviço de storage.
        
        Args:
            output_dir: Diretório raiz para saída dos arquivos
            dry_run: Se True, apenas loga sem escrever em disco
            file_prefix: Prefixo para nomes dos arquivos gerados
        """
        self.output_dir = Path(output_dir)
        self.dry_run = dry_run
        self.file_prefix = file_prefix
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Estrutura de diretórios organizada
        self.paths = {
            "json_detalhado": self.output_dir / "json" / "detalhado",
            "json_resumo": self.output_dir / "json" / "resumo",
            "csv": self.output_dir / "csv",
            "metadata": self.output_dir / "metadata",
        }
        
        # Contadores para métricas
        self.stats = {
            "arquivos_criados": 0,
            "registros_salvos": 0,
            "bytes_escritos": 0,
            "erros": 0,
        }
        
        if not self.dry_run:
            self._create_directories()
            logger.info(f"StorageService inicializado | Output: {self.output_dir}")
    
    def _create_directories(self) -> None:
        """Cria a estrutura de diretórios necessária."""
        for path in self.paths.values():
            path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Diretórios criados: {list(self.paths.keys())}")
    
    def save_json(
        self, 
        servidor: ServidorCompleto, 
        modo: str = "detalhado"
    ) -> Optional[Path]:
        """
        Salva um servidor em formato JSON.
        
        Args:
            servidor: Objeto ServidorCompleto a ser salvo
            modo: "detalhado" (todos os dados) ou "resumo" (apenas campos essenciais)
            
        Returns:
            Path do arquivo criado ou None se dry_run
        """
        try:
            if modo == "detalhado":
                data = self._serialize_servidor_completo(servidor)
                output_path = self.paths["json_detalhado"]
            elif modo == "resumo":
                data = self._serialize_resumo(servidor)
                output_path = self.paths["json_resumo"]
            else:
                raise ValueError(f"Modo desconhecido: {modo}")
            
            # Nome do arquivo: prefixo_matricula_timestamp.json
            filename = f"{self.file_prefix}_{servidor.dados_basicos.matricula}_{self.timestamp}.json"
            filepath = output_path / filename
            
            if self.dry_run:
                logger.debug(f"[DRY RUN] Salvaria JSON em: {filepath}")
                return None
            
            # Escrita atômica: escreve em temp e depois renomeia
            temp_path = filepath.with_suffix(".tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                bytes_written = temp_path.stat().st_size
            
            temp_path.rename(filepath)  # Atomic move
            
            self.stats["arquivos_criados"] += 1
            self.stats["registros_salvos"] += 1
            self.stats["bytes_escritos"] += bytes_written
            
            logger.debug(f"✓ JSON salvo: {filepath.name} ({bytes_written} bytes)")
            return filepath
            
        except Exception as e:
            self.stats["erros"] += 1
            logger.error(f"Erro ao salvar JSON de {servidor.dados_basicos.matricula}: {e}")
            return None
    
    def save_csv(
        self,
        servidor: ServidorCompleto,
        arquivo_resumo: Optional[str] = None,
        arquivo_remuneracoes: Optional[str] = None,
    ) -> tuple[Optional[Path], Optional[Path]]:
        """
        Salva dados do servidor em formato CSV.
        
        Cria dois arquivos:
        - Resumo: um registro por servidor com dados básicos
        - Remunerações: um registro por mês/ano de remuneração
        
        Args:
            servidor: Objeto ServidorCompleto a ser salvo
            arquivo_resumo: Nome opcional para o arquivo de resumo
            arquivo_remuneracoes: Nome opcional para o arquivo de remunerações
            
        Returns:
            Tupla com paths dos arquivos criados (ou None se dry_run)
        """
        csv_dir = self.paths["csv"]
        
        # Define nomes padrão dos arquivos
        resumo_file = arquivo_resumo or f"{self.file_prefix}_resumo_{self.timestamp}.csv"
        rem_file = arquivo_remuneracoes or f"{self.file_prefix}_remuneracoes_{self.timestamp}.csv"
        
        resumo_path = csv_dir / resumo_file
        rem_path = csv_dir / rem_file
        
        if self.dry_run:
            logger.debug(f"[DRY RUN] Salvaria CSVs: {resumo_file}, {rem_file}")
            return None, None
        
        try:
            # Salva resumo do servidor
            self._append_csv_row(
                filepath=resumo_path,
                fieldnames=self._get_resumo_fieldnames(),
                rowdata=self._serialize_resumo_csv(servidor),
                is_new=not resumo_path.exists(),
            )
            
            # Salva cada remuneração como linha separada
            for remuneracao in servidor.remuneracoes:
                self._append_csv_row(
                    filepath=rem_path,
                    fieldnames=self._get_remuneracao_fieldnames(),
                    rowdata=self._serialize_remuneracao_csv(servidor, remuneracao),
                    is_new=not rem_path.exists(),
                )
            
            logger.debug(f"✓ CSVs atualizados para matrícula {servidor.dados_basicos.matricula}")
            return resumo_path, rem_path
            
        except Exception as e:
            self.stats["erros"] += 1
            logger.error(f"Erro ao salvar CSV de {servidor.dados_basicos.matricula}: {e}")
            return None, None
    
    def save_batch_json(
        self,
        servidores: List[ServidorCompleto],
        filename: Optional[str] = None,
    ) -> Optional[Path]:
        """
        Salva múltiplos servidores em um único arquivo JSON.
        Útil para exportação em lote ou backup.
        
        Args:
            servidores: Lista de ServidorCompleto
            filename: Nome opcional do arquivo
            
        Returns:
            Path do arquivo criado ou None
        """
        if not servidores:
            logger.warning("Nenhum servidor para salvar em batch")
            return None
        
        filename = filename or f"{self.file_prefix}_batch_{self.timestamp}.json"
        filepath = self.paths["json_detalhado"] / filename
        
        if self.dry_run:
            logger.debug(f"[DRY RUN] Salvaria batch JSON: {filename} com {len(servidores)} registros")
            return None
        
        try:
            data = {
                "metadata": {
                    "gerado_em": datetime.now().isoformat(),
                    "total_registros": len(servidores),
                    "periodo": {
                        "inicio": min(s.dados_basicos.data_admissao for s in servidores).isoformat(),
                        "fim": max(s.dados_basicos.data_admissao for s in servidores).isoformat(),
                    }
                },
                "servidores": [
                    self._serialize_servidor_completo(s) for s in servidores
                ]
            }
            
            temp_path = filepath.with_suffix(".tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                bytes_written = temp_path.stat().st_size
            
            temp_path.rename(filepath)
            
            self.stats["arquivos_criados"] += 1
            self.stats["registros_salvos"] += len(servidores)
            self.stats["bytes_escritos"] += bytes_written
            
            logger.info(f"✓ Batch JSON salvo: {filename} ({len(servidores)} registros, {bytes_written/1024:.1f} KB)")
            return filepath
            
        except Exception as e:
            self.stats["erros"] += 1
            logger.error(f"Erro ao salvar batch JSON: {e}")
            return None
    
    def _append_csv_row(
        self,
        filepath: Path,
        fieldnames: List[str],
        rowdata: dict,
        is_new: bool,
    ) -> None:
        """
        Adiciona uma linha ao CSV, criando o arquivo com header se necessário.
        
        Args:
            filepath: Caminho do arquivo CSV
            fieldnames: Lista de colunas
            rowdata: Dicionário com dados da linha
            is_new: Se True, escreve o header primeiro
        """
        with open(filepath, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if is_new:
                writer.writeheader()
            writer.writerow(rowdata)
    
    # ========================================================================
    # MÉTODOS DE SERIALIZAÇÃO (Transformam domínio → dados para persistência)
    # ========================================================================
    
    def _serialize_servidor_completo(self, servidor: ServidorCompleto) -> dict:
        """Serializa ServidorCompleto para JSON detalhado."""
        return {
            "metadata": {
                "exportado_em": datetime.now().isoformat(),
                "versao_schema": "1.0",
            },
            "dados_basicos": servidor.dados_basicos.to_dict(),
            "remuneracoes": [rem.to_dict() for rem in servidor.remuneracoes],
            "agregados": {
                "media_remuneracao_liquida": servidor.calcular_media_liquida(),
                "total_remuneracoes": len(servidor.remuneracoes),
                "esta_ativo": servidor.dados_basicos.esta_ativo,
            }
        }
    
    def _serialize_resumo(self, servidor: ServidorCompleto) -> dict:
        """Cria versão resumida para consultas rápidas."""
        return {
            "matricula": servidor.dados_basicos.matricula,
            "nome": servidor.dados_basicos.nome,
            "orgao": servidor.dados_basicos.orgao,
            "cargo": servidor.dados_basicos.cargo,
            "data_admissao": servidor.dados_basicos.data_admissao.isoformat(),
            "esta_ativo": servidor.dados_basicos.esta_ativo,
            "ultima_remuneracao_liquida": (
                servidor.remuneracoes[-1].remuneracao_liquida 
                if servidor.remuneracoes else None
            ),
            "total_remuneracoes_registradas": len(servidor.remuneracoes),
        }
    
    def _get_resumo_fieldnames(self) -> List[str]:
        """Retorna colunas para CSV de resumo."""
        return [
            "matricula", "nome", "orgao", "cargo", "lotacao",
            "data_admissao", "data_desligamento", "carga_horaria",
            "esta_ativo", "ultima_remuneracao_liquida", "total_remuneracoes_registradas"
        ]
    
    def _serialize_resumo_csv(self, servidor: ServidorCompleto) -> dict:
        """Prepara dados do servidor para linha CSV de resumo."""
        return {
            "matricula": servidor.dados_basicos.matricula,
            "nome": servidor.dados_basicos.nome,
            "orgao": servidor.dados_basicos.orgao,
            "cargo": servidor.dados_basicos.cargo,
            "lotacao": servidor.dados_basicos.lotacao,
            "data_admissao": servidor.dados_basicos.data_admissao.isoformat(),
            "data_desligamento": (
                servidor.dados_basicos.data_desligamento.isoformat()
                if servidor.dados_basicos.data_desligamento else ""
            ),
            "carga_horaria": servidor.dados_basicos.carga_horaria,
            "esta_ativo": servidor.dados_basicos.esta_ativo,
            "ultima_remuneracao_liquida": (
                servidor.remuneracoes[-1].remuneracao_liquida
                if servidor.remuneracoes else ""
            ),
            "total_remuneracoes_registradas": len(servidor.remuneracoes),
        }
    
    def _get_remuneracao_fieldnames(self) -> List[str]:
        """Retorna colunas para CSV de remunerações."""
        return [
            "matricula", "nome", "periodo", "orgao", "cargo", 
            "tipo_cargo_categoria", "tipo_cargo_original", "nivel",
            "remuneracao_bruta", "remuneracao_liquida", "vencimento_base",
            "total_proventos", "total_deducoes", "deducoes_obrigatorias",
            "proventos_diversos"
    ]
    
    def _serialize_remuneracao_csv(
        self, 
        servidor: ServidorCompleto, 
        remuneracao: RemuneracaoServidor
    ) -> dict:
        """Prepara dados de remuneração para linha CSV."""
        return {
            "matricula": servidor.dados_basicos.matricula,
            "nome": remuneracao.nome,
            "periodo": f"{remuneracao.mes:02d}/{remuneracao.ano}",
            "orgao": remuneracao.orgao_principal,
            "cargo": remuneracao.cargo_principal,
            # ✅ CORREÇÃO: usa .categoria ao invés de .value
            "tipo_cargo_categoria": (
                remuneracao.tipo_cargo_principal.categoria
                if remuneracao.tipo_cargo_principal else ""
            ),
            "tipo_cargo_original": (
                remuneracao.tipo_cargo_principal.valor_original
                if remuneracao.tipo_cargo_principal else ""
            ),
            "nivel": remuneracao.nivel_principal,
            "remuneracao_bruta": remuneracao.remuneracao_bruta,
            "remuneracao_liquida": remuneracao.remuneracao_liquida,
            "vencimento_base": remuneracao.vencimento_base,
            "total_proventos": remuneracao.total_proventos,
            "total_deducoes": remuneracao.total_deducoes,
            "deducoes_obrigatorias": remuneracao.deducoes_obrigatorias,
            "proventos_diversos": remuneracao.proventos_diversos,
    }
    
    # ========================================================================
    # UTILITÁRIOS E MÉTRICAS
    # ========================================================================
    
    def get_stats(self) -> dict:
        """Retorna estatísticas de persistência."""
        return {
            **self.stats,
            "output_dir": str(self.output_dir),
            "dry_run": self.dry_run,
        }
    
    def cleanup_temp_files(self) -> int:
        """Remove arquivos temporários (.tmp) deixados por falhas."""
        removed = 0
        for root, _, files in (self.output_dir).rglob("*"):
            for file in files:
                if file.suffix == ".tmp":
                    try:
                        file.unlink()
                        removed += 1
                        logger.debug(f"Removido temp: {file}")
                    except Exception as e:
                        logger.warning(f"Não foi possível remover {file}: {e}")
        if removed:
            logger.info(f"Cleanup: {removed} arquivos temporários removidos")
        return removed
    
    def __repr__(self) -> str:
        return f"StorageService(output_dir={self.output_dir}, dry_run={self.dry_run})"