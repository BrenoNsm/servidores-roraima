"""
Serviço que orquestra a coleta de dados das duas APIs.
Responsável pelo fluxo: Lista → Detalhamento → Consolidação → Classificação de Gênero.
"""
import logging
from datetime import datetime
from typing import List, Optional, Iterator
from dataclasses import replace

from src.domain.models.servidor import ServidorBasico, ServidorCompleto, RemuneracaoServidor
from src.infrastructure.api.servidores_api import ServidoresAPIClient
from src.infrastructure.api.detalhamento_api import DetalhamentoAPIClient
from src.application.services.gender_classifier import GenderClassifier
from src.config.settings import settings

logger = logging.getLogger(__name__)


class CollectorService:
    """
    Serviço de alto nível para coleta de dados de servidores.
    
    Este serviço coordena:
    1. Busca da lista de servidores por período
    2. Enriquecimento com dados de remuneração
    3. Classificação de gênero pelo nome
    4. Tratamento de erros e retry por item
    """
    
    def __init__(
        self,
        servidores_client: Optional[ServidoresAPIClient] = None,
        detalhamento_client: Optional[DetalhamentoAPIClient] = None,
        gender_classifier: Optional[GenderClassifier] = None,
    ):
        """
        Inicializa o serviço de coleta.
        
        Args:
            servidores_client: Cliente para API de lista de servidores
            detalhamento_client: Cliente para API de detalhamento
            gender_classifier: Classificador de gênero (opcional)
        """
        self.servidores_client = servidores_client or ServidoresAPIClient()
        self.detalhamento_client = detalhamento_client or DetalhamentoAPIClient()
        self.gender_classifier = gender_classifier
        
        # Métricas de execução
        self.stats = {
            "servidores_encontrados": 0,
            "detalhes_coletados": 0,
            "generos_classificados": 0,
            "erros_api": 0,
            "erros_parse": 0,
        }
    
    def coletar_periodo(
        self,
        data_inicio: str,
        data_fim: str,
        orgao: Optional[str] = None,
        coletar_detalhes: bool = True,
        classificar_genero: bool = True,
    ) -> Iterator[ServidorCompleto]:
        """
        Generator que produz servidores completos para um período.
        
        Usar generator permite processamento em streaming,
        economizando memória para grandes volumes.
        
        Args:
            data_inicio: Data inicial no formato YYYY-MM-DD
            data_fim: Data final no formato YYYY-MM-DD
            orgao: Filtro opcional por órgão (ex: "SEED")
            coletar_detalhes: Se True, busca remunerações detalhadas
            classificar_genero: Se True, classifica gênero pelo nome
            
        Yields:
            ServidorCompleto: Servidor com dados básicos + remunerações + gênero
        """
        logger.info(f"Iniciando coleta: {data_inicio} até {data_fim} | órgão: {orgao or 'TODOS'}")
        
        # Passo 1: Buscar lista básica de servidores
        servidores_basicos = self.servidores_client.buscar_servidores(
            data_inicio=data_inicio,
            data_fim=data_fim,
            orgao=orgao,
        )
        self.stats["servidores_encontrados"] = len(servidores_basicos)
        logger.info(f"Encontrados {len(servidores_basicos)} servidores na lista")
        
        # Passo 2: Para cada servidor, enriquecer com detalhes e gênero
        for servidor_basico in servidores_basicos:
            try:
                # Cria servidor completo com dados básicos
                servidor_completo = ServidorCompleto(dados_basicos=servidor_basico)
                
                # Passo 2a: Coletar remunerações detalhadas
                if coletar_detalhes:
                    remuneracoes = self._coletar_remuneracoes(
                        matricula=servidor_basico.matricula,
                        data_admissao=servidor_basico.data_admissao,
                        data_desligamento=servidor_basico.data_desligamento,
                    )
                    for rem in remuneracoes:
                        servidor_completo.adicionar_remuneracao(rem)
                    self.stats["detalhes_coletados"] += len(remuneracoes)
                
                # Passo 2b: Classificar gênero pelo nome
                if classificar_genero and self.gender_classifier:
                    servidor_completo = self._classificar_genero(servidor_completo)
                
                yield servidor_completo
                
            except Exception as e:
                self.stats["erros_api"] += 1
                logger.error(
                    f"Falha ao processar servidor {servidor_basico.matricula}: {e}",
                    exc_info=settings.pipeline.VERBOSE
                )
                # Continua para o próximo (fail-fast por item, não por lote)
                continue
    
    def _coletar_remuneracoes(
        self,
        matricula: str,
        data_admissao: datetime,
        data_desligamento: Optional[datetime] = None,
    ) -> List[RemuneracaoServidor]:
        """
        Coleta histórico de remunerações para uma matrícula.
        
        ESTRATÉGIA:
        - Busca desde a data de admissão até a data de desligamento (ou hoje)
        - Itera mês a mês no período de vínculo
        - Log detalhado para debug
        
        Args:
            matricula: Número da matrícula do servidor
            data_admissao: Data de admissão do servidor
            data_desligamento: Data de desligamento (None se ainda ativo)
            
        Returns:
            Lista de RemuneracaoServidor coletadas
        """
        remuneracoes = []
        
        # Define janela de busca baseada no vínculo do servidor
        data_fim_busca = data_desligamento if data_desligamento else datetime.now()
        
        # Limita a data fim ao mês atual (não busca dados futuros)
        hoje = datetime.now()
        if data_fim_busca > hoje:
            data_fim_busca = hoje
        
        logger.debug(f"Buscando remunerações para {matricula}: {data_admissao.date()} até {data_fim_busca.date()}")
        
        # Itera mês a mês no período de vínculo
        mes_atual = data_admissao.month
        ano_atual = data_admissao.year
        
        iteracoes = 0
        max_iteracoes = 60  # Máximo 5 anos de histórico
        
        while True:
            # Interrompe se passou da data fim
            data_referencia = datetime(ano_atual, mes_atual, 1)
            if data_referencia > data_fim_busca:
                break
            
            # Safety break para evitar loop infinito
            iteracoes += 1
            if iteracoes > max_iteracoes:
                logger.warning(f"Limite de {max_iteracoes} iterações atingido para {matricula}")
                break
            
            try:
                # Busca remunerações para o mês/ano atual
                # ⚠️ A API de detalhamento já formata a matrícula internamente
                remuneracoes_mes = self.detalhamento_client.buscar_detalhes_com_paginacao(
                    matricula=matricula,
                    mes=mes_atual,
                    ano=ano_atual,
                )
                
                if remuneracoes_mes:
                    remuneracoes.extend(remuneracoes_mes)
                    logger.debug(f"  ✓ {len(remuneracoes_mes)} registro(s) para {mes_atual:02d}/{ano_atual}")
                else:
                    logger.debug(f"  - Sem dados para {mes_atual:02d}/{ano_atual}")
                
            except Exception as e:
                logger.warning(
                    f"Não foi possível coletar remuneração de {matricula} "
                    f"para {mes_atual:02d}/{ano_atual}: {e}"
                )
                # Continua para o próximo mês (fail-soft)
            
            # Avança para próximo mês
            mes_atual += 1
            if mes_atual > 12:
                mes_atual = 1
                ano_atual += 1
        
        if remuneracoes:
            logger.info(f"✓ Coletadas {len(remuneracoes)} remunerações para {matricula}")
        return remuneracoes
    
    def _classificar_genero(self, servidor_completo: ServidorCompleto) -> ServidorCompleto:
        """
        Classifica o gênero do servidor baseado no primeiro nome.
        
        Args:
            servidor_completo: Servidor com dados básicos
            
        Returns:
            ServidorCompleto com campos de gênero preenchidos
        """
        try:
            nome_completo = servidor_completo.dados_basicos.nome
            genero_info = self.gender_classifier.classificar(nome_completo)
            
            if genero_info:
                # Atualiza dados básicos com gênero (cria novo objeto imutável)
                novos_dados_basicos = replace(
                    servidor_completo.dados_basicos,
                    genero=genero_info["genero"],
                    genero_confianca=genero_info["confianca"]
                )
                servidor_completo.dados_basicos = novos_dados_basicos
                self.stats["generos_classificados"] += 1
                
                logger.debug(
                    f"Gênero classificado: {nome_completo} → {genero_info['genero']} "
                    f"(confiança: {genero_info['confianca']})"
                )
            else:
                logger.debug(f"Gênero não classificado para: {nome_completo}")
                
        except Exception as e:
            logger.warning(f"Erro ao classificar gênero de {servidor_completo.dados_basicos.nome}: {e}")
        
        return servidor_completo
    
    def get_stats(self) -> dict:
        """Retorna estatísticas da última execução."""
        return self.stats.copy()
    
    def close(self) -> None:
        """Libera recursos dos clientes HTTP."""
        self.servidores_client.close()
        self.detalhamento_client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # Não suprime exceções