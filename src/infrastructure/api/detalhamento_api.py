"""
Cliente para a API de detalhamento de remunerações.
CORREÇÃO: Remove parâmetro 'page' e formata matrícula corretamente.
"""
import logging
import re
from typing import List, Optional, Dict

from src.domain.models.servidor import (
    RemuneracaoServidor, 
    LancamentoRemuneracao, 
    MatriculaDetalhe,
    TipoCargo
)
from src.infrastructure.api.base_client import BaseAPIClient, APIClientError
from src.config.settings import settings

logger = logging.getLogger(__name__)


class DetalhamentoAPIClient(BaseAPIClient):
    """Cliente para a API de detalhamento de remunerações."""
    
    def __init__(self):
        super().__init__(
            base_url=settings.api.DETALHAMENTO_BASE_URL,
            timeout=settings.api.DETALHAMENTO_TIMEOUT,
            api_name="DetalhamentoAPI"
        )
    
    @staticmethod
    def formatar_matricula(matricula: str) -> str:
        """Formata matrícula para o padrão da API de detalhamento."""
        return re.sub(r'[^0-9]', '', matricula)
    
    def buscar_detalhes(
        self,
        matricula: str,
        mes: int,
        ano: int,
    ) -> Optional[RemuneracaoServidor]:
        """Busca detalhes de remuneração para uma matrícula específica."""
        matricula_api = self.formatar_matricula(matricula)
        
        # ⚠️ SEM 'page' nos parâmetros!
        params = {
            "matricula": matricula_api,
            "mes": mes,
            "ano": ano,
        }
        
        try:
            response = self.get(settings.api.DETALHAMENTO_ENDPOINT, params=params)
            
            if response.get("status") != "Success":
                logger.debug(f"API retornou '{response.get('status')}' para {matricula_api} {mes}/{ano}")
                return None
            
            data = response.get("data", {})
            content = data.get("content", [])
            
            if not content:
                logger.debug(f"Nenhum dado encontrado para {matricula_api} em {mes}/{ano}")
                return None
            
            metadata = {
                "status": response.get("status"),
                "message": response.get("message"),
                "totalPages": data.get("totalPages", 1),
                "number": data.get("number", 0),
                "totalElements": data.get("totalElements", 0),
            }
            
            return self._parse_remuneracao(content[0], response_metadata=metadata)
            
        except APIClientError as e:
            if e.status_code == 404:
                logger.debug(f"Matrícula {matricula_api} não encontrada em {mes}/{ano}")
                return None
            logger.warning(f"Erro ao buscar {matricula_api} {mes}/{ano}: {e.message}")
            return None
    
    def buscar_detalhes_com_paginacao(
        self,
        matricula: str,
        mes: int,
        ano: int,
    ) -> List[RemuneracaoServidor]:
        """Busca detalhes - versão que retorna lista."""
        result = self.buscar_detalhes(matricula, mes, ano)
        return [result] if result else []
    
    def _parse_remuneracao(
        self, 
        data: Dict, 
        response_metadata: Optional[Dict] = None
    ) -> RemuneracaoServidor:
        """Transforma dados brutos em entidade RemuneracaoServidor."""
        
        def clean(value: str) -> str:
            return str(value).strip(" •\t\n\r") if value else ""
        
        # Parse dos lançamentos
        lancamentos = []
        lancamentos_raw = []
        
        for l in data.get("lancamentos", []):
            lancamentos.append(LancamentoRemuneracao(
                tipo_evento=str(l.get("tipoEvento", "")).strip(),
                evento=str(l.get("evento", "")).strip(),
                quantidade=float(l.get("quantidade", 0) or 0),
                valor=float(l.get("valor", 0) or 0),
                numero_parcelamento=int(l.get("numeroParcelamento", 1) or 1),
                numero_parcela_paga=int(l.get("numeroParcelaPaga", 1) or 1),
            ))
            lancamentos_raw.append(dict(l))
        
        # Parse das matrículas vinculadas
        matriculas_detalhe = [
            MatriculaDetalhe(
                matricula=str(m.get("matricula", "")).strip(),
                orgao=str(m.get("orgao", "")).strip(),
                cargo=str(m.get("cargo", "")).strip(),
                tipo_cargo=TipoCargo.from_api(str(m.get("tipoCargo", "")).strip()),
                nivel=str(m.get("nivel", "")).strip(),
            )
            for m in data.get("matriculas", [])
        ]
        
        meta = response_metadata or {}
        
        return RemuneracaoServidor(
            cpf_mascarado=clean(data.get("cpf", "")),
            nome=clean(data.get("nome", "")).upper(),
            mes=int(data.get("mes", 0)),
            ano=int(data.get("ano", 0)),
            matriculas=matriculas_detalhe,
            matriculas_agrupadas=clean(data.get("matriculasAgrupadas", "")),
            orgao_principal=clean(data.get("orgao", "")),
            cargo_principal=clean(data.get("cargo", "")),
            tipo_cargo_principal=TipoCargo.from_api(clean(data.get("tipoCargo", ""))),
            nivel_principal=clean(data.get("nivel", "")),
            matricula=clean(data.get("matricula", "")),
            lancamentos=lancamentos,
            lancamentos_raw=lancamentos_raw,
            deducoes_obrigatorias=float(data.get("deducoesObrigatorias", 0) or 0),
            deducoes_autorizadas=float(data.get("deducoesAutorizadas", 0) or 0),
            remuneracao_bruta=float(data.get("remuneracaoBruta", 0) or 0),
            remuneracao_liquida=float(data.get("remuneracaoLiquida", 0) or 0),
            vencimento_base=float(data.get("vencimento", 0) or 0),
            proventos_diversos=float(data.get("proventosDiversos", 0) or 0),
            pagina_atual=int(meta.get("number", 0)),
            total_paginas=int(meta.get("totalPages", 1)),
            total_elementos=int(meta.get("totalElements", 0)),
            tamanho_pagina=int(meta.get("size", 20)),
            numero_elementos=int(meta.get("numberOfElements", 0)),
            status_api=clean(meta.get("status", "")),
            mensagem_api=clean(meta.get("message", "")),
            payload_original=data if settings.pipeline.VERBOSE else None,
        )