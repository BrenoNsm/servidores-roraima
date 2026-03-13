"""
Entidades de domínio que representam os dados de servidores.
Estas classes são puras, sem dependências externas.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict
import re


class TipoCargo:
    """
    Classe para tratamento flexível de tipos de cargo.
    
    Armazena o valor original da API e oferece métodos para:
    - Categorização em tipos padrão
    - Validação de valores conhecidos
    - Normalização para análise
    """
    
    # Categorias padrão para análise consolidada
    CATEGORIAS = {
        "EFETIVO": ["EFETIVO", "EFETIVO - ESTATUTÁRIO", "PROVIMENTO EFETIVO"],
        "COMISSIONADO": ["COMISSIONADO", "CARGO COMISSIONADO", "FUNÇÃO COMISSIONADA"],
        "TEMPORARIO": [
            "TEMPORARIO", "TEMPORÁRIO", "ACT", 
            "ACT - ADMITIDO EM CARATER TEMPORARIO",
            "CONTRATO POR PRAZO DETERMINADO",
            "TEMPORÁRIO - LEI 892/2013",
            "ADM. CARÁTER TEMPORÁRIO"
        ],
        "ESTAGIARIO": ["ESTAGIARIO", "ESTAGIÁRIO", "BOLSA ESTÁGIO"],
        "OUTROS": []
    }
    
    def __init__(self, valor_api: str):
        self.valor_original = str(valor_api).strip().upper() if valor_api else ""
        self._categoria = self._classificar()
    
    def _classificar(self) -> str:
        """Classifica o valor original em uma categoria padrão."""
        if not self.valor_original:
            return "DESCONHECIDO"
        
        for categoria, padrões in self.CATEGORIAS.items():
            if categoria == "OUTROS":
                continue
            for padrão in padrões:
                if padrão in self.valor_original or self.valor_original in padrão:
                    return categoria
        
        return "OUTROS"
    
    @property
    def categoria(self) -> str:
        return self._categoria
    
    @property
    def e_efetivo(self) -> bool:
        return self._categoria == "EFETIVO"
    
    @property
    def e_comissionado(self) -> bool:
        return self._categoria == "COMISSIONADO"
    
    @property
    def e_temporario(self) -> bool:
        return self._categoria == "TEMPORARIO"
    
    @property
    def e_estagiario(self) -> bool:
        return self._categoria == "ESTAGIARIO"
    
    def to_dict(self) -> dict:
        """Serializa para dicionário com todos os níveis de informação."""
        return {
            "valor_original": self.valor_original,
            "categoria": self._categoria,
            "eh_efetivo": self.e_efetivo,
            "eh_comissionado": self.e_comissionado,
            "eh_temporario": self.e_temporario,
            "eh_estagiario": self.e_estagiario,
        }
    
    def __str__(self) -> str:
        return self.valor_original
    
    def __repr__(self) -> str:
        return f"TipoCargo(original='{self.valor_original}', categoria='{self._categoria}')"
    
    @classmethod
    def from_api(cls, valor: str) -> "TipoCargo":
        """Factory method para criar a partir do valor da API."""
        return cls(valor)
    
    @classmethod
    def from_string(cls, valor: str) -> Optional["TipoCargo"]:
        """Compatibilidade com código legado."""
        if not valor:
            return None
        return cls(valor)


@dataclass(frozen=True)
class ServidorBasico:
    """Representa informações básicas de um servidor."""
    matricula: str
    nome: str
    orgao: str
    cargo: str
    lotacao: str
    data_admissao: datetime
    data_desligamento: Optional[datetime]
    carga_horaria: int
    genero: Optional[str] = None #F or M or None
    genero_confianca: Optional[str] = None #high, medium or low 
    
    @property
    def esta_ativo(self) -> bool:
        if self.data_desligamento is None:
            return True
        return datetime.now() < self.data_desligamento
    
    def to_dict(self) -> dict:
        return {
            "matricula": self.matricula,
            "nome": self.nome,
            "orgao": self.orgao,
            "cargo": self.cargo,
            "lotacao": self.lotacao,
            "data_admissao": self.data_admissao.isoformat() if self.data_admissao else None,
            "data_desligamento": self.data_desligamento.isoformat() if self.data_desligamento else None,
            "carga_horaria": self.carga_horaria,
            "genero": self.genero,
            "genero_confianca": self.genero_confianca,
        }


@dataclass(frozen=True)
class LancamentoRemuneracao:
    """Representa um lançamento individual de remuneração."""
    tipo_evento: str
    evento: str
    quantidade: float
    valor: float
    numero_parcelamento: int
    numero_parcela_paga: int


@dataclass(frozen=True)
class MatriculaDetalhe:
    """Detalhes específicos de uma matrícula dentro do servidor."""
    matricula: str
    orgao: str
    cargo: str
    tipo_cargo: Optional[TipoCargo]
    nivel: str
    
    def to_dict(self) -> dict:
        return {
            "matricula": self.matricula,
            "orgao": self.orgao,
            "cargo": self.cargo,
            "tipo_cargo": self.tipo_cargo.to_dict() if self.tipo_cargo else None,
            "nivel": self.nivel,
        }


@dataclass(frozen=True)
class RemuneracaoServidor:
    """Representa a remuneração detalhada de um servidor."""
    # Dados pessoais
    cpf_mascarado: str
    nome: str
    mes: int
    ano: int
    
    # Informações da matrícula
    matriculas: List[MatriculaDetalhe]
    orgao_principal: str
    cargo_principal: str
    tipo_cargo_principal: Optional[TipoCargo]
    nivel_principal: str
    matricula: str
    
    # Valores financeiros
    lancamentos: List[LancamentoRemuneracao]
    deducoes_obrigatorias: float
    deducoes_autorizadas: float
    remuneracao_bruta: float
    remuneracao_liquida: float
    vencimento_base: float
    proventos_diversos: float
    
    # Metadados da API
    pagina_atual: int = 0
    total_paginas: int = 1
    total_elementos: int = 0
    tamanho_pagina: int = 20
    numero_elementos: int = 0
    matriculas_agrupadas: str = ""
    status_api: str = ""
    mensagem_api: str = ""
    lancamentos_raw: List[Dict] = field(default_factory=list)
    payload_original: Optional[Dict] = None
    
    @property
    def total_proventos(self) -> float:
        return sum(l.valor for l in self.lancamentos if l.tipo_evento == "P")
    
    @property
    def total_deducoes(self) -> float:
        return sum(l.valor for l in self.lancamentos if l.tipo_evento == "D")
    
    @property
    def matricula_principal(self) -> str:
        if self.matriculas:
            return self.matriculas[0].matricula
        return self.matricula
    
    def to_dict(self) -> dict:
        """Serializa para dicionário COM TODOS OS CAMPOS."""
        return {
            "cpf_mascarado": self.cpf_mascarado,
            "nome": self.nome,
            "periodo": f"{self.mes:02d}/{self.ano}",
            "mes": self.mes,
            "ano": self.ano,
            "matricula": self.matricula_principal,
            "orgao": self.orgao_principal,
            "cargo": self.cargo_principal,
            "tipo_cargo": self.tipo_cargo_principal.to_dict() if self.tipo_cargo_principal else None,
            "tipo_cargo_categoria": self.tipo_cargo_principal.categoria if self.tipo_cargo_principal else None,
            "nivel": self.nivel_principal,
            "matriculas": [m.to_dict() for m in self.matriculas],
            "matriculas_agrupadas": self.matriculas_agrupadas,
            "remuneracao_bruta": self.remuneracao_bruta,
            "remuneracao_liquida": self.remuneracao_liquida,
            "vencimento_base": self.vencimento_base,
            "proventos_diversos": self.proventos_diversos,
            "deducoes_obrigatorias": self.deducoes_obrigatorias,
            "deducoes_autorizadas": self.deducoes_autorizadas,
            "total_proventos": self.total_proventos,
            "total_deducoes": self.total_deducoes,
            "lancamentos": [
                {
                    "tipo_evento": l.tipo_evento,
                    "evento": l.evento,
                    "quantidade": l.quantidade,
                    "valor": l.valor,
                    "numero_parcelamento": l.numero_parcelamento,
                    "numero_parcela_paga": l.numero_parcela_paga,
                }
                for l in self.lancamentos
            ],
            "lancamentos_raw": self.lancamentos_raw,
            "api_metadata": {
                "pagina_atual": self.pagina_atual,
                "total_paginas": self.total_paginas,
                "total_elementos": self.total_elementos,
                "tamanho_pagina": self.tamanho_pagina,
                "numero_elementos": self.numero_elementos,
                "status": self.status_api,
                "mensagem": self.mensagem_api,
            },
            "payload_original": self.payload_original,
        }


@dataclass
class ServidorCompleto:
    """Entidade rica que combina dados básicos + remunerações."""
    dados_basicos: ServidorBasico
    remuneracoes: List[RemuneracaoServidor] = field(default_factory=list)
    
    def adicionar_remuneracao(self, remuneracao: RemuneracaoServidor) -> None:
        self.remuneracoes.append(remuneracao)
    
    def get_remuneracao_por_periodo(self, mes: int, ano: int) -> Optional[RemuneracaoServidor]:
        for rem in self.remuneracoes:
            if rem.mes == mes and rem.ano == ano:
                return rem
        return None
    
    def calcular_media_liquida(self) -> Optional[float]:
        if not self.remuneracoes:
            return None
        liquidos = [r.remuneracao_liquida for r in self.remuneracoes if r.remuneracao_liquida > 0]
        return sum(liquidos) / len(liquidos) if liquidos else None