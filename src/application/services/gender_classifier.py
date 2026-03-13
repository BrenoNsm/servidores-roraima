"""
Serviço de classificação de gênero baseado em nomes.
Utiliza base de dados de referência com variantes de nomes.

CORREÇÃO: Busca eficiente em nomes principais E variantes.
"""
import csv
import logging
import unicodedata
import re
from pathlib import Path
from typing import Optional, Dict, Set
from dataclasses import dataclass

logger = logging.getLogger(__name__)


def normalizar_nome(nome: str) -> str:
    """
    Normaliza nome removendo acentos e caracteres especiais.
    
    Exemplos:
        "JOÃO" → "JOAO"
        "MARIA JOSÉ" → "MARIA JOSE"
        "ANTÔNIO" → "ANTONIO"
        "RENÉE" → "RENEE"
    
    Args:
        nome: Nome original (pode ter acentos)
        
    Returns:
        Nome normalizado sem acentos
    """
    if not nome:
        return ""
    
    # Converte para maiúsculas e remove espaços extras
    nome = str(nome).strip().upper()
    
    # Normaliza Unicode (decompõe caracteres acentuados)
    # Ex: "Ã" → "A" + "◌" (til combinante)
    nome_normalizado = unicodedata.normalize('NFD', nome)
    
    # Remove todos os diacríticos (acentos, til, cedilha, etc.)
    # Mantém apenas caracteres ASCII básicos
    nome_sem_acentos = ''.join(
        char for char in nome_normalizado
        if unicodedata.category(char) != 'Mn'  # Mn = Mark, Nonspacing (diacríticos)
    )
    
    # Remove caracteres não-alfanuméricos (exceto espaços)
    nome_limpo = re.sub(r'[^A-Z0-9\s]', '', nome_sem_acentos)
    
    # Remove espaços múltiplos
    nome_limpo = re.sub(r'\s+', ' ', nome_limpo).strip()
    
    return nome_limpo


@dataclass
class NomeInfo:
    """Informações sobre um nome e suas variantes."""
    nome_principal: str
    classificacao: str  # 'F' ou 'M'
    frequencia_feminino: int
    frequencia_masculino: int
    frequencia_total: int
    ratio: float
    variantes: Set[str]
    variantes_normalizadas: Set[str]


class GenderClassifier:
    """
    Classificador de gênero baseado em nomes.
    
    Carrega uma base de referência CSV que mapeia nomes principais
    e suas variantes para classificação de gênero.
    
    ESTRATÉGIA DE BUSCA:
    1. Indexa TODAS as variantes (normalizadas) → nome principal
    2. Busca por nome normalizado encontra tanto principal quanto variantes
    3. Ex: "ZILMA" (variante) → indexado → encontra "ZILMA" → retorna info do nome principal
    """
    
    def __init__(self, csv_path: str):
        """
        Inicializa o classificador carregando a base de dados.
        
        Args:
            csv_path: Caminho para o arquivo gruposnomes.csv
        """
        self.csv_path = Path(csv_path)
        self.nome_para_info: Dict[str, NomeInfo] = {}  # nome_principal → info
        self.mapa_busca: Dict[str, str] = {}  # nome_normalizado (principal ou variante) → nome_principal
        
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Arquivo de referência não encontrado: {self.csv_path}")
        
        self._carregar_base()
        logger.info(f"GenderClassifier carregado: {len(self.nome_para_info)} nomes principais, "
                   f"{len(self.mapa_busca)} total de entradas indexadas")
    
    def _carregar_base(self) -> None:
        """
        Carrega o CSV de referência e cria índices de busca otimizados.
        
        ESTRATÉGIA:
        - Para cada linha, indexa o nome principal E todas as variantes
        - Todas as chaves são normalizadas (sem acentos)
        - Todas apontam para o nome_principal
        """
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Pula cabeçalho
            
            # Identifica colunas fixas
            col_name = header.index('name') if 'name' in header else 0
            col_class = header.index('classification') if 'classification' in header else 1
            col_freq_f = header.index('frequency_female') if 'frequency_female' in header else 2
            col_freq_m = header.index('frequency_male') if 'frequency_male' in header else 3
            col_freq_t = header.index('frequency_total') if 'frequency_total' in header else 4
            col_ratio = header.index('ratio') if 'ratio' in header else 5
            
            # Colunas de variantes começam após 'ratio' (coluna G em diante = índice 6+)
            col_variantes_start = 6
            
            total_variantes = 0
            
            for row_num, row in enumerate(reader, start=2):
                try:
                    if not row or not row[col_name].strip():
                        continue
                    
                    # Dados fixos
                    nome_principal = row[col_name].strip().upper()
                    classificacao = row[col_class].strip().upper()
                    freq_f = int(row[col_freq_f]) if len(row) > col_freq_f and row[col_freq_f].strip() else 0
                    freq_m = int(row[col_freq_m]) if len(row) > col_freq_m and row[col_freq_m].strip() else 0
                    freq_t = int(row[col_freq_t]) if len(row) > col_freq_t and row[col_freq_t].strip() else 0
                    ratio = float(row[col_ratio]) if len(row) > col_ratio and row[col_ratio].strip() else 0.0
                    
                    # Normaliza nome principal
                    nome_principal_normalizado = normalizar_nome(nome_principal)
                    
                    # Coleta todas as variantes
                    variantes = set()
                    variantes_normalizadas = set()
                    
                    # Adiciona nome principal às variantes
                    variantes.add(nome_principal)
                    variantes_normalizadas.add(nome_principal_normalizado)
                    
                    # Processa colunas de variantes (G em diante)
                    for i in range(col_variantes_start, len(row)):
                        variante = row[i].strip().upper() if i < len(row) else ""
                        if variante:
                            variantes.add(variante)
                            variante_normalizada = normalizar_nome(variante)
                            if variante_normalizada:
                                variantes_normalizadas.add(variante_normalizada)
                    
                    # Cria objeto de informação
                    info = NomeInfo(
                        nome_principal=nome_principal,
                        classificacao=classificacao,
                        frequencia_feminino=freq_f,
                        frequencia_masculino=freq_m,
                        frequencia_total=freq_t,
                        ratio=ratio,
                        variantes=variantes,
                        variantes_normalizadas=variantes_normalizadas,
                    )
                    
                    # Indexa pelo nome principal normalizado
                    self.nome_para_info[nome_principal] = info
                    
                    # ✅ CORREÇÃO CRÍTICA: Indexa TODAS as formas de busca → nome_principal
                    # Isso inclui nome principal E todas as variantes
                    for variante_norm in variantes_normalizadas:
                        self.mapa_busca[variante_norm] = nome_principal
                        total_variantes += 1
                    
                    # Também indexa variações sem espaço (ex: "MARIAJOSE" → "MARIA JOSE")
                    for variante_norm in variantes_normalizadas:
                        variante_sem_espaco = variante_norm.replace(" ", "")
                        if variante_sem_espaco and variante_sem_espaco != variante_norm:
                            if variante_sem_espaco not in self.mapa_busca:
                                self.mapa_busca[variante_sem_espaco] = nome_principal
                    
                except Exception as e:
                    logger.warning(f"Erro ao processar linha {row_num}: {row[:3] if row else 'vazia'} - {e}")
                    continue
            
            logger.debug(f"Indexação concluída: {len(self.nome_para_info)} nomes principais, "
                        f"{total_variantes} variantes indexadas")
    
    def classificar(self, nome_completo: str) -> Optional[Dict]:
        """
        Classifica o gênero de uma pessoa pelo nome.
        
        ESTRATÉGIA DE BUSCA MULTI-CAMADA:
        1. Extrai primeiro nome e normaliza
        2. Busca direta no mapa_busca (que tem principais + variantes)
        3. Se não encontrar, tenta busca parcial
        4. Retorna None se não encontrado
        
        Args:
            nome_completo: Nome completo da pessoa (pode ter acentos)
            
        Returns:
            Dicionário com classificação ou None se não encontrado
            
        Exemplo:
            {
                "nome_busca": "ZILMA",
                "nome_principal": "ZILMA",  # ou o nome principal da linha
                "genero": "Feminino",
                "confianca": "high",
                "frequencia_total": 12345,
                "ratio": 0.95
            }
        """
        if not nome_completo:
            logger.debug("Nome vazio ou None")
            return None
        
        # Extrai primeiro nome
        partes_nome = nome_completo.strip().upper().split()
        if not partes_nome:
            return None
        
        primeiro_nome_original = partes_nome[0]
        
        # Remove caracteres especiais (mantém apenas letras)
        primeiro_nome_original = ''.join(c for c in primeiro_nome_original if c.isalpha())
        
        if not primeiro_nome_original:
            logger.debug(f"Nome sem caracteres alfabéticos: {nome_completo}")
            return None
        
        # Normaliza nome (remove acentos)
        primeiro_nome_normalizado = normalizar_nome(primeiro_nome_original)
        
        logger.debug(f"Buscando: '{primeiro_nome_original}' → normalizado: '{primeiro_nome_normalizado}'")
        
        # ESTRATÉGIA 1: Busca direta no mapa_busca (mais rápida)
        # Isso funciona para nomes principais E variantes
        if primeiro_nome_normalizado in self.mapa_busca:
            nome_principal = self.mapa_busca[primeiro_nome_normalizado]
            info = self.nome_para_info.get(nome_principal)
            if info:
                logger.debug(f"✓ Encontrado (busca direta): {primeiro_nome_normalizado} → {nome_principal} ({info.classificacao})")
                return self._formatar_resultado(
                    nome_busca=primeiro_nome_original,
                    nome_principal=nome_principal,
                    info=info,
                    confianca="high"
                )
        
        # ESTRATÉGIA 2: Busca sem espaços (ex: "MARIAJOSE" → "MARIA JOSE")
        primeiro_nome_sem_espaco = primeiro_nome_normalizado.replace(" ", "")
        if primeiro_nome_sem_espaco in self.mapa_busca:
            nome_principal = self.mapa_busca[primeiro_nome_sem_espaco]
            info = self.nome_para_info.get(nome_principal)
            if info:
                logger.debug(f"✓ Encontrado (sem espaços): {primeiro_nome_sem_espaco} → {nome_principal}")
                return self._formatar_resultado(
                    nome_busca=primeiro_nome_original,
                    nome_principal=nome_principal,
                    info=info,
                    confianca="high"
                )
        
        # ESTRATÉGIA 3: Busca parcial (substring) em variantes normalizadas
        # Útil para nomes compostos ou abreviações
        for nome_norm, nome_principal in self.mapa_busca.items():
            if primeiro_nome_normalizado in nome_norm or nome_norm in primeiro_nome_normalizado:
                info = self.nome_para_info.get(nome_principal)
                if info:
                    logger.debug(f"✓ Encontrado (busca parcial): {primeiro_nome_normalizado} ↔ {nome_norm}")
                    return self._formatar_resultado(
                        nome_busca=primeiro_nome_original,
                        nome_principal=nome_principal,
                        info=info,
                        confianca="medium"
                    )
        
        # Não encontrado
        logger.debug(f"✗ Não classificado: {primeiro_nome_original} (normalizado: {primeiro_nome_normalizado})")
        return None
    
    def _formatar_resultado(
        self, 
        nome_busca: str, 
        nome_principal: str, 
        info: NomeInfo, 
        confianca: str
    ) -> Dict:
        """Formata o resultado da classificação."""
        return {
            "nome_busca": nome_busca,  # Nome original da busca (com acentos)
            "nome_principal": info.nome_principal,  # Nome principal na base
            "genero": "Feminino" if info.classificacao == "F" else "Masculino",
            "genero_codigo": info.classificacao,
            "confianca": confianca,
            "frequencia_feminino": info.frequencia_feminino,
            "frequencia_masculino": info.frequencia_masculino,
            "frequencia_total": info.frequencia_total,
            "ratio": info.ratio,
            "detalhes": {
                "variantes_count": len(info.variantes),
                "nome_exato_na_base": nome_busca.upper() in info.variantes or normalizar_nome(nome_busca) in info.variantes_normalizadas
            }
        }
    
    def estatisticas(self) -> Dict:
        """Retorna estatísticas da base de dados."""
        total_f = sum(1 for info in self.nome_para_info.values() if info.classificacao == "F")
        total_m = sum(1 for info in self.nome_para_info.values() if info.classificacao == "M")
        
        # Calcula total de variantes únicas
        todas_variantes = set()
        for info in self.nome_para_info.values():
            todas_variantes.update(info.variantes_normalizadas)
        
        return {
            "total_nomes_principais": len(self.nome_para_info),
            "total_entradas_indexadas": len(self.mapa_busca),
            "total_variantes_unicas": len(todas_variantes),
            "nomes_femininos": total_f,
            "nomes_masculinos": total_m,
            "arquivo": str(self.csv_path),
        }
    
    def buscar_nome(self, nome: str) -> Optional[NomeInfo]:
        """
        Método utilitário para buscar informações de um nome específico.
        Útil para debug e testes.
        
        Args:
            nome: Nome a buscar
            
        Returns:
            NomeInfo ou None
        """
        resultado = self.classificar(nome)
        if resultado:
            return self.nome_para_info.get(resultado["nome_principal"])
        return None