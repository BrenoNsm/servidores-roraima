#!/usr/bin/env python3
"""
Script principal para executar o pipeline de coleta de servidores do estado de Roraima.

Este script orquestra todo o fluxo:
1. Carrega configurações e logging
2. Inicializa clientes de API
3. Inicializa classificador de gênero (se disponível)
4. Executa coleta em streaming
5. Salva dados em JSON/CSV
6. Gera relatório de estatísticas

Uso:
    python scripts/run_pipeline.py --inicio 2025-01-01 --fim 2025-12-31 --formato json

Autor: Pipeline de Dados - Servidores Roraima
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import Optional  # ← ADICIONADO: Import necessário para type hints

# Adiciona src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import settings, Config
from src.config.logger import setup_logging
from src.application.services.collector_service import CollectorService
from src.application.services.storage_service import StorageService
from src.application.services.gender_classifier import GenderClassifier

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parse argumentos da linha de comando.
    
    Returns:
        Namespace com todos os argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description="Pipeline de coleta de servidores do estado de Roraima",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Coleta completa com gênero e remunerações
  python scripts/run_pipeline.py --inicio 2025-01-01 --fim 2025-12-31
  
  # Apenas lista básica (mais rápido)
  python scripts/run_pipeline.py --sem-detalhes --sem-genero
  
  # Filtrar por órgão específico
  python scripts/run_pipeline.py --orgao SEED
  
  # Teste sem salvar dados
  python scripts/run_pipeline.py --dry-run --verbose
        """
    )
    
    parser.add_argument(
        "--inicio", "-i",
        type=str,
        default=settings.pipeline.DEFAULT_START_DATE,
        help="Data inicial (YYYY-MM-DD). Default: %(default)s"
    )
    parser.add_argument(
        "--fim", "-f",
        type=str,
        default=settings.pipeline.DEFAULT_END_DATE,
        help="Data final (YYYY-MM-DD). Default: %(default)s"
    )
    parser.add_argument(
        "--orgao", "-o",
        type=str,
        default=None,
        help="Filtrar por órgão específico (ex: SEED)"
    )
    parser.add_argument(
        "--sem-detalhes", "-d",
        action="store_true",
        help="Não coletar dados de remuneração (apenas lista básica)"
    )
    parser.add_argument(
        "--sem-genero", "-g",
        action="store_true",
        help="Não classificar gênero pelo nome"
    )
    parser.add_argument(
        "--formato", "-fmt",
        choices=["json", "csv", "both"],
        default="json",
        help="Formato de saída. Default: %(default)s"
    )
    parser.add_argument(
        "--output", "-out",
        type=str,
        default=settings.pipeline.OUTPUT_DIR,
        help="Diretório de saída. Default: %(default)s"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Habilitar logs detalhados"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executar sem salvar dados (apenas teste)"
    )
    
    return parser.parse_args()


def inicializar_gender_classifier() -> Optional[GenderClassifier]:
    """
    Inicializa o classificador de gênero se o arquivo CSV existir.
    
    Returns:
        GenderClassifier ou None se arquivo não encontrado
    """
    csv_path = Path("data/reference/gruposnomes.csv")
    
    if csv_path.exists():
        try:
            classifier = GenderClassifier(str(csv_path))
            stats = classifier.estatisticas()
            logger.info(f"✓ Classificador de gênero carregado: {stats['total_nomes_principais']} nomes, "
                       f"{stats['total_entradas_indexadas']} entradas indexadas")
            logger.info(f"  • Nomes femininos: {stats['nomes_femininos']}")
            logger.info(f"  • Nomes masculinos: {stats['nomes_masculinos']}")
            return classifier
        except Exception as e:
            logger.warning(f"⚠️  Erro ao carregar classificador de gênero: {e}")
            return None
    else:
        logger.warning(f"⚠️  Arquivo {csv_path} não encontrado. "
                      "Classificação de gênero desabilitada.")
        logger.warning(f"  Para habilitar, coloque o arquivo em: {csv_path}")
        return None


def main() -> int:
    """
    Função principal do pipeline.
    
    Returns:
        Código de saída (0 = sucesso, 1 = erro)
    """
    args = parse_args()
    
    # Configura logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(level=log_level, log_dir=settings.pipeline.LOG_DIR)
    
    logger.info("=" * 70)
    logger.info("🚀 PIPELINE DE COLETA DE SERVIDORES - RORAIMA")
    logger.info("=" * 70)
    logger.info(f"📅 Período: {args.inicio} até {args.fim}")
    if args.orgao:
        logger.info(f"🏢 Filtro por órgão: {args.orgao}")
    logger.info(f"💾 Formato de saída: {args.formato}")
    logger.info(f"📁 Diretório de saída: {args.output}")
    if args.dry_run:
        logger.warning("⚠️  DRY RUN: Nenhum dado será salvo em disco")
    logger.info("=" * 70)
    
    try:
        # Inicializa classificador de gênero (opcional)
        gender_classifier = None
        if not args.sem_genero:
            gender_classifier = inicializar_gender_classifier()
        else:
            logger.info("⚠️  Classificação de gênero desabilitada por flag --sem-genero")
        
        # Inicializa serviços dentro de context manager (garante cleanup)
        with CollectorService(gender_classifier=gender_classifier) as collector:
            storage = StorageService(output_dir=args.output, dry_run=args.dry_run)
            
            # Executa coleta em streaming
            total_processados = 0
            servidores_com_genero = 0
            servidores_com_remuneracao = 0
            
            logger.info("\n📥 Iniciando coleta de dados...\n")
            
            for servidor in collector.coletar_periodo(
                data_inicio=args.inicio,
                data_fim=args.fim,
                orgao=args.orgao,
                coletar_detalhes=not args.sem_detalhes,
                classificar_genero=not args.sem_genero and gender_classifier is not None,
            ):
                # Salva conforme formato solicitado
                if args.formato in ["json", "both"]:
                    storage.save_json(servidor, modo="detalhado")
                if args.formato in ["csv", "both"]:
                    storage.save_csv(servidor)
                
                total_processados += 1
                
                # Conta servidores com gênero classificado
                if servidor.dados_basicos.genero:
                    servidores_com_genero += 1
                
                # Conta servidores com remunerações
                if servidor.remuneracoes:
                    servidores_com_remuneracao += 1
                
                # Log de progresso a cada N registros
                if total_processados % 50 == 0:
                    logger.info(f"⏳ Processados: {total_processados} servidores...")
            
            # Relatório final
            stats_coleta = collector.get_stats()
            stats_storage = storage.get_stats()
            
            logger.info("\n" + "=" * 70)
            logger.info("📊 ESTATÍSTICAS DA EXECUÇÃO")
            logger.info("=" * 70)
            logger.info(f"✅ Servidores encontrados: {stats_coleta['servidores_encontrados']}")
            logger.info(f"✅ Servidores processados: {total_processados}")
            if total_processados > 0:
                logger.info(f"✅ Servidores com gênero: {servidores_com_genero} "
                          f"({servidores_com_genero/total_processados*100:.1f}%)")
                logger.info(f"✅ Servidores com remuneração: {servidores_com_remuneracao} "
                          f"({servidores_com_remuneracao/total_processados*100:.1f}%)")
            logger.info(f"✅ Detalhes de remuneração coletados: {stats_coleta['detalhes_coletados']}")
            logger.info(f"✅ Arquivos criados: {stats_storage['arquivos_criados']}")
            logger.info(f"✅ Bytes escritos: {stats_storage['bytes_escritos']/1024:.1f} KB")
            logger.info(f"❌ Erros na API: {stats_coleta['erros_api']}")
            logger.info(f"❌ Erros no storage: {stats_storage['erros']}")
            
            if args.dry_run:
                logger.warning("\n⚠️  DRY RUN: Nenhum dado foi salvo em disco")
            
            logger.info("\n" + "=" * 70)
            logger.info("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
            logger.info("=" * 70)
            
            return 0
            
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Execução interrompida pelo usuário")
        return 130
    except Exception as e:
        logger.exception(f"❌ Erro crítico no pipeline: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())