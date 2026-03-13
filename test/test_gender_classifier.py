#!/usr/bin/env python3
"""Testa o classificador de gênero."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.application.services.gender_classifier import GenderClassifier

def testar():
    csv_path = "data/reference/gruposnomes.csv"
    
    if not Path(csv_path).exists():
        print(f"❌ Arquivo não encontrado: {csv_path}")
        return False
    
    print("🔍 Carregando classificador...")
    classifier = GenderClassifier(csv_path)
    
    print("\n📊 Estatísticas:")
    stats = classifier.estatisticas()
    for key, value in stats.items():
        print(f"  • {key}: {value}")
    
    # Testes com nomes de exemplo
    nomes_teste = [
        "ABELARDO BARAO PENHA",
        "ZILMA SEBASTIANA GAMA DE ALMEIDA",
        "MARIA SILVA",
        "JOÃO SANTOS",
        "MARIA JOSÉ",
        "ANTÔNIO OLIVEIRA",
        "VALDÉLIA LENA",
    ]
    
    print("\n🧪 Testes de classificação:")
    for nome in nomes_teste:
        resultado = classifier.classificar(nome)
        if resultado:
            print(f"✓ {nome:50} → {resultado['genero']:10} "
                  f"(confiança: {resultado['confianca']})")
        else:
            print(f"✗ {nome:50} → Não classificado")
    
    return True

if __name__ == "__main__":
    success = testar()
    sys.exit(0 if success else 1)