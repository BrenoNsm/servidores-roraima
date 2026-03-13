# scripts/test_fix.py
#!/usr/bin/env python3
"""Testa a correção da API de detalhamento."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.infrastructure.api.detalhamento_api import DetalhamentoAPIClient

def testar_correcao():
    """Testa se a correção funciona com os exemplos reais."""
    
    client = DetalhamentoAPIClient()
    
    # Teste 1: Matrícula com traços → deve ser formatada automaticamente
    print("🔍 Teste 1: Formatação de matrícula")
    matricula_original = "0170444-3-01"
    matricula_formatada = client.formatar_matricula(matricula_original)
    print(f"   Original: {matricula_original}")
    print(f"   Formatada: {matricula_formatada}")
    assert matricula_formatada == "0170444301", "Formatação falhou!"
    print("   ✓ Formatação OK\n")
    
    # Teste 2: Chamada real à API (sem parâmetro page)
    print("🔍 Teste 2: Chamada real à API")
    print(f"   Buscando: {matricula_formatada} em 09/2025")
    
    try:
        result = client.buscar_detalhes(matricula_original, mes=9, ano=2025)
        
        if result:
            print(f"   ✅ SUCESSO! Dados coletados:")
            print(f"      • Nome: {result.nome}")
            print(f"      • Cargo: {result.cargo_principal}")
            print(f"      • Bruto: R$ {result.remuneracao_bruta:,.2f}")
            print(f"      • Líquido: R$ {result.remuneracao_liquida:,.2f}")
            print(f"      • Lançamentos: {len(result.lancamentos)}")
            
            # Mostra todos os lançamentos
            for l in result.lancamentos:
                sinal = "+" if l.tipo_evento == "P" else "-"
                print(f"         {sinal} R$ {l.valor:,.2f} | {l.evento}")
            
            # Valida que capturamos os campos extras
            assert result.matriculas_agrupadas != "", "matriculasAgrupadas não capturado!"
            assert result.lancamentos_raw, "lancamentos_raw não capturado!"
            print(f"      • Campos extras: ✓ matriculasAgrupadas, ✓ lancamentos_raw")
            
        else:
            print(f"   ⚠️  Sem dados retornados (pode ser normal para este mês)")
            
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        client.close()
    
    print("\n✅ Teste concluído!")


if __name__ == "__main__":
    testar_correcao()