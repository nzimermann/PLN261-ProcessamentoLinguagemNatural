"""
Buscador semântico de bebidas — Word2Vec.

Lê os artefatos gerados por vetorizar_w2v.py e o corpus original para
montar um buscador interativo no terminal.

O usuário digita palavras descritivas (ex.: "forte doce amadeirada") e
o script retorna os 10 produtos mais similares e os 10 menos similares,
usando similaridade de cosseno no espaço vetorial do Word2Vec.

Uso:
    python buscar_w2v.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from sklearn.metrics.pairwise import cosine_similarity

# ---------------------------------------------------------------------------
# Caminhos dos arquivos
# ---------------------------------------------------------------------------

MATRIX_PATH = Path("data/processed/vectors/w2v/w2v_matrix.npy")
SKUS_PATH = Path("data/processed/vectors/w2v/w2v_skus.json")
MODEL_PATH = Path("data/processed/vectors/w2v/word2vec.model")
CORPUS_JSONL = Path("data/processed/corpus/corpus_w2v.jsonl")

# Nomes dos campos no corpus_w2v.jsonl — ajuste se os seus forem diferentes
CAMPO_SKU = "sku"
CAMPO_NOME = "nome"  # tente também: "titulo", "name", "produto"
CAMPO_CATEGORIA = "categoria"  # tente também: "tipo", "classe"
CAMPO_DESCRICAO = "descricao"  # tente também: "texto", "descricão", "description"

N_RESULTADOS = 10  # quantidade de itens em cada tabela (top e bottom)

# ---------------------------------------------------------------------------
# Carregamento
# ---------------------------------------------------------------------------


def carregar_tudo():
    """Carrega matriz, SKUs, modelo W2V e metadados do corpus."""
    matriz = np.load(str(MATRIX_PATH))
    skus = json.loads(SKUS_PATH.read_text(encoding="utf-8"))
    modelo = Word2Vec.load(str(MODEL_PATH))

    # Lê o corpus para obter nome, categoria e descrição de cada produto
    metadados = {}  # sku → {nome, categoria, descricao}
    with open(CORPUS_JSONL, encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if not linha:
                continue
            reg = json.loads(linha)
            sku = str(reg.get(CAMPO_SKU, ""))
            metadados[sku] = {
                "nome": reg.get(CAMPO_NOME, "—"),
                "categoria": reg.get(CAMPO_CATEGORIA, "—"),
                "descricao": reg.get(CAMPO_DESCRICAO, "—"),
            }

    return matriz, skus, modelo, metadados


# ---------------------------------------------------------------------------
# Busca
# ---------------------------------------------------------------------------


def vetorizar_consulta(consulta: str, modelo: Word2Vec) -> np.ndarray | None:
    """
    Converte a consulta em um vetor pela média dos vetores de cada palavra.

    Palavras fora do vocabulário são ignoradas silenciosamente.
    Retorna None se nenhuma palavra da consulta existir no vocabulário.
    """
    tokens = consulta.lower().split()
    vetores = [modelo.wv[t] for t in tokens if t in modelo.wv]

    if not vetores:
        palavras_desconhecidas = [t for t in tokens if t not in modelo.wv]
        print(
            f"\n[!] Nenhuma palavra encontrada no vocabulário: {palavras_desconhecidas}"
        )
        print(
            "    Tente palavras presentes nas descrições dos produtos (ex.: 'amadeirado', 'suave', 'defumado')."
        )
        return None

    palavras_reconhecidas = [t for t in tokens if t in modelo.wv]
    if len(palavras_reconhecidas) < len(tokens):
        ignoradas = [t for t in tokens if t not in modelo.wv]
        print(f"[!] Palavras ignoradas (fora do vocabulário): {ignoradas}")

    return np.mean(vetores, axis=0)


def buscar(
    consulta: str, matriz: np.ndarray, skus: list, modelo: Word2Vec, metadados: dict
) -> None:
    """
    Realiza a busca semântica e imprime as duas tabelas de resultados.

    Args:
        consulta:   String digitada pelo usuário.
        matriz:     Matriz de embeddings (produtos × dimensões).
        skus:       Lista de SKUs alinhada com as linhas da matriz.
        modelo:     Modelo Word2Vec treinado.
        metadados:  Dicionário sku → {nome, categoria, descricao}.
    """
    vetor_consulta = vetorizar_consulta(consulta, modelo)
    if vetor_consulta is None:
        return

    # Calcula similaridade de cosseno entre a consulta e todos os produtos
    similaridades = cosine_similarity(vetor_consulta.reshape(1, -1), matriz)[0]

    # Monta o DataFrame completo com todos os produtos
    df = pd.DataFrame(
        {
            "sku": skus,
            "similaridade": similaridades,
        }
    )

    # Adiciona colunas de metadados
    df["nome"] = df["sku"].map(lambda s: metadados.get(s, {}).get("nome", "—"))
    df["categoria"] = df["sku"].map(
        lambda s: metadados.get(s, {}).get("categoria", "—")
    )
    df["descricao"] = df["sku"].map(
        lambda s: metadados.get(s, {}).get("descricao", "—")
    )

    # Ordena por similaridade decrescente
    df = df.sort_values("similaridade", ascending=False).reset_index(drop=True)

    # Formata similaridade para exibição
    df["similaridade"] = df["similaridade"].round(4)

    colunas_exibir = ["sku", "nome", "categoria", "descricao", "similaridade"]

    # --- Top 10 (mais similares) ---
    top10 = df.head(N_RESULTADOS)[colunas_exibir].copy()
    top10.index = range(1, N_RESULTADOS + 1)

    print(f"\n{'='*70}")
    print(f'  TOP {N_RESULTADOS} — Mais similares a: "{consulta}"')
    print(f"{'='*70}")
    print(top10.to_string())

    # --- Bottom 10 (menos similares) — mantém ordem decrescente ---
    df = df.sort_values(by="similaridade", ascending=False).reset_index(drop=True)
    bottom10 = df.sort_values(by="similaridade", ascending=False, ignore_index=True)
    # bottom10 = df.tail(N_RESULTADOS)[colunas_exibir].copy()
    # bottom10 = bottom10.sort_values("similaridade", ascending=False).reset_index(
    #     drop=True
    # )
    bottom10.index = range(1, N_RESULTADOS + 1)

    print(f"\n{'='*70}")
    print(f'  BOTTOM {N_RESULTADOS} — Menos similares a: "{consulta}"')
    print(f"{'='*70}")
    print(bottom10.to_string())
    print()


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------


def main():
    print("Carregando artefatos Word2Vec…")
    matriz, skus, modelo, metadados = carregar_tudo()
    print(f"Pronto. {len(skus)} produtos · {len(modelo.wv)} tokens no vocabulário.\n")

    while True:
        try:
            consulta = input("Busca (ou Enter para sair): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nEncerrando.")
            break

        if not consulta:
            print("Encerrando.")
            break

        buscar(consulta, matriz, skus, modelo, metadados)


if __name__ == "__main__":
    main()
