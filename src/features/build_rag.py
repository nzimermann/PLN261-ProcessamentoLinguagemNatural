"""
Constrói o índice FAISS para o agente RAG de bebidas.

Reutiliza os embeddings BERT já gerados por vectorize_bert.py, evitando
re-computação. Cada produto é um documento — não há necessidade de chunking,
pois os textos de produto são atômicos e de tamanho controlado.

Entradas:
    models/bert/bert_matrix.npy    — embeddings normalizados (N, 768)
    models/bert/bert_skus.json     — lista de SKUs alinhada com a matriz
    data/processed/products.csv    — metadados completos dos produtos

Saídas:
    models/rag/faiss.index         — índice FAISS pronto para busca
    models/rag/rag_metadata.json   — lista de metadados na mesma ordem do índice

Uso:
    pip install faiss-cpu
    python build_rag.py
"""

import json

import faiss
import numpy as np
import pandas as pd

from src import logger
from src.config import PROCESSED_DIR, VECTORS_BERT, VECTORS_RAG

# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------

MATRIX_PATH = VECTORS_BERT / "bert_matrix.npy"
SKUS_PATH = VECTORS_BERT / "bert_skus.json"
PRODUCTS_CSV = PROCESSED_DIR / "products.csv"
INDEX_PATH = VECTORS_RAG / "faiss.index"
META_PATH = VECTORS_RAG / "rag_metadata.json"

# ---------------------------------------------------------------------------
# Carregamento
# ---------------------------------------------------------------------------


def carregar_embeddings() -> tuple[np.ndarray, list[str]]:
    """Carrega a matriz de embeddings BERT e os SKUs correspondentes."""
    if not MATRIX_PATH.exists():
        logger.error("Matriz não encontrada: %s", MATRIX_PATH)
        logger.error("Execute vectorize_bert.py antes desta etapa.")
        raise FileNotFoundError(MATRIX_PATH)

    matriz = np.load(str(MATRIX_PATH)).astype(np.float32)
    skus = json.loads(SKUS_PATH.read_text(encoding="utf-8"))

    logger.info("Embeddings carregados: shape=%s  dtype=%s", matriz.shape, matriz.dtype)

    # Garante vetores normalizados — IndexFlatIP com vetores L2=1 equivale a cosine
    normas = np.linalg.norm(matriz, axis=1, keepdims=True)
    normas = np.where(normas == 0, 1e-10, normas)
    matriz = matriz / normas

    return matriz, skus


def carregar_produtos(skus: list[str]) -> list[dict]:
    """Lê products.csv e monta a lista de metadados alinhada com os SKUs.

    Cada posição i da lista corresponde à linha i da matriz FAISS,
    permitindo recuperar os dados do produto pelo índice retornado pela busca.
    """
    df = pd.read_csv(PRODUCTS_CSV, dtype={"sku": str})
    df = df.drop_duplicates(subset="sku", keep="first").set_index("sku")

    metadados = []
    ausentes = 0

    for sku in skus:
        if sku not in df.index:
            logger.warning("SKU %s não encontrado no CSV — usando registro vazio.", sku)
            ausentes += 1
            metadados.append({"sku": sku})
            continue

        row = df.loc[sku]
        metadados.append(
            {
                "sku": sku,
                "nome": str(row.get("name", "")),
                "marca": str(row.get("brand", "")),
                "categoria": str(row.get("category", "")),
                "descricao": str(row.get("description", "")),
                "preco": float(row["price"]) if pd.notna(row.get("price")) else None,
                "em_estoque": bool(row.get("in_stock", True)),
                "url": str(row.get("url", "")),
            }
        )

    if ausentes:
        logger.warning("%d SKU(s) sem dados no CSV.", ausentes)

    logger.info("Metadados montados: %d produtos", len(metadados))
    return metadados


# ---------------------------------------------------------------------------
# Construção do índice FAISS
# ---------------------------------------------------------------------------


def construir_indice(matriz: np.ndarray) -> faiss.Index:
    """Cria um índice FAISS IndexFlatIP (produto interno = cosine similarity
    para vetores normalizados em L2).

    IndexFlatIP é a escolha correta aqui porque:
    - Os vetores já estão normalizados (norma = 1)
    - Produto interno de vetores normalizados == similaridade de cosseno
    - Flat garante busca exata (sem aproximação), ideal para < 100k documentos
    """
    dimensao = matriz.shape[1]
    logger.info("Construindo IndexFlatIP (dim=%d, n=%d)…", dimensao, len(matriz))

    indice = faiss.IndexFlatIP(dimensao)
    indice.add(matriz)

    logger.info("Índice construído: %d vetores indexados.", indice.ntotal)
    return indice


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------


def salvar(indice: faiss.Index, metadados: list[dict]) -> None:
    VECTORS_RAG.mkdir(parents=True, exist_ok=True)

    faiss.write_index(indice, str(INDEX_PATH))
    logger.info("Índice FAISS salvo: %s", INDEX_PATH)

    META_PATH.write_text(
        json.dumps(metadados, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Metadados salvos : %s  (%d entradas)", META_PATH, len(metadados))


# ---------------------------------------------------------------------------
# Validação rápida
# ---------------------------------------------------------------------------


def validar(indice: faiss.Index, matriz: np.ndarray, metadados: list[dict]) -> None:
    """Faz uma busca de sanidade: cada produto deve ser o mais similar a si mesmo."""
    amostra_idx = [0, 1, 2]
    amostra = matriz[amostra_idx]

    scores, vizinhos = indice.search(amostra, k=1)

    print("\n" + "=" * 60)
    print("ÍNDICE RAG — VALIDAÇÃO DE SANIDADE")
    print("=" * 60)
    print(f"Vetores no índice : {indice.ntotal}")
    print(f"Dimensão          : {indice.d}")
    print(f"Tipo de índice    : {type(indice).__name__}")
    print(f"\nBusca de sanidade (cada produto deve recuperar a si mesmo):")
    for i, idx in enumerate(amostra_idx):
        meta = metadados[idx]
        score = scores[i][0]
        viz = vizinhos[i][0]
        ok = "✓" if viz == idx else "✗ FALHA"
        print(
            f"  [{ok}] SKU {meta['sku']:<8} → vizinho={viz}  score={score:.4f}  "
            f"nome: {meta.get('nome','')[:40]}"
        )
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    print("\n" + "=" * 60)
    print("CONSTRUÇÃO DO ÍNDICE RAG")
    print("=" * 60)

    print("\n[1/4] Carregando embeddings BERT…")
    matriz, skus = carregar_embeddings()
    print(f"      {len(skus)} produtos  ·  {matriz.shape[1]} dimensões")

    print("\n[2/4] Carregando metadados dos produtos…")
    metadados = carregar_produtos(skus)

    print("\n[3/4] Construindo índice FAISS…")
    indice = construir_indice(matriz)

    print("\n[4/4] Salvando e validando…")
    salvar(indice, metadados)
    validar(indice, matriz, metadados)

    print("\nPróximo passo: python agente_bebidas.py")


if __name__ == "__main__":
    main()
