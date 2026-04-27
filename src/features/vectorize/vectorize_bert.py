"""
Vetoriza o corpus de bebidas usando BERTugues (sentence-transformers).

Lê o corpus_bert.jsonl gerado por preparar_corpus_bert.py, codifica cada
produto com o modelo neuralmind/bert-base-portuguese-cased e salva a
matriz de embeddings no mesmo formato usado pelo Word2Vec — permitindo
comparação direta entre os dois modelos.

Entrada : data/processed/corpus/corpus_bert.jsonl
Saída   : models/bert/bert_matrix.npy
          models/bert/bert_skus.json

Uso:
    pip install sentence-transformers
    python vetorizar_bert.py
"""

import json
import numpy as np
from sentence_transformers import SentenceTransformer

from src import logger
from src.config import CORPUS_BERT, VECTORS_BERT

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

# Modelo BERTugues via sentence-transformers.
# Produz embeddings de 768 dimensões otimizados para similaridade semântica.
BERT_MODEL = "neuralmind/bert-base-portuguese-cased"

# Número de textos processados por vez.
# Reduza para 16 ou 8 se receber erros de memória (OOM).
BATCH_SIZE = 32

# Se True, normaliza os vetores para norma unitária antes de salvar.
# Recomendado: torna a similaridade de cosseno equivalente ao produto escalar,
# acelerando buscas futuras. Não altera a estrutura dos clusters.
NORMALIZAR = True

# Arquivos de saída
MATRIX_PATH = VECTORS_BERT / "bert_matrix.npy"
SKUS_PATH   = VECTORS_BERT / "bert_skus.json"

# ---------------------------------------------------------------------------
# Carregamento do corpus
# ---------------------------------------------------------------------------

def carregar_corpus() -> tuple[list[str], list[str]]:
    """Lê corpus_bert.jsonl e retorna (skus, textos) alinhados por índice."""
    if not CORPUS_BERT.exists():
        logger.error("Corpus não encontrado: %s", CORPUS_BERT)
        logger.error("Execute preparar_corpus_bert.py antes desta etapa.")
        raise FileNotFoundError(CORPUS_BERT)

    skus, textos = [], []

    with open(CORPUS_BERT, encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if not linha:
                continue
            registro = json.loads(linha)
            skus.append(str(registro["sku"]))
            textos.append(registro["texto"])

    logger.info("Corpus carregado: %d produtos", len(skus))
    return skus, textos


# ---------------------------------------------------------------------------
# Vetorização
# ---------------------------------------------------------------------------

def carregar_modelo() -> SentenceTransformer:
    logger.info("Carregando modelo: %s", BERT_MODEL)
    logger.info("(Primeira execução faz download ~440 MB — aguarde)")
    modelo = SentenceTransformer(BERT_MODEL)
    logger.info("Modelo carregado.")
    return modelo


def vetorizar(textos: list[str], modelo: SentenceTransformer) -> np.ndarray:
    """Codifica todos os textos em embeddings BERT.

    O parâmetro show_progress_bar exibe uma barra de progresso por batch,
    útil para acompanhar o andamento em corpora grandes.
    """
    logger.info(
        "Vetorizando %d textos (batch_size=%d)…", len(textos), BATCH_SIZE
    )

    matriz = modelo.encode(
        textos,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
        convert_to_numpy=True,
    )

    logger.info("Embeddings gerados: shape=%s  dtype=%s", matriz.shape, matriz.dtype)
    return matriz


def normalizar(matriz: np.ndarray) -> np.ndarray:
    """Normaliza cada vetor para norma unitária (L2)."""
    normas = np.linalg.norm(matriz, axis=1, keepdims=True)
    normas = np.where(normas == 0, 1e-10, normas)  # evita divisão por zero
    return matriz / normas


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------

def salvar(matriz: np.ndarray, skus: list[str]) -> None:
    VECTORS_BERT.mkdir(parents=True, exist_ok=True)

    np.save(str(MATRIX_PATH), matriz)
    logger.info("Matriz salva : %s  %s", MATRIX_PATH, matriz.shape)

    SKUS_PATH.write_text(json.dumps(skus, ensure_ascii=False), encoding="utf-8")
    logger.info("SKUs salvos  : %s  (%d entradas)", SKUS_PATH, len(skus))


# ---------------------------------------------------------------------------
# Inspeção
# ---------------------------------------------------------------------------

def inspecionar(matriz: np.ndarray, skus: list[str]) -> None:
    # Similaridade média entre os 5 primeiros produtos como sanidade básica
    amostra = matriz[:5]
    sim = amostra @ amostra.T  # produto escalar = cosseno se normalizado

    print("\n" + "=" * 60)
    print("VETORIZAÇÃO BERT — RESUMO")
    print("=" * 60)
    print(f"Produtos vetorizados : {len(skus)}")
    print(f"Dimensão do embedding: {matriz.shape[1]}")
    print(f"Normalizado (L2)     : {NORMALIZAR}")
    print(f"Modelo               : {BERT_MODEL}")
    print(f"Matriz salva em      : {MATRIX_PATH}")
    print(f"\nSimilaridade cosseno (5 primeiros produtos):")
    for i in range(len(amostra)):
        scores = "  ".join(f"{sim[i, j]:.3f}" for j in range(len(amostra)))
        print(f"  SKU {skus[i]:<8} → {scores}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    skus, textos = carregar_corpus()
    modelo       = carregar_modelo()
    matriz       = vetorizar(textos, modelo)

    if NORMALIZAR:
        matriz = normalizar(matriz)
        logger.info("Vetores normalizados (L2).")

    salvar(matriz, skus)
    inspecionar(matriz, skus)


if __name__ == "__main__":
    main()
