"""
Prepara o corpus para vetorização com BERTugues.

Lê o CSV de produtos e gera um JSONL onde cada linha contém o SKU
e um texto corrido que será enviado ao BERT. Diferente do Word2Vec,
o BERT espera linguagem natural — sem tokenização prévia, sem remoção
de stopwords.

Entrada : data/processed/products.csv
Saída   : data/processed/corpus/corpus_bert.jsonl

Uso:
    python preparar_corpus_bert.py
"""

import json
import pandas as pd

from src import logger
from src.config import PROCESSED_DIR, CORPUS_DIR

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

CSV_PATH    = PROCESSED_DIR / "products.csv"
OUTPUT_PATH = CORPUS_DIR / "corpus_bert.jsonl"

# Número máximo de caracteres do texto final enviado ao BERT.
# O BERTugues suporta até 512 tokens (~2000–2500 caracteres).
# Textos maiores serão truncados pelo tokenizador do modelo — não há perda
# de informação crítica pois o início do texto tem as partes mais relevantes.
MAX_CHARS = 2000

# ---------------------------------------------------------------------------
# Montagem do texto
# ---------------------------------------------------------------------------

def montar_texto(row: pd.Series) -> str:
    """Concatena os campos do produto em texto corrido para o BERT.

    Ordem escolhida por relevância semântica:
        1. Nome      — identifica o produto
        2. Marca     — contexto de origem
        3. Categoria — ancora o domínio semântico
        4. Descrição — maior riqueza de conteúdo

    Campos vazios ou NaN são ignorados silenciosamente.
    """
    partes = []

    for campo in ["name", "brand", "category", "description"]:
        valor = row.get(campo)
        if pd.notna(valor) and str(valor).strip():
            partes.append(str(valor).strip())

    texto = ". ".join(partes)
    return texto[:MAX_CHARS]

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def carregar_csv() -> pd.DataFrame:
    logger.info("Lendo CSV: %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH, dtype={"sku": str})
    logger.info("Produtos carregados: %d", len(df))
    logger.info("Colunas: %s", df.columns.tolist())
    return df


def gerar_corpus(df: pd.DataFrame) -> list[dict]:
    """Gera a lista de registros {sku, texto} para o corpus BERT."""
    corpus = []
    ignorados = 0

    for _, row in df.iterrows():
        texto = montar_texto(row)

        if not texto:
            logger.warning("SKU %s ignorado — texto vazio após montagem.", row["sku"])
            ignorados += 1
            continue

        corpus.append({
            "sku":  row["sku"],
            "texto": texto,
        })

    if ignorados:
        logger.warning("%d produto(s) ignorados por texto vazio.", ignorados)

    logger.info("Registros gerados: %d", len(corpus))
    return corpus


def salvar_jsonl(corpus: list[dict]) -> None:
    CORPUS_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for registro in corpus:
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")

    logger.info("Corpus salvo: %s", OUTPUT_PATH)


def inspecionar(corpus: list[dict]) -> None:
    """Exibe estatísticas e um exemplo para conferência visual."""
    comprimentos = [len(r["texto"]) for r in corpus]

    print("\n" + "=" * 60)
    print("CORPUS BERT — INSPEÇÃO")
    print("=" * 60)
    print(f"Total de registros : {len(corpus)}")
    print(f"Caracteres/texto   : mín {min(comprimentos)} · "
          f"méd {sum(comprimentos) // len(comprimentos)} · "
          f"máx {max(comprimentos)}")
    print(f"Limite configurado : {MAX_CHARS} caracteres")
    print(f"Arquivo de saída   : {OUTPUT_PATH}")
    print("\n--- Exemplo (primeiro registro) ---")
    exemplo = corpus[0]
    print(f"SKU  : {exemplo['sku']}")
    print(f"Texto: {exemplo['texto'][:300]}…")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    df     = carregar_csv()
    corpus = gerar_corpus(df)
    salvar_jsonl(corpus)
    inspecionar(corpus)


if __name__ == "__main__":
    main()
