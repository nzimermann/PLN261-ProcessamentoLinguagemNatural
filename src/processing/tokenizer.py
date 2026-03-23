"""
Pipeline de tokenização — Casa da Bebida.

Lê data/processed/products.csv e data/processed/reviews.csv (somente leitura),
tokeniza os campos de texto relevantes para NLP usando spaCy com o modelo
português pt_core_news_lg, e salva os resultados em:

    data/processed/tokens_products.jsonl  — tokens de name e description
    data/processed/tokens_reviews.jsonl   — tokens de review_body

Estrutura de cada linha nos arquivos de saída (.jsonl):
    {
        "sku": "21",
        "field": "description",          # campo de origem do texto
        "text": "texto original limpo",  # texto que foi tokenizado
        "tokens": [
            {
                "text":    "whisky",     # forma original no texto
                "lemma":   "whisky",     # forma lematizada
                "pos":     "NOUN",       # part-of-speech (Universal Dependencies)
                "tag":     "NOUN",       # tag morfológica detalhada do modelo
                "is_stop": false,        # é stopword?
                "is_punct": false,       # é pontuação?
                "is_alpha": true,        # contém apenas letras?
                "shape":   "xxxx"        # padrão ortográfico (ex: Xxxx, dddd)
            },
            ...
        ]
    }

Para reviews, a chave "id" substitui "field":
    { "sku": "21", "id": 1, "text": "...", "tokens": [...] }

Instalação:
    pip install spacy
    python -m spacy download pt_core_news_lg

Uso:
    python tokenizar_produtos.py
"""

import csv
import json
import logging
import sys
from pathlib import Path
from typing import Iterator

import spacy
from spacy.language import Language
from spacy.tokens import Doc

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

PROCESSED_DIR = Path("data/processed")

PRODUCTS_CSV = PROCESSED_DIR / "products.csv"
REVIEWS_CSV = PROCESSED_DIR / "reviews.csv"

TOKENS_PRODUCTS_JSONL = PROCESSED_DIR / "tokens_products.jsonl"
TOKENS_REVIEWS_JSONL = PROCESSED_DIR / "tokens_reviews.jsonl"

# Modelo spaCy para português — use pt_core_news_lg para melhor acurácia de
# lematização e POS; use pt_core_news_sm se memória for uma restrição.
SPACY_MODEL = "pt_core_news_lg"

# Campos de products.csv que serão tokenizados (em ordem de relevância para NLP)
PRODUCT_TEXT_FIELDS = ["name", "description"]

# Tamanho do lote para nlp.pipe() — aumentar melhora throughput em grandes volumes
BATCH_SIZE = 32

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Carregamento do modelo
# ---------------------------------------------------------------------------


def carregar_modelo(nome_modelo: str) -> Language:
    """Carrega o modelo spaCy desabilitando componentes desnecessários para
    tokenização (parser de dependências e NER), reduzindo uso de memória e
    aumentando velocidade de processamento.

    O tagger (POS) e o morphologizer são mantidos pois alimentam lemma e pos.
    """
    try:
        nlp = spacy.load(nome_modelo, exclude=["parser", "ner"])
        log.info("Modelo '%s' carregado.", nome_modelo)
        return nlp
    except OSError:
        log.error(
            "Modelo '%s' não encontrado. Execute:\n" "    python -m spacy download %s",
            nome_modelo,
            nome_modelo,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Leitura dos CSVs
# ---------------------------------------------------------------------------


def ler_products_csv(caminho: Path) -> list[dict]:
    """Lê products.csv retornando somente os campos necessários para NLP."""
    with open(caminho, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def ler_reviews_csv(caminho: Path) -> list[dict]:
    """Lê reviews.csv retornando somente os campos necessários para NLP."""
    with open(caminho, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# Serialização de tokens
# ---------------------------------------------------------------------------


def serializar_token(token) -> dict:
    """Converte um token spaCy num dict com os atributos relevantes para NLP."""
    return {
        "text": token.text,
        "lemma": token.lemma_.lower(),
        "pos": token.pos_,
        "tag": token.tag_,
        "is_stop": token.is_stop,
        "is_punct": token.is_punct,
        "is_alpha": token.is_alpha,
        "shape": token.shape_,
    }


def doc_para_tokens(doc: Doc) -> list[dict]:
    """Extrai todos os tokens de um Doc spaCy como lista de dicts."""
    return [serializar_token(t) for t in doc]


# ---------------------------------------------------------------------------
# Geração de registros a tokenizar (produtos)
# ---------------------------------------------------------------------------


def gerar_registros_produto(
    rows: list[dict],
    campos: list[str],
) -> Iterator[tuple[dict, str, str]]:
    """Itera sobre os produtos e campos de texto, gerando tuplas
    (row, campo, texto) somente quando o texto não estiver vazio."""
    for row in rows:
        for campo in campos:
            texto = row.get(campo, "").strip()
            if texto:
                yield row, campo, texto


# ---------------------------------------------------------------------------
# Pipeline de tokenização: produtos
# ---------------------------------------------------------------------------


def tokenizar_produtos(
    nlp: Language,
    rows: list[dict],
    caminho_saida: Path,
) -> int:
    """Tokeniza os campos de texto de products.csv e grava tokens_products.jsonl.

    Usa nlp.pipe() para processar em lotes, maximizando throughput.
    Retorna o número de registros gravados.
    """
    log.info(
        "Tokenizando produtos (%d linhas × %d campo(s))…",
        len(rows),
        len(PRODUCT_TEXT_FIELDS),
    )

    # Materializa pares (metadados, texto) para poder zipar com os docs
    pares: list[tuple[dict, str]] = []
    textos: list[str] = []

    for row, campo, texto in gerar_registros_produto(rows, PRODUCT_TEXT_FIELDS):
        pares.append((row, campo))
        textos.append(texto)

    gravados = 0
    with open(caminho_saida, "w", encoding="utf-8") as f_out:
        for (row, campo), doc in zip(
            pares,
            nlp.pipe(textos, batch_size=BATCH_SIZE),
        ):
            registro = {
                "sku": row["sku"],
                "field": campo,
                "text": doc.text,
                "tokens": doc_para_tokens(doc),
            }
            f_out.write(json.dumps(registro, ensure_ascii=False) + "\n")
            gravados += 1

    log.info("  tokens_products.jsonl: %d registro(s) gravado(s).", gravados)
    return gravados


# ---------------------------------------------------------------------------
# Pipeline de tokenização: reviews
# ---------------------------------------------------------------------------


def tokenizar_reviews(
    nlp: Language,
    rows: list[dict],
    caminho_saida: Path,
) -> int:
    """Tokeniza review_body de reviews.csv e grava tokens_reviews.jsonl.

    Reviews com review_body vazio são ignoradas com aviso.
    Retorna o número de registros gravados.
    """
    log.info("Tokenizando reviews (%d linhas)…", len(rows))

    # Filtra reviews sem texto e coleta metadados
    meta_validos: list[dict] = []
    textos: list[str] = []
    vazias = 0

    for row in rows:
        texto = row.get("review_body", "").strip()
        if not texto:
            vazias += 1
            continue
        meta_validos.append(row)
        textos.append(texto)

    if vazias:
        log.warning("  %d review(s) com review_body vazio ignorada(s).", vazias)

    gravados = 0
    with open(caminho_saida, "w", encoding="utf-8") as f_out:
        for row, doc in zip(
            meta_validos,
            nlp.pipe(textos, batch_size=BATCH_SIZE),
        ):
            registro = {
                "sku": row["sku"],
                "id": int(row["id"]),
                "text": doc.text,
                "tokens": doc_para_tokens(doc),
            }
            f_out.write(json.dumps(registro, ensure_ascii=False) + "\n")
            gravados += 1

    log.info("  tokens_reviews.jsonl: %d registro(s) gravado(s).", gravados)
    return gravados


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    # Validação de existência dos arquivos de entrada antes de qualquer trabalho
    for caminho in (PRODUCTS_CSV, REVIEWS_CSV):
        if not caminho.exists():
            log.error("Arquivo de entrada não encontrado: '%s'", caminho)
            sys.exit(1)

    nlp = carregar_modelo(SPACY_MODEL)

    log.info("Lendo arquivos de entrada…")
    products_rows = ler_products_csv(PRODUCTS_CSV)
    reviews_rows = ler_reviews_csv(REVIEWS_CSV)
    log.info(
        "  %d produto(s) | %d review(s) carregados.",
        len(products_rows),
        len(reviews_rows),
    )

    total_produtos = tokenizar_produtos(nlp, products_rows, TOKENS_PRODUCTS_JSONL)
    total_reviews = tokenizar_reviews(nlp, reviews_rows, TOKENS_REVIEWS_JSONL)

    log.info(
        "Tokenização concluída: %d registro(s) de produto(s), %d registro(s) de review(s).",
        total_produtos,
        total_reviews,
    )
    log.info("  → %s", TOKENS_PRODUCTS_JSONL)
    log.info("  → %s", TOKENS_REVIEWS_JSONL)


if __name__ == "__main__":
    main()
