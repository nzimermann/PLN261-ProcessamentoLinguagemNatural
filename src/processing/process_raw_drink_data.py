"""
Pipeline de transformação e exportação de produtos — Casa da Bebida.

Lê os arquivos .jsonl de data/raw/extracted_products/, aplica limpeza e
validação em cada produto e suas reviews, e exporta dois CSVs normalizados:

    data/processed/products.csv   — uma linha por produto
    data/processed/reviews.csv    — uma linha por review (FK: sku)

Estratégia de execução: arquivo por arquivo, gravação somente após validação
completa do lote.

Uso:
    python transformar_produtos.py

Dependências: somente biblioteca padrão do Python (csv, json, html, pathlib, re).
"""

import csv
import html
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

EXTRACTED_PRODUCTS_DIR = Path("data/raw/extracted_products")
PROCESSED_DIR = Path("data/processed")

PRODUCTS_CSV = PROCESSED_DIR / "products.csv"
REVIEWS_CSV = PROCESSED_DIR / "reviews.csv"

CATEGORIAS: list[str] = [
    "cachaca",
    "conhaque",
    "coquetel",
    "espumante",
    "licor",
    "rum",
    "sake",
    "whisky",
    "gin",
    "tequila",
    "vinho",
    "champagne",
    "vodka",
]

PRODUCTS_FIELDNAMES: list[str] = [
    "sku",
    "category",
    "name",
    "brand",
    "description",
    "url",
    "price",
    "in_stock",
    "image_primary",
    "image_count",
    "rating_value",
    "review_count",
]

REVIEWS_FIELDNAMES: list[str] = [
    "sku",
    "rating",
    "author",
    "review_body",
]

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
# Limpeza de texto
# ---------------------------------------------------------------------------

def limpar_texto(texto: str) -> str:
    """Remove whitespace redundante e entidades HTML de uma string.

    Passos aplicados em ordem:
      1. Decodifica entidades HTML (&amp; → &, &nbsp; → espaço, etc.)
      2. Substitui sequências de whitespace (\\r, \\n, \\t, espaços múltiplos)
         por um único espaço.
      3. Remove espaços nas bordas (strip).
    """
    if not isinstance(texto, str):
        return ""
    texto = html.unescape(texto)
    texto = re.sub(r"[\r\n\t]+", " ", texto)
    texto = re.sub(r" {2,}", " ", texto)
    return texto.strip()


def limpar_texto_review(texto: str) -> str:
    """Limpeza de review_body: mesmo que limpar_texto com strip agressivo."""
    return limpar_texto(texto)


# ---------------------------------------------------------------------------
# Validação de campos obrigatórios
# ---------------------------------------------------------------------------

class ErroValidacao(Exception):
    """Lançada quando um campo obrigatório está ausente ou com tipo inválido."""


def exigir_campo(dado: dict, campo: str, tipo: type, contexto: str) -> Any:
    """Retorna o valor de `campo` em `dado` validando tipo. Lança ErroValidacao
    se o campo estiver ausente, None ou com tipo incorreto."""
    valor = dado.get(campo)
    if valor is None:
        raise ErroValidacao(f"[{contexto}] Campo obrigatório ausente: '{campo}'")
    if not isinstance(valor, tipo):
        raise ErroValidacao(
            f"[{contexto}] Campo '{campo}' esperava {tipo.__name__}, "
            f"recebeu {type(valor).__name__}: {valor!r}"
        )
    return valor


# ---------------------------------------------------------------------------
# Transformação de produto
# ---------------------------------------------------------------------------

def transformar_produto(raw: dict, categoria: str) -> dict:
    """Converte um objeto ld+json bruto no dict limpo para products.csv.

    Lança ErroValidacao se qualquer campo obrigatório estiver ausente.
    """
    ctx = f"SKU {raw.get('sku', '?')}"

    sku: str = str(exigir_campo(raw, "sku", str, ctx))
    name: str = limpar_texto(exigir_campo(raw, "name", str, ctx))

    # brand (opcional — incluído como string vazia quando ausente)
    brand_obj = raw.get("brand")
    brand_raw = brand_obj.get("name") if isinstance(brand_obj, dict) else None
    brand: str = limpar_texto(brand_raw) if brand_raw else ""

    # description
    description: str = limpar_texto(exigir_campo(raw, "description", str, ctx))

    # offers
    offers = raw.get("offers")
    if not isinstance(offers, dict):
        raise ErroValidacao(f"[{ctx}] Campo 'offers' ausente ou inválido.")

    url: str = exigir_campo(offers, "url", str, f"{ctx} > offers")

    price_raw = offers.get("price")
    if price_raw is None:
        raise ErroValidacao(f"[{ctx}] Campo 'offers.price' ausente.")
    try:
        price: float = float(price_raw)
    except (TypeError, ValueError):
        raise ErroValidacao(f"[{ctx}] 'offers.price' não conversível para float: {price_raw!r}")

    availability: str = offers.get("availability", "")
    in_stock: bool = availability.endswith("InStock")

    # images
    images = raw.get("image", [])
    if isinstance(images, str):
        images = [images]
    image_primary: str = images[0] if images else ""
    image_count: int = len(images)

    # aggregateRating (opcional)
    agg = raw.get("aggregateRating")
    if isinstance(agg, dict):
        rating_value_raw = agg.get("ratingValue")
        review_count_raw = agg.get("reviewCount")
        try:
            rating_value: float | None = round(float(rating_value_raw), 2) if rating_value_raw is not None else None
            review_count: int = int(review_count_raw) if review_count_raw is not None else 0
        except (TypeError, ValueError):
            rating_value = None
            review_count = 0
    else:
        rating_value = None
        review_count = 0

    return {
        "sku": sku,
        "category": categoria,
        "name": name,
        "brand": brand,
        "description": description,
        "url": url,
        "price": price,
        "in_stock": in_stock,
        "image_primary": image_primary,
        "image_count": image_count,
        "rating_value": rating_value if rating_value is not None else "",
        "review_count": review_count,
    }


# ---------------------------------------------------------------------------
# Transformação de reviews
# ---------------------------------------------------------------------------

def transformar_reviews(raw: dict) -> list[dict]:
    """Extrai e limpa todas as reviews de um produto bruto.

    Reviews com campos obrigatórios ausentes são ignoradas individualmente
    com log de aviso (não abortam o produto inteiro).
    """
    sku: str = str(raw.get("sku", "?"))
    reviews_raw: list = raw.get("review", [])

    if not isinstance(reviews_raw, list):
        return []

    reviews_limpos: list[dict] = []

    for i, r in enumerate(reviews_raw):
        posicao = f"SKU {sku} review[{i}]"
        try:
            rating_raw = r.get("reviewRating", {}).get("ratingValue")
            if rating_raw is None:
                raise ErroValidacao(f"[{posicao}] 'reviewRating.ratingValue' ausente.")
            rating: int = int(float(rating_raw))

            author_raw = r.get("author", {}).get("name")
            if not author_raw:
                raise ErroValidacao(f"[{posicao}] 'author.name' ausente.")
            author: str = limpar_texto(author_raw)

            body_raw = r.get("reviewBody")
            if body_raw is None:
                raise ErroValidacao(f"[{posicao}] 'reviewBody' ausente.")
            review_body: str = limpar_texto_review(body_raw)

        except ErroValidacao as exc:
            log.warning("Review ignorada — %s", exc)
            continue

        reviews_limpos.append({
            "sku": sku,
            "rating": rating,
            "author": author,
            "review_body": review_body,
        })

    return reviews_limpos


# ---------------------------------------------------------------------------
# I/O: leitura do .jsonl
# ---------------------------------------------------------------------------

def carregar_jsonl(caminho: Path) -> list[dict]:
    """Carrega todas as linhas de um arquivo .jsonl em uma lista de dicts.

    Linhas vazias são ignoradas. Linhas com JSON inválido interrompem o
    carregamento do arquivo com log de erro.
    """
    produtos: list[dict] = []
    with open(caminho, encoding="utf-8") as f:
        for numero, linha in enumerate(f, start=1):
            linha = linha.strip()
            if not linha:
                continue
            try:
                produtos.append(json.loads(linha))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"JSON inválido na linha {numero} de '{caminho.name}': {exc}"
                ) from exc
    return produtos


# ---------------------------------------------------------------------------
# I/O: gravação dos CSVs
# ---------------------------------------------------------------------------

def gravar_csv(
    caminho: Path,
    fieldnames: list[str],
    linhas: list[dict],
    modo_append: bool,
) -> None:
    """Grava `linhas` no CSV `caminho`.

    Se `modo_append` for True, abre em modo 'a' (sem reescrever o cabeçalho).
    Caso contrário, abre em modo 'w' (cria o arquivo e escreve o cabeçalho).
    """
    modo = "a" if modo_append else "w"
    escrever_cabecalho = not modo_append

    with open(caminho, mode=modo, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if escrever_cabecalho:
            writer.writeheader()
        writer.writerows(linhas)


# ---------------------------------------------------------------------------
# Processamento de um arquivo .jsonl completo (um lote / uma categoria)
# ---------------------------------------------------------------------------

def processar_arquivo(
    caminho_jsonl: Path,
    categoria: str,
    primeiro_arquivo: bool,
) -> tuple[int, int]:
    """Executa o pipeline completo para um arquivo .jsonl:

    1. Carrega todas as linhas em memória.
    2. Transforma e valida cada produto → products_batch.
    3. Transforma e valida cada review → reviews_batch.
    4. Somente após o lote inteiro ser processado sem erros críticos:
         → Grava products_batch em products.csv (append ou criação).
         → Grava reviews_batch em reviews.csv (append ou criação).

    Produtos que falham na validação são ignorados individualmente com log;
    reviews inválidas também são ignoradas individualmente.

    Retorna (produtos_gravados, reviews_gravadas).
    """
    log.info("Processando '%s' (categoria: %s)", caminho_jsonl.name, categoria)

    # Etapa 1: carga
    raws = carregar_jsonl(caminho_jsonl)
    log.info("  %d linha(s) carregada(s).", len(raws))

    # Etapas 2 e 3: transformação e validação acumulando os dois lotes
    products_batch: list[dict] = []
    reviews_batch: list[dict] = []
    skus_vistos: set[str] = set()

    for raw in raws:
        sku = str(raw.get("sku", ""))

        # Deduplicação dentro do arquivo
        if sku in skus_vistos:
            log.warning("  SKU duplicado ignorado dentro do arquivo: %s", sku)
            continue
        skus_vistos.add(sku)

        # Transformação do produto
        try:
            produto = transformar_produto(raw, categoria)
        except ErroValidacao as exc:
            log.error("  Produto ignorado — %s", exc)
            continue

        # Transformação das reviews (falhas individuais já logadas internamente)
        reviews = transformar_reviews(raw)

        products_batch.append(produto)
        reviews_batch.extend(reviews)

    if not products_batch:
        log.warning("  Nenhum produto válido em '%s'. Arquivo ignorado.", caminho_jsonl.name)
        return 0, 0

    # Etapa 4: gravação atômica do lote — somente após validação completa
    modo_append = not primeiro_arquivo
    gravar_csv(PRODUCTS_CSV, PRODUCTS_FIELDNAMES, products_batch, modo_append)
    gravar_csv(REVIEWS_CSV, REVIEWS_FIELDNAMES, reviews_batch, modo_append)

    log.info(
        "  Gravados: %d produto(s), %d review(s).",
        len(products_batch),
        len(reviews_batch),
    )
    return len(products_batch), len(reviews_batch)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    total_produtos = 0
    total_reviews = 0
    arquivos_processados = 0
    primeiro_arquivo = True

    for categoria in CATEGORIAS:
        caminho = EXTRACTED_PRODUCTS_DIR / f"{categoria}_products.jsonl"

        if not caminho.exists():
            log.warning("Arquivo não encontrado, pulando: '%s'", caminho)
            continue

        try:
            produtos, reviews = processar_arquivo(caminho, categoria, primeiro_arquivo)
        except (ValueError, OSError) as exc:
            log.error("Falha crítica ao processar '%s': %s — arquivo ignorado.", caminho.name, exc)
            continue

        if produtos > 0:
            primeiro_arquivo = False
            arquivos_processados += 1
            total_produtos += produtos
            total_reviews += reviews

    log.info(
        "Pipeline concluído: %d arquivo(s), %d produto(s), %d review(s).",
        arquivos_processados,
        total_produtos,
        total_reviews,
    )
    if total_produtos > 0:
        log.info("  → %s", PRODUCTS_CSV)
        log.info("  → %s", REVIEWS_CSV)


if __name__ == "__main__":
    main()
