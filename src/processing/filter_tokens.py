"""
Etapa 1 do pipeline de clusterização — Filtragem de tokens.

Lê data/processed/tokens_products.jsonl (somente leitura), aplica os filtros
linguísticos relevantes para clusterização por características de bebidas, e
salva o resultado em:

    data/processed/tokens_products_filtered.jsonl

Estrutura de cada linha na saída:
    {
        "sku":    "21",
        "field":  "description",
        "lemmas": ["whisky", "joia", "mundo", "destilado", "envelhecido", ...]
    }

Apenas `lemma` é preservado — texto original e demais atributos dos tokens
são descartados pois a vetorização opera exclusivamente sobre formas lematizadas.

Filtros aplicados (todos devem ser verdadeiros para o token ser mantido):
    1. is_alpha  = True   → remove números, símbolos, URLs, pontuação mista
    2. is_punct  = False  → remove pontuação pura
    3. is_stop   = False  → remove stopwords (artigos, preposições, conjunções)
    4. pos in    NOUN, ADJ, VERB  → mantém apenas classes gramaticais com
                                    conteúdo semântico relevante para o domínio
    5. len(lemma) >= 3    → remove radicais residuais de 1-2 caracteres

Uso:
    python filtrar_tokens.py
"""

import json
import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

PROCESSED_DIR = Path("data/processed")

TOKENS_PRODUCTS_JSONL = PROCESSED_DIR / "tokens_products.jsonl"
FILTERED_PRODUCTS_JSONL = PROCESSED_DIR / "tokens_products_filtered.jsonl"

# Classes gramaticais mantidas para clusterização por características
# NOUN → "amadeirado", "carvalho", "aroma", "destilado"
# ADJ  → "encorpado", "seco", "frutado", "suave"
# VERB → "envelhecer", "destilar" (infinitivos lematizados úteis em descriptions)
# ADV  → "levemente", "meio", "muito"
# POS_PERMITIDOS: frozenset[str] = frozenset({"NOUN", "ADJ", "VERB", "ADV"})
POS_PERMITIDOS: frozenset[str] = frozenset({"NOUN", "ADJ", "VERB"})

TEXT_LEMMA = "lemma"  # lemma

# Tamanho mínimo do lemma (remove resíduos como "ml", "é", "á")
TAMANHO_MINIMO_LEMMA = 3

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
# Leitura do JSONL de tokens
# ---------------------------------------------------------------------------


def carregar_tokens(caminho: Path) -> list[dict]:
    """Carrega todas as linhas do arquivo de tokens em memória."""
    registros = []
    with open(caminho, encoding="utf-8") as f:
        for numero, linha in enumerate(f, start=1):
            linha = linha.strip()
            if not linha:
                continue
            try:
                registros.append(json.loads(linha))
            except json.JSONDecodeError as exc:
                log.warning("Linha %d ignorada — JSON inválido: %s", numero, exc)
    return registros


# ---------------------------------------------------------------------------
# Filtro de tokens
# ---------------------------------------------------------------------------


def token_e_relevante(token: dict) -> bool:
    """Retorna True se o token deve ser mantido após todos os filtros."""
    return (
        (token.get("is_alpha") or False)  # apenas tokens alfabéticos
        and not token.get("is_punct")  # sem pontuação
        and not token.get("is_stop")  # sem stopwords
        and token.get("pos") in POS_PERMITIDOS  # POS relevante para o domínio
        and len(token.get(TEXT_LEMMA, "")) >= TAMANHO_MINIMO_LEMMA  # lemma mínimo
    )


def filtrar_tokens_registro(registro: dict) -> list[str]:
    """Aplica os filtros a todos os tokens de um registro e retorna
    somente os lemmas aprovados, em letras minúsculas."""
    return [
        token[TEXT_LEMMA].lower()
        for token in registro.get("tokens", [])
        if token_e_relevante(token)
    ]


# ---------------------------------------------------------------------------
# Agregação por SKU
# ---------------------------------------------------------------------------


def agregar_por_sku(registros: list[dict]) -> dict[str, list[str]]:
    """Agrega os lemmas filtrados de todos os campos (name + description)
    de um mesmo SKU em uma única lista.

    A ordem é preservada: lemmas de 'name' vêm antes dos de 'description',
    refletindo a relevância decrescente dos campos.
    """
    agregado: dict[str, list[str]] = {}
    for registro in registros:
        sku = registro["sku"]
        lemmas = filtrar_tokens_registro(registro)
        if sku not in agregado:
            agregado[sku] = []
        agregado[sku].extend(lemmas)
    return agregado


# ---------------------------------------------------------------------------
# Gravação do JSONL filtrado
# ---------------------------------------------------------------------------


def gravar_filtrado(
    agregado: dict[str, list[str]],
    caminho_saida: Path,
) -> int:
    """Grava o arquivo JSONL filtrado — uma linha por SKU.

    Retorna o número de registros gravados.
    """
    gravados = 0
    with open(caminho_saida, "w", encoding="utf-8") as f:
        for sku, lemmas in agregado.items():
            if not lemmas:
                log.warning("SKU %s ficou sem lemmas após filtragem — ignorado.", sku)
                continue
            registro = {"sku": sku, "lemmas": lemmas}
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")
            gravados += 1
    return gravados


# ---------------------------------------------------------------------------
# Diagnóstico
# ---------------------------------------------------------------------------


def logar_diagnostico(agregado: dict[str, list[str]]) -> None:
    """Loga estatísticas da filtragem para inspeção antes de prosseguir."""
    contagens = [len(lemmas) for lemmas in agregado.values()]
    if not contagens:
        return
    log.info(
        "  Lemmas por SKU — mín: %d | máx: %d | média: %.1f",
        min(contagens),
        max(contagens),
        sum(contagens) / len(contagens),
    )
    # Amostra: SKU com mais lemmas (produto mais descrito)
    sku_mais_rico = max(agregado, key=lambda s: len(agregado[s]))
    amostra = agregado[sku_mais_rico][:15]
    log.info(
        "  SKU mais rico: %s (%d lemmas) — amostra: %s",
        sku_mais_rico,
        len(agregado[sku_mais_rico]),
        amostra,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    if not TOKENS_PRODUCTS_JSONL.exists():
        log.error("Arquivo de entrada não encontrado: '%s'", TOKENS_PRODUCTS_JSONL)
        sys.exit(1)

    log.info("Carregando tokens de '%s'…", TOKENS_PRODUCTS_JSONL.name)
    registros = carregar_tokens(TOKENS_PRODUCTS_JSONL)
    log.info("  %d registro(s) carregado(s).", len(registros))

    log.info("Aplicando filtros e agregando por SKU…")
    agregado = agregar_por_sku(registros)
    log.info("  %d SKU(s) únicos após agregação.", len(agregado))
    logar_diagnostico(agregado)

    log.info("Gravando '%s'…", FILTERED_PRODUCTS_JSONL.name)
    gravados = gravar_filtrado(agregado, FILTERED_PRODUCTS_JSONL)
    log.info("  %d SKU(s) gravado(s).", gravados)

    log.info("Filtragem concluída → %s", FILTERED_PRODUCTS_JSONL)


if __name__ == "__main__":
    main()
