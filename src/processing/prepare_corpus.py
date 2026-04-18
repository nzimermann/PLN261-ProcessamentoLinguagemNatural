"""
Preparo de corpus — pipeline de clusterização.

Lê data/processed/tokens_products.jsonl e produz três artefatos:

    data/processed/corpus_skus.json          — lista master de SKUs (ordem canônica)
    data/processed/corpus_bow_tfidf.jsonl    — corpus com filtros pesados para BoW/TF-IDF
    data/processed/corpus_w2v.jsonl          — corpus com filtros leves para Word2Vec

Uso:
    python preparar_corpus.py
"""

import json
import logging
import sys
from pathlib import Path
from src.config import DATA_DIR

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

PROCESSED_DIR = DATA_DIR / "processed"
TOKENS_JSONL = PROCESSED_DIR / "tokens" / "tokens_products.jsonl"
CORPUS_SKUS_JSON = PROCESSED_DIR / "corpus" / "corpus_skus.json"
CORPUS_BOW_TFIDF_JSONL = PROCESSED_DIR / "corpus" / "corpus_bow_tfidf.jsonl"
CORPUS_W2V_JSONL = PROCESSED_DIR / "corpus" / "corpus_w2v.jsonl"

# Classes gramaticais com conteúdo semântico relevante para o domínio
POS_SEMANTICOS: frozenset[str] = frozenset({"NOUN", "VERB", "ADV"})

# Comprimento mínimo de token para eliminar resíduos curtos ("ml", "ex", "á")
TAMANHO_MINIMO_TOKEN = 3

# Termos de boilerplate estrutural do e-commerce que escapam dos filtros
# automáticos (não são stopwords do spaCy, mas não carregam valor semântico
# para clusterização de características de bebidas)
STOPLIST_DOMINIO: frozenset[str] = frozenset(
    {
        "característica",
        "características",
        "volume",
        "graduação",
        "alcoólica",
        "alcoólico",
        "álcool",
        "origem",
        "país",
        "região",
        "importado",
        "qualidade",
        "método",
        "informações",
        "técnicas",
        "ficha",
        "descrição",
        "produto",
        "técnica",
        "produção",
        "curiosidade",
        "história",
        "kit",
        "unidade",
        "comprar",
        "entrega",
        "embalagem",
        "anos",
        "litros",
        "garrafa",
        "ano",
        "loja",
        "preço",
        "ml",
        "sabor",
        "nota",
        "teor",
        "bebida",
    }
)

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
# Leitura
# ---------------------------------------------------------------------------


def carregar_tokens(caminho: Path) -> list[dict]:
    """Carrega todos os registros de tokens do JSONL em memória.

    Lança ValueError em caso de JSON malformado para evitar
    propagação silenciosa de dados corrompidos.
    """
    registros: list[dict] = []
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, encoding="utf-8") as f:
        for numero, linha in enumerate(f, start=1):
            linha = linha.strip()
            if not linha:
                continue
            try:
                registros.append(json.loads(linha))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"JSON inválido na linha {numero} de '{caminho.name}': {exc}"
                ) from exc
    return registros


# ---------------------------------------------------------------------------
# Filtros individuais de token
# ---------------------------------------------------------------------------


def _e_alfabetico(token: dict) -> bool:
    return bool(token.get("is_alpha"))


def _nao_e_pontuacao(token: dict) -> bool:
    return not token.get("is_punct")


def _nao_e_stopword(token: dict) -> bool:
    return not token.get("is_stop")


def _pos_e_semantico(token: dict) -> bool:
    return token.get("pos") in POS_SEMANTICOS


def _tamanho_suficiente(valor: str) -> bool:
    return len(valor) >= TAMANHO_MINIMO_TOKEN


def _nao_esta_na_stoplist(valor: str) -> bool:
    return valor.lower() not in STOPLIST_DOMINIO


# ---------------------------------------------------------------------------
# Estratégias de filtragem
# ---------------------------------------------------------------------------


def aplicar_filtros_pesados(tokens: list[dict]) -> list[tuple[str, str]]:
    """Retorna pares (lemma, text) filtrados para uso em BoW e TF-IDF.

    Ambas as formas são preservadas para permitir parametrizar nos scripts
    de vetorização qual delas usar (FORMA_VETORIZACAO = "lemmas" | "textos").

    Filtros aplicados (todos devem ser verdadeiros):
        - token alfabético
        - não é pontuação
        - não é stopword
        - POS semântico (NOUN, VERB, ADV)
        - lemma com tamanho mínimo (proxy para ambas as formas)
        - lemma fora da stoplist de domínio
    """
    resultado: list[tuple[str, str]] = []
    for token in tokens:
        if not _e_alfabetico(token):
            continue
        if not _nao_e_pontuacao(token):
            continue
        if not _nao_e_stopword(token):
            continue
        if not _pos_e_semantico(token):
            continue

        lemma = token.get("lemma", "").lower()
        if not _tamanho_suficiente(lemma):
            continue
        if not _nao_esta_na_stoplist(lemma):
            continue

        texto = token.get("text", "").lower()
        if not _nao_esta_na_stoplist(texto):
            continue

        resultado.append((lemma, texto))
    return resultado


def aplicar_filtros_leves(tokens: list[dict]) -> list[str]:
    """Retorna tokens (forma original) filtrados para uso em Word2Vec.

    Word2Vec aprende de contexto — stopwords e termos genéricos são mantidos
    intencionalmente para preservar a integridade das janelas de co-ocorrência.
    A ordem dos tokens é preservada pelo mesmo motivo.

    Filtros aplicados:
        - token alfabético
        - não é pontuação
        - texto com tamanho mínimo
    """
    resultado: list[str] = []
    for token in tokens:
        if not _e_alfabetico(token):
            continue
        if not _nao_e_pontuacao(token):
            continue

        texto = token.get("text", "").lower()
        if not _tamanho_suficiente(texto):
            continue

        resultado.append(texto)
    return resultado


# ---------------------------------------------------------------------------
# Transformação de registros
# ---------------------------------------------------------------------------


def extrair_tokens_de_registro(registro: dict) -> list[dict]:
    """Retorna a lista de tokens de um registro de tokens_products.jsonl."""
    return registro.get("tokens", [])


def construir_registro_corpus_w2v(sku: str, termos: list[str]) -> dict:
    """Monta o registro de saída para o corpus Word2Vec."""
    return {"sku": sku, "termos": termos}


def construir_registro_corpus_bow_tfidf(
    sku: str,
    pares: list[tuple[str, str]],
) -> dict:
    """Monta o registro de saída para o corpus BoW/TF-IDF.

    Persiste lemmas e textos separadamente para que os scripts de
    vetorização possam escolher qual forma usar via FORMA_VETORIZACAO.
    """
    lemmas = [par[0] for par in pares]
    textos = [par[1] for par in pares]
    return {"sku": sku, "lemmas": lemmas, "textos": textos}


# ---------------------------------------------------------------------------
# Agregação por SKU
# ---------------------------------------------------------------------------


def agregar_corpus_w2v_por_sku(registros: list[dict]) -> dict[str, list[str]]:
    """Agrega tokens (forma original) por SKU para o corpus Word2Vec.

    Aplica filtros leves e preserva a ordem dos tokens — necessária para
    a janela de co-ocorrência do Word2Vec.
    """
    agregado: dict[str, list[str]] = {}

    for registro in registros:
        sku: str = registro.get("sku", "")
        tokens = extrair_tokens_de_registro(registro)
        termos = aplicar_filtros_leves(tokens)

        if sku not in agregado:
            agregado[sku] = []
        agregado[sku].extend(termos)

    vazios = [sku for sku, termos in agregado.items() if not termos]
    for sku in vazios:
        log.warning("SKU %s sem termos (w2v) — será omitido do corpus.", sku)

    return {sku: t for sku, t in agregado.items() if t}


def agregar_corpus_bow_tfidf_por_sku(
    registros: list[dict],
) -> dict[str, list[tuple[str, str]]]:
    """Agrega pares (lemma, text) por SKU para o corpus BoW/TF-IDF.

    Aplica filtros pesados e acumula ambas as formas de cada token,
    permitindo que o script de vetorização escolha qual usar.
    """
    agregado: dict[str, list[tuple[str, str]]] = {}

    for registro in registros:
        sku: str = registro.get("sku", "")
        tokens = extrair_tokens_de_registro(registro)
        pares = aplicar_filtros_pesados(tokens)

        if sku not in agregado:
            agregado[sku] = []
        agregado[sku].extend(pares)

    vazios = [sku for sku, pares in agregado.items() if not pares]
    for sku in vazios:
        log.warning("SKU %s sem pares (bow/tfidf) — será omitido do corpus.", sku)

    return {sku: p for sku, p in agregado.items() if p}


# ---------------------------------------------------------------------------
# Validação de consistência entre corpus
# ---------------------------------------------------------------------------


def validar_consistencia_de_skus(
    skus_bow_tfidf: set[str],
    skus_w2v: set[str],
) -> None:
    """Verifica se os dois corpus cobrem exatamente os mesmos SKUs.

    Diferenças indicam que um produto ficou sem termos em uma das estratégias
    de filtragem, o que tornaria a comparação entre métodos inválida para
    aquele produto.
    """
    apenas_bow = skus_bow_tfidf - skus_w2v
    apenas_w2v = skus_w2v - skus_bow_tfidf

    if apenas_bow:
        log.warning(
            "%d SKU(s) presentes em bow/tfidf mas ausentes em w2v: %s",
            len(apenas_bow),
            sorted(apenas_bow)[:10],
        )
    if apenas_w2v:
        log.warning(
            "%d SKU(s) presentes em w2v mas ausentes em bow/tfidf: %s",
            len(apenas_w2v),
            sorted(apenas_w2v)[:10],
        )
    if not apenas_bow and not apenas_w2v:
        log.info("  Consistência validada: os dois corpus cobrem os mesmos SKUs.")


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------


def salvar_skus(skus: list[str], caminho: Path) -> None:
    """Persiste a lista master de SKUs em JSON."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(skus, f, ensure_ascii=False)
    log.info("corpus_skus.json salvo: %d SKUs → %s", len(skus), caminho)


def _gravar_jsonl(registros_iter, caminho: Path) -> int:
    """Grava um iterável de dicts como JSONL. Retorna o total gravado."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    gravados = 0
    with open(caminho, "w", encoding="utf-8") as f:
        for registro in registros_iter:
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")
            gravados += 1
    return gravados


def salvar_corpus_w2v_jsonl(
    agregado: dict[str, list[str]],
    skus_ordenados: list[str],
    caminho: Path,
) -> None:
    """Persiste o corpus Word2Vec como JSONL na ordem canônica dos SKUs."""
    registros = (
        construir_registro_corpus_w2v(sku, agregado[sku])
        for sku in skus_ordenados
        if sku in agregado
    )
    gravados = _gravar_jsonl(registros, caminho)
    log.info("%s salvo: %d registros → %s", caminho.name, gravados, caminho)


def salvar_corpus_bow_tfidf_jsonl(
    agregado: dict[str, list[tuple[str, str]]],
    skus_ordenados: list[str],
    caminho: Path,
) -> None:
    """Persiste o corpus BoW/TF-IDF como JSONL na ordem canônica dos SKUs.

    Cada linha contém os campos "lemmas" e "textos" separados para
    permitir parametrização nos scripts de vetorização.
    """
    registros = (
        construir_registro_corpus_bow_tfidf(sku, agregado[sku])
        for sku in skus_ordenados
        if sku in agregado
    )
    gravados = _gravar_jsonl(registros, caminho)
    log.info("%s salvo: %d registros → %s", caminho.name, gravados, caminho)


# ---------------------------------------------------------------------------
# Diagnóstico
# ---------------------------------------------------------------------------


def _comprimento_entrada(entrada) -> int:
    """Retorna o número de itens de uma entrada do agregado.

    Suporta tanto list[str] (W2V) quanto list[tuple[str, str]] (BoW/TF-IDF).
    """
    return len(entrada)


def logar_estatisticas(nome: str, agregado: dict) -> None:
    """Imprime estatísticas descritivas do corpus para inspeção.

    Compatível com dict[str, list[str]] (W2V) e
    dict[str, list[tuple[str, str]]] (BoW/TF-IDF).
    """
    contagens = [_comprimento_entrada(v) for v in agregado.values()]
    if not contagens:
        log.warning("%s: corpus vazio.", nome)
        return
    amostra_sku = max(agregado, key=lambda s: _comprimento_entrada(agregado[s]))
    log.info(
        "  %s — SKUs: %d | itens/doc: mín %d · méd %.0f · máx %d",
        nome,
        len(contagens),
        min(contagens),
        sum(contagens) / len(contagens),
        max(contagens),
    )
    amostra = agregado[amostra_sku][:6]
    log.info(
        "  %s — SKU mais rico: %s (%d itens) | amostra: %s",
        nome,
        amostra_sku,
        _comprimento_entrada(agregado[amostra_sku]),
        amostra,
    )


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------


def derivar_skus_canonicos(
    bow_tfidf: dict,
    w2v: dict,
) -> list[str]:
    """Retorna a lista de SKUs presentes em ambos os corpus, em ordem estável.

    Apenas SKUs comuns aos dois corpus entram na lista master — garante que
    todos os vetorizadores produzirão uma linha para cada SKU listado.
    """
    skus_comuns = sorted(set(bow_tfidf.keys()) & set(w2v.keys()))
    descartados = (len(bow_tfidf) + len(w2v)) // 2 - len(skus_comuns)
    if descartados > 0:
        log.warning("%d SKU(s) descartados por ausência em um dos corpus.", descartados)
    return skus_comuns


def main() -> None:
    if not TOKENS_JSONL.exists():
        log.error("Arquivo de entrada não encontrado: '%s'", TOKENS_JSONL)
        sys.exit(1)

    log.info("Carregando '%s'…", TOKENS_JSONL.name)
    registros = carregar_tokens(TOKENS_JSONL)
    log.info("  %d registros carregados.", len(registros))

    log.info("Agregando corpus BoW/TF-IDF (filtros pesados · lemma + text)…")
    corpus_bow_tfidf = agregar_corpus_bow_tfidf_por_sku(registros)
    logar_estatisticas("bow_tfidf", corpus_bow_tfidf)

    log.info("Agregando corpus Word2Vec (filtros leves · text · ordem)…")
    corpus_w2v = agregar_corpus_w2v_por_sku(registros)
    logar_estatisticas("w2v", corpus_w2v)

    log.info("Validando consistência entre corpus…")
    validar_consistencia_de_skus(set(corpus_bow_tfidf), set(corpus_w2v))

    skus_canonicos = derivar_skus_canonicos(corpus_bow_tfidf, corpus_w2v)
    log.info("  %d SKUs na lista canônica.", len(skus_canonicos))

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    salvar_skus(skus_canonicos, CORPUS_SKUS_JSON)
    salvar_corpus_bow_tfidf_jsonl(
        corpus_bow_tfidf, skus_canonicos, CORPUS_BOW_TFIDF_JSONL
    )
    salvar_corpus_w2v_jsonl(corpus_w2v, skus_canonicos, CORPUS_W2V_JSONL)

    log.info("Preparo de corpus concluído.")


if __name__ == "__main__":
    main()
