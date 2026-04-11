"""
Vetorização TF-IDF — pipeline de clusterização.

Lê data/processed/corpus/corpus_bow_tfidf.jsonl e transforma os termos
de cada produto em vetores ponderados usando TF-IDF (TfidfVectorizer).

TF (Term Frequency): frequência do termo no documento.
IDF (Inverse Document Frequency): penaliza termos comuns em muitos documentos,
amplifica termos raros e discriminativos.

Saída:
    data/processed/vectors/tfidf_matrix.npz     — matriz esparsa TF-IDF
    data/processed/vectors/tfidf_skus.json      — SKUs na ordem das linhas
    data/processed/vectors/tfidf_features.json  — vocabulário na ordem das colunas

Uso:
    pip install scikit-learn scipy numpy
    python vetorizar_tfidf.py
"""

import json
import logging
import sys
from pathlib import Path

import numpy as np
from scipy.sparse import csr_matrix, save_npz
from sklearn.feature_extraction.text import TfidfVectorizer

# ---------------------------------------------------------------------------
# Configuração — ajuste aqui para experimentar diferentes abordagens
# ---------------------------------------------------------------------------

CORPUS_DIR = Path("data/processed/corpus")
VECTORS_TFIDF_DIR = Path("data/processed/vectors/tfidf")

CORPUS_JSONL = CORPUS_DIR / "corpus_bow_tfidf.jsonl"
OUTPUT_MATRIX = VECTORS_TFIDF_DIR / "tfidf_matrix.npz"
OUTPUT_SKUS = VECTORS_TFIDF_DIR / "tfidf_skus.json"
OUTPUT_FEATURES = VECTORS_TFIDF_DIR / "tfidf_features.json"

# Forma dos termos usada na vetorização:
#   USAR_LEMMAS = True,  USAR_TEXTOS = False → formas lematizadas
#   USAR_LEMMAS = False, USAR_TEXTOS = True  → formas originais  ← melhor resultado observado
#   USAR_LEMMAS = True,  USAR_TEXTOS = True  → intercalado (lemma_0, texto_0, lemma_1, ...)
USAR_LEMMAS: bool = True
USAR_TEXTOS: bool = False

# n-gramas: (1, 1) = apenas unigrams | (1, 2) = uni + bigrams
# Bigrams capturam modificadores de grau: "levemente seco", "meio amargo"
NGRAM_RANGE: tuple[int, int] = (1, 1)

# Ignora termos que aparecem em menos de MIN_DF documentos.
# Valor absoluto (int) ou proporção do corpus (float entre 0 e 1).
MIN_DF: int | float = 2

# Ignora termos que aparecem em mais de MAX_DF documentos.
# Reduzir abaixo de 0.90 elimina boilerplate que o IDF não penaliza suficientemente.
MAX_DF: int | float = 0.90

# Limite de features (dimensionalidade máxima do vocabulário).
# None = sem limite.
MAX_FEATURES: int | None = 5000

# Normalização dos vetores:
#   "l2" → norma euclideana (cada vetor tem módulo 1) — padrão recomendado
#   "l1" → soma dos valores absolutos = 1 (mais interpretável como distribuição)
#   None → sem normalização (mantém pesos TF-IDF brutos)
NORMALIZACAO: str | None = "l2"

# Suavização do IDF: evita divisão por zero para termos ausentes no corpus de treino.
# Recomendado manter True na maioria dos casos.
SUAVIZAR_IDF: bool = True

# Quantos produtos amostrar na inspeção por produto (Seção 4)
N_AMOSTRAS_INSPECAO: int = 5

# Quantas top features exibir por produto na inspeção
N_TOP_FEATURES: int = 10

# Seed para fixar a amostra entre execuções — os mesmos produtos
# são sempre exibidos, permitindo comparar resultados entre ajustes
SEED_AMOSTRA: int = 42

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
# Validação de configuração
# ---------------------------------------------------------------------------


def validar_configuracao() -> None:
    """Lança ValueError se a combinação de parâmetros for inválida."""
    if not USAR_LEMMAS and not USAR_TEXTOS:
        raise ValueError(
            "Configuração inválida: USAR_LEMMAS e USAR_TEXTOS não podem ser "
            "ambos False. Ative ao menos um dos campos."
        )
    if NORMALIZACAO not in ("l2", "l1", None):
        raise ValueError(
            f"NORMALIZACAO deve ser 'l2', 'l1' ou None. Recebido: {NORMALIZACAO!r}"
        )


# ---------------------------------------------------------------------------
# Leitura do corpus
# ---------------------------------------------------------------------------


def carregar_corpus(caminho: Path) -> list[dict]:
    """Carrega todos os registros do JSONL em memória.

    Lança ValueError em caso de JSON malformado para evitar
    propagação silenciosa de dados corrompidos.
    """
    registros: list[dict] = []
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
# Montagem dos documentos
# ---------------------------------------------------------------------------


def extrair_termos(registro: dict) -> list[str]:
    """Extrai os termos de um registro de acordo com USAR_LEMMAS e USAR_TEXTOS.

    Quando ambos estão ativos, os termos são intercalados por posição
    (lemma_0, texto_0, lemma_1, texto_1...) para que cada par de formas
    do mesmo token fique adjacente, beneficiando a geração de bigrams.
    """
    lemmas: list[str] = registro.get("lemmas", []) if USAR_LEMMAS else []
    textos: list[str] = registro.get("textos", []) if USAR_TEXTOS else []

    if USAR_LEMMAS and USAR_TEXTOS:
        comprimento = min(len(lemmas), len(textos))
        intercalados: list[str] = []
        for i in range(comprimento):
            intercalados.append(lemmas[i])
            intercalados.append(textos[i])
        return intercalados

    return lemmas or textos


def montar_documentos(registros: list[dict]) -> tuple[list[str], list[str]]:
    """Retorna (skus, documentos) a partir dos registros do corpus.

    Documentos são strings de termos separados por espaço — formato
    esperado pelo TfidfVectorizer. Registros sem termos são ignorados
    com aviso de log.
    """
    skus: list[str] = []
    documentos: list[str] = []

    for registro in registros:
        sku = registro.get("sku", "")
        termos = extrair_termos(registro)

        if not termos:
            log.warning("SKU %s sem termos após extração — ignorado.", sku)
            continue

        skus.append(sku)
        documentos.append(" ".join(termos))

    return skus, documentos


# ---------------------------------------------------------------------------
# Vocabulário
# ---------------------------------------------------------------------------


def construir_vocabulario(documentos: list[str]) -> TfidfVectorizer:
    """Instancia e treina o TfidfVectorizer com os parâmetros configurados.

    O fit é separado do transform para inspecionar o vocabulário
    antes de transformar os documentos.
    """
    vectorizer = TfidfVectorizer(
        ngram_range=NGRAM_RANGE,
        min_df=MIN_DF,  # type: ignore[arg-type]
        max_df=MAX_DF,
        max_features=MAX_FEATURES,
        norm=NORMALIZACAO,  # type: ignore[arg-type]
        smooth_idf=SUAVIZAR_IDF,
    )
    vectorizer.fit(documentos)
    return vectorizer


def separar_por_tipo_ngram(
    vocabulario: np.ndarray,
) -> tuple[list[str], list[str]]:
    """Separa o vocabulário em unigrams e bigrams para diagnóstico."""
    unigrams = [f for f in vocabulario if " " not in f]
    bigrams = [f for f in vocabulario if " " in f]
    return unigrams, bigrams


def calcular_idf_por_feature(
    vectorizer: TfidfVectorizer,
    vocabulario: np.ndarray,
    n: int = 10,
) -> tuple[list[tuple[str, float]], list[tuple[str, float]]]:
    """Retorna as N features com maior e menor IDF.

    IDF alto → termo raro, muito discriminativo.
    IDF baixo → termo comum, pouco discriminativo — candidato à stoplist.
    """
    idf_scores: np.ndarray = vectorizer.idf_
    top_idf_idx = idf_scores.argsort()[::-1][:n]
    bot_idf_idx = idf_scores.argsort()[:n]

    top = [(str(vocabulario[i]), round(float(idf_scores[i]), 4)) for i in top_idf_idx]
    bot = [(str(vocabulario[i]), round(float(idf_scores[i]), 4)) for i in bot_idf_idx]
    return top, bot


# ---------------------------------------------------------------------------
# Transformação
# ---------------------------------------------------------------------------


def transformar_documentos(
    vectorizer: TfidfVectorizer,
    documentos: list[str],
) -> csr_matrix:
    """Aplica o vocabulário treinado e produz a matriz TF-IDF."""
    matriz: csr_matrix = vectorizer.transform(documentos)  # type: ignore[assignment]
    return matriz


def calcular_esparsidade(matriz: csr_matrix) -> float:
    """Retorna a proporção de elementos zero na matriz."""
    shape = matriz.shape
    assert shape is not None and len(shape) == 2
    total = int(shape[0]) * int(shape[1])
    if total == 0:
        return 0.0
    return 1 - (int(matriz.nnz) / total)


# ---------------------------------------------------------------------------
# Inspeção por produto
# ---------------------------------------------------------------------------


def selecionar_amostra(skus: list[str], n: int, seed: int) -> list[int]:
    """Retorna índices de N produtos para inspeção, fixados pela seed.

    Usa random.Random isolado para não afetar o estado global do gerador.
    """
    import random

    rng = random.Random(seed)
    indices = list(range(len(skus)))
    return rng.sample(indices, min(n, len(indices)))


def top_features_de_produto(
    vetor: np.ndarray,
    vocabulario: np.ndarray,
    n: int,
) -> list[tuple[str, float]]:
    """Retorna as N features com maior peso TF-IDF para um produto."""
    top_idx = vetor.argsort()[::-1][:n]
    return [
        (str(vocabulario[i]), round(float(vetor[i]), 4))
        for i in top_idx
        if vetor[i] > 0
    ]


def imprimir_inspecao_por_produto(
    matriz: csr_matrix,
    skus: list[str],
    vocabulario: np.ndarray,
    indices_amostra: list[int],
) -> None:
    """Imprime as top features de cada produto da amostra."""
    print("\n" + "=" * 60)
    print("SEÇÃO 4 — INSPEÇÃO POR PRODUTO (top features)")
    print("=" * 60)

    for idx in indices_amostra:
        vetor = np.asarray(matriz[idx].todense()).flatten()
        top = top_features_de_produto(vetor, vocabulario, N_TOP_FEATURES)
        print(f"\nSKU {skus[idx]}:")
        for feature, peso in top:
            print(f"  {feature:<35} {peso}")


# ---------------------------------------------------------------------------
# Diagnóstico global
# ---------------------------------------------------------------------------


def imprimir_features_globais(
    matriz: csr_matrix,
    vocabulario: np.ndarray,
    n: int = 20,
) -> None:
    """Imprime as features com maior peso acumulado no corpus."""
    print("\n" + "=" * 60)
    print("SEÇÃO 5 — FEATURES MAIS RELEVANTES NO CORPUS")
    print("=" * 60)

    pesos_globais = np.asarray(matriz.sum(axis=0)).flatten()
    top_idx = pesos_globais.argsort()[::-1][:n]

    print(f"Top {n} features por peso acumulado:")
    for i in top_idx:
        print(f"  {vocabulario[i]:<40} {pesos_globais[i]:.2f}")


def imprimir_diagnostico_idf(
    vectorizer: TfidfVectorizer,
    vocabulario: np.ndarray,
) -> None:
    """Imprime features com IDF extremo para detectar boilerplate residual.

    Features com IDF muito baixo aparecem em quase todos os documentos
    e são candidatas à stoplist de domínio em preparar_corpus.py.
    """
    print("\n" + "=" * 60)
    print("SEÇÃO 6 — DIAGNÓSTICO IDF (termos raros vs. ubíquos)")
    print("=" * 60)

    top_idf, bot_idf = calcular_idf_por_feature(vectorizer, vocabulario)

    print("10 termos mais raros (IDF alto — mais discriminativos):")
    for feature, score in top_idf:
        print(f"  {feature:<35} {score}")

    print("\n10 termos mais comuns (IDF baixo — candidatos à stoplist):")
    for feature, score in bot_idf:
        print(f"  {feature:<35} {score}")


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------


def salvar_matriz(matriz: csr_matrix, caminho: Path) -> None:
    save_npz(str(caminho), matriz)
    log.info("Matriz salva: %s  (shape %s, nnz %d)", caminho, matriz.shape, matriz.nnz)


def salvar_json(dados: list, caminho: Path, descricao: str) -> None:
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False)
    log.info("%s salvo: %s  (%d entradas)", descricao, caminho, len(dados))


# ---------------------------------------------------------------------------
# Seções de execução
# ---------------------------------------------------------------------------


def secao_carregamento() -> tuple[list[str], list[str]]:
    print("\n" + "=" * 60)
    print("SEÇÃO 1 — CARREGAMENTO")
    print("=" * 60)

    registros = carregar_corpus(CORPUS_JSONL)
    skus, documentos = montar_documentos(registros)

    forma = []
    if USAR_LEMMAS:
        forma.append("lemmas")
    if USAR_TEXTOS:
        forma.append("textos")

    print(f"Registros carregados : {len(registros)}")
    print(f"Documentos válidos   : {len(documentos)}")
    print(f"Forma utilizada      : {' + '.join(forma)}")
    print(f"Tokens totais        : {sum(len(d.split()) for d in documentos):,}")
    print(f"Exemplo (SKU {skus[0]})    : {documentos[0][:100]}…")

    return skus, documentos


def secao_vocabulario(documentos: list[str]) -> TfidfVectorizer:
    print("\n" + "=" * 60)
    print("SEÇÃO 2 — VOCABULÁRIO")
    print("=" * 60)

    vectorizer = construir_vocabulario(documentos)
    vocabulario = vectorizer.get_feature_names_out()
    unigrams, bigrams = separar_por_tipo_ngram(vocabulario)

    print(f"Total de features    : {len(vocabulario):,}")
    print(f"Unigrams             : {len(unigrams):,}")
    print(f"Bigrams              : {len(bigrams):,}")
    print(f"Primeiras 15         : {list(vocabulario[:15])}")
    if bigrams:
        print(f"Amostra de bigrams   : {bigrams[:10]}")

    return vectorizer


def secao_transformacao(
    vectorizer: TfidfVectorizer,
    documentos: list[str],
) -> csr_matrix:
    print("\n" + "=" * 60)
    print("SEÇÃO 3 — TRANSFORMAÇÃO")
    print("=" * 60)

    matriz = transformar_documentos(vectorizer, documentos)
    shape = matriz.shape
    assert shape is not None and len(shape) == 2
    n_linhas = int(shape[0])
    n_colunas = int(shape[1])
    esparsidade = calcular_esparsidade(matriz)

    print(f"Dimensões            : {n_linhas} produtos × {n_colunas} features")
    print(f"Elementos não-zero   : {int(matriz.nnz):,}")
    print(f"Esparsidade          : {esparsidade:.1%}")
    print(f"Normalização         : {NORMALIZACAO or 'nenhuma'}")
    print(f"Memória (denso est.) : {n_linhas * n_colunas * 8 / 1024 / 1024:.1f} MB")
    print(f"Memória (esparso)    : {matriz.data.nbytes / 1024:.1f} KB")

    return matriz


def secao_inspecao(
    matriz: csr_matrix,
    skus: list[str],
    vectorizer: TfidfVectorizer,
) -> None:
    vocabulario = vectorizer.get_feature_names_out()
    indices = selecionar_amostra(skus, N_AMOSTRAS_INSPECAO, SEED_AMOSTRA)
    imprimir_inspecao_por_produto(matriz, skus, vocabulario, indices)
    imprimir_features_globais(matriz, vocabulario)
    imprimir_diagnostico_idf(vectorizer, vocabulario)


def secao_persistencia(
    matriz: csr_matrix,
    skus: list[str],
    vectorizer: TfidfVectorizer,
) -> None:
    print("\n" + "=" * 60)
    print("SEÇÃO 7 — PERSISTÊNCIA")
    print("=" * 60)

    VECTORS_TFIDF_DIR.mkdir(parents=True, exist_ok=True)
    salvar_matriz(matriz, OUTPUT_MATRIX)
    salvar_json(skus, OUTPUT_SKUS, "SKUs")
    salvar_json(
        vectorizer.get_feature_names_out().tolist(),
        OUTPUT_FEATURES,
        "Features",
    )
    print(f"\nVetorização TF-IDF concluída. Próxima etapa: vetorizar_w2v.py")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    validar_configuracao()

    if not CORPUS_JSONL.exists():
        log.error("Corpus não encontrado: '%s'", CORPUS_JSONL)
        log.error("Execute preparar_corpus.py antes desta etapa.")
        sys.exit(1)

    skus, documentos = secao_carregamento()
    vectorizer = secao_vocabulario(documentos)
    matriz = secao_transformacao(vectorizer, documentos)
    secao_inspecao(matriz, skus, vectorizer)
    secao_persistencia(matriz, skus, vectorizer)


if __name__ == "__main__":
    main()
