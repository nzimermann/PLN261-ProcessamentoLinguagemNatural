"""
Vetorização BoW — pipeline de clusterização.

Lê data/processed/corpus/corpus_bow_tfidf.jsonl e transforma os termos
de cada produto em vetores de frequência usando Bag of Words (CountVectorizer).

Saída:
    data/processed/vectors/bow_matrix.npz     — matriz esparsa de contagens
    data/processed/vectors/bow_skus.json      — SKUs na ordem das linhas
    data/processed/vectors/bow_features.json  — vocabulário na ordem das colunas

Uso:
    pip install scikit-learn scipy numpy
    python vetorizar_bow.py
"""

import json
import sys
from pathlib import Path
from src import logger
from src.config import CORPUS_DIR, VECTORS_BOW

import numpy as np
from scipy.sparse import csr_matrix, save_npz
from sklearn.feature_extraction.text import CountVectorizer

# ---------------------------------------------------------------------------
# Configuração — ajuste aqui para experimentar diferentes abordagens
# ---------------------------------------------------------------------------

CORPUS_JSONL = CORPUS_DIR / "corpus_bow_tfidf.jsonl"
OUTPUT_MATRIX = VECTORS_BOW / "bow_matrix.npz"
OUTPUT_SKUS = VECTORS_BOW / "bow_skus.json"
OUTPUT_FEATURES = VECTORS_BOW / "bow_features.json"

# Campos do corpus a incluir na representação do documento.
# Ambos podem ser ativados simultaneamente — os termos serão concatenados
# na ordem: lemmas primeiro, textos depois (ou apenas o campo ativo).
# Útil para testar se combinar as duas formas melhora a separação dos clusters.
USAR_LEMMAS: bool = True
USAR_TEXTOS: bool = False

# n-gramas: (1, 1) = apenas unigrams | (1, 2) = uni + bigrams
NGRAM_RANGE: tuple[int, int] = (1, 1)

# Ignora termos que aparecem em menos de MIN_DF documentos.
# Valor absoluto (int) ou proporção do corpus (float entre 0 e 1).
MIN_DF: int | float = 2

# Ignora termos que aparecem em mais de MAX_DF documentos.
# Proporção (float) ou valor absoluto (int).
MAX_DF: int | float = 0.90

# Limite de features (dimensionalidade máxima do vocabulário).
# None = sem limite.
MAX_FEATURES: int | None = 5000

# Quantos produtos amostrar na inspeção por produto (Seção 4)
N_AMOSTRAS_INSPECAO: int = 5

# Quantas top features exibir por produto na inspeção
N_TOP_FEATURES: int = 10

# Seed para fixar a amostra entre execuções — os mesmos produtos
# são sempre exibidos, permitindo comparar resultados entre ajustes
SEED_AMOSTRA: int = 42


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


def montar_documentos(
    registros: list[dict],
) -> tuple[list[str], list[str]]:
    """Retorna (skus, documentos) a partir dos registros do corpus.

    Documentos são strings de termos separados por espaço — formato
    esperado pelo CountVectorizer. Registros sem termos são ignorados
    com aviso de log para não contaminar o vocabulário com strings vazias.
    """
    skus: list[str] = []
    documentos: list[str] = []

    for registro in registros:
        sku = registro.get("sku", "")
        termos = extrair_termos(registro)

        if not termos:
            logger.warning("SKU %s sem termos após extração — ignorado.", sku)
            continue

        skus.append(sku)
        documentos.append(" ".join(termos))

    return skus, documentos


# ---------------------------------------------------------------------------
# Vocabulário
# ---------------------------------------------------------------------------


def construir_vocabulario(documentos: list[str]) -> CountVectorizer:
    """Instancia e treina o CountVectorizer com os parâmetros configurados.

    O fit é separado do transform para permitir inspecionar o vocabulário
    antes de transformar os documentos.
    """
    vectorizer = CountVectorizer(
        ngram_range=NGRAM_RANGE,
        min_df=MIN_DF,  # type: ignore[arg-type]
        max_df=MAX_DF,  # type: ignore[arg-type]
        max_features=MAX_FEATURES,
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


# ---------------------------------------------------------------------------
# Transformação
# ---------------------------------------------------------------------------


def transformar_documentos(
    vectorizer: CountVectorizer,
    documentos: list[str],
) -> csr_matrix:
    """Aplica o vocabulário treinado e produz a matriz de contagens."""
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

    Usa espaçamento uniforme em vez de random.sample para garantir que
    a amostra cubra todo o corpus (início, meio e fim), independentemente
    da seed escolhida.
    """
    import random

    rng = random.Random(seed)
    indices = list(range(len(skus)))
    return rng.sample(indices, min(n, len(indices)))


def top_features_de_produto(
    vetor: np.ndarray,
    vocabulario: np.ndarray,
    n: int,
) -> list[tuple[str, int]]:
    """Retorna as N features com maior contagem para um produto."""
    top_idx = vetor.argsort()[::-1][:n]
    return [(str(vocabulario[i]), int(vetor[i])) for i in top_idx if vetor[i] > 0]


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
        for feature, contagem in top:
            print(f"  {feature:<35} {contagem}")


# ---------------------------------------------------------------------------
# Diagnóstico global
# ---------------------------------------------------------------------------


def imprimir_features_globais(
    matriz: csr_matrix,
    vocabulario: np.ndarray,
    n: int = 20,
) -> None:
    """Imprime as features com maior contagem acumulada no corpus."""
    print("\n" + "=" * 60)
    print("SEÇÃO 5 — FEATURES MAIS FREQUENTES NO CORPUS")
    print("=" * 60)

    contagens_globais = np.asarray(matriz.sum(axis=0)).flatten()
    top_idx = contagens_globais.argsort()[::-1][:n]

    print(f"Top {n} features por contagem acumulada:")
    for i in top_idx:
        print(f"  {vocabulario[i]:<40} {int(contagens_globais[i])}")


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------


def salvar_matriz(matriz: csr_matrix, caminho: Path) -> None:
    save_npz(str(caminho), matriz)
    logger.info(
        "Matriz salva: %s  (shape %s, nnz %d)", caminho, matriz.shape, matriz.nnz
    )


def salvar_json(dados: list, caminho: Path, descricao: str) -> None:
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False)
    logger.info("%s salvo: %s  (%d entradas)", descricao, caminho, len(dados))


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


def secao_vocabulario(documentos: list[str]) -> CountVectorizer:
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
    vectorizer: CountVectorizer,
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
    print(f"Memória (denso est.) : {n_linhas * n_colunas * 4 / 1024 / 1024:.1f} MB")
    print(f"Memória (esparso)    : {matriz.data.nbytes / 1024:.1f} KB")

    return matriz


def secao_inspecao(
    matriz: csr_matrix,
    skus: list[str],
    vectorizer: CountVectorizer,
) -> None:
    vocabulario = vectorizer.get_feature_names_out()
    indices = selecionar_amostra(skus, N_AMOSTRAS_INSPECAO, SEED_AMOSTRA)
    imprimir_inspecao_por_produto(matriz, skus, vocabulario, indices)
    imprimir_features_globais(matriz, vocabulario)


def secao_persistencia(
    matriz: csr_matrix,
    skus: list[str],
    vectorizer: CountVectorizer,
) -> None:
    print("\n" + "=" * 60)
    print("SEÇÃO 6 — PERSISTÊNCIA")
    print("=" * 60)

    VECTORS_BOW_DIR.mkdir(parents=True, exist_ok=True)
    salvar_matriz(matriz, OUTPUT_MATRIX)
    salvar_json(skus, OUTPUT_SKUS, "SKUs")
    salvar_json(
        vectorizer.get_feature_names_out().tolist(),
        OUTPUT_FEATURES,
        "Features",
    )
    print(f"\nVetorização BoW concluída. Próxima etapa: vetorizar_tfidf.py")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    validar_configuracao()

    if not CORPUS_JSONL.exists():
        logger.error("Corpus não encontrado: '%s'", CORPUS_JSONL)
        logger.error("Execute preparar_corpus.py antes desta etapa.")
        sys.exit(1)

    skus, documentos = secao_carregamento()
    vectorizer = secao_vocabulario(documentos)
    matriz = secao_transformacao(vectorizer, documentos)
    secao_inspecao(matriz, skus, vectorizer)
    secao_persistencia(matriz, skus, vectorizer)


if __name__ == "__main__":
    main()
