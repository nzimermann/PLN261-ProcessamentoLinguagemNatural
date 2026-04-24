"""
Vetorização Word2Vec — pipeline de clusterização.

Lê data/processed/corpus/corpus_w2v.jsonl, treina um modelo Word2Vec sobre
as sequências de tokens de cada produto e representa cada produto como a
média dos vetores de seus tokens (document embedding por mean pooling).

Diferença fundamental em relação ao BoW e TF-IDF:
    BoW/TF-IDF operam sobre frequências — cada dimensão é um termo do vocabulário.
    Word2Vec aprende um espaço vetorial denso onde termos semanticamente próximos
    ficam próximos geometricamente. "amadeirado" e "carvalho" podem ficar vizinhos
    mesmo sem co-ocorrer no mesmo documento.

Saída:
    data/processed/vectors/w2v_matrix.npy      — matriz densa (produtos × VECTOR_SIZE)
    data/processed/vectors/w2v_skus.json       — SKUs na ordem das linhas
    data/processed/vectors/w2v_model/          — modelo treinado (reutilizável)

Uso:
    pip install gensim numpy
    python vetorizar_w2v.py
"""

import json
import sys
from pathlib import Path
from src import logger
from src.config import CORPUS_DIR, VECTORS_W2V

import numpy as np
from gensim.models import Word2Vec

# ---------------------------------------------------------------------------
# Configuração — ajuste aqui para experimentar diferentes abordagens
# ---------------------------------------------------------------------------

CORPUS_JSONL = CORPUS_DIR / "corpus_w2v.jsonl"
OUTPUT_MATRIX = VECTORS_W2V / "w2v_matrix.npy"
OUTPUT_SKUS = VECTORS_W2V / "w2v_skus.json"
OUTPUT_MODEL = VECTORS_W2V / "word2vec.model"

# Dimensionalidade dos vetores de palavras.
# Valores maiores capturam mais nuance semântica, mas exigem mais dados
# para treinar bem. Para corpora pequenos (<10k documentos), 100–200 é
# um intervalo seguro. Aumentar para 300 só se o corpus for grande.
VECTOR_SIZE: int = 100

# Tamanho da janela de contexto (tokens à esquerda e à direita do token alvo).
# Janelas menores (2–4) aprendem relações sintáticas próximas.
# Janelas maiores (5–10) aprendem relações semânticas mais amplas.
# Para descrições de produto, 5 é um ponto de partida equilibrado.
WINDOW: int = 5

# Ignora tokens que aparecem em menos de MIN_COUNT documentos.
# Com corpora pequenos, reduzir para 1 ou 2 para não descartar vocabulário
# específico do domínio que aparece raramente mas é semanticamente rico.
MIN_COUNT: int = 2

# Algoritmo de treinamento:
#   "skip-gram" (sg=1) — prediz contexto a partir do token alvo.
#                        Melhor para tokens raros e vocabulário específico.
#   "cbow"      (sg=0) — prediz token alvo a partir do contexto.
#                        Mais rápido, melhor para tokens frequentes.
# Para vocabulário de domínio (nomes de bebidas, técnicas de produção),
# skip-gram tende a produzir melhores representações.
ALGORITMO: str = "cbow"

# Número de épocas de treinamento.
# Mais épocas melhoram a qualidade dos vetores mas aumentam o tempo de treino.
# Para corpora pequenos, 10–30 épocas compensam o volume reduzido de dados.
EPOCHS: int = 20

# Número de threads para treinamento paralelo.
WORKERS: int = 4

# Seed para reprodutibilidade do treinamento.
# Word2Vec tem componente estocástico — fixar a seed garante que o mesmo
# corpus produza o mesmo modelo entre execuções.
SEED: int = 42

# Quantos produtos amostrar na inspeção por produto (Seção 4)
N_AMOSTRAS_INSPECAO: int = 5

# Quantos tokens vizinhos exibir por token na inspeção semântica (Seção 5)
N_VIZINHOS_TOKEN: int = 5

# Quantos termos representativos exibir por produto na inspeção (Seção 4)
N_TOP_TERMOS: int = 10

# Seed para fixar a amostra de produtos entre execuções
SEED_AMOSTRA: int = 42


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
# Preparação das sequências
# ---------------------------------------------------------------------------


def extrair_sequencia(registro: dict) -> list[str]:
    """Extrai a lista de tokens de um registro do corpus W2V."""
    return registro.get("termos", [])


def montar_sequencias(
    registros: list[dict],
) -> tuple[list[str], list[list[str]]]:
    """Retorna (skus, sequencias) a partir dos registros do corpus.

    Sequências vazias são ignoradas com aviso — um produto sem tokens
    não pode ser representado por Word2Vec.
    """
    skus: list[str] = []
    sequencias: list[list[str]] = []

    for registro in registros:
        sku = registro.get("sku", "")
        tokens = extrair_sequencia(registro)

        if not tokens:
            logger.warning("SKU %s sem tokens — ignorado.", sku)
            continue

        skus.append(sku)
        sequencias.append(tokens)

    return skus, sequencias


# ---------------------------------------------------------------------------
# Treinamento do modelo
# ---------------------------------------------------------------------------


def treinar_word2vec(sequencias: list[list[str]]) -> Word2Vec:
    """Treina o modelo Word2Vec sobre as sequências do corpus.

    O parâmetro sg (skip-gram) é derivado da constante ALGORITMO para
    manter a configuração legível no topo do arquivo.
    """
    sg = 1 if ALGORITMO == "skip-gram" else 0

    modelo = Word2Vec(
        sentences=sequencias,
        vector_size=VECTOR_SIZE,
        window=WINDOW,
        min_count=MIN_COUNT,
        workers=WORKERS,
        epochs=EPOCHS,
        seed=SEED,
        sg=sg,
    )
    return modelo


# ---------------------------------------------------------------------------
# Document embedding por mean pooling
# ---------------------------------------------------------------------------


def vetor_de_token(modelo: Word2Vec, token: str) -> np.ndarray | None:
    """Retorna o vetor de um token ou None se estiver fora do vocabulário."""
    if token in modelo.wv:
        return modelo.wv[token]
    return None


def calcular_embedding_documento(
    modelo: Word2Vec,
    tokens: list[str],
) -> np.ndarray | None:
    """Calcula o embedding de um documento como média dos vetores dos seus tokens.

    Tokens fora do vocabulário (OOV) são ignorados silenciosamente.
    Retorna None se nenhum token do documento estiver no vocabulário —
    situação que indica MIN_COUNT muito alto ou documento muito curto.
    """
    vetores = [
        vetor_de_token(modelo, token)
        for token in tokens
        if vetor_de_token(modelo, token) is not None
    ]

    if not vetores:
        return None

    return np.mean(np.array(vetores), axis=0)


def construir_matriz_embeddings(
    modelo: Word2Vec,
    skus: list[str],
    sequencias: list[list[str]],
) -> tuple[list[str], np.ndarray]:
    """Constrói a matriz de embeddings (produtos × VECTOR_SIZE).

    Produtos cujo embedding resulte None são removidos da lista de SKUs
    com aviso de log — a matriz final contém apenas produtos representáveis.

    Retorna (skus_validos, matriz) com garantia de alinhamento entre
    índice de linha e posição em skus_validos.
    """
    skus_validos: list[str] = []
    embeddings: list[np.ndarray] = []

    for sku, tokens in zip(skus, sequencias):
        embedding = calcular_embedding_documento(modelo, tokens)

        if embedding is None:
            logger.warning(
                "SKU %s sem tokens no vocabulário após treinamento — ignorado.", sku
            )
            continue

        skus_validos.append(sku)
        embeddings.append(embedding)

    return skus_validos, np.vstack(embeddings)


# ---------------------------------------------------------------------------
# Inspeção semântica
# ---------------------------------------------------------------------------


def selecionar_amostra(skus: list[str], n: int, seed: int) -> list[int]:
    """Retorna índices de N produtos para inspeção, fixados pela seed.

    Usa random.Random isolado para não afetar o estado global do gerador.
    """
    import random

    rng = random.Random(seed)
    indices = list(range(len(skus)))
    return rng.sample(indices, min(n, len(indices)))


def tokens_mais_representativos(
    modelo: Word2Vec,
    tokens: list[str],
    n: int,
) -> list[tuple[str, float]]:
    """Retorna os N tokens do documento com vetor mais próximo do embedding médio.

    Proximidade é medida por similaridade de cosseno entre o vetor do token
    e o embedding do documento. Tokens mais próximos da média são os que
    melhor representam o "centro semântico" do produto.
    """
    embedding_doc = calcular_embedding_documento(modelo, tokens)
    if embedding_doc is None:
        return []

    tokens_no_vocab = [t for t in tokens if t in modelo.wv]
    if not tokens_no_vocab:
        return []

    # Deduplica preservando ordem de primeira ocorrência
    tokens_unicos = list(dict.fromkeys(tokens_no_vocab))

    similaridades: list[tuple[str, float]] = []
    for token in tokens_unicos:
        vetor = modelo.wv[token]
        norma_doc = np.linalg.norm(embedding_doc)
        norma_token = np.linalg.norm(vetor)
        if norma_doc == 0 or norma_token == 0:
            continue
        cosseno = float(np.dot(embedding_doc, vetor) / (norma_doc * norma_token))
        similaridades.append((token, round(cosseno, 4)))

    return sorted(similaridades, key=lambda x: x[1], reverse=True)[:n]


def vizinhos_semanticos(
    modelo: Word2Vec,
    token: str,
    n: int,
) -> list[tuple[str, float]]:
    """Retorna os N tokens mais próximos de um token no espaço vetorial."""
    if token not in modelo.wv:
        return []
    return [(t, round(float(s), 4)) for t, s in modelo.wv.most_similar(token, topn=n)]


def imprimir_inspecao_por_produto(
    modelo: Word2Vec,
    skus: list[str],
    sequencias: list[list[str]],
    indices_amostra: list[int],
) -> None:
    """Imprime os tokens mais representativos de cada produto da amostra."""
    print("\n" + "=" * 60)
    print("SEÇÃO 4 — INSPEÇÃO POR PRODUTO (tokens mais representativos)")
    print("=" * 60)
    print("Tokens com maior similaridade de cosseno ao embedding médio do produto.\n")

    for idx in indices_amostra:
        top = tokens_mais_representativos(modelo, sequencias[idx], N_TOP_TERMOS)
        print(f"SKU {skus[idx]}:")
        for token, score in top:
            print(f"  {token:<35} {score}")
        print()


# ---------------------------------------------------------------------------
# Diagnóstico global do modelo
# ---------------------------------------------------------------------------


def imprimir_cobertura_vocabulario(
    modelo: Word2Vec,
    sequencias: list[list[str]],
) -> None:
    """Imprime estatísticas de cobertura do vocabulário treinado."""
    print("\n" + "=" * 60)
    print("SEÇÃO 5 — VOCABULÁRIO E COBERTURA")
    print("=" * 60)

    vocab_size = len(modelo.wv)
    total_tokens = sum(len(s) for s in sequencias)
    tokens_unicos = len(set(t for s in sequencias for t in s))
    tokens_oov = tokens_unicos - vocab_size

    print(f"Tokens únicos no corpus  : {tokens_unicos:,}")
    print(f"Tokens no vocabulário W2V: {vocab_size:,}  (min_count={MIN_COUNT})")
    print(f"Tokens descartados (OOV) : {max(tokens_oov, 0):,}")
    print(f"Cobertura do vocabulário : {vocab_size / tokens_unicos:.1%}")
    print(f"Dimensão dos vetores     : {VECTOR_SIZE}")
    print(f"Algoritmo                : {ALGORITMO}")
    print(f"Janela de contexto       : {WINDOW}")
    print(f"Épocas de treinamento    : {EPOCHS}")


def imprimir_vizinhos_semanticos(modelo: Word2Vec) -> None:
    """Imprime vizinhos semânticos de termos-chave do domínio de bebidas.

    Permite avaliar se o espaço vetorial aprendeu relações semânticas
    relevantes: termos de sabor próximos entre si, categorias agrupadas, etc.
    """
    print("\n" + "=" * 60)
    print("SEÇÃO 6 — VIZINHOS SEMÂNTICOS (sanidade do espaço vetorial)")
    print("=" * 60)

    termos_referencia = [
        t
        for t in [
            "whisky",
            "rum",
            "vodka",
            "vinho",
            "gin",
            "aroma",
            "seco",
            "suave",
            "carvalho",
            "fruta",
        ]
        if t in modelo.wv
    ]

    if not termos_referencia:
        print("Nenhum termo de referência encontrado no vocabulário.")
        return

    for termo in termos_referencia[:6]:
        vizinhos = vizinhos_semanticos(modelo, termo, N_VIZINHOS_TOKEN)
        pares = "  |  ".join(f"{t} ({s})" for t, s in vizinhos)
        print(f"  {termo:<12} → {pares}")


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------


def salvar_matriz(matriz: np.ndarray, caminho: Path) -> None:
    """Salva a matriz densa como .npy — formato nativo do numpy."""
    np.save(str(caminho), matriz)
    logger.info(
        "Matriz salva: %s  (shape %s, dtype %s)",
        caminho,
        matriz.shape,
        matriz.dtype,
    )


def salvar_skus(skus: list[str], caminho: Path) -> None:
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(skus, f, ensure_ascii=False)
    logger.info("SKUs salvos: %s  (%d entradas)", caminho, len(skus))


def salvar_modelo(modelo: Word2Vec, caminho: Path) -> None:
    """Salva o modelo completo para reutilização ou análise posterior."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    modelo.save(str(caminho))
    logger.info("Modelo W2V salvo: %s", caminho)


# ---------------------------------------------------------------------------
# Seções de execução
# ---------------------------------------------------------------------------


def secao_carregamento() -> tuple[list[str], list[list[str]]]:
    print("\n" + "=" * 60)
    print("SEÇÃO 1 — CARREGAMENTO")
    print("=" * 60)

    registros = carregar_corpus(CORPUS_JSONL)
    skus, sequencias = montar_sequencias(registros)

    comprimentos = [len(s) for s in sequencias]
    print(f"Registros carregados : {len(registros)}")
    print(f"Documentos válidos   : {len(sequencias)}")
    print(
        f"Tokens/doc           : mín {min(comprimentos)} · "
        f"méd {sum(comprimentos)/len(comprimentos):.0f} · "
        f"máx {max(comprimentos)}"
    )
    print(f"Total de tokens      : {sum(comprimentos):,}")
    print(f"Exemplo (SKU {skus[0]})    : {sequencias[0][:15]}…")

    return skus, sequencias


def secao_treinamento(sequencias: list[list[str]]) -> Word2Vec:
    print("\n" + "=" * 60)
    print("SEÇÃO 2 — TREINAMENTO DO MODELO")
    print("=" * 60)

    print(f"Algoritmo    : {ALGORITMO}")
    print(f"Vector size  : {VECTOR_SIZE}")
    print(f"Window       : {WINDOW}")
    print(f"Min count    : {MIN_COUNT}")
    print(f"Epochs       : {EPOCHS}")
    print(f"Workers      : {WORKERS}")
    print(f"Seed         : {SEED}")
    print("Treinando…")

    modelo = treinar_word2vec(sequencias)

    print(f"Vocabulário treinado : {len(modelo.wv)} tokens")
    return modelo


def secao_embeddings(
    modelo: Word2Vec,
    skus: list[str],
    sequencias: list[list[str]],
) -> tuple[list[str], np.ndarray]:
    print("\n" + "=" * 60)
    print("SEÇÃO 3 — EMBEDDINGS DE DOCUMENTO (mean pooling)")
    print("=" * 60)

    skus_validos, matriz = construir_matriz_embeddings(modelo, skus, sequencias)

    print(f"Produtos representados : {len(skus_validos)}/{len(skus)}")
    print(
        f"Dimensões da matriz    : {matriz.shape[0]} produtos × {matriz.shape[1]} dims"
    )
    print(f"Dtype                  : {matriz.dtype}")
    print(f"Memória                : {matriz.nbytes / 1024:.1f} KB")
    print(f"Norma média dos vetores: {np.linalg.norm(matriz, axis=1).mean():.4f}")

    return skus_validos, matriz


def secao_inspecao(
    modelo: Word2Vec,
    skus: list[str],
    sequencias: list[list[str]],
) -> None:
    indices = selecionar_amostra(skus, N_AMOSTRAS_INSPECAO, SEED_AMOSTRA)
    imprimir_inspecao_por_produto(modelo, skus, sequencias, indices)
    imprimir_cobertura_vocabulario(modelo, sequencias)
    imprimir_vizinhos_semanticos(modelo)


def secao_persistencia(
    matriz: np.ndarray,
    skus: list[str],
    modelo: Word2Vec,
) -> None:
    print("\n" + "=" * 60)
    print("SEÇÃO 7 — PERSISTÊNCIA")
    print("=" * 60)

    VECTORS_W2V.mkdir(parents=True, exist_ok=True)
    salvar_matriz(matriz, OUTPUT_MATRIX)
    salvar_skus(skus, OUTPUT_SKUS)
    salvar_modelo(modelo, OUTPUT_MODEL)

    print("\nVetorização Word2Vec concluída. Próxima etapa: comparar_metodos.py")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    if not CORPUS_JSONL.exists():
        logger.error("Corpus não encontrado: '%s'", CORPUS_JSONL)
        logger.error("Execute preparar_corpus.py antes desta etapa.")
        sys.exit(1)

    skus, sequencias = secao_carregamento()
    modelo = secao_treinamento(sequencias)
    skus_validos, matriz = secao_embeddings(modelo, skus, sequencias)
    secao_inspecao(modelo, skus_validos, sequencias)
    secao_persistencia(matriz, skus_validos, modelo)


if __name__ == "__main__":
    main()
