"""
Visualização comparativa dos três métodos de vetorização.

Lê as matrizes geradas pelos três vetorizadores e produz dois gráficos
comparativos lado a lado para BoW, TF-IDF e Word2Vec:

    Figura 1 — Top features por produto (amostra fixada de 5 produtos)
               Barras horizontais com peso/score de cada feature.
               Para BoW/TF-IDF: termos com maior peso na matriz.
               Para W2V: palavras do vocabulário mais próximas do embedding.

    Figura 2 — Heatmap de similaridade de cosseno por categoria
               Matriz de categorias × categorias com similaridade média.
               Permite avaliar se cada método separou as categorias corretamente.

Uso:
    pip install matplotlib seaborn scikit-learn scipy numpy gensim
    python visualizar_resultados.py
"""

import csv
import json
import random
import sys
from typing import NamedTuple

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import seaborn as sns
from matplotlib.figure import Figure
from scipy.sparse import csr_matrix, load_npz
from sklearn.metrics.pairwise import cosine_similarity
from src import logger
from src.config import *

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

# Matrizes e metadados por método
BOW_MATRIX = VECTORS_BOW / "bow_matrix.npz"
BOW_SKUS = VECTORS_BOW / "bow_skus.json"
BOW_FEATURES = VECTORS_BOW / "bow_features.json"

TFIDF_MATRIX = VECTORS_TFIDF / "tfidf_matrix.npz"
TFIDF_SKUS = VECTORS_TFIDF / "tfidf_skus.json"
TFIDF_FEATURES = VECTORS_TFIDF / "tfidf_features.json"

W2V_MATRIX = VECTORS_W2V / "w2v_matrix.npy"
W2V_SKUS = VECTORS_W2V / "w2v_skus.json"
W2V_MODEL = VECTORS_W2V / "word2vec.model"

CORPUS_SKUS = CORPUS_DIR / "corpus_skus.json"
PRODUCTS_CSV = PROCESSED_DIR / "products.csv"

# Amostra fixada — deve ser idêntica à usada nos scripts de vetorização
N_AMOSTRAS: int = 5
SEED_AMOSTRA: int = 42

# Quantas top features exibir por produto em cada método
N_TOP_FEATURES: int = 10

# Arquivo de saída dos gráficos (None = só exibir, não salvar)
SALVAR_FIGURA_1: Path | None = REPORTS_DIR / "grafico_top_features.png"
SALVAR_FIGURA_2: Path | None = REPORTS_DIR / "grafico_heatmap_categorias.png"

# DPI das figuras salvas
DPI: int = 150


# ---------------------------------------------------------------------------
# Estruturas de dados
# ---------------------------------------------------------------------------


class DadosMetodo(NamedTuple):
    """Agrupa todos os artefatos de um método de vetorização."""

    nome: str
    matriz: np.ndarray  # sempre densa para cálculos uniformes
    skus: list[str]
    features: list[str] | None  # None para W2V (não há features nomeadas)


class MetadatoProduto(NamedTuple):
    nome: str
    categoria: str


# ---------------------------------------------------------------------------
# Leitura de artefatos
# ---------------------------------------------------------------------------


def carregar_json(caminho: Path) -> list:
    with open(caminho, encoding="utf-8") as f:
        return json.load(f)


def carregar_matriz_esparsa(caminho: Path) -> np.ndarray:
    """Carrega .npz esparso e converte para array denso."""
    matriz: csr_matrix = load_npz(str(caminho))  # type: ignore[assignment]
    return matriz.toarray()


def carregar_matriz_densa(caminho: Path) -> np.ndarray:
    return np.load(str(caminho))


def carregar_metadata_produtos(caminho: Path) -> dict[str, MetadatoProduto]:
    """Lê products.csv e indexa por SKU."""
    metadata: dict[str, MetadatoProduto] = {}
    with open(caminho, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            metadata[row["sku"]] = MetadatoProduto(
                nome=row.get("name", ""),
                categoria=row.get("category", ""),
            )
    return metadata


def carregar_modelo_w2v(caminho: Path):
    """Carrega o modelo Word2Vec treinado."""
    try:
        from gensim.models import Word2Vec

        return Word2Vec.load(str(caminho))
    except ImportError:
        logger.warning("gensim não instalado — top features W2V indisponível.")
        return None
    except Exception as exc:
        logger.warning("Não foi possível carregar modelo W2V: %s", exc)
        return None


def carregar_dados_metodo(
    nome: str,
    caminho_matriz: Path,
    caminho_skus: Path,
    caminho_features: Path | None,
    esparso: bool,
) -> DadosMetodo | None:
    """Carrega os artefatos de um método e retorna None se algum estiver ausente."""
    for caminho in filter(None, [caminho_matriz, caminho_skus, caminho_features]):
        if not caminho.exists():
            logger.warning(
                "Artefato ausente para %s: '%s' — método ignorado.", nome, caminho
            )
            return None

    matriz = (
        carregar_matriz_esparsa(caminho_matriz)
        if esparso
        else carregar_matriz_densa(caminho_matriz)
    )
    skus = carregar_json(caminho_skus)
    features = carregar_json(caminho_features) if caminho_features else None

    logger.info("%-8s carregado: %d produtos × %d dims", nome, *matriz.shape)
    return DadosMetodo(nome=nome, matriz=matriz, skus=skus, features=features)


# ---------------------------------------------------------------------------
# Alinhamento de SKUs
# ---------------------------------------------------------------------------


def alinhar_ao_corpus_master(
    dados: DadosMetodo,
    skus_master: list[str],
) -> DadosMetodo:
    """Reordena e filtra a matriz para corresponder à ordem dos SKUs master.

    Garante que a linha i de cada método corresponde ao mesmo produto,
    condição necessária para comparação direta entre matrizes.
    """
    indice_sku = {sku: i for i, sku in enumerate(dados.skus)}
    indices_validos = [indice_sku[s] for s in skus_master if s in indice_sku]
    skus_alinhados = [
        skus_master[i] for i, s in enumerate(skus_master) if s in indice_sku
    ]

    return DadosMetodo(
        nome=dados.nome,
        matriz=dados.matriz[indices_validos],
        skus=skus_alinhados,
        features=dados.features,
    )


def skus_comuns(metodos: list[DadosMetodo]) -> list[str]:
    """Retorna SKUs presentes em todos os métodos, na ordem do primeiro."""
    conjuntos = [set(m.skus) for m in metodos]
    comuns = conjuntos[0].intersection(*conjuntos[1:])
    return [s for s in metodos[0].skus if s in comuns]


# ---------------------------------------------------------------------------
# Amostra fixada
# ---------------------------------------------------------------------------


def selecionar_amostra(skus: list[str], n: int, seed: int) -> list[int]:
    """Retorna índices fixados pela seed — idêntico ao critério dos vetorizadores."""
    rng = random.Random(seed)
    return rng.sample(list(range(len(skus))), min(n, len(skus)))


# ---------------------------------------------------------------------------
# Top features por método
# ---------------------------------------------------------------------------


def top_features_esparso(
    dados: DadosMetodo,
    idx_produto: int,
    n: int,
) -> list[tuple[str | list[str], float]]:
    """Top features para BoW ou TF-IDF: maiores pesos na linha do produto."""
    assert dados.features is not None
    vetor = dados.matriz[idx_produto]
    top_idx = vetor.argsort()[::-1][:n]
    return [
        (dados.features[i], round(float(vetor[i]), 4)) for i in top_idx if vetor[i] > 0
    ]


def top_features_w2v(
    modelo,
    dados: DadosMetodo,
    idx_produto: int,
    n: int,
) -> list[tuple[str, float]]:
    """Top features para W2V: palavras do vocabulário mais próximas do embedding.

    Usa similar_by_vector do gensim para encontrar os termos cujo vetor
    tem maior similaridade de cosseno com o embedding do produto.
    """
    if modelo is None:
        return [("(modelo W2V indisponível)", 0.0)]

    embedding = dados.matriz[idx_produto]
    try:
        vizinhos = modelo.wv.similar_by_vector(embedding, topn=n)
        return [(token, round(float(score), 4)) for token, score in vizinhos]
    except Exception as exc:
        logger.warning("Erro ao calcular vizinhos W2V: %s", exc)
        return []


def obter_top_features(
    dados: DadosMetodo,
    idx_produto: int,
    n: int,
    modelo_w2v=None,
) -> list[tuple[str | list[str], float]] | list[tuple[str, float]]:
    """Despacha para a estratégia correta de acordo com o método."""
    if dados.features is not None:
        return top_features_esparso(dados, idx_produto, n)
    return top_features_w2v(modelo_w2v, dados, idx_produto, n)


# ---------------------------------------------------------------------------
# Similaridade por categoria
# ---------------------------------------------------------------------------


def calcular_similaridade_categorias(
    dados: DadosMetodo,
    metadata: dict[str, MetadatoProduto],
) -> tuple[np.ndarray, list[str]]:
    """Calcula similaridade média de cosseno entre pares de categorias.

    Para cada par (cat_A, cat_B), calcula a média das similaridades de cosseno
    entre todos os produtos de cat_A e todos os de cat_B.

    Retorna (matriz_categorias, lista_categorias_ordenadas).
    """
    categorias_por_sku = {
        sku: metadata[sku].categoria for sku in dados.skus if sku in metadata
    }
    categorias = sorted(set(categorias_por_sku.values()))
    n_cat = len(categorias)
    cat_idx = {c: i for i, c in enumerate(categorias)}

    # Agrupa índices de linha por categoria
    grupos: dict[str, list[int]] = {c: [] for c in categorias}
    for i, sku in enumerate(dados.skus):
        if sku in categorias_por_sku:
            grupos[categorias_por_sku[sku]].append(i)

    sim_total = cosine_similarity(dados.matriz)
    matriz_cat = np.zeros((n_cat, n_cat))

    for cat_a in categorias:
        for cat_b in categorias:
            idxs_a = grupos[cat_a]
            idxs_b = grupos[cat_b]
            if not idxs_a or not idxs_b:
                continue
            bloco = sim_total[np.ix_(idxs_a, idxs_b)]
            matriz_cat[cat_idx[cat_a], cat_idx[cat_b]] = bloco.mean()

    return matriz_cat, categorias


# ---------------------------------------------------------------------------
# Figura 1 — Top features por produto
# ---------------------------------------------------------------------------


def _label_produto(sku: str, metadata: dict[str, MetadatoProduto]) -> str:
    """Rótulo compacto para título de subplot: SKU + nome truncado."""
    if sku in metadata:
        nome = metadata[sku].nome
        nome_curto = nome[:32] + "…" if len(nome) > 32 else nome
        return f"SKU {sku} — {nome_curto}"
    return f"SKU {sku}"


def _cor_por_score(scores: list[float], cmap_name: str) -> list:
    """Mapeia scores normalizados para cores de um colormap."""
    cmap = plt.get_cmap(cmap_name)
    maximo = max(scores) if scores else 1.0
    return [cmap(0.35 + 0.65 * (s / maximo)) for s in scores]


PALETAS_METODO = {"BoW": "Blues", "TF-IDF": "Purples", "Word2Vec": "Oranges"}


def plotar_top_features(
    metodos: list[DadosMetodo],
    indices_amostra: list[int],
    metadata: dict[str, MetadatoProduto],
    modelo_w2v=None,
) -> Figure:
    """Gera a figura com subplots de barras horizontais: método × produto."""
    n_metodos = len(metodos)
    n_produtos = len(indices_amostra)

    fig, axes = plt.subplots(
        nrows=n_produtos,
        ncols=n_metodos,
        figsize=(6.5 * n_metodos, 3.2 * n_produtos),
    )
    fig.suptitle("Top features por produto", fontsize=15, fontweight="bold", y=1.01)

    # Garante que axes seja sempre 2D
    if n_produtos == 1:
        axes = axes[np.newaxis, :]
    if n_metodos == 1:
        axes = axes[:, np.newaxis]

    for col, dados in enumerate(metodos):
        paleta = PALETAS_METODO.get(dados.nome, "Greens")

        for row, idx in enumerate(indices_amostra):
            ax = axes[row, col]
            sku = dados.skus[idx]

            top = obter_top_features(dados, idx, N_TOP_FEATURES, modelo_w2v)
            if not top:
                ax.set_visible(False)
                continue

            features_plot = [f for f, _ in top]
            scores_plot = [s for _, s in top]
            cores = _cor_por_score(scores_plot, paleta)

            barras = ax.barh(
                range(len(features_plot)),
                scores_plot,
                color=cores,
                edgecolor="none",
                height=0.7,
            )

            ax.set_yticks(range(len(features_plot)))
            ax.set_yticklabels(features_plot, fontsize=9)
            ax.invert_yaxis()
            ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.3f"))
            ax.tick_params(axis="x", labelsize=8)

            # Valor numérico no final de cada barra
            for barra, score in zip(barras, scores_plot):
                ax.text(
                    barra.get_width() + barra.get_width() * 0.02,
                    barra.get_y() + barra.get_height() / 2,
                    f"{score:.3f}",
                    va="center",
                    fontsize=7.5,
                    color="#555555",
                )

            # Título da coluna apenas na primeira linha
            if row == 0:
                ax.set_title(dados.nome, fontsize=12, fontweight="bold", pad=8)

            # Rótulo do produto na margem esquerda apenas na primeira coluna
            if col == 0:
                ax.set_ylabel(
                    _label_produto(sku, metadata),
                    fontsize=8,
                    labelpad=6,
                )
            else:
                ax.set_ylabel("")

            ax.spines[["top", "right", "left"]].set_visible(False)
            ax.spines["bottom"].set_linewidth(0.5)
            ax.set_axisbelow(True)
            ax.xaxis.grid(True, linestyle="--", linewidth=0.4, alpha=0.6)

    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Figura 2 — Heatmap de similaridade por categoria
# ---------------------------------------------------------------------------


def plotar_heatmaps_categorias(
    metodos: list[DadosMetodo],
    metadata: dict[str, MetadatoProduto],
) -> Figure:
    """Gera a figura com três heatmaps de similaridade de categorias."""
    n_metodos = len(metodos)

    fig, axes = plt.subplots(
        nrows=1,
        ncols=n_metodos,
        figsize=(7.5 * n_metodos, 6.5),
    )
    fig.suptitle(
        "Similaridade de cosseno média entre categorias",
        fontsize=15,
        fontweight="bold",
        y=1.02,
    )

    if n_metodos == 1:
        axes = [axes]

    for ax, dados in zip(axes, metodos):
        mat_cat, categorias = calcular_similaridade_categorias(dados, metadata)

        sns.heatmap(
            mat_cat,
            ax=ax,
            xticklabels=categorias,
            yticklabels=categorias,
            annot=True,
            fmt=".2f",
            cmap="YlOrRd",
            linewidths=0.4,
            linecolor="#dddddd",
            vmin=0.0,
            vmax=1.0,
            cbar_kws={"shrink": 0.8, "label": "similaridade"},
            annot_kws={"size": 7},
        )

        ax.set_title(dados.nome, fontsize=12, fontweight="bold", pad=10)
        ax.tick_params(axis="x", labelsize=8, rotation=45)
        ax.tick_params(axis="y", labelsize=8, rotation=0)

    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Persistência das figuras
# ---------------------------------------------------------------------------


def salvar_figura(fig: Figure, caminho: Path | None) -> None:
    if caminho is None:
        return
    caminho.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(caminho, dpi=DPI, bbox_inches="tight")
    logger.info("Figura salva: %s", caminho)


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------


def carregar_todos_metodos() -> tuple[list[DadosMetodo], object]:
    """Carrega os três métodos e o modelo W2V. Retorna apenas os disponíveis."""
    bow = carregar_dados_metodo("BoW", BOW_MATRIX, BOW_SKUS, BOW_FEATURES, esparso=True)
    tfidf = carregar_dados_metodo(
        "TF-IDF", TFIDF_MATRIX, TFIDF_SKUS, TFIDF_FEATURES, esparso=True
    )
    w2v = carregar_dados_metodo("Word2Vec", W2V_MATRIX, W2V_SKUS, None, esparso=False)
    modelo_w2v = carregar_modelo_w2v(W2V_MODEL) if w2v else None

    metodos = [m for m in [bow, tfidf, w2v] if m is not None]
    if not metodos:
        logger.error(
            "Nenhum método disponível. Execute os scripts de vetorização primeiro."
        )
        sys.exit(1)

    return metodos, modelo_w2v


def alinhar_metodos(
    metodos: list[DadosMetodo],
    skus_master: list[str],
) -> list[DadosMetodo]:
    """Alinha todos os métodos ao conjunto master de SKUs."""
    alinhados = [alinhar_ao_corpus_master(m, skus_master) for m in metodos]
    skus_finais = skus_comuns(alinhados)
    logger.info("%d SKUs comuns entre todos os métodos disponíveis.", len(skus_finais))
    return [alinhar_ao_corpus_master(m, skus_finais) for m in alinhados]


def main() -> None:
    logger.info("Carregando artefatos…")
    metodos, modelo_w2v = carregar_todos_metodos()

    skus_master = (
        carregar_json(CORPUS_SKUS) if CORPUS_SKUS.exists() else metodos[0].skus
    )
    metodos = alinhar_metodos(metodos, skus_master)

    metadata = carregar_metadata_produtos(PRODUCTS_CSV) if PRODUCTS_CSV.exists() else {}
    indices_amostra = selecionar_amostra(metodos[0].skus, N_AMOSTRAS, SEED_AMOSTRA)

    logger.info("Gerando Figura 1 — top features por produto…")
    fig1 = plotar_top_features(metodos, indices_amostra, metadata, modelo_w2v)
    salvar_figura(fig1, SALVAR_FIGURA_1)

    logger.info("Gerando Figura 2 — heatmap de similaridade por categoria…")
    fig2 = plotar_heatmaps_categorias(metodos, metadata)
    salvar_figura(fig2, SALVAR_FIGURA_2)

    plt.show()
    logger.info("Visualização concluída.")


if __name__ == "__main__":
    main()
