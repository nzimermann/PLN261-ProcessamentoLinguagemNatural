"""
Visualização de clusters Word2Vec — mapa 2D de bebidas alcoólicas.

Lê os artefatos gerados por vetorizar_w2v.py:
    data/processed/vectors/w2v/w2v_matrix.npy
    data/processed/vectors/w2v/w2v_skus.json
    data/processed/vectors/w2v/word2vec.model

Reduz a matriz de embeddings para 2D (UMAP ou t-SNE como fallback) e
plota cada produto como um ponto colorido por categoria de bebida.

Estratégia de categorização (em ordem de preferência):
    1. Campo de categoria no corpus_w2v.jsonl  (ex.: "categoria": "Gin")
    2. Inferência por centroide — calcula a similaridade de cosseno entre
       o vetor do produto e o centroide de cada categoria no espaço W2V.

Saída:
    data/processed/vectors/w2v/clusters_w2v.png   — figura em alta resolução

Uso:
    pip install umap-learn matplotlib numpy
    python visualizar_clusters_w2v.py
"""

import json
import logging
import sys
from pathlib import Path
from src.config import DATA_DIR

import numpy as np
from gensim.models import Word2Vec

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

VECTORS_DIR = DATA_DIR / "processed" / "vectors" / "w2v"
CORPUS_JSONL = DATA_DIR / "processed" / "corpus" / "corpus_w2v.jsonl"

MATRIX_PATH = VECTORS_DIR / "w2v_matrix.npy"
SKUS_PATH = VECTORS_DIR / "w2v_skus.json"
MODEL_PATH = VECTORS_DIR / "word2vec.model"
OUTPUT_PNG = VECTORS_DIR / "clusters_w2v.png"

# Campo buscado no corpus_w2v.jsonl para obter a categoria do produto.
# Ajuste se o seu corpus usar um nome diferente (ex.: "tipo", "classe").
CAMPO_CATEGORIA = "categoria"

# Método de redução de dimensionalidade.
# "umap"  — mais rápido, preserva melhor a estrutura global dos clusters.
# "tsne"  — alternativa clássica; mais lento para corpora grandes.
METODO_REDUCAO: str = "umap"

# Hiperparâmetros UMAP
UMAP_N_NEIGHBORS: int = 15  # Vizinhos locais — controla local vs global
UMAP_MIN_DIST: float = 0.1  # Compactação interna dos clusters
UMAP_METRIC: str = "cosine"  # Cosine é ideal para embeddings

# Hiperparâmetros t-SNE (usado apenas se METODO_REDUCAO = "tsne")
TSNE_PERPLEXITY: int = 30
TSNE_MAX_ITER: int = 1000
TSNE_METRIC: str = "cosine"

# Seed para reprodutibilidade da redução
SEED: int = 42

# Dimensões da figura em polegadas
FIGURE_WIDTH: float = 16.0
FIGURE_HEIGHT: float = 11.0

# Resolução de exportação (DPI)
EXPORT_DPI: int = 180

# Tamanho e transparência dos pontos no scatter
PONTO_TAMANHO: int = 18
PONTO_ALPHA: float = 0.72

# ---------------------------------------------------------------------------
# Categorias e paleta de cores
# ---------------------------------------------------------------------------

# Mapeamento: nome canônico da categoria → termos de referência no vocabulário W2V.
# Esses termos são usados para construir o centroide da categoria quando
# a categorização via corpus não está disponível.
# Adicione ou remova categorias conforme o seu catálogo de produtos.
CATEGORIAS: dict[str, list[str]] = {
    "Gin": ["gin", "zimbro", "london", "dry", "beefeater", "hendrick"],
    "Whisky": ["whisky", "whiskey", "bourbon", "scotch", "malte", "turfa"],
    "Vodka": ["vodka", "absolut", "russo", "belvedere", "stolichnaya"],
    "Rum": ["rum", "havana", "caribenho", "havano", "kraken"],
    "Cachaça": ["cachaça", "aguardente", "caipirinha", "alambique"],
    "Vinho": [
        "vinho",
        "tinto",
        "branco",
        "rosé",
        "malbec",
        "merlot",
        "cabernet",
        "chardonnay",
    ],
    "Espumante": ["espumante", "prosecco", "champagne", "cava", "brut", "cuvée"],
    "Cerveja": ["cerveja", "lager", "ale", "ipa", "pilsen", "malte", "lupulo"],
    "Licor": ["licor", "amaretto", "cointreau", "baileys", "triple", "sec"],
    "Conhaque": ["conhaque", "cognac", "brandy", "armagnac", "vsop", "xo"],
    "Tequila": ["tequila", "mezcal", "agave", "blanco", "reposado", "añejo"],
    "Absinto": ["absinto", "absinthe", "anis", "artemísia", "pastis"],
}

# Paleta de cores — uma cor distinta por categoria.
# Ordem alinhada com a inserção do dicionário CATEGORIAS acima.
PALETA: list[str] = [
    "#4C8EDA",  # Gin        — azul médio
    "#E8A838",  # Whisky     — âmbar
    "#6DC0C0",  # Vodka      — ciano gelado
    "#C45E3E",  # Rum        — terracota
    "#8BC34A",  # Cachaça    — verde folha
    "#9B59B6",  # Vinho      — roxo uva
    "#F06292",  # Espumante  — rosa champagne
    "#FFA726",  # Cerveja    — dourado cevada
    "#26A69A",  # Licor      — verde-azulado
    "#78909C",  # Conhaque   — cinza cobre
    "#EF5350",  # Tequila    — vermelho agave
    "#AB47BC",  # Absinto    — lilás
]

COR_DESCONHECIDA: str = "#CCCCCC"  # Produtos não categorizados

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
# Carregamento dos artefatos
# ---------------------------------------------------------------------------


def carregar_artefatos() -> tuple[np.ndarray, list[str], Word2Vec]:
    """Carrega a matriz de embeddings, os SKUs e o modelo Word2Vec."""
    for caminho in [MATRIX_PATH, SKUS_PATH, MODEL_PATH]:
        if not caminho.exists():
            log.error("Arquivo não encontrado: '%s'", caminho)
            log.error(
                "Execute vetorizar_w2v.py antes desta etapa para gerar os artefatos."
            )
            sys.exit(1)

    matriz = np.load(str(MATRIX_PATH))
    log.info("Matriz carregada: shape=%s  dtype=%s", matriz.shape, matriz.dtype)

    with open(SKUS_PATH, encoding="utf-8") as f:
        skus: list[str] = json.load(f)
    log.info("SKUs carregados: %d entradas", len(skus))

    modelo = Word2Vec.load(str(MODEL_PATH))
    log.info("Modelo W2V carregado: %d tokens no vocabulário", len(modelo.wv))

    if len(skus) != matriz.shape[0]:
        log.error(
            "Inconsistência: %d SKUs mas %d linhas na matriz.",
            len(skus),
            matriz.shape[0],
        )
        sys.exit(1)

    return matriz, skus, modelo


# ---------------------------------------------------------------------------
# Estratégia 1 — categorias via corpus_w2v.jsonl
# ---------------------------------------------------------------------------


def tentar_categorias_do_corpus(skus: list[str]) -> dict[str, str] | None:
    """Tenta mapear SKU → categoria lendo o campo CAMPO_CATEGORIA do corpus.

    Retorna None se o corpus não existir ou não contiver o campo esperado.
    """
    if not CORPUS_JSONL.exists():
        log.info(
            "Corpus '%s' não encontrado — pulando leitura de categorias.",
            CORPUS_JSONL,
        )
        return None

    mapa: dict[str, str] = {}
    encontrou_campo = False

    with open(CORPUS_JSONL, encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if not linha:
                continue
            try:
                registro = json.loads(linha)
            except json.JSONDecodeError:
                continue

            sku = str(registro.get("sku", ""))
            categoria = registro.get(CAMPO_CATEGORIA)

            if categoria is not None:
                encontrou_campo = True
                mapa[sku] = str(categoria)

    if not encontrou_campo:
        log.info(
            "Campo '%s' não encontrado no corpus — usando inferência por centroide.",
            CAMPO_CATEGORIA,
        )
        return None

    cobertura = sum(1 for sku in skus if sku in mapa)
    log.info(
        "Categorias do corpus: %d/%d SKUs cobertos (campo '%s').",
        cobertura,
        len(skus),
        CAMPO_CATEGORIA,
    )
    return mapa


# ---------------------------------------------------------------------------
# Estratégia 2 — inferência por centroide no espaço W2V
# ---------------------------------------------------------------------------


def _centroide_categoria(
    modelo: Word2Vec,
    termos: list[str],
) -> np.ndarray | None:
    """Calcula o centroide (média) dos vetores dos termos de uma categoria.

    Ignora termos ausentes do vocabulário. Retorna None se nenhum termo
    da lista estiver no vocabulário — categoria sem representação no corpus.
    """
    vetores = [modelo.wv[t] for t in termos if t in modelo.wv]
    if not vetores:
        return None
    return np.mean(np.array(vetores), axis=0)


def _similaridade_cosseno(a: np.ndarray, b: np.ndarray) -> float:
    """Similaridade de cosseno entre dois vetores."""
    norma_a = np.linalg.norm(a)
    norma_b = np.linalg.norm(b)
    if norma_a == 0 or norma_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norma_a * norma_b))


def inferir_categorias_por_centroide(
    matriz: np.ndarray,
    skus: list[str],
    modelo: Word2Vec,
) -> dict[str, str]:
    """Infere a categoria de cada produto por similaridade de cosseno ao centroide.

    Para cada produto, computa a similaridade entre seu embedding e o centroide
    de cada categoria. A categoria com maior similaridade é atribuída.

    Produtos com similaridade máxima abaixo de 0.0 ficam como "Desconhecido".
    """
    # Constrói centroides apenas para categorias com representação no vocabulário
    centroides: dict[str, np.ndarray] = {}
    for nome_cat, termos in CATEGORIAS.items():
        centroide = _centroide_categoria(modelo, termos)
        if centroide is not None:
            centroides[nome_cat] = centroide
        else:
            log.warning(
                "Categoria '%s' sem termos no vocabulário W2V — ignorada na inferência.",
                nome_cat,
            )

    if not centroides:
        log.error("Nenhuma categoria pôde ser construída com o vocabulário atual.")
        sys.exit(1)

    nomes_cats = list(centroides.keys())
    mat_centroides = np.array([centroides[c] for c in nomes_cats])  # (K, D)

    # Normaliza embeddings para cosseno eficiente via produto escalar
    normas_matriz = np.linalg.norm(matriz, axis=1, keepdims=True)
    normas_centroides = np.linalg.norm(mat_centroides, axis=1, keepdims=True)

    # Evita divisão por zero
    normas_matriz = np.where(normas_matriz == 0, 1e-10, normas_matriz)
    normas_centroides = np.where(normas_centroides == 0, 1e-10, normas_centroides)

    matriz_norm = matriz / normas_matriz  # (N, D)
    centroides_norm = mat_centroides / normas_centroides  # (K, D)

    # Similaridade: (N, D) × (D, K) → (N, K)
    similaridades = matriz_norm @ centroides_norm.T

    indices_melhor = np.argmax(similaridades, axis=1)

    mapa: dict[str, str] = {}
    for i, sku in enumerate(skus):
        mapa[sku] = nomes_cats[indices_melhor[i]]

    contagem = {}
    for cat in mapa.values():
        contagem[cat] = contagem.get(cat, 0) + 1
    log.info("Distribuição inferida: %s", contagem)

    return mapa


# ---------------------------------------------------------------------------
# Redução de dimensionalidade
# ---------------------------------------------------------------------------


def reduzir_para_2d(matriz: np.ndarray) -> np.ndarray:
    """Reduz a matriz de embeddings para 2D usando UMAP ou t-SNE.

    UMAP é preferido por ser mais rápido e preservar melhor a estrutura
    global dos clusters. t-SNE é usado como fallback caso umap-learn não
    esteja instalado.
    """
    if METODO_REDUCAO == "umap":
        try:
            import umap  # noqa: PLC0415

            log.info(
                "Reduzindo com UMAP (n_neighbors=%d, min_dist=%.2f, metric=%s)…",
                UMAP_N_NEIGHBORS,
                UMAP_MIN_DIST,
                UMAP_METRIC,
            )
            reducer = umap.UMAP(
                n_components=2,
                n_neighbors=UMAP_N_NEIGHBORS,
                min_dist=UMAP_MIN_DIST,
                metric=UMAP_METRIC,
                random_state=SEED,
            )
            return np.asarray(reducer.fit_transform(matriz))

        except ImportError:
            log.warning(
                "umap-learn não instalado. Usando t-SNE como fallback. "
                "Para instalar: pip install umap-learn"
            )

    log.info(
        "Reduzindo com t-SNE (perplexity=%d, max_iter=%d, metric=%s)…",
        TSNE_PERPLEXITY,
        TSNE_MAX_ITER,
        TSNE_METRIC,
    )
    from sklearn.manifold import TSNE  # noqa: PLC0415

    tsne = TSNE(
        n_components=2,
        perplexity=min(TSNE_PERPLEXITY, matriz.shape[0] - 1),
        max_iter=TSNE_MAX_ITER,
        metric=TSNE_METRIC,
        random_state=SEED,
        init="pca",
    )
    return tsne.fit_transform(matriz)


# ---------------------------------------------------------------------------
# Visualização
# ---------------------------------------------------------------------------


def _cor_categoria(nome: str, mapa_cores: dict[str, str]) -> str:
    return mapa_cores.get(nome, COR_DESCONHECIDA)


def plotar_clusters(
    coords_2d: np.ndarray,
    skus: list[str],
    mapa_sku_categoria: dict[str, str],
) -> None:
    """Gera o scatter plot 2D dos clusters de bebidas e salva em PNG.

    Cada categoria recebe uma cor distinta definida em PALETA. O gráfico
    exibe todas as categorias presentes nos dados com uma legenda interativa
    e informações de contexto (método de redução, número de produtos).
    """
    import matplotlib.pyplot as plt
    import matplotlib.patheffects as pe
    from matplotlib.lines import Line2D

    # Constrói o mapeamento categoria → cor a partir das categorias presentes
    cats_presentes = sorted(set(mapa_sku_categoria.values()))
    cats_canonicas = list(CATEGORIAS.keys())

    mapa_cores: dict[str, str] = {}
    for cat in cats_presentes:
        if cat in cats_canonicas:
            idx = cats_canonicas.index(cat)
            mapa_cores[cat] = PALETA[idx % len(PALETA)]
        else:
            mapa_cores[cat] = COR_DESCONHECIDA

    # Estilo do gráfico
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.facecolor": "#0F1117",
            "figure.facecolor": "#0F1117",
            "text.color": "#E8E8E8",
            "axes.labelcolor": "#E8E8E8",
            "xtick.color": "#888888",
            "ytick.color": "#888888",
            "axes.edgecolor": "#2A2A3A",
            "grid.color": "#1E1E2E",
            "grid.linewidth": 0.6,
        }
    )

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH, FIGURE_HEIGHT))
    fig.patch.set_facecolor("#0F1117")

    # Grade sutil ao fundo
    ax.grid(True, linestyle="--", alpha=0.35, zorder=0)

    # Plota cada categoria separadamente para controle de cor e legenda
    contagem_por_cat: dict[str, int] = {}
    for cat in cats_presentes:
        indices = [
            i for i, sku in enumerate(skus) if mapa_sku_categoria.get(sku) == cat
        ]
        if not indices:
            continue

        xs = coords_2d[indices, 0]
        ys = coords_2d[indices, 1]
        cor = mapa_cores[cat]
        contagem_por_cat[cat] = len(indices)

        ax.scatter(
            xs,
            ys,
            s=PONTO_TAMANHO,
            color=cor,
            alpha=PONTO_ALPHA,
            linewidths=0.0,
            zorder=2,
            label=cat,
            rasterized=True,
        )

    # Legenda personalizada com contagem de produtos por categoria
    elementos_legenda = []
    for cat in cats_presentes:
        if cat not in contagem_por_cat:
            continue
        cor = mapa_cores[cat]
        n = contagem_por_cat[cat]
        marcador = Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor=cor,
            markeredgecolor="none",
            markersize=8,
            label=f"{cat}  ({n})",
        )
        elementos_legenda.append(marcador)

    legenda = ax.legend(
        handles=elementos_legenda,
        loc="upper left",
        frameon=True,
        framealpha=0.25,
        facecolor="#1A1A2E",
        edgecolor="#3A3A5A",
        fontsize=9.5,
        labelcolor="#E8E8E8",
        title="Categorias",
        title_fontsize=10,
        borderpad=0.9,
        labelspacing=0.55,
    )
    legenda.get_title().set_color("#AAAACC")

    # Título e rótulos dos eixos
    metodo_label = METODO_REDUCAO.upper()
    total_produtos = len(skus)
    total_cats = len(cats_presentes)

    ax.set_title(
        f"Mapa Semântico de Bebidas — Word2Vec + {metodo_label}",
        fontsize=15,
        fontweight="bold",
        color="#E8E8F8",
        pad=16,
    )
    ax.set_xlabel(f"{metodo_label} — Dimensão 1", fontsize=10, labelpad=8)
    ax.set_ylabel(f"{metodo_label} — Dimensão 2", fontsize=10, labelpad=8)

    # Rodapé com metadados
    rodape = (
        f"{total_produtos} produtos  ·  {total_cats} categorias  ·  "
        f"embeddings {coords_2d.shape[0]}×{coords_2d.shape[1]}  ·  "
        f"redução: {metodo_label}"
    )
    fig.text(
        0.5,
        0.012,
        rodape,
        ha="center",
        fontsize=8.5,
        color="#666688",
        style="italic",
    )

    ax.tick_params(labelsize=8)

    plt.tight_layout(rect=(0, 0.03, 1, 1))

    VECTORS_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        str(OUTPUT_PNG),
        dpi=EXPORT_DPI,
        bbox_inches="tight",
        facecolor=fig.get_facecolor(),
    )
    log.info("Gráfico salvo: %s  (%d DPI)", OUTPUT_PNG, EXPORT_DPI)

    plt.show()


# ---------------------------------------------------------------------------
# Seções de execução
# ---------------------------------------------------------------------------


def secao_carregamento() -> tuple[np.ndarray, list[str], Word2Vec]:
    print("\n" + "=" * 60)
    print("SEÇÃO 1 — CARREGAMENTO DOS ARTEFATOS")
    print("=" * 60)

    matriz, skus, modelo = carregar_artefatos()

    print(f"Produtos   : {len(skus)}")
    print(f"Dimensões  : {matriz.shape[1]}")
    print(f"Vocabulário: {len(modelo.wv)} tokens")

    return matriz, skus, modelo


def secao_categorizacao(
    matriz: np.ndarray,
    skus: list[str],
    modelo: Word2Vec,
) -> dict[str, str]:
    print("\n" + "=" * 60)
    print("SEÇÃO 2 — CATEGORIZAÇÃO DOS PRODUTOS")
    print("=" * 60)

    mapa = tentar_categorias_do_corpus(skus)

    if mapa is None:
        print("Estratégia: inferência por centroide no espaço Word2Vec.")
        mapa = inferir_categorias_por_centroide(matriz, skus, modelo)
    else:
        print(f"Estratégia: campo '{CAMPO_CATEGORIA}' do corpus_w2v.jsonl.")

    # Estatísticas de distribuição
    contagem: dict[str, int] = {}
    for cat in mapa.values():
        contagem[cat] = contagem.get(cat, 0) + 1

    print(f"\nDistribuição de produtos por categoria:")
    for cat, n in sorted(contagem.items(), key=lambda x: -x[1]):
        barra = "█" * min(n // 2, 40)
        print(f"  {cat:<15} {n:>4}  {barra}")

    return mapa


def secao_reducao(matriz: np.ndarray) -> np.ndarray:
    print("\n" + "=" * 60)
    print("SEÇÃO 3 — REDUÇÃO DE DIMENSIONALIDADE")
    print("=" * 60)
    print(f"Método     : {METODO_REDUCAO.upper()}")
    print(f"Entrada    : {matriz.shape}")
    print(f"Saída      : ({matriz.shape[0]}, 2)")
    print("Processando…")

    coords = reduzir_para_2d(matriz)

    print(f"Concluído. Shape final: {coords.shape}")
    return coords


def secao_visualizacao(
    coords_2d: np.ndarray,
    skus: list[str],
    mapa_sku_categoria: dict[str, str],
) -> None:
    print("\n" + "=" * 60)
    print("SEÇÃO 4 — VISUALIZAÇÃO")
    print("=" * 60)
    print(f"Gerando scatter plot…  ({OUTPUT_PNG})")

    plotar_clusters(coords_2d, skus, mapa_sku_categoria)

    print("Visualização concluída.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    matriz, skus, modelo = secao_carregamento()
    mapa_sku_categoria = secao_categorizacao(matriz, skus, modelo)
    coords_2d = secao_reducao(matriz)
    secao_visualizacao(coords_2d, skus, mapa_sku_categoria)


if __name__ == "__main__":
    main()
