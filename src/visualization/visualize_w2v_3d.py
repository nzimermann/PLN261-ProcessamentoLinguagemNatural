"""
Visualização 3D interativa de clusters Word2Vec — bebidas alcoólicas.

Lê os artefatos gerados por vectorize_w2v.py:
    models/w2v/w2v_matrix.npy
    models/w2v/w2v_skus.json
    models/w2v/word2vec.model

Reduz a matriz de embeddings para 3D (UMAP ou t-SNE como fallback) e
renderiza cada produto como um ponto colorido por categoria de bebida
usando Plotly para visualização 3D interativa no navegador.

Saída:
    reports/clusters_w2v_3d.html   — visualização interativa

Uso:
    pip install umap-learn plotly numpy gensim
    python -m src.visualization.visualize_w2v_3d
"""

import json
import sys

import numpy as np
from gensim.models import Word2Vec

from src import logger
from src.config import PROCESSED_DIR, REPORTS_DIR, CORPUS_DIR, VECTORS_W2V

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

CORPUS_JSONL = CORPUS_DIR / "corpus_w2v.jsonl"

MATRIX_PATH = VECTORS_W2V / "w2v_matrix.npy"
SKUS_PATH = VECTORS_W2V / "w2v_skus.json"
MODEL_PATH = VECTORS_W2V / "word2vec.model"
PRODUCTS_CSV = PROCESSED_DIR / "products.csv"
OUTPUT_HTML = REPORTS_DIR / "clusters_w2v_3d.html"

CAMPO_CATEGORIA = "categoria"

# Método de redução de dimensionalidade: "umap" ou "tsne"
METODO_REDUCAO: str = "umap"

# Hiperparâmetros UMAP
UMAP_N_NEIGHBORS: int = 15
UMAP_MIN_DIST: float = 0.1
UMAP_METRIC: str = "cosine"

# Hiperparâmetros t-SNE (fallback)
TSNE_PERPLEXITY: int = 30
TSNE_MAX_ITER: int = 1000
TSNE_METRIC: str = "cosine"

SEED: int = 42

# Tamanho dos pontos no scatter 3D
PONTO_TAMANHO: int = 3
PONTO_OPACIDADE: float = 0.8

# ---------------------------------------------------------------------------
# Categorias e paleta de cores
# ---------------------------------------------------------------------------

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

PALETA: list[str] = [
    "#4C8EDA",  # Gin
    "#C90057",  # Whisky
    "#6DC0C0",  # Vodka
    "#C45E3E",  # Rum
    "#8BC34A",  # Cachaça
    "#9B59B6",  # Vinho
    "#F06292",  # Espumante
    "#FFA726",  # Cerveja
    "#26A69A",  # Licor
    "#78909C",  # Conhaque
    "#EF5350",  # Tequila
    "#FF0000",  # Absinto
]

COR_DESCONHECIDA: str = "#CCCCCC"


# ---------------------------------------------------------------------------
# Carregamento dos artefatos
# ---------------------------------------------------------------------------


def carregar_artefatos() -> tuple[np.ndarray, list[str], Word2Vec]:
    """Carrega a matriz de embeddings, os SKUs e o modelo Word2Vec."""
    for caminho in [MATRIX_PATH, SKUS_PATH, MODEL_PATH]:
        if not caminho.exists():
            logger.error("Arquivo não encontrado: '%s'", caminho)
            logger.error(
                "Execute vectorize_w2v.py antes desta etapa para gerar os artefatos."
            )
            sys.exit(1)

    matriz = np.load(str(MATRIX_PATH))
    logger.info("Matriz carregada: shape=%s  dtype=%s", matriz.shape, matriz.dtype)

    with open(SKUS_PATH, encoding="utf-8") as f:
        skus: list[str] = json.load(f)
    logger.info("SKUs carregados: %d entradas", len(skus))

    modelo = Word2Vec.load(str(MODEL_PATH))
    logger.info("Modelo W2V carregado: %d tokens no vocabulário", len(modelo.wv))

    if len(skus) != matriz.shape[0]:
        logger.error(
            "Inconsistência: %d SKUs mas %d linhas na matriz.",
            len(skus),
            matriz.shape[0],
        )
        sys.exit(1)

    return matriz, skus, modelo


# ---------------------------------------------------------------------------
# Categorização
# ---------------------------------------------------------------------------


def tentar_categorias_do_corpus(skus: list[str]) -> dict[str, str] | None:
    """Tenta mapear SKU → categoria lendo o campo CAMPO_CATEGORIA do corpus."""
    if not CORPUS_JSONL.exists():
        logger.info("Corpus '%s' não encontrado.", CORPUS_JSONL)
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
        logger.info("Campo '%s' não encontrado no corpus.", CAMPO_CATEGORIA)
        return None

    cobertura = sum(1 for sku in skus if sku in mapa)
    logger.info("Categorias do corpus: %d/%d SKUs cobertos.", cobertura, len(skus))
    return mapa


def _centroide_categoria(modelo: Word2Vec, termos: list[str]) -> np.ndarray | None:
    """Calcula o centroide dos vetores dos termos de uma categoria."""
    vetores = [modelo.wv[t] for t in termos if t in modelo.wv]
    if not vetores:
        return None
    return np.mean(np.array(vetores), axis=0)


def inferir_categorias_por_centroide(
    matriz: np.ndarray,
    skus: list[str],
    modelo: Word2Vec,
) -> dict[str, str]:
    """Infere a categoria de cada produto por similaridade de cosseno ao centroide."""
    centroides: dict[str, np.ndarray] = {}
    for nome_cat, termos in CATEGORIAS.items():
        centroide = _centroide_categoria(modelo, termos)
        if centroide is not None:
            centroides[nome_cat] = centroide
        else:
            logger.warning("Categoria '%s' sem termos no vocabulário W2V.", nome_cat)

    if not centroides:
        logger.error("Nenhuma categoria pôde ser construída.")
        sys.exit(1)

    nomes_cats = list(centroides.keys())
    mat_centroides = np.array([centroides[c] for c in nomes_cats])

    normas_matriz = np.linalg.norm(matriz, axis=1, keepdims=True)
    normas_centroides = np.linalg.norm(mat_centroides, axis=1, keepdims=True)

    normas_matriz = np.where(normas_matriz == 0, 1e-10, normas_matriz)
    normas_centroides = np.where(normas_centroides == 0, 1e-10, normas_centroides)

    matriz_norm = matriz / normas_matriz
    centroides_norm = mat_centroides / normas_centroides

    similaridades = matriz_norm @ centroides_norm.T
    indices_melhor = np.argmax(similaridades, axis=1)

    mapa: dict[str, str] = {}
    for i, sku in enumerate(skus):
        mapa[sku] = str(nomes_cats[indices_melhor[i]])

    contagem = {}
    for cat in mapa.values():
        contagem[cat] = contagem.get(cat, 0) + 1
    logger.info("Distribuição inferida: %s", contagem)

    return mapa


# ---------------------------------------------------------------------------
# Redução de dimensionalidade para 3D
# ---------------------------------------------------------------------------


def reduzir_para_3d(matriz: np.ndarray) -> np.ndarray:
    """Reduz a matriz de embeddings para 3D usando UMAP ou t-SNE."""
    if METODO_REDUCAO == "umap":
        try:
            import umap  # noqa: PLC0415 # pyright: ignore [reportMissingImports]

            logger.info(
                "Reduzindo para 3D com UMAP (n_neighbors=%d, min_dist=%.2f, metric=%s)…",
                UMAP_N_NEIGHBORS,
                UMAP_MIN_DIST,
                UMAP_METRIC,
            )
            reducer = umap.UMAP(
                n_components=3,
                n_neighbors=UMAP_N_NEIGHBORS,
                min_dist=UMAP_MIN_DIST,
                metric=UMAP_METRIC,
                random_state=SEED,
            )
            return np.asarray(reducer.fit_transform(matriz))

        except ImportError:
            logger.warning(
                "umap-learn não instalado. Usando t-SNE como fallback. "
                "Para instalar: pip install umap-learn"
            )

    logger.info(
        "Reduzindo para 3D com t-SNE (perplexity=%d, max_iter=%d, metric=%s)…",
        TSNE_PERPLEXITY,
        TSNE_MAX_ITER,
        TSNE_METRIC,
    )
    from sklearn.manifold import TSNE  # noqa: PLC0415

    tsne = TSNE(
        n_components=3,
        perplexity=min(TSNE_PERPLEXITY, matriz.shape[0] - 1),
        max_iter=TSNE_MAX_ITER,
        metric=TSNE_METRIC,
        random_state=SEED,
        init="random",  # PCA init não suporta n_components=3 com métrica cosine
    )
    return tsne.fit_transform(matriz)


# ---------------------------------------------------------------------------
# Carregamento de dados dos produtos (para tooltip)
# ---------------------------------------------------------------------------


def carregar_produtos_csv() -> dict[str, dict]:
    """Carrega products.csv e retorna um dicionário SKU → info do produto."""
    import csv  # noqa: PLC0415

    produtos: dict[str, dict] = {}
    if not PRODUCTS_CSV.exists():
        logger.warning("products.csv não encontrado em %s", PRODUCTS_CSV)
        return produtos

    with open(PRODUCTS_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = str(row.get("sku", ""))
            produtos[sku] = {
                "name": row.get("name", ""),
                "category": row.get("category", ""),
                "brand": row.get("brand", ""),
                "description": row.get("description", ""),
            }

    logger.info("Produtos carregados do CSV: %d entradas", len(produtos))
    return produtos


def _truncar(texto: str, max_chars: int = 80) -> str:
    """Trunca texto longo adicionando reticências."""
    if len(texto) <= max_chars:
        return texto
    return texto[:max_chars].rstrip() + "…"


# ---------------------------------------------------------------------------
# Visualização 3D com Plotly
# ---------------------------------------------------------------------------


def plotar_clusters_3d(
    coords_3d: np.ndarray,
    skus: list[str],
    mapa_sku_categoria: dict[str, str],
) -> None:
    """Gera o scatter plot 3D interativo e salva como HTML."""
    import plotly.graph_objects as go  # noqa: PLC0415

    # Carrega info dos produtos para o tooltip
    produtos = carregar_produtos_csv()

    cats_canonicas = list(CATEGORIAS.keys())

    # Monta mapa de cores
    mapa_cores: dict[str, str] = {}
    for cat in cats_canonicas:
        idx = cats_canonicas.index(cat)
        mapa_cores[cat] = PALETA[idx % len(PALETA)]

    # Agrupa pontos por categoria
    categorias_por_sku = [mapa_sku_categoria.get(sku, "Desconhecido") for sku in skus]

    cats_presentes = sorted(set(categorias_por_sku))

    fig = go.Figure()

    for cat in cats_presentes:
        indices = [i for i, c in enumerate(categorias_por_sku) if c == cat]
        cor = mapa_cores.get(cat, COR_DESCONHECIDA)

        # Constrói hover text rico para cada ponto
        hover_texts = []
        for i in indices:
            sku = skus[i]
            cat_modelo = mapa_sku_categoria.get(sku, "Desconhecido")
            info = produtos.get(sku, {})
            cat_real = info.get("category", "N/A")
            nome = info.get("name", "N/A")
            descricao = _truncar(info.get("description", ""), 100)

            hover = (
                f"<b>SKU:</b> {sku}<br>"
                f"<b>Nome:</b> {nome}<br>"
                f"<b>Categoria Modelo:</b> {cat_modelo}<br>"
                f"<b>Categoria Real:</b> {cat_real}<br>"
                f"<b>Descrição:</b> {descricao}"
            )
            hover_texts.append(hover)

        fig.add_trace(
            go.Scatter3d(
                x=coords_3d[indices, 0],
                y=coords_3d[indices, 1],
                z=coords_3d[indices, 2],
                mode="markers",
                name=f"{cat} ({len(indices)})",
                marker=dict(
                    size=PONTO_TAMANHO,
                    color=cor,
                    opacity=PONTO_OPACIDADE,
                    line=dict(width=0),
                ),
                hovertext=hover_texts,
                hoverinfo="text",
            )
        )

    metodo_label = METODO_REDUCAO.upper()
    total_produtos = len(skus)
    total_cats = len(cats_presentes)

    fig.update_layout(
        title=dict(
            text=(
                f"Mapa Semântico 3D de Bebidas — Word2Vec + {metodo_label}<br>"
                f"<sub>{total_produtos} produtos · {total_cats} categorias</sub>"
            ),
            x=0.5,
            font=dict(size=16, color="#E8E8F8"),
        ),
        scene=dict(
            xaxis=dict(
                title=f"{metodo_label} — Dim 1",
                backgroundcolor="#0F1117",
                gridcolor="#1E1E2E",
                showbackground=True,
                zerolinecolor="#2A2A3A",
            ),
            yaxis=dict(
                title=f"{metodo_label} — Dim 2",
                backgroundcolor="#0F1117",
                gridcolor="#1E1E2E",
                showbackground=True,
                zerolinecolor="#2A2A3A",
            ),
            zaxis=dict(
                title=f"{metodo_label} — Dim 3",
                backgroundcolor="#0F1117",
                gridcolor="#1E1E2E",
                showbackground=True,
                zerolinecolor="#2A2A3A",
            ),
        ),
        paper_bgcolor="#0F1117",
        plot_bgcolor="#0F1117",
        font=dict(color="#E8E8E8"),
        legend=dict(
            bgcolor="rgba(26, 26, 46, 0.8)",
            bordercolor="#3A3A5A",
            borderwidth=1,
            font=dict(size=11, color="#E8E8E8"),
            traceorder="normal",
            itemsizing="constant",
            yanchor="top",
            y=0.95,
            xanchor="left",
            x=0.01,
        ),
        margin=dict(l=0, r=0, b=0, t=60),
    )

    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(OUTPUT_HTML), include_plotlyjs="cdn")
    logger.info("Visualização 3D salva: %s", OUTPUT_HTML)

    fig.show()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    print("\n" + "=" * 60)
    print("VISUALIZAÇÃO 3D — WORD2VEC CLUSTERS")
    print("=" * 60)

    # 1. Carregamento
    print("\n[1/4] Carregando artefatos…")
    matriz, skus, modelo = carregar_artefatos()
    print(
        f"      Produtos: {len(skus)}  |  Dimensões: {matriz.shape[1]}  |  Vocab: {len(modelo.wv)}"
    )

    # 2. Categorização
    print("\n[2/4] Categorizando produtos…")
    mapa = tentar_categorias_do_corpus(skus)
    if mapa is None:
        print("      Estratégia: inferência por centroide")
        mapa = inferir_categorias_por_centroide(matriz, skus, modelo)
    else:
        print(f"      Estratégia: campo '{CAMPO_CATEGORIA}' do corpus")

    contagem: dict[str, int] = {}
    for cat in mapa.values():
        contagem[cat] = contagem.get(cat, 0) + 1
    for cat, n in sorted(contagem.items(), key=lambda x: -x[1]):
        print(f"      {cat:<15} {n:>4}")

    # 3. Redução para 3D
    print(f"\n[3/4] Reduzindo para 3D ({METODO_REDUCAO.upper()})…")
    coords_3d = reduzir_para_3d(matriz)
    print(f"      Shape final: {coords_3d.shape}")

    # 4. Visualização
    print(f"\n[4/4] Gerando visualização 3D interativa…")
    plotar_clusters_3d(coords_3d, skus, mapa)
    print(f"      Salvo em: {OUTPUT_HTML}")
    print("\n" + "=" * 60)
    print("Concluído! Abra o arquivo HTML no navegador para interagir.")
    print("=" * 60)


if __name__ == "__main__":
    main()
