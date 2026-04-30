"""
Visualização 3D interativa de clusters BERTugues — bebidas alcoólicas.

Lê os artefatos gerados por vectorize_bert.py:
    models/bert/bert_matrix.npy
    models/bert/bert_skus.json

Lê metadados dos produtos para tooltip e categorização:
    data/processed/products.csv

Reduz a matriz de 768 dimensões para 3D (UMAP ou t-SNE como fallback) e
renderiza cada produto como um ponto colorido por categoria usando Plotly.

Diferente do W2V, o BERT não precisa de um modelo carregado para inferir
categorias — a coluna `category` do CSV original é usada diretamente, com
normalização dos nomes para o padrão canônico do projeto.

Saída:
    reports/clusters_bert_3d.html — visualização interativa

Uso:
    pip install umap-learn plotly numpy pandas
    python -m src.visualization.visualize_bert_3d
"""

import json
import sys

import numpy as np
import pandas as pd

from src import logger
from src.config import PROCESSED_DIR, REPORTS_DIR, VECTORS_BERT

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

MATRIX_PATH = VECTORS_BERT / "bert_matrix.npy"
SKUS_PATH = VECTORS_BERT / "bert_skus.json"
PRODUCTS_CSV = PROCESSED_DIR / "products.csv"
OUTPUT_HTML = REPORTS_DIR / "clusters_bert_3d.html"

# Método de redução: "umap" (recomendado) ou "tsne"
METODO_REDUCAO: str = "umap"

# Hiperparâmetros UMAP
# n_neighbors maior → mais estrutura global; menor → mais estrutura local
UMAP_N_NEIGHBORS: int = 15
UMAP_MIN_DIST: float = 0.1
UMAP_METRIC: str = "cosine"  # ideal para embeddings normalizados

# Hiperparâmetros t-SNE (usado somente se umap-learn não estiver instalado)
TSNE_PERPLEXITY: int = 30
TSNE_MAX_ITER: int = 1000
TSNE_METRIC: str = "cosine"

SEED: int = 42

PONTO_TAMANHO: int = 3
PONTO_OPACIDADE: float = 0.8

# Comprimento máximo da descrição exibida no tooltip
TOOLTIP_DESC_MAX_CHARS: int = 120

# ---------------------------------------------------------------------------
# Paleta de cores e normalização de categorias
# ---------------------------------------------------------------------------

# Mapeamento de variações encontradas no CSV → nome canônico exibido no gráfico.
# Adicione entradas conforme novos valores aparecerem em products.csv.
NORMALIZAR_CATEGORIA: dict[str, str] = {
    # Cachaça
    "cachaca": "Cachaça",
    "cachaça": "Cachaça",
    # Whisky
    "whisky": "Whisky",
    "whiskey": "Whisky",
    # Vinho
    "vinho": "Vinho",
    "wine": "Vinho",
    # Gin
    "gin": "Gin",
    # Vodka
    "vodka": "Vodka",
    # Rum
    "rum": "Rum",
    # Espumante
    "espumante": "Espumante",
    "sparkling": "Espumante",
    "champagne": "Espumante",
    # Cerveja
    "cerveja": "Cerveja",
    "beer": "Cerveja",
    # Licor
    "licor": "Licor",
    "liqueur": "Licor",
    # Conhaque
    "conhaque": "Conhaque",
    "cognac": "Conhaque",
    "brandy": "Conhaque",
    # Tequila
    "tequila": "Tequila",
    "mezcal": "Tequila",
    # Absinto
    "absinto": "Absinto",
    "absinthe": "Absinto",
}

# Paleta alinhada com os nomes canônicos acima (ordem alfabética)
PALETA: dict[str, str] = {
    "Absinto": "#AB47BC",
    "Cachaça": "#8BC34A",
    "Cerveja": "#FFA726",
    "Conhaque": "#78909C",
    "Espumante": "#F06292",
    "Gin": "#4C8EDA",
    "Licor": "#26A69A",
    "Rum": "#C45E3E",
    "Tequila": "#EF5350",
    "Vinho": "#9B59B6",
    "Vodka": "#6DC0C0",
    "Whisky": "#E8A838",
}

COR_DESCONHECIDA: str = "#CCCCCC"


# ---------------------------------------------------------------------------
# Carregamento dos artefatos
# ---------------------------------------------------------------------------


def carregar_artefatos() -> tuple[np.ndarray, list[str]]:
    """Carrega a matriz de embeddings BERT e a lista de SKUs."""
    for caminho in [MATRIX_PATH, SKUS_PATH]:
        if not caminho.exists():
            logger.error("Arquivo não encontrado: '%s'", caminho)
            logger.error("Execute vectorize_bert.py antes desta etapa.")
            sys.exit(1)

    matriz = np.load(str(MATRIX_PATH))
    logger.info("Matriz BERT carregada: shape=%s  dtype=%s", matriz.shape, matriz.dtype)

    skus: list[str] = json.loads(SKUS_PATH.read_text(encoding="utf-8"))
    logger.info("SKUs carregados: %d entradas", len(skus))

    if len(skus) != matriz.shape[0]:
        logger.error(
            "Inconsistência: %d SKUs mas %d linhas na matriz.",
            len(skus),
            matriz.shape[0],
        )
        sys.exit(1)

    return matriz, skus


# ---------------------------------------------------------------------------
# Carregamento e categorização via CSV
# ---------------------------------------------------------------------------


def carregar_produtos(skus: list[str]) -> pd.DataFrame:
    """Lê products.csv, normaliza categorias e alinha com a lista de SKUs.

    Retorna um DataFrame indexado por SKU com as colunas:
        categoria_raw, categoria, nome, marca, descricao, preco
    """
    if not PRODUCTS_CSV.exists():
        logger.error("products.csv não encontrado: %s", PRODUCTS_CSV)
        sys.exit(1)

    df = pd.read_csv(PRODUCTS_CSV, dtype={"sku": object})  # type: ignore[arg-type]

    # Normaliza os nomes de categoria para o padrão canônico
    df["categoria"] = (
        df["category"]
        .str.lower()
        .str.strip()
        .map(NORMALIZAR_CATEGORIA)
        .fillna("Desconhecido")
    )

    df = df.rename(
        columns={
            "name": "nome",
            "brand": "marca",
            "description": "descricao",
            "price": "preco",
            "category": "categoria_raw",
        }
    )

    # Remove duplicatas de SKU mantendo a primeira ocorrência.
    # SKUs duplicados no CSV fariam df.loc[sku] retornar uma Series
    # em vez de um escalar, causando TypeError na categorização.
    duplicados = df["sku"].duplicated().sum()
    if duplicados:
        logger.warning(
            "%d linha(s) duplicada(s) removidas do CSV (mesmo SKU).", duplicados
        )
        df = df.drop_duplicates(subset="sku", keep="first")

    df = df.set_index("sku")

    # Mantém somente os SKUs presentes na matriz, na mesma ordem
    skus_presentes = [s for s in skus if s in df.index]
    skus_ausentes = [s for s in skus if s not in df.index]

    if skus_ausentes:
        logger.warning(
            "%d SKU(s) da matriz não encontrados no CSV: %s…",
            len(skus_ausentes),
            skus_ausentes[:5],
        )

    df_alinhado = df.reindex(skus_presentes)

    # Estatísticas de categorização
    contagem = df_alinhado["categoria"].value_counts()
    logger.info("Distribuição de categorias:")
    for cat, n in contagem.items():
        logger.info("  %-15s %d", cat, n)

    return df_alinhado


# ---------------------------------------------------------------------------
# Redução de dimensionalidade 768D → 3D
# ---------------------------------------------------------------------------


def reduzir_para_3d(matriz: np.ndarray) -> np.ndarray:
    """Reduz embeddings BERT (768D) para 3D usando UMAP ou t-SNE."""
    if METODO_REDUCAO == "umap":
        try:
            import umap  # noqa: PLC0415 # pyright: ignore[reportMissingImports]

            logger.info(
                "Reduzindo 768D → 3D com UMAP "
                "(n_neighbors=%d, min_dist=%.2f, metric=%s)…",
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
                low_memory=False,
            )
            return np.asarray(reducer.fit_transform(matriz))

        except ImportError:
            logger.warning(
                "umap-learn não instalado — usando t-SNE como fallback. "
                "Instale com: pip install umap-learn"
            )

    logger.info(
        "Reduzindo 768D → 3D com t-SNE " "(perplexity=%d, max_iter=%d, metric=%s)…",
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
# Construção do tooltip
# ---------------------------------------------------------------------------


def _truncar(texto: str | None, max_chars: int) -> str:
    texto_str = str(texto).strip() if texto else ""
    if not texto_str:
        return "—"
    return (
        texto_str
        if len(texto_str) <= max_chars
        else texto_str[:max_chars].rstrip() + "…"
    )


def construir_tooltip(sku: str, row: pd.Series) -> str:
    """Monta o HTML do tooltip exibido ao passar o mouse sobre o ponto."""
    nome = _truncar(row.get("nome", ""), 60)
    marca = _truncar(row.get("marca", ""), 40)
    cat_raw = row.get("categoria_raw", "—")
    cat_norm = row.get("categoria", "—")
    descricao = _truncar(row.get("descricao", ""), TOOLTIP_DESC_MAX_CHARS)
    preco = row.get("preco", "—")

    preco_fmt = f"R$ {preco:.2f}" if isinstance(preco, float) else str(preco)

    return (
        f"<b>SKU:</b> {sku}<br>"
        f"<b>Nome:</b> {nome}<br>"
        f"<b>Marca:</b> {marca}<br>"
        f"<b>Categoria:</b> {cat_norm} <i>({cat_raw})</i><br>"
        f"<b>Preço:</b> {preco_fmt}<br>"
        f"<b>Descrição:</b> {descricao}"
    )


# ---------------------------------------------------------------------------
# Visualização 3D com Plotly
# ---------------------------------------------------------------------------


def plotar_clusters_3d(
    coords_3d: np.ndarray,
    skus: list[str],
    df_produtos: pd.DataFrame,
) -> None:
    """Gera o scatter plot 3D interativo e salva como HTML standalone."""
    import plotly.graph_objects as go  # noqa: PLC0415

    # .at[] garante retorno escalar mesmo com duplicatas residuais no índice.
    # .loc[] pode retornar Series quando há mais de uma linha com o mesmo índice.
    categorias_por_sku = [
        (
            str(df_produtos.at[sku, "categoria"])
            if sku in df_produtos.index
            else "Desconhecido"
        )
        for sku in skus
    ]

    # Limita a legenda a 11 categorias no máximo
    # Se houver mais de 11, agrupa as menores em "Others"
    cat_counts = pd.Series(categorias_por_sku).value_counts()
    cats_presentes = list(cat_counts.head(11).index)

    if len(cat_counts) > 11:
        # Merge remaining categories into "Others"
        cats_menores = set(cat_counts.index) - set(cats_presentes)
        categorias_por_sku = [
            c if c in cats_presentes else "Others" for c in categorias_por_sku
        ]
        cats_presentes = sorted(set(categorias_por_sku))

    metodo_label = METODO_REDUCAO.upper()
    total_produtos = len(skus)
    total_cats = len(cats_presentes)

    fig = go.Figure()

    for cat in cats_presentes:
        indices = [i for i, c in enumerate(categorias_por_sku) if c == cat]
        cor = PALETA.get(str(cat), COR_DESCONHECIDA)

        hover_texts = [
            (
                construir_tooltip(skus[i], df_produtos.loc[[skus[i]]].iloc[0])
                if skus[i] in df_produtos.index
                else f"<b>SKU:</b> {skus[i]}<br><i>Sem dados no CSV</i>"
            )
            for i in indices
        ]

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
                hoverlabel=dict(
                    bgcolor="#1A1A2E",
                    bordercolor=cor,
                    font=dict(size=12, color="#E8E8E8"),
                    namelength=0,
                ),
            )
        )

    eixo_base = dict(
        backgroundcolor="#0F1117",
        gridcolor="#1E1E2E",
        showbackground=True,
        zerolinecolor="#2A2A3A",
        tickfont=dict(color="#888888", size=9),
    )

    fig.update_layout(
        title=dict(
            text=(
                f"Mapa Semântico 3D de Bebidas — BERTugues + {metodo_label}<br>"
                f"<sup>{total_produtos} produtos · {total_cats} categorias · "
                f"embeddings 768D → 3D</sup>"
            ),
            x=0.5,
            font=dict(size=16, color="#E8E8F8"),
        ),
        scene=dict(
            xaxis=dict(title=f"{metodo_label} — Dim 1", **eixo_base),
            yaxis=dict(title=f"{metodo_label} — Dim 2", **eixo_base),
            zaxis=dict(title=f"{metodo_label} — Dim 3", **eixo_base),
            bgcolor="#0F1117",
        ),
        paper_bgcolor="#0F1117",
        plot_bgcolor="#0F1117",
        font=dict(color="#E8E8E8"),
        legend=dict(
            bgcolor="rgba(26, 26, 46, 0.85)",
            bordercolor="#3A3A5A",
            borderwidth=1,
            font=dict(size=11, color="#E8E8E8"),
            traceorder="normal",
            itemsizing="constant",
            valign="top",
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
    print("VISUALIZAÇÃO 3D — BERT CLUSTERS")
    print("=" * 60)

    print("\n[1/4] Carregando artefatos BERT…")
    matriz, skus = carregar_artefatos()
    print(f"      Produtos: {len(skus)}  |  Dimensões: {matriz.shape[1]}")

    print("\n[2/4] Carregando metadados e categorizando…")
    df_produtos = carregar_produtos(skus)
    contagem = df_produtos["categoria"].value_counts()
    for cat, n in contagem.items():
        print(f"      {cat:<15} {n:>4}")

    print(f"\n[3/4] Reduzindo 768D → 3D ({METODO_REDUCAO.upper()})…")
    coords_3d = reduzir_para_3d(matriz)
    print(f"      Shape final: {coords_3d.shape}")

    print("\n[4/4] Gerando visualização 3D interativa…")
    plotar_clusters_3d(coords_3d, skus, df_produtos)
    print(f"      Salvo em: {OUTPUT_HTML}")

    print("\n" + "=" * 60)
    print("Concluído! Abra o HTML no navegador para interagir.")
    print("=" * 60)


if __name__ == "__main__":
    main()
