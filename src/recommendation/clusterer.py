"""
Clusterização K-Means sobre embeddings Word2Vec.

Lê a matriz de embeddings W2V (models/w2v/w2v_matrix.npy) e os SKUs
correspondentes (models/w2v/w2v_skus.json), aplica normalização L2
(para usar distância cosseno via K-Means euclidiano) e determina o
número ideal de clusters via silhouette score num range configurável.

Artefatos gerados:
    models/w2v/cluster_labels.json      — {sku: cluster_id}
    models/w2v/cluster_centroids.npy    — centroides (K × D), normalizados

Uso:
    python -m src.recommendation.clusterer
"""

import json
import sys
from dataclasses import dataclass, field

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import normalize

from src import logger
from src.config import (
    CLUSTERS_W2V_CENTROIDS,
    CLUSTERS_W2V_LABELS,
    VECTORS_W2V,
)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

MATRIX_PATH = VECTORS_W2V / "w2v_matrix.npy"
SKUS_PATH = VECTORS_W2V / "w2v_skus.json"

# Range de K para busca automática via silhouette score.
K_MIN: int = 5
K_MAX: int = 20

# Override manual — se definido, ignora a busca automática.
K_MANUAL: int | None = None

SEED: int = 42
N_INIT: int = 10
MAX_ITER: int = 300


# ---------------------------------------------------------------------------
# Resultado da clusterização
# ---------------------------------------------------------------------------


@dataclass
class ClusterResult:
    """Resultado da clusterização K-Means."""

    labels: dict[str, int]       # SKU → cluster_id
    centroids: np.ndarray        # (K, D) centroides normalizados
    n_clusters: int
    skus: list[str]
    silhouette: float

    _label_array: np.ndarray = field(repr=False, default_factory=lambda: np.array([]))

    def skus_do_cluster(self, cluster_id: int) -> list[str]:
        """Retorna os SKUs pertencentes a um cluster."""
        return [s for s, c in self.labels.items() if c == cluster_id]


# ---------------------------------------------------------------------------
# Carregamento
# ---------------------------------------------------------------------------


def _carregar_artefatos() -> tuple[np.ndarray, list[str]]:
    for caminho in [MATRIX_PATH, SKUS_PATH]:
        if not caminho.exists():
            logger.error("Arquivo não encontrado: '%s'", caminho)
            sys.exit(1)

    matriz = np.load(str(MATRIX_PATH))
    with open(SKUS_PATH, encoding="utf-8") as f:
        skus: list[str] = json.load(f)

    if len(skus) != matriz.shape[0]:
        logger.error(
            "Inconsistência: %d SKUs mas %d linhas na matriz.", len(skus), matriz.shape[0]
        )
        sys.exit(1)

    logger.info("Artefatos carregados: %d produtos, %d dimensões", len(skus), matriz.shape[1])
    return matriz, skus


# ---------------------------------------------------------------------------
# Busca do K ótimo
# ---------------------------------------------------------------------------


def _encontrar_k_otimo(matriz_norm: np.ndarray, k_min: int, k_max: int) -> tuple[int, float]:
    """Testa K de k_min a k_max e retorna o que maximiza o silhouette score."""
    melhor_k = k_min
    melhor_score = -1.0

    logger.info("Buscando K ótimo no range [%d, %d]…", k_min, k_max)

    for k in range(k_min, k_max + 1):
        km = KMeans(n_clusters=k, random_state=SEED, n_init=N_INIT, max_iter=MAX_ITER)
        labels = km.fit_predict(matriz_norm)
        score = silhouette_score(matriz_norm, labels, metric="euclidean")
        logger.info("  K=%2d  silhouette=%.4f", k, score)

        if score > melhor_score:
            melhor_score = score
            melhor_k = k

    logger.info("K ótimo: %d (silhouette=%.4f)", melhor_k, melhor_score)
    return melhor_k, melhor_score


# ---------------------------------------------------------------------------
# Clusterização
# ---------------------------------------------------------------------------


def clusterizar(
    k_override: int | None = K_MANUAL,
    k_min: int = K_MIN,
    k_max: int = K_MAX,
) -> ClusterResult:
    """Executa a clusterização K-Means e retorna o ClusterResult.

    Se k_override for fornecido, usa esse K diretamente.
    Caso contrário, busca o K ótimo via silhouette score.
    """
    matriz, skus = _carregar_artefatos()

    # Normalização L2 — K-Means euclidiano em vetores normalizados
    # equivale a minimizar distância cosseno.
    matriz_norm = normalize(matriz, norm="l2")

    if k_override is not None:
        k = k_override
        km = KMeans(n_clusters=k, random_state=SEED, n_init=N_INIT, max_iter=MAX_ITER)
        label_array = km.fit_predict(matriz_norm)
        sil = silhouette_score(matriz_norm, label_array, metric="euclidean")
        logger.info("K manual: %d (silhouette=%.4f)", k, sil)
    else:
        k, _ = _encontrar_k_otimo(matriz_norm, k_min, k_max)
        km = KMeans(n_clusters=k, random_state=SEED, n_init=N_INIT, max_iter=MAX_ITER)
        label_array = km.fit_predict(matriz_norm)
        sil = silhouette_score(matriz_norm, label_array, metric="euclidean")

    # Centroides já normalizados (pois os dados de entrada são normalizados)
    centroids = km.cluster_centers_

    labels = {sku: int(label_array[i]) for i, sku in enumerate(skus)}

    # Distribuição
    contagem: dict[int, int] = {}
    for c in label_array:
        contagem[int(c)] = contagem.get(int(c), 0) + 1
    for cid in sorted(contagem):
        logger.info("  Cluster %2d: %4d produtos", cid, contagem[cid])

    return ClusterResult(
        labels=labels,
        centroids=centroids,
        n_clusters=k,
        skus=skus,
        silhouette=sil,
        _label_array=label_array,
    )


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------


def salvar(result: ClusterResult) -> None:
    """Salva os artefatos de clusterização em disco."""
    CLUSTERS_W2V_LABELS.parent.mkdir(parents=True, exist_ok=True)

    with open(CLUSTERS_W2V_LABELS, "w", encoding="utf-8") as f:
        json.dump(result.labels, f, ensure_ascii=False, indent=2)
    logger.info("Labels salvos: %s", CLUSTERS_W2V_LABELS)

    np.save(str(CLUSTERS_W2V_CENTROIDS), result.centroids)
    logger.info("Centroides salvos: %s", CLUSTERS_W2V_CENTROIDS)


def carregar() -> ClusterResult:
    """Carrega artefatos de clusterização previamente salvos."""
    for caminho in [CLUSTERS_W2V_LABELS, CLUSTERS_W2V_CENTROIDS]:
        if not caminho.exists():
            logger.error("Artefato de clusterização não encontrado: '%s'", caminho)
            logger.error("Execute: python -m src.recommendation.clusterer")
            sys.exit(1)

    with open(CLUSTERS_W2V_LABELS, encoding="utf-8") as f:
        labels: dict[str, int] = json.load(f)

    centroids = np.load(str(CLUSTERS_W2V_CENTROIDS))
    skus = list(labels.keys())
    n_clusters = centroids.shape[0]

    label_array = np.array([labels[s] for s in skus])
    sil = 0.0  # Não recalcula — valor informativo apenas

    logger.info(
        "Clusterização carregada: %d clusters, %d produtos", n_clusters, len(skus)
    )

    return ClusterResult(
        labels=labels,
        centroids=centroids,
        n_clusters=n_clusters,
        skus=skus,
        silhouette=sil,
        _label_array=label_array,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    print("\n" + "=" * 60)
    print("CLUSTERIZAÇÃO K-MEANS — WORD2VEC EMBEDDINGS")
    print("=" * 60)

    result = clusterizar()

    print(f"\nClusters: {result.n_clusters}")
    print(f"Silhouette: {result.silhouette:.4f}")
    print(f"Produtos: {len(result.skus)}")

    salvar(result)
    print("\nArtefatos salvos com sucesso.")


if __name__ == "__main__":
    main()
