"""
Engine de recomendação de bebidas baseado em Word2Vec + K-Means.

Usa similaridade de cosseno entre embeddings W2V para encontrar bebidas
com descrições similares, com um mecanismo de diversidade que limita
quantas recomendações vêm do mesmo cluster (evitando loops de
recomendação onde o usuário só vê os mesmos produtos).

Arquitetura desacoplada do CLI — pode ser reutilizado em GUI, API, etc.

Uso programático:
    from src.recommendation import RecommendationEngine

    engine = RecommendationEngine()
    results = engine.recommend("SKU123", n=10, max_same_cluster=7)
"""

import csv
import json
import sys
from collections import defaultdict
from dataclasses import dataclass

import numpy as np
from sklearn.preprocessing import normalize

from src import logger
from src.config import PROCESSED_DIR, VECTORS_W2V
from src.recommendation.clusterer import carregar as carregar_clusters


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

MATRIX_PATH = VECTORS_W2V / "w2v_matrix.npy"
SKUS_PATH = VECTORS_W2V / "w2v_skus.json"
PRODUCTS_CSV = PROCESSED_DIR / "products.csv"

MAX_SAME_CLUSTER_DEFAULT: int = 7
N_RECOMMENDATIONS_DEFAULT: int = 10


# ---------------------------------------------------------------------------
# Dataclasses de resultado
# ---------------------------------------------------------------------------


@dataclass
class Product:
    """Informações básicas de um produto."""

    sku: str
    name: str
    category: str
    brand: str
    description: str

    def to_dict(self) -> dict:
        return {
            "sku": self.sku,
            "name": self.name,
            "category": self.category,
            "brand": self.brand,
            "description": self.description,
        }


@dataclass
class Recommendation:
    """Uma recomendação individual com metadados."""

    sku: str
    name: str
    category: str
    brand: str
    similarity: float
    cluster_id: int
    description_preview: str

    def to_dict(self) -> dict:
        return {
            "sku": self.sku,
            "name": self.name,
            "category": self.category,
            "brand": self.brand,
            "similarity": round(self.similarity, 4),
            "cluster_id": self.cluster_id,
            "description_preview": self.description_preview,
        }


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class RecommendationEngine:
    """Engine de recomendação de bebidas com diversidade cross-cluster.

    Carrega todos os artefatos no __init__ e mantém em memória para
    consultas rápidas e repetidas (ideal para CLI interativo ou servidor).
    """

    def __init__(self) -> None:
        self._matriz, self._skus = self._carregar_vetores()
        self._matriz_norm = normalize(self._matriz, norm="l2")
        self._clusters = carregar_clusters()
        self._produtos = self._carregar_produtos_csv()

        # Índice SKU → posição na matriz para O(1) lookup
        self._sku_to_idx: dict[str, int] = {
            sku: i for i, sku in enumerate(self._skus)
        }

        logger.info(
            "RecommendationEngine pronto: %d produtos, %d clusters",
            len(self._skus),
            self._clusters.n_clusters,
        )

    # ------------------------------------------------------------------
    # Carregamento
    # ------------------------------------------------------------------

    @staticmethod
    def _carregar_vetores() -> tuple[np.ndarray, list[str]]:
        for caminho in [MATRIX_PATH, SKUS_PATH]:
            if not caminho.exists():
                logger.error("Arquivo não encontrado: '%s'", caminho)
                sys.exit(1)

        matriz = np.load(str(MATRIX_PATH))
        with open(SKUS_PATH, encoding="utf-8") as f:
            skus: list[str] = json.load(f)

        if len(skus) != matriz.shape[0]:
            logger.error(
                "Inconsistência: %d SKUs mas %d linhas na matriz.",
                len(skus),
                matriz.shape[0],
            )
            sys.exit(1)

        return matriz, skus

    @staticmethod
    def _carregar_produtos_csv() -> dict[str, Product]:
        produtos: dict[str, Product] = {}
        if not PRODUCTS_CSV.exists():
            logger.warning("products.csv não encontrado em %s", PRODUCTS_CSV)
            return produtos

        with open(PRODUCTS_CSV, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sku = str(row.get("sku", ""))
                produtos[sku] = Product(
                    sku=sku,
                    name=row.get("name", ""),
                    category=row.get("category", ""),
                    brand=row.get("brand", ""),
                    description=row.get("description", ""),
                )

        return produtos

    # ------------------------------------------------------------------
    # Busca por nome
    # ------------------------------------------------------------------

    def search_by_name(self, query: str, max_results: int = 20) -> list[Product]:
        """Busca produtos cujo nome contém a query (case-insensitive).

        Retorna até max_results produtos ordenados por relevância
        (match exato primeiro, depois parcial).
        """
        query_lower = query.lower().strip()
        if not query_lower:
            return []

        exact: list[Product] = []
        partial: list[Product] = []

        for sku in self._skus:
            product = self._produtos.get(sku)
            if product is None:
                continue

            name_lower = product.name.lower()
            if name_lower == query_lower:
                exact.append(product)
            elif query_lower in name_lower:
                partial.append(product)

        results = exact + partial
        return results[:max_results]

    def get_product(self, sku: str) -> Product | None:
        """Retorna informações de um produto por SKU."""
        return self._produtos.get(sku)

    # ------------------------------------------------------------------
    # Similaridade
    # ------------------------------------------------------------------

    def _cosine_similarities(self, sku: str) -> np.ndarray:
        """Calcula similaridade de cosseno entre um SKU e todos os produtos.

        Retorna array de shape (N,) com similaridades.
        """
        idx = self._sku_to_idx.get(sku)
        if idx is None:
            raise ValueError(f"SKU '{sku}' não encontrado na matriz de embeddings.")

        vetor = self._matriz_norm[idx]  # (D,)
        # (N, D) @ (D,) → (N,)
        return self._matriz_norm @ vetor

    # ------------------------------------------------------------------
    # Recomendação com diversidade
    # ------------------------------------------------------------------

    def recommend(
        self,
        sku: str,
        n: int = N_RECOMMENDATIONS_DEFAULT,
        max_same_cluster: int = MAX_SAME_CLUSTER_DEFAULT,
    ) -> list[Recommendation]:
        """Recomenda até N bebidas similares com diversidade cross-cluster.

        Algoritmo:
        1. Calcula similaridade de cosseno entre o produto query e todos.
        2. Remove o próprio produto do ranking.
        3. Ordena por similaridade decrescente.
        4. Aplica cap de max_same_cluster para o cluster do produto query.
        5. Preenche slots restantes round-robin dos outros clusters,
           sempre pegando o candidato mais similar disponível de cada cluster.

        Args:
            sku: SKU do produto de referência.
            n: Número de recomendações desejadas.
            max_same_cluster: Máximo de recomendações do mesmo cluster
                              do produto query.

        Returns:
            Lista de Recommendation ordenada por similaridade (com diversidade).
        """
        similarities = self._cosine_similarities(sku)
        query_cluster = self._clusters.labels.get(sku, -1)

        # Monta candidatos: (idx, similarity, cluster_id), excluindo o próprio
        query_idx = self._sku_to_idx[sku]
        candidatos = []
        for i in range(len(self._skus)):
            if i == query_idx:
                continue
            c_sku = self._skus[i]
            cluster_id = self._clusters.labels.get(c_sku, -1)
            candidatos.append((i, float(similarities[i]), cluster_id))

        # Ordena por similaridade decrescente
        candidatos.sort(key=lambda x: x[1], reverse=True)

        # Separa candidatos por cluster
        por_cluster: dict[int, list[tuple[int, float, int]]] = defaultdict(list)
        for cand in candidatos:
            por_cluster[cand[2]].append(cand)

        # Fase 1: pegar até max_same_cluster do cluster do query
        selecionados: list[tuple[int, float, int]] = []
        mesmo_cluster = por_cluster.pop(query_cluster, [])
        for cand in mesmo_cluster[:max_same_cluster]:
            selecionados.append(cand)

        # Sobra do mesmo cluster (caso precise depois)
        sobra_mesmo = mesmo_cluster[max_same_cluster:]

        # Fase 2: preencher slots restantes round-robin dos outros clusters
        slots_restantes = n - len(selecionados)
        if slots_restantes > 0:
            # Ordena clusters por melhor candidato (mais similar)
            outros_clusters = sorted(
                por_cluster.keys(),
                key=lambda cid: por_cluster[cid][0][1] if por_cluster[cid] else -1,
                reverse=True,
            )

            # Ponteiros de consumo para cada cluster
            ponteiros: dict[int, int] = {cid: 0 for cid in outros_clusters}

            adicionados = 0
            while adicionados < slots_restantes:
                progresso = False
                for cid in outros_clusters:
                    if adicionados >= slots_restantes:
                        break
                    fila = por_cluster[cid]
                    ptr = ponteiros[cid]
                    if ptr < len(fila):
                        selecionados.append(fila[ptr])
                        ponteiros[cid] = ptr + 1
                        adicionados += 1
                        progresso = True

                if not progresso:
                    # Todos os clusters esgotados — usa sobra do mesmo cluster
                    for cand in sobra_mesmo:
                        if adicionados >= slots_restantes:
                            break
                        selecionados.append(cand)
                        adicionados += 1
                    break

        # Ordena resultado final por similaridade
        selecionados.sort(key=lambda x: x[1], reverse=True)

        # Monta lista de Recommendation
        resultado: list[Recommendation] = []
        for idx, sim, cluster_id in selecionados[:n]:
            s = self._skus[idx]
            product = self._produtos.get(s)
            name = product.name if product else s
            category = product.category if product else ""
            brand = product.brand if product else ""
            desc = product.description if product else ""
            desc_preview = desc[:100].rstrip() + "…" if len(desc) > 100 else desc

            resultado.append(
                Recommendation(
                    sku=s,
                    name=name,
                    category=category,
                    brand=brand,
                    similarity=sim,
                    cluster_id=cluster_id,
                    description_preview=desc_preview,
                )
            )

        return resultado

    def recommend_as_dicts(
        self,
        sku: str,
        n: int = N_RECOMMENDATIONS_DEFAULT,
        max_same_cluster: int = MAX_SAME_CLUSTER_DEFAULT,
    ) -> list[dict]:
        """Wrapper que retorna recomendações como lista de dicts (JSON-friendly)."""
        return [r.to_dict() for r in self.recommend(sku, n, max_same_cluster)]
