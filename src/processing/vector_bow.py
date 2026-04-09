"""
Etapa 2b do pipeline de clusterização — Vetorização Bag of Words (BoW).

Lê data/processed/tokens_products_filtered.jsonl e transforma os lemmas
de cada produto em vetores de contagem usando CountVectorizer.

Execute o script seção por seção para acompanhar cada etapa do processo.
Cada seção é independente e imprime seu próprio diagnóstico.

Saída:
    data/processed/bow_matrix.npz       — matriz esparsa de contagens BoW
    data/processed/bow_skus.json        — lista de SKUs na ordem das linhas
    data/processed/bow_features.json    — lista de features (vocab) na ordem das colunas

Uso:
    pip install scikit-learn scipy numpy
    python vector_bow.py
"""

import json
import sys
from pathlib import Path

import numpy as np
from scipy.sparse import csr_matrix, save_npz
from sklearn.feature_extraction.text import CountVectorizer

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

PROCESSED_DIR = Path("data/processed")

FILTERED_JSONL = PROCESSED_DIR / "tokens_products_filtered.jsonl"
OUTPUT_MATRIX = PROCESSED_DIR / "bow_matrix.npz"
OUTPUT_SKUS = PROCESSED_DIR / "bow_skus.json"
OUTPUT_FEATURES = PROCESSED_DIR / "bow_features.json"

# BoW — parâmetros como constantes tipadas individualmente
NGRAM_RANGE: tuple[int, int] = (1, 1)  # unigrams apenas
MIN_DF: int = 2  # ignora termos que aparecem em < 2 docs
MAX_DF: float = 0.60  # ignora termos em > 60% dos docs
MAX_FEATURES: int = 5000  # limite de dimensionalidade

STOPLIST_DOMINIO: frozenset[str] = frozenset(
    {
        "características",
        "volume",
        "graduação",
        "alcoólica",
        "alcoólico",
        "origem",
        "país",
        "ano",
        "região",
        "importado",
        "qualidade",
        "produção",
        "método",
        "informações",
        "técnicas",
        "ficha",
        "descrição",
        "produto",
        "kit",
        "unidade",
        "comprar",
        "loja",
        "entrega",
        "preço",
        "embalagem",
        "anos",
        "ml",
        "litros",
        "garrafa",
        "sabor",
        "nota",
        "teor",
        "bebida",
    }
)


# ============================================================================
# SEÇÃO 1 — CARREGAMENTO
# Lê o JSONL filtrado e monta duas listas paralelas: SKUs e documentos.
# Um "documento" aqui é a string de lemmas separados por espaço, que é
# o formato que o CountVectorizer espera como entrada.
# ============================================================================

print("\n" + "=" * 60)
print("SEÇÃO 1 — CARREGAMENTO")
print("=" * 60)

if not FILTERED_JSONL.exists():
    print(f"[ERRO] Arquivo não encontrado: '{FILTERED_JSONL}'")
    print("Execute filtrar_tokens.py antes desta etapa.")
    sys.exit(1)

skus: list[str] = []
documentos: list[str] = []

with open(FILTERED_JSONL, encoding="utf-8") as f:
    for linha in f:
        linha = linha.strip()
        if not linha:
            continue
        registro = json.loads(linha)
        skus.append(registro["sku"])
        # Junta lemmas em uma string — formato esperado pelo CountVectorizer
        documentos.append(
            " ".join(t for t in registro["lemmas"] if t not in STOPLIST_DOMINIO)
        )

print(f"Produtos carregados : {len(skus)}")
print(f"Exemplo SKU         : {skus[0]}")
print(f"Documento bruto     : {documentos[0][:120]}...")
print(f"Total de tokens     : {sum(len(d.split()) for d in documentos)}")


# ============================================================================
# SEÇÃO 2 — CONSTRUÇÃO DO VOCABULÁRIO
# O vectorizer faz duas passagens: primeiro constrói o vocabulário (fit),
# depois transforma (transform). Aqui separamos as duas para inspecionar
# o vocabulário antes de transformar.
# ============================================================================

print("\n" + "=" * 60)
print("SEÇÃO 2 — CONSTRUÇÃO DO VOCABULÁRIO")
print("=" * 60)

vectorizer = CountVectorizer(
    ngram_range=NGRAM_RANGE,
    min_df=MIN_DF,
    max_df=MAX_DF,
    max_features=MAX_FEATURES,
)
vectorizer.fit(documentos)

vocabulario = vectorizer.get_feature_names_out()
print(f"Total de features (vocab)   : {len(vocabulario)}")
print(f"Primeiras 20 features       : {list(vocabulario[:20])}")
print(f"Últimas 20 features         : {list(vocabulario[-20:])}")

# Separa unigrams de bigrams para diagnóstico
unigrams = [f for f in vocabulario if " " not in f]
bigrams = [f for f in vocabulario if " " in f]
print(f"\nUnigrams no vocab           : {len(unigrams)}")
print(f"Bigrams no vocab            : {len(bigrams)}")


# ============================================================================
# SEÇÃO 3 — TRANSFORMAÇÃO
# Aplica o vocabulário aprendido a cada documento e produz a matriz de
# contagens. Cada célula (i, j) contém quantas vezes o termo j aparece
# no documento i. A matriz é esparsa pois a maioria dos termos não aparece
# em cada produto.
# ============================================================================

print("\n" + "=" * 60)
print("SEÇÃO 3 — TRANSFORMAÇÃO")
print("=" * 60)

matriz: csr_matrix = vectorizer.transform(documentos)  # type: ignore[assignment]

n_linhas, n_colunas = matriz.shape  # type: ignore[misc]
n_elementos = matriz.nnz  # número de elementos não-zero
total_elementos = n_linhas * n_colunas
esparsidade = 1 - (n_elementos / total_elementos)

print(f"Dimensões da matriz         : {n_linhas} produtos × {n_colunas} features")
print(f"Elementos não-zero          : {n_elementos:,}")
print(f"Esparsidade                 : {esparsidade:.1%}")
print(f"Memória (denso estimado)    : {total_elementos * 8 / 1024 / 1024:.1f} MB")
print(f"Memória (esparso real)      : {matriz.data.nbytes / 1024:.1f} KB")
print(f"Contagem máxima por célula  : {int(matriz.max())}")
print(f"Contagem média (não-zero)   : {matriz.data.mean():.2f}")


# ============================================================================
# SEÇÃO 4 — INSPEÇÃO POR PRODUTO
# Para cada produto, mostra quais features têm maior contagem.
# Diferente do TF-IDF, aqui os valores são contagens brutas — termos que
# aparecem mais vezes no texto têm valores mais altos.
# ============================================================================

print("\n" + "=" * 60)
print("SEÇÃO 4 — INSPEÇÃO POR PRODUTO (top features)")
print("=" * 60)

N_TOP = 10  # quantas top features mostrar por produto
N_AMOSTRAS = 5  # quantos produtos amostrar

indices_amostra = np.linspace(0, n_linhas - 1, N_AMOSTRAS, dtype=int)

for idx in indices_amostra:
    vetor = matriz[idx].toarray().flatten()
    top_idx = vetor.argsort()[::-1][:N_TOP]
    top = [(vocabulario[i], int(vetor[i])) for i in top_idx if vetor[i] > 0]
    print(f"\nSKU {skus[idx]}:")
    for feature, contagem in top:
        print(f"  {feature:<30} {contagem}")


# ============================================================================
# SEÇÃO 5 — DIAGNÓSTICO DE FEATURES GLOBAIS
# Mostra quais features têm maior frequência total no corpus inteiro.
# Features com contagem muito alta são candidatas a ajuste no max_df
# ou adição na STOPLIST_DOMINIO.
# ============================================================================

print("\n" + "=" * 60)
print("SEÇÃO 5 — FEATURES MAIS FREQUENTES NO CORPUS")
print("=" * 60)

contagens_globais = np.asarray(matriz.sum(axis=0)).flatten()
top_global_idx = contagens_globais.argsort()[::-1][:20]

print("Top 20 features por contagem acumulada no corpus:")
for i in top_global_idx:
    print(f"  {vocabulario[i]:<35} {int(contagens_globais[i])}")


# ============================================================================
# SEÇÃO 6 — PERSISTÊNCIA
# Salva a matriz esparsa e os metadados de SKUs e features.
# Os três arquivos juntos são suficientes para reconstruir qualquer
# linha ou coluna da matriz na etapa de clusterização.
# ============================================================================

print("\n" + "=" * 60)
print("SEÇÃO 6 — PERSISTÊNCIA")
print("=" * 60)

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

save_npz(OUTPUT_MATRIX, matriz)
print(f"Matriz salva          : {OUTPUT_MATRIX}")

with open(OUTPUT_SKUS, "w", encoding="utf-8") as f:
    json.dump(skus, f, ensure_ascii=False)
print(f"SKUs salvos           : {OUTPUT_SKUS}  ({len(skus)} entradas)")

with open(OUTPUT_FEATURES, "w", encoding="utf-8") as f:
    json.dump(vocabulario.tolist(), f, ensure_ascii=False)
print(f"Features salvas       : {OUTPUT_FEATURES}  ({len(vocabulario)} entradas)")

print("\nVetorização BoW concluída. Próxima etapa: clusterização.")
