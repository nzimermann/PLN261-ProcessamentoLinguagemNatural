"""
Análise de sentimentos das reviews — predição de rating (1-5).

Lê data/processed/reviews.csv, aplica o modelo pré-treinado
nlptown/bert-base-multilingual-uncased-sentiment (BERT multilingual
fine-tunado para classificação de 1-5 estrelas em reviews de e-commerce)
e prediz o rating que o usuário atribuiu com base apenas no texto da review.

A tokenização é interna ao BERT — não requer pipeline spaCy.

Saídas:
    reports/sentiment_analysis.csv          — reviews + predicted_rating, acertou, erro_abs
    reports/sentiment_confusion_matrix.png  — heatmap da confusion matrix
    reports/sentiment_report.txt            — classification report + exemplos de erros

Uso:
    python -m src.features.sentiment_analysis
"""

import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import torch
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
)
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from src import logger
from src.config import PROCESSED_DIR, REPORTS_DIR

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

REVIEWS_CSV = PROCESSED_DIR / "reviews.csv"

# Modelo BERT multilingual tunado para predição de 1-5 estrelas.
# Treinado em reviews de e-commerce em inglês, alemão, holandês, francês,
# espanhol e italiano
SENTIMENT_MODEL = "nlptown/bert-base-multilingual-uncased-sentiment"

# Número de reviews processadas por vez.
# Reduza para 8 ou 16 se receber erros de memória (OOM).
BATCH_SIZE = 32

# Comprimento máximo em tokens que o BERT processa.
MAX_LENGTH = 256

# Arquivos de saída
OUTPUT_CSV = REPORTS_DIR / "sentiment_analysis.csv"
OUTPUT_MATRIX_PNG = REPORTS_DIR / "sentiment_confusion_matrix.png"
OUTPUT_REPORT_TXT = REPORTS_DIR / "sentiment_report.txt"


# ---------------------------------------------------------------------------
# Carregamento dos dados
# ---------------------------------------------------------------------------


def carregar_reviews(caminho) -> pd.DataFrame:
    """Lê reviews.csv e descarta linhas sem texto."""
    df = pd.read_csv(caminho, dtype={"sku": str, "id": str})
    total_original = len(df)

    df = df.dropna(subset=["review_body"])
    df = df[df["review_body"].str.strip().astype(bool)].copy()
    df["rating"] = df["rating"].astype(int)

    descartadas = total_original - len(df)
    if descartadas:
        logger.info("Reviews sem texto descartadas: %d", descartadas)

    logger.info("Reviews carregadas: %d", len(df))
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Modelo
# ---------------------------------------------------------------------------


def carregar_modelo(nome: str):
    """Carrega tokenizer e modelo de classificação de sentimentos."""
    logger.info("Carregando modelo: %s", nome)
    logger.info("(Primeira execução faz download ~700 MB — aguarde)")

    tokenizer = AutoTokenizer.from_pretrained(nome)
    model = AutoModelForSequenceClassification.from_pretrained(nome)
    model.eval()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    logger.info("Modelo carregado — device: %s", device)

    return tokenizer, model, device


# ---------------------------------------------------------------------------
# Predição
# ---------------------------------------------------------------------------


def predizer_batch(
    textos: list[str],
    tokenizer,
    model,
    device: torch.device,
) -> list[int]:
    """Prediz o rating (1-5) para um batch de textos.

    O modelo nlptown retorna 5 logits cujos índices 0-4 correspondem
    a 1-5 estrelas. Aplica argmax e converte para a escala 1-5.
    """
    inputs = tokenizer(
        textos,
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
        return_tensors="pt",
    ).to(device)

    with torch.no_grad():
        logits = model(**inputs).logits

    indices = torch.argmax(logits, dim=-1).cpu().numpy()
    return (indices + 1).tolist()  # índice 0 → 1 estrela, …, 4 → 5 estrelas


def predizer_todos(
    textos: list[str],
    tokenizer,
    model,
    device: torch.device,
) -> list[int]:
    """Prediz ratings em batches para todas as reviews com log de progresso."""
    predicoes: list[int] = []
    total = len(textos)

    for inicio in range(0, total, BATCH_SIZE):
        batch = textos[inicio : inicio + BATCH_SIZE]
        predicoes.extend(predizer_batch(batch, tokenizer, model, device))

        processados = min(inicio + BATCH_SIZE, total)
        logger.info(
            "  Predição: %d/%d  (%.0f%%)",
            processados,
            total,
            100 * processados / total,
        )

    return predicoes


# ---------------------------------------------------------------------------
# Métricas
# ---------------------------------------------------------------------------


def calcular_metricas(y_real: np.ndarray, y_pred: np.ndarray) -> dict:
    """Calcula métricas de classificação para avaliação do modelo."""
    acc_exata = accuracy_score(y_real, y_pred)
    acc_um_estrela = np.mean(np.abs(y_real - y_pred) <= 1)
    f1_macro = f1_score(y_real, y_pred, average="macro", zero_division=0)
    f1_weighted = f1_score(y_real, y_pred, average="weighted", zero_division=0)
    cm = confusion_matrix(y_real, y_pred, labels=[1, 2, 3, 4, 5])
    report = classification_report(
        y_real, y_pred, labels=[1, 2, 3, 4, 5], zero_division=0
    )

    logger.info("Accuracy exata:    %.4f", acc_exata)
    logger.info("Accuracy (±1):     %.4f", acc_um_estrela)
    logger.info("F1 macro:          %.4f", f1_macro)
    logger.info("F1 weighted:       %.4f", f1_weighted)

    return {
        "acc_exata": acc_exata,
        "acc_um_estrela": acc_um_estrela,
        "f1_macro": f1_macro,
        "f1_weighted": f1_weighted,
        "confusion_matrix": cm,
        "report": report,
    }


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------


def salvar_confusion_matrix(cm: np.ndarray, caminho) -> None:
    """Plota e salva a confusion matrix como imagem PNG."""
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        cmap="Blues",
        xticklabels=[1, 2, 3, 4, 5],
        yticklabels=[1, 2, 3, 4, 5],
        ax=ax,
    )
    ax.set_xlabel("Rating Predito", fontsize=12)
    ax.set_ylabel("Rating Real", fontsize=12)
    ax.set_title(
        "Análise de Sentimentos — Predição de Rating (1-5 estrelas)", fontsize=13
    )
    fig.tight_layout()
    fig.savefig(str(caminho), dpi=150)
    plt.close(fig)
    logger.info("Confusion matrix salva: %s", caminho)


def salvar_relatorio(df: pd.DataFrame, metricas: dict, caminho) -> None:
    """Salva relatório textual com métricas e exemplos de erros grandes."""
    n = len(df)
    acertos = int(df["acertou"].sum())

    with open(caminho, "w", encoding="utf-8") as f:
        f.write("=" * 65 + "\n")
        f.write("ANÁLISE DE SENTIMENTOS — RELATÓRIO\n")
        f.write(f"Modelo: {SENTIMENT_MODEL}\n")
        f.write("=" * 65 + "\n\n")

        f.write(f"Total de reviews analisadas : {n}\n")
        f.write(f"Acertos exatos              : {acertos}/{n}  ({100*metricas['acc_exata']:.1f}%)\n")
        f.write(f"Acertos dentro de ±1 estrela: {int(metricas['acc_um_estrela']*n)}/{n}  ({100*metricas['acc_um_estrela']:.1f}%)\n")
        f.write(f"Erro médio absoluto         : {df['erro_abs'].mean():.3f} estrelas\n")
        f.write(f"F1 macro                    : {metricas['f1_macro']:.4f}\n")
        f.write(f"F1 weighted                 : {metricas['f1_weighted']:.4f}\n\n")

        # Distribuição de ratings reais vs preditos
        f.write("Distribuição de ratings:\n")
        f.write(f"{'Estrelas':<10} {'Real':>8} {'Predito':>9}\n")
        f.write("-" * 30 + "\n")
        for estrela in range(1, 6):
            real_n = int((df["rating"] == estrela).sum())
            pred_n = int((df["predicted_rating"] == estrela).sum())
            f.write(f"{'★' * estrela:<10} {real_n:>8} {pred_n:>9}\n")

        f.write("\n\nClassification Report:\n")
        f.write(metricas["report"])

        # Exemplos de erros grandes
        erros_grandes = df[df["erro_abs"] >= 2].head(15)
        if not erros_grandes.empty:
            f.write(f"\n\nExemplos com |erro| ≥ 2 estrelas ({len(df[df['erro_abs'] >= 2])} total):\n")
            f.write("-" * 65 + "\n")
            for _, row in erros_grandes.iterrows():
                texto = str(row["review_body"])[:120]
                f.write(
                    f"  SKU {row['sku']:>6} | Real: {int(row['rating'])}★ | "
                    f"Pred: {int(row['predicted_rating'])}★ | {texto}\n"
                )

    logger.info("Relatório salvo: %s", caminho)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Carregar reviews
    df = carregar_reviews(REVIEWS_CSV)

    # 2. Carregar modelo
    tokenizer, model, device = carregar_modelo(SENTIMENT_MODEL)

    # 3. Predizer ratings
    textos = df["review_body"].tolist()
    logger.info("Iniciando predição de %d reviews…", len(textos))
    predicoes = predizer_todos(textos, tokenizer, model, device)

    df["predicted_rating"] = predicoes
    df["acertou"] = df["rating"] == df["predicted_rating"]
    df["erro_abs"] = (df["predicted_rating"] - df["rating"]).abs()

    # 4. Salvar CSV com predições
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    logger.info("CSV de predições salvo: %s", OUTPUT_CSV)

    # 5. Calcular métricas
    y_real = df["rating"].values
    y_pred = df["predicted_rating"].values
    metricas = calcular_metricas(y_real, y_pred)

    # 6. Salvar confusion matrix
    salvar_confusion_matrix(metricas["confusion_matrix"], OUTPUT_MATRIX_PNG)

    # 7. Salvar relatório texto
    salvar_relatorio(df, metricas, OUTPUT_REPORT_TXT)

    # 8. Resumo no terminal
    n = len(df)
    print("\n" + "=" * 65)
    print("RESUMO — ANÁLISE DE SENTIMENTOS")
    print("=" * 65)
    print(f"Reviews analisadas  : {n}")
    print(f"Accuracy exata      : {metricas['acc_exata']:.1%}")
    print(f"Accuracy (±1 ★)     : {metricas['acc_um_estrela']:.1%}")
    print(f"Erro médio absoluto : {df['erro_abs'].mean():.3f} estrelas")
    print(f"F1 macro            : {metricas['f1_macro']:.4f}")
    print(f"F1 weighted         : {metricas['f1_weighted']:.4f}")
    print(f"\nOutputs:")
    print(f"  {OUTPUT_CSV}")
    print(f"  {OUTPUT_MATRIX_PNG}")
    print(f"  {OUTPUT_REPORT_TXT}")
    print("=" * 65)


if __name__ == "__main__":
    main()
