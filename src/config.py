from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
TOKENS_DIR = PROCESSED_DIR / "tokens"
CORPUS_DIR = PROCESSED_DIR / "corpus"

MODELS_DIR = ROOT_DIR / "models"

VECTORS_BOW = MODELS_DIR / "bow"
VECTORS_TFIDF = MODELS_DIR / "tfidf"
VECTORS_W2V = MODELS_DIR / "w2v"
