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
VECTORS_BERT = MODELS_DIR / "bert"

VECTORS_RAG = MODELS_DIR / "rag"
RAG_INDEX = VECTORS_RAG / "faiss.index"
RAG_METADATA = VECTORS_RAG / "rag_metadata.json"

CORPUS_BERT = CORPUS_DIR / "corpus_bert.jsonl"

CLUSTERS_W2V_LABELS = VECTORS_W2V / "cluster_labels.json"
CLUSTERS_W2V_CENTROIDS = VECTORS_W2V / "cluster_centroids.npy"

REPORTS_DIR = ROOT_DIR / "reports"
