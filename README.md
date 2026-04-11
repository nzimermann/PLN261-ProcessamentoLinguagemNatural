# PLN261-ProcessamentoLinguagemNatural
Repositório do que foi desenvolvido no curso de Processamento de Linguagem Natural

`pip install -r requirements.txt`

`python -m spacy download pt_core_news_lg`

```mermaid
flowchart TD
    A(fa:fa-robot Scrappers) -->|Get raw data| B(fa:fa-code process_drink_data.py)
    B --> C[fa:fa-table products.csv]
    B --> D[fa:fa-table reviews.csv]
    C --> E(fa:fa-code tokenizer.py)
    D --> E
    E --> F[fa:fa-file-code tokens_products.jsonl]
    E --> G[fa:fa-file-code tokens_reviews.jsonl]
    F --> H(fa:fa-code prepare_corpus.py)
    G --> H
    H --> I[fa:fa-file-code corpus_bow_tfidf.jsonl]
    H --> J[fa:fa-file-code corpus_skus.jsonl]
    H --> K[fa:fa-file-code corpus_w2v.jsonl]
    I --> L(fa:fa-code vectorize_bow.py)
    I --> M(fa:fa-code vectorize_tfidf.py)
    K --> N(fa:fa-code vectorize_w2v.py)
    L --> O[fa:fa-map bow vectors]
    M --> P[fa:fa-map tfidf vectors]
    N --> Q[fa:fa-map word2vec vectors]
```
