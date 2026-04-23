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
    O --> R(fa:fa-bar-chart visualize_results.py)
    P --> R
    Q --> R
    Q --> S(fa:fa-bar-chart visualize_w2v_cluster.py)
```

---

- [ ] Adicionar uma gráfico de visualização 3D do modelo gerado pelo Word2Vec
    - [ ] Adicionar um tooltip por item para visualização de características do produto (sku, nome, descrição)
- [ ] Criar um script vectorize_bert.py, utilizando o BERTugues para vetorização dos produtos (verificar se é preciso gerar um corpus específico para este caso)
- [ ] Criar um heatmap do modelo gerado pelo Word2Vec para comparação dos pares de produtos mais similares
- [ ] Analisar se o processo de vetorização está correto, heatmap deveria exibir 1.0 para categorias iguais na diagonal principal
- [x] Reorganizar os arquivos para uma [estrutura padrão de ciência de dados](https://cookiecutter-data-science.drivendata.org/).
