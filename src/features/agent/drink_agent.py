"""
Agente especialista em bebidas — RAG com BERTugues + Gemini.

Fluxo por consulta:
    1. Embed da query com BERTugues (mesmo modelo usado na vetorização)
    2. Retrieve top-k produtos mais similares via FAISS (busca exata)
    3. Montagem do contexto em blocos <produto_i> com dados reais do CSV
    4. Geração de resposta fundamentada com Gemini (google-generativeai)

O agente responde APENAS com base nos produtos recuperados, citando
explicitamente cada recomendação com SKU e nome.

Pré-requisitos:
    python construir_indice_rag.py   (gera faiss.index e rag_metadata.json)

Uso:
    pip install faiss-cpu google-generativeai sentence-transformers
    export GOOGLE_API_KEY="sua_chave"
    python agente_bebidas.py
"""

import json
import os
import textwrap
from getpass import getpass

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

from google.generativeai.client import configure as gemini_configure
from google.generativeai.generative_models import GenerativeModel
from google.generativeai.types import GenerationConfig

from src import logger
from src.config import VECTORS_RAG

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

INDEX_PATH = VECTORS_RAG / "faiss.index"
META_PATH = VECTORS_RAG / "rag_metadata.json"

# Modelo de embedding — deve ser o mesmo usado em vectorize_bert.py
BERT_MODEL = "neuralmind/bert-base-portuguese-cased"

# Gemini: ajuste o modelo conforme sua cota de API
# Opções comuns: "gemini-1.5-flash", "gemini-1.5-pro", "gemini-2.0-flash"
GEMINI_MODEL = "gemini-2.5-flash"

# Número de produtos recuperados por consulta
TOP_K = 5

# Máximo de caracteres da descrição por produto no contexto
MAX_CHARS_DESCRICAO = 800

# Parâmetros de geração do Gemini
TEMPERATURE = 0.3  # baixo: respostas mais focadas e precisas
MAX_OUTPUT_TOKENS = 1024

# ---------------------------------------------------------------------------
# System instruction — personalidade e regras do agente
# ---------------------------------------------------------------------------

SYSTEM_INSTRUCTION = """\
Você é um sommelier e especialista em bebidas alcoólicas com profundo conhecimento \
em destilados, vinhos, cervejas e licores brasileiros e internacionais.

Seu papel é recomendar bebidas com base nas preferências e características descritas \
pelo usuário, utilizando EXCLUSIVAMENTE os produtos fornecidos no contexto abaixo.

Regras:
- Cite sempre o SKU e o nome de cada produto recomendado.
- Explique por que o produto atende à solicitação, mencionando características \
relevantes (notas de sabor, aroma, origem, preço, ocasião de consumo).
- Se um produto atender parcialmente, diga o que falta.
- Se nenhum produto do contexto for adequado, informe claramente — não invente produtos.
- Responda sempre em português brasileiro, com tom acolhedor e profissional.
- Ao final, sugira como o usuário pode refinar a busca para encontrar o ideal.
"""

# ---------------------------------------------------------------------------
# Carregamento dos artefatos
# ---------------------------------------------------------------------------


def carregar_indice_e_metadados() -> tuple[faiss.Index, list[dict]]:
    """Carrega o índice FAISS e os metadados dos produtos."""
    for caminho in [INDEX_PATH, META_PATH]:
        if not caminho.exists():
            logger.error("Arquivo não encontrado: %s", caminho)
            logger.error("Execute construir_indice_rag.py antes desta etapa.")
            raise FileNotFoundError(caminho)

    indice = faiss.read_index(str(INDEX_PATH))
    metadados = json.loads(META_PATH.read_text(encoding="utf-8"))

    logger.info("Índice FAISS carregado: %d vetores  dim=%d", indice.ntotal, indice.d)
    logger.info("Metadados carregados  : %d produtos", len(metadados))

    return indice, metadados


def carregar_modelo_bert() -> SentenceTransformer:
    """Carrega o modelo BERTugues para embedding das queries."""
    logger.info("Carregando modelo de embedding: %s", BERT_MODEL)
    modelo = SentenceTransformer(BERT_MODEL)
    logger.info("Modelo carregado.")
    return modelo


def configurar_gemini() -> GenerativeModel:
    """Configura a API do Gemini e retorna o modelo generativo."""
    chave = os.environ.get("GOOGLE_API_KEY", "").strip()
    if not chave:
        print("\nGoogle API Key não encontrada na variável GOOGLE_API_KEY.")
        chave = getpass("Informe sua GOOGLE_API_KEY: ").strip()
        os.environ["GOOGLE_API_KEY"] = chave

    gemini_configure(api_key=chave)
    modelo = GenerativeModel(GEMINI_MODEL)
    logger.info("Gemini configurado: %s", GEMINI_MODEL)
    return modelo


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------


def embed_query(texto: str, modelo_bert: SentenceTransformer) -> np.ndarray:
    """Embeda a query com BERTugues e normaliza para cosine similarity."""
    vetor = modelo_bert.encode([texto], convert_to_numpy=True)[0]

    norma = np.linalg.norm(vetor)
    if norma > 0:
        vetor = vetor / norma

    return vetor.astype(np.float32)


def recuperar_produtos(
    query: str,
    modelo_bert: SentenceTransformer,
    indice: faiss.Index,
    metadados: list[dict],
    k: int = TOP_K,
) -> list[tuple[dict, float]]:
    """Recupera os k produtos mais relevantes para a query.

    Retorna lista de (metadado, score) em ordem decrescente de similaridade.
    """
    vetor_query = embed_query(query, modelo_bert)
    scores, indices = indice.search(vetor_query.reshape(1, -1), k=k)

    resultados = []
    for idx, score in zip(indices[0], scores[0]):
        if idx == -1:  # FAISS retorna -1 quando não há resultados suficientes
            continue
        resultados.append((metadados[idx], float(score)))

    return resultados


# ---------------------------------------------------------------------------
# Construção do contexto RAG
# ---------------------------------------------------------------------------


def _formatar_preco(preco) -> str:
    if preco is None:
        return "Não informado"
    try:
        return f"R$ {float(preco):.2f}"
    except (ValueError, TypeError):
        return str(preco)


def _truncar(texto: str, max_chars: int) -> str:
    if not texto or texto == "nan":
        return "Não disponível."
    return texto if len(texto) <= max_chars else texto[:max_chars].rstrip() + "…"


def construir_contexto(resultados: list[tuple[dict, float]]) -> str:
    """Monta os blocos de contexto no formato <produto_i> para o prompt.

    Cada bloco contém os dados estruturados do produto: SKU, nome, marca,
    categoria, preço e descrição. O score de similaridade é incluído para
    que o modelo saiba o grau de relevância de cada item.
    """
    blocos = []

    for i, (meta, score) in enumerate(resultados):
        descricao = _truncar(meta.get("descricao", ""), MAX_CHARS_DESCRICAO)
        preco = _formatar_preco(meta.get("preco"))
        estoque = "Em estoque" if meta.get("em_estoque", True) else "Fora de estoque"

        bloco = (
            f"<produto_{i}>\n"
            f"SKU       : {meta.get('sku', '?')}\n"
            f"Nome      : {meta.get('nome', '?')}\n"
            f"Marca     : {meta.get('marca', '?')}\n"
            f"Categoria : {meta.get('categoria', '?')}\n"
            f"Preço     : {preco}\n"
            f"Estoque   : {estoque}\n"
            f"Relevância: {score:.4f}\n"
            f"Descrição : {descricao}\n"
            f"</produto_{i}>"
        )
        blocos.append(bloco)

    return "\n\n".join(blocos)


# ---------------------------------------------------------------------------
# Geração da resposta com Gemini
# ---------------------------------------------------------------------------


def gerar_resposta(
    query: str,
    contexto: str,
    modelo_gemini: GenerativeModel,
) -> str:
    """Monta o prompt RAG e gera a resposta com Gemini."""

    prompt = textwrap.dedent(f"""
    {SYSTEM_INSTRUCTION}

    [CONSULTA DO USUÁRIO]
    {query}

    [PRODUTOS RECUPERADOS]
    {contexto}

    [INSTRUÇÕES DE SAÍDA]
    - Baseie-se EXCLUSIVAMENTE nos produtos acima.
    - Recomende os mais adequados, citando (produto_i), SKU e nome.
    - Justifique cada recomendação com base nas características do produto.
    - Se nenhum produto for ideal, diga explicitamente e explique o porquê.
    - Finalize com uma sugestão de como refinar a busca.
    """).strip()

    cfg = GenerationConfig(
        temperature=TEMPERATURE,
        max_output_tokens=MAX_OUTPUT_TOKENS,
    )

    resposta = modelo_gemini.generate_content(prompt, generation_config=cfg)

    # Fallback seguro para diferentes versões da SDK
    texto = getattr(resposta, "text", None)
    if not texto and hasattr(resposta, "candidates") and resposta.candidates:
        texto = resposta.candidates[0].content.parts[0].text

    return texto or "[Sem resposta retornada pelo modelo]"


# ---------------------------------------------------------------------------
# Pipeline RAG completo
# ---------------------------------------------------------------------------


def consultar(
    query: str,
    modelo_bert: SentenceTransformer,
    indice: faiss.Index,
    metadados: list[dict],
    modelo_gemini: GenerativeModel,
) -> None:
    """Executa o ciclo RAG completo para uma consulta e imprime os resultados."""

    # 1. Recuperação
    resultados = recuperar_produtos(query, modelo_bert, indice, metadados)
    if not resultados:
        print("\n[!] Nenhum produto recuperado. Tente uma consulta diferente.")
        return

    # 2. Contexto
    contexto = construir_contexto(resultados)

    # 3. Geração
    print("\nConsultando Gemini…")
    resposta = gerar_resposta(query, contexto, modelo_gemini)

    # 4. Saída
    separador = "=" * 60
    print(f"\n{separador}")
    print("RECOMENDAÇÃO DO ESPECIALISTA")
    print(separador)
    print(resposta)

    print(f"\n{separador}")
    print(f"PRODUTOS CONSULTADOS (top {len(resultados)})")
    print(separador)
    for i, (meta, score) in enumerate(resultados):
        print(
            f"  [{i}] SKU {meta.get('sku','?'):<6}  "
            f"score={score:.4f}  "
            f"{meta.get('nome','?')[:55]}"
        )
    print()


# ---------------------------------------------------------------------------
# Loop interativo
# ---------------------------------------------------------------------------


def main() -> None:
    print("\n" + "=" * 60)
    print("AGENTE ESPECIALISTA EM BEBIDAS")
    print("RAG com BERTugues + Gemini")
    print("=" * 60)

    print("\nCarregando recursos…")
    indice, metadados = carregar_indice_e_metadados()
    modelo_bert = carregar_modelo_bert()
    modelo_gemini = configurar_gemini()

    print("\nPronto! Digite sua consulta (Enter vazio para sair).")
    print("Exemplos:")
    print('  "Quero um whisky com notas amadeiradas e defumadas"')
    print('  "Gin floral e cítrico para fazer drinks"')
    print('  "Vinho tinto encorpado para harmonizar com carnes"')
    print('  "Cachaça premium para presente até R$ 200"')
    print()

    while True:
        try:
            query = input("Consulta: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nEncerrando agente.")
            break

        if not query:
            print("Encerrando agente.")
            break

        consultar(query, modelo_bert, indice, metadados, modelo_gemini)


if __name__ == "__main__":
    main()
