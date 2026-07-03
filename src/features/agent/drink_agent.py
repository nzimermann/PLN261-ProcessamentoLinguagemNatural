from __future__ import annotations

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
import re
import textwrap
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
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

# Pool maior usado antes do reranqueamento semântico/contextual.
CANDIDATE_POOL = 12

# Quantidade de turnos preservados em memória curta.
HISTORICO_MAX_TURNOS = 4

# Máximo de caracteres da descrição por produto no contexto
MAX_CHARS_DESCRICAO = 800

# Máximo de caracteres guardados por turno para contexto da conversa.
MAX_CHARS_TURNO = 280

# Parâmetros de geração do Gemini
TEMPERATURE = 0.3  # baixo: respostas mais focadas e precisas
MAX_OUTPUT_TOKENS = 1536 # limitar output pra não estourar cota de tokens do Gemini

# Quantos turnos entram efetivamente no prompt; a memória interna continua maior.
HISTORICO_PROMPT_TURNOS = 2

# Peso de cada sinal no reranqueamento final.
PESO_SEMANTICO = 0.56
PESO_LEXICAL = 0.16
PESO_FAMILIA = 0.22
PESO_ESTILO = 0.06

# Bônus quando a query deixa clara a família prioritária.
PRIORIDADE_FAMILIA_BONUS = {
    1: 0.22,
    2: 0.12,
}

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
- Use o histórico recente da conversa quando a solicitação for uma continuidade \
  do turno anterior.
"""


FAMILY_KEYWORDS: dict[str, set[str]] = {
    "vinho": {
        "vinho",
        "vinhos",
        "tinto",
        "branco",
        "rose",
        "rosé",
        "malbec",
        "cabernet",
        "merlot",
        "syrah",
        "pinot",
        "sauvignon",
        "porto",
        "alentejo",
        "douro",
    },
    "espumante": {
        "espumante",
        "champagne",
        "champanhe",
        "cava",
        "prosecco",
        "brut",
        "proseco",
    },
    "aperitivo": {"aperitivo", "aperol", "spritz", "pisco"},
    "whisky": {
        "whisky",
        "whiskey",
        "scotch",
        "bourbon",
        "single malt",
        "blended",
        "malte",
    },
    "gin": {"gin", "botanical", "botanicals", "tonica", "tônica"},
    "vodka": {"vodka"},
    "cachaca": {"cachaca", "cachaça", "aguardente", "alambique", "cana"},
    "licor": {"licor", "liqueur", "creme"},
    "rum": {"rum", "ron"},
    "tequila": {"tequila", "mezcal"},
    "sake": {"sake", "saque", "saquê"},
    "conhaque": {"conhaque", "cognac", "brandy", "armagnac"},
}


QUERY_SIGNAL_BOOSTS: dict[str, dict[str, float]] = {
    "romantico": {"vinho": 2.6, "espumante": 2.1, "licor": 0.7},
    "jantar": {"vinho": 1.4, "espumante": 1.0, "licor": 0.4},
    "frio": {"vinho": 1.4, "whisky": 1.1, "conhaque": 1.0, "licor": 0.8},
    "inverno": {"vinho": 1.2, "whisky": 1.0, "conhaque": 0.9, "licor": 0.8},
    "presente": {
        "vinho": 0.8,
        "espumante": 0.8,
        "whisky": 0.6,
        "conhaque": 0.5,
        "licor": 0.5,
    },
    "celebracao": {"espumante": 1.8, "vinho": 1.0},
    "drinks": {"gin": 1.3, "vodka": 1.0, "rum": 0.9, "tequila": 0.9},
    "coquetel": {"gin": 1.3, "vodka": 1.0, "rum": 0.9, "tequila": 0.9},
    "barato": {},
    "economico": {},
    "economica": {},
    "econômico": {},
    "econômica": {},
}


STYLE_SIGNAL_BOOSTS: dict[str, dict[str, float]] = {
    "romantico": {
        "elegante": 0.35,
        "sofisticado": 0.35,
        "delicado": 0.25,
        "aveludado": 0.25,
        "refinado": 0.2,
    },
    "frio": {
        "encorpado": 0.3,
        "amadeirado": 0.25,
        "especiarias": 0.25,
        "quente": 0.15,
        "intenso": 0.15,
    },
}


CONTEXTO_DEPENDENTE = {
    "mais",
    "tambem",
    "também",
    "outro",
    "outra",
    "esse",
    "essa",
    "isso",
    "ele",
    "ela",
    "aquela",
    "aquele",
    "melhor",
    "barato",
    "barata",
    "caro",
    "cara",
}


@dataclass
class TurnoConversa:
    usuario: str
    assistente: str


@dataclass
class MemoriaConversacao:
    """Memória curta da conversa para turnos dependentes do contexto."""

    max_turnos: int = HISTORICO_MAX_TURNOS
    turnos: list[TurnoConversa] = field(default_factory=list)

    def registrar(self, usuario: str, assistente: str) -> None:
        self.turnos.append(
            TurnoConversa(
                usuario=_resumir_texto(usuario, MAX_CHARS_TURNO),
                assistente=_resumir_texto(assistente, MAX_CHARS_TURNO),
            )
        )
        if len(self.turnos) > self.max_turnos:
            self.turnos = self.turnos[-self.max_turnos :]

    def resumo_historico(self) -> str:
        if not self.turnos:
            return "Sem histórico recente."

        linhas: list[str] = []
        for indice, turno in enumerate(self.turnos, start=1):
            linhas.append(f"Turno {indice} - usuário: {turno.usuario}")
            linhas.append(f"Turno {indice} - assistente: {turno.assistente}")

        return "\n".join(linhas)

    def resumo_prompt(self, max_turnos: int = HISTORICO_PROMPT_TURNOS) -> str:
        if not self.turnos:
            return "Sem histórico recente."

        turnos_recentes = self.turnos[-max_turnos:]
        linhas: list[str] = []
        for turno in turnos_recentes:
            linhas.append(f"Usuário: {turno.usuario}")
            linhas.append(f"Assistente: {turno.assistente}")

        return "\n".join(linhas)

    def resumo_preferencias(self) -> str:
        contagem: Counter[str] = Counter()
        for turno in self.turnos:
            contagem.update(_extrair_sinais(turno.usuario))

        if not contagem:
            return ""

        sinais_prioritarios = [
            sinal for sinal, _ in contagem.most_common(6) if sinal not in CONTEXTO_DEPENDENTE
        ]
        return ", ".join(sinais_prioritarios)

    def expandir_consulta(self, query: str) -> str:
        partes = [query.strip()]

        if not self.turnos:
            return query.strip()

        resumo_preferencias = self.resumo_preferencias()
        if resumo_preferencias:
            partes.append(f"preferências recentes: {resumo_preferencias}")

        if _precisa_contexto(query):
            partes.append(f"contexto anterior: {self.turnos[-1].usuario}")

        return " | ".join(parte for parte in partes if parte)


def _normalizar_texto(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    return texto.lower()


def _resumir_texto(texto: str, max_chars: int) -> str:
    texto = re.sub(r"\s+", " ", texto).strip()
    if len(texto) <= max_chars:
        return texto
    return texto[:max_chars].rstrip() + "…"


def _tokenizar(texto: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", _normalizar_texto(texto))


def _texto_produto(meta: dict) -> str:
    return " ".join(
        str(meta.get(campo, ""))
        for campo in ("nome", "marca", "categoria", "descricao")
        if meta.get(campo)
    )


def _extrair_sinais(texto: str) -> list[str]:
    texto_normalizado = _normalizar_texto(texto)
    sinais: list[str] = []

    for familia, palavras in FAMILY_KEYWORDS.items():
        if any(palavra in texto_normalizado for palavra in palavras):
            sinais.append(familia)

    for sinal in QUERY_SIGNAL_BOOSTS:
        if sinal in texto_normalizado:
            sinais.append(sinal)

    return sinais


def _precisa_contexto(query: str) -> bool:
    tokens = set(_tokenizar(query))
    if len(tokens) <= 6:
        return True
    return bool(tokens & CONTEXTO_DEPENDENTE)


def inferir_familia_produto(meta: dict) -> str:
    texto = _normalizar_texto(_texto_produto(meta))
    melhor_familia = "desconhecida"
    melhor_score = 0

    for familia, palavras in FAMILY_KEYWORDS.items():
        score = sum(1 for palavra in palavras if palavra in texto)
        if score > melhor_score:
            melhor_familia = familia
            melhor_score = score

    return melhor_familia


def _pontuar_familias_consulta(query: str) -> dict[str, float]:
    texto = _normalizar_texto(query)
    pontuacoes: dict[str, float] = {familia: 0.0 for familia in FAMILY_KEYWORDS}

    for familia, palavras in FAMILY_KEYWORDS.items():
        if any(palavra in texto for palavra in palavras):
            pontuacoes[familia] += 3.0

    for sinal, boosts in QUERY_SIGNAL_BOOSTS.items():
        if sinal in texto:
            for familia, peso in boosts.items():
                pontuacoes[familia] += peso

    return pontuacoes


def _perfil_intencao(query: str) -> tuple[list[str], list[str]]:
    """Retorna (familias_primarias, familias_secundarias) para a query."""
    texto = _normalizar_texto(query)
    primarias: list[str] = []
    secundarias: list[str] = []

    tem_ocasião_romantica = any(p in texto for p in {"romantico", "romântico", "jantar"})
    tem_clima_frio = any(p in texto for p in {"frio", "inverno", "invernal", "gelado"})
    tem_drinks = any(p in texto for p in {"drink", "drinks", "coquetel", "coquetéis", "coquetel", "aperitivo"})
    tem_doce = any(p in texto for p in {"doce", "sobremesa", "sobremesas", "licor"})

    if tem_drinks:
        primarias.extend(["gin", "vodka", "rum", "tequila", "aperitivo"])
        secundarias.extend(["espumante", "vinho"])
        return primarias, secundarias

    if tem_ocasião_romantica or tem_clima_frio:
        primarias.extend(["vinho", "espumante"])
        secundarias.extend(["conhaque", "whisky"])
        if tem_doce:
            secundarias.append("licor")

    if not primarias:
        pontuacoes = _pontuar_familias_consulta(query)
        ordenadas = [familia for familia, score in sorted(pontuacoes.items(), key=lambda item: item[1], reverse=True) if score > 0]
        primarias = ordenadas[:2]
        secundarias = ordenadas[2:4]

    return primarias, secundarias


def _pontuar_estilo(query: str, meta: dict) -> float:
    texto = _normalizar_texto(query)
    produto = _normalizar_texto(_texto_produto(meta))
    pontuacao = 0.0

    for sinal, palavras in STYLE_SIGNAL_BOOSTS.items():
        if sinal not in texto:
            continue
        for palavra, peso in palavras.items():
            if palavra in produto:
                pontuacao += peso

    return min(pontuacao, 1.0)


def _pontuar_preco(query: str, meta: dict) -> float:
    texto = _normalizar_texto(query)
    if not any(
        sinal in texto for sinal in {"barato", "barata", "economico", "economica", "econômico", "econômica"}
    ):
        return 0.0

    preco = meta.get("preco")
    try:
        preco_float = float(preco)
    except (TypeError, ValueError):
        return 0.0

    # Normaliza um benefício maior para itens mais baratos.
    return max(0.0, min(1.0, 1.0 - (preco_float / 300.0)))


def pontuar_resultado(query: str, meta: dict, score_semantico: float) -> float:
    texto_query = _normalizar_texto(query)
    texto_produto = _normalizar_texto(_texto_produto(meta))

    tokens_query = set(_tokenizar(texto_query))
    tokens_produto = set(_tokenizar(texto_produto))
    overlap = tokens_query & tokens_produto
    lexical_score = len(overlap) / max(len(tokens_query), 1)

    familia = inferir_familia_produto(meta)
    pesos_familia = _pontuar_familias_consulta(query)
    familia_score = pesos_familia.get(familia, 0.0)
    if familia != "desconhecida" and familia in texto_query:
        familia_score += 0.5

    familias_primarias, familias_secundarias = _perfil_intencao(query)
    if familia in familias_primarias:
        prioridade = familias_primarias.index(familia) + 1
        familia_score += PRIORIDADE_FAMILIA_BONUS.get(prioridade, 0.0)
    elif familia in familias_secundarias:
        familia_score += 0.08
    elif familias_primarias:
        familia_score -= 0.12

    estilo_score = _pontuar_estilo(query, meta)
    preco_score = _pontuar_preco(query, meta)

    score = (
        PESO_SEMANTICO * float(score_semantico)
        + PESO_LEXICAL * lexical_score
        + PESO_FAMILIA * min(familia_score / 4.0, 1.0)
        + PESO_ESTILO * max(estilo_score, preco_score)
    )

    if meta.get("em_estoque") is False:
        score -= 0.08

    return score


def reranquear_resultados(
    query: str,
    resultados: list[tuple[dict, float]],
) -> list[tuple[dict, float]]:
    melhores_por_sku: dict[str, tuple[dict, float]] = {}

    for meta, score in resultados:
        sku = str(meta.get("sku", ""))
        if not sku:
            continue

        pontuado = (meta, pontuar_resultado(query, meta, score))
        atual = melhores_por_sku.get(sku)
        if atual is None or pontuado[1] > atual[1]:
            melhores_por_sku[sku] = pontuado

    reranqueados = list(melhores_por_sku.values())
    reranqueados.sort(key=lambda item: item[1], reverse=True)

    familias_primarias, familias_secundarias = _perfil_intencao(query)
    if familias_primarias:
        filtrados = [item for item in reranqueados if inferir_familia_produto(item[0]) in familias_primarias]
        if len(filtrados) >= 2:
            return filtrados[:TOP_K]

        filtrados = [item for item in reranqueados if inferir_familia_produto(item[0]) in (familias_primarias + familias_secundarias)]
        if filtrados:
            return filtrados[:TOP_K]

    return reranqueados


def construir_consulta_contextual(query: str, memoria: MemoriaConversacao | None) -> str:
    if memoria is None:
        return query.strip()
    return memoria.expandir_consulta(query)


def construir_memoria_prompt(memoria: MemoriaConversacao | None) -> str:
    if memoria is None:
        return "Sem histórico recente."
    return memoria.resumo_prompt()


def resumo_intencao_query(query: str) -> str:
    primarias, secundarias = _perfil_intencao(query)
    if not primarias and not secundarias:
        return "Intenção: geral."
    if secundarias:
        return f"Intenção: priorizar {', '.join(primarias[:2])} e considerar {', '.join(secundarias[:2])}."
    return f"Intenção: priorizar {', '.join(primarias[:2])}."

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
    pool = max(k, CANDIDATE_POOL)
    scores, indices = indice.search(vetor_query.reshape(1, -1), k=pool)

    resultados = []
    for idx, score in zip(indices[0], scores[0]):
        if idx == -1:  # FAISS retorna -1 quando não há resultados suficientes
            continue
        resultados.append((metadados[idx], float(score)))

    return reranquear_resultados(query, resultados)[:k]


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

    for i, (meta, score) in enumerate(resultados[:3]):
        descricao = _truncar(meta.get("descricao", ""), MAX_CHARS_DESCRICAO)
        preco = _formatar_preco(meta.get("preco"))
        estoque = "Em estoque" if meta.get("em_estoque", True) else "Fora de estoque"
        familia = inferir_familia_produto(meta)

        bloco = (
            f"<produto_{i}>\n"
            f"SKU       : {meta.get('sku', '?')}\n"
            f"Nome      : {meta.get('nome', '?')}\n"
            f"Marca     : {meta.get('marca', '?')}\n"
            f"Categoria : {meta.get('categoria', '?')}\n"
            f"Família   : {familia}\n"
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
    consulta_contextual: str,
    contexto: str,
    modelo_gemini: GenerativeModel,
    memoria: MemoriaConversacao | None = None,
) -> str:
    """Monta o prompt RAG e gera a resposta com Gemini."""

    historico = construir_memoria_prompt(memoria)

    prompt = textwrap.dedent(f"""
    {SYSTEM_INSTRUCTION}

    [HISTÓRICO RECENTE]
    {historico}

    [LEITURA RÁPIDA DA CONSULTA]
    {resumo_intencao_query(query)}

    [CONSULTA DO USUÁRIO]
    {query}

    [CONSULTA CONTEXTUALIZADA PARA BUSCA]
    {consulta_contextual}

    [PRODUTOS RECUPERADOS]
    {contexto}

    [INSTRUÇÕES DE SAÍDA]
    - Baseie-se EXCLUSIVAMENTE nos produtos acima.
    - Seja direto e objetivo, com no máximo 2 recomendações.
    - Mantenha a resposta em até 120 palavras.
    - Recomende os mais adequados, citando (produto_i), SKU e nome.
    - Justifique cada recomendação com base nas características do produto.
    - Se nenhum produto for ideal, diga explicitamente e explique o porquê.
    - Finalize com uma sugestão de como refinar a busca.
    - Evite repetir o mesmo SKU.
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
    memoria: MemoriaConversacao | None = None,
) -> None:
    """Executa o ciclo RAG completo para uma consulta e imprime os resultados."""

    consulta_contextual = construir_consulta_contextual(query, memoria)

    # 1. Recuperação
    resultados = recuperar_produtos(
        consulta_contextual,
        modelo_bert,
        indice,
        metadados,
    )
    if not resultados:
        print("\n[!] Nenhum produto recuperado. Tente uma consulta diferente.")
        return

    # 2. Contexto
    contexto = construir_contexto(resultados)

    # 3. Geração
    print("\nConsultando Gemini…")
    resposta = gerar_resposta(
        query,
        consulta_contextual,
        contexto,
        modelo_gemini,
        memoria=memoria,
    )

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

    if memoria is not None:
        memoria.registrar(query, resposta)


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
    memoria = MemoriaConversacao()

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

        consultar(
            query,
            modelo_bert,
            indice,
            metadados,
            modelo_gemini,
            memoria=memoria,
        )


if __name__ == "__main__":
    main()
