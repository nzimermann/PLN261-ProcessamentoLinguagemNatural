"""
Extrator de dados de produtos da Casa da Bebida.

Para cada arquivo *_links.json em data/raw/product_links/, acessa cada URL
de produto, extrai o bloco <script type="application/ld+json"> e salva as
linhas em data/raw/extracted_products/<categoria>_products.jsonl.

Uso:
    python extrair_produtos.py

Dependências:
    pip install requests beautifulsoup4
"""

import json
import logging
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

PRODUCT_LINKS_DIR = Path("data/raw/product_links")
EXTRACTED_PRODUCTS_DIR = Path("data/raw/extracted_products")

# Intervalo entre requisições (segundos)
REQUEST_DELAY = 1.0

# Timeout por requisição (segundos)
REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------


def listar_arquivos_de_links(diretorio: Path) -> list[Path]:
    """Retorna todos os arquivos *_links.json no diretório informado, ordenados."""
    arquivos = sorted(diretorio.glob("*_links.json"))
    if not arquivos:
        log.warning("Nenhum arquivo *_links.json encontrado em '%s'.", diretorio)
    return arquivos


def slug_para_nome_jsonl(arquivo_links: Path) -> str:
    """Converte o nome do arquivo de links no nome do arquivo de saída .jsonl.

    Exemplo:
        cachaca_links.json -> cachaca_products.jsonl
    """
    slug = arquivo_links.stem.replace("_links", "")
    return f"{slug}_products.jsonl"


def carregar_links(arquivo: Path) -> list[str]:
    """Carrega a lista de URLs de produto de um arquivo JSON."""
    with open(arquivo, encoding="utf-8") as f:
        dados = json.load(f)
    if not isinstance(dados, list):
        raise ValueError(f"Formato inesperado em {arquivo}: esperava uma lista.")
    return dados


def fazer_requisicao(url: str, sessao: requests.Session) -> requests.Response | None:
    """Realiza uma requisição GET. Retorna None em caso de falha."""
    try:
        resposta = sessao.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resposta.raise_for_status()
        return resposta
    except requests.RequestException as exc:
        log.warning("Falha ao acessar %s: %s", url, exc)
        return None


def extrair_ldjson(soup: BeautifulSoup) -> dict | None:
    """Extrai e faz o parse do primeiro bloco <script type='application/ld+json'>.

    Retorna None se o bloco não existir ou não for um JSON válido.
    """
    tag = soup.find("script", type="application/ld+json")
    if not tag or not tag.string:
        return None
    try:
        return json.loads(tag.string)
    except json.JSONDecodeError as exc:
        log.warning("JSON inválido no ld+json: %s", exc)
        return None


def serializar_jsonl(dados: dict) -> str:
    """Serializa um dicionário como uma linha JSON (sem quebra de linha no final)."""
    return json.dumps(dados, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Lógica principal por categoria
# ---------------------------------------------------------------------------


def processar_categoria(
    arquivo_links: Path,
    diretorio_saida: Path,
    sessao: requests.Session,
) -> None:
    """Processa todos os links de uma categoria e grava o .jsonl de saída.

    Links duplicados ou que falhem na requisição são ignorados com log de aviso.
    Se o arquivo .jsonl de saída já existir, ele é sobrescrito.
    """
    nome_saida = slug_para_nome_jsonl(arquivo_links)
    caminho_saida = diretorio_saida / nome_saida

    links = carregar_links(arquivo_links)
    links_unicos = list(dict.fromkeys(links))  # preserva ordem, remove duplicatas

    duplicatas = len(links) - len(links_unicos)
    if duplicatas:
        log.info(
            "  %d link(s) duplicado(s) removido(s) de '%s'.",
            duplicatas,
            arquivo_links.name,
        )

    log.info(
        "Processando '%s': %d link(s) únicos → '%s'",
        arquivo_links.name,
        len(links_unicos),
        caminho_saida,
    )

    extraidos = 0
    falhas = 0

    with open(caminho_saida, "w", encoding="utf-8") as f_saida:
        for i, url in enumerate(links_unicos, start=1):
            log.info("  [%d/%d] %s", i, len(links_unicos), url)

            resposta = fazer_requisicao(url, sessao)
            if resposta is None:
                falhas += 1
                time.sleep(REQUEST_DELAY)
                continue

            soup = BeautifulSoup(resposta.text, "html.parser")
            dados = extrair_ldjson(soup)

            if dados is None:
                log.warning("  ld+json não encontrado em: %s", url)
                falhas += 1
                time.sleep(REQUEST_DELAY)
                continue

            f_saida.write(serializar_jsonl(dados) + "\n")
            extraidos += 1

            time.sleep(REQUEST_DELAY)

    log.info(
        "  Concluído: %d extraído(s), %d falha(s). Salvo em '%s'.",
        extraidos,
        falhas,
        caminho_saida,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    EXTRACTED_PRODUCTS_DIR.mkdir(parents=True, exist_ok=True)

    sessao = requests.Session()
    arquivos = listar_arquivos_de_links(PRODUCT_LINKS_DIR)

    if not arquivos:
        log.error(
            "Nenhum arquivo de links encontrado. "
            "Verifique se o diretório '%s' existe e contém arquivos *_links.json.",
            PRODUCT_LINKS_DIR,
        )
        return

    for arquivo in arquivos:
        processar_categoria(arquivo, EXTRACTED_PRODUCTS_DIR, sessao)
        time.sleep(REQUEST_DELAY)

    log.info("Extração concluída para %d categoria(s).", len(arquivos))


if __name__ == "__main__":
    main()
