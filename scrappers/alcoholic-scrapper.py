import json
import time
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

BASE_URL = "https://www.casadabebida.com.br"

CATEGORIAS: list[str] = [
    "https://www.casadabebida.com.br/cachaca/",
    "https://www.casadabebida.com.br/conhaque/",
    "https://www.casadabebida.com.br/coquetel/",
    "https://www.casadabebida.com.br/espumante/",
    "https://www.casadabebida.com.br/licor/",
    "https://www.casadabebida.com.br/rum/",
    "https://www.casadabebida.com.br/sake/",
    "https://www.casadabebida.com.br/whisky/",
    "https://www.casadabebida.com.br/gin/",
    "https://www.casadabebida.com.br/tequila/",
    "https://www.casadabebida.com.br/vinho/",
    "https://www.casadabebida.com.br/champagne/",
    "https://www.casadabebida.com.br/vodka/",
]

MENSAGEM_SEM_PRODUTOS = "não há produtos disponíveis nesta categoria"

OUTPUT_DIR = Path("/data")

REQUEST_DELAY = 1.0

REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------


def extrair_slug_categoria(url_categoria: str) -> str:
    """Retorna o slug da categoria a partir da URL.

    Exemplo:
        "https://www.casadabebida.com.br/cachaca/" -> "cachaca"
    """
    partes = url_categoria.rstrip("/").split("/")
    return partes[-1]


def montar_url_pagina(url_categoria: str, numero_pagina: int) -> str:
    """Monta a URL de uma página específica dentro de uma categoria.

    Página 1 é a URL base; páginas seguintes recebem o sufixo /pagina-N/.

    Exemplos:
        (cachaca/, 1) -> "https://.../cachaca/"
        (cachaca/, 3) -> "https://.../cachaca/pagina-3/"
    """
    if numero_pagina == 1:
        return url_categoria
    return f"{url_categoria}pagina-{numero_pagina}/"


def fazer_requisicao(url: str, sessao: requests.Session) -> requests.Response | None:
    """Realiza uma requisição GET e retorna a Response.

    Retorna None em caso de falha de rede ou status HTTP de erro.
    """
    try:
        resposta = sessao.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resposta.raise_for_status()
        return resposta
    except requests.RequestException as exc:
        log.warning("Falha ao acessar %s: %s", url, exc)
        return None


def pagina_sem_produtos(soup: BeautifulSoup) -> bool:
    """Verifica se a página contém a mensagem de categoria sem produtos."""
    texto = soup.get_text(separator=" ").lower()
    return MENSAGEM_SEM_PRODUTOS in texto


def extrair_links_produtos(soup: BeautifulSoup) -> list[str]:
    """Extrai os links absolutos de todos os produtos na página.

    Os links ficam no atributo href das tags <a class="product-img">
    dentro de cada <div class="product-thumb">.
    """
    links: list[str] = []

    for card in soup.find_all("div", class_="product-thumb"):
        ancora = card.find("a", class_="product-img")
        if not ancora:
            continue

        href = ancora.get("href")
        href = href.strip() if isinstance(href, str) else ""
        if not href:
            continue

        # Garante que o link seja absoluto
        if not href.startswith("http"):
            href = BASE_URL + href

        links.append(href)

    return links


# ---------------------------------------------------------------------------
# Lógica principal por categoria
# ---------------------------------------------------------------------------


def coletar_links_categoria(url_categoria: str, sessao: requests.Session) -> list[str]:
    """Coleta os links de todos os produtos de uma categoria, paginando.

    Interrompe quando encontra uma página sem produtos.
    """
    slug = extrair_slug_categoria(url_categoria)
    todos_links: list[str] = []
    pagina = 1

    log.info("Iniciando categoria '%s'", slug)

    while pagina < 10:
        url_pagina = montar_url_pagina(url_categoria, pagina)
        log.info("  Buscando página %d: %s", pagina, url_pagina)

        resposta = fazer_requisicao(url_pagina, sessao)

        if resposta is None:
            log.warning(
                "  Encerrando '%s' na página %d por falha de requisição.", slug, pagina
            )
            break

        soup = BeautifulSoup(resposta.text, "html.parser")

        if pagina_sem_produtos(soup):
            log.info(
                "  Página %d sem produtos — paginação encerrada para '%s'.",
                pagina,
                slug,
            )
            break

        links_pagina = extrair_links_produtos(soup)

        if not links_pagina:
            # Não encontrou cards de produto mas também não exibiu mensagem de
            # categoria vazia — encerra por precaução.
            log.info(
                "  Nenhum produto encontrado na página %d de '%s' — encerrando.",
                pagina,
                slug,
            )
            break

        todos_links.extend(links_pagina)
        log.info(
            "  %d produto(s) coletado(s) nesta página (total: %d).",
            len(links_pagina),
            len(todos_links),
        )

        pagina += 1
        time.sleep(REQUEST_DELAY)

    log.info(
        "Categoria '%s' concluída: %d produto(s) no total.", slug, len(todos_links)
    )
    return todos_links


def salvar_json(slug: str, links: list[str], diretorio: Path) -> None:
    """Salva a lista de links em um arquivo JSON nomeado pelo slug da categoria."""
    diretorio.mkdir(parents=True, exist_ok=True)
    caminho = diretorio / f"{slug}.json"

    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(links, f, ensure_ascii=False, indent=4)

    log.info("Salvo: %s (%d links)", caminho, len(links))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    sessao = requests.Session()

    for url_categoria in CATEGORIAS:
        slug = extrair_slug_categoria(url_categoria)

        links = coletar_links_categoria(url_categoria, sessao)
        salvar_json(slug, links, OUTPUT_DIR)
        time.sleep(REQUEST_DELAY)

    log.info("Scraping concluído.")


if __name__ == "__main__":
    main()
