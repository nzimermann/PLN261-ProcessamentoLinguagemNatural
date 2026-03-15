import re
import csv
import sys
import time
import requests
from bs4 import BeautifulSoup

MAIN_URL = "https://www.ufrgs.br/propur/ensino-pessoal/ensino/producoes/"
OUTPUT_FILE = "/data/producoes_propur.csv"
CSV_FIELDS = [
    "Tipo_documento",
    "Autor",
    "Título",
    "Orientador",
    "Coorientador",
    "Palavras_Chave",
    "Link_PDF",
    "Fonte_URL",
    "Ano",
    "Resumo",
]
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}
DELAY = 1.2

PREPOSITIONS = {
    "da",
    "de",
    "do",
    "das",
    "dos",
    "e",
    "van",
    "von",
    "di",
    "del",
    "della",
    "du",
    "le",
    "la",
    "des",
    "y",
}

PROF_PREFIX_RE = re.compile(
    r"^(?:[Pp]rof(?:ª|\.ª|a)?\.?\s*)"
    r"(?:[Dd]r(?:ª|\.ª|a)?\.?\s*|[Mm]e\.?\s*|[Mm]sc\.?\s*|[Pp][Hh][Dd]\.?\s*|[Mm]s\.?\s*)?"
)


def normalize_allcaps_name(raw):
    raw = " ".join(raw.split()).strip()
    if not raw:
        return ""
    words = raw.split()
    sobrenome = words[-1].upper()
    given = []
    for w in words[:-1]:
        wl = w.lower()
        if wl in PREPOSITIONS:
            given.append(wl)
        else:
            given.append(w[0].upper() + w[1:].lower() if len(w) > 1 else w.upper())
    return f"{sobrenome}, {' '.join(given)}" if given else sobrenome


def normalize_prof_name(raw):
    raw = " ".join(raw.split()).strip()
    raw = PROF_PREFIX_RE.sub("", raw).strip().strip(".,;")
    if not raw:
        return ""
    words = raw.split()
    sobrenome = words[-1].upper()
    given = []
    for w in words[:-1]:
        if w.lower() in PREPOSITIONS:
            given.append(w.lower())
        else:
            given.append(w)
    return f"{sobrenome}, {' '.join(given)}" if given else sobrenome


def normalize_keywords(raw):
    raw = raw.strip().rstrip(".")
    for sep in [";", "|"]:
        parts = raw.split(sep)
        if len(parts) > 1:
            break
    else:
        parts_dot = re.split(r"\.\s+", raw)
        parts = parts_dot if len(parts_dot) > 1 else re.split(r",\s*", raw)
    return "|".join(p.strip().strip(".,;").lower() for p in parts if p.strip())


def clean(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def replace_br(tag):
    for br in tag.find_all("br"):
        br.replace_with("\n")


def fetch_soup(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return BeautifulSoup(resp.text, "html.parser")
        except Exception:
            if attempt < retries - 1:
                time.sleep(3)
    return None


def get_container(soup):
    for sel in [
        "div.page-content-wrap",
        "div.entry-content",
        "div.page-content",
        "article",
    ]:
        c = soup.select_one(sel)
        if c:
            return c
    return soup.body


def is_titulo_paragraph(elem):
    return (
        hasattr(elem, "name")
        and elem.name == "p"
        and bool(re.search(r"T[íi]tulo", elem.get_text(), re.IGNORECASE))
    )


def group_entries(container):
    groups, current = [], []
    for child in container.children:
        if is_titulo_paragraph(child):
            if current:
                groups.append(current)
            current = [child]
        elif current:
            current.append(child)
    if current:
        groups.append(current)
    return groups


def parse_header(p_tag):
    p_copy = BeautifulSoup(str(p_tag), "html.parser").find("p")
    text = ""

    if not p_copy is None:
        replace_br(p_copy)
        text = p_copy.get_text()

    titulo = ""
    for span in p_tag.find_all("span", style=True):
        if re.search(r"color\s*:\s*#[cC][fF]", span.get("style", "")):
            titulo += " " + span.get_text(" ")
    titulo = clean(titulo)

    if not titulo:
        m = re.search(
            r"T[íi]tulo\s*:?\s*(.+?)(?=\n(?:Autor|Linha de Pesquisa|Orientador|Co.?orientador|Data da Defesa)|\Z)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        titulo = clean(m.group(1)) if m else ""

    m = re.search(
        r"Autor[ae]?\s*:\s*([A-ZÁÉÍÓÚÃÕÂÊÔÀÇÜ][^\n]+?)(?=\n|\Z)", text, re.IGNORECASE
    )
    autor = clean(m.group(1)) if m else ""

    m = re.search(
        r"Orientador[ae]?\s*:\s*(.+?)(?=\n(?:Co.?orientador|Data da Defesa|Banca|Linha de Pesquisa)|\Z)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    orientador = clean(m.group(1)) if m else ""

    m = re.search(
        r"Co.?orientador[ae]?\s*:\s*(.+?)(?=\n(?:Data da Defesa|Banca|Linha de Pesquisa)|\Z)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    coorientador = clean(m.group(1)) if m else None

    m = re.search(r"Data da Defesa\s*:\s*\d{1,2}/\d{1,2}/(\d{4})", text, re.IGNORECASE)
    year = int(m.group(1)) if m else None

    return titulo, autor, orientador, coorientador, year


def extract_link(elem):
    a = elem.find("a", href=True)
    if not a:
        return None
    href = a["href"].strip()
    return None if ("ainda" in href.lower() or not href.startswith("http")) else href


def parse_new_format_entry(elements):
    titulo, autor, orient, coori, year = parse_header(elements[0])
    resumo, palavras_chave, link_pdf = "", "", None

    for elem in elements[1:]:
        if not hasattr(elem, "name"):
            continue
        if elem.name == "details":
            summary = elem.find("summary")
            if not summary or "RESUMO" not in summary.get_text().upper():
                continue
            full_text = ""
            for ip in elem.find_all("p"):
                ip_copy = BeautifulSoup(str(ip), "html.parser").find("p")
                full_text = ""

                if not ip_copy is None:
                    replace_br(ip_copy)
                    full_text += ip_copy.get_text() + "\n"

            pk_match = re.search(
                r"Palavras.chave\s*:\s*(.+?)(?=\nKeywords|\Z)",
                full_text,
                re.IGNORECASE | re.DOTALL,
            )
            if pk_match:
                palavras_chave = normalize_keywords(pk_match.group(1))
                cut = full_text.lower().find("palavras")
                resumo_text = full_text[:cut] if cut != -1 else full_text
            else:
                resumo_text = full_text
            resumo = clean(resumo_text)
        elif elem.name == "p":
            t = elem.get_text()
            if re.search(r"Texto [Cc]ompleto", t):
                link_pdf = extract_link(elem)

    return {
        "titulo": titulo,
        "autor": normalize_allcaps_name(autor),
        "orientador": normalize_prof_name(orient),
        "coorientador": normalize_prof_name(coori) if coori else None,
        "palavras_chave": palavras_chave,
        "link_pdf": link_pdf,
        "year": year,
        "resumo": resumo,
    }


def parse_old_format_entry(elements):
    titulo, autor, orient, coori, year = parse_header(elements[0])
    resumo, palavras_chave, link_pdf = "", "", None
    in_resumo = False

    for elem in elements[1:]:
        if not hasattr(elem, "name") or elem.name != "p":
            continue
        p_copy = BeautifulSoup(str(elem), "html.parser").find("p")
        text = ""

        if not p_copy is None:
            replace_br(p_copy)
            text = p_copy.get_text()

        if re.search(r"\bResumo\s*:", text, re.IGNORECASE):
            in_resumo = True
            m = re.search(r"\bResumo\s*:\s*(.+)", text, re.IGNORECASE | re.DOTALL)
            resumo = clean(m.group(1)) if m else ""
        elif re.search(r"Palavras.chave\s*:", text, re.IGNORECASE):
            in_resumo = False
            m = re.search(r"Palavras.chave\s*:\s*(.+)", text, re.IGNORECASE | re.DOTALL)
            if m:
                palavras_chave = normalize_keywords(m.group(1))
        elif re.search(r"\b(?:Abstract|Keywords)\b", text, re.IGNORECASE):
            in_resumo = False
        elif re.search(r"Texto [Cc]ompleto", text, re.IGNORECASE):
            in_resumo = False
            link_pdf = extract_link(elem)
        elif in_resumo:
            resumo = clean(resumo + " " + clean(text))

    return {
        "titulo": titulo,
        "autor": normalize_allcaps_name(autor),
        "orientador": normalize_prof_name(orient),
        "coorientador": normalize_prof_name(coori) if coori else None,
        "palavras_chave": palavras_chave,
        "link_pdf": link_pdf,
        "year": year,
        "resumo": resumo,
    }


def is_new_format(doc_type, year):
    if year is None:
        return False
    return year >= 2023 if doc_type == "Dissertação" else year >= 2022


def extract_year_from_url(url):
    m = re.search(r"-(\d{4})(?:-ate-\d{4})?/?(?:[^0-9]|$)", url)
    if m:
        return int(m.group(1))
    m = re.search(r"/(\d{4})/", url)
    if m:
        return int(m.group(1))
    return None


def get_page_links(soup):
    container = get_container(soup)
    links = []
    seen_urls = set()
    for a in container.find_all("a", href=True):
        href = a["href"].strip()
        label = a.get_text(strip=True)
        if href in seen_urls:
            continue
        if re.search(r"disserta[cç]", label, re.IGNORECASE):
            links.append(("Dissertação", href))
            seen_urls.add(href)
        elif re.search(r"\btese[s]?\b", label, re.IGNORECASE):
            links.append(("Tese", href))
            seen_urls.add(href)
    return links


def parse_page(url, doc_type, url_year):
    soup = fetch_soup(url)
    if not soup:
        print(f"    [ERRO] Falha ao acessar: {url}")
        return []

    container = get_container(soup)
    groups = group_entries(container)
    new_fmt = is_new_format(doc_type, url_year)
    records = []

    for group in groups:
        try:
            parsed = (
                parse_new_format_entry(group)
                if new_fmt
                else parse_old_format_entry(group)
            )
        except Exception as exc:
            print(f"    [AVISO] Erro ao parsear entrada: {exc}")
            continue

        if not parsed["titulo"]:
            continue

        records.append(
            {
                "Tipo_documento": doc_type,
                "Autor": parsed["autor"],
                "Título": parsed["titulo"],
                "Orientador": parsed["orientador"],
                "Coorientador": parsed["coorientador"],
                "Palavras_Chave": parsed["palavras_chave"],
                "Link_PDF": parsed["link_pdf"],
                "Fonte_URL": url,
                "Ano": parsed["year"] or url_year,
                "Resumo": parsed["resumo"],
            }
        )

    return records


def main():
    print(f"[1/3] Acessando página principal: {MAIN_URL}")
    main_soup = fetch_soup(MAIN_URL)
    if not main_soup:
        print("Erro fatal: não foi possível acessar a página principal.")
        sys.exit(1)

    links = get_page_links(main_soup)
    print(f"[2/3] {len(links)} páginas encontradas. Iniciando extração...\n")

    seen = set()
    all_records = []

    for doc_type, url in links:
        url_year = extract_year_from_url(url)
        fmt_label = "novo" if is_new_format(doc_type, url_year) else "antigo"
        print(f"  [{doc_type} | {url_year} | formato {fmt_label}] {url}")

        records = parse_page(url, doc_type, url_year)

        added = 0
        for r in records:
            key = (r["Autor"], r["Título"])
            if key not in seen and r["Título"]:
                seen.add(key)
                all_records.append(r)
                added += 1

        print(f"    → {added} registro(s) adicionado(s) ({len(all_records)} total)")
        time.sleep(DELAY)

    print(f"\n[3/3] Salvando {len(all_records)} registros em '{OUTPUT_FILE}'...")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(all_records)

    print(f"Concluído! Arquivo gerado: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
