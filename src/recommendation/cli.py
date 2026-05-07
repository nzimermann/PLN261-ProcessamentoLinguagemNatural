"""
CLI interativo para o recomendador de bebidas.

Loop: buscar produto por nome → selecionar → ver recomendações →
navegar para um recomendado ou buscar de novo → sair com 'q'.

Uso:
    python -m src.recommendation.cli
"""

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from src.recommendation.engine import RecommendationEngine, Product, Recommendation

console = Console()


# ---------------------------------------------------------------------------
# Formatação
# ---------------------------------------------------------------------------


def _tabela_busca(produtos: list[Product]) -> Table:
    table = Table(
        title="Resultados da busca",
        show_lines=False,
        header_style="bold cyan",
        border_style="dim",
    )
    table.add_column("#", style="bold white", width=4, justify="right")
    table.add_column("Nome", style="white", max_width=55)
    table.add_column("Categoria", style="yellow")
    table.add_column("Marca", style="green")
    table.add_column("SKU", style="dim")

    for i, p in enumerate(produtos, 1):
        table.add_row(str(i), p.name, p.category, p.brand, p.sku)

    return table


def _tabela_recomendacoes(recs: list[Recommendation], query_name: str) -> Table:
    table = Table(
        title=f"Recomendações para: {query_name}",
        show_lines=False,
        header_style="bold magenta",
        border_style="dim",
    )
    table.add_column("#", style="bold white", width=4, justify="right")
    table.add_column("Nome", style="white", max_width=45)
    table.add_column("Categoria", style="yellow")
    table.add_column("Marca", style="green")
    table.add_column("Similaridade", style="cyan", justify="right")
    table.add_column("Cluster", style="blue", justify="center")
    table.add_column("SKU", style="dim")

    for i, r in enumerate(recs, 1):
        sim_str = f"{r.similarity:.4f}"
        table.add_row(
            str(i), r.name, r.category, r.brand, sim_str, str(r.cluster_id), r.sku
        )

    return table


# ---------------------------------------------------------------------------
# Fluxo interativo
# ---------------------------------------------------------------------------


def _selecionar_indice(prompt: str, maximo: int) -> int | None:
    """Lê um índice numérico do usuário. Retorna None se inválido."""
    try:
        valor = int(prompt)
        if 1 <= valor <= maximo:
            return valor
    except ValueError:
        pass
    return None


def main() -> None:
    console.print(
        Panel(
            Text.from_markup(
                "[bold magenta]Recomendador de Bebidas[/bold magenta]\n"
                "[dim]Baseado em Word2Vec + K-Means com diversidade cross-cluster[/dim]\n\n"
                "[dim]Comandos:[/dim]\n"
                "  [cyan]q[/cyan]  Sair\n"
                "  [cyan]v[/cyan]  Voltar à busca\n"
                "  [cyan]#[/cyan]  Selecionar item pelo número"
            ),
            border_style="magenta",
        )
    )

    console.print("[dim]Carregando artefatos…[/dim]")
    engine = RecommendationEngine()
    console.print("[green]Pronto![/green]\n")

    while True:
        # --- Fase 1: Busca ---
        query = console.input("[bold cyan]Buscar bebida:[/bold cyan] ").strip()

        if query.lower() == "q":
            break

        if not query:
            continue

        resultados = engine.search_by_name(query)

        if not resultados:
            console.print(f"[yellow]Nenhum produto encontrado para '{query}'.[/yellow]\n")
            continue

        console.print()
        console.print(_tabela_busca(resultados))
        console.print()

        # --- Fase 2: Seleção ---
        sku_selecionado: str | None = None

        while sku_selecionado is None:
            sel = console.input(
                "[bold cyan]Selecione um produto (#) ou[/bold cyan] [cyan]v[/cyan] [bold cyan]para voltar:[/bold cyan] "
            ).strip()

            if sel.lower() == "q":
                return

            if sel.lower() == "v":
                break

            idx = _selecionar_indice(sel, len(resultados))
            if idx is not None:
                sku_selecionado = resultados[idx - 1].sku
            else:
                console.print("[red]Número inválido.[/red]")

        if sku_selecionado is None:
            console.print()
            continue

        # --- Fase 3: Recomendações em loop ---
        while True:
            produto = engine.get_product(sku_selecionado)
            nome_query = produto.name if produto else sku_selecionado

            recs = engine.recommend(sku_selecionado)

            console.print()
            console.print(_tabela_recomendacoes(recs, nome_query))
            console.print()

            nav = console.input(
                "[bold cyan]Navegar para recomendação (#),[/bold cyan] "
                "[cyan]v[/cyan] [bold cyan]para nova busca ou[/bold cyan] "
                "[cyan]q[/cyan] [bold cyan]para sair:[/bold cyan] "
            ).strip()

            if nav.lower() == "q":
                return

            if nav.lower() == "v":
                console.print()
                break

            idx = _selecionar_indice(nav, len(recs))
            if idx is not None:
                sku_selecionado = recs[idx - 1].sku
            else:
                console.print("[red]Número inválido.[/red]")


if __name__ == "__main__":
    main()
