import typer
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich import box

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()

PRICE_COLORS = {"low": "green", "typical": "yellow", "high": "red"}


def _parse_price(price_str: str) -> Optional[float]:
    """Parse '$1,234' or 'R$2,224' → numeric value for filtering."""
    if not price_str:
        return None
    import re
    m = re.search(r"[\d,]+(?:\.\d+)?", price_str.replace(",", ""))
    try:
        return float(m.group()) if m else None
    except (ValueError, AttributeError):
        return None


def _to_int_stops(stops) -> int:
    try:
        return int(stops)
    except (ValueError, TypeError):
        return 0


def _fmt_stops(stops) -> str:
    n = _to_int_stops(stops)
    if n == 0:
        return "[green]Nonstop[/green]"
    return f"[yellow]{n} stop{'s' if n > 1 else ''}[/yellow]"


def _price_sentiment(current_price: str) -> str:
    color = PRICE_COLORS.get(current_price, "white")
    return f"[{color}]{current_price.title()} prices[/{color}]"


def _dedupe(flights) -> list:
    seen = set()
    out = []
    for f in flights:
        if not f.name:
            continue
        key = (f.name, f.departure, f.arrival, f.price)
        if key not in seen:
            seen.add(key)
            out.append(f)
    return out


@app.command()
def search(
    origin: str = typer.Argument(..., help="Origin airport IATA code (e.g. JFK)"),
    destination: str = typer.Argument(..., help="Destination airport IATA code (e.g. LHR)"),
    date: str = typer.Argument(..., help="Departure date (DD-MM-YYYY)"),
    return_date: Optional[str] = typer.Option(None, "--return", "-r", help="Return date for round trips (DD-MM-YYYY)"),
    seat: str = typer.Option("economy", "--seat", "-s", help="economy|business|first|premium-economy"),
    adults: int = typer.Option(1, "--adults", "-a", help="Number of adults"),
    children: int = typer.Option(0, "--children", "-c", help="Number of children"),
    max_stops: Optional[int] = typer.Option(None, "--max-stops", help="Filter: maximum number of stops"),
    max_price: Optional[float] = typer.Option(None, "--max-price", "-p", help="Filter: maximum price (numeric, in displayed currency)"),
    limit: int = typer.Option(20, "--limit", "-n", help="Max rows to display"),
):
    """Search for flights and display prices."""
    from fast_flights import FlightData, Passengers
    from fast_flights.filter import TFSData
    from fast_flights.core import get_flights_from_filter
    from datetime import datetime

    origin = origin.upper()
    destination = destination.upper()

    def _parse_date(d: str, label: str) -> str:
        try:
            return datetime.strptime(d, "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            console.print(f"[red]Invalid {label}: {d!r} — expected DD-MM-YYYY (e.g. 30-04-2026)[/red]")
            raise typer.Exit(1)

    date = _parse_date(date, "date")
    if return_date:
        return_date = _parse_date(return_date, "return date")

    trip = "round-trip" if return_date else "one-way"
    flight_data = [FlightData(date=date, from_airport=origin, to_airport=destination)]
    if return_date:
        flight_data.append(FlightData(date=return_date, from_airport=destination, to_airport=origin))

    passengers = Passengers(adults=adults, children=children, infants_in_seat=0, infants_on_lap=0)

    tfs = TFSData.from_interface(
        flight_data=flight_data,
        trip=trip,
        passengers=passengers,
        seat=seat,
    )

    import sys, io

    def _fetch():
        old_stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            return get_flights_from_filter(tfs, currency="", mode="common")
        finally:
            sys.stderr = old_stderr

    result = None
    with console.status(f"Searching {origin} → {destination} on {date}…", spinner="dots"):
        for attempt in range(3):
            try:
                result = _fetch()
                if any(f.name for f in result.flights):
                    break
            except Exception as e:
                if attempt == 2:
                    console.print(f"[red]Error: {e}[/red]")
                    raise typer.Exit(1)

    if result is None:
        console.print("[red]No results returned.[/red]")
        raise typer.Exit(1)

    flights = _dedupe(result.flights or [])

    if not flights:
        console.print("[yellow]No flights found for this route/date.[/yellow]")
        raise typer.Exit(0)

    # Best flights first, then sort by price
    flights.sort(key=lambda f: (not f.is_best, _parse_price(f.price) or 0))

    if max_stops is not None:
        flights = [f for f in flights if _to_int_stops(f.stops) <= max_stops]
    if max_price is not None:
        flights = [f for f in flights if (_parse_price(f.price) or float("inf")) <= max_price]

    if not flights:
        console.print("[yellow]No flights match your filters.[/yellow]")
        raise typer.Exit(0)

    displayed = flights[:limit]
    remaining = len(flights) - limit

    pax = adults + children
    table = Table(
        title=(
            f"[bold]{origin} → {destination}[/bold]  ·  {date}  ·  {seat.title()}  ·  {pax} pax"
            f"  ·  {_price_sentiment(result.current_price)}"
        ),
        box=box.ROUNDED,
        header_style="bold cyan",
        show_lines=False,
    )
    table.add_column("Airline")
    table.add_column("Departs", style="bold white")
    table.add_column("Arrives", style="white")
    table.add_column("Duration", style="dim")
    table.add_column("Stops", justify="center")
    table.add_column("Price", justify="right", style="bold green")

    for f in displayed:
        name = f"[bold]{f.name}[/bold]" if f.is_best else f.name
        table.add_row(
            name,
            f.departure,
            f.arrival,
            f.duration,
            _fmt_stops(f.stops),
            f.price or "—",
        )

    console.print()
    console.print(table)
    if remaining > 0:
        console.print(f"  [dim]+ {remaining} more results — use --limit to show more[/dim]")
    console.print()


if __name__ == "__main__":
    app()
