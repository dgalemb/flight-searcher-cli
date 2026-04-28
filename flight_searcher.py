import sys
import io
import re
import threading
import concurrent.futures
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table
from rich import box
from rich.progress import Progress, SpinnerColumn, BarColumn, TaskProgressColumn, TextColumn

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()

PRICE_COLORS = {"low": "green", "typical": "yellow", "high": "red"}


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _parse_price(price_str: str) -> Optional[float]:
    if not price_str:
        return None
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


def _search_one_way(frm, to, date_str, seat, passengers):
    from fast_flights import FlightData
    from fast_flights.filter import TFSData
    from fast_flights.core import get_flights_from_filter

    tfs = TFSData.from_interface(
        flight_data=[FlightData(date=date_str, from_airport=frm, to_airport=to)],
        trip="one-way",
        passengers=passengers,
        seat=seat,
    )

    def _fetch():
        old_stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            return get_flights_from_filter(tfs, currency="", mode="common")
        finally:
            sys.stderr = old_stderr

    for attempt in range(3):
        try:
            result = _fetch()
            if any(f.name for f in result.flights):
                return result
        except Exception as e:
            if attempt == 2:
                raise
    return result


def _parse_date_arg(d: str, label: str) -> str:
    try:
        return datetime.strptime(d, "%d-%m-%Y").strftime("%Y-%m-%d")
    except ValueError:
        console.print(f"[red]Invalid {label}: {d!r} — expected DD-MM-YYYY (e.g. 30-04-2026)[/red]")
        raise typer.Exit(1)


def _print_results(result, frm, to, date_str, seat, pax, max_stops, max_price, limit):
    flights = _dedupe(result.flights or [])
    if not flights:
        console.print("[yellow]No flights found for this route/date.[/yellow]")
        return

    flights.sort(key=lambda f: (not f.is_best, _parse_price(f.price) or 0))

    if max_stops is not None:
        flights = [f for f in flights if _to_int_stops(f.stops) <= max_stops]
    if max_price is not None:
        flights = [f for f in flights if (_parse_price(f.price) or float("inf")) <= max_price]

    if not flights:
        console.print("[yellow]No flights match your filters.[/yellow]")
        return

    displayed = flights[:limit]
    remaining = len(flights) - limit

    display_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")

    table = Table(
        title=(
            f"[bold]{frm} → {to}[/bold]  ·  {display_date}  ·  {seat.title()}  ·  {pax} pax"
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
        table.add_row(name, f.departure, f.arrival, f.duration, _fmt_stops(f.stops), f.price or "—")

    console.print()
    console.print(table)
    if remaining > 0:
        console.print(f"  [dim]+ {remaining} more results — use --limit to show more[/dim]")
    console.print()


# ── Weekend search helpers ─────────────────────────────────────────────────────

def _easter(year: int) -> date:
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def _brazilian_holidays(year: int) -> dict:
    easter = _easter(year)
    carnival = easter - timedelta(days=47)
    corpus = easter + timedelta(days=60)
    return {
        date(year, 1, 1): "New Year's Day",
        carnival - timedelta(1): "Carnival Monday",
        carnival: "Carnival Tuesday",
        easter - timedelta(2): "Good Friday",
        date(year, 4, 21): "Tiradentes",
        date(year, 5, 1): "Labor Day",
        corpus: "Corpus Christi",
        date(year, 9, 7): "Independence Day",
        date(year, 10, 12): "Nossa Senhora Aparecida",
        date(year, 11, 2): "All Souls' Day",
        date(year, 11, 15): "Republic Day",
        date(year, 11, 20): "Black Consciousness Day",
        date(year, 12, 25): "Christmas",
    }


@dataclass
class DateWindow:
    d: date
    min_hour: int
    max_hour: int
    desc: str


@dataclass
class WeekendWindow:
    outbound_windows: List[DateWindow]
    inbound_windows: List[DateWindow]
    is_long: bool = False
    holiday_name: Optional[str] = None

    def date_range(self) -> str:
        start = min(dw.d for dw in self.outbound_windows)
        end = max(dw.d for dw in self.inbound_windows)
        if start.month == end.month:
            return f"{start.strftime('%b %-d')}–{end.strftime('%-d')}"
        return f"{start.strftime('%b %-d')} – {end.strftime('%b %-d')}"

    def label(self) -> str:
        prefix = "★ " if self.is_long else "  "
        s = f"{prefix}{self.date_range()}"
        if self.holiday_name:
            s += f"\n  ({self.holiday_name})"
        return s


@dataclass
class BestFlight:
    d: date
    departure: str
    airline: str
    price_str: str
    price_num: float
    stops: int = 0

    def display(self) -> str:
        m = re.match(r"(\d+:\d+)\s*([AP]M)", self.departure)
        time_str = f"{m.group(1)}{m.group(2)}" if m else "?"
        day = self.d.strftime("%a %-d %b")
        stops_str = "Nonstop" if self.stops == 0 else f"{self.stops} stop{'s' if self.stops > 1 else ''}"
        return f"{day}  {time_str}  {stops_str}\n{self.airline}  {self.price_str}"


@dataclass
class WeekendResult:
    window: WeekendWindow
    best_out: Optional[BestFlight] = None
    best_in: Optional[BestFlight] = None

    @property
    def total(self) -> Optional[float]:
        if self.best_out and self.best_in:
            return self.best_out.price_num + self.best_in.price_num
        return None


def _generate_weekend_windows(start: date, end: date, holidays: dict) -> List[WeekendWindow]:
    windows = []
    # Advance to first Friday
    days_to_fri = (4 - start.weekday()) % 7 or 7
    d = start + timedelta(days=days_to_fri)

    while d <= end:
        fri, sat, sun, mon = d, d + timedelta(1), d + timedelta(2), d + timedelta(3)
        thu, tue = fri - timedelta(1), mon + timedelta(1)

        is_long = False
        holiday_name = None
        outbound: List[DateWindow] = []
        inbound: List[DateWindow] = []

        # Friday holiday → can fly Thu evening or all-day Friday
        if fri in holidays:
            is_long = True
            holiday_name = holidays[fri]
            outbound.append(DateWindow(thu, 17, 23, "Thu evening"))
            outbound.append(DateWindow(fri, 0, 23, f"Fri ({holiday_name})"))

        # Standard outbound
        outbound.append(DateWindow(fri, 17, 23, "Fri evening"))
        outbound.append(DateWindow(sat, 0, 10, "Sat morning"))

        # Standard inbound
        inbound.append(DateWindow(sun, 12, 23, "Sun afternoon"))
        inbound.append(DateWindow(mon, 0, 7, "Mon early"))

        # Monday holiday → can return all-day Monday or Tue early
        if mon in holidays:
            is_long = True
            holiday_name = holiday_name or holidays[mon]
            inbound.append(DateWindow(mon, 0, 23, f"Mon ({holidays[mon]})"))
            inbound.append(DateWindow(tue, 0, 7, "Tue early"))

        windows.append(WeekendWindow(
            outbound_windows=outbound,
            inbound_windows=inbound,
            is_long=is_long,
            holiday_name=holiday_name,
        ))
        d += timedelta(7)

    return windows


def _parse_flight_hour(departure_str: str) -> Optional[int]:
    m = re.match(r"(\d+):(\d+)\s*(AM|PM)", departure_str.strip())
    if not m:
        return None
    h, ampm = int(m.group(1)), m.group(3)
    if ampm == "PM" and h != 12:
        h += 12
    elif ampm == "AM" and h == 12:
        h = 0
    return h


def _find_best_in_window(flights, dw: DateWindow, max_stops: Optional[int] = None) -> Optional[BestFlight]:
    best: Optional[BestFlight] = None
    for f in flights:
        hour = _parse_flight_hour(f.departure)
        if hour is None or not (dw.min_hour <= hour <= dw.max_hour):
            continue
        if max_stops is not None and _to_int_stops(f.stops) > max_stops:
            continue
        price = _parse_price(f.price)
        if price is None:
            continue
        if best is None or price < best.price_num:
            best = BestFlight(d=dw.d, departure=f.departure, airline=f.name,
                              price_str=f.price, price_num=price,
                              stops=_to_int_stops(f.stops))
    return best


# ── Airport resolution ────────────────────────────────────────────────────────

def _airports():
    import airportsdata
    return airportsdata.load("IATA")


def _normalize(s: str) -> str:
    import unicodedata
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().strip()


def _resolve_airport(query: str) -> str:
    """Return a validated IATA code. Accepts a code directly or a city/airport name."""
    db = _airports()

    # Direct IATA code
    if re.match(r"^[A-Za-z]{3}$", query):
        code = query.upper()
        if code in db:
            return code

    # City / name search
    q = _normalize(query)

    # Exact city match first, then partial fallback
    matches = [(c, i) for c, i in db.items() if _normalize(i.get("city", "")) == q]
    if not matches:
        matches = [(c, i) for c, i in db.items()
                   if q in _normalize(i.get("city", "")) or q in _normalize(i.get("name", ""))]

    if not matches:
        console.print(f"[red]No airports found for {query!r}. Try an IATA code (e.g. GRU).[/red]")
        raise typer.Exit(1)

    if len(matches) == 1:
        code, info = matches[0]
        console.print(f"  [dim]→ {code}  {info['name']} ({info['city']}, {info['country']})[/dim]")
        return code

    # Multiple results — show pick-list and exit
    console.print(f"\n[yellow]Multiple airports found for {query!r}:[/yellow]\n")
    for code, info in sorted(matches, key=lambda x: x[0])[:10]:
        subd = f"{info['subd']}, " if info.get("subd") else ""
        console.print(f"  [bold]{code}[/bold]  {info['name']}")
        console.print(f"       {info['city']}, {subd}{info['country']}\n")
    if len(matches) > 10:
        console.print(f"  [dim]… and {len(matches) - 10} more[/dim]\n")
    console.print("[dim]Re-run with the specific code, e.g.:[/dim]")
    code_example = sorted(matches, key=lambda x: x[0])[0][0]
    console.print(f"[dim]  flights search {code_example} ... [/dim]\n")
    raise typer.Exit(0)


# ── Commands ───────────────────────────────────────────────────────────────────

@app.command()
def search(
    origin: str = typer.Argument(..., help="Origin airport IATA code (e.g. GRU)"),
    destination: str = typer.Argument(..., help="Destination airport IATA code (e.g. LHR)"),
    date: str = typer.Argument(..., help="Departure date (DD-MM-YYYY)"),
    return_date: Optional[str] = typer.Option(None, "--return", "-r", help="Return date (DD-MM-YYYY)"),
    seat: str = typer.Option("economy", "--seat", "-s", help="economy|business|first|premium-economy"),
    adults: int = typer.Option(1, "--adults", "-a", help="Number of adults"),
    children: int = typer.Option(0, "--children", "-c", help="Number of children"),
    max_stops: Optional[int] = typer.Option(None, "--max-stops", help="Filter: maximum number of stops"),
    max_price: Optional[float] = typer.Option(None, "--max-price", "-p", help="Filter: maximum price (numeric, in displayed currency)"),
    limit: int = typer.Option(20, "--limit", "-n", help="Max rows to display"),
):
    """Search for flights on a specific date."""
    from fast_flights import Passengers

    origin = _resolve_airport(origin)
    destination = _resolve_airport(destination)
    date = _parse_date_arg(date, "date")
    if return_date:
        return_date = _parse_date_arg(return_date, "return date")

    passengers = Passengers(adults=adults, children=children, infants_in_seat=0, infants_on_lap=0)
    pax = adults + children

    with console.status(f"Searching {origin} → {destination} on {date}…", spinner="dots"):
        try:
            outbound = _search_one_way(origin, destination, date, seat, passengers)
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)

    _print_results(outbound, origin, destination, date, seat, pax, max_stops, max_price, limit)

    if return_date:
        with console.status(f"Searching {destination} → {origin} on {return_date}…", spinner="dots"):
            try:
                inbound = _search_one_way(destination, origin, return_date, seat, passengers)
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                raise typer.Exit(1)

        _print_results(inbound, destination, origin, return_date, seat, pax, max_stops, max_price, limit)


@app.command()
def weekends(
    origin: str = typer.Argument(..., help="Origin airport IATA code (e.g. GRU)"),
    destination: str = typer.Argument(..., help="Destination airport IATA code (e.g. MVD)"),
    months: Optional[str] = typer.Option(None, "--months", "-m", help="Month or range to search, e.g. 8 or 6-7. Defaults to next 2 months."),
    seat: str = typer.Option("economy", "--seat", "-s", help="economy|business|first|premium-economy"),
    adults: int = typer.Option(1, "--adults", "-a", help="Number of adults"),
    children: int = typer.Option(0, "--children", "-c", help="Number of children"),
    max_stops: Optional[int] = typer.Option(None, "--max-stops", help="Filter: maximum number of stops per leg"),
    max_price: Optional[float] = typer.Option(None, "--max-price", "-p", help="Filter: max total round-trip price"),
):
    """Find and rank the cheapest weekends to visit a destination."""
    import calendar
    from fast_flights import Passengers

    origin = _resolve_airport(origin)
    destination = _resolve_airport(destination)

    today = date.today()

    if months is None:
        start = today + timedelta(days=1)
        end = today + timedelta(days=60)
    else:
        parts = months.split("-")
        try:
            fm, tm = (int(parts[0]), int(parts[1])) if len(parts) == 2 else (int(parts[0]), int(parts[0]))
        except ValueError:
            console.print("[red]Invalid --months value. Use a month number (8) or range (6-7).[/red]")
            raise typer.Exit(1)

        if not (1 <= fm <= 12 and 1 <= tm <= 12):
            console.print("[red]Months must be between 1 and 12.[/red]")
            raise typer.Exit(1)

        year = today.year
        if fm < today.month:
            year += 1
        start = date(year, fm, 1)
        if start <= today:
            start = today + timedelta(days=1)

        to_year = year if tm >= fm else year + 1
        end = date(to_year, tm, calendar.monthrange(to_year, tm)[1])

    holidays: dict = {}
    for yr in {start.year, end.year}:
        holidays.update(_brazilian_holidays(yr))

    windows = _generate_weekend_windows(start, end, holidays)
    if not windows:
        console.print("[yellow]No weekends found in the specified range.[/yellow]")
        raise typer.Exit(0)

    # Collect unique (date_str, frm, to) searches
    search_keys: dict = {}
    for w in windows:
        for dw in w.outbound_windows:
            search_keys[(dw.d.strftime("%Y-%m-%d"), origin, destination)] = None
        for dw in w.inbound_windows:
            search_keys[(dw.d.strftime("%Y-%m-%d"), destination, origin)] = None

    passengers = Passengers(adults=adults, children=children, infants_in_seat=0, infants_on_lap=0)
    cache: dict = {}
    sem = threading.Semaphore(3)

    def _run(key):
        date_str, frm, to = key
        with sem:
            try:
                result = _search_one_way(frm, to, date_str, seat, passengers)
            except Exception:
                result = None
        cache[key] = result

    n_searches = len(search_keys)
    console.print(
        f"\nSearching [bold]{n_searches}[/bold] dates across "
        f"[bold]{len(windows)}[/bold] weekends  ({origin} ↔ {destination})\n"
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Fetching flights…", total=n_searches)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(_run, k) for k in search_keys]
            for fut in concurrent.futures.as_completed(futures):
                fut.result()
                progress.advance(task)

    # Score each weekend
    results: List[WeekendResult] = []
    for w in windows:
        best_out: Optional[BestFlight] = None
        for dw in w.outbound_windows:
            r = cache.get((dw.d.strftime("%Y-%m-%d"), origin, destination))
            if not r:
                continue
            bf = _find_best_in_window(_dedupe(r.flights or []), dw, max_stops)
            if bf and (best_out is None or bf.price_num < best_out.price_num):
                best_out = bf

        best_in: Optional[BestFlight] = None
        for dw in w.inbound_windows:
            r = cache.get((dw.d.strftime("%Y-%m-%d"), destination, origin))
            if not r:
                continue
            bf = _find_best_in_window(_dedupe(r.flights or []), dw, max_stops)
            if bf and (best_in is None or bf.price_num < best_in.price_num):
                best_in = bf

        results.append(WeekendResult(window=w, best_out=best_out, best_in=best_in))

    # Filter and sort
    if max_price is not None:
        results = [r for r in results if r.total is not None and r.total <= max_price]

    results.sort(key=lambda r: (r.total is None, r.total or 0))

    if not results or all(r.total is None for r in results):
        console.print("[yellow]No weekend options found. Try a different month range or relax --max-stops / --max-price.[/yellow]")
        raise typer.Exit(0)

    pax = adults + children
    table = Table(
        title=f"[bold]{origin} ↔ {destination}[/bold]  ·  {seat.title()}  ·  {pax} pax  ·  Best weekends",
        box=box.ROUNDED,
        header_style="bold cyan",
        show_lines=True,
    )
    table.add_column("Weekend", no_wrap=True)
    table.add_column(f"Outbound ({origin}→{destination})", style="white")
    table.add_column(f"Return ({destination}→{origin})", style="white")
    table.add_column("Total", justify="right", style="bold green", no_wrap=True)

    has_long = False
    for wr in results:
        if wr.total is None:
            continue
        if wr.window.is_long:
            has_long = True

        # Extract currency symbol for total
        price_str = wr.best_out.price_str if wr.best_out else ""
        currency = re.match(r"([^\d]+)", price_str).group(1) if price_str else ""
        total_str = f"{currency}{wr.total:,.0f}"

        table.add_row(
            wr.window.label(),
            wr.best_out.display() if wr.best_out else "—",
            wr.best_in.display() if wr.best_in else "—",
            total_str,
        )

    console.print()
    console.print(table)
    console.print()
    if has_long:
        console.print("  [dim]★ = long weekend due to Brazilian public holiday[/dim]\n")


if __name__ == "__main__":
    app()
