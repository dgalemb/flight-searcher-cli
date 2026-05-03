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

CURRENCY_SYMBOLS = {
    "USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥",
    "BRL": "R$", "ARS": "AR$", "UYU": "$U", "CAD": "CA$",
    "AUD": "A$", "CHF": "CHF ", "CNY": "¥", "MXN": "MX$",
}

_fx_cache: dict = {}


def _detect_currency(price_str: str) -> Optional[str]:
    if not price_str:
        return None
    s = price_str.strip()
    # Order matters — check multi-char prefixes first
    for prefix, code in [("R$", "BRL"), ("AR$", "ARS"), ("CA$", "CAD"),
                         ("A$", "AUD"), ("$U", "UYU"), ("MX$", "MXN"),
                         ("US$", "USD")]:
        if s.startswith(prefix):
            return code
    if s.startswith("$"): return "USD"
    if s.startswith("€"): return "EUR"
    if s.startswith("£"): return "GBP"
    if s.startswith("¥"): return "JPY"
    m = re.match(r"^([A-Z]{3})", s)
    return m.group(1) if m else None


def _get_fx_rate(from_cur: str, to_cur: str) -> Optional[float]:
    if from_cur == to_cur:
        return 1.0
    key = (from_cur, to_cur)
    if key in _fx_cache:
        return _fx_cache[key]

    import urllib.request, json
    rate: Optional[float] = None

    # Primary: fawazahmed0/currency-api (no key, supports ~200 currencies including UYU)
    try:
        url = (
            f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest"
            f"/v1/currencies/{from_cur.lower()}.json"
        )
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read())
            rate = data.get(from_cur.lower(), {}).get(to_cur.lower())
    except Exception:
        pass

    # Fallback: Frankfurter (ECB-backed, ~30 majors)
    if rate is None:
        try:
            url = f"https://api.frankfurter.dev/v1/latest?base={from_cur}&symbols={to_cur}"
            req = urllib.request.Request(url, headers={"User-Agent": "flight-searcher/0.1"})
            with urllib.request.urlopen(req, timeout=5) as r:
                rate = json.loads(r.read())["rates"].get(to_cur)
        except Exception:
            rate = None

    _fx_cache[key] = rate
    return rate


def _format_price(price_str: str, target: Optional[str], rate: Optional[float]) -> str:
    if not price_str:
        return "—"
    if not target or not rate:
        return price_str
    num = _parse_price(price_str)
    if num is None:
        return price_str
    converted = num * rate
    symbol = CURRENCY_SYMBOLS.get(target, f"{target} ")
    return f"{symbol}{converted:,.0f}"


def _resolve_fx(flights, target_currency: Optional[str]) -> tuple:
    """Detect source currency from first priced flight; return (target, rate) or (None, None)."""
    if not target_currency:
        return None, None
    target = target_currency.upper()
    src = next((_detect_currency(f.price) for f in flights if _detect_currency(f.price)), None)
    if not src:
        return None, None
    rate = _get_fx_rate(src, target)
    if rate is None:
        console.print(f"[yellow]Warning: could not fetch {src}→{target} rate; showing original prices.[/yellow]")
        return None, None
    return target, rate


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

    import time
    last_exc = None
    result = None
    for attempt in range(5):
        try:
            result = _fetch()
            if result and any(f.name for f in result.flights):
                return result
        except Exception as e:
            last_exc = e
        if attempt < 4:
            time.sleep(0.5 * (attempt + 1))
    if result is None and last_exc is not None:
        raise last_exc
    return result


def _parse_date_arg(d: str, label: str) -> str:
    try:
        return datetime.strptime(d, "%d-%m-%Y").strftime("%Y-%m-%d")
    except ValueError:
        console.print(f"[red]Invalid {label}: {d!r} — expected DD-MM-YYYY (e.g. 30-04-2026)[/red]")
        raise typer.Exit(1)


def _print_results(result, frm, to, date_str, seat, pax, max_stops, max_price, limit, target_currency=None):
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
    target, rate = _resolve_fx(flights, target_currency)

    title = (
        f"[bold]{frm} → {to}[/bold]  ·  {display_date}  ·  {seat.title()}  ·  {pax} pax"
        f"  ·  {_price_sentiment(result.current_price)}"
    )
    if target and rate:
        title += f"  ·  [dim]prices in {target}[/dim]"

    table = Table(title=title, box=box.ROUNDED, header_style="bold cyan", show_lines=False)
    table.add_column("Airline")
    table.add_column("Departs", style="bold white")
    table.add_column("Arrives", style="white")
    table.add_column("Duration", style="dim")
    table.add_column("Stops", justify="center")
    table.add_column("Price", justify="right", style="bold green")

    for f in displayed:
        name = f"[bold]{f.name}[/bold]" if f.is_best else f.name
        table.add_row(name, f.departure, f.arrival, f.duration, _fmt_stops(f.stops),
                      _format_price(f.price, target, rate))

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

    def display(self, target: Optional[str] = None, rate: Optional[float] = None) -> str:
        m = re.match(r"(\d+:\d+)\s*([AP]M)", self.departure)
        time_str = f"{m.group(1)}{m.group(2)}" if m else "?"
        day = self.d.strftime("%a %-d %b")
        stops_str = "Nonstop" if self.stops == 0 else f"{self.stops} stop{'s' if self.stops > 1 else ''}"
        price = _format_price(self.price_str, target, rate)
        return f"{day}  {time_str}  {stops_str}\n{self.airline}  {price}"


@dataclass
class TripOption:
    out: BestFlight
    inb: BestFlight
    total: float
    duration_hours: float


@dataclass
class WeekendResult:
    window: WeekendWindow
    options: List[TripOption]

    @property
    def total(self) -> Optional[float]:
        return self.options[0].total if self.options else None


def _trip_duration_hours(out: BestFlight, inb: BestFlight) -> float:
    """Approx time at destination, using outbound dep → inbound dep."""
    from datetime import time as _time
    out_h = _parse_flight_hour(out.departure) or 0
    in_h = _parse_flight_hour(inb.departure) or 0
    out_dt = datetime.combine(out.d, _time(out_h))
    in_dt = datetime.combine(inb.d, _time(in_h))
    return (in_dt - out_dt).total_seconds() / 3600


def _fmt_duration_h(hours: float) -> str:
    days = int(hours // 24)
    h = int(hours % 24)
    return f"{days}d {h}h" if days > 0 else f"{h}h"


def _pareto_options(window: WeekendWindow, cache: dict, origin: str, destination: str,
                    max_stops: Optional[int]) -> List[TripOption]:
    """Find Pareto-optimal (price, duration) trip combinations for a weekend."""
    def best_per_window(windows, frm, to):
        out = []
        for dw in windows:
            r = cache.get((dw.d.strftime("%Y-%m-%d"), frm, to))
            if not r:
                continue
            bf = _find_best_in_window(_dedupe(r.flights or []), dw, max_stops)
            if bf:
                out.append(bf)
        # Dedupe — multiple windows may pick the same flight
        seen = set()
        deduped = []
        for f in out:
            key = (f.d, f.departure, f.airline, f.price_str)
            if key not in seen:
                seen.add(key)
                deduped.append(f)
        return deduped

    outbounds = best_per_window(window.outbound_windows, origin, destination)
    inbounds = best_per_window(window.inbound_windows, destination, origin)

    options: List[TripOption] = []
    for o in outbounds:
        for i in inbounds:
            if i.d < o.d:
                continue
            dur = _trip_duration_hours(o, i)
            if dur <= 0:
                continue
            options.append(TripOption(out=o, inb=i, total=o.price_num + i.price_num, duration_hours=dur))

    # Pareto: keep options not dominated by any other (cheaper-or-equal AND longer-or-equal, strictly better in one)
    pareto: List[TripOption] = []
    for opt in options:
        dominated = any(
            other is not opt
            and other.total <= opt.total
            and other.duration_hours >= opt.duration_hours
            and (other.total < opt.total or other.duration_hours > opt.duration_hours)
            for other in options
        )
        if not dominated:
            pareto.append(opt)

    pareto.sort(key=lambda o: o.total)
    return pareto


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


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    from math import radians, sin, cos, sqrt, atan2
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


PROXIMITY_RADIUS_KM = 100


def _resolve_airport(query: str) -> str:
    """Return a validated IATA code. Accepts a code directly or a city/airport name."""
    db = _airports()

    # Direct IATA code — fast path
    if re.match(r"^[A-Za-z]{3}$", query):
        code = query.upper()
        if code in db:
            return code

    q = _normalize(query)

    # Exact city name match → use their centroid for proximity search
    exact = [(c, i) for c, i in db.items() if _normalize(i.get("city", "")) == q]

    if exact:
        ref_lat = sum(i["lat"] for _, i in exact) / len(exact)
        ref_lon = sum(i["lon"] for _, i in exact) / len(exact)
        matches = sorted(
            [(c, i, _haversine_km(ref_lat, ref_lon, i["lat"], i["lon"]))
             for c, i in db.items() if i.get("lat") and i.get("lon")],
            key=lambda x: x[2],
        )
        matches = [(c, i, d) for c, i, d in matches if d <= PROXIMITY_RADIUS_KM]
    else:
        # Partial string fallback (no geo)
        partial = [(c, i) for c, i in db.items()
                   if q in _normalize(i.get("city", "")) or q in _normalize(i.get("name", ""))]
        if not partial:
            console.print(f"[red]No airports found for {query!r}. Try an IATA code (e.g. GRU).[/red]")
            raise typer.Exit(1)
        matches = [(c, i, 0.0) for c, i in sorted(partial, key=lambda x: x[0])]

    if not matches:
        console.print(f"[red]No airports found for {query!r}. Try an IATA code (e.g. GRU).[/red]")
        raise typer.Exit(1)

    if len(matches) == 1:
        code, info, dist = matches[0]
        console.print(f"  [dim]→ {code}  {info['name']} ({info['city']}, {info['country']})[/dim]")
        return code

    # Multiple results — show pick-list sorted by distance and exit
    console.print(f"\n[yellow]Airports within {PROXIMITY_RADIUS_KM} km of {query!r}:[/yellow]\n")
    for code, info, dist in matches[:10]:
        dist_str = f"  {dist:.0f} km" if dist > 0 else ""
        subd = f"{info['subd']}, " if info.get("subd") else ""
        console.print(f"  [bold]{code}[/bold]  {info['name']}[dim]{dist_str}[/dim]")
        console.print(f"       {info['city']}, {subd}{info['country']}\n")
    if len(matches) > 10:
        console.print(f"  [dim]… and {len(matches) - 10} more[/dim]\n")
    console.print("[dim]Re-run with the specific airport code, e.g.:[/dim]")
    console.print(f"[dim]  flights search {matches[0][0]} ... [/dim]\n")
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
    currency: Optional[str] = typer.Option(None, "--currency", help="Convert prices to this currency (e.g. USD, EUR)"),
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

    _print_results(outbound, origin, destination, date, seat, pax, max_stops, max_price, limit, currency)

    if return_date:
        with console.status(f"Searching {destination} → {origin} on {return_date}…", spinner="dots"):
            try:
                inbound = _search_one_way(destination, origin, return_date, seat, passengers)
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                raise typer.Exit(1)

        _print_results(inbound, destination, origin, return_date, seat, pax, max_stops, max_price, limit, currency)


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
    currency: Optional[str] = typer.Option(None, "--currency", help="Convert prices to this currency (e.g. USD, EUR)"),
    options: int = typer.Option(2, "--options", "-o", help="Max Pareto-optimal options to show per weekend (cheapest + more time at destination)"),
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
        opts = _pareto_options(w, cache, origin, destination, max_stops)
        results.append(WeekendResult(window=w, options=opts))

    # Filter by max total price (applied to each option)
    if max_price is not None:
        for r in results:
            r.options = [o for o in r.options if o.total <= max_price]

    results = [r for r in results if r.options]
    results.sort(key=lambda r: r.total or 0)

    if not results:
        console.print("[yellow]No weekend options found. Try a different month range or relax --max-stops / --max-price.[/yellow]")
        raise typer.Exit(0)

    # Resolve FX once across all priced flights
    sample_flights = [bf for wr in results for opt in wr.options for bf in (opt.out, opt.inb)]
    target, rate = _resolve_fx(
        [type("F", (), {"price": bf.price_str})() for bf in sample_flights],
        currency,
    )

    pax = adults + children
    title = f"[bold]{origin} ↔ {destination}[/bold]  ·  {seat.title()}  ·  {pax} pax  ·  Best weekends"
    if target and rate:
        title += f"  ·  [dim]prices in {target}[/dim]"

    table = Table(title=title, box=box.ROUNDED, header_style="bold cyan", show_lines=False)
    table.add_column("Weekend", no_wrap=True)
    table.add_column(f"Outbound ({origin}→{destination})", style="white")
    table.add_column(f"Return ({destination}→{origin})", style="white")
    table.add_column("At dest", justify="right", style="dim", no_wrap=True)
    table.add_column("Total", justify="right", style="bold green", no_wrap=True)

    has_long = False
    for wr_idx, wr in enumerate(results):
        if wr.window.is_long:
            has_long = True

        if wr_idx > 0:
            table.add_section()

        for opt_idx, opt in enumerate(wr.options[:options]):
            if target and rate:
                symbol = CURRENCY_SYMBOLS.get(target, f"{target} ")
                total_str = f"{symbol}{opt.total * rate:,.0f}"
            else:
                price_str = opt.out.price_str
                symbol = re.match(r"([^\d]+)", price_str).group(1) if price_str else ""
                total_str = f"{symbol}{opt.total:,.0f}"

            label = wr.window.label() if opt_idx == 0 else ""
            table.add_row(
                label,
                opt.out.display(target, rate),
                opt.inb.display(target, rate),
                _fmt_duration_h(opt.duration_hours),
                total_str,
            )

    console.print()
    console.print(table)
    console.print()
    if has_long:
        console.print("  [dim]★ = long weekend due to Brazilian public holiday[/dim]\n")


if __name__ == "__main__":
    app()
