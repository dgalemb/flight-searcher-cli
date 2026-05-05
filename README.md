# flight-searcher

A terminal CLI to search Google Flights and rank cheap weekends to travel a route. Built on top of [`fast-flights`](https://github.com/AWeirdDev/flights), with city-name lookup, currency conversion, and Pareto-optimal weekend ranking on top.

## Install

Requires Python ≥3.10 and [`uv`](https://github.com/astral-sh/uv).

```bash
git clone https://github.com/dgalemb/flight-searcher-cli.git
cd flight-searcher-cli
uv tool install --editable .
```

This installs the `flights` command on your PATH. Editable mode means edits to the source are picked up without reinstalling.

To uninstall:

```bash
uv tool uninstall flight-searcher
```

## Commands

The CLI has two subcommands: `search` (one route + date) and `weekends` (rank cheapest weekends in a date range).

### `flights search <ORIGIN> <DESTINATION> <DATE>`

Search a single route on a specific date. Optional return flight for round-trips.

```bash
flights search GRU LHR 30-04-2026
flights search GRU LHR 30-04-2026 --return 07-05-2026
flights search "sao paulo" montevideo 30-04-2026 --currency USD
```

**Options:**

| Flag | Description |
|---|---|
| `--return`, `-r` | Return date (DD-MM-YYYY) — searches both legs as separate one-ways |
| `--seat`, `-s` | `economy` (default), `premium-economy`, `business`, `first` |
| `--adults`, `-a` | Number of adults (default 1) |
| `--children`, `-c` | Number of children (default 0) |
| `--max-stops` | Filter: max number of stops (e.g. `0` for nonstop only) |
| `--max-price`, `-p` | Filter: max price (numeric, in displayed currency) |
| `--currency` | Convert prices to this currency code (e.g. `USD`, `EUR`, `BRL`) |
| `--limit`, `-n` | Max rows to display (default 20) |

### `flights weekends <ORIGIN> <DESTINATION>`

Find and rank the cheapest weekends to travel a route. Generates outbound (Friday evening / Saturday morning) and return (Sunday afternoon / Monday very early) windows for each weekend in range, runs all searches in parallel, and returns Pareto-optimal options sorted by price.

```bash
flights weekends GRU MVD                          # default: next 2 months
flights weekends GRU MVD --months 8               # August only
flights weekends GRU MVD --months 6-7 --max-stops 0 --currency USD
```

**Options:**

| Flag | Description |
|---|---|
| `--months`, `-m` | Single month (`8`) or range (`6-7`). Year auto-inferred. Default: next 2 months. |
| `--seat`, `-s` | Same as `search` |
| `--adults`, `-a` / `--children`, `-c` | Passenger counts |
| `--max-stops` | Filter: max stops per leg |
| `--max-price`, `-p` | Filter: max round-trip total |
| `--currency` | Convert all prices to this currency |
| `--options`, `-o` | Max Pareto-optimal options shown per weekend (default 2) |

**Long weekends:** Brazilian public holidays are auto-detected. If Friday is a holiday, Thursday-evening / all-day-Friday outbound options are added. If Monday is a holiday, all-day-Monday / Tuesday-early return options are added. Long weekends are flagged with ★ in the output.

## City-name lookup

Both commands accept either a 3-letter IATA code (`GRU`) or a city name (`"sao paulo"`). City names with spaces must be quoted.

- **3-letter code** → validated and used directly
- **Unambiguous city** → auto-resolved with a confirmation note
- **Multiple airports within 100 km** → pick-list shown, sorted by distance from the city centroid (so `"sao paulo"` lists GRU, CGH, SAO, then VCP at ~80km, etc.)

Accent-insensitive: `São Paulo`, `sao paulo`, and `Sao Paulo` all match.

## Currency conversion

`--currency USD` (or any code) detects the source currency from Google's price string and converts using:

1. [`fawazahmed0/currency-api`](https://github.com/fawazahmed0/exchange-api) (primary, ~200 currencies including UYU, ARS)
2. [Frankfurter](https://frankfurter.dev) (fallback, ECB-backed, ~30 majors)

If neither source supports the conversion, original prices are shown with a warning.

## Notes & limitations

- **Prices reflect Google's locale** — by default Google returns prices in your detected location's currency. Use `--currency` to convert.
- **Round-trip search runs as two one-ways** — the underlying library has known issues with native round-trip mode, so `--return` runs the inbound as a separate search.
- **Layover details aren't exposed** — for connecting flights, the tool shows total duration and stop count but not the connecting airport.
- **Scraper occasionally returns empty results** — Google Flights sometimes serves a partial "Loading…" page. The tool retries up to 5× with backoff, but long-haul international routes (especially out of UY) can still fail. Workaround: search the legs separately via a known hub like GRU.
