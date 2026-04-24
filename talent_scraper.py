"""EU+UK / LATAM Talent Scraper — Adzuna-backed pipeline.

Sources:
    mock      — hardcoded dataset, no network
    adzuna    — Adzuna jobs API (https://developer.adzuna.com)

Usage
-----
    python talent_scraper.py --region eu    --source adzuna --output data/eu_data.json
    python talent_scraper.py --region latam --source adzuna --output data/latam_data.json

Env vars (read via os.environ; never hardcoded):
    ADZUNA_APP_ID
    ADZUNA_APP_KEY
    ADZUNA_RESULTS_PER_PAGE   default 50
    ADZUNA_MAX_DAYS_OLD       default 30
    ADZUNA_SLEEP_SECS         delay between calls, default 0.4
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable


# ---------- region config ----------
# Per-company: aliases used for search + employer-name filter, country code for
# Adzuna endpoint, display zone for the dashboard, and tier tag.
# Adzuna country codes supported: at, au, be, br, ca, ch, de, es, fr, gb, in,
# it, mx, nl, nz, pl, sg, us, za.

EU_COMPANIES: dict[str, dict] = {
    # UK · FCA
    "IG Group":          {"aliases": ["IG Group", "IG Markets"],         "country": "gb", "zone": "UK"},
    "CMC Markets":       {"aliases": ["CMC Markets"],                    "country": "gb", "zone": "UK"},
    "Plus500 UK":        {"aliases": ["Plus500"],                        "country": "gb", "zone": "UK"},
    "Pepperstone UK":    {"aliases": ["Pepperstone"],                    "country": "gb", "zone": "UK"},
    "Trading 212":       {"aliases": ["Trading 212", "Trading212"],      "country": "gb", "zone": "UK"},
    "ActivTrades":       {"aliases": ["ActivTrades"],                    "country": "gb", "zone": "UK"},
    "OANDA UK":          {"aliases": ["OANDA"],                          "country": "gb", "zone": "UK"},
    # DE · BaFin
    "XTB Germany":       {"aliases": ["XTB", "X-Trade Brokers"],         "country": "de", "zone": "DE"},
    "JFD Brokers":       {"aliases": ["JFD", "JFD Brokers", "JFD Bank"], "country": "de", "zone": "DE"},
    # PL · KNF (XTB HQ)
    "XTB (PL HQ)":       {"aliases": ["XTB", "X-Trade Brokers"],         "country": "pl", "zone": "PL"},
    # DK ·  — Adzuna has no DK endpoint; Saxo searched via DE / NL (Saxo has
    # licensed entities in both) to still capture EU hiring signal.
    "Saxo Bank":         {"aliases": ["Saxo Bank", "Saxo"],              "country": "nl", "zone": "NL"},
    # CH · FINMA
    "Swissquote":        {"aliases": ["Swissquote"],                     "country": "ch", "zone": "CH"},
    "Dukascopy":         {"aliases": ["Dukascopy"],                      "country": "ch", "zone": "CH"},
}

LATAM_COMPANIES: dict[str, dict] = {
    # BR · CVM — Adzuna has a BR endpoint
    "XP Investimentos":  {"aliases": ["XP Investimentos", "XP Inc"],     "country": "br", "zone": "BR"},
    "Avenue":            {"aliases": ["Avenue Securities", "Avenue"],    "country": "br", "zone": "BR"},
    "Rico":              {"aliases": ["Rico Investimentos", "Rico"],     "country": "br", "zone": "BR"},
    "BTG Pactual":       {"aliases": ["BTG Pactual"],                    "country": "br", "zone": "BR"},
    "Modalmais":         {"aliases": ["Modalmais", "Modal Mais"],        "country": "br", "zone": "BR"},
    "Clear":             {"aliases": ["Clear Corretora", "Clear"],       "country": "br", "zone": "BR"},
    # MX · CNBV
    "GBM":               {"aliases": ["GBM", "Grupo Bursátil Mexicano"], "country": "mx", "zone": "MX"},
    "Actinver":          {"aliases": ["Actinver"],                       "country": "mx", "zone": "MX"},
    # Regional ops with BR/MX presence
    "XTB LATAM":         {"aliases": ["XTB", "X-Trade Brokers"],         "country": "mx", "zone": "MX"},
    "Capex.com":         {"aliases": ["Capex.com", "Capex"],             "country": "br", "zone": "BR"},
}

REGIONS: dict[str, dict] = {
    "eu":    {"companies": EU_COMPANIES,    "label": "EU+UK"},
    "latam": {"companies": LATAM_COMPANIES, "label": "LATAM"},
}


# ---------- classification ----------
# Same 3-bucket taxonomy as the MENA dashboard. First-match-wins. Jobs matching
# nothing are dropped (out of scope).

FUNCTION_RULES: list[tuple[str, list[tuple[str, ...]]]] = [
    ("Compliance / Risk", [
        ("compliance",), ("regulatory",), ("legal counsel",), ("in-house counsel",),
        ("aml",), ("anti-money",), ("transaction monitoring",), ("financial crime",),
        ("kyc",), ("market surveillance",), ("trade surveillance",), ("surveillance",),
        ("credit risk",), ("risk manager",), ("risk analyst",), ("risk officer",),
        ("mlro",), ("fca",), ("bafin",), ("knf",), ("finma",), ("cysec",), ("cvm",),
        ("conduct risk",),
    ]),
    ("Sales / BD", [
        ("institutional sales",), ("b2b sales",), ("prime brokerage",),
        ("family office",), ("business development",), ("sales executive",),
        ("sales manager",), ("account manager",), ("account executive",),
        ("partnerships",), ("introducing broker",), ("ib manager",),
        ("country manager",), ("regional manager",), ("head of sales",),
        ("sales",),
    ]),
    ("Marketing", [
        ("performance marketing",), ("growth marketing",), ("marketing manager",),
        ("content manager",), ("brand manager",), ("seo",), ("paid media",),
        ("marketing",), ("brand",),
    ]),
]


# Currency per Adzuna country endpoint. Adzuna returns salary in the country's
# primary currency.
COUNTRY_CURRENCY: dict[str, str] = {
    "gb": "GBP", "de": "EUR", "fr": "EUR", "nl": "EUR", "es": "EUR", "it": "EUR",
    "pl": "PLN", "ch": "CHF", "at": "EUR", "be": "EUR",
    "br": "BRL", "mx": "MXN",
}

HOT_COUNT_THRESHOLD = 5


# ---------- schema ----------

TAIPEI_TZ = timezone(timedelta(hours=8), name="Asia/Taipei")


@dataclass
class TalentRecord:
    company: str
    zone: str              # UK / DE / PL / CH / NL / BR / MX / ...
    function: str          # Sales / BD | Compliance / Risk | Marketing
    count: int
    hot: bool = False
    salary_min: int | None = None
    salary_max: int | None = None
    salary_currency: str = "USD"
    country: str = ""      # adzuna country code
    source: str = "mock"
    updated_at: str = field(default_factory=lambda: datetime.now(TAIPEI_TZ).isoformat(timespec="seconds"))


# ---------- fetcher interface ----------

class Fetcher:
    name: str = "base"

    def __init__(self, region: str, timeout: float = 20.0) -> None:
        self.region = region
        self.timeout = timeout
        self.log = logging.getLogger(f"talent.fetcher.{self.name}.{region}")

    def fetch(self) -> Iterable[TalentRecord]:  # pragma: no cover
        raise NotImplementedError


class MockFetcher(Fetcher):
    """Tiny hardcoded dataset so the pipeline + HTML render something even
    before Adzuna is wired in."""

    name = "mock"

    _EU_ROWS: list[tuple] = [
        # (company, zone, function, count, hot)
        ("IG Group",        "UK", "Sales / BD",        12, True),
        ("IG Group",        "UK", "Compliance / Risk",  7, True),
        ("IG Group",        "UK", "Marketing",          3, False),
        ("CMC Markets",     "UK", "Compliance / Risk",  4, False),
        ("CMC Markets",     "UK", "Sales / BD",         3, False),
        ("Plus500 UK",      "UK", "Compliance / Risk",  3, False),
        ("Pepperstone UK",  "UK", "Sales / BD",         5, True),
        ("XTB Germany",     "DE", "Sales / BD",         2, False),
        ("XTB (PL HQ)",     "PL", "Compliance / Risk",  3, False),
        ("Saxo Bank",       "NL", "Compliance / Risk",  4, False),
        ("Swissquote",      "CH", "Sales / BD",         2, False),
    ]
    _LATAM_ROWS: list[tuple] = [
        ("XP Investimentos", "BR", "Sales / BD",         6, True),
        ("XP Investimentos", "BR", "Compliance / Risk",  3, False),
        ("BTG Pactual",      "BR", "Compliance / Risk",  4, False),
        ("GBM",              "MX", "Sales / BD",         2, False),
        ("Actinver",         "MX", "Compliance / Risk",  2, False),
    ]

    def fetch(self) -> list[TalentRecord]:
        rows = self._EU_ROWS if self.region == "eu" else self._LATAM_ROWS
        out = []
        for company, zone, function, count, hot in rows:
            out.append(TalentRecord(
                company=company, zone=zone, function=function,
                count=count, hot=hot, source=self.name,
            ))
        return out


class AdzunaFetcher(Fetcher):
    """Adzuna API fetcher.

    One request per (company, primary country). Each request fetches up to
    ADZUNA_RESULTS_PER_PAGE results via what_phrase=<alias>. Returned jobs are
    filtered by company.display_name ~ alias, then classified into the 3
    buckets and aggregated per (company, zone, function).

    API docs: https://developer.adzuna.com/docs/search
    """

    name = "adzuna"
    API_BASE = "https://api.adzuna.com/v1/api/jobs"

    def __init__(self, region: str, timeout: float = 20.0) -> None:
        super().__init__(region=region, timeout=timeout)
        self.app_id = os.environ.get("ADZUNA_APP_ID")
        self.app_key = os.environ.get("ADZUNA_APP_KEY")
        self.results_per_page = int(os.environ.get("ADZUNA_RESULTS_PER_PAGE", "50"))
        self.max_days_old = int(os.environ.get("ADZUNA_MAX_DAYS_OLD", "30"))
        self.sleep_between = float(os.environ.get("ADZUNA_SLEEP_SECS", "0.4"))
        if region not in REGIONS:
            raise ValueError(f"unknown region {region!r}. available: {sorted(REGIONS)}")
        self.companies = REGIONS[region]["companies"]

    def fetch(self) -> list[TalentRecord]:
        if not self.app_id or not self.app_key:
            raise RuntimeError(
                "ADZUNA_APP_ID and ADZUNA_APP_KEY must be set. Get them free at "
                "https://developer.adzuna.com/signup"
            )

        buckets: dict[tuple[str, str, str], dict] = defaultdict(
            lambda: {"count": 0, "salary_mins": [], "salary_maxs": [], "currency": "USD", "country": ""}
        )

        for display_name, cfg in self.companies.items():
            aliases = cfg["aliases"]
            country = cfg["country"]
            default_zone = cfg["zone"]
            currency = COUNTRY_CURRENCY.get(country, "USD")

            raw_jobs: list[dict] = []
            # Only first alias is used for search — saves quota. Remaining
            # aliases still used for employer-name matching on the response.
            primary_alias = aliases[0]
            try:
                hits = self._search(country, primary_alias)
            except Exception:
                self.log.exception("company=%s country=%s failed — skipping", display_name, country)
                continue
            raw_jobs.extend(hits)
            time.sleep(self.sleep_between)

            matched = self._filter_by_employer(raw_jobs, aliases)
            self.log.info(
                "company=%s country=%s raw=%d matched=%d",
                display_name, country, len(raw_jobs), len(matched),
            )

            for job in matched:
                function = self._classify_function(job)
                if not function:
                    continue
                bucket = buckets[(display_name, default_zone, function)]
                bucket["count"] += 1
                bucket["currency"] = currency
                bucket["country"] = country
                smin, smax = self._extract_salary(job)
                if smin is not None:
                    bucket["salary_mins"].append(smin)
                if smax is not None:
                    bucket["salary_maxs"].append(smax)

        records: list[TalentRecord] = []
        for (company, zone, function), bucket in buckets.items():
            smin = min(bucket["salary_mins"]) if bucket["salary_mins"] else None
            smax = max(bucket["salary_maxs"]) if bucket["salary_maxs"] else None
            records.append(TalentRecord(
                company=company, zone=zone, function=function,
                count=bucket["count"],
                hot=bucket["count"] >= HOT_COUNT_THRESHOLD,
                salary_min=smin, salary_max=smax, salary_currency=bucket["currency"],
                country=bucket["country"],
                source=self.name,
            ))
        records.sort(key=lambda r: (r.zone, r.company, -r.count))
        return records

    # --- internals ---

    def _search(self, country: str, alias: str) -> list[dict]:
        params = {
            "app_id":            self.app_id,
            "app_key":           self.app_key,
            "results_per_page":  str(self.results_per_page),
            "what_phrase":       alias,
            "max_days_old":      str(self.max_days_old),
            "content-type":      "application/json",
        }
        url = f"{self.API_BASE}/{country}/search/1?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={
            "User-Agent": "eu-latam-talent-scraper/1.0",
            "Accept":     "application/json",
        })
        self.log.debug("GET %s", url.replace(self.app_key or "", "***"))
        payload = self._request_with_retry(req)
        return payload.get("results") or []

    def _request_with_retry(self, req: urllib.request.Request, max_attempts: int = 3) -> dict:
        last_err: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                if e.code == 429 or 500 <= e.code < 600:
                    wait = min(2 ** attempt, 30)
                    self.log.warning("HTTP %d on attempt %d — sleeping %ds", e.code, attempt, wait)
                    time.sleep(wait)
                    last_err = e
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                wait = min(2 ** attempt, 30)
                self.log.warning("network error %r on attempt %d — sleeping %ds", e, attempt, wait)
                time.sleep(wait)
                last_err = e
        raise last_err or RuntimeError("request_with_retry: unreachable")

    @staticmethod
    def _filter_by_employer(jobs: list[dict], aliases: list[str]) -> list[dict]:
        lowered = [a.lower() for a in aliases]
        seen: set[str] = set()
        out: list[dict] = []
        for job in jobs:
            emp = ((job.get("company") or {}).get("display_name") or "").lower()
            if not emp:
                continue
            # match if any alias is a substring of the employer name or vice versa
            if not any(a in emp or emp in a for a in lowered):
                continue
            jid = job.get("id") or job.get("redirect_url") or f"{emp}|{job.get('title')}"
            if jid in seen:
                continue
            seen.add(jid)
            out.append(job)
        return out

    @staticmethod
    def _classify_function(job: dict) -> str | None:
        haystack = (
            (job.get("title") or "") + " \n " + (job.get("description") or "")
        ).lower()
        for label, patterns in FUNCTION_RULES:
            for terms in patterns:
                if all(t.lower() in haystack for t in terms):
                    return label
        return None

    @staticmethod
    def _extract_salary(job: dict) -> tuple[int | None, int | None]:
        # Skip Adzuna's predicted salaries — they're estimates, not posted values.
        if str(job.get("salary_is_predicted") or "0") == "1":
            return None, None
        smin = job.get("salary_min")
        smax = job.get("salary_max")
        smin_i = int(smin) if isinstance(smin, (int, float)) else None
        smax_i = int(smax) if isinstance(smax, (int, float)) else None
        return smin_i, smax_i


FETCHERS: dict[str, Callable[[str, float], Fetcher]] = {
    "mock":   MockFetcher,
    "adzuna": AdzunaFetcher,
}


# ---------- pipeline ----------

def build_payload(records: Iterable[TalentRecord], source: str, region: str) -> dict:
    records = list(records)
    return {
        "generated_at": datetime.now(TAIPEI_TZ).isoformat(timespec="seconds"),
        "region": region,
        "region_label": REGIONS[region]["label"],
        "source": source,
        "record_count": len(records),
        "records": [asdict(r) for r in records],
    }


def write_json(payload: dict, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(output.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(output)


def run(source: str, region: str, output: Path, timeout: float) -> int:
    log = logging.getLogger("talent.pipeline")
    factory = FETCHERS.get(source)
    if factory is None:
        log.error("unknown source %r. available: %s", source, ", ".join(sorted(FETCHERS)))
        return 2

    fetcher = factory(region=region, timeout=timeout)
    log.info("running source=%s region=%s timeout=%.1fs", source, region, timeout)

    try:
        records = list(fetcher.fetch())
    except Exception:
        log.exception("fetch failed")
        return 1

    if not records:
        log.warning("fetcher returned 0 records — writing empty payload anyway")

    payload = build_payload(records, source=source, region=region)
    try:
        write_json(payload, output)
    except OSError:
        log.exception("failed to write %s", output)
        return 1

    log.info("wrote %d records -> %s", len(records), output)
    return 0


# ---------- cli ----------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="EU+UK / LATAM talent pipeline (Adzuna-backed).")
    p.add_argument("--source",  default="mock",  choices=sorted(FETCHERS))
    p.add_argument("--region",  default="eu",    choices=sorted(REGIONS))
    p.add_argument("--output",  default=None,
                   help="output json path. default: data/<region>_data.json")
    p.add_argument("--timeout", type=float, default=25.0)
    p.add_argument("--verbose", "-v", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    output_path = args.output or f"data/{args.region}_data.json"
    output = Path(output_path).resolve()
    return run(source=args.source, region=args.region, output=output, timeout=args.timeout)


if __name__ == "__main__":
    sys.exit(main())
