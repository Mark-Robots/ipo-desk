#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPO Desk - feed generator (storico 5 anni)
Calendario IPO (Finnhub) + performance post-quotazione (yfinance).
Output: docs/data/ipos.json + registro persistente docs/data/registry.json

Env:
  FINNHUB_API_KEY   chiave gratuita finnhub.io (obbligatoria)
  MAX_FULL_FETCH    quante IPO "nuove" calcolare per run (default 250)

Strategia per coprire 5 anni senza massacrare yfinance:
  - checkpoint D1/W1/M1/Y1 calcolati UNA volta e congelati nel registro
    quando l'IPO ha superato 1 anno (non cambiano piu')
  - il run giornaliero ricalcola solo le IPO recenti (checkpoint aperti)
  - il prezzo corrente di TUTTI i titoli viene aggiornato in bulk
    (yf.download a blocchi di 50)
Il backfill iniziale si distribuisce su piu' run (MAX_FULL_FETCH per volta):
con ~1000 IPO bastano 4-5 esecuzioni manuali del workflow.
"""

import json
import math
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

try:
    import yfinance as yf
except ImportError:
    print("yfinance mancante: pip install yfinance", file=sys.stderr)
    sys.exit(1)

# ----------------------------- configurazione ------------------------------

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "docs" / "data"
OUT_FILE = DATA_DIR / "ipos.json"
REG_FILE = DATA_DIR / "registry.json"

FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
FINNHUB_URL = "https://finnhub.io/api/v1/calendar/ipo"

LOOKBACK_DAYS = 1850            # ~5 anni di storico
FORWARD_DAYS = 120              # finestra calendario futuro
MIN_DEAL_VALUE = 30e6           # filtra collocamenti < 30M$ (rumore shelf/SPAC)
MAX_FULL_FETCH = int(os.environ.get("MAX_FULL_FETCH", "250"))
SLEEP_S = 0.35                  # pausa tra fetch storici singoli
BULK_CHUNK = 50                 # ticker per chiamata bulk prezzi correnti
FREEZE_AFTER_SESSIONS = 260     # oltre la 252a seduta i checkpoint sono definitivi

TRADING_OFFSETS = {"d1": 0, "w1": 4, "m1": 20, "y1": 251}

# ------------------------------- utilities ---------------------------------


def log(msg: str) -> None:
    print(f"[ipo-desk] {msg}", flush=True)


def parse_price(raw) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    if not nums:
        return None
    vals = [float(n) for n in nums]
    return round(sum(vals) / len(vals), 4)


def pct(base, value):
    if not base or value is None:
        return None
    return round((value / base - 1.0) * 100.0, 2)


def chunks_dates(start: date, end: date, days: int = 90):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        yield cur, nxt
        cur = nxt + timedelta(days=1)


def fetch_calendar(start: date, end: date) -> list[dict]:
    rows: list[dict] = []
    n_chunks = math.ceil((end - start).days / 90)
    done = 0
    for a, b in chunks_dates(start, end):
        params = {"from": a.isoformat(), "to": b.isoformat(), "token": FINNHUB_KEY}
        for attempt in range(3):
            try:
                r = requests.get(FINNHUB_URL, params=params, timeout=30)
                if r.status_code == 429:
                    time.sleep(15)
                    continue
                r.raise_for_status()
                rows.extend(r.json().get("ipoCalendar", []) or [])
                break
            except Exception as exc:  # noqa: BLE001
                log(f"calendario {a}->{b} tentativo {attempt+1}: {exc}")
                time.sleep(5)
        done += 1
        if done % 5 == 0:
            log(f"calendario: blocco {done}/{n_chunks}")
        time.sleep(1.1)  # free tier 60 call/min
    return rows


# ------------------------------ registro IPO -------------------------------


def load_registry() -> dict:
    if REG_FILE.exists():
        try:
            return json.loads(REG_FILE.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}
    return {}


def update_registry(registry: dict, cal_rows: list[dict], today: date) -> None:
    for row in cal_rows:
        sym = (row.get("symbol") or "").strip().upper()
        d = (row.get("date") or "").strip()
        status = (row.get("status") or "").lower()
        if not sym or not d:
            continue
        if status == "withdrawn":
            registry.pop(sym, None)
            continue
        try:
            ipo_d = date.fromisoformat(d)
        except ValueError:
            continue
        value = float(row.get("totalSharesValue") or 0)
        if value and value < MIN_DEAL_VALUE:
            continue
        if status == "priced" or ipo_d <= today:
            entry = registry.get(sym, {})
            entry.update(
                {
                    "name": row.get("name") or entry.get("name") or sym,
                    "ipo_date": d,
                    "exchange": row.get("exchange") or entry.get("exchange") or "",
                    "deal_value": value or entry.get("deal_value") or 0,
                }
            )
            p = parse_price(row.get("price"))
            if p:
                entry["ipo_price"] = p
            registry[sym] = entry
    cutoff = today - timedelta(days=LOOKBACK_DAYS)
    for sym in list(registry.keys()):
        try:
            if date.fromisoformat(registry[sym]["ipo_date"]) < cutoff:
                del registry[sym]
        except Exception:  # noqa: BLE001
            del registry[sym]


# ----------------------------- performance ---------------------------------


def full_fetch(sym: str, meta: dict) -> dict | None:
    """Storico completo dal giorno di quotazione: calcola tutti i checkpoint."""
    try:
        hist = yf.Ticker(sym).history(
            start=meta["ipo_date"], auto_adjust=False, actions=False
        )
    except Exception as exc:  # noqa: BLE001
        log(f"{sym}: errore yfinance {exc}")
        return None
    time.sleep(SLEEP_S)
    if hist is None or hist.empty:
        return None
    closes = hist["Close"].dropna().tolist()
    opens = hist["Open"].dropna().tolist()
    if not closes:
        return None
    base = meta.get("ipo_price") or (opens[0] if opens else None)
    if not base:
        return None
    perf = {
        "base": round(float(base), 4),
        "price_source": "collocamento" if meta.get("ipo_price") else "open D1",
        "open_d1": round(opens[0], 2) if opens else None,
        "sessions": len(closes),
        "last": round(closes[-1], 2),
        "frozen": len(closes) >= FREEZE_AFTER_SESSIONS,
        "fetched": date.today().isoformat(),
    }
    for key, off in TRADING_OFFSETS.items():
        perf[f"px_{key}"] = round(closes[off], 2) if len(closes) > off else None
    return perf


def bulk_last_prices(symbols: list[str]) -> dict[str, float]:
    """Prezzo corrente di tutti i titoli, a blocchi (1 chiamata ogni 50)."""
    out: dict[str, float] = {}
    for i in range(0, len(symbols), BULK_CHUNK):
        block = symbols[i : i + BULK_CHUNK]
        try:
            df = yf.download(
                tickers=" ".join(block),
                period="5d",
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=True,
            )
        except Exception as exc:  # noqa: BLE001
            log(f"bulk prezzi blocco {i//BULK_CHUNK+1}: {exc}")
            continue
        for sym in block:
            try:
                series = (
                    df[sym]["Close"] if len(block) > 1 else df["Close"]
                ).dropna()
                if len(series):
                    out[sym] = round(float(series.iloc[-1]), 2)
            except Exception:  # noqa: BLE001
                continue
        time.sleep(1.0)
    return out


def compute(registry: dict, today: date) -> list[dict]:
    # 1) chi ha bisogno del fetch completo: mai calcolato, o checkpoint aperti
    need_full, frozen_syms = [], []
    for sym, meta in registry.items():
        try:
            ipo_d = date.fromisoformat(meta["ipo_date"])
        except Exception:  # noqa: BLE001
            continue
        if ipo_d > today:
            continue
        perf = meta.get("perf")
        if perf and perf.get("frozen"):
            frozen_syms.append(sym)
        else:
            need_full.append(sym)

    # piu' recenti prima: il backfill profondo si distribuisce sui run successivi
    need_full.sort(key=lambda s: registry[s]["ipo_date"], reverse=True)
    todo = need_full[:MAX_FULL_FETCH]
    log(f"fetch completi: {len(todo)} (in coda {max(0,len(need_full)-len(todo))}), "
        f"congelate: {len(frozen_syms)}")

    for n, sym in enumerate(todo, 1):
        perf = full_fetch(sym, registry[sym])
        if perf:
            registry[sym]["perf"] = perf
        else:
            registry[sym].setdefault("perf_fail", 0)
            registry[sym]["perf_fail"] += 1
            if registry[sym]["perf_fail"] >= 5:  # delisted / ticker fantasma
                registry[sym]["perf"] = {"dead": True, "frozen": True}
        if n % 25 == 0:
            log(f"fetch completi: {n}/{len(todo)}")

    # 2) prezzo corrente in bulk per le congelate ancora vive
    alive_frozen = [
        s for s in frozen_syms if not registry[s].get("perf", {}).get("dead")
    ]
    if alive_frozen:
        log(f"aggiorno prezzi correnti bulk: {len(alive_frozen)} titoli")
        lasts = bulk_last_prices(alive_frozen)
        for sym, px in lasts.items():
            registry[sym]["perf"]["last"] = px

    # 3) costruisci righe output
    rows: list[dict] = []
    for sym, meta in registry.items():
        perf = meta.get("perf")
        if not perf or perf.get("dead"):
            continue
        base = perf.get("base")
        row = {
            "symbol": sym,
            "name": meta.get("name", sym),
            "exchange": meta.get("exchange", ""),
            "ipo_date": meta["ipo_date"],
            "ipo_price": round(base, 2) if base else None,
            "price_source": perf.get("price_source", ""),
            "deal_value": meta.get("deal_value", 0),
            "open_d1": perf.get("open_d1"),
            "pop_open": pct(base, perf.get("open_d1")),
            "last": perf.get("last"),
            "ret_last": pct(base, perf.get("last")),
            "sessions": perf.get("sessions", 0),
        }
        for key in TRADING_OFFSETS:
            px = perf.get(f"px_{key}")
            row[f"px_{key}"] = px
            row[f"ret_{key}"] = pct(base, px)
        rows.append(row)
    rows.sort(key=lambda r: r["ipo_date"], reverse=True)
    return rows


# --------------------------------- main -------------------------------------


def main() -> int:
    if not FINNHUB_KEY:
        log("FINNHUB_API_KEY assente")
        return 1

    today = date.today()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    registry = load_registry()

    # il calendario storico completo serve solo finche' il registro e' vuoto:
    # a regime basta una finestra corta (-30gg) + futuro
    deep = len(registry) < 50
    start = today - timedelta(days=LOOKBACK_DAYS if deep else 30)
    cal = fetch_calendar(start, today + timedelta(days=FORWARD_DAYS))
    log(f"calendario ({'storico 5 anni' if deep else 'incrementale'}): {len(cal)} righe")

    upcoming = []
    for row in cal:
        d = (row.get("date") or "").strip()
        status = (row.get("status") or "").lower()
        try:
            ipo_d = date.fromisoformat(d)
        except ValueError:
            continue
        if ipo_d < today or status in ("priced", "withdrawn"):
            continue
        value = float(row.get("totalSharesValue") or 0)
        if value and value < MIN_DEAL_VALUE:
            continue
        upcoming.append(
            {
                "symbol": (row.get("symbol") or "").upper(),
                "name": row.get("name") or "",
                "date": d,
                "exchange": row.get("exchange") or "",
                "price_range": row.get("price") or "",
                "shares": row.get("numberOfShares") or 0,
                "deal_value": value,
                "status": status or "expected",
            }
        )
    upcoming.sort(key=lambda r: r["date"])

    update_registry(registry, cal, today)
    listed = compute(registry, today)

    REG_FILE.write_text(
        json.dumps(registry, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )
    payload = {
        "updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params": {"min_deal_value": MIN_DEAL_VALUE, "lookback_days": LOOKBACK_DAYS},
        "backfill_pending": max(
            0,
            sum(
                1
                for s, m in registry.items()
                if not m.get("perf")
                and date.fromisoformat(m["ipo_date"]) <= today
            ),
        ),
        "upcoming": upcoming,
        "listed": listed,
    }
    OUT_FILE.write_text(
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )
    log(
        f"output: {len(upcoming)} in arrivo, {len(listed)} quotate, "
        f"backfill residuo {payload['backfill_pending']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
