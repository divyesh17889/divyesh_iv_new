import os
import time
import logging
from datetime import datetime, timezone
from urllib.parse import quote_plus
from dotenv import load_dotenv
from flask import Flask, render_template, jsonify, request
import requests

# WebSocket
from flask_socketio import SocketIO, emit, join_room, leave_room


# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
PORT = int(os.getenv("PORT", "8000"))

BASE = "https://www.nseindia.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/option-chain",
    "Connection": "keep-alive",
}

INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}

s = requests.Session()
s.headers.update(HEADERS)


def safe_float(val):
    """Safely convert to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def prime():
    try:
        s.get(BASE, timeout=10)
        s.get(BASE + "/option-chain", timeout=10)
        logger.info("Session primed successfully")
    except requests.RequestException as e:
        logger.warning(f"Session priming failed: {e}")


def get_json(url, retries=3):
    for _ in range(retries):
        try:
            r = s.get(url, timeout=12)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (401, 403):
                prime()
            time.sleep(0.5)
        except requests.RequestException as e:
            logger.warning(f"Request exception for {url}: {e}")
            prime()
            time.sleep(0.5)
    logger.error(f"NSE fetch failed for URL: {url}")
    raise RuntimeError("NSE fetch failed")


def oc_url(symbol):
    symbol = symbol.upper().strip()
    if symbol in INDEX_SYMBOLS:
        return f"{BASE}/api/option-chain-indices?symbol={quote_plus(symbol)}"
    return f"{BASE}/api/option-chain-equities?symbol={quote_plus(symbol)}"


def parse_rows(oc_json, chosen_expiry=None):
    if not oc_json or "records" not in oc_json:
        logger.warning("Invalid OC JSON structure")
        return {"rows": [], "expiries": [], "underlying": None, "symbol_name": None}
    rec = oc_json["records"]
    expiries = rec.get("expiryDates", []) or []
    data = rec.get("data", []) or []
    symbol_name = rec.get("index") or rec.get("underlying")
    underlying = safe_float(rec.get("underlyingValue"))
    rows = []
    for item in data:
        strike = safe_float(item.get("strikePrice"))
        expiry = item.get("expiryDate")
        if chosen_expiry and expiry != chosen_expiry:
            continue
        ce = item.get("CE") or {}
        pe = item.get("PE") or {}
        rows.append({
            "symbol": symbol_name,
            "underlying": underlying,
            "expiry": expiry,
            "strike": strike,
            "CE_iv": safe_float(ce.get("impliedVolatility")),
            "PE_iv": safe_float(pe.get("impliedVolatility")),
            "CE_ltp": safe_float(ce.get("lastPrice")),
            "PE_ltp": safe_float(pe.get("lastPrice")),
            "CE_vol": safe_float(ce.get("totalTradedVolume") or ce.get("lastTradedVolume") or ce.get("totalTradedQty")),
            "PE_vol": safe_float(pe.get("totalTradedVolume") or pe.get("lastTradedVolume") or pe.get("totalTradedQty")),
        })
    rows = [r for r in rows if r["strike"] is not None]
    rows.sort(key=lambda r: r["strike"])
    return {"rows": rows, "expiries": expiries, "underlying": underlying, "symbol_name": symbol_name}


def nearest_strike_iv(rows, under, side):
    best, bestd = None, float('inf')
    for r in rows:
        iv = r.get(f"{side}_iv")
        if iv is None:
            continue
        strike = r["strike"]
        if strike is None:
            continue
        d = abs(strike - under)
        if d < bestd:
            best, bestd = r, d
    if not best:
        return None, None
    return best["strike"], best.get(f"{side}_iv")


def get_straddle_info(rows, underlying):
    if not rows or underlying is None:
        return None, None, None, None

    atm_strike = None
    for r in rows:
        if r["strike"] and abs(r["strike"] - underlying) < 0.1:
            atm_strike = r["strike"]
            break
    if not atm_strike:
        atm_strike = min(rows, key=lambda x: abs(x["strike"] - underlying))["strike"]

    ce_row = None
    pe_row = None
    for r in rows:
        if r["strike"] == atm_strike:
            if r.get("CE_ltp"): ce_row = r
            if r.get("PE_ltp"): pe_row = r

    if not ce_row or not pe_row:
        return None, None, None, None

    ce_ltp = safe_float(ce_row["CE_ltp"])
    pe_ltp = safe_float(pe_row["PE_ltp"])
    straddle_price = round(ce_ltp + pe_ltp, 2)
    straddle_iv = round((safe_float(ce_row["CE_iv"]) + safe_float(pe_row["PE_iv"])) / 2, 2)

    return straddle_price, straddle_iv, atm_strike, f"{atm_strike} CE + {atm_strike} PE"


def build_strategy(ce_hits, pe_hits):
    if ce_hits and not pe_hits:
        return {"bias": "Bullish", "suggestion": "Call Buy / Bull Call Spread", "strength": ce_hits[0]["inc"]}
    if pe_hits and not ce_hits:
        return {"bias": "Bearish", "suggestion": "Put Buy / Bear Put Spread", "strength": pe_hits[0]["inc"]}
    if ce_hits and pe_hits:
        strength = max(ce_hits[0]["inc"], pe_hits[0]["inc"])
        return {"bias": "Vol Expansion", "suggestion": "Long Straddle / Strangle", "strength": strength}
    return {"bias": "Neutral", "suggestion": "Wait / No Trade", "strength": 0.0}


# ------------------ Flask + SocketIO ------------------
app = Flask(__name__, template_folder='templates', static_folder='static')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# Track active realtime tasks per session id
_realtime_tasks = {}  # sid -> {'running': True/False}


def scan_symbols(symbols, thr=5.0, side="ALL", overrides=None):
    """
    Run the same logic as /api/breakout_scan but return list of dicts (out).
    This function is safe to call from background tasks.
    """
    if overrides is None:
        overrides = {}
    symbols = [s.upper().strip() for s in (symbols or []) if s]
    out = []
    for sym in symbols:
        try:
            # small buffer per symbol to be polite
            time.sleep(0.15)
            oc = get_json(oc_url(sym))
            base = parse_rows(oc)
            used = overrides.get(sym) or (base["expiries"][0] if base["expiries"] else None)
            parsed = parse_rows(oc, chosen_expiry=used)
            rows, under = parsed["rows"], parsed["underlying"]
            if not rows or under is None:
                logger.debug(f"No data for {sym}")
                continue

            atm_ce_strike, atm_ce_iv = nearest_strike_iv(rows, under, "CE")
            atm_pe_strike, atm_pe_iv = nearest_strike_iv(rows, under, "PE")

            atm_ce_ltp = None
            atm_pe_ltp = None
            for r in rows:
                if r["strike"] == atm_ce_strike:
                    atm_ce_ltp = r.get("CE_ltp")
                if r["strike"] == atm_pe_strike:
                    atm_pe_ltp = r.get("PE_ltp")

            def build_hits(side_name, comp):
                hits = []
                if comp is None:
                    return hits
                for r in rows:
                    iv = r.get(f"{side_name}_iv")
                    ltp = r.get(f"{side_name}_ltp")
                    if iv is None:
                        continue
                    strike = r["strike"]
                    if strike is None:
                        continue
                    if side_name == "CE" and strike < under:
                        continue
                    if side_name == "PE" and strike > under:
                        continue
                    inc = iv - comp
                    if inc >= thr:
                        dist_pct = abs(strike - under) / under * 100.0
                        hits.append({
                            "strike": strike,
                            "iv": iv,
                            "ltp": ltp,
                            "inc": round(inc, 2),
                            "dist_pct": round(dist_pct, 2)
                        })
                hits.sort(key=lambda x: (x["inc"], -abs(x["dist_pct"])), reverse=True)
                return hits

            ce_hits = build_hits("CE", atm_ce_iv) if side in ("ALL", "CE") else []
            pe_hits = build_hits("PE", atm_pe_iv) if side in ("ALL", "PE") else []

            def best_from(hits):
                if not hits:
                    return {}
                best = {"MaxJump": hits[0]}
                best["ClosestATM"] = sorted(hits, key=lambda h: abs(h["dist_pct"]))[0]
                return best

            strategy = build_strategy(ce_hits, pe_hits)

            out.append({
                "symbol": sym,
                "expiry_used": used or "-",
                "underlying": round(under, 2) if under else 0,
                "atm_ce_iv": round(atm_ce_iv, 2) if atm_ce_iv is not None else None,
                "atm_pe_iv": round(atm_pe_iv, 2) if atm_pe_iv is not None else None,
                "atm_ce_ltp": round(atm_ce_ltp, 2) if atm_ce_ltp is not None else None,
                "atm_pe_ltp": round(atm_pe_ltp, 2) if atm_pe_ltp is not None else None,
                "ce_hits": ce_hits or [],
                "pe_hits": pe_hits or [],
                "best": {
                    "ce_hit_count": len(ce_hits),
                    "pe_hit_count": len(pe_hits),
                    "ce_max_inc": round(max((h["inc"] for h in ce_hits), default=0), 2),
                    "pe_max_inc": round(max((h["inc"] for h in pe_hits), default=0), 2),
                    "ce_hits": ce_hits or [],
                    "pe_hits": pe_hits or [],
                },
                "strategy": strategy
            })
        except Exception as e:
            logger.warning(f"Scan error for {sym}: {e}")
            continue

    return out


# ------------------ REST Endpoints (kept mostly same) ------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/oc", methods=["GET"])
def api_oc():
    symbol = request.args.get("symbol", "SBIN").upper().strip()
    if not symbol:
        return jsonify({"error": "Symbol required"}), 400
    expiry = request.args.get("expiry")
    try:
        oc = get_json(oc_url(symbol))
        base = parse_rows(oc)
        used = expiry or (base["expiries"][0] if base["expiries"] else None)
        parsed = parse_rows(oc, chosen_expiry=used)
        return jsonify({
            "symbol": symbol,
            "expiries": base["expiries"],
            "expiry_used": used,
            "underlying": parsed["underlying"],
            "rows": parsed["rows"],
        })
    except Exception as e:
        logger.error(f"API OC error for {symbol}: {e}")
        return jsonify({"error": "Failed to fetch data"}), 500


@app.route("/api/breakout_scan", methods=["POST"])
def api_breakout_scan():
    data = request.get_json(silent=True) or {}
    symbols = [s.upper().strip() for s in (data.get("symbols") or []) if s]
    if not symbols:
        return jsonify({"error": "At least one symbol required"}), 400
    try:
        thr = float(data.get("threshold", 5.0))
        if thr < 0:
            return jsonify({"error": "Threshold must be >=0"}), 400
    except ValueError:
        return jsonify({"error": "Invalid threshold"}), 400
    side = (data.get("side") or "ALL").upper()
    if side not in ("ALL", "CE", "PE"):
        return jsonify({"error": "Side must be ALL, CE, or PE"}), 400
    overrides = data.get("expiry_overrides") or {}

    out = scan_symbols(symbols, thr=thr, side=side, overrides=overrides)
    return jsonify({"data": out, "ts": datetime.now(timezone.utc).isoformat()})


@app.route("/api/suggested", methods=["GET"])
def api_suggested():
    popular = [
    "360ONE", "ABB", "APLAPOLLO", "AUBANK", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS",
    "ABCAPITAL", "ALKEM", "AMBER", "AMBUJACEM", "ANGELONE", "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT",
    "ASTRAL", "AUROPHARMA", "DMART", "AXISBANK", "BSE", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV",
    "BANDHANBNK", "BANKBARODA", "BANKINDIA", "BDL", "BEL", "BHARATFORG", "BHEL", "BPCL",
    "BHARTIARTL", "BIOCON", "BLUESTARCO", "BOSCHLTD", "BRITANNIA", "CGPOWER", "CANBK", "CDSL",
    "CHOLAFIN", "CIPLA", "COALINDIA", "COFORGE", "COLPAL", "CAMS", "CONCOR", "CROMPTON",
    "CUMMINSIND", "CYIENT", "DLF", "DABUR", "DALBHARAT", "DELHIVERY", "DIVISLAB", "DIXON",
    "DRREDDY", "ETERNAL", "EICHERMOT", "EXIDEIND", "NYKAA", "FORTIS", "GAIL", "GMRAIRPORT",
    "GLENMARK", "GODREJCP", "GODREJPROP", "GRASIM", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE",
    "HFCL", "HAVELLS", "HEROMOTOCO", "HINDALCO", "HAL", "HINDPETRO", "HINDUNILVR", "HINDZINC",
    "POWERINDIA", "HUDCO", "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDFCFIRSTB", "IIFL", "ITC",
    "INDIANB", "IEX", "IOC", "IRCTC", "IRFC", "IREDA", "IGL", "INDUSTOWER",
    "INDUSINDBK", "NAUKRI", "INFY", "INOXWIND", "INDIGO", "JINDALSTEL", "JSWENERGY", "JSWSTEEL",
    "JIOFIN", "JUBLFOOD", "KEI", "KPITTECH", "KALYANKJIL", "KAYNES", "KFINTECH", "KOTAKBANK",
    "LTF", "LICHSGFIN", "LTIM", "LT", "LAURUSLABS", "LICI", "LODHA", "LUPIN",
    "M&M", "MANAPPURAM", "MANKIND", "MARICO", "MARUTI", "MFSL", "MAXHEALTH", "MAZDOCK",
    "MPHASIS", "MCX", "MUTHOOTFIN", "NBCC", "NCC", "NHPC", "NMDC", "NTPC",
    "NATIONALUM", "NESTLEIND", "NUVAMA", "OBEROIRLTY", "ONGC", "OIL", "PAYTM", "OFSS",
    "POLICYBZR", "PGEL", "PIIND", "PNBHOUSING", "PAGEIND", "PATANJALI", "PERSISTENT", "PETRONET",
    "PIDILITIND", "PPLPHARMA", "POLYCAB", "PFC", "POWERGRID", "PRESTIGE", "PNB", "RBLBANK",
    "RECLTD", "RVNL", "RELIANCE", "SBICARD", "SBILIFE", "SHREECEM", "SRF", "SAMMAANCAP",
    "MOTHERSON", "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SONACOMS", "SBIN", "SAIL", "SUNPHARMA",
    "SUPREMEIND", "SUZLON", "SYNGENE", "TATACONSUM", "TITAGARH", "TVSMOTOR", "TCS", "TATAELXSI",
    "TMPV", "TATAPOWER", "TATASTEEL", "TATATECH", "TECHM", "FEDERALBNK", "INDHOTEL", "PHOENIXLTD",
    "TITAN", "TORNTPHARM", "TORNTPOWER", "TRENT", "TIINDIA", "UNOMINDA", "UPL", "ULTRACEMCO",
    "UNIONBANK", "UNITDSPR", "VBL", "VEDL", "IDEA", "VOLTAS", "WIPRO", "YESBANK",
    "ZYDUSLIFE"
    ]
    return jsonify({"symbols": popular})


# ---- strategy_ltp_scan (copied from original app.py) ----
@app.route("/api/strategy_ltp_scan", methods=["POST"])
def api_strategy_ltp_scan():
    data = request.get_json(silent=True) or {}
    symbols = [s.upper().strip() for s in (data.get("symbols") or []) if s][:210]
    if not symbols:
        symbols = ["NIFTY", "BANKNIFTY", "RELIANCE", "TCS"]

    try:
        buy_lots = int(data.get("buy_lots", 1))
        sell_lots = int(data.get("sell_lots", 3))
        target = float(data.get("target_diff", 6.0))
        tolerance = float(data.get("tolerance", 1.0))
        side = (data.get("side") or "ALL").upper()
        atm_from_pct = float(data.get("atm_from_pct", 0.0))
        atm_to_pct = float(data.get("atm_to_pct", 5.0))
        min_strike_diff = float(data.get("min_strike_diff", 1.0))
        max_strike_diff = float(data.get("max_strike_diff", 10.0))
    except:
        return jsonify({"error": "Invalid inputs"}), 400

    LOT_SIZES = {
   '360ONE': 500,
    'ABB': 125,
    'ABCAPITAL': 3100,
    'ADANIENSOL': 675,
    'ADANIENT': 300,
    'ADANIGREEN': 600,
    'ADANIPORTS': 475,
    'ALKEM': 125,
    'AMBER': 100,
    'AMBUJACEM': 1050,
    'ANGELONE': 250,
    'APLAPOLLO': 350,
    'APOLLOHOSP': 125,
    'ASHOKLEY': 5000,
    'ASIANPAINT': 250,
    'ASTRAL': 425,
    'AUBANK': 1000,
    'AUROPHARMA': 550,
    'AXISBANK': 625,
    'BAJAJ-AUTO': 75,
    'BAJAJFINSV': 250,
    'BAJFINANCE': 750,
    'BANDHANBNK': 3600,
    'BANKBARODA': 2925,
    'BANKINDIA': 5200,
    'BANKNIFTY': 35,
    'BDL': 325,
    'BEL': 1425,
    'BHARATFORG': 500,
    'BHARTIARTL': 475,
    'BHEL': 2625,
    'BIOCON': 2500,
    'BLUESTARCO': 325,
    'BOSCHLTD': 25,
    'BPCL': 1975,
    'BRITANNIA': 125,
    'BSE': 375,
    'CAMS': 150,
    'CANBK': 6750,
    'CDSL': 475,
    'CGPOWER': 850,
    'CHOLAFIN': 625,
    'CIPLA': 375,
    'COALINDIA': 1350,
    'COFORGE': 375,
    'COLPAL': 225,
    'CONCOR': 1250,
    'CROMPTON': 1800,
    'CUMMINSIND': 200,
    'CYIENT': 425,
    'DABUR': 1250,
    'DALBHARAT': 325,
    'DELHIVERY': 2075,
    'DIVISLAB': 100,
    'DIXON': 50,
    'DLF': 825,
    'DMART': 150,
    'DRREDDY': 625,
    'EICHERMOT': 175,
    'ETERNAL': 2425,
    'EXIDEIND': 1800,
    'FEDERALBNK': 5000,
    'FINNIFTY': 65,
    'FORTIS': 775,
    'GAIL': 3150,
    'GLENMARK': 375,
    'GMRAIRPORT': 6975,
    'GODREJCP': 500,
    'GODREJPROP': 275,
    'GRASIM': 250,
    'HAL': 150,
    'HAVELLS': 500,
    'HCLTECH': 350,
    'HDFCAMC': 150,
    'HDFCBANK': 550,
    'HDFCLIFE': 1100,
    'HEROMOTOCO': 150,
    'HFCL': 6450,
    'HINDALCO': 700,
    'HINDPETRO': 2025,
    'HINDUNILVR': 300,
    'HINDZINC': 1225,
    'HUDCO': 2775,
    'ICICIBANK': 700,
    'ICICIGI': 325,
    'ICICIPRULI': 925,
    'IDEA': 71475,
    'IDFCFIRSTB': 9275,
    'IEX': 3750,
    'IGL': 2750,
    'IIFL': 1650,
    'INDHOTEL': 1000,
    'INDIANB': 1000,
    'INDIGO': 150,
    'INDUSINDBK': 700,
    'INDUSTOWER': 1700,
    'INFY': 400,
    'INOXWIND': 3272,
    'IOC': 4875,
    'IRCTC': 875,
    'IREDA': 3450,
    'IRFC': 4250,
    'ITC': 1600,
    'JINDALSTEL': 625,
    'JIOFIN': 2350,
    'JSWENERGY': 1000,
    'JSWSTEEL': 675,
    'JUBLFOOD': 1250,
    'KALYANKJIL': 1175,
    'KAYNES': 100,
    'KEI': 175,
    'KFINTECH': 450,
    'KOTAKBANK': 400,
    'KPITTECH': 400,
    'LAURUSLABS': 850,
    'LICHSGFIN': 1000,
    'LICI': 700,
    'LODHA': 450,
    'LT': 175,
    'LTF': 4462,
    'LTIM': 150,
    'LUPIN': 425,
    'M&M': 200,
    'MANAPPURAM': 3000,
    'MANKIND': 225,
    'MARICO': 1200,
    'MARUTI': 50,
    'MAXHEALTH': 525,
    'MAZDOCK': 175,
    'MCX': 125,
    'MFSL': 400,
    'MIDCPNIFTY': 140,
    'MOTHERSON': 6150,
    'MPHASIS': 275,
    'MUTHOOTFIN': 275,
    'NATIONALUM': 3750,
    'NAUKRI': 375,
    'NBCC': 6500,
    'NCC': 2700,
    'NESTLEIND': 500,
    'NHPC': 6400,
    'NIFTY': 75,
    'NMDC': 6750,
    'NTPC': 1500,
    'NUVAMA': 75,
    'NYKAA': 3125,
    'OBEROIRLTY': 350,
    'OFSS': 75,
    'OIL': 1400,
    'ONGC': 2250,
    'PAGEIND': 15,
    'PATANJALI': 900,
    'PAYTM': 725,
    'PERSISTENT': 100,
    'PETRONET': 1800,
    'PFC': 1300,
    'PGEL': 700,
    'PHOENIXLTD': 350,
    'PIDILITIND': 500,
    'PIIND': 175,
    'PNB': 8000,
    'PNBHOUSING': 650,
    'POLICYBZR': 350,
    'POLYCAB': 125,
    'POWERGRID': 1900,
    'POWERINDIA': 50,
    'PPLPHARMA': 2500,
    'PRESTIGE': 450,
    'RBLBANK': 3175,
    'RECLTD': 1275,
    'RELIANCE': 500,
    'RVNL': 1375,
    'SAIL': 4700,
    'SAMMAANCAP': 4300,
    'SBICARD': 800,
    'SBILIFE': 375,
    'SBIN': 750,
    'SHREECEM': 25,
    'SHRIRAMFIN': 825,
    'SIEMENS': 125,
    'SOLARINDS': 75,
    'SONACOMS': 1050,
    'SRF': 200,
    'SUNPHARMA': 350,
    'SUPREMEIND': 175,
    'SUZLON': 8000,
    'SYNGENE': 1000,
    'TATACONSUM': 550,
    'TATAELXSI': 100,
    'TATAPOWER': 1450,
    'TATASTEEL': 5500,
    'TATATECH': 800,
    'TCS': 175,
    'TECHM': 600,
    'TIINDIA': 200,
    'TITAGARH': 725,
    'TITAN': 175,
    'TMPV': 800,
    'TORNTPHARM': 250,
    'TORNTPOWER': 375,
    'TRENT': 100,
    'TVSMOTOR': 175,
    'ULTRACEMCO': 50,
    'UNIONBANK': 4425,
    'UNITDSPR': 400,
    'UNOMINDA': 550,
    'UPL': 1355,
    'VBL': 1025,
    'VEDL': 1150,
    'VOLTAS': 375,
    'WIPRO': 3000,
    'YESBANK': 31100,
    'ZYDUSLIFE': 900
    }

    def lot_for(sym):
        return LOT_SIZES.get(sym.upper(), 1)

    out = []
    for sym in symbols:
        try:
            oc = get_json(oc_url(sym))
            base = parse_rows(oc)
            used = data.get("expiry_overrides", {}).get(sym) or (base["expiries"][0] if base["expiries"] else None)
            parsed = parse_rows(oc, chosen_expiry=used)
            rows, under = parsed["rows"], parsed["underlying"]  # <--- PEHLE YEH

            # Ab straddle calculate karo
            straddle_price, straddle_iv, atm_strike, straddle_label = get_straddle_info(rows, under)
            if not rows or under is None:
                continue

            # Build strike map (filter ATM range)
            strike_map = {}
            for r in rows:
                strike = r["strike"]
                if not strike:
                    continue
                dist_pct = abs(strike - under) / under * 100
                if dist_pct < atm_from_pct or dist_pct > atm_to_pct:
                    continue
                strike_map[strike] = {
                    "CE_ltp": safe_float(r.get("CE_ltp")),
                    "PE_ltp": safe_float(r.get("PE_ltp")),
                }

            if not strike_map:
                continue

            strikes = sorted(strike_map.keys())
            matches = []
            atm_strike = min(strikes, key=lambda x: abs(x - under))

            sides = ["CE", "PE"] if side == "ALL" else [side]

            for side_name in sides:
                buy_side = sell_side = side_name
                buy_key = f"{buy_side}_ltp"
                sell_key = f"{sell_side}_ltp"

                for buy_strike in strikes:
                    if buy_side == "CE" and buy_strike < atm_strike:
                        continue
                    if buy_side == "PE" and buy_strike > atm_strike:
                        continue

                    buy_ltp = strike_map[buy_strike].get(buy_key)
                    if buy_ltp is None:
                        continue

                    if buy_side == "CE":
                        sell_strikes = [s for s in strikes if s > buy_strike]
                    else:
                        sell_strikes = [s for s in strikes if s < buy_strike]

                    for sell_strike in sell_strikes:
                        sell_ltp = strike_map[sell_strike].get(sell_key)
                        if sell_ltp is None:
                            continue
                        diff_pct = abs(sell_strike - buy_strike) / under * 100.0 if under else 0.0
                        if diff_pct < min_strike_diff or diff_pct > max_strike_diff:
                            continue

                        net = (buy_ltp * buy_lots) - (sell_ltp * sell_lots)
                        abs_net = abs(net)

                        if (target - tolerance) <= abs_net <= (target + tolerance):
                            lot = lot_for(sym)
                            total_pnl = net * lot
                            matches.append({
                                "buy_strike": buy_strike,
                                "sell_strike": sell_strike,
                                "buy_side": buy_side,
                                "sell_side": sell_side,
                                "buy_ltp": round(buy_ltp, 2),
                                "sell_ltp": round(sell_ltp, 2),
                                "net_per_lot": round(net, 2),
                                "total_pnl": round(total_pnl),
                                "lot_size": lot,
                                "expiry": used
                            })

            if matches:
                out.append({
                    "symbol": sym,
                    "underlying": under,
                    "expiry_used": used,
                    "matches": matches
                })

        except Exception as e:
            logger.warning(f"Error in {sym}: {e}")
            continue

    return jsonify({"data": out})


if __name__ == "__main__":
    prime()
    app.run(host="0.0.0.0", port=PORT, debug=False)




# ==================== GOD MODE PREMIUM SURGE DETECTOR v10 ====================
from datetime import datetime
from collections import deque

# Global cache with history (last 5 prices) + timestamp
PREV_SURGE_CACHE = {}  # (sym, expiry) → {strike: {"CE": deque([prices]), "PE": deque([prices]), "ts": datetime}}

LOT_SIZES = {
    "NIFTY": 25, "BANKNIFTY": 15, "FINNIFTY": 25, "MIDCPNIFTY": 50,
    "SENSEX": 10, "SENSEX50": 15, "INDIAVIX": 100
}

def get_lot_size(sym): return LOT_SIZES.get(sym.upper(), 50)

@app.route("/api/premium_surge", methods=["POST"])
def api_premium_surge():
    data = request.get_json(silent=True) or {}
    symbols = [s.upper().strip() for s in (data.get("symbols") or []) if s][:200]
    if not symbols:
        return jsonify({"data": []})

    min_pct = float(data.get("min_pct", 200))
    overrides = data.get("expiry_overrides") or {}
    out = []

    for sym in symbols:
        try:
            oc = get_json(oc_url(sym))
            base = parse_rows(oc)
            used = overrides.get(sym) or (base["expiries"][0] if base["expiries"] else None)
            parsed = parse_rows(oc, chosen_expiry=used)
            rows = parsed.get("rows", [])
            under = parsed.get("underlying")
            if not rows or under is None: continue

            key = (sym, used)
            if key not in PREV_SURGE_CACHE:
                PREV_SURGE_CACHE[key] = {}

            lot_size = get_lot_size(sym)

            for r in rows:
                strike = r.get("strike")
                if not strike or under is None: continue

                if strike not in PREV_SURGE_CACHE[key]:
                    PREV_SURGE_CACHE[key][strike] = {"CE": deque(maxlen=5), "PE": deque(maxlen=5), "ts": None}

                cache = PREV_SURGE_CACHE[key][strike]

                # === SUPER INTELLIGENT CE CHECK ===
                ce = r.get("CE_ltp")
                ce_vol = r.get("CE_vol") or 0
                if ce and strike > under + 10:  # True OTM only
                    if cache["CE"]:
                        prev = cache["CE"][-1]
                        if prev > 0.1:  # ignore noise below 0.1
                            pct = (ce - prev) / prev * 100.0
                            speed = 0
                            if len(cache["CE"]) >= 3:
                                speed = (ce - cache["CE"][0]) / cache["CE"][0] * 100.0  # 3-tick momentum

                            lots = ce_vol / lot_size
                            strength = "NUCLEAR" if pct >= 1000 else "EXTREME" if pct >= 500 else "STRONG" if pct >= 300 else "GOOD"

                            if pct >= min_pct:
                                out.append({
                                    "symbol": sym,
                                    "expiry": used,
                                    "strike": int(strike),
                                    "side": "CE",
                                    "prev": round(prev, 2),
                                    "curr": round(ce, 2),
                                    "pct": round(pct, 1),
                                    "speed": round(speed, 1),
                                    "volume": int(ce_vol),
                                    "lots": round(lots, 2),
                                    "strength": strength,
                                    "type": "OTM_BOMB" if lots < 5 else "INSTITUTIONAL_BOMB",
                                    "timestamp": datetime.now().strftime("%H:%M:%S")
                                })
                    cache["CE"].append(ce)

                # === SAME FOR PE ===
                pe = r.get("PE_ltp")
                pe_vol = r.get("PE_vol") or 0
                if pe and strike < under - 10:
                    if cache["PE"]:
                        prev = cache["PE"][-1]
                        if prev > 0.1:
                            pct = (pe - prev) / prev * 100.0
                            speed = 0
                            if len(cache["PE"]) >= 3:
                                speed = (pe - cache["PE"][0]) / cache["PE"][0] * 100.0

                            lots = pe_vol / lot_size
                            strength = "NUCLEAR" if pct >= 1000 else "EXTREME" if pct >= 500 else "STRONG" if pct >= 300 else "GOOD"

                            if pct >= min_pct:
                                out.append({
                                    "symbol": sym,
                                    "expiry": used,
                                    "strike": int(strike),
                                    "side": "PE",
                                    "prev": round(prev, 2),
                                    "curr": round(pe, 2),
                                    "pct": round(pct, 1),
                                    "speed": round(speed, 1),
                                    "volume": int(pe_vol),
                                    "lots": round(lots, 2),
                                    "strength": strength,
                                    "type": "OTM_BOMB" if lots < 5 else "INSTITUTIONAL_BOMB",
                                    "timestamp": datetime.now().strftime("%H:%M:%S")
                                })
                    cache["PE"].append(pe)

        except Exception as e:
            logger.warning(f"Surge error {sym}: {e}")

    # GOD LEVEL SORTING: NUCLEAR > EXTREME > SPEED > %
    def scoreBomb(x):
        score = x["pct"]
        if x["strength"] == "NUCLEAR": score += 5000
        if x["strength"] == "EXTREME": score += 2000
        if x["speed"] > x["pct"] * 1.5: score += 1000
        if x["type"] == "INSTITUTIONAL_BOMB": score += 500
        return score

    out.sort(key=scoreBomb, reverse=True)
    return jsonify({"data": out[:50]})  # top 50 bombs only

# ------------------ SocketIO Handlers ------------------
@socketio.on("connect")
def handle_connect():
    sid = request.sid
    logger.info(f"Client connected: {sid}")
    _realtime_tasks[sid] = {"running": False}

@socketio.on("disconnect")
def handle_disconnect():
    sid = request.sid
    logger.info(f"Client disconnected: {sid}")
    if sid in _realtime_tasks:
        _realtime_tasks[sid]["running"] = False
        del _realtime_tasks[sid]

@socketio.on("start_realtime")
def handle_start_realtime(data):
    sid = request.sid
    logger.info(f"start_realtime from {sid} payload={data}")
    symbols = data.get("symbols") or []
    try:
        thr = float(data.get("threshold") or 5.0)
    except:
        thr = 5.0
    side = (data.get("side") or "ALL").upper()
    if side not in ("ALL", "CE", "PE"):
        side = "ALL"
    interval = int(data.get("interval") or 15)
    overrides = data.get("expiry_overrides") or {}

    # Stop previous if running
    if sid in _realtime_tasks and _realtime_tasks[sid].get("running"):
        _realtime_tasks[sid]["running"] = False

    _realtime_tasks[sid] = {"running": True}

    def _bg_loop():
        logger.info(f"Realtime loop started for {sid} interval={interval}s symbols={symbols[:10]}")
        while _realtime_tasks.get(sid, {}).get("running"):
            try:
                out = scan_symbols(symbols, thr=thr, side=side, overrides=overrides)
                payload = {"data": out, "ts": datetime.now(timezone.utc).isoformat()}
                socketio.emit("update_data", payload, to=sid)
            except Exception as e:
                logger.warning(f"Realtime scan error for {sid}: {e}")
                time.sleep(max(1, interval))
        logger.info(f"Realtime loop stopped for {sid}")

    socketio.start_background_task(_bg_loop)
    emit("realtime_started", {"ok": True})

@socketio.on("stop_realtime")
def handle_stop_realtime():
    sid = request.sid
    logger.info(f"stop_realtime from {sid}")
    if sid in _realtime_tasks:
        _realtime_tasks[sid]["running"] = False
    emit("realtime_stopped", {"ok": True})


if __name__ == "__main__":
    logger.info(f"Starting server on port {PORT}")
    socketio.run(app, host="0.0.0.0", port=PORT)
    
    import os

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

