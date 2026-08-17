# -*- coding: utf-8 -*-
"""QMT built-in Context strategy for the jingjia filter.

Paste this file into the standard QMT Python strategy editor and run it from
the model trading panel.  It intentionally does not use xtquant, XtQuantTrader,
or an external Qt launcher.
"""

import csv
import datetime as dt
import math
import os
import time
import traceback


# =========================
# User configuration
# =========================

ACCOUNT_ID = "YOUR_ACCOUNT_ID"
ACCOUNT_TYPE = "stock"
STRATEGY_NAME = "jingjia_filter_0927_qmt"

SECTOR_NAME = "\u6caa\u6df1A\u80a1"
SECTOR_NAMES = (
    SECTOR_NAME,
    "\u6caa\u6df1\u4eacA\u80a1",
    "A\u80a1",
    "\u4e0a\u8bc1A\u80a1",
    "\u6df1\u8bc1A\u80a1",
)
MAX_ORDER_STOCKS = 2

FIRST_MIN_RISE_PCT = 5.0
FINAL_MIN_RISE_PCT = -1.0
FINAL_MAX_RISE_PCT = 3.0
FIRST_RISE_LIMIT_UP = False
EXCLUDE_FINAL_PRICE_BELOW_5 = True

RANK_BASE_MIN_RISE_PCT = -10.0
RANK_BASE_MAX_RISE_PCT = 10.0
PRIOR_MAX_DROP_CHECK = False
PRIOR_MAX_DROP_THRESHOLD_PCT = -7.0
PRIOR_T1_RISE_CHECK = False
PRIOR_T1_MAX_RISE_PCT = 7.0

ORDER_PRICE_RISE_PCT = 0.5
SINGLE_STOCK_ASSET_RATIO = 0.30
LOT_SIZE = 100
PRICE_TICK = 0.01
CHUNK_SIZE = 100

DOWNLOAD_DAILY = True
DOWNLOAD_DAILY_LOOKBACK_DAYS = 30
RETRY_DOWNLOAD_ON_EMPTY_DAILY = True
REQUIRE_0900_DAILY_DOWNLOAD = True
WRITE_CSV = True
OUTPUT_DIR = "DAqmt_jingjia_logs"
FALLBACK_OUTPUT_DIR = "DAqmt_jingjia_logs"
LOG_TO_FILE = True
SAMPLE_DEBUG_EXAMPLES = 8
LOG_TICK_CHUNKS = False


# =========================
# Schedule
# =========================

DAILY_DOWNLOAD_TIME = dt.time(9, 0, 0)
PREPARE_TIME = dt.time(9, 15, 5)
ORDER_TIME = dt.time(9, 27, 0)
FINAL_CANCEL_TIME = dt.time(10, 0, 0)
EVALUATE_TIME = dt.time(9, 26, 35)

SAMPLE_DEFS = [
    ("precheck", dt.time(9, 16, 0), dt.time(9, 16, 0), dt.time(9, 18, 0), 120),
    ("a", dt.time(9, 20, 3), dt.time(9, 20, 0), dt.time(9, 20, 20), 20),
    ("b", dt.time(9, 21, 0), dt.time(9, 20, 55), dt.time(9, 21, 20), 20),
    ("c", dt.time(9, 22, 0), dt.time(9, 21, 55), dt.time(9, 22, 20), 20),
    ("d", dt.time(9, 23, 0), dt.time(9, 22, 55), dt.time(9, 23, 20), 20),
    ("e", dt.time(9, 24, 0), dt.time(9, 23, 55), dt.time(9, 24, 20), 20),
    ("f", dt.time(9, 25, 5), dt.time(9, 25, 0), dt.time(9, 26, 59), 85),
]


class G(object):
    pass


g = G()


def reset_day_state(preserve_daily_download=False):
    daily_download_date = getattr(g, "daily_download_date", None)
    daily_download_done = getattr(g, "daily_download_done", False)
    daily_download_codes = getattr(g, "daily_download_codes", [])
    daily_download_in_progress = getattr(g, "daily_download_in_progress", False)
    g.trade_date = None
    g.universe = []
    g.states = {}
    g.selected = []
    g.order_targets = []
    g.submitted_remarks = {}
    g.order_ids_by_remark = {}
    g.prepared = False
    g.evaluated = False
    g.ordered = False
    g.final_canceled = False
    if preserve_daily_download and daily_download_date == dt.date.today():
        g.daily_download_date = daily_download_date
        g.daily_download_done = daily_download_done
        g.daily_download_codes = daily_download_codes
        g.daily_download_in_progress = daily_download_in_progress
    else:
        g.daily_download_date = None
        g.daily_download_done = False
        g.daily_download_codes = []
        g.daily_download_in_progress = False


reset_day_state()


def ensure_output_dir():
    for path in (OUTPUT_DIR, os.path.abspath(FALLBACK_OUTPUT_DIR)):
        try:
            if not os.path.isdir(path):
                os.makedirs(path)
            return path
        except Exception:
            continue
    return None


def log(msg):
    line = "[jingjia_qmt] {0} {1}".format(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line)
    if not LOG_TO_FILE:
        return
    try:
        output_dir = ensure_output_dir()
        if not output_dir:
            return
        day = dt.date.today().strftime("%Y%m%d")
        path = os.path.join(output_dir, "jingjia_qmt_{0}.log".format(day))
        with open(path, "a") as fp:
            fp.write(line + "\n")
    except Exception:
        pass


def fmt_dt(value):
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return ""


def short_quote_debug(code, quote, quote_dt, price, reason):
    return "{0}:{1}:time={2}:raw={3}:lastPrice={4}:lastClose={5}:open={6}:price={7}".format(
        code,
        reason,
        fmt_dt(quote_dt),
        quote.get("time", quote.get("timetag", quote.get("stime", ""))) if quote else "",
        quote.get("lastPrice", quote.get("last_price", "")) if quote else "",
        quote.get("lastClose", quote.get("last_close", "")) if quote else "",
        quote.get("open", "") if quote else "",
        price,
    )


def count_sampled(label):
    return len([1 for state in g.states.values() if label in state.get("samples", {})])


def log_stage_state(stage):
    try:
        counts = []
        for label in ["precheck", "a", "b", "c", "d", "e", "f"]:
            counts.append("{0}={1}".format(label, count_sampled(label)))
        log("{0} state prepared={1} evaluated={2} ordered={3} states={4} selected={5} targets={6} samples {7}".format(
            stage,
            getattr(g, "prepared", False),
            getattr(g, "evaluated", False),
            getattr(g, "ordered", False),
            len(getattr(g, "states", {})),
            len(getattr(g, "selected", [])),
            len(getattr(g, "order_targets", [])),
            ",".join(counts),
        ))
    except Exception:
        log("{0} state log failed: {1}".format(stage, traceback.format_exc()))


def log_reason_counts(states):
    counts = {}
    for state in states:
        reason = state.get("reason") or "EMPTY"
        for item in reason.split(";"):
            counts[item] = counts.get(item, 0) + 1
    summary = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    log("evaluation reason counts: {0}".format(
        ", ".join("{0}={1}".format(name, count) for name, count in summary[:20])
    ))


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def first_attr(value, names, default=None):
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
        if isinstance(value, dict) and name in value:
            return value.get(name)
    return default


def chunked(items, size):
    return [items[i:i + size] for i in range(0, len(items), size)]


def round_to_tick(price, tick=PRICE_TICK):
    if tick <= 0:
        tick = PRICE_TICK
    return round(round(price / tick) * tick, 4)


def volume_for_budget(budget, price):
    if budget <= 0 or price <= 0:
        return 0
    return int(math.floor(budget / price / LOT_SIZE) * LOT_SIZE)


def today_at(value):
    return dt.datetime.combine(dt.date.today(), value)


def next_time_point(value):
    now = dt.datetime.now()
    target = dt.datetime.combine(now.date(), value)
    if target <= now:
        target += dt.timedelta(days=1)
    return target


def schedule_daily(C, func, time_value, name):
    target = next_time_point(time_value)
    try:
        C.schedule_run(func, target.strftime("%Y%m%d%H%M%S"), -1, dt.timedelta(days=1), name)
        log("scheduled {0} at {1}".format(name, target.strftime("%Y-%m-%d %H:%M:%S")))
    except Exception:
        log("schedule failed {0}: {1}".format(name, traceback.format_exc()))


def schedule_once(C, func, time_value, repeat_seconds, name):
    target = dt.datetime.combine(dt.date.today(), time_value)
    if target <= dt.datetime.now():
        return
    try:
        C.schedule_run(func, target.strftime("%Y%m%d%H%M%S"), repeat_seconds, dt.timedelta(seconds=1), name)
        log("scheduled window {0} at {1} repeat={2}".format(name, target.strftime("%H:%M:%S"), repeat_seconds))
    except Exception:
        log("schedule window failed {0}: {1}".format(name, traceback.format_exc()))


def parse_trade_date(value):
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) < 8:
        return None
    try:
        return dt.datetime.strptime(digits[:8], "%Y%m%d").date()
    except Exception:
        return None


def quote_datetime(quote):
    if not quote:
        return None
    value = quote.get("time") or quote.get("timetag") or quote.get("stime")
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    try:
        number = float(value)
        if number > 100000000000:
            return dt.datetime.fromtimestamp(number / 1000.0)
        if number > 1000000000:
            return dt.datetime.fromtimestamp(number)
    except Exception:
        pass
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    for fmt, width in (("%Y%m%d%H%M%S", 14), ("%Y%m%d", 8)):
        if len(digits) >= width:
            try:
                return dt.datetime.strptime(digits[:width], fmt)
            except Exception:
                pass
    return None


def get_tick_price(quote):
    if not quote:
        return 0.0
    for field in ("lastPrice", "last_price", "price", "open"):
        price = safe_float(quote.get(field), 0.0)
        if price > 0:
            return price
    return 0.0


def normalize_stock_code(code):
    text = str(code or "").strip().upper()
    if not text:
        return ""
    text = text.replace("_", ".").replace("-", ".")
    parts = [part for part in text.split(".") if part]
    market_map = {
        "SH": "SH",
        "SSE": "SH",
        "SHSE": "SH",
        "SZ": "SZ",
        "SZSE": "SZ",
    }
    if len(parts) >= 2:
        if parts[0].isdigit() and len(parts[0]) == 6:
            market = market_map.get(parts[1])
            if market:
                return parts[0] + "." + market
        if parts[1].isdigit() and len(parts[1]) == 6:
            market = market_map.get(parts[0])
            if market:
                return parts[1] + "." + market
    if len(text) == 8 and text[:2] in ("SH", "SZ") and text[2:].isdigit():
        return text[2:] + "." + text[:2]
    if text.isdigit() and len(text) == 6:
        if text.startswith(("600", "601", "603", "605")):
            return text + ".SH"
        if text.startswith(("000", "001", "002", "003")):
            return text + ".SZ"
    return text


def code_is_main_board(code):
    code = normalize_stock_code(code)
    symbol, _, market = code.partition(".")
    if market == "SH":
        return symbol.startswith(("600", "601", "603", "605"))
    if market == "SZ":
        return symbol.startswith(("000", "001", "002", "003"))
    return False


def name_is_normal(name):
    text = str(name or "").upper()
    if not text:
        return True
    return (
        "ST" not in text
        and not text.startswith("N")
        and not text.startswith("C")
        and "\u9000" not in text
    )


def get_stock_name(C, code):
    try:
        return C.get_stock_name(code)
    except Exception:
        return ""


def call_stock_list_in_sector(C, sector_name):
    realtime = int(time.time() * 1000)
    funcs = [getattr(C, "get_stock_list_in_sector", None), globals().get("get_stock_list_in_sector")]
    for func in funcs:
        if not callable(func):
            continue
        for args in ((sector_name, realtime), (sector_name,)):
            try:
                return func(*args) or []
            except TypeError:
                continue
            except Exception:
                log("get_stock_list_in_sector failed sector={0} func={1}: {2}".format(
                    sector_name,
                    getattr(func, "__name__", str(func)),
                    traceback.format_exc(),
                ))
                break
    return []


def call_get_full_tick(C, codes):
    funcs = [getattr(C, "get_full_tick", None), globals().get("get_full_tick")]
    for func in funcs:
        if not callable(func):
            continue
        try:
            return func(codes) or {}
        except Exception:
            log("call_get_full_tick failed func={0} codes={1}: {2}".format(
                getattr(func, "__name__", str(func)),
                len(codes),
                traceback.format_exc(),
            ))
            continue
    return {}


def call_get_market_data_ex(C, fields, codes, start_date, end_date):
    kwargs = dict(
        period="1d",
        start_time=start_date,
        end_time=end_date,
        count=-1,
        dividend_type="none",
        fill_data=False,
        subscribe=False,
    )
    funcs = [getattr(C, "get_market_data_ex", None), globals().get("get_market_data_ex")]
    for func in funcs:
        if not callable(func):
            continue
        try:
            return func(fields, codes, **kwargs) or {}
        except TypeError:
            try:
                return func(fields, codes, "1d", start_date, end_date, -1, "none", False, False) or {}
            except Exception:
                log("call_get_market_data_ex positional failed func={0} codes={1}: {2}".format(
                    getattr(func, "__name__", str(func)),
                    len(codes),
                    traceback.format_exc(),
                ))
                continue
        except Exception:
            log("call_get_market_data_ex keyword failed func={0} codes={1}: {2}".format(
                getattr(func, "__name__", str(func)),
                len(codes),
                traceback.format_exc(),
            ))
            continue
    return {}


def call_download_history_data(C, code, start_date, end_date):
    funcs = [globals().get("download_history_data"), getattr(C, "download_history_data", None)]
    for func in funcs:
        if not callable(func):
            continue
        try:
            return bool(func(code, "1d", start_date, end_date))
        except TypeError:
            try:
                return bool(func(code, "1d"))
            except Exception:
                log("download_history_data fallback failed code={0} func={1}: {2}".format(
                    code,
                    getattr(func, "__name__", str(func)),
                    traceback.format_exc(),
                ))
                continue
        except Exception:
            log("download_history_data failed code={0} func={1}: {2}".format(
                code,
                getattr(func, "__name__", str(func)),
                traceback.format_exc(),
            ))
            continue
    return False


def get_all_codes(C):
    codes = []
    raw_seen = set()
    for sector_name in SECTOR_NAMES:
        try:
            sector_codes = call_stock_list_in_sector(C, sector_name)
        except Exception:
            log("get_stock_list_in_sector failed sector={0}: {1}".format(sector_name, traceback.format_exc()))
            sector_codes = []
        log("universe sector {0}: raw={1}".format(sector_name, len(sector_codes)))
        for code in sector_codes:
            normalized = normalize_stock_code(code)
            if normalized and normalized not in raw_seen:
                raw_seen.add(normalized)
                codes.append(normalized)
    if not codes:
        log("universe raw code examples: none")
    else:
        log("universe raw unique={0} examples={1}".format(len(codes), ",".join(codes[:10])))
    result = []
    for code in codes:
        if code_is_main_board(code):
            name = get_stock_name(C, code)
            if name_is_normal(name):
                result.append(code)
    result = sorted(set(result))
    log("universe filtered main_board_normal={0} from_raw={1} examples={2}".format(
        len(result),
        len(codes),
        ",".join(result[:10]) if result else "none",
    ))
    try:
        C.set_universe(result)
    except Exception:
        pass
    return result


def row_close(row):
    if hasattr(row, "get"):
        return safe_float(row.get("close"), 0.0)
    if isinstance(row, dict):
        return safe_float(row.get("close"), 0.0)
    return 0.0


def row_low(row):
    if hasattr(row, "get"):
        return safe_float(row.get("low"), 0.0)
    if isinstance(row, dict):
        return safe_float(row.get("low"), 0.0)
    return 0.0


def rows_from_table(table):
    if table is None:
        return []
    if hasattr(table, "iterrows"):
        rows = []
        for row_index, row in table.iterrows():
            bar_date = parse_trade_date(row_index)
            low_price = row_low(row)
            close_price = row_close(row)
            if bar_date is not None and close_price > 0:
                rows.append((bar_date, low_price, close_price))
        return rows
    if isinstance(table, dict):
        close_data = table.get("close", table)
        low_data = table.get("low", {})
        if isinstance(close_data, dict):
            rows = []
            for key, close_price in close_data.items():
                bar_date = parse_trade_date(key)
                low_price = 0.0
                if isinstance(low_data, dict):
                    low_price = safe_float(low_data.get(key), 0.0)
                close_price = safe_float(close_price)
                if bar_date is not None and close_price > 0:
                    rows.append((bar_date, low_price, close_price))
            return rows
    return []


def download_daily_if_needed(C, codes, target_date):
    if not DOWNLOAD_DAILY:
        log("download daily skipped: DOWNLOAD_DAILY=False")
        return 0
    start_date = (target_date - dt.timedelta(days=DOWNLOAD_DAILY_LOOKBACK_DAYS)).strftime("%Y%m%d")
    end_date = target_date.strftime("%Y%m%d")
    if not callable(globals().get("download_history_data")) and not callable(getattr(C, "download_history_data", None)):
        log("download_history_data not available; please download daily bars in QMT data manager")
        return 0
    started = time.time()
    log("download daily bars start stocks={0} start={1} end={2}".format(len(codes), start_date, end_date))
    ok_count = 0
    for index, code in enumerate(codes, start=1):
        if call_download_history_data(C, code, start_date, end_date):
            ok_count += 1
        if index % 300 == 0:
            log("download daily progress {0}/{1}".format(index, len(codes)))
    log("download daily bars finished ok={0} total={1} elapsed={2:.3f}s".format(
        ok_count,
        len(codes),
        time.time() - started,
    ))
    return ok_count


def load_rank_base_closes(C, codes, target_date):
    start_date = (target_date - dt.timedelta(days=DOWNLOAD_DAILY_LOOKBACK_DAYS)).strftime("%Y%m%d")
    end_date = target_date.strftime("%Y%m%d")
    result = {}

    def add_refs_from_history(history, part):
        added = 0
        empty_tables = 0
        for code in part:
            rows = []
            table = history.get(code) if isinstance(history, dict) else None
            parsed_rows = rows_from_table(table)
            if not parsed_rows:
                empty_tables += 1
            for bar_date, low_price, close_price in parsed_rows:
                if bar_date < target_date:
                    rows.append((bar_date, low_price, close_price))
            rows.sort(key=lambda item: item[0])
            if len(rows) >= 3:
                rank_base_date, _rank_low, rank_base_close = rows[-3]
                prior_t2_date, prior_t2_low, prior_t2_close = rows[-2]
                prior_t1_date, prior_t1_low, prior_t1_close = rows[-1]
                prior_t2_max_drop_pct = None
                prior_t1_max_drop_pct = None
                prior_t1_rise_pct = None
                if rank_base_close > 0 and prior_t2_low > 0:
                    prior_t2_max_drop_pct = (prior_t2_low / rank_base_close - 1.0) * 100.0
                if prior_t2_close > 0 and prior_t1_low > 0:
                    prior_t1_max_drop_pct = (prior_t1_low / prior_t2_close - 1.0) * 100.0
                if prior_t2_close > 0 and prior_t1_close > 0:
                    prior_t1_rise_pct = (prior_t1_close / prior_t2_close - 1.0) * 100.0
                prior_t1_limit_up = None
                if prior_t2_close > 0 and prior_t1_close > 0:
                    prior_t1_up_limit = round_to_tick(prior_t2_close * 1.10)
                    prior_t1_limit_up = abs(prior_t1_close - prior_t1_up_limit) <= PRICE_TICK / 2.0 + 1e-9
                result[code] = {
                    "rank_base_date": rank_base_date,
                    "rank_base_close": rank_base_close,
                    "prior_t2_date": prior_t2_date,
                    "prior_t2_max_drop_pct": prior_t2_max_drop_pct,
                    "prior_t1_date": prior_t1_date,
                    "prior_t1_max_drop_pct": prior_t1_max_drop_pct,
                    "prior_t1_rise_pct": prior_t1_rise_pct,
                    "prior_t1_limit_up": prior_t1_limit_up,
                }
                added += 1
        return added, empty_tables

    for part_index, part in enumerate(chunked(codes, CHUNK_SIZE), start=1):
        try:
            history = call_get_market_data_ex(C, ["low", "close"], part, start_date, end_date)
        except Exception:
            log("daily load chunk failed {0}: {1}".format(part_index, traceback.format_exc()))
            continue
        added, empty_tables = add_refs_from_history(history, part)
        if added == 0 and empty_tables == len(part) and RETRY_DOWNLOAD_ON_EMPTY_DAILY:
            log("daily load chunk {0}: all empty, redownload and retry stocks={1}".format(
                part_index,
                len(part),
            ))
            ok_count = download_daily_if_needed(C, part, target_date)
            try:
                history = call_get_market_data_ex(C, ["low", "close"], part, start_date, end_date)
                added, empty_tables = add_refs_from_history(history, part)
                log("daily load chunk {0}: retry after download ok={1} added={2} empty_tables={3}".format(
                    part_index,
                    ok_count,
                    added,
                    empty_tables,
                ))
            except Exception:
                log("daily load chunk retry failed {0}: {1}".format(part_index, traceback.format_exc()))
        log("daily load chunk {0}: stocks={1} added={2} empty_tables={3} usable_total={4}".format(
            part_index,
            len(part),
            added,
            empty_tables,
            len(result),
        ))
    log("rank base prepared usable={0} skipped={1}".format(len(result), len(codes) - len(result)))
    return result


def get_latest_ticks(C, codes):
    result = {}
    for part_index, part in enumerate(chunked(codes, CHUNK_SIZE), start=1):
        try:
            data = call_get_full_tick(C, part)
            result.update(data)
            if LOG_TICK_CHUNKS:
                log("get_full_tick chunk {0}: requested={1} returned={2} cumulative={3}".format(
                    part_index,
                    len(part),
                    len(data) if hasattr(data, "__len__") else 0,
                    len(result),
                ))
        except Exception:
            log("get_full_tick failed: {0}".format(traceback.format_exc()))
    return result


def daily_download_ready_for_today():
    return (
        g.daily_download_date == dt.date.today()
        and g.daily_download_done
        and bool(g.daily_download_codes)
    )


def run_daily_download(C):
    trade_date = dt.date.today()
    g.daily_download_in_progress = True
    log("daily download begin trade_date={0} weekday={1}".format(
        trade_date.strftime("%Y-%m-%d"),
        trade_date.weekday(),
    ))
    if trade_date.weekday() >= 5:
        reset_day_state()
        g.daily_download_in_progress = False
        log("daily download skipped: weekend")
        return
    codes = get_all_codes(C)
    g.daily_download_date = trade_date
    g.daily_download_done = False
    g.daily_download_codes = codes
    log("daily download universe loaded main_board_normal={0}".format(len(codes)))
    if not codes:
        g.daily_download_in_progress = False
        return
    ok_count = download_daily_if_needed(C, codes, trade_date)
    g.daily_download_done = ok_count > 0
    g.daily_download_in_progress = False
    log("daily download ready trade_date={0} done={1} ok={2} stocks={3}".format(
        trade_date.strftime("%Y-%m-%d"),
        g.daily_download_done,
        ok_count,
        len(codes),
    ))


def prepare_states(C):
    trade_date = dt.date.today()
    log("prepare begin trade_date={0}".format(trade_date.strftime("%Y-%m-%d")))
    reset_day_state(preserve_daily_download=True)
    g.trade_date = trade_date
    if daily_download_ready_for_today():
        codes = list(g.daily_download_codes)
        log("reuse 09:00 daily download universe main_board_normal={0}".format(len(codes)))
    else:
        if REQUIRE_0900_DAILY_DOWNLOAD:
            log("prepare skipped: 09:00 daily download not ready date={0} done={1} in_progress={2} codes={3}".format(
                getattr(g, "daily_download_date", None),
                getattr(g, "daily_download_done", False),
                getattr(g, "daily_download_in_progress", False),
                len(getattr(g, "daily_download_codes", []) or []),
            ))
            return
        codes = get_all_codes(C)
    g.universe = codes
    log("universe loaded main_board_normal={0}".format(len(codes)))
    if not codes:
        return
    if daily_download_ready_for_today():
        log("skip prepare daily download: already done at 09:00")
    else:
        download_daily_if_needed(C, codes, trade_date)
    daily_refs = load_rank_base_closes(C, codes, trade_date)
    ticks = get_latest_ticks(C, codes)
    log("prepare preclose tick snapshot requested={0} returned={1}".format(len(codes), len(ticks)))
    states = {}
    skipped_no_preclose = 0
    for code in codes:
        quote = ticks.get(code) or {}
        pre_close = safe_float(quote.get("lastClose"), 0.0)
        if pre_close <= 0:
            skipped_no_preclose += 1
            continue
        name = get_stock_name(C, code)
        daily_ref = daily_refs.get(code, {})
        states[code] = {
            "code": code,
            "name": name,
            "pre_close": pre_close,
            "up_limit": round_to_tick(pre_close * 1.10),
            "rank_base_date": daily_ref.get("rank_base_date"),
            "rank_base_close": safe_float(daily_ref.get("rank_base_close"), 0.0),
            "prior_t2_date": daily_ref.get("prior_t2_date"),
            "prior_t2_max_drop_pct": daily_ref.get("prior_t2_max_drop_pct"),
            "prior_t1_date": daily_ref.get("prior_t1_date"),
            "prior_t1_max_drop_pct": daily_ref.get("prior_t1_max_drop_pct"),
            "prior_t1_rise_pct": daily_ref.get("prior_t1_rise_pct"),
            "prior_t1_limit_up": daily_ref.get("prior_t1_limit_up"),
            "samples": {},
            "passed": False,
            "reason": "",
        }
    g.states = states
    g.prepared = True
    log("states prepared usable={0} skipped_no_preclose={1} rank_base_usable={2}".format(
        len(states),
        skipped_no_preclose,
        len(daily_refs),
    ))
    log_stage_state("after_prepare")


def valid_quote_for_window(quote_dt, minimum_time, maximum_time):
    if quote_dt is None:
        return False
    current_date = dt.date.today()
    min_dt = dt.datetime.combine(current_date, minimum_time)
    max_dt = dt.datetime.combine(current_date, maximum_time)
    return min_dt <= quote_dt <= max_dt


def collect_sample(C, label, target_time, minimum_time, maximum_time):
    if not g.prepared:
        log("sample {0} skipped: not prepared".format(label))
        return
    pending = [code for code, state in g.states.items() if label not in state["samples"]]
    if not pending:
        log("sample {0}: skipped no pending total={1}".format(label, len(g.states)))
        return
    started = time.time()
    ticks = get_latest_ticks(C, pending)
    captured = 0
    missing_quote = 0
    invalid_time = 0
    invalid_price = 0
    examples = []
    for code in pending:
        quote = ticks.get(code) or {}
        quote_dt = quote_datetime(quote)
        if not quote:
            missing_quote += 1
            if len(examples) < SAMPLE_DEBUG_EXAMPLES:
                examples.append(short_quote_debug(code, quote, quote_dt, 0.0, "missing_quote"))
            continue
        if not valid_quote_for_window(quote_dt, minimum_time, maximum_time):
            invalid_time += 1
            if len(examples) < SAMPLE_DEBUG_EXAMPLES:
                examples.append(short_quote_debug(code, quote, quote_dt, get_tick_price(quote), "invalid_time"))
            continue
        price = get_tick_price(quote)
        if price <= 0:
            invalid_price += 1
            if len(examples) < SAMPLE_DEBUG_EXAMPLES:
                examples.append(short_quote_debug(code, quote, quote_dt, price, "invalid_price"))
            continue
        g.states[code]["samples"][label] = {
            "target_time": target_time,
            "quote_time": quote_dt,
            "scan_time": dt.datetime.now(),
            "price": price,
        }
        captured += 1
    sampled_total = count_sampled(label)
    elapsed_ms = (time.time() - started) * 1000.0
    log("sample {0}: window={1}~{2} pending_before={3} returned={4} captured={5} sampled_total={6}/{7} missing_quote={8} invalid_time={9} invalid_price={10} elapsed_ms={11:.1f}".format(
        label,
        minimum_time.strftime("%H:%M:%S"),
        maximum_time.strftime("%H:%M:%S"),
        len(pending),
        len(ticks),
        captured,
        sampled_total,
        len(g.states),
        missing_quote,
        invalid_time,
        invalid_price,
        elapsed_ms,
    ))
    if examples:
        log("sample {0} examples: {1}".format(label, " | ".join(examples)))


def collect_precheck(C):
    collect_sample(C, "precheck", dt.time(9, 16, 0), dt.time(9, 16, 0), dt.time(9, 18, 0))


def collect_a(C):
    collect_sample(C, "a", dt.time(9, 20, 3), dt.time(9, 20, 0), dt.time(9, 20, 20))


def collect_b(C):
    collect_sample(C, "b", dt.time(9, 21, 0), dt.time(9, 20, 55), dt.time(9, 21, 20))


def collect_c(C):
    collect_sample(C, "c", dt.time(9, 22, 0), dt.time(9, 21, 55), dt.time(9, 22, 20))


def collect_d(C):
    collect_sample(C, "d", dt.time(9, 23, 0), dt.time(9, 22, 55), dt.time(9, 23, 20))


def collect_e(C):
    collect_sample(C, "e", dt.time(9, 24, 0), dt.time(9, 23, 55), dt.time(9, 24, 20))


def collect_f(C):
    collect_sample(C, "f", dt.time(9, 25, 5), dt.time(9, 25, 0), dt.time(9, 26, 59))


SAMPLE_CALLBACKS = {
    "precheck": collect_precheck,
    "a": collect_a,
    "b": collect_b,
    "c": collect_c,
    "d": collect_d,
    "e": collect_e,
    "f": collect_f,
}


def on_prepare(C):
    try:
        log("on_prepare begin")
        if dt.date.today().weekday() >= 5:
            reset_day_state()
            log("prepare skipped: weekend")
            return
        prepare_states(C)
        if not getattr(g, "prepared", False):
            log("sample scheduling skipped: prepare not completed")
            return
        for label, start_time, _min_time, _max_time, repeat_seconds in SAMPLE_DEFS:
            schedule_once(C, SAMPLE_CALLBACKS[label], start_time, repeat_seconds, "sample_" + label)
    except Exception:
        log("prepare failed: {0}".format(traceback.format_exc()))


def on_daily_download(C):
    try:
        run_daily_download(C)
    except Exception:
        g.daily_download_in_progress = False
        log("daily download failed: {0}".format(traceback.format_exc()))


def state_sample_prices(state):
    samples = state["samples"]
    labels = ["a", "b", "c", "d", "e", "f"]
    if not all(label in samples for label in labels):
        return None
    return [samples[label]["price"] for label in labels]


def rank_f_vs_base_pct(state):
    if "f" not in state["samples"]:
        return None
    base = safe_float(state.get("rank_base_close"), 0.0)
    if base <= 0:
        return None
    return (state["samples"]["f"]["price"] / base - 1) * 100.0


def rank_f_minus_base(state):
    if "f" not in state["samples"]:
        return None
    base = safe_float(state.get("rank_base_close"), 0.0)
    if base <= 0:
        return None
    return state["samples"]["f"]["price"] - base


def first_rule_label():
    if FIRST_RISE_LIMIT_UP:
        return "a_limit_up"
    return "a_rise>{0:.2f}".format(FIRST_MIN_RISE_PCT)


def evaluate_states(C):
    log_stage_state("before_evaluate")
    selected = []
    for state in g.states.values():
        samples = state["samples"]
        missing = []
        if "precheck" not in samples:
            missing.append("missing_precheck_sample")
        prices = state_sample_prices(state)
        if prices is None:
            count = len([x for x in ["a", "b", "c", "d", "e", "f"] if x in samples])
            missing.append("missing_samples:{0}/6".format(count))
        if missing:
            state["passed"] = False
            state["reason"] = ";".join(missing)
            continue

        tolerance = PRICE_TICK / 2.0 + 1e-9
        pre_close = state["pre_close"]
        up_limit = state["up_limit"]
        a, _b, _c, _d, e, f = prices
        precheck_price = samples["precheck"]["price"]
        precheck_ok = abs(precheck_price - up_limit) <= tolerance
        a_limit_up = abs(a - up_limit) <= tolerance
        a_rise_pct = (a / pre_close - 1) * 100.0 if pre_close > 0 else -9999
        if FIRST_RISE_LIMIT_UP:
            a_ok = a_limit_up
            a_reason = "a_not_limit_up"
        else:
            a_ok = a_rise_pct > FIRST_MIN_RISE_PCT
            a_reason = "a_rise_not_above_{0:g}".format(FIRST_MIN_RISE_PCT)
        mono_ok = all(prices[i] + tolerance >= prices[i + 1] for i in range(len(prices) - 1))
        f_rise_pct = (f / pre_close - 1) * 100.0 if pre_close > 0 else 9999
        f_ok = FINAL_MIN_RISE_PCT <= f_rise_pct <= FINAL_MAX_RISE_PCT
        final_price_ok = (not EXCLUDE_FINAL_PRICE_BELOW_5) or f >= 5.0
        base = safe_float(state.get("rank_base_close"), 0.0)
        base_ok = base > 0
        rank_pct = rank_f_vs_base_pct(state)
        rank_ok = rank_pct is not None and RANK_BASE_MIN_RISE_PCT <= rank_pct <= RANK_BASE_MAX_RISE_PCT
        last_drop_ok = (e - f) < (up_limit - e)

        checks = [
            ("precheck_not_limit_up", precheck_ok),
            (a_reason, a_ok),
            ("monotonic_non_increasing", mono_ok),
            ("f_rise_outside_{0:g}_to_{1:g}".format(FINAL_MIN_RISE_PCT, FINAL_MAX_RISE_PCT), f_ok),
            ("f_price_below_5", final_price_ok),
            ("rank_base_missing", base_ok),
            ("f_rank_base_rise_outside_-10_to_10", rank_ok),
            ("last_drop_not_less_than_limit_to_e", last_drop_ok),
        ]
        if PRIOR_MAX_DROP_CHECK:
            prior_t2_drop = state.get("prior_t2_max_drop_pct")
            prior_t1_drop = state.get("prior_t1_max_drop_pct")
            checks.append((
                "prior_t2_max_drop_below_{0:g}".format(PRIOR_MAX_DROP_THRESHOLD_PCT),
                prior_t2_drop is not None and prior_t2_drop >= PRIOR_MAX_DROP_THRESHOLD_PCT,
            ))
            checks.append((
                "prior_t1_max_drop_below_{0:g}".format(PRIOR_MAX_DROP_THRESHOLD_PCT),
                prior_t1_drop is not None and prior_t1_drop >= PRIOR_MAX_DROP_THRESHOLD_PCT,
            ))
        if PRIOR_T1_RISE_CHECK:
            prior_t1_rise = state.get("prior_t1_rise_pct")
            checks.append((
                "prior_t1_rise_above_{0:g}".format(PRIOR_T1_MAX_RISE_PCT),
                prior_t1_rise is not None and prior_t1_rise <= PRIOR_T1_MAX_RISE_PCT,
            ))
        failed = [name for name, ok in checks if not ok]
        state["passed"] = len(failed) == 0
        state["reason"] = "PASS" if state["passed"] else ";".join(failed)
        if state["passed"]:
            selected.append(state)

    selected.sort(key=lambda s: s["code"])
    g.selected = selected
    g.order_targets = sorted_order_targets(selected)
    g.evaluated = True
    log_reason_counts(g.states.values())
    log("evaluation selected={0} order_targets={1} rule=precheck_limit_up,{2},a_to_f_non_increasing,f_rise=[{3:g},{4:g}],f_price_must_ge_5={5},rank_f_vs_t3=[{6:g},{7:g}],prior_max_drop_check={8},prior_t1_rise_check={9}".format(
        len(selected),
        len(g.order_targets),
        first_rule_label(),
        FINAL_MIN_RISE_PCT,
        FINAL_MAX_RISE_PCT,
        EXCLUDE_FINAL_PRICE_BELOW_5,
        RANK_BASE_MIN_RISE_PCT,
        RANK_BASE_MAX_RISE_PCT,
        PRIOR_MAX_DROP_CHECK,
        PRIOR_T1_RISE_CHECK,
    ))
    if selected:
        log("evaluation selected codes: {0}".format(",".join(state["code"] for state in selected)))
    else:
        log("evaluation selected codes: none")
    if g.order_targets:
        log("evaluation order target codes: {0}".format(",".join(state["code"] for state in g.order_targets)))
        for index, state in enumerate(g.order_targets, start=1):
            log("evaluation order rank={0} code={1} f_vs_t3_pct={2:.4f} prior_t1_limit_up={3}".format(
                index,
                state["code"],
                rank_f_vs_base_pct(state),
                state.get("prior_t1_limit_up"),
            ))
    else:
        log("evaluation order target codes: none")
    log_stage_state("after_evaluate")
    if WRITE_CSV:
        write_csv_files()


def sorted_order_targets(selected):
    rankable = [state for state in selected if rank_f_vs_base_pct(state) is not None]
    rankable.sort(key=lambda state: (
        1 if state.get("prior_t1_limit_up") else 0,
        rank_f_vs_base_pct(state),
        state["code"],
    ))
    return rankable[:MAX_ORDER_STOCKS]


def sample_text(state, label, field):
    sample = state["samples"].get(label)
    if not sample:
        return ""
    value = sample.get(field)
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def write_csv_files():
    try:
        output_dir = ensure_output_dir()
        if not output_dir:
            log("write csv skipped: no writable output dir")
            return
        day = g.trade_date.strftime("%Y%m%d") if g.trade_date else dt.date.today().strftime("%Y%m%d")
        all_path = os.path.join(output_dir, "jingjia_filter_all_{0}.csv".format(day))
        selected_path = os.path.join(output_dir, "jingjia_filter_selected_{0}.csv".format(day))
        fields = [
            "passed", "reason", "code", "name", "pre_close", "up_limit",
            "rank_base_date", "rank_base_close", "rank_f_vs_base_pct",
            "prior_t2_date", "prior_t2_max_drop_pct",
            "prior_t1_date", "prior_t1_max_drop_pct", "prior_t1_rise_pct",
            "prior_t1_limit_up",
            "p_precheck", "p_a", "p_b", "p_c", "p_d", "p_e", "p_f",
            "quote_precheck", "quote_a", "quote_b", "quote_c", "quote_d", "quote_e", "quote_f",
        ]
        order_rank = dict((state["code"], i) for i, state in enumerate(g.order_targets, start=1))
        fields.insert(6, "order_rank")
        with open(all_path, "w", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=fields)
            writer.writeheader()
            for state in g.states.values():
                writer.writerow(csv_row(state, order_rank))
        with open(selected_path, "w", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=fields)
            writer.writeheader()
            for state in g.order_targets:
                writer.writerow(csv_row(state, order_rank))
        log("csv written all={0} selected={1}".format(all_path, selected_path))
    except Exception:
        log("write csv failed: {0}".format(traceback.format_exc()))


def csv_row(state, order_rank):
    rank_date = state.get("rank_base_date")
    if isinstance(rank_date, dt.date):
        rank_date = rank_date.strftime("%Y-%m-%d")
    prior_t2_date = state.get("prior_t2_date")
    if isinstance(prior_t2_date, dt.date):
        prior_t2_date = prior_t2_date.strftime("%Y-%m-%d")
    prior_t1_date = state.get("prior_t1_date")
    if isinstance(prior_t1_date, dt.date):
        prior_t1_date = prior_t1_date.strftime("%Y-%m-%d")
    row = {
        "passed": state.get("passed"),
        "reason": state.get("reason"),
        "code": state.get("code"),
        "name": state.get("name"),
        "pre_close": round(state.get("pre_close", 0), 4),
        "up_limit": round(state.get("up_limit", 0), 4),
        "order_rank": order_rank.get(state.get("code"), ""),
        "rank_base_date": rank_date or "",
        "rank_base_close": round(state.get("rank_base_close", 0), 4) if state.get("rank_base_close") else "",
        "rank_f_vs_base_pct": round(rank_f_vs_base_pct(state), 4) if rank_f_vs_base_pct(state) is not None else "",
        "prior_t2_date": prior_t2_date or "",
        "prior_t2_max_drop_pct": round(state.get("prior_t2_max_drop_pct"), 4) if state.get("prior_t2_max_drop_pct") is not None else "",
        "prior_t1_date": prior_t1_date or "",
        "prior_t1_max_drop_pct": round(state.get("prior_t1_max_drop_pct"), 4) if state.get("prior_t1_max_drop_pct") is not None else "",
        "prior_t1_rise_pct": round(state.get("prior_t1_rise_pct"), 4) if state.get("prior_t1_rise_pct") is not None else "",
        "prior_t1_limit_up": state.get("prior_t1_limit_up") if state.get("prior_t1_limit_up") is not None else "",
    }
    for label in ["precheck", "a", "b", "c", "d", "e", "f"]:
        sample_price = sample_text(state, label, "price")
        row["p_" + label] = round(safe_float(sample_price), 4) if sample_price not in ("", None) else ""
        row["quote_" + label] = sample_text(state, label, "quote_time")
    return row


def on_evaluate(C):
    try:
        log("on_evaluate begin")
        if not g.prepared:
            log("evaluate skipped: not prepared")
            return
        evaluate_states(C)
    except Exception:
        log("evaluate failed: {0}".format(traceback.format_exc()))


def get_account_asset():
    try:
        infos = get_trade_detail_data(ACCOUNT_ID, ACCOUNT_TYPE, "account") or []
    except Exception:
        log("get account failed: {0}".format(traceback.format_exc()))
        return 0.0, 0.0
    if not infos:
        return 0.0, 0.0
    info = infos[0]
    available = safe_float(first_attr(
        info,
        ("m_dAvailable", "m_dAvailableCash", "m_dEnableBalance", "m_dAvailableBalance", "available", "cash"),
    ), 0.0)
    total = safe_float(first_attr(
        info,
        ("m_dBalance", "m_dAsset", "m_dTotalAsset", "m_dAssureAsset", "balance", "asset"),
    ), 0.0)
    return available, total


def submit_orders(C):
    log("submit_orders begin")
    log_stage_state("before_order")
    if not g.evaluated:
        evaluate_states(C)
    targets = g.order_targets
    if not targets:
        log("order skipped: no targets")
        return
    available, total = get_account_asset()
    if available <= 0 or total <= 0:
        log("order skipped: invalid asset available={0:.2f} total={1:.2f}".format(available, total))
        return
    stock_count = len(targets)
    budget = min(total * SINGLE_STOCK_ASSET_RATIO, available / stock_count)
    log("order allocation stocks={0} available={1:.2f} total={2:.2f} budget={3:.2f}".format(
        stock_count, available, total, budget
    ))
    submitted = 0
    skipped = 0
    failed = 0
    for state in targets:
        price = round_to_tick(state["pre_close"] * (1 + ORDER_PRICE_RISE_PCT / 100.0))
        volume = volume_for_budget(budget, price)
        if volume < LOT_SIZE:
            log("order skip {0} volume<{1}".format(state["code"], LOT_SIZE))
            skipped += 1
            continue
        remark = "{0}_{1}_{2}".format(STRATEGY_NAME, dt.date.today().strftime("%Y%m%d"), state["code"])
        try:
            passorder(23, 1101, ACCOUNT_ID, state["code"], 11, price, volume, STRATEGY_NAME, 2, remark, C)
            g.submitted_remarks[remark] = {
                "code": state["code"],
                "name": state["name"],
                "volume": volume,
                "price": price,
            }
            log("passorder submitted code={0} price={1:.3f} volume={2} remark={3}".format(
                state["code"], price, volume, remark
            ))
            submitted += 1
        except Exception:
            failed += 1
            log("passorder failed {0}: {1}".format(state["code"], traceback.format_exc()))
    g.ordered = True
    log("order summary targets={0} submitted={1} skipped={2} failed={3}".format(
        len(targets), submitted, skipped, failed
    ))
    log_stage_state("after_order")


def on_order(C):
    try:
        log("on_order begin")
        submit_orders(C)
    except Exception:
        log("order failed: {0}".format(traceback.format_exc()))


def order_callback(C, orderInfo):
    try:
        remark = first_attr(orderInfo, ("m_strRemark",), "")
        order_id = first_attr(orderInfo, ("m_strOrderSysID",), "")
        if remark in g.submitted_remarks and order_id:
            g.order_ids_by_remark[remark] = order_id
            log("order callback remark={0} order_id={1}".format(remark, order_id))
    except Exception:
        log("order_callback failed: {0}".format(traceback.format_exc()))


def on_final_cancel(C):
    try:
        log("on_final_cancel begin")
        cancel_unfinished_orders(C)
    except Exception:
        log("final cancel failed: {0}".format(traceback.format_exc()))


def cancel_unfinished_orders(C):
    if not g.submitted_remarks:
        log("final cancel skipped: no submitted remarks")
        return
    try:
        orders = get_trade_detail_data(ACCOUNT_ID, ACCOUNT_TYPE, "order", STRATEGY_NAME) or []
    except Exception:
        log("query orders failed: {0}".format(traceback.format_exc()))
        return
    submitted_remarks = set(g.submitted_remarks)
    canceled = 0
    skipped = 0
    for order in orders:
        remark = first_attr(order, ("m_strRemark",), "")
        if remark not in submitted_remarks:
            continue
        order_id = first_attr(order, ("m_strOrderSysID",), "")
        code = first_attr(order, ("m_strInstrumentID", "m_stockCode"), g.submitted_remarks[remark]["code"])
        traded = safe_int(first_attr(order, ("m_nVolumeTraded",), 0))
        remaining = safe_int(first_attr(order, ("m_nVolumeTotal",), 0))
        original = safe_int(first_attr(order, ("m_nVolumeTotalOriginal",), g.submitted_remarks[remark]["volume"]))
        if remaining <= 0 or traded >= original:
            skipped += 1
            log("final cancel skip fully done code={0} order_id={1} traded={2} original={3}".format(
                code, order_id, traded, original
            ))
            continue
        try:
            if can_cancel_order(order_id, ACCOUNT_ID, ACCOUNT_TYPE):
                ok = cancel(order_id, ACCOUNT_ID, ACCOUNT_TYPE, C)
                canceled += int(bool(ok))
                log("final cancel code={0} order_id={1} remaining={2} result={3}".format(
                    code, order_id, remaining, ok
                ))
            else:
                skipped += 1
                log("final cancel skip not cancelable code={0} order_id={1}".format(code, order_id))
        except Exception:
            log("cancel failed code={0} order_id={1}: {2}".format(code, order_id, traceback.format_exc()))
    g.final_canceled = True
    log("final cancel summary submitted={0} canceled={1} skipped={2}".format(
        len(g.submitted_remarks), canceled, skipped
    ))


def init(ContextInfo):
    log("init QMT Context strategy account={0} output_dir={1} fallback_output_dir={2}".format(
        ACCOUNT_ID,
        OUTPUT_DIR,
        os.path.abspath(FALLBACK_OUTPUT_DIR),
    ))
    log("config max_order={0} a_min={1:.2f} f_rise=[{2:.2f},{3:.2f}] rank_f_vs_t3=[{4:.2f},{5:.2f}] f_price_must_ge_5={6} prior_max_drop_check={7} prior_max_drop_threshold={8:.2f} prior_t1_rise_check={9} prior_t1_max_rise={10:.2f} require_0900_daily_download={11} retry_empty_daily={12} order_price_rise={13:.2f}% single_stock_asset_ratio={14:.2f} write_csv={15} log_to_file={16}".format(
        MAX_ORDER_STOCKS,
        FIRST_MIN_RISE_PCT,
        FINAL_MIN_RISE_PCT,
        FINAL_MAX_RISE_PCT,
        RANK_BASE_MIN_RISE_PCT,
        RANK_BASE_MAX_RISE_PCT,
        EXCLUDE_FINAL_PRICE_BELOW_5,
        PRIOR_MAX_DROP_CHECK,
        PRIOR_MAX_DROP_THRESHOLD_PCT,
        PRIOR_T1_RISE_CHECK,
        PRIOR_T1_MAX_RISE_PCT,
        REQUIRE_0900_DAILY_DOWNLOAD,
        RETRY_DOWNLOAD_ON_EMPTY_DAILY,
        ORDER_PRICE_RISE_PCT,
        SINGLE_STOCK_ASSET_RATIO,
        WRITE_CSV,
        LOG_TO_FILE,
    ))
    try:
        ContextInfo.set_account(ACCOUNT_ID)
    except Exception:
        log("set_account failed: {0}".format(traceback.format_exc()))
    reset_day_state()
    schedule_daily(ContextInfo, on_daily_download, DAILY_DOWNLOAD_TIME, "jingjia_daily_download")
    schedule_daily(ContextInfo, on_prepare, PREPARE_TIME, "jingjia_prepare")
    schedule_daily(ContextInfo, on_evaluate, EVALUATE_TIME, "jingjia_evaluate")
    schedule_daily(ContextInfo, on_order, ORDER_TIME, "jingjia_order")
    schedule_daily(ContextInfo, on_final_cancel, FINAL_CANCEL_TIME, "jingjia_final_cancel")
    now = dt.datetime.now()
    if dt.date.today().weekday() < 5 and DAILY_DOWNLOAD_TIME <= now.time() < PREPARE_TIME:
        log("daily download catch-up: model started after 09:00 before prepare")
        on_daily_download(ContextInfo)


def after_init(ContextInfo):
    log("after_init finished")
    log_stage_state("after_init")


def handlebar(ContextInfo):
    return


def stop(ContextInfo):
    log("strategy stopped")
    log_stage_state("stop")
