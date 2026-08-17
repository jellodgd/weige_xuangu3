# -*- coding: utf-8 -*-
"""QMT built-in data probe for diagnosing available market data APIs.

Paste this file into the standard QMT Python strategy editor and run it from
the model trading panel. It does not place orders. It only logs what the QMT
Context/global APIs can return for sectors, stocks, ticks, and daily bars.
"""

import datetime as dt
import os
import time
import traceback


OUTPUT_DIR = "D:\\\u56fd\u91d1QMT\u4ea4\u6613\u7aef\u6a21\u62df\\DAqmt_jingjia_logs"
FALLBACK_OUTPUT_SUBDIR = "DAqmt_jingjia_logs"
PROBE_ACCOUNT_ID = "YOUR_ACCOUNT_ID"
PROBE_DOWNLOAD_HISTORY = True
PROBE_DOWNLOAD_CODES = 3

SECTOR_NAMES = (
    "\u6caa\u6df1A\u80a1",
    "\u6caa\u6df1\u4eacA\u80a1",
    "A\u80a1",
    "\u4e0a\u8bc1A\u80a1",
    "\u6df1\u8bc1A\u80a1",
    "\u6caaA",
    "\u6df1A",
)

SAMPLE_CODES = (
    "600000.SH",
    "000001.SZ",
    "002658.SZ",
    "002208.SZ",
    "002037.SZ",
)


class G:
    pass


g = G()


def now_text():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dir(path):
    try:
        os.makedirs(path)
        return path
    except OSError:
        if os.path.isdir(path):
            return path
        return ""


def setup_log_path():
    primary = ensure_dir(OUTPUT_DIR)
    if primary:
        return os.path.join(primary, "qmt_data_probe_{0}.log".format(dt.date.today().strftime("%Y%m%d")))
    base = os.getcwd()
    fallback = ensure_dir(os.path.join(base, FALLBACK_OUTPUT_SUBDIR))
    if fallback:
        return os.path.join(fallback, "qmt_data_probe_{0}.log".format(dt.date.today().strftime("%Y%m%d")))
    return ""


def log(message):
    line = "[qmt_probe] {0} {1}".format(now_text(), message)
    try:
        print(line)
    except Exception:
        pass
    path = getattr(g, "log_path", "")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def safe_len(value):
    try:
        return len(value)
    except Exception:
        return -1


def short_repr(value, limit=500):
    try:
        text = repr(value)
    except Exception:
        text = "<repr failed>"
    if len(text) > limit:
        return text[:limit] + "...(truncated)"
    return text


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


def call_with_variants(name, funcs, arg_variants):
    results = []
    for func in funcs:
        if not callable(func):
            continue
        func_name = getattr(func, "__name__", str(func))
        for args, kwargs in arg_variants:
            try:
                value = func(*args, **kwargs)
                results.append((func_name, args, kwargs, value, None))
                break
            except TypeError as exc:
                results.append((func_name, args, kwargs, None, "TypeError: {0}".format(exc)))
                continue
            except Exception:
                results.append((func_name, args, kwargs, None, traceback.format_exc()))
                break
    if not results:
        log("{0}: no callable function found".format(name))
    return results


def probe_context(C):
    log("context type={0}".format(type(C)))
    attrs = [
        "get_stock_list_in_sector",
        "get_stock_name",
        "get_full_tick",
        "get_market_data_ex",
        "get_market_data",
        "get_history_data",
        "get_local_data",
        "set_universe",
        "get_universe",
        "subscribe_quote",
        "unsubscribe_quote",
        "download_history_data",
    ]
    for attr in attrs:
        log("context attr {0}: {1}".format(attr, callable(getattr(C, attr, None))))
    for attr in attrs:
        log("global attr {0}: {1}".format(attr, callable(globals().get(attr))))


def get_sector_codes(C, sector_name):
    realtime = int(time.time() * 1000)
    funcs = [getattr(C, "get_stock_list_in_sector", None), globals().get("get_stock_list_in_sector")]
    variants = [
        ((sector_name, realtime), {}),
        ((sector_name,), {}),
    ]
    results = call_with_variants("get_stock_list_in_sector", funcs, variants)
    for func_name, args, _kwargs, value, error in results:
        if error:
            log("sector {0} func={1} args={2} error={3}".format(sector_name, func_name, short_repr(args), error.splitlines()[-1]))
            continue
        log("sector {0} func={1} args={2} returned_len={3} examples={4}".format(
            sector_name,
            func_name,
            short_repr(args),
            safe_len(value),
            short_repr(list(value or [])[:10]),
        ))
        if value:
            return list(value)
    return []


def probe_universe(C):
    all_codes = []
    seen = set()
    for sector_name in SECTOR_NAMES:
        codes = get_sector_codes(C, sector_name)
        for code in codes:
            normalized = normalize_stock_code(code)
            if normalized and normalized not in seen:
                seen.add(normalized)
                all_codes.append(normalized)
    main_board = [code for code in all_codes if code_is_main_board(code)]
    log("universe merged raw_unique={0} main_board={1} examples_raw={2} examples_main={3}".format(
        len(all_codes),
        len(main_board),
        ",".join(all_codes[:20]) if all_codes else "none",
        ",".join(main_board[:20]) if main_board else "none",
    ))
    if main_board:
        try:
            C.set_universe(main_board[:500])
            log("set_universe first_500 success")
        except Exception:
            log("set_universe failed: {0}".format(traceback.format_exc()))
    return main_board


def probe_names(C, codes):
    funcs = [getattr(C, "get_stock_name", None), globals().get("get_stock_name")]
    targets = list(codes[:10]) if codes else list(SAMPLE_CODES)
    for code in targets:
        results = call_with_variants("get_stock_name", funcs, [((code,), {})])
        for func_name, _args, _kwargs, value, error in results:
            if error:
                log("name code={0} func={1} error={2}".format(code, func_name, error.splitlines()[-1]))
            else:
                log("name code={0} func={1} value={2}".format(code, func_name, short_repr(value, 120)))


def probe_ticks(C, codes):
    targets = list(codes[:20]) if codes else list(SAMPLE_CODES)
    funcs = [getattr(C, "get_full_tick", None), globals().get("get_full_tick")]
    results = call_with_variants("get_full_tick", funcs, [((targets,), {})])
    for func_name, _args, _kwargs, value, error in results:
        if error:
            log("full_tick func={0} codes={1} error={2}".format(func_name, len(targets), error.splitlines()[-1]))
            continue
        keys = list((value or {}).keys()) if hasattr(value, "keys") else []
        log("full_tick func={0} requested={1} returned={2} keys={3}".format(
            func_name,
            len(targets),
            len(keys),
            ",".join([str(k) for k in keys[:10]]) if keys else "none",
        ))
        for key in keys[:5]:
            log("full_tick example key={0} quote={1}".format(key, short_repr(value.get(key), 700)))


def probe_daily(C, codes):
    targets = list(codes[:20]) if codes else list(SAMPLE_CODES)
    download_targets = targets[:PROBE_DOWNLOAD_CODES]
    end_date = dt.date.today().strftime("%Y%m%d")
    start_date = (dt.date.today() - dt.timedelta(days=30)).strftime("%Y%m%d")
    if PROBE_DOWNLOAD_HISTORY:
        funcs = [getattr(C, "download_history_data", None), globals().get("download_history_data")]
        for code in download_targets:
            results = call_with_variants(
                "download_history_data",
                funcs,
                [
                    ((code, "1d", start_date, end_date), {}),
                    ((code, "1d"), {}),
                ],
            )
            for func_name, args, kwargs, value, error in results:
                if error:
                    log("download_history_data code={0} func={1} args={2} error={3}".format(
                        code,
                        func_name,
                        short_repr(args, 120),
                        error.splitlines()[-1],
                    ))
                else:
                    log("download_history_data code={0} func={1} args={2} value={3}".format(
                        code,
                        func_name,
                        short_repr(args, 120),
                        short_repr(value, 120),
                    ))
    funcs = [getattr(C, "get_market_data_ex", None), globals().get("get_market_data_ex")]
    variants = [
        ((["low", "close"], targets), dict(period="1d", start_time=start_date, end_time=end_date, count=-1, dividend_type="none", fill_data=False, subscribe=False)),
        ((["low", "close"], targets), dict(period="1d", start_time=start_date, end_time=end_date, count=-1, dividend_type="none", fill_data=True, subscribe=True)),
        ((["low", "close"], targets), dict(period="1d", end_time=end_date, count=30, dividend_type="none", fill_data=True, subscribe=True)),
        ((["low", "close"], targets, "1d", start_date, end_date, -1, "none", False, False), {}),
    ]
    results = call_with_variants("get_market_data_ex", funcs, variants)
    for func_name, args, kwargs, value, error in results:
        if error:
            log("daily func={0} args={1} kwargs={2} error={3}".format(func_name, short_repr(args, 160), short_repr(kwargs, 160), error.splitlines()[-1]))
            continue
        keys = list((value or {}).keys()) if hasattr(value, "keys") else []
        log("daily func={0} requested={1} returned={2} keys={3}".format(
            func_name,
            len(targets),
            len(keys),
            ",".join([str(k) for k in keys[:10]]) if keys else "none",
        ))
        for key in keys[:3]:
            table = value.get(key)
            log("daily example key={0} type={1} len={2} data={3}".format(
                key,
                type(table),
                safe_len(table),
                short_repr(table, 700),
            ))

    funcs = [getattr(C, "get_market_data", None), globals().get("get_market_data")]
    variants = [
        ((["low", "close"],), dict(stock_code=targets, period="1d", start_time=start_date, end_time=end_date, count=-1, dividend_type="none")),
        ((["low", "close"], targets, start_date, end_date, True, "1d", "none", -1), {}),
        ((["low", "close"],), dict(stock_code=targets[:1], period="1d", end_time=end_date, count=30, dividend_type="none")),
    ]
    results = call_with_variants("get_market_data", funcs, variants)
    for func_name, args, kwargs, value, error in results:
        if error:
            log("old_daily_market_data func={0} args={1} kwargs={2} error={3}".format(func_name, short_repr(args, 160), short_repr(kwargs, 160), error.splitlines()[-1]))
            continue
        log("old_daily_market_data func={0} type={1} len={2} data={3}".format(
            func_name,
            type(value),
            safe_len(value),
            short_repr(value, 1000),
        ))

    funcs = [getattr(C, "get_history_data", None), globals().get("get_history_data")]
    for field in ["low", "close"]:
        results = call_with_variants(
            "get_history_data",
            funcs,
            [
                ((30, "1d", field, "none", True), {}),
                ((30, "1d", field), {}),
            ],
        )
        for func_name, args, kwargs, value, error in results:
            if error:
                log("history_data field={0} func={1} args={2} error={3}".format(field, func_name, short_repr(args, 120), error.splitlines()[-1]))
                continue
            keys = list((value or {}).keys()) if hasattr(value, "keys") else []
            log("history_data field={0} func={1} returned={2} keys={3} example={4}".format(
                field,
                func_name,
                len(keys),
                ",".join([str(k) for k in keys[:10]]) if keys else "none",
                short_repr(value.get(keys[0]) if keys else value, 700) if hasattr(value, "get") else short_repr(value, 700),
            ))

    funcs = [getattr(C, "get_local_data", None), globals().get("get_local_data")]
    results = call_with_variants(
        "get_local_data",
        funcs,
        [
            ((targets[:1], start_date, end_date, "1d", "none", 30), {}),
            ((targets[:1], start_date, end_date, "1d", "none", -1), {}),
        ],
    )
    for func_name, args, kwargs, value, error in results:
        if error:
            log("local_data func={0} args={1} error={2}".format(func_name, short_repr(args, 160), error.splitlines()[-1]))
            continue
        log("local_data func={0} type={1} len={2} data={3}".format(
            func_name,
            type(value),
            safe_len(value),
            short_repr(value, 1000),
        ))


def run_probe(C):
    log("probe begin account={0}".format(PROBE_ACCOUNT_ID))
    probe_context(C)
    codes = probe_universe(C)
    probe_names(C, codes)
    probe_ticks(C, codes)
    probe_daily(C, codes)
    log("probe finished main_board={0}".format(len(codes)))


def init(C):
    g.log_path = setup_log_path()
    log("init data probe log_path={0}".format(g.log_path or "stdout only"))


def after_init(C):
    log("after_init begin")
    try:
        run_probe(C)
    except Exception:
        log("probe failed: {0}".format(traceback.format_exc()))
    log("after_init finished")


def handlebar(C):
    return


def stop(C):
    log("strategy stopped")
