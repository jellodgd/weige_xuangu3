# coding: gbk
"""Native BigQMT auction selector with MiniQMT-equivalent order handling."""

# Bump this version for every code update before copying it into BigQMT.
STRATEGY_VERSION = "2026.09.27.3"

import csv
import datetime as dt
import json
import os
import time
import threading
import traceback
from collections import Counter
from decimal import Decimal, ROUND_HALF_UP


# BigQMT runs this strategy from its bin.x64 working directory.
LOG_DIR = "DAqmt_jingjia_logs"
ALL_A_SHARE_SECTOR = "沪深A股"


# 可选筛选条件：在大 QMT 中直接修改这些值；对应 MiniQMT Qt 的三个复选框。
# 是否要求 F 点价格达到最低价格。
ENABLE_F_PRICE_FILTER = True
# 启用价格过滤后，F 点价格必须大于或等于该值（元）。
F_PRICE_MIN = 5.0
# 是否要求 T-1 当日涨幅不超过上限。
ENABLE_T1_RISE_FILTER = True
# 启用 T-1 涨幅过滤后，T-1 当日涨幅必须小于或等于该百分比。
T1_RISE_MAX_PCT = 7.0
# 是否启用 T-2/T-1 两日最大跌幅过滤。
ENABLE_PRIOR_TWO_DAY_MAX_DROP_FILTER = False
# 启用前两日跌幅过滤后，最大跌幅不得低于该百分比。
PRIOR_TWO_DAY_MAX_DROP_THRESHOLD_PCT = -5.0
# 最终最多选中并尝试下单的股票数量。
# Most-selected count: edit this value directly, e.g. 3 means select at most 3 stocks.
MAX_SELECTED_STOCKS = 2
# 单只股票最多使用账户总资产的比例，例如 0.50 表示 50%。
SINGLE_STOCK_ASSET_RATIO = 0.50

# 账号和账号类型由大 QMT“模型交易”页面在运行时提供。
# 不要在这里为账号增加写死的备用值。
# 策略名称：用于日志、委托备注和查询本策略委托。
STRATEGY_NAME = "DAqmt2_jingjia_filter"
# 是否允许提交委托：True 会调用下单接口；False 只做选股和日志。
ORDER_SUBMISSION_ENABLED = True
# 委托价格相对昨收的上浮百分比，例如 0.5 表示昨收价上浮 0.5%。
ORDER_PRICE_RISE_PCT = 0.5
# 买入数量按每手股数向下取整，A 股通常为 100 股一手。
LOT_SIZE = 100

# 核心选股阈值，应与 MiniQMT 默认参数保持一致。
# A 点相对昨收的涨幅必须大于该百分比。
MIN_A_RISE_PCT = 5.0
# F 点相对昨收的最低允许涨幅百分比。
FINAL_MIN_RISE_PCT = -1.0
# F 点相对昨收的最高允许涨幅百分比。
FINAL_MAX_RISE_PCT = 3.0
# F 点相对 T-3 收盘价的最低允许涨幅百分比。
RANK_BASE_MIN_RISE_PCT = -10.0
# F 点相对 T-3 收盘价的最高允许涨幅百分比。
RANK_BASE_MAX_RISE_PCT = 10.0


# 为 T-1、T-2、T-3 等计算读取/下载的日线自然日回看范围。
DAILY_LOOKBACK_CALENDAR_DAYS = 30
# 日线数据每批处理的股票数量。
DAILY_CHUNK_SIZE = 100
# 行情 quote_time 不在目标时间窗时，每次补采样前等待的秒数。
FRESH_RETRY_SECONDS = 0.20

PREPARE_TIME = dt.time(9, 0, 0)
PRECHECK1_TIME = dt.time(9, 15, 1)
PRECHECK1_WINDOW_START = dt.time(9, 15, 0)
PRECHECK1_UNTIL = dt.time(9, 15, 59)
PRECHECK2_TIME = dt.time(9, 16, 0)
PRECHECK2_WINDOW_START = dt.time(9, 16, 0)
PRECHECK2_UNTIL = dt.time(9, 18, 0)
SAMPLE_PLAN = [
    ("a", dt.time(9, 20, 3), dt.time(9, 20, 0), dt.time(9, 20, 20)),
    ("b", dt.time(9, 21, 0), dt.time(9, 20, 55), dt.time(9, 21, 20)),
    ("c", dt.time(9, 22, 0), dt.time(9, 21, 55), dt.time(9, 22, 20)),
    ("d", dt.time(9, 23, 0), dt.time(9, 22, 55), dt.time(9, 23, 20)),
    ("e", dt.time(9, 24, 0), dt.time(9, 23, 55), dt.time(9, 24, 20)),
    ("f", dt.time(9, 25, 5), dt.time(9, 25, 0), dt.time(9, 26, 59)),
]
FINAL_SAMPLE_UNTIL = dt.time(9, 26, 30)
ORDER_TIME = dt.time(9, 27, 0)
ORDER_PROBE_TIME = dt.time(9, 27, 5)
FINAL_CANCEL_TIME = dt.time(10, 0, 0)
# First cache check accepts up to 20 exclusions. After one morning full
# download, proceed with all date-complete stocks regardless of missing count.
DAILY_CACHE_ALLOWED_UNAVAILABLE_CODES = 20
DAILY_SYNC_LOCK = threading.Lock()
LOG_LOCK = threading.RLock()
DOWNLOAD_PROGRESS_INTERVAL_SECONDS = 5.0

STATE = {
    "trade_date": None,
    "universe": {},
    "daily": {},
    "preclose": {},
    "up_limit": {},
    "precheck1": {},
    "precheck2": {},
    "samples": {},
    "selection_rows": [],
    "passed_rows": [],
    "order_targets": [],
    "not_ordered_rows": [],
    "submitted_orders": {},
    "order_callbacks": {},
    "deal_callbacks": [],
    "runtime_account_id": "",
    "runtime_account_type": "",
    "runtime_account_source": "",
    "account_preflight": {},
    "scheduled_names": [],
    "last_stage": "created",
    "daily_cache_ready": False,
    "daily_cache_missing_codes": [],
    "startup_sync_pending": False,
    "morning_prepare_done": False,
    "startup_schedules_done": False,
    "startup_account_retry_done": False,
    "startup_summary_logged": False,
}


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _first_attr(value, names, default=None):
    for name in names:
        try:
            if isinstance(value, dict) and name in value:
                return value.get(name)
            candidate = getattr(value, name, None)
            if candidate is not None:
                return candidate
        except Exception:
            pass
    return default


def _model_trading_account(ContextInfo):
    """Read the account and type supplied by the BigQMT Model Trading page."""
    account_candidates = (
        ("model_trading_global", globals().get("account")),
        ("context_account", getattr(ContextInfo, "account", None)),
    )
    type_candidates = (
        ("model_trading_global", globals().get("accountType")),
        ("context_account_type", getattr(ContextInfo, "accountType", None)),
    )
    account_id, account_source = "", "unavailable"
    account_type, type_source = "", "unavailable"
    for source, value in account_candidates:
        text = str(value or "").strip()
        if text and text.lower() not in ("none", "null"):
            account_id, account_source = text, source
            break
    for source, value in type_candidates:
        text = str(value or "").strip().upper()
        if text and text.lower() not in ("none", "null"):
            account_type, type_source = text, source
            break
    return account_id, account_type, account_source, type_source


def _capture_runtime_account(ContextInfo):
    account_id, account_type, account_source, type_source = _model_trading_account(ContextInfo)
    STATE["runtime_account_id"] = account_id
    STATE["runtime_account_type"] = account_type
    STATE["runtime_account_source"] = account_source + "/" + type_source
    return account_id, account_type, account_source, type_source


def _runtime_account():
    return STATE.get("runtime_account_id") or "", STATE.get("runtime_account_type") or ""


def _account_row_summary(row):
    return {
        "account_id": str(_first_attr(row, ("m_strAccountID", "m_strAccountId", "account_id"), "") or ""),
        "available_cash": round(_safe_float(_first_attr(
            row,
            ("m_dAvailable", "m_dAvailableCash", "m_dEnableBalance", "m_dAvailableBalance"),
            0.0,
        )), 2),
        "total_asset": round(_safe_float(_first_attr(
            row,
            ("m_dBalance", "m_dAsset", "m_dTotalAsset", "m_dAssureAsset"),
            0.0,
        )), 2),
    }


def _query_account_rows(account_id, account_type):
    diagnostics = []
    if not account_id or not account_type:
        return [], [{"error": "runtime_model_account_or_type_unavailable"}]
    for detail_type in ("ACCOUNT", "account"):
        try:
            rows = get_trade_detail_data(account_id, account_type, detail_type) or []
            diagnostics.append({"detail_type": detail_type, "row_count": len(rows)})
            if rows:
                return list(rows), diagnostics
        except Exception as exc:
            diagnostics.append({"detail_type": detail_type, "error": repr(exc)})
    return [], diagnostics


def _run_account_preflight(ContextInfo, attempt):
    """Verify the model-bound account without sending any order."""
    model_account, model_type, account_source, type_source = _capture_runtime_account(ContextInfo)
    rows, query_diagnostics = _query_account_rows(model_account, model_type)
    account_ready = bool(model_account and model_type)
    preflight = {
        "attempt": attempt,
        "model_bound_account_id": model_account,
        "model_bound_account_type": model_type,
        "model_account_source": account_source,
        "model_account_type_source": type_source,
        "account_ready": account_ready,
        "order_blocked": not account_ready,
        "block_reason": "model_trading_account_or_type_unavailable" if not account_ready else "",
        "account_row_count": len(rows),
        "account": _account_row_summary(rows[0]) if rows else None,
        "query_diagnostics": query_diagnostics,
    }
    STATE["account_preflight"] = preflight
    _log(dict(preflight, event="account_preflight"))
    _write_status("account_preflight", {
        "model_bound_account_id": model_account,
        "model_bound_account_type": model_type,
        "account_preflight_order_blocked": not account_ready,
    })


def _main_board_code(code):
    symbol, _, market = str(code).upper().partition(".")
    if market == "SH":
        return symbol.startswith(("600", "601", "603", "605"))
    if market == "SZ":
        return symbol.startswith(("000", "001", "002", "003"))
    return False


def _normal_name(name):
    value = str(name or "").upper().replace(" ", "")
    return bool(value and "ST" not in value and not value.startswith(("N", "C")) and "退" not in value)


def _round_to_tick(price, tick):
    tick = tick if tick > 0 else 0.01
    units = (Decimal(str(price)) / Decimal(str(tick))).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    return float(units * Decimal(str(tick)))


def _parse_date(value):
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) < 8:
        return None
    try:
        return dt.datetime.strptime(digits[:8], "%Y%m%d").date()
    except Exception:
        return None


def _quote_time(quote):
    quote = quote or {}
    raw = quote.get("time", quote.get("timetag", quote.get("stime")))
    if raw is None:
        return None
    try:
        value = int(raw)
        if value > 1000000000000:
            return dt.datetime.fromtimestamp(value / 1000.0)
    except Exception:
        pass
    digits = "".join(ch for ch in str(raw) if ch.isdigit())
    if len(digits) >= 14:
        try:
            return dt.datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        except Exception:
            return None
    return None


def _quote_time_raw(quote):
    quote = quote or {}
    return quote.get("time", quote.get("timetag", quote.get("stime")))


def _time_text(value):
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if value else ""



def _console_lines(record):
    """Keep UI messages short; the JSONL retains every original field."""
    r = record
    event = r.get("event", "unknown")
    if event == "daily_download_heartbeat":
        return ["日线下载进度：接口已返回={}/{}（{}%），异常={}；当前股票={}，本次等待={}秒，总耗时={}秒。实际数据完整性将在下载后检查。".format(
            r.get("done"), r.get("total"), r.get("percent"), r.get("failed"),
            r.get("current_code") or "等待下一只", r.get("waiting_s"), r.get("elapsed_s"))]
    if event == "daily_sync_notice":
        return [r.get("message", "")]
    if event == "daily_cache_status":
        return ["{}：股票池={}只，有效={}只，缺失={}只，允许缺失={}只；{}。".format(
            r.get("phase"), r.get("total"), r.get("usable"), r.get("missing_count"),
            r.get("allowed_unavailable_codes"),
            "符合首次缓存放行条件" if r.get("within_threshold") else "超过首次缓存放行阈值")]
    if event == "daily_selection_ready":
        return ["日线准备结束：有效={}只，可进入选股={}只，排除缺失={}只；早晨补下载={}；{}。".format(
            r.get("usable"), r.get("preclose_count"), r.get("missing_count"), r.get("retried"),
            "继续选股流程" if r.get("ready") else "无可用股票，无法选股")]
    who = "{} {}".format(r.get("code", ""), r.get("name", "")).strip()
    if event == "sample_finished":
        return ["采样 {}：有效={} / {}，缺失={}，行情窗口={}".format(
            r.get("label"), r.get("valid_price"), r.get("requested"),
            r.get("pending"), r.get("quote_time_window"))]
    if event == "sample_pending_debug":
        return ["采样缺失 {}：共 {} 只，原因={}；个股明细见 JSONL".format(
            r.get("label"), r.get("pending"), r.get("reason_counts", {}))]
    if event == "precheck_result":
        return ["预检 {}：有效={}，涨停={}，缺失={}".format(
            r.get("label"), r.get("valid"), r.get("limit_up_count"), r.get("missing_count"))]
    if event == "selection_finished":
        rows = r.get("all_passed", [])
        lines = ["选股完成：通过={}，下单目标={}，数量上限外={}".format(
            r.get("passed_count", len(rows)), len(r.get("order_targets", [])),
            len(r.get("not_ordered_due_to_limit", [])))]
        lines.extend("入选：{} {}，F={}，F相对T-3={}%，昨日涨幅={}%".format(
            x.get("code"), x.get("name"), x.get("f"), x.get("f_vs_t3_pct"), x.get("t1_rise_pct")) for x in rows)
        return lines
    if event == "order_allocation":
        return ["资金分配：可用={:.2f}，总资产={:.2f}，单股比例={}%，每股预算={:.2f}".format(
            r.get("available_cash", 0), r.get("total_asset", 0),
            r.get("single_stock_asset_ratio_pct"), r.get("per_stock_budget", 0))]
    if event in ("order_intent", "passorder_called"):
        title = "准备委托" if event == "order_intent" else "下单接口已调用（待委托确认）"
        return ["{}：{}，价格={}，数量={}股，金额={:.2f}{}".format(
            title, who, r.get("price"), r.get("volume"), r.get("amount", 0),
            "，返回=" + str(r.get("passorder_result")) if event == "passorder_called" else "")]
    if event == "order_summary":
        return ["下单调用汇总：目标={}，调用={}，跳过={}，异常={}（调用数不代表委托成功数）".format(
            r.get("order_targets"), r.get("passorder_called"), r.get("skipped"), r.get("failed"))]
    if event == "order_query_probe":
        rows = r.get("relevant_orders", [])
        lines = ["委托核查 {}：账户委托={}，本次相关={}{}".format(
            r.get("phase"), r.get("returned_count"), r.get("relevant_count"),
            "，尚未查到委托，请检查交易端" if r.get("submitted_remarks") and not rows else "")]
        lines.extend("委托记录：{}，编号={}，状态={}，成交={}/{}股".format(
            x.get("code"), x.get("order_id"), x.get("status"), x.get("traded_volume"), x.get("total_volume")) for x in rows)
        return lines
    if event == "optional_filter_only_notice":
        return ["附加条件淘汰：共 {} 只（非入选股票）".format(r.get("candidate_count"))]
    if event == "optional_filter_only_candidate":
        return ["附加条件淘汰：{}，原因={}，F={}，昨日涨幅={}%".format(
            who, r.get("filtered_by"), r.get("f_price"), r.get("t1_rise_pct"))]
    if event == "account_preflight":
        return ["账户预检：账户={}，就绪={}，阻止下单={}，原因={}".format(
            r.get("model_bound_account_id"), r.get("account_ready"), r.get("order_blocked"), r.get("block_reason") or "无")]
    if event == "init":
        return ["\u7b56\u7565\u542f\u52a8\uff1a\u7248\u672c={}\uff0c\u8d26\u6237={}\uff0c\u5141\u8bb8\u4e0b\u5355={}".format(
            r.get("strategy_version"), r.get("account_id"), r.get("order_submission"))]
    if event == "startup_parameters":
        return [
            "\u542f\u52a8\u53c2\u6570\uff08\u9010\u884c\uff09\uff1a",
            "  T-1\u6da8\u5e45\u8fc7\u6ee4\uff1a{}\uff1b\u4e0a\u9650\uff1a{}%".format(r.get("t1_rise_filter_enabled"), r.get("t1_rise_max_pct")),
            "  \u524d\u4e24\u65e5\u6700\u5927\u8dcc\u5e45\u8fc7\u6ee4\uff1a{}\uff1b\u9608\u503c\uff1a{}%".format(r.get("prior_two_day_drop_enabled"), r.get("prior_two_day_max_drop_threshold_pct")),
            "  \u5355\u53ea\u8d44\u91d1\u4f7f\u7528\u6bd4\u4f8b\uff1a{}%".format(r.get("single_stock_asset_ratio_pct")),
            "  \u6700\u591a\u9009\u80a1\u6570\uff1a{}\u53ea".format(r.get("max_selected")),
            "  \u662f\u5426\u8c03\u7528\u4e0b\u5355\u63a5\u53e3\uff1a{}".format(r.get("order_submission")),
        ]
    if event == "waiting_for_prepare_time":
        return ["\u7b49\u5f85\u5f00\u59cb\uff1a\u5f53\u524d={}\uff0c\u5c06\u7b49\u5f85\u81f3 {} \u5f00\u59cb\u8fd0\u884c\uff1b\u8bf7\u4fdd\u6301 QMT\u3001\u7b56\u7565\u5b9e\u4f8b\u548c\u7535\u8111\u6b63\u5e38\u8fd0\u884c\u3001\u4e0d\u4f11\u7720\u3002".format(
            r.get("now"), r.get("prepare_at"))]
    if event in ("ranking_daily_load_chunk", "daily_download_progress", "sample_collection_scope", "order_candidate_split", "order_asset"):
        return []
    # Unknown/new events remain visible, without dumping nested stock arrays.
    values = []
    for key, value in r.items():
        if key in ("event", "logged_at", "strategy", "strategy_version", "traceback"):
            continue
        if isinstance(value, (dict, list, tuple)):
            values.append("{}={}项（详见JSONL）".format(key, len(value)))
        else:
            values.append("{}={}".format(key, str(value).replace("\n", " ")[:240]))
    return [event + ": " + " ".join(values)]


def _print_console_record(record):
    event = record.get("event", "")
    level = "ERROR" if "error" in event else "INFO"
    if event in ("sample_pending_debug", "schedule_missed") or (
            event == "order_query_probe" and record.get("submitted_remarks") and not record.get("relevant_orders")):
        level = "WARN"
    prefix = "{} [{}] ".format(record.get("logged_at", ""), level)
    try:
        for message in _console_lines(record):
            print(prefix + message)
    except Exception as exc:
        print(prefix + event + " 日志格式化失败；完整记录见JSONL：" + repr(exc))

def _log(record):
    with LOG_LOCK:
        _write_log_record(record)


def _write_log_record(record):
    record["logged_at"] = _time_text(dt.datetime.now())
    record["strategy"] = "DAqmt2_jingjia_filter"
    record["strategy_version"] = STRATEGY_VERSION
    line = json.dumps(record, ensure_ascii=False, sort_keys=True)
    _print_console_record(record)
    try:
        if not os.path.isdir(LOG_DIR):
            os.makedirs(LOG_DIR)
        path = os.path.join(LOG_DIR, "daqmt2_jingjia_" + dt.datetime.now().strftime("%Y%m%d") + ".jsonl")
        with open(path, "a", encoding="gbk") as fp:
            fp.write(line + "\n")
    except Exception as exc:
        print("[daqmt2_jingjia] file_log_error=" + repr(exc))


def _write_status(event, extra=None):
    payload = {
        "event": event,
        "strategy_version": STRATEGY_VERSION,
        "updated_at": _time_text(dt.datetime.now()),
        "pid": os.getpid(),
        "trade_date": str(STATE.get("trade_date") or ""),
        "last_stage": STATE.get("last_stage"),
        "universe_count": len(STATE.get("universe") or {}),
        "daily_count": len(STATE.get("daily") or {}),
        "preclose_count": len(STATE.get("preclose") or {}),
        "precheck1_count": len(STATE.get("precheck1") or {}),
        "precheck2_count": len(STATE.get("precheck2") or {}),
        "order_target_count": len(STATE.get("order_targets") or []),
        "submitted_order_count": len(STATE.get("submitted_orders") or {}),
        "order_callback_count": len(STATE.get("order_callbacks") or {}),
        "deal_callback_count": len(STATE.get("deal_callbacks") or []),
        "account_preflight": STATE.get("account_preflight") or {},
        "scheduled_names": list(STATE.get("scheduled_names") or []),
    }
    if extra:
        payload.update(extra)
    try:
        if not os.path.isdir(LOG_DIR):
            os.makedirs(LOG_DIR)
        path = os.path.join(LOG_DIR, "daqmt2_jingjia_last_status.json")
        with open(path, "w", encoding="gbk") as fp:
            json.dump(payload, fp, ensure_ascii=False, sort_keys=True)
    except Exception as exc:
        print("[daqmt2_jingjia] status_write_error=" + repr(exc))


def _set_stage(stage):
    STATE["last_stage"] = stage
    _write_status("stage_changed")


def _guard(label, func):
    try:
        _set_stage(label + "_started")
        return func()
    except Exception as exc:
        STATE["last_stage"] = label + "_error"
        _write_status("callback_error", {"label": label, "error": repr(exc)})
        _log({"event": "callback_error", "label": label, "error": repr(exc), "traceback": traceback.format_exc()})
        return None
    finally:
        if not str(STATE.get("last_stage") or "").endswith("_error"):
            _set_stage(label + "_finished")


def _sector_codes(ContextInfo):
    attempts = []
    for label, args in (("context_two_args", (ALL_A_SHARE_SECTOR, -1)), ("context_one_arg", (ALL_A_SHARE_SECTOR,))):
        try:
            values = ContextInfo.get_stock_list_in_sector(*args) or []
            attempts.append(label + "_count=" + str(len(values)))
            if values:
                return list(values), attempts
        except Exception as exc:
            attempts.append(label + "_error=" + repr(exc))
    try:
        values = get_stock_list_in_sector(ALL_A_SHARE_SECTOR) or []
        attempts.append("global_function_count=" + str(len(values)))
        return list(values), attempts
    except Exception as exc:
        attempts.append("global_function_error=" + repr(exc))
    return [], attempts


def _instrument_detail(ContextInfo, code):
    try:
        return ContextInfo.get_instrument_detail(code, True) or {}
    except TypeError:
        return ContextInfo.get_instrument_detail(code) or {}
    except AttributeError:
        return ContextInfo.get_instrumentdetail(code) or {}


def _load_universe(ContextInfo):
    started = time.perf_counter()
    all_codes, sources = _sector_codes(ContextInfo)
    universe = {}
    skipped = Counter()
    for raw_code in all_codes:
        code = str(raw_code).upper()
        if not _main_board_code(code):
            skipped["not_main_board"] += 1
            continue
        try:
            detail = _instrument_detail(ContextInfo, code)
        except Exception:
            skipped["detail_error"] += 1
            continue
        name = str(detail.get("InstrumentName") or detail.get("instrumentName") or "")
        if not _normal_name(name):
            skipped["not_normal_a_share"] += 1
            continue
        universe[code] = {
            "name": name,
            "price_tick": _safe_float(detail.get("PriceTick") or detail.get("priceTick"), 0.01) or 0.01,
            "detail_up_limit": _safe_float(detail.get("UpStopPrice") or detail.get("upStopPrice")),
        }
    STATE["universe"] = universe
    _log({
        "event": "universe_loaded",
        "all_a_share_count": len(all_codes),
        "main_board_normal": len(universe),
        "sources": sources,
        "skipped": dict(skipped),
        "elapsed_s": round(time.perf_counter() - started, 3),
    })


def _trade_dates(ContextInfo, start_date):
    try:
        raw_dates = ContextInfo.get_trading_dates(
            "000001.SZ",
            start_date.strftime("%Y%m%d"),
            (start_date + dt.timedelta(days=10)).strftime("%Y%m%d"),
            -1,
            "1d",
        ) or []
    except Exception as exc:
        _log({"event": "trading_dates_error", "error": repr(exc)})
        raw_dates = []
    return sorted({item for item in (_parse_date(value) for value in raw_dates) if item})


def _next_weekday(value):
    candidate = value + dt.timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += dt.timedelta(days=1)
    return candidate


def _next_trade_date(ContextInfo):
    now = dt.datetime.now()
    today = now.date()
    cutoff = dt.datetime.combine(today, FINAL_SAMPLE_UNTIL)
    if now < cutoff:
        dates = _trade_dates(ContextInfo, today)
        for value in dates:
            if value >= today:
                return value
        _log({"event": "trade_date_calendar_fallback", "mode": "same_day", "now": _time_text(now), "trade_date": str(today)})
        return today
    tomorrow = today + dt.timedelta(days=1)
    dates = _trade_dates(ContextInfo, tomorrow)
    for value in dates:
        if value >= tomorrow:
            return value
    fallback = _next_weekday(today)
    _log({"event": "trade_date_calendar_fallback", "mode": "next_weekday", "now": _time_text(now), "trade_date": str(fallback)})
    return fallback

def _chunks(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _download_daily(ContextInfo, codes, max_seconds, mode="missing_only"):
    codes = list(codes or [])
    started = time.perf_counter()
    progress = {"done": 0, "failed": 0, "current_code": "", "item_started": started}
    progress_lock = threading.Lock()
    stopped = threading.Event()

    def report():
        with progress_lock:
            snapshot = dict(progress)
        now = time.perf_counter()
        _log({"event": "daily_download_heartbeat", "done": snapshot["done"],
              "total": len(codes), "failed": snapshot["failed"],
              "percent": round(100.0 * snapshot["done"] / len(codes), 1) if codes else 0,
              "current_code": snapshot["current_code"],
              "waiting_s": round(now - snapshot["item_started"], 1) if snapshot["current_code"] else 0,
              "elapsed_s": round(now - started, 1)})

    def heartbeat():
        # This worker only reports progress; QMT data APIs stay on the callback thread.
        while not stopped.wait(DOWNLOAD_PROGRESS_INTERVAL_SECONDS):
            report()

    reporter = threading.Thread(target=heartbeat, name="daily-download-progress")
    reporter.daemon = True
    report()
    reporter.start()
    try:
        return _download_daily_impl(ContextInfo, codes, max_seconds, mode, progress, progress_lock)
    finally:
        stopped.set()
        reporter.join(timeout=1.0)
        report()


def _download_daily_impl(ContextInfo, codes, max_seconds, mode, progress, progress_lock):
    codes = list(codes or [])
    trade_date = STATE["trade_date"]
    if not codes or trade_date is None:
        _log({"event": "daily_download_skipped", "reason": "no_missing_codes_or_trade_date"})
        return {"attempted": 0, "ok": 0, "failed": 0, "remaining": 0, "budget_exhausted": False}
    start_date = (trade_date - dt.timedelta(days=DAILY_LOOKBACK_CALENDAR_DAYS)).strftime("%Y%m%d")
    end_date = trade_date.strftime("%Y%m%d")
    started = time.perf_counter()
    deadline = started + max(0.0, _safe_float(max_seconds)) if max_seconds is not None else None
    ok_count = 0
    fail_count = 0
    attempted = 0
    _log({"event": "daily_download_started", "stocks": len(codes), "start": start_date, "end": end_date, "max_seconds": max_seconds, "mode": mode})
    for code in codes:
        if deadline is not None and attempted and time.perf_counter() >= deadline:
            break
        attempted += 1
        with progress_lock:
            progress.update(current_code=code, item_started=time.perf_counter())
        try:
            download_history_data(code, "1d", start_date, end_date)
            ok_count += 1
        except Exception as exc:
            fail_count += 1
            if fail_count <= 10:
                _log({"event": "daily_download_item_error", "code": code, "error": repr(exc)})
        with progress_lock:
            progress.update(done=attempted, failed=fail_count, current_code="")
        if attempted % 100 == 0 or attempted == len(codes):
            _log({"event": "daily_download_progress", "done": attempted, "total": len(codes), "ok": ok_count, "failed": fail_count, "elapsed_s": round(time.perf_counter() - started, 3)})
    elapsed = round(time.perf_counter() - started, 3)
    remaining = len(codes) - attempted
    budget_exhausted = remaining > 0
    _log({"event": "daily_download_finished", "stocks": len(codes), "attempted": attempted, "ok": ok_count, "failed": fail_count, "remaining": remaining, "budget_exhausted": budget_exhausted, "elapsed_s": elapsed})
    return {"attempted": attempted, "ok": ok_count, "failed": fail_count, "remaining": remaining, "budget_exhausted": budget_exhausted}

def _row_value(row, name):
    try:
        return _safe_float(row.get(name))
    except Exception:
        try:
            return _safe_float(row[name])
        except Exception:
            return 0.0


def _daily_cache_missing_codes(ContextInfo):
    trade_date = STATE.get("trade_date")
    expected_dates = [value for value in _trade_dates(ContextInfo, trade_date - dt.timedelta(days=10)) if value < trade_date][-3:] if trade_date else []
    missing = []
    for code in STATE.get("universe") or {}:
        item = (STATE.get("daily") or {}).get(code)
        actual_dates = [item.get("rank_base_date"), item.get("prior_t2_date"), item.get("prior_t1_date")] if item else []
        if len(expected_dates) != 3 or actual_dates != expected_dates:
            missing.append(code)
    return missing, expected_dates

def _prepare_daily(ContextInfo):
    trade_date = STATE["trade_date"]
    codes = list(STATE["universe"])
    if not codes or trade_date is None:
        _log({"event": "daily_prepare_skipped", "reason": "missing_universe_or_trade_date"})
        return
    start_date = (trade_date - dt.timedelta(days=DAILY_LOOKBACK_CALENDAR_DAYS)).strftime("%Y%m%d")
    end_date = trade_date.strftime("%Y%m%d")
    daily = {}
    started = time.perf_counter()
    _log({"event": "ranking_daily_prepare_started", "stocks": len(codes), "start": start_date, "end": end_date})
    for chunk_index, part in enumerate(_chunks(codes, DAILY_CHUNK_SIZE), start=1):
        data = ContextInfo.get_market_data_ex(
            ["low", "close"], part, period="1d", start_time=start_date, end_time=end_date,
            count=-1, dividend_type="none", fill_data=False, subscribe=False,
        ) or {}
        for code in part:
            table = data.get(code)
            if table is None or not hasattr(table, "iterrows"):
                continue
            rows = []
            for row_index, row in table.iterrows():
                bar_date = _parse_date(row_index)
                low = _row_value(row, "low")
                close = _row_value(row, "close")
                if bar_date and bar_date < trade_date and close > 0:
                    rows.append((bar_date, low, close))
            rows.sort(key=lambda item: item[0])
            if len(rows) < 3:
                continue
            base_date, base_low, base_close = rows[-3]
            t2_date, t2_low, t2_close = rows[-2]
            t1_date, t1_low, t1_close = rows[-1]
            tick = STATE["universe"][code]["price_tick"]
            daily[code] = {
                "rank_base_date": base_date,
                "rank_base_close": base_close,
                "prior_t2_date": t2_date,
                "prior_t2_max_drop_pct": ((t2_low / base_close - 1) * 100) if t2_low > 0 else None,
                "prior_t1_date": t1_date,
                "prior_t1_max_drop_pct": ((t1_low / t2_close - 1) * 100) if t1_low > 0 and t2_close > 0 else None,
                "prior_t1_rise_pct": ((t1_close / t2_close - 1) * 100) if t2_close > 0 else None,
                "prior_t1_limit_up": abs(t1_close - _round_to_tick(t2_close * 1.10, tick)) <= tick / 2.0 + 1e-9,
            }
        _log({"event": "ranking_daily_load_chunk", "chunk": chunk_index, "stocks": len(part), "returned": len(data), "usable_so_far": len(daily)})
    expected_dates = [value for value in _trade_dates(ContextInfo, trade_date - dt.timedelta(days=10)) if value < trade_date][-3:]
    latest_expected = expected_dates[-1] if expected_dates else None
    missing_latest = [code for code, item in daily.items() if latest_expected and item["prior_t1_date"] != latest_expected]
    missing_recent = [
        code for code, item in daily.items()
        if expected_dates and [item["rank_base_date"], item["prior_t2_date"], item["prior_t1_date"]] != expected_dates
    ]
    _log({
        "event": "daily_ready_check",
        "expected_recent_3": [value.strftime("%Y-%m-%d") for value in expected_dates],
        "total": len(codes),
        "usable": len(daily),
        "latest_ready": len(daily) - len(missing_latest),
        "recent3_ready": len(daily) - len(missing_recent),
        "missing_latest_count": len(missing_latest),
        "missing_recent3_count": len(missing_recent),
        "missing_latest_examples": missing_latest[:20],
        "missing_recent3_examples": missing_recent[:20],
        "rank_base_date_counts": dict(Counter(item["rank_base_date"].strftime("%Y-%m-%d") for item in daily.values())),
        "prior_t2_date_counts": dict(Counter(item["prior_t2_date"].strftime("%Y-%m-%d") for item in daily.values())),
        "prior_t1_date_counts": dict(Counter(item["prior_t1_date"].strftime("%Y-%m-%d") for item in daily.values())),
    })
    STATE["daily"] = daily
    _log({"event": "ranking_base_close_prepared", "usable": len(daily), "skipped": len(codes) - len(daily), "elapsed_s": round(time.perf_counter() - started, 3)})


def _prepare_preclose(ContextInfo):
    codes = list(STATE["daily"])
    started = time.perf_counter()
    ticks = ContextInfo.get_full_tick(codes) or {}
    preclose = {}
    up_limit = {}
    for code in codes:
        quote = ticks.get(code) or {}
        close = _safe_float(quote.get("lastClose"))
        if close <= 0:
            continue
        meta = STATE["universe"][code]
        preclose[code] = close
        # All retained stocks are normal main-board A shares, so their limit is 10%.
        # Recalculate here because the strategy may have started the prior evening.
        up_limit[code] = _round_to_tick(close * 1.10, meta["price_tick"])
    STATE["preclose"] = preclose
    STATE["up_limit"] = up_limit
    STATE["precheck1"] = {}
    STATE["precheck2"] = {}
    # Store samples by their fixed A-F label. A missing quote leaves only its own CSV field empty.
    STATE["samples"] = {code: {} for code in preclose}
    _log({"event": "preclose_snapshot", "requested": len(codes), "returned": len(ticks), "usable": len(preclose), "elapsed_s": round(time.perf_counter() - started, 3)})


def _collect_sample(ContextInfo, label, target_time, min_time, max_time, deadline_time, precheck_key=None, codes=None):
    trade_date = STATE["trade_date"]
    if trade_date is None:
        _log({"event": "sample_skipped", "label": label, "reason": "preclose_not_ready"})
        return
    codes = list(codes) if codes is not None else list(STATE["preclose"])
    if not codes:
        _log({"event": "sample_skipped", "label": label, "reason": "no_candidate_codes"})
        return
    minimum = dt.datetime.combine(trade_date, min_time)
    maximum = dt.datetime.combine(trade_date, max_time)
    deadline = dt.datetime.combine(trade_date, deadline_time)
    pending = set(codes)
    attempts = 0
    valid = 0
    last_returned = 0
    pending_reasons = {}
    pending_debug = {}
    started = time.perf_counter()
    while pending:
        attempts += 1
        request_codes = list(pending)
        ticks = ContextInfo.get_full_tick(request_codes) or {}
        last_returned = len(ticks)
        scan_time = dt.datetime.now()
        for code in list(pending):
            quote = ticks.get(code) or {}
            price = _safe_float(quote.get("lastPrice"))
            quote_time = _quote_time(quote)
            if not quote:
                reason = "missing_native_full_tick"
            elif price <= 0:
                reason = "last_price_not_positive"
            elif quote_time is None:
                reason = "quote_time_missing_or_unparseable"
            elif quote_time < minimum:
                reason = "quote_time_before_window"
            elif quote_time > maximum:
                reason = "quote_time_after_window"
            else:
                reason = ""
            if reason:
                pending_reasons[code] = reason
                pending_debug[code] = {
                    "code": code,
                    "name": STATE["universe"].get(code, {}).get("name", ""),
                    "reason": reason,
                    "lastPrice": price,
                    "lastClose": _safe_float(quote.get("lastClose")),
                    "quote_time": _time_text(quote_time),
                    "quote_time_raw": str(_quote_time_raw(quote) or ""),
                }
                continue
            sample = {
                "price": price,
                "quote_time": quote_time,
                "scan_time": scan_time,
                "price_source": "native_full_tick.lastPrice",
            }
            if precheck_key:
                STATE[precheck_key][code] = sample
            else:
                STATE["samples"].setdefault(code, {})[label] = sample
            pending.discard(code)
            pending_reasons.pop(code, None)
            pending_debug.pop(code, None)
            valid += 1
        if dt.datetime.now() >= deadline:
            break
        if pending:
            time.sleep(FRESH_RETRY_SECONDS)
    _log({
        "event": "sample_finished",
        "label": label,
        "target": target_time.strftime("%H:%M:%S"),
        "quote_time_window": min_time.strftime("%H:%M:%S") + "~" + max_time.strftime("%H:%M:%S"),
        "requested": len(codes),
        "returned": last_returned,
        "valid_price": valid,
        "pending": len(pending),
        "attempts": attempts,
        "fresh_required": True,
        "collect_until": _time_text(deadline),
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "pending_reason_counts": dict(Counter(pending_reasons.values())),
    })
    if pending:
        _log({
            "event": "sample_pending_debug",
            "label": label,
            "pending": len(pending),
            "quote_time_window": min_time.strftime("%H:%M:%S") + "~" + max_time.strftime("%H:%M:%S"),
            "reason_counts": dict(Counter(pending_reasons.values())),
            "examples": [pending_debug[code] for code in sorted(pending_debug)[:20]],
        })


def _sample_is_limit_up(code, sample):
    if not sample:
        return False
    meta = STATE["universe"].get(code) or {}
    up_limit = _safe_float(STATE["up_limit"].get(code), 0.0)
    tick = _safe_float(meta.get("price_tick"), 0.01) or 0.01
    return up_limit > 0 and abs(_safe_float(sample.get("price"), 0.0) - up_limit) <= tick / 2.0 + 1e-9


def _dual_precheck_pass_codes():
    return [
        code for code in STATE["preclose"]
        # Precheck 1 missing data is explicitly accepted; a returned quote
        # must still equal the limit-up price.
        if (
            not STATE["precheck1"].get(code)
            or _sample_is_limit_up(code, STATE["precheck1"].get(code))
        )
        and _sample_is_limit_up(code, STATE["precheck2"].get(code))
    ]


def _log_precheck_summary(label, precheck_key):
    samples = STATE.get(precheck_key) or {}
    missing = []
    limit_up = []
    for code in STATE["preclose"]:
        sample = samples.get(code)
        if not sample:
            missing.append(code)
            continue
        if _sample_is_limit_up(code, sample):
            limit_up.append({
                "code": code,
                "name": (STATE["universe"].get(code) or {}).get("name", ""),
                "price": round(_safe_float(sample.get("price")), 4),
                "up_limit": round(_safe_float(STATE["up_limit"].get(code)), 4),
                "quote_time": _time_text(sample.get("quote_time")),
                "scan_time": _time_text(sample.get("scan_time")),
                "source": sample.get("price_source", "native_full_tick.lastPrice"),
            })
    _log({
        "event": "precheck_result",
        "label": label,
        "universe": len(STATE["preclose"]),
        "valid": len(samples),
        "limit_up_count": len(limit_up),
        "not_limit_up_count": len(samples) - len(limit_up),
        "missing_count": len(missing),
        "missing_accepted_for_selection": label == "precheck1",
        "missing_examples": sorted(missing)[:20],
        "limit_up": limit_up,
    })


def _write_result_csv(rows, order_target_rows, not_ordered_rows):
    date_text = STATE["trade_date"].strftime("%Y%m%d")
    fields = [
        "code", "name", "passed", "reason", "candidate_rank", "order_rank", "order_status",
        "preclose", "up_limit", "price_tick", "preclose_source",
        "precheck1_price", "precheck1_rise_pct", "precheck1_quote_time", "precheck1_scan_time", "precheck1_source", "precheck1_limit_up", "precheck1_missing_accepted",
        "precheck2_price", "precheck2_rise_pct", "precheck2_quote_time", "precheck2_scan_time", "precheck2_source", "precheck2_limit_up",
        "a", "a_rise_pct", "a_quote_time", "a_scan_time", "a_source",
        "b", "b_rise_pct", "b_quote_time", "b_scan_time", "b_source",
        "c", "c_rise_pct", "c_quote_time", "c_scan_time", "c_source",
        "d", "d_rise_pct", "d_quote_time", "d_scan_time", "d_source",
        "e", "e_rise_pct", "e_quote_time", "e_scan_time", "e_source",
        "f", "f_rise_pct", "f_quote_time", "f_scan_time", "f_source",
        "rank_base_date", "rank_base_close", "f_vs_t3_pct",
        "prior_t2_date", "prior_t2_max_drop_pct", "prior_t1_date", "t1_max_drop_pct", "t1_rise_pct", "t1_limit_up",
    ]
    for suffix, values in (
        ("all", rows),
        ("selected", order_target_rows),
        ("not_ordered_due_to_limit", not_ordered_rows),
    ):
        try:
            path = os.path.join(LOG_DIR, "daqmt2_jingjia_" + suffix + "_" + date_text + ".csv")
            with open(path, "w", newline="", encoding="gbk") as fp:
                writer = csv.DictWriter(fp, fieldnames=fields)
                writer.writeheader()
                writer.writerows(values)
            _log({"event": "result_csv_written", "kind": suffix, "path": path, "rows": len(values)})
        except Exception as exc:
            _log({"event": "result_csv_error", "kind": suffix, "error": repr(exc)})


def _evaluate():
    rows = []
    passed = []
    rejected = Counter()
    for code, meta in STATE["universe"].items():
        daily = STATE["daily"].get(code)
        preclose = STATE["preclose"].get(code, 0.0)
        up_limit = STATE["up_limit"].get(code, 0.0)
        precheck1 = STATE["precheck1"].get(code)
        precheck2 = STATE["precheck2"].get(code)
        samples = STATE["samples"].get(code, {})
        row = {
            "code": code, "name": meta["name"], "passed": False, "reason": "", "order_rank": "",
            "preclose": round(preclose, 4) if preclose > 0 else "",
            "up_limit": round(up_limit, 4) if up_limit > 0 else "",
            "price_tick": meta["price_tick"], "preclose_source": "native_full_tick.lastClose" if preclose > 0 else "",
            "precheck1_price": "", "precheck1_rise_pct": "", "precheck1_quote_time": "", "precheck1_scan_time": "", "precheck1_source": "", "precheck1_limit_up": "", "precheck1_missing_accepted": not bool(precheck1),
            "precheck2_price": "", "precheck2_rise_pct": "", "precheck2_quote_time": "", "precheck2_scan_time": "", "precheck2_source": "", "precheck2_limit_up": "",
            "rank_base_date": "", "rank_base_close": "", "f_vs_t3_pct": "",
            "prior_t2_date": "", "prior_t2_max_drop_pct": "", "prior_t1_date": "", "t1_max_drop_pct": "", "t1_rise_pct": "", "t1_limit_up": "",
        }
        for sample_label, _target, _minimum, _maximum in SAMPLE_PLAN:
            row.update({
                sample_label: "", sample_label + "_rise_pct": "",
                sample_label + "_quote_time": "", sample_label + "_scan_time": "", sample_label + "_source": "",
            })
        if daily:
            row.update({
                "rank_base_date": daily["rank_base_date"].strftime("%Y-%m-%d"),
                "rank_base_close": round(daily["rank_base_close"], 4),
                "prior_t2_date": daily["prior_t2_date"].strftime("%Y-%m-%d"),
                "prior_t2_max_drop_pct": round(daily["prior_t2_max_drop_pct"], 4) if daily["prior_t2_max_drop_pct"] is not None else "",
                "prior_t1_date": daily["prior_t1_date"].strftime("%Y-%m-%d"),
                "t1_max_drop_pct": round(daily["prior_t1_max_drop_pct"], 4) if daily["prior_t1_max_drop_pct"] is not None else "",
                "t1_rise_pct": round(daily["prior_t1_rise_pct"], 4) if daily["prior_t1_rise_pct"] is not None else "",
                "t1_limit_up": bool(daily["prior_t1_limit_up"]),
            })
        if precheck1:
            row.update({
                "precheck1_price": round(precheck1["price"], 4),
                "precheck1_rise_pct": round((precheck1["price"] / preclose - 1) * 100, 4) if preclose > 0 else "",
                "precheck1_quote_time": _time_text(precheck1["quote_time"]),
                "precheck1_scan_time": _time_text(precheck1["scan_time"]),
                "precheck1_source": precheck1.get("price_source", "native_full_tick.lastPrice"),
                "precheck1_limit_up": _sample_is_limit_up(code, precheck1),
            })
        if precheck2:
            row.update({
                "precheck2_price": round(precheck2["price"], 4),
                "precheck2_rise_pct": round((precheck2["price"] / preclose - 1) * 100, 4) if preclose > 0 else "",
                "precheck2_quote_time": _time_text(precheck2["quote_time"]),
                "precheck2_scan_time": _time_text(precheck2["scan_time"]),
                "precheck2_source": precheck2.get("price_source", "native_full_tick.lastPrice"),
                "precheck2_limit_up": _sample_is_limit_up(code, precheck2),
            })
        for sample_label, _target, _minimum, _maximum in SAMPLE_PLAN:
            sample = samples.get(sample_label)
            if not sample:
                continue
            row.update({
                sample_label: round(sample["price"], 4),
                sample_label + "_rise_pct": round((sample["price"] / preclose - 1) * 100, 4) if preclose > 0 else "",
                sample_label + "_quote_time": _time_text(sample["quote_time"]),
                sample_label + "_scan_time": _time_text(sample["scan_time"]),
                sample_label + "_source": sample.get("price_source", "native_full_tick.lastPrice"),
            })
        if not daily or preclose <= 0 or up_limit <= 0:
            row["reason"] = "missing_data"
            rows.append(row)
            rejected["missing_data"] += 1
            continue
        if not precheck2:
            row["reason"] = "missing_precheck2_sample"
            rows.append(row)
            rejected["missing_precheck2_sample"] += 1
            continue
        failed_prechecks = []
        if precheck1 and not _sample_is_limit_up(code, precheck1):
            failed_prechecks.append("precheck1_not_limit_up")
        if not _sample_is_limit_up(code, precheck2):
            failed_prechecks.append("precheck2_not_limit_up")
        if failed_prechecks:
            row["reason"] = ";".join(failed_prechecks)
            rows.append(row)
            for item in failed_prechecks:
                rejected[item] += 1
            continue
        if len(samples) != 6:
            row["reason"] = "missing_samples:" + str(len(samples)) + "/6"
            rows.append(row)
            rejected["missing_samples"] += 1
            continue
        prices = [samples[sample_label]["price"] for sample_label, _target, _minimum, _maximum in SAMPLE_PLAN]
        a, b, c, d, e, f = prices
        tolerance = meta["price_tick"] / 2.0 + 1e-9
        f_rise_pct = (f / preclose - 1) * 100
        rank_pct = (f / daily["rank_base_close"] - 1) * 100
        checks = {
            "a_rise_not_above_5": ((a / preclose - 1) * 100) > MIN_A_RISE_PCT,
            "monotonic_non_increasing": all(prices[index] + tolerance >= prices[index + 1] for index in range(5)),
            "f_rise_outside_-1_to_3": FINAL_MIN_RISE_PCT <= f_rise_pct <= FINAL_MAX_RISE_PCT,
            "f_price_below_minimum": (not ENABLE_F_PRICE_FILTER) or f >= F_PRICE_MIN,
            "f_rank_base_rise_outside_-10_to_10": RANK_BASE_MIN_RISE_PCT <= rank_pct <= RANK_BASE_MAX_RISE_PCT,
            "last_drop_not_less_than_limit_to_e": (e - f) < (up_limit - e),
            "prior_t1_rise_above_max": (not ENABLE_T1_RISE_FILTER) or daily["prior_t1_rise_pct"] <= T1_RISE_MAX_PCT,
        }
        if ENABLE_PRIOR_TWO_DAY_MAX_DROP_FILTER:
            checks["prior_t2_max_drop_below_threshold"] = daily["prior_t2_max_drop_pct"] >= PRIOR_TWO_DAY_MAX_DROP_THRESHOLD_PCT
            checks["prior_t1_max_drop_below_threshold"] = daily["prior_t1_max_drop_pct"] >= PRIOR_TWO_DAY_MAX_DROP_THRESHOLD_PCT
        failed = [name for name, ok in checks.items() if not ok]
        row.update({
            "f_rise_pct": round(f_rise_pct, 4),
            "f_vs_t3_pct": round(rank_pct, 4),
        })
        if failed:
            row["reason"] = ";".join(failed)
            for item in failed:
                rejected[item] += 1
            rows.append(row)
            continue
        row["passed"] = True
        row["reason"] = "PASS"
        rows.append(row)
        passed.append(row)
    passed.sort(key=lambda item: (1 if item["t1_limit_up"] else 0, item["f_vs_t3_pct"], item["code"]))
    order_targets = passed[:MAX_SELECTED_STOCKS]
    not_ordered_rows = passed[MAX_SELECTED_STOCKS:]
    for index, row in enumerate(passed, start=1):
        row["candidate_rank"] = index
        row["order_status"] = "order_target" if index <= MAX_SELECTED_STOCKS else "not_ordered_due_to_max_selected"
    for index, row in enumerate(order_targets, start=1):
        row["order_rank"] = index
    STATE["selection_rows"] = rows
    STATE["passed_rows"] = passed
    STATE["order_targets"] = order_targets
    STATE["not_ordered_rows"] = not_ordered_rows
    _write_result_csv(rows, order_targets, not_ordered_rows)
    _log({
        "event": "order_candidate_split",
        "passed_count": len(passed),
        "max_selected_stocks": MAX_SELECTED_STOCKS,
        "order_target_count": len(order_targets),
        "order_targets": order_targets,
        "not_ordered_due_to_limit_count": len(not_ordered_rows),
        "not_ordered_due_to_limit": not_ordered_rows,
    })
    _log({
        "event": "selection_finished",
        "passed_count": len(passed),
        "order_targets": order_targets,
        "not_ordered_due_to_limit": not_ordered_rows,
        "all_passed": passed,
        "rejected_reason_counts": dict(rejected),
        "rule": {
            "precheck1_limit_up_or_missing": True,
            "precheck2_limit_up": True,
            "a_rise_gt": MIN_A_RISE_PCT,
            "f_price_filter_enabled": ENABLE_F_PRICE_FILTER,
            "f_price_min": F_PRICE_MIN,
            "t1_rise_filter_enabled": ENABLE_T1_RISE_FILTER,
            "t1_rise_max_pct": T1_RISE_MAX_PCT,
            "prior_two_day_drop_enabled": ENABLE_PRIOR_TWO_DAY_MAX_DROP_FILTER,
            "sort": "t1_not_limit_up_first_then_f_vs_t3_pct_ascending",
            "max_selected": MAX_SELECTED_STOCKS,
            "order_submission": ORDER_SUBMISSION_ENABLED,
        },
    })


def _query_trade_details(detail_type, strategy_name=None):
    account_id, account_type = _runtime_account()
    if not account_id or not account_type:
        raise RuntimeError("model_trading_account_or_type_unavailable")
    query_args = (account_id, account_type, detail_type)
    if strategy_name:
        try:
            return get_trade_detail_data(*(query_args + (strategy_name,))) or []
        except TypeError:
            pass
    return get_trade_detail_data(*query_args) or []


def _log_optional_filter_only_candidates():
    """Match MiniQMT's 09:27 notice for stocks rejected only by optional filters."""
    enabled_reasons = set()
    enabled_filters = []
    if ENABLE_F_PRICE_FILTER:
        enabled_reasons.add("f_price_below_minimum")
        enabled_filters.append("f_price>=" + str(F_PRICE_MIN))
    if ENABLE_T1_RISE_FILTER:
        enabled_reasons.add("prior_t1_rise_above_max")
        enabled_filters.append("t1_rise<=" + str(T1_RISE_MAX_PCT))
    if ENABLE_PRIOR_TWO_DAY_MAX_DROP_FILTER:
        enabled_reasons.update(("prior_t2_max_drop_below_threshold", "prior_t1_max_drop_below_threshold"))
        enabled_filters.append("prior_t2_t1_max_drop>=" + str(PRIOR_TWO_DAY_MAX_DROP_THRESHOLD_PCT))

    candidates = []
    if enabled_reasons:
        for row in STATE.get("selection_rows") or []:
            if row.get("passed") or not row.get("reason") or row.get("reason") == "PASS":
                continue
            failed_reasons = set(str(row["reason"]).split(";"))
            if failed_reasons and failed_reasons.issubset(enabled_reasons):
                candidates.append(row)
    candidates.sort(key=lambda item: item["code"])
    _log({
        "event": "optional_filter_only_notice",
        "enabled_filters": enabled_filters,
        "candidate_count": len(candidates),
    })
    for row in candidates:
        _log({
            "event": "optional_filter_only_candidate",
            "code": row["code"],
            "name": row["name"],
            "filtered_by": row["reason"],
            "f_price": row["f"],
            "t1_rise_pct": row["t1_rise_pct"],
            "prior_t2_max_drop_pct": row["prior_t2_max_drop_pct"],
            "t1_max_drop_pct": row["t1_max_drop_pct"],
        })


def _query_account_asset():
    rows = []
    errors = []
    for detail_type in ("ACCOUNT", "account", "ASSET", "asset"):
        try:
            rows = _query_trade_details(detail_type)
            if rows:
                break
        except Exception as exc:
            errors.append(detail_type + ":" + repr(exc))
    if not rows:
        account_id, account_type = _runtime_account()
        _log({
            "event": "order_asset_empty",
            "account_id": account_id,
            "account_type": account_type,
            "errors": errors,
        })
        return 0.0, 0.0
    row = rows[0]
    available_cash = _safe_float(_first_attr(
        row,
        ("m_dAvailable", "m_dAvailableCash", "m_dEnableBalance", "m_dAvailableBalance", "available_cash", "cash"),
        0.0,
    ))
    total_asset = _safe_float(_first_attr(
        row,
        ("m_dBalance", "m_dAsset", "m_dTotalAsset", "m_dAssureAsset", "total_asset", "asset"),
        0.0,
    ))
    _log({
        "event": "order_asset",
        "account_id": _runtime_account()[0],
        "account_type": _runtime_account()[1],
        "available_cash": round(available_cash, 2),
        "total_asset": round(total_asset, 2),
    })
    return available_cash, total_asset


def _order_remark(code):
    return STRATEGY_NAME + "_" + dt.date.today().strftime("%Y%m%d") + "_" + code


def _order_code(order):
    code = str(_first_attr(order, ("m_strStockCode", "m_strInstrumentID", "stock_code", "code"), "") or "")
    market = str(_first_attr(order, ("m_strExchangeID", "market", "exchange"), "") or "")
    return code if "." in code or not market else code + "." + market


def _order_probe_record(order):
    return {
        "code": _order_code(order),
        "remark": str(_first_attr(order, ("m_strRemark", "m_strUserOrderId", "remark", "user_order_id"), "") or ""),
        "order_id": str(_first_attr(order, ("m_strOrderSysID", "m_nOrderID", "order_id"), "") or ""),
        "status": str(_first_attr(order, ("m_nOrderStatus", "order_status", "m_strStatus"), "") or ""),
        "price": _safe_float(_first_attr(order, ("m_dPrice", "m_dOrderPrice", "price"), 0.0)),
        "total_volume": _safe_int(_first_attr(order, ("m_nVolumeTotalOriginal", "m_nOrderVolume", "order_volume"), 0)),
        "traded_volume": _safe_int(_first_attr(order, ("m_nVolumeTraded", "traded_volume"), 0)),
    }


def _log_unfiltered_order_probe(phase):
    """Query the whole account without a strategy-name filter for diagnostics."""
    try:
        orders = _query_trade_details("ORDER")
    except Exception as exc:
        _log({"event": "order_query_probe_error", "phase": phase, "error": repr(exc), "traceback": traceback.format_exc()})
        return
    submitted = STATE.get("submitted_orders") or {}
    target_codes = set(record.get("code") for record in submitted.values())
    expected_remarks = set(submitted.keys())
    relevant = []
    for order in orders:
        record = _order_probe_record(order)
        if record["code"] in target_codes or record["remark"] in expected_remarks:
            relevant.append(record)
    _log({
        "event": "order_query_probe",
        "phase": phase,
        "account_id": _runtime_account()[0],
        "account_type": _runtime_account()[1],
        "query_mode": "unfiltered_account_orders",
        "returned_count": len(orders),
        "submitted_remarks": sorted(expected_remarks),
        "target_codes": sorted(target_codes),
        "relevant_count": len(relevant),
        "relevant_orders": relevant,
    })


def _submit_orders(ContextInfo):
    if not ORDER_SUBMISSION_ENABLED:
        _log({"event": "order_skipped", "reason": "order_submission_disabled"})
        return
    if STATE["submitted_orders"]:
        _log({"event": "order_skipped", "reason": "orders_already_signaled"})
        return
    if not STATE["order_targets"]:
        _evaluate()
    targets = STATE["order_targets"]
    if not targets:
        _log({"event": "order_skipped", "reason": "no_order_targets"})
        return

    preflight = STATE.get("account_preflight") or {}
    if preflight.get("order_blocked"):
        _log({
            "event": "order_skipped",
            "reason": preflight.get("block_reason") or "account_preflight_failed",
            "model_bound_account_id": preflight.get("model_bound_account_id"),
            "model_bound_account_type": preflight.get("model_bound_account_type"),
        })
        return

    available_cash, total_asset = _query_account_asset()
    if available_cash <= 0 or total_asset <= 0:
        _log({
            "event": "order_skipped",
            "reason": "invalid_account_asset",
            "available_cash": available_cash,
            "total_asset": total_asset,
        })
        return

    stock_count = len(targets)
    single_stock_asset_cap = total_asset * SINGLE_STOCK_ASSET_RATIO
    average_available_cash = available_cash / stock_count
    per_stock_budget = min(single_stock_asset_cap, average_available_cash)
    _log({
        "event": "order_allocation",
        "account_id": _runtime_account()[0],
        "account_type": _runtime_account()[1],
        "targets": [row["code"] for row in targets],
        "stock_count": stock_count,
        "available_cash": round(available_cash, 2),
        "total_asset": round(total_asset, 2),
        "single_stock_asset_cap": round(single_stock_asset_cap, 2),
        "single_stock_asset_ratio_pct": SINGLE_STOCK_ASSET_RATIO * 100,
        "average_available_cash": round(average_available_cash, 2),
        "per_stock_budget": round(per_stock_budget, 2),
        "order_price_rise_pct": ORDER_PRICE_RISE_PCT,
    })

    signaled = 0
    skipped = 0
    failed = 0
    for rank, row in enumerate(targets, start=1):
        code = row["code"]
        meta = STATE["universe"].get(code) or {}
        preclose = _safe_float(STATE["preclose"].get(code), 0.0)
        price_tick = _safe_float(meta.get("price_tick"), 0.01) or 0.01
        order_price = _round_to_tick(preclose * (1 + ORDER_PRICE_RISE_PCT / 100.0), price_tick)
        volume = int(per_stock_budget / order_price / LOT_SIZE) * LOT_SIZE if order_price > 0 else 0
        if volume < LOT_SIZE:
            skipped += 1
            _log({
                "event": "order_skipped",
                "reason": "budget_below_one_lot",
                "rank": rank,
                "code": code,
                "name": row["name"],
                "budget": round(per_stock_budget, 2),
                "order_price": order_price,
                "volume": volume,
            })
            continue

        remark = _order_remark(code)
        amount = order_price * volume
        record = {
            "code": code,
            "name": row["name"],
            "rank": rank,
            "remark": remark,
            "price": order_price,
            "volume": volume,
            "amount": amount,
            "budget": per_stock_budget,
            "f_vs_t3_pct": row.get("f_vs_t3_pct"),
            "t1_limit_up": row.get("t1_limit_up"),
        }
        _log(dict(record, event="order_intent"))
        try:
            passorder_result = passorder(
                23,
                1101,
                _runtime_account()[0],
                code,
                11,
                order_price,
                volume,
                STRATEGY_NAME,
                2,
                remark,
                ContextInfo,
            )
            STATE["submitted_orders"][remark] = record
            signaled += 1
            _log(dict(
                record,
                event="passorder_called",
                passorder_result=repr(passorder_result),
                passorder_result_type=type(passorder_result).__name__,
            ))
        except Exception as exc:
            failed += 1
            _log(dict(record, event="passorder_error", error=repr(exc), traceback=traceback.format_exc()))
    _log({
        "event": "order_summary",
        "order_targets": len(targets),
        "passorder_called": signaled,
        "skipped": skipped,
        "failed": failed,
    })


def _cancel_unfilled_orders(ContextInfo):
    if not STATE["submitted_orders"]:
        _log({"event": "final_cancel_skipped", "reason": "no_strategy_orders"})
        return
    try:
        orders = _query_trade_details("ORDER", STRATEGY_NAME)
    except Exception as exc:
        _log({"event": "final_cancel_error", "step": "query_orders", "error": repr(exc), "traceback": traceback.format_exc()})
        return

    canceled = 0
    skipped = 0
    failed = 0
    for order in orders:
        remark = str(_first_attr(order, ("m_strRemark", "m_strUserOrderId", "remark", "user_order_id"), "") or "")
        if remark not in STATE["submitted_orders"]:
            continue
        record = STATE["submitted_orders"][remark]
        order_id = str(_first_attr(order, ("m_strOrderSysID", "m_nOrderID", "order_id"), "") or "")
        total_volume = _safe_int(_first_attr(order, ("m_nVolumeTotalOriginal", "m_nOrderVolume", "order_volume"), record["volume"]))
        traded_volume = _safe_int(_first_attr(order, ("m_nVolumeTraded", "traded_volume"), 0))
        status = _first_attr(order, ("m_nOrderStatus", "order_status", "m_strStatus"), "")
        if total_volume > 0 and traded_volume >= total_volume:
            skipped += 1
            _log(dict(record, event="final_cancel_skipped", reason="fully_traded", order_id=order_id, status=status, traded_volume=traded_volume, total_volume=total_volume))
            continue
        if not order_id:
            skipped += 1
            _log(dict(record, event="final_cancel_skipped", reason="missing_order_id", status=status))
            continue
        try:
            cancelable = can_cancel_order(order_id, _runtime_account()[0], _runtime_account()[1])
        except Exception:
            cancelable = True
        if not cancelable:
            skipped += 1
            _log(dict(record, event="final_cancel_skipped", reason="not_cancelable", order_id=order_id, status=status))
            continue
        try:
            result = cancel(order_id, _runtime_account()[0], _runtime_account()[1], ContextInfo)
            success = bool(result)
            if success:
                canceled += 1
            else:
                failed += 1
            _log(dict(record, event="final_cancel", order_id=order_id, status=status, traded_volume=traded_volume, total_volume=total_volume, result=result, success=success))
        except Exception as exc:
            failed += 1
            _log(dict(record, event="final_cancel_error", order_id=order_id, error=repr(exc), traceback=traceback.format_exc()))
    _log({
        "event": "final_cancel_summary",
        "submitted": len(STATE["submitted_orders"]),
        "canceled": canceled,
        "skipped": skipped,
        "failed": failed,
    })


def _schedule_nightly_daily_sync(ContextInfo):
    now = dt.datetime.now()
    if now >= dt.datetime.combine(STATE["trade_date"], PREPARE_TIME):
        return False
    target = now + dt.timedelta(seconds=1)
    # Reserve before scheduling, including the 08:59:59 -> 09:00 boundary.
    STATE["startup_sync_pending"] = True
    try:
        ContextInfo.schedule_run(_on_nightly_daily_sync,
                                 target.strftime("%Y%m%d%H%M%S"),
                                 1, None, "nightly_daily_sync")
        STATE["scheduled_names"].append("nightly_daily_sync")
        _log({"event": "scheduled", "name": "nightly_daily_sync", "target": _time_text(target)})
        return True
    except Exception as exc:
        _log({"event": "nightly_daily_sync_schedule_error", "error": repr(exc)})
        _on_nightly_daily_sync(ContextInfo)
        return False


def _cache_snapshot(ContextInfo, phase):
    _prepare_daily(ContextInfo)
    missing, expected_dates = _daily_cache_missing_codes(ContextInfo)
    STATE["daily_cache_missing_codes"] = list(missing)
    _log({"event": "daily_cache_status", "phase": phase,
          "total": len(STATE["universe"]),
          "usable": len(STATE["universe"]) - len(missing),
          "missing_count": len(missing),
          "allowed_unavailable_codes": DAILY_CACHE_ALLOWED_UNAVAILABLE_CODES,
          "within_threshold": len(missing) <= DAILY_CACHE_ALLOWED_UNAVAILABLE_CODES,
          "expected_recent_3": [str(value) for value in expected_dates],
          "missing_examples": missing[:20]})
    return missing


def _on_nightly_daily_sync(ContextInfo):
    if not DAILY_SYNC_LOCK.acquire(False):
        return
    try:
        def work():
            _log({"event": "daily_sync_notice", "message": "启动全股票池日线下载；完成后核验缓存，下载期间不做09:00缓存检查。"})
            result = _download_daily(ContextInfo, list(STATE["universe"]), None, mode="startup_full")
            _cache_snapshot(ContextInfo, "启动下载完成")
            _log(dict(result, event="nightly_daily_sync_finished"))
        _guard("nightly_daily_sync", work)
    finally:
        STATE["startup_sync_pending"] = False
        DAILY_SYNC_LOCK.release()
    # Also works when QMT serializes callbacks and the 09:00 callback is delayed.
    if dt.datetime.now() >= dt.datetime.combine(STATE["trade_date"], PREPARE_TIME):
        _on_download_and_prepare(ContextInfo)
    _log_startup_summary_when_ready()

def _schedule(ContextInfo, trade_date, target_time, name, callback):
    target = dt.datetime.combine(trade_date, target_time)
    if target <= dt.datetime.now():
        _log({"event": "schedule_missed", "name": name, "target": _time_text(target)})
        return False
    ContextInfo.schedule_run(callback, target.strftime("%Y%m%d%H%M%S"), 1, None, name)
    STATE["scheduled_names"].append(name)
    _log({"event": "scheduled", "name": name, "target": _time_text(target)})
    return True


def _daily_cache_ready_or_log(stage):
    if STATE.get("daily_cache_ready"):
        return True
    _log({
        "event": "stage_skipped_daily_cache_incomplete",
        "stage": stage,
        "missing_count": len(STATE.get("daily_cache_missing_codes") or []),
    })
    return False


def _on_download_and_prepare(ContextInfo):
    if STATE.get("startup_sync_pending"):
        _log({"event": "daily_sync_notice", "message": "09:00准备暂缓：启动下载尚未完成，完成后自动检查；不重复下载。"})
        return
    if not DAILY_SYNC_LOCK.acquire(False):
        _log({"event": "daily_sync_notice", "message": "日线下载或核验进行中，本次不重复检查或下载。"})
        return
    try:
        if STATE.get("morning_prepare_done"):
            return
        def work():
            STATE["daily_cache_ready"] = False
            missing = _cache_snapshot(ContextInfo, "09:00缓存核验")
            retried = len(missing) > DAILY_CACHE_ALLOWED_UNAVAILABLE_CODES
            if retried:
                _log({"event": "daily_sync_notice", "message": "缓存缺失超过20只：开始一次全股票池日线补下载；完成后按有效股票继续选股，不再限制缺失总数。"})
                _download_daily(ContextInfo, list(STATE["universe"]), None, mode="morning_full_retry")
                missing = _cache_snapshot(ContextInfo, "早晨补下载完成")
            # Relax only the aggregate threshold, never individual date validity.
            for code in missing:
                STATE["daily"].pop(code, None)
            _prepare_preclose(ContextInfo)
            STATE["daily_cache_ready"] = bool(STATE["preclose"])
            STATE["morning_prepare_done"] = True
            _log({"event": "daily_selection_ready",
                  "usable": len(STATE["daily"]), "preclose_count": len(STATE["preclose"]),
                  "missing_count": len(missing), "retried": retried,
                  "ready": STATE["daily_cache_ready"]})
        _guard("daily_cache_verify", work)
    finally:
        DAILY_SYNC_LOCK.release()

def _on_precheck1(ContextInfo):
    def work():
        if not _daily_cache_ready_or_log("precheck1"):
            return
        _collect_sample(
            ContextInfo,
            "precheck1",
            PRECHECK1_TIME,
            PRECHECK1_WINDOW_START,
            PRECHECK1_UNTIL,
            PRECHECK1_UNTIL,
            "precheck1",
        )
        _log_precheck_summary("precheck1", "precheck1")
    _guard("precheck1", work)

def _on_precheck2(ContextInfo):
    def work():
        if not _daily_cache_ready_or_log("precheck2"):
            return
        _collect_sample(
            ContextInfo,
            "precheck2",
            PRECHECK2_TIME,
            PRECHECK2_WINDOW_START,
            PRECHECK2_UNTIL,
            PRECHECK2_UNTIL,
            "precheck2",
        )
        _log_precheck_summary("precheck2", "precheck2")
    _guard("precheck2", work)

def _sample_callback(label, target, minimum, maximum, is_final):
    def callback(ContextInfo):
        deadline = FINAL_SAMPLE_UNTIL if is_final else maximum
        def work():
            if not _daily_cache_ready_or_log("sample_" + label):
                return
            precheck_passed_codes = _dual_precheck_pass_codes()
            # Always sample the complete prepared universe. This preserves A-F
            # diagnostics even for stocks rejected by either precheck.
            codes = list(STATE["preclose"])
            _log({
                "event": "sample_collection_scope",
                "label": label,
                "scope": "all_prepared_stocks",
                "stock_count": len(codes),
                "precheck_passed_for_selection": len(precheck_passed_codes),
            })
            _collect_sample(ContextInfo, label, target, minimum, maximum, deadline, None, codes)
        _guard("sample_" + label, work)
    return callback


def _on_evaluate(ContextInfo):
    def work():
        if not _daily_cache_ready_or_log("evaluate"):
            return
        _evaluate(ContextInfo)
    _guard("evaluate", work)

def _on_order(ContextInfo):
    def work():
        if not _daily_cache_ready_or_log("order"):
            return
        _submit_orders(ContextInfo)
        _log_unfiltered_order_probe("immediate_after_order")
        _log_optional_filter_only_candidates()
    _guard("order", work)

def _on_order_probe(ContextInfo):
    _guard("order_probe", lambda: _log_unfiltered_order_probe("five_seconds_after_order"))


def _on_final_cancel(ContextInfo):
    def work():
        _log_unfiltered_order_probe("before_final_cancel")
        _cancel_unfilled_orders(ContextInfo)
    _guard("final_cancel", work)


def _on_account_preflight_retry(ContextInfo):
    _guard("account_preflight_retry", lambda: _run_account_preflight(ContextInfo, "startup_retry"))
    STATE["startup_account_retry_done"] = True
    _log_startup_summary_when_ready()


def init(ContextInfo):
    _log({"event": "daily_sync_notice", "message": "已进入策略初始化，正在读取和绑定账户；尚未开始日线下载。"})
    _set_stage("init")
    account_id, account_type, account_source, type_source = _capture_runtime_account(ContextInfo)
    account_bound = False
    account_bind_error = ""
    if account_id and account_type:
        try:
            ContextInfo.set_account(account_id)
            account_bound = True
        except Exception as exc:
            account_bind_error = repr(exc)
    else:
        account_bind_error = "model_trading_account_or_type_unavailable"
    _log({
        "event": "init",
        "strategy_version": STRATEGY_VERSION,
        "read_only": False,
        "order_submission": ORDER_SUBMISSION_ENABLED,
        "account_id": account_id,
        "account_type": account_type,
        "account_source": account_source,
        "account_type_source": type_source,
        "account_bound": account_bound,
        "account_bind_error": account_bind_error,
        "pid": os.getpid(),
        "defaults": {
            "a_rise_gt": MIN_A_RISE_PCT,
            "f_price_filter_enabled": ENABLE_F_PRICE_FILTER,
            "f_price_min": F_PRICE_MIN,
            "t1_rise_filter_enabled": ENABLE_T1_RISE_FILTER,
            "t1_rise_max_pct": T1_RISE_MAX_PCT,
            "prior_two_day_drop_enabled": ENABLE_PRIOR_TWO_DAY_MAX_DROP_FILTER,
            "prior_two_day_max_drop_threshold_pct": PRIOR_TWO_DAY_MAX_DROP_THRESHOLD_PCT,
            "max_selected": MAX_SELECTED_STOCKS,
            "single_stock_asset_ratio_pct": SINGLE_STOCK_ASSET_RATIO * 100,
            "order_price_rise_pct": ORDER_PRICE_RISE_PCT,
        },
    })
    _run_account_preflight(ContextInfo, "init")


def after_init(ContextInfo):
    def work():
        retry_at = dt.datetime.now() + dt.timedelta(seconds=5)
        try:
            ContextInfo.schedule_run(
                _on_account_preflight_retry,
                retry_at.strftime("%Y%m%d%H%M%S"),
                1,
                None,
                "account_preflight_retry",
            )
            STATE["scheduled_names"].append("account_preflight_retry")
            _log({"event": "scheduled", "name": "account_preflight_retry", "target": _time_text(retry_at)})
        except Exception as exc:
            _log({"event": "account_preflight_retry_schedule_error", "error": repr(exc)})
            STATE["startup_account_retry_done"] = True
        _load_universe(ContextInfo)
        trade_date = _next_trade_date(ContextInfo)
        STATE["trade_date"] = trade_date
        now = dt.datetime.now()

        _log({
            "event": "live_schedule",
            "trade_date": str(trade_date),
            "prepare_at": str(PREPARE_TIME),
            "precheck1_at": str(PRECHECK1_TIME),
            "precheck2_at": str(PRECHECK2_TIME),
            "now": _time_text(now),
        })
        _schedule_nightly_daily_sync(ContextInfo)
        if trade_date == now.date() and PREPARE_TIME <= now.time() < PRECHECK1_TIME:
            _log({"event": "daily_download_late_start", "now": _time_text(now)})
            _on_download_and_prepare(ContextInfo)
        else:
            _schedule(ContextInfo, trade_date, PREPARE_TIME, "daily_download_and_prepare", _on_download_and_prepare)
        _schedule(ContextInfo, trade_date, PRECHECK1_TIME, "precheck1", _on_precheck1)
        _schedule(ContextInfo, trade_date, PRECHECK2_TIME, "precheck2", _on_precheck2)
        for label, target, minimum, maximum in SAMPLE_PLAN:
            _schedule(ContextInfo, trade_date, target, "sample_" + label, _sample_callback(label, target, minimum, maximum, label == "f"))
        _schedule(ContextInfo, trade_date, dt.time(9, 26, 31), "evaluate", _on_evaluate)
        _schedule(ContextInfo, trade_date, ORDER_TIME, "order", _on_order)
        _schedule(ContextInfo, trade_date, ORDER_PROBE_TIME, "order_probe", _on_order_probe)
        _schedule(ContextInfo, trade_date, FINAL_CANCEL_TIME, "final_cancel", _on_final_cancel)
        _write_status("after_init_scheduled")
        STATE["startup_schedules_done"] = True
        _log_startup_summary_when_ready()
    _guard("after_init", work)


def _log_startup_summary_when_ready():
    # Account retry can be queued behind the synchronous download in QMT.
    # Wait for both so the waiting notice is the final startup console line.
    with LOG_LOCK:
        if (STATE.get("startup_summary_logged") or STATE.get("startup_sync_pending")
                or not STATE.get("startup_schedules_done")
                or not STATE.get("startup_account_retry_done")):
            return
        STATE["startup_summary_logged"] = True
        now = dt.datetime.now()
        trade_date = STATE["trade_date"]
        _log({
            "event": "startup_parameters",
            "t1_rise_filter_enabled": ENABLE_T1_RISE_FILTER,
            "t1_rise_max_pct": T1_RISE_MAX_PCT,
            "prior_two_day_drop_enabled": ENABLE_PRIOR_TWO_DAY_MAX_DROP_FILTER,
            "prior_two_day_max_drop_threshold_pct": PRIOR_TWO_DAY_MAX_DROP_THRESHOLD_PCT,
            "single_stock_asset_ratio_pct": SINGLE_STOCK_ASSET_RATIO * 100,
            "max_selected": MAX_SELECTED_STOCKS,
            "order_submission": ORDER_SUBMISSION_ENABLED,
        })
        prepare_at = dt.datetime.combine(trade_date, PREPARE_TIME)
        if now < prepare_at:
            _log({
                "event": "waiting_for_prepare_time",
                "now": _time_text(now),
                "trade_date": str(trade_date),
                "prepare_at": _time_text(prepare_at),
            })


def handlebar(ContextInfo):
    pass


def order_callback(ContextInfo, orderInfo):
    try:
        remark = str(_first_attr(orderInfo, ("m_strRemark", "m_strUserOrderId", "remark", "user_order_id"), "") or "")
        if remark not in STATE["submitted_orders"]:
            return
        order_id = str(_first_attr(orderInfo, ("m_strOrderSysID", "m_nOrderID", "order_id"), "") or "")
        status = _first_attr(orderInfo, ("m_nOrderStatus", "order_status", "m_strStatus"), "")
        traded_volume = _safe_int(_first_attr(orderInfo, ("m_nVolumeTraded", "traded_volume"), 0))
        total_volume = _safe_int(_first_attr(orderInfo, ("m_nVolumeTotalOriginal", "m_nOrderVolume", "order_volume"), STATE["submitted_orders"][remark]["volume"]))
        callback = {
            "order_id": order_id,
            "status": status,
            "traded_volume": traded_volume,
            "total_volume": total_volume,
        }
        STATE["order_callbacks"][remark] = callback
        _log(dict(STATE["submitted_orders"][remark], event="order_callback", **callback))
    except Exception as exc:
        _log({"event": "order_callback_error", "error": repr(exc), "traceback": traceback.format_exc()})


def deal_callback(ContextInfo, dealInfo):
    try:
        remark = str(_first_attr(dealInfo, ("m_strRemark", "m_strUserOrderId", "remark", "user_order_id"), "") or "")
        if remark not in STATE["submitted_orders"]:
            return
        deal = {
            "remark": remark,
            "order_id": str(_first_attr(dealInfo, ("m_strOrderSysID", "m_nOrderID", "order_id"), "") or ""),
            "deal_id": str(_first_attr(dealInfo, ("m_strTradeID", "m_nDealID", "deal_id"), "") or ""),
            "deal_price": _safe_float(_first_attr(dealInfo, ("m_dTradePrice", "m_dPrice", "deal_price"), 0.0)),
            "deal_volume": _safe_int(_first_attr(dealInfo, ("m_nVolume", "m_nTradeVolume", "deal_volume"), 0)),
        }
        STATE["deal_callbacks"].append(deal)
        _log(dict(STATE["submitted_orders"][remark], event="deal_callback", **deal))
    except Exception as exc:
        _log({"event": "deal_callback_error", "error": repr(exc), "traceback": traceback.format_exc()})


def stop(ContextInfo):
    STATE["last_stage"] = "strategy_stop"
    _write_status("strategy_stop")
    _log({
        "event": "strategy_stop",
        "pid": os.getpid(),
        "trade_date": str(STATE.get("trade_date") or ""),
        "scheduled_names": STATE.get("scheduled_names", []),
        "submitted_order_count": len(STATE.get("submitted_orders") or {}),
        "order_callback_count": len(STATE.get("order_callbacks") or {}),
        "deal_callback_count": len(STATE.get("deal_callbacks") or []),
    })
