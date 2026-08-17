# -*- coding: utf-8 -*-
"""Monitor MiniQMT jingjia snapshots and filter weak limit-up candidates.

The script reuses the same MiniQMT market connection style as the reference
strategy under ./reference-like folder "参考":

- xtdata.connect() for MiniQMT market data
- xtdata.subscribe_whole_quote(["SH", "SZ"], callback) for live quote warmup
- xtdata.get_stock_list_in_sector("沪深A股") for the universe
- xtdata.get_instrument_detail() and xtdata.get_full_tick() for metadata/ticks

Default rule:
- main-board normal A-share, i.e. 10% price-limit stock universe
- 09:16 precheck price is the limit-up price
- a rise is above the configured threshold versus previous close, default 5%
- a >= b >= c >= d >= e >= f
- f is between -1% and +3% versus previous close
- f is between -10% and +10% versus T-3 close
- (e - f) < (limit-up price - e)

Run from the repo root in the QMT Python environment:
    python jingjia_filter.py
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "qmt_accounts_config.json"
LOG_DIR = BASE_DIR / "logs"

XTDATA_IP = "127.0.0.1"
XTDATA_PORT: int | None = None

DEFAULT_SAMPLE_TIMES = [
    "09:20:03",
    "09:21:00",
    "09:22:00",
    "09:23:00",
    "09:24:00",
    "09:25:05",
]
PREPARE_TIME = dt_time(9, 15, 5)
DAILY_DOWNLOAD_TIME = dt_time(9, 0, 0)
DAILY_DOWNLOAD_LOOKBACK_DAYS = 30
PRECHECK_SAMPLE_TIME = dt_time(9, 16, 0)
PRECHECK_COLLECT_UNTIL = dt_time(9, 18, 0)
FINAL_SAMPLE_COLLECT_UNTIL = dt_time(9, 26, 30)
FIRST_SAMPLE_MIN_RISE_PCT = 5.0
FINAL_MIN_RISE_PCT = -1.0
FINAL_MAX_RISE_PCT = 3.0
RANK_BASE_MIN_RISE_PCT = -10.0
RANK_BASE_MAX_RISE_PCT = 10.0
PRIOR_MAX_DROP_THRESHOLD_PCT = -9.0
PRIOR_T1_MAX_RISE_PCT = 7.0
ORDER_TIME = dt_time(9, 27, 0)
FINAL_CANCEL_TIME = dt_time(10, 0, 0)
ORDER_PRICE_RISE_PCT = 0.5
SINGLE_STOCK_ASSET_RATIO = 0.30
LOT_SIZE = 100
STRATEGY_NAME = "jingjia_filter_0927"
MAX_ORDER_STOCKS = 2
QUOTE_TIME_TOLERANCE_SECONDS = 1.0
FINAL_QUOTE_TIME_TOLERANCE_SECONDS = 4.0

from xtquant import xtconstant, xtdata, xttrader
from xtquant.xttype import StockAccount

try:
    from xtquant import xtconn
except ImportError:
    xtconn = None

latest_quotes: dict[str, dict[str, Any]] = {}
quote_received_at: dict[str, datetime] = {}
quote_lock = threading.Lock()


@dataclass(frozen=True)
class StockMeta:
    code: str
    name: str
    price_tick: float
    detail_pre_close: float
    detail_up_limit: float


@dataclass(frozen=True)
class QmtRuntimeConfig:
    account_id: str
    qmt_root: str


@dataclass
class TradeSession:
    trader: Any
    account: Any


@dataclass(frozen=True)
class SubmittedOrder:
    order_id: int
    code: str
    name: str
    volume: int


@dataclass(frozen=True)
class SamplePoint:
    target_time: dt_time
    scan_time: datetime
    quote_time: datetime | None
    price: float
    price_source: str


@dataclass(frozen=True)
class QuoteWindow:
    minimum: dt_time
    maximum: dt_time | None = None


@dataclass(frozen=True)
class DailyReference:
    rank_base_date: date
    rank_base_close: float
    prior_t2_date: date
    prior_t2_max_drop_pct: float | None
    prior_t1_date: date
    prior_t1_max_drop_pct: float | None
    prior_t1_rise_pct: float | None
    prior_t1_limit_up: bool | None


@dataclass
class StockState:
    code: str
    name: str
    price_tick: float
    pre_close: float
    up_limit: float
    preclose_source: str
    rank_base_date: date | None = None
    rank_base_close: float = 0.0
    prior_t2_date: date | None = None
    prior_t2_max_drop_pct: float | None = None
    prior_t1_date: date | None = None
    prior_t1_max_drop_pct: float | None = None
    prior_t1_rise_pct: float | None = None
    prior_t1_limit_up: bool | None = None
    precheck_sample: SamplePoint | None = None
    samples: list[SamplePoint] = field(default_factory=list)
    passed: bool = False
    reason: str = ""


def safe_float(value: Any) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else 0.0
    except (TypeError, ValueError):
        return 0.0


def safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def first_attr(value: Any, names: tuple[str, ...], default: Any = None) -> Any:
    if value is None:
        return default
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return default


def xtconstant_values(names: tuple[str, ...]) -> set[Any]:
    return {
        getattr(xtconstant, name)
        for name in names
        if hasattr(xtconstant, name)
    }


def parse_quote_time(quote: dict[str, Any] | None) -> datetime | None:
    if not quote:
        return None
    timestamp = safe_float(quote.get("time"))
    if timestamp <= 0:
        return None
    if timestamp > 10_000_000_000:
        timestamp /= 1000
    try:
        return datetime.fromtimestamp(timestamp)
    except (OSError, OverflowError, ValueError):
        return None


def quote_is_today(quote: dict[str, Any] | None) -> bool:
    quote_time = parse_quote_time(quote)
    return quote_time is not None and quote_time.date() == datetime.now().date()


def fmt_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def fmt_time(value: dt_time) -> str:
    return value.strftime("%H:%M:%S")


def round_to_tick(price: float, tick: float) -> float:
    tick = tick if tick > 0 else 0.01
    value = (
        Decimal(str(price)) / Decimal(str(tick))
    ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return float(value * Decimal(str(tick)))


def minimum_quote_datetime(
    target_dt: datetime,
    sample_index: int,
    sample_count: int,
) -> datetime:
    tolerance_seconds = (
        FINAL_QUOTE_TIME_TOLERANCE_SECONDS
        if sample_count > 1 and sample_index == sample_count - 1
        else QUOTE_TIME_TOLERANCE_SECONDS
    )
    return target_dt - timedelta(seconds=tolerance_seconds)


def replace_time(value: datetime, new_time: dt_time) -> datetime:
    return datetime.combine(value.date(), new_time)


def live_quote_window(
    target_dt: datetime,
    sample_time: dt_time,
    sample_index: int | None = None,
) -> tuple[datetime, datetime | None]:
    if sample_time == PRECHECK_SAMPLE_TIME:
        window = QuoteWindow(dt_time(9, 16, 0), dt_time(9, 18, 0))
    elif sample_index == 0:
        window = QuoteWindow(dt_time(9, 20, 0), dt_time(9, 20, 20))
    elif sample_index == 1:
        window = QuoteWindow(dt_time(9, 20, 55), dt_time(9, 21, 20))
    elif sample_index == 2:
        window = QuoteWindow(dt_time(9, 21, 55), dt_time(9, 22, 20))
    elif sample_index == 3:
        window = QuoteWindow(dt_time(9, 22, 55), dt_time(9, 23, 20))
    elif sample_index == 4:
        window = QuoteWindow(dt_time(9, 23, 55), dt_time(9, 24, 20))
    elif sample_index == 5:
        window = QuoteWindow(dt_time(9, 25, 0), dt_time(9, 26, 59))
    else:
        raise ValueError(f"unsupported live sample index: {sample_index}")
    quote_min = replace_time(target_dt, window.minimum)
    quote_max = replace_time(target_dt, window.maximum) if window.maximum else None
    return quote_min, quote_max


def parse_sample_times(value: str) -> list[dt_time]:
    items = [item.strip() for item in value.split(",") if item.strip()]
    if not items:
        raise ValueError("sample time list is empty")
    result: list[dt_time] = []
    for item in items:
        try:
            result.append(datetime.strptime(item, "%H:%M:%S").time())
        except ValueError as exc:
            raise ValueError(f"invalid sample time: {item}") from exc
    if result != sorted(result):
        raise ValueError("sample times must be sorted ascending")
    return result


def parse_hhmmss(value: str, option_name: str) -> dt_time:
    try:
        return datetime.strptime(value, "%H:%M:%S").time()
    except ValueError as exc:
        raise ValueError(f"{option_name} must use HH:MM:SS, got {value!r}") from exc


def parse_trade_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) < 8:
        return None
    try:
        return datetime.strptime(digits[:8], "%Y%m%d").date()
    except ValueError:
        return None


def parse_xt_trading_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, dt_time())
    try:
        number = float(value)
        if number > 100000000000:
            return datetime.fromtimestamp(number / 1000.0)
        if number > 1000000000:
            return datetime.fromtimestamp(number)
    except (TypeError, ValueError):
        pass
    trade_date = parse_trade_date(value)
    if trade_date is None:
        return None
    return datetime.combine(trade_date, dt_time())


def parse_target_date(value: str) -> date:
    trade_date = parse_trade_date(value)
    if trade_date is None:
        raise ValueError(f"invalid trade date: {value}")
    return trade_date


def normalize_stock_code(value: str) -> str:
    code = value.strip().upper()
    if "." in code:
        symbol, _, market = code.partition(".")
        return f"{symbol}.{market}"
    if code.startswith(("SH", "SZ")) and len(code) > 2:
        symbol = code[2:]
        market = code[:2]
        return f"{symbol}.{market}"
    if code.startswith(("6", "5", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "1", "2", "3")):
        return f"{code}.SZ"
    return code


def next_live_schedule_date(sample_times: list[dt_time], now: datetime | None = None) -> date:
    current = now or datetime.now()
    candidate = current.date()
    first_target_today = datetime.combine(candidate, sample_times[0])
    if current > first_target_today:
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def get_trading_dates_until(end_date: date, lookback_days: int) -> list[date]:
    start_date = (end_date - timedelta(days=max(lookback_days, 10))).strftime("%Y%m%d")
    end_text = end_date.strftime("%Y%m%d")
    try:
        raw_dates = xtdata.get_trading_dates("SH", start_date, end_text, -1) or []
    except Exception:
        logging.exception("get_trading_dates failed; fallback to weekday calendar")
        raw_dates = []
    dates = sorted(
        set(
            parsed.date()
            for parsed in (parse_xt_trading_datetime(item) for item in raw_dates)
            if parsed is not None and parsed.date() <= end_date
        )
    )
    if dates:
        return dates
    current = end_date - timedelta(days=max(lookback_days, 10))
    result: list[date] = []
    while current <= end_date:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result


def latest_completed_trading_date(now: datetime | None = None) -> date:
    current = now or datetime.now()
    dates = get_trading_dates_until(current.date(), DAILY_DOWNLOAD_LOOKBACK_DAYS + 15)
    if current.date() in dates and current.time() < dt_time(15, 30):
        dates = [item for item in dates if item < current.date()]
    else:
        dates = [item for item in dates if item <= current.date()]
    if not dates:
        raise RuntimeError("no completed trading date found")
    return dates[-1]


def wait_until(target: datetime, label: str, interval_seconds: float) -> None:
    while True:
        remaining = (target - datetime.now()).total_seconds()
        if remaining <= 0:
            logging.info("%s reached, drift=%.3fs", label, -remaining)
            return
        if remaining <= 5:
            sleep_seconds = interval_seconds
        elif remaining <= 60:
            sleep_seconds = 1.0
        else:
            sleep_seconds = 60.0
        time.sleep(min(max(0.01, sleep_seconds), remaining))


def on_whole_quote(datas: dict[str, dict[str, Any]]) -> None:
    received_at = datetime.now()
    with quote_lock:
        latest_quotes.update(datas)
        for code in datas:
            quote_received_at[code] = received_at


def quote_snapshot() -> dict[str, dict[str, Any]]:
    with quote_lock:
        return dict(latest_quotes)


def clear_quote_cache() -> None:
    with quote_lock:
        latest_quotes.clear()
        quote_received_at.clear()


def is_main_board(code: str) -> bool:
    symbol, _, market = code.partition(".")
    if market == "SH":
        return symbol.startswith(("600", "601", "603", "605"))
    if market == "SZ":
        return symbol.startswith(("000", "001", "002", "003"))
    return False


def valid_name(name: str) -> bool:
    normalized = name.upper().replace(" ", "")
    return bool(
        normalized
        and "ST" not in normalized
        and not normalized.startswith(("N", "C"))
        and "\u9000" not in normalized
    )


def load_qmt_config(config_path: Path) -> QmtRuntimeConfig:
    if not config_path.exists():
        raise FileNotFoundError(f"QMT config not found: {config_path}")
    payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    accounts = payload.get("accounts") or []
    if not accounts:
        raise ValueError(f"no accounts in config: {config_path}")

    active_account_id = str(payload.get("active_account_id") or "").strip()
    selected = accounts[0]
    if active_account_id:
        selected = next(
            (
                item for item in accounts
                if str(item.get("account_id") or "").strip() == active_account_id
            ),
            None,
        )
        if selected is None:
            raise ValueError(
                f"active_account_id={active_account_id} not found in config: {config_path}"
            )

    account_id = str(selected.get("account_id") or "").strip()
    qmt_root = str(selected.get("qmt_root") or "").strip()
    if not account_id:
        raise ValueError(f"selected account misses account_id: {config_path}")
    if not qmt_root:
        raise ValueError(f"selected account misses qmt_root: {config_path}")
    return QmtRuntimeConfig(account_id=account_id, qmt_root=qmt_root)


def configure_logging(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / f"jingjia_filter_{datetime.now():%Y%m%d}.log"

    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    logging.info("log file: %s", log_path)


def connect_market_service(qmt_root: str, strict_root_check: bool) -> None:
    if xtconn is None:
        logging.info("xtconn is unavailable in this xtquant package; skip instance scan")
    else:
        instances = xtconn.scan_all_server_instance()
        logging.info("XtQuant instances: %d", len(instances))
        for index, item in enumerate(instances, 1):
            logging.info(
                "instance %d: running=%s address=%s:%s root=%s data=%s type=%s",
                index,
                item.get("is_running"),
                item.get("ip"),
                item.get("port"),
                item.get("root_dir"),
                item.get("data_dir"),
                item.get("client_type"),
            )

    if XTDATA_PORT is None:
        client = xtdata.connect()
    else:
        client = xtdata.connect(XTDATA_IP, XTDATA_PORT)

    app_dir = os.path.abspath(client.get_app_dir())
    data_dir = os.path.abspath(client.get_data_dir())
    peer = getattr(client, "get_peer_addr", lambda: "")()
    logging.info("market connected: peer=%s app_dir=%s data_dir=%s", peer, app_dir, data_dir)

    expected = os.path.normcase(os.path.normpath(qmt_root))
    actual = os.path.normcase(os.path.normpath(app_dir))
    if strict_root_check and expected not in actual:
        raise RuntimeError(
            f"connected MiniQMT root mismatch: expected={qmt_root}, actual={app_dir}"
        )


def connect_market_service_with_retry(
    qmt_root: str,
    strict_root_check: bool,
    retry_interval_seconds: float,
) -> None:
    retry_interval_seconds = max(1.0, retry_interval_seconds)
    attempt = 0
    while True:
        attempt += 1
        try:
            connect_market_service(qmt_root, strict_root_check)
            if attempt > 1:
                logging.info("QMT reconnected successfully after %d attempts", attempt)
            return
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            logging.warning(
                "QMT disconnected or unavailable: %s; retry in %.1fs (attempt=%d)",
                exc,
                retry_interval_seconds,
                attempt,
            )
            time.sleep(retry_interval_seconds)


class JingjiaTradeCallback(xttrader.XtQuantTraderCallback):
    def on_disconnected(self) -> None:
        logging.error("trade connection disconnected")

    def on_order_error(self, order_error: Any) -> None:
        logging.error(
            "order error: order_id=%s error_id=%s message=%s",
            first_attr(order_error, ("order_id", "m_nOrderID"), ""),
            first_attr(order_error, ("error_id", "m_nErrorID"), ""),
            first_attr(order_error, ("error_msg", "m_strErrorMsg"), ""),
        )


def connect_trade_account(config: QmtRuntimeConfig) -> TradeSession:
    userdata_mini = Path(config.qmt_root) / "userdata_mini"
    if not userdata_mini.is_dir():
        raise RuntimeError(f"MiniQMT userdata directory not found: {userdata_mini}")

    session_id = int(datetime.now().strftime("%H%M%S"))
    trader = xttrader.XtQuantTrader(str(userdata_mini), session_id)
    trader.register_callback(JingjiaTradeCallback())
    trader.start()

    connect_result = trader.connect()
    if connect_result != 0:
        try:
            trader.stop()
        finally:
            raise RuntimeError(f"trade connection failed: result={connect_result}")

    account = StockAccount(config.account_id, "STOCK")
    subscribe_result = trader.subscribe(account)
    if subscribe_result != 0:
        try:
            trader.stop()
        finally:
            raise RuntimeError(
                f"trade account subscribe failed: account={config.account_id} "
                f"result={subscribe_result}"
            )

    asset = trader.query_stock_asset(account)
    available_cash = safe_float(
        first_attr(
            asset,
            ("cash", "enable_balance", "available_cash", "m_dEnableBalance"),
            0,
        )
    )
    total_asset = safe_float(
        first_attr(asset, ("total_asset", "m_dBalance", "asset"), 0)
    )
    logging.info(
        "trade account connected: account=%s session=%s available_cash=%.2f total_asset=%.2f",
        config.account_id,
        session_id,
        available_cash,
        total_asset,
    )
    return TradeSession(trader=trader, account=account)


def stop_trade_account(session: TradeSession | None) -> None:
    if session is None:
        return
    try:
        session.trader.stop()
    except Exception:
        logging.exception("stop trade connection failed")


def log_startup_data_check(code: str, price_mode: str) -> None:
    normalized_code = normalize_stock_code(code)
    logging.info("startup data check begin: code=%s", normalized_code)

    detail = xtdata.get_instrument_detail(normalized_code) or {}
    if detail:
        logging.info(
            "startup detail: code=%s name=%s pre_close=%s up_stop=%s down_stop=%s price_tick=%s",
            normalized_code,
            detail.get("InstrumentName", ""),
            detail.get("PreClose", ""),
            detail.get("UpStopPrice", ""),
            detail.get("DownStopPrice", ""),
            detail.get("PriceTick", ""),
        )
    else:
        logging.warning("startup detail: code=%s returned empty detail", normalized_code)

    started = time.perf_counter()
    ticks = xtdata.get_full_tick([normalized_code]) or {}
    elapsed_ms = (time.perf_counter() - started) * 1000
    quote = ticks.get(normalized_code) or {}
    if not quote:
        logging.warning(
            "startup tick: code=%s returned empty tick elapsed_ms=%.3f",
            normalized_code,
            elapsed_ms,
        )
        return

    price, source = quote_price(quote, price_mode)
    logging.info(
        "startup tick: code=%s quote_time=%s price=%s price_source=%s lastPrice=%s lastClose=%s open=%s high=%s low=%s volume=%s amount=%s elapsed_ms=%.3f",
        normalized_code,
        fmt_datetime(parse_quote_time(quote)),
        price,
        source,
        quote.get("lastPrice", ""),
        quote.get("lastClose", ""),
        quote.get("open", quote.get("openPrice", "")),
        quote.get("high", ""),
        quote.get("low", ""),
        quote.get("volume", ""),
        quote.get("amount", ""),
        elapsed_ms,
    )


def load_universe() -> dict[str, StockMeta]:
    started = time.perf_counter()
    sector_name = "\u6caa\u6df1A\u80a1"
    all_codes = xtdata.get_stock_list_in_sector(sector_name) or []
    universe: dict[str, StockMeta] = {}
    for code in all_codes:
        if not is_main_board(code):
            continue
        detail = xtdata.get_instrument_detail(code) or {}
        name = str(detail.get("InstrumentName") or "")
        if not valid_name(name):
            continue
        universe[code] = StockMeta(
            code=code,
            name=name,
            price_tick=safe_float(detail.get("PriceTick")) or 0.01,
            detail_pre_close=safe_float(detail.get("PreClose")),
            detail_up_limit=safe_float(detail.get("UpStopPrice")),
        )
    logging.info(
        "universe loaded: all=%d main_board_normal=%d elapsed=%.3fs",
        len(all_codes),
        len(universe),
        time.perf_counter() - started,
    )
    return universe


def load_rank_base_closes(
    universe: dict[str, StockMeta],
    target_date: date,
    chunk_size: int,
    download_daily: bool,
    lookback_days: int = DAILY_DOWNLOAD_LOOKBACK_DAYS,
) -> dict[str, DailyReference]:
    """Load daily references used by ranking and optional prior-day checks.

    Example: for a Monday target date, this picks the previous Wednesday close;
    for a Thursday target date, this picks the same-week Monday close.
    """
    codes = list(universe)
    start_date, end_date = daily_bar_date_range(target_date, lookback_days)

    if download_daily:
        download_daily_bars(codes, target_date, chunk_size, lookback_days)
    else:
        logging.info("skip ranking daily download; read ranking bars from local/server cache")

    daily_refs: dict[str, DailyReference] = {}
    started = time.perf_counter()
    for index, part in enumerate(chunked(codes, chunk_size), start=1):
        history = xtdata.get_market_data_ex(
            field_list=["low", "close"],
            stock_list=part,
            period="1d",
            start_time=start_date,
            end_time=end_date,
            count=-1,
            dividend_type="none",
            fill_data=False,
        ) or {}
        logging.info(
            "ranking daily load chunk %d: stocks=%d returned=%d",
            index,
            len(part),
            len(history),
        )

        for code in part:
            meta = universe[code]
            table = history.get(code)
            if table is None or not hasattr(table, "iterrows"):
                continue
            rows: list[tuple[date, float, float]] = []
            for row_index, row in table.iterrows():
                bar_date = parse_trade_date(row_index)
                low_price = row_float(row, "low")
                close_price = row_float(row, "close")
                if bar_date is None or bar_date >= target_date or close_price <= 0:
                    continue
                rows.append((bar_date, low_price, close_price))
            rows.sort(key=lambda item: item[0])
            if len(rows) >= 3:
                rank_base_date, _rank_base_low, rank_base_close = rows[-3]
                t2_date, t2_low, t2_close = rows[-2]
                t1_date, t1_low, t1_close = rows[-1]
                t2_drop_pct = (
                    (t2_low / rank_base_close - 1) * 100
                    if t2_low > 0 and rank_base_close > 0
                    else None
                )
                t1_drop_pct = (
                    (t1_low / t2_close - 1) * 100
                    if t1_low > 0 and t2_close > 0
                    else None
                )
                t1_rise_pct = (
                    (t1_close / t2_close - 1) * 100
                    if t1_close > 0 and t2_close > 0
                    else None
                )
                t1_up_limit = round_to_tick(t2_close * 1.10, meta.price_tick)
                t1_limit_up = (
                    abs(t1_close - t1_up_limit) <= meta.price_tick / 2 + 1e-9
                    if t1_close > 0 and t2_close > 0
                    else None
                )
                daily_refs[code] = DailyReference(
                    rank_base_date=rank_base_date,
                    rank_base_close=rank_base_close,
                    prior_t2_date=t2_date,
                    prior_t2_max_drop_pct=t2_drop_pct,
                    prior_t1_date=t1_date,
                    prior_t1_max_drop_pct=t1_drop_pct,
                    prior_t1_rise_pct=t1_rise_pct,
                    prior_t1_limit_up=t1_limit_up,
                )

    logging.info(
        "ranking base close prepared: usable=%d skipped=%d elapsed=%.3fs",
        len(daily_refs),
        len(universe) - len(daily_refs),
        time.perf_counter() - started,
    )
    return daily_refs


def daily_bar_date_range(target_date: date, lookback_days: int) -> tuple[str, str]:
    start_date = (target_date - timedelta(days=max(1, lookback_days))).strftime("%Y%m%d")
    end_date = target_date.strftime("%Y%m%d")
    return start_date, end_date


def latest_daily_download_end_date(target_date: date) -> date:
    try:
        completed_date = latest_completed_trading_date()
    except Exception:
        logging.exception("latest completed trading date lookup failed; use target_date for daily download")
        return target_date
    return min(target_date, completed_date)


def download_daily_bars(
    codes: list[str],
    target_date: date,
    chunk_size: int,
    lookback_days: int = DAILY_DOWNLOAD_LOOKBACK_DAYS,
) -> None:
    end_trade_date = latest_daily_download_end_date(target_date)
    start_date, end_date = daily_bar_date_range(end_trade_date, lookback_days)
    logging.info(
        "download daily bars: stocks=%d start=%s end=%s target_trade_date=%s mode=all_at_once",
        len(codes),
        start_date,
        end_date,
        target_date.strftime("%Y-%m-%d"),
    )
    started = time.perf_counter()
    ok = xtdata.download_history_data2(codes, "1d", start_date, end_date)
    logging.info(
        "daily download all_at_once finished: stocks=%d ok=%s elapsed=%.3fs",
        len(codes),
        ok,
        time.perf_counter() - started,
    )
    logging.info("daily download finished: stocks=%d elapsed=%.3fs", len(codes), time.perf_counter() - started)


def prepare_stock_states(
    universe: dict[str, StockMeta],
    target_date: date,
    chunk_size: int,
    download_daily: bool,
    lookback_days: int = DAILY_DOWNLOAD_LOOKBACK_DAYS,
) -> dict[str, StockState]:
    codes = list(universe)
    daily_refs = load_rank_base_closes(
        universe,
        target_date,
        chunk_size,
        download_daily,
        lookback_days,
    )
    started = time.perf_counter()
    ticks = xtdata.get_full_tick(codes) or {}
    logging.info(
        "preclose snapshot: requested=%d returned=%d elapsed=%.3fs",
        len(codes),
        len(ticks),
        time.perf_counter() - started,
    )

    states: dict[str, StockState] = {}
    for code, meta in universe.items():
        quote = ticks.get(code) or {}
        tick_close = safe_float(quote.get("lastClose"))
        if quote_is_today(quote) and tick_close > 0:
            pre_close = tick_close
            source = "tick.lastClose"
        else:
            pre_close = meta.detail_pre_close
            source = "instrument.PreClose"
        if pre_close <= 0:
            continue

        up_limit = round_to_tick(pre_close * 1.10, meta.price_tick)
        daily_ref = daily_refs.get(code)
        states[code] = StockState(
            code=code,
            name=meta.name,
            price_tick=meta.price_tick,
            pre_close=pre_close,
            up_limit=up_limit,
            preclose_source=source,
            rank_base_date=daily_ref.rank_base_date if daily_ref else None,
            rank_base_close=daily_ref.rank_base_close if daily_ref else 0.0,
            prior_t2_date=daily_ref.prior_t2_date if daily_ref else None,
            prior_t2_max_drop_pct=daily_ref.prior_t2_max_drop_pct if daily_ref else None,
            prior_t1_date=daily_ref.prior_t1_date if daily_ref else None,
            prior_t1_max_drop_pct=daily_ref.prior_t1_max_drop_pct if daily_ref else None,
            prior_t1_rise_pct=daily_ref.prior_t1_rise_pct if daily_ref else None,
            prior_t1_limit_up=daily_ref.prior_t1_limit_up if daily_ref else None,
        )
    logging.info("preclose prepared: usable=%d skipped=%d", len(states), len(universe) - len(states))
    return states


def check_recent_daily_ready(
    universe: dict[str, StockMeta],
    chunk_size: int,
    lookback_days: int = DAILY_DOWNLOAD_LOOKBACK_DAYS,
) -> bool:
    expected_date = latest_completed_trading_date()
    trading_dates = get_trading_dates_until(expected_date, lookback_days + 15)
    required_dates = [item for item in trading_dates if item <= expected_date][-3:]
    if len(required_dates) < 3:
        raise RuntimeError("less than three completed trading dates are available")

    start_date, end_date = daily_bar_date_range(expected_date, lookback_days)
    codes = list(universe)
    dates_by_code: dict[str, set[date]] = {}
    started = time.perf_counter()
    for index, part in enumerate(chunked(codes, chunk_size), start=1):
        history = xtdata.get_market_data_ex(
            field_list=["low", "close"],
            stock_list=part,
            period="1d",
            start_time=start_date,
            end_time=end_date,
            count=-1,
            dividend_type="none",
            fill_data=False,
        ) or {}
        logging.info(
            "daily ready load chunk %d: stocks=%d returned=%d",
            index,
            len(part),
            len(history),
        )
        for code in part:
            table = history.get(code)
            code_dates: set[date] = set()
            if table is not None and hasattr(table, "iterrows"):
                for row_index, row in table.iterrows():
                    bar_date = parse_trade_date(row_index)
                    low_price = row_float(row, "low")
                    close_price = row_float(row, "close")
                    if bar_date is not None and low_price > 0 and close_price > 0:
                        code_dates.add(bar_date)
            dates_by_code[code] = code_dates

    latest_by_code = {
        code: max(code_dates)
        for code, code_dates in dates_by_code.items()
        if code_dates
    }
    common_latest = (
        min(latest_by_code.values())
        if len(latest_by_code) == len(codes) and latest_by_code
        else None
    )
    required_set = set(required_dates)
    missing_latest = [
        code for code in codes
        if code not in latest_by_code or latest_by_code[code] < expected_date
    ]
    missing_recent = [
        code for code in codes
        if not required_set.issubset(dates_by_code.get(code, set()))
    ]
    count_by_required_date = {
        required_date: sum(1 for code_dates in dates_by_code.values() if required_date in code_dates)
        for required_date in required_dates
    }
    ready = not missing_latest and not missing_recent

    logging.info(
        "daily ready check: expected_ready_through=%s required_recent_3=%s total=%d "
        "latest_ready=%d recent3_ready=%d elapsed=%.3fs",
        expected_date.strftime("%Y-%m-%d"),
        ",".join(item.strftime("%Y-%m-%d") for item in required_dates),
        len(codes),
        len(codes) - len(missing_latest),
        len(codes) - len(missing_recent),
        time.perf_counter() - started,
    )
    logging.info(
        "daily ready check required_date_counts: %s",
        ", ".join(
            "{0}={1}/{2}".format(required_date.strftime("%Y-%m-%d"), count_by_required_date[required_date], len(codes))
            for required_date in required_dates
        ),
    )
    logging.info(
        "daily ready check common_latest_ready_date=%s",
        common_latest.strftime("%Y-%m-%d") if common_latest else "NONE",
    )
    if missing_latest:
        preview = ",".join(
            "{0}:{1}".format(
                code,
                latest_by_code[code].strftime("%Y-%m-%d") if code in latest_by_code else "NONE",
            )
            for code in missing_latest[:50]
        )
        logging.warning(
            "daily ready check missing latest date first %d: %s",
            min(50, len(missing_latest)),
            preview,
        )
    if missing_recent:
        preview = ",".join(missing_recent[:50])
        logging.warning(
            "daily ready check missing recent 3 trading dates first %d: %s",
            min(50, len(missing_recent)),
            preview,
        )
    if not ready:
        return False
    logging.info("daily ready check passed: all stocks are ready through %s", expected_date.strftime("%Y-%m-%d"))
    return True


def chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[index:index + size] for index in range(0, len(items), size)]


def row_float(row: Any, field: str) -> float:
    if hasattr(row, "get"):
        return safe_float(row.get(field))
    if isinstance(row, dict):
        return safe_float(row.get(field))
    return 0.0


def prepare_historical_stock_states(
    universe: dict[str, StockMeta],
    target_date: date,
    chunk_size: int,
    download_daily: bool,
) -> dict[str, StockState]:
    codes = list(universe)
    start_date = (target_date - timedelta(days=30)).strftime("%Y%m%d")
    end_date = target_date.strftime("%Y%m%d")

    if download_daily:
        logging.info(
            "download daily bars: stocks=%d start=%s end=%s chunk_size=%d",
            len(codes),
            start_date,
            end_date,
            chunk_size,
        )
        for index, part in enumerate(chunked(codes, chunk_size), start=1):
            ok = xtdata.download_history_data2(part, "1d", start_date, end_date)
            logging.info("daily download chunk %d: stocks=%d ok=%s", index, len(part), ok)
    else:
        logging.info("skip daily download; read daily bars from local/server cache")

    states: dict[str, StockState] = {}
    valid_daily = 0
    started = time.perf_counter()
    for index, part in enumerate(chunked(codes, chunk_size), start=1):
        history = xtdata.get_market_data_ex(
            field_list=["open", "high", "low", "close"],
            stock_list=part,
            period="1d",
            start_time=start_date,
            end_time=end_date,
            count=-1,
            dividend_type="none",
            fill_data=False,
        ) or {}
        logging.info("daily load chunk %d: stocks=%d returned=%d", index, len(part), len(history))

        for code in part:
            meta = universe[code]
            table = history.get(code)
            if table is None or not hasattr(table, "iterrows"):
                continue
            rows: list[tuple[date, float, float]] = []
            for row_index, row in table.iterrows():
                bar_date = parse_trade_date(row_index)
                low_price = row_float(row, "low")
                close_price = row_float(row, "close")
                if bar_date is None or bar_date >= target_date or close_price <= 0:
                    continue
                rows.append((bar_date, low_price, close_price))
            if not rows:
                continue
            rows.sort(key=lambda item: item[0])
            prev_trade_date, _prev_low, pre_close = rows[-1]
            rank_base_date: date | None = None
            rank_base_close = 0.0
            prior_t2_date: date | None = None
            prior_t2_max_drop_pct: float | None = None
            prior_t1_date: date | None = None
            prior_t1_max_drop_pct: float | None = None
            prior_t1_rise_pct: float | None = None
            prior_t1_limit_up: bool | None = None
            if len(rows) >= 3:
                rank_base_date, _rank_base_low, rank_base_close = rows[-3]
                prior_t2_date, t2_low, t2_close = rows[-2]
                prior_t1_date, t1_low, t1_close = rows[-1]
                prior_t2_max_drop_pct = (
                    (t2_low / rank_base_close - 1) * 100
                    if t2_low > 0 and rank_base_close > 0
                    else None
                )
                prior_t1_max_drop_pct = (
                    (t1_low / t2_close - 1) * 100
                    if t1_low > 0 and t2_close > 0
                    else None
                )
                prior_t1_rise_pct = (
                    (t1_close / t2_close - 1) * 100
                    if t1_close > 0 and t2_close > 0
                    else None
                )
                t1_up_limit = round_to_tick(t2_close * 1.10, meta.price_tick)
                prior_t1_limit_up = (
                    abs(t1_close - t1_up_limit) <= meta.price_tick / 2 + 1e-9
                    if t1_close > 0 and t2_close > 0
                    else None
                )
            up_limit = round_to_tick(pre_close * 1.10, meta.price_tick)
            states[code] = StockState(
                code=code,
                name=meta.name,
                price_tick=meta.price_tick,
                pre_close=pre_close,
                up_limit=up_limit,
                preclose_source=f"daily.close:{prev_trade_date:%Y-%m-%d}",
                rank_base_date=rank_base_date,
                rank_base_close=rank_base_close,
                prior_t2_date=prior_t2_date,
                prior_t2_max_drop_pct=prior_t2_max_drop_pct,
                prior_t1_date=prior_t1_date,
                prior_t1_max_drop_pct=prior_t1_max_drop_pct,
                prior_t1_rise_pct=prior_t1_rise_pct,
                prior_t1_limit_up=prior_t1_limit_up,
            )
            valid_daily += 1

    logging.info(
        "historical preclose prepared: usable=%d skipped=%d elapsed=%.3fs",
        valid_daily,
        len(universe) - valid_daily,
        time.perf_counter() - started,
    )
    return states


def quote_price(quote: dict[str, Any], mode: str) -> tuple[float, str]:
    if mode == "last":
        return safe_float(quote.get("lastPrice")), "lastPrice"
    if mode == "open":
        for key in ("open", "openPrice", "open_price", "Open", "OPEN"):
            price = safe_float(quote.get(key))
            if price > 0:
                return price, key
        return safe_float(quote.get("lastPrice")), "lastPrice_fallback"
    if mode == "auto":
        price = safe_float(quote.get("lastPrice"))
        if price > 0:
            return price, "lastPrice"
        for key in ("open", "openPrice", "open_price", "Open", "OPEN"):
            price = safe_float(quote.get(key))
            if price > 0:
                return price, key
    raise ValueError(f"unsupported price mode: {mode}")


def first_quote_field(quote: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in quote:
            return quote.get(name)
    return ""


def collect_sample(
    sample_time: dt_time,
    minimum_quote_dt: datetime,
    maximum_quote_dt: datetime | None,
    collection_deadline: datetime | None,
    states: dict[str, StockState],
    codes: list[str],
    price_mode: str,
    fresh_timeout_seconds: float,
    fresh_retry_interval: float,
    require_fresh_quote: bool,
    store_as_precheck: bool = False,
) -> None:
    started = time.perf_counter()
    pending = set(codes)
    deadline = collection_deadline or (
        datetime.now() + timedelta(seconds=max(0.0, fresh_timeout_seconds))
    )
    valid = 0
    attempts = 0
    last_returned = 0
    last_debug_quotes: list[tuple[str, str, dict[str, Any]]] = []

    while pending:
        attempts += 1
        request_codes = list(pending)
        ticks = xtdata.get_full_tick(request_codes) or {}
        fallback = quote_snapshot()
        last_returned = len(ticks)
        scan_time = datetime.now()
        if store_as_precheck:
            debug_quotes: list[tuple[str, str, dict[str, Any]]] = []
            for debug_code in request_codes:
                full_tick_quote = ticks.get(debug_code) or {}
                whole_quote = fallback.get(debug_code) or {}
                if full_tick_quote:
                    debug_quotes.append((debug_code, "full_tick", full_tick_quote))
                    if len(debug_quotes) >= 20:
                        break
                if whole_quote and whole_quote is not full_tick_quote:
                    debug_quotes.append((debug_code, "whole_quote", whole_quote))
                    if len(debug_quotes) >= 20:
                        break
            if debug_quotes:
                last_debug_quotes = debug_quotes

        for code in list(pending):
            state = states.get(code)
            if state is None:
                pending.discard(code)
                continue

            quote = ticks.get(code)
            source_suffix = ""
            if not quote:
                quote = fallback.get(code) or {}
                source_suffix = ".whole_quote"

            price, source = quote_price(quote, price_mode)
            quote_time = parse_quote_time(quote)
            fresh_ok = (
                not require_fresh_quote
                or (
                    quote_time is not None
                    and quote_time >= minimum_quote_dt
                    and (
                        maximum_quote_dt is None
                        or quote_time <= maximum_quote_dt
                    )
                )
            )
            if price <= 0 or not fresh_ok:
                continue

            sample = SamplePoint(
                target_time=sample_time,
                scan_time=scan_time,
                quote_time=quote_time,
                price=price,
                price_source=f"{source}{source_suffix}",
            )
            if store_as_precheck:
                state.precheck_sample = sample
            else:
                state.samples.append(sample)
            pending.discard(code)
            valid += 1

        if not require_fresh_quote or datetime.now() >= deadline:
            break
        if pending:
            time.sleep(max(0.01, fresh_retry_interval))

    elapsed_ms = (time.perf_counter() - started) * 1000.0

    logging.info(
        "sample %s: quote_time_window=%s~%s requested=%d returned=%d valid_price=%d "
        "pending=%d attempts=%d fresh_required=%s collect_until=%s elapsed_ms=%.3f",
        fmt_time(sample_time),
        fmt_datetime(minimum_quote_dt),
        fmt_datetime(maximum_quote_dt),
        len(codes),
        last_returned,
        valid,
        len(pending),
        attempts,
        require_fresh_quote,
        fmt_datetime(deadline),
        elapsed_ms,
    )
    if store_as_precheck and valid == 0:
        logging.warning(
            "precheck quote debug: no valid precheck prices; showing=%d "
            "quote_time_window=%s~%s price_mode=%s",
            len(last_debug_quotes),
            fmt_datetime(minimum_quote_dt),
            fmt_datetime(maximum_quote_dt),
            price_mode,
        )
        for code, quote_source, quote in last_debug_quotes:
            state = states.get(code)
            price, price_source = quote_price(quote, price_mode)
            logging.warning(
                "precheck quote debug item: code=%s name=%s source=%s "
                "quote_time=%s raw_time=%s lastPrice=%s lastClose=%s open=%s "
                "price=%s price_source=%s",
                code,
                state.name if state else "",
                quote_source,
                fmt_datetime(parse_quote_time(quote)),
                quote.get("time", ""),
                quote.get("lastPrice", ""),
                quote.get("lastClose", ""),
                first_quote_field(
                    quote,
                    ("open", "openPrice", "open_price", "Open", "OPEN"),
                ),
                price,
                price_source,
            )


def dataframe_row_to_quote(row: Any) -> dict[str, Any]:
    if hasattr(row, "to_dict"):
        return dict(row.to_dict())
    if isinstance(row, dict):
        return row
    return {}


def collect_historical_samples(
    target_date: date,
    sample_times: list[dt_time],
    states: dict[str, StockState],
    price_mode: str,
    chunk_size: int,
    max_lag_seconds: float,
    download_tick: bool,
    codes: list[str] | None = None,
    store_as_precheck: bool = False,
) -> None:
    target_codes = codes if codes is not None else list(states)
    if not target_codes:
        return

    first_target_dt = datetime.combine(target_date, sample_times[0])
    query_start_dt = minimum_quote_datetime(
        first_target_dt,
        sample_index=0,
        sample_count=len(sample_times),
    )
    query_start = query_start_dt.strftime("%Y%m%d%H%M%S")
    query_end_dt = datetime.combine(target_date, sample_times[-1]) + timedelta(
        seconds=max(10, int(max_lag_seconds) + 2)
    )
    query_end = query_end_dt.strftime("%Y%m%d%H%M%S")
    targets = [datetime.combine(target_date, value) for value in sample_times]

    logging.info(
        "historical tick: stocks=%d start=%s end=%s chunk_size=%d download=%s",
        len(target_codes),
        query_start,
        query_end,
        chunk_size,
        download_tick,
    )

    for chunk_index, part in enumerate(chunked(target_codes, chunk_size), start=1):
        if download_tick:
            ok = xtdata.download_history_data2(part, "tick", query_start, query_end)
            logging.info("tick download chunk %d: stocks=%d ok=%s", chunk_index, len(part), ok)

        tick_data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=part,
            period="tick",
            start_time=query_start,
            end_time=query_end,
            count=-1,
            dividend_type="none",
            fill_data=False,
        ) or {}
        logging.info("tick load chunk %d: returned=%d", chunk_index, len(tick_data))

        for code in part:
            state = states.get(code)
            table = tick_data.get(code)
            if state is None or table is None or not hasattr(table, "iterrows"):
                continue

            rows: list[tuple[datetime, dict[str, Any]]] = []
            for row_index, row in table.iterrows():
                row_time = parse_trade_date(row_index)
                digits = "".join(ch for ch in str(row_index) if ch.isdigit())
                if len(digits) >= 14:
                    try:
                        row_dt = datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
                    except ValueError:
                        row_dt = None
                else:
                    row_dt = None
                if row_dt is None and row_time is not None:
                    timestamp = safe_float(row_float(row, "time"))
                    if timestamp > 10_000_000_000:
                        timestamp /= 1000
                    row_dt = datetime.fromtimestamp(timestamp) if timestamp > 0 else None
                if row_dt is None or row_dt.date() != target_date:
                    continue
                rows.append((row_dt, dataframe_row_to_quote(row)))

            rows.sort(key=lambda item: item[0])
            cursor = 0
            for sample_index, (sample_time, target_dt) in enumerate(
                zip(sample_times, targets)
            ):
                minimum_quote_dt = minimum_quote_datetime(
                    target_dt,
                    sample_index,
                    len(sample_times),
                )
                while cursor < len(rows) and rows[cursor][0] < minimum_quote_dt:
                    cursor += 1
                if cursor >= len(rows):
                    continue

                candidate_index = cursor
                while (
                    candidate_index + 1 < len(rows)
                    and rows[candidate_index + 1][0] <= target_dt
                ):
                    candidate_index += 1
                row_dt, quote = rows[candidate_index]
                lag_seconds = (row_dt - target_dt).total_seconds()
                if lag_seconds > max_lag_seconds:
                    continue
                cursor = candidate_index
                price, source = quote_price(quote, price_mode)
                if price <= 0:
                    continue
                sample = SamplePoint(
                    target_time=sample_time,
                    scan_time=row_dt,
                    quote_time=row_dt,
                    price=price,
                    price_source=f"history.{source}",
                )
                if store_as_precheck:
                    state.precheck_sample = sample
                else:
                    state.samples.append(sample)


def first_sample_rule_label(first_min_rise_pct: float, first_rise_limit_up: bool) -> str:
    if first_rise_limit_up:
        return "a_limit_up"
    return f"a_rise>{first_min_rise_pct:.2f}"


def evaluate_states(
    states: dict[str, StockState],
    sample_count: int,
    first_min_rise_pct: float,
    first_rise_limit_up: bool,
    prior_max_drop_check: bool,
    prior_max_drop_threshold_pct: float,
    prior_t1_rise_check: bool,
    prior_t1_max_rise_pct: float,
    exclude_final_price_below_5: bool,
) -> list[StockState]:
    selected: list[StockState] = []
    for state in states.values():
        missing_reasons = []
        if state.precheck_sample is None:
            missing_reasons.append("missing_precheck_sample")
        if len(state.samples) < sample_count:
            missing_reasons.append(f"missing_samples:{len(state.samples)}/{sample_count}")
        if missing_reasons:
            state.passed = False
            state.reason = ";".join(missing_reasons)
            continue

        prices = [sample.price for sample in state.samples[:sample_count]]
        tolerance = state.price_tick / 2 + 1e-9
        a, _b, _c, _d, e, f = prices
        precheck_is_limit_up = (
            state.precheck_sample is not None
            and abs(state.precheck_sample.price - state.up_limit) <= tolerance
        )
        a_is_limit_up = abs(a - state.up_limit) <= tolerance
        a_rise_pct = (
            (a / state.pre_close - 1) * 100
            if state.pre_close > 0
            else -math.inf
        )
        a_rise_ok = (
            a_is_limit_up
            if first_rise_limit_up
            else a_rise_pct > first_min_rise_pct
        )
        monotonic_ok = all(
            prices[index] + tolerance >= prices[index + 1]
            for index in range(len(prices) - 1)
        )
        final_rise_pct = (
            (f / state.pre_close - 1) * 100
            if state.pre_close > 0
            else math.inf
        )
        final_rise_ok = FINAL_MIN_RISE_PCT <= final_rise_pct <= FINAL_MAX_RISE_PCT
        final_price_ok = not exclude_final_price_below_5 or f >= 5
        final_drop_smaller = (e - f) < (state.up_limit - e)
        rank_base_available = state.rank_base_close > 0
        rank_base_rise_pct = (
            (f / state.rank_base_close - 1) * 100
            if rank_base_available
            else math.inf
        )
        rank_base_rise_ok = (
            not rank_base_available
            or RANK_BASE_MIN_RISE_PCT <= rank_base_rise_pct <= RANK_BASE_MAX_RISE_PCT
        )
        prior_t2_drop_ok = (
            state.prior_t2_max_drop_pct is not None
            and state.prior_t2_max_drop_pct >= prior_max_drop_threshold_pct
        )
        prior_t1_drop_ok = (
            state.prior_t1_max_drop_pct is not None
            and state.prior_t1_max_drop_pct >= prior_max_drop_threshold_pct
        )
        prior_t1_rise_ok = (
            state.prior_t1_rise_pct is not None
            and state.prior_t1_rise_pct <= prior_t1_max_rise_pct
        )

        checks = {
            "precheck_not_limit_up": precheck_is_limit_up,
            (
                "a_not_limit_up"
                if first_rise_limit_up
                else f"a_rise_not_above_{first_min_rise_pct:g}"
            ): a_rise_ok,
            "monotonic_non_increasing": monotonic_ok,
            f"f_rise_outside_{FINAL_MIN_RISE_PCT:g}_to_{FINAL_MAX_RISE_PCT:g}": final_rise_ok,
            "f_price_below_5": final_price_ok,
            "rank_base_missing": rank_base_available,
            "f_rank_base_rise_outside_-10_to_10": rank_base_rise_ok,
            "last_drop_not_less_than_limit_to_e": final_drop_smaller,
        }
        if prior_max_drop_check:
            checks.update(
                {
                    f"prior_t2_max_drop_below_{prior_max_drop_threshold_pct:g}": prior_t2_drop_ok,
                    f"prior_t1_max_drop_below_{prior_max_drop_threshold_pct:g}": prior_t1_drop_ok,
                }
            )
        if prior_t1_rise_check:
            checks[f"prior_t1_rise_above_{prior_t1_max_rise_pct:g}"] = prior_t1_rise_ok
        state.passed = all(checks.values())
        state.reason = "PASS" if state.passed else ";".join(
            name for name, ok in checks.items() if not ok
        )
        if state.passed:
            selected.append(state)

    selected.sort(key=lambda item: item.code)
    logging.info(
        "evaluation finished: selected=%d rule=precheck_limit_up,%s,"
        "a>=b>=c>=d>=e>=f,f_rise[%.2f,%.2f],rank_f_vs_t3[%.2f,%.2f],"
        "(e-f)<(up_limit-e),exclude_f_price_below_5=%s,"
        "prior_max_drop_check=%s threshold=%.2f,"
        "prior_t1_rise_check=%s max=%.2f",
        len(selected),
        first_sample_rule_label(first_min_rise_pct, first_rise_limit_up),
        FINAL_MIN_RISE_PCT,
        FINAL_MAX_RISE_PCT,
        RANK_BASE_MIN_RISE_PCT,
        RANK_BASE_MAX_RISE_PCT,
        exclude_final_price_below_5,
        prior_max_drop_check,
        prior_max_drop_threshold_pct,
        prior_t1_rise_check,
        prior_t1_max_rise_pct,
    )
    return selected


def final_sample(state: StockState) -> SamplePoint | None:
    return state.samples[-1] if state.samples else None


def rank_f_minus_base(state: StockState) -> float | None:
    sample = final_sample(state)
    if sample is None or state.rank_base_close <= 0:
        return None
    return sample.price - state.rank_base_close


def rank_f_vs_base_pct(state: StockState) -> float | None:
    sample = final_sample(state)
    if sample is None or state.rank_base_close <= 0:
        return None
    return (sample.price / state.rank_base_close - 1) * 100


def sorted_order_targets(
    selected: list[StockState],
    max_order_stocks: int,
) -> list[StockState]:
    rankable = [
        state
        for state in selected
        if rank_f_vs_base_pct(state) is not None
    ]
    def sort_key(state: StockState) -> tuple[int, float, str]:
        ranking_pct = rank_f_vs_base_pct(state)
        # When the T-1 rise filter is disabled, prefer stocks that did not close
        # at their T-1 limit-up price; rank each group by F versus T-3 ascending.
        t1_limit_up_group = 1 if state.prior_t1_limit_up else 0
        return (t1_limit_up_group, ranking_pct or 0.0, state.code)

    return sorted(
        rankable,
        key=sort_key,
    )[:max_order_stocks]


def write_csv(
    output_dir: Path,
    states: list[StockState],
    sample_times: list[dt_time],
    filename: str,
    max_order_stocks: int,
) -> Path:
    path = output_dir / filename
    fields = [
        "passed",
        "reason",
        "code",
        "name",
        "pre_close",
        "up_limit",
        "price_tick",
        "preclose_source",
        "precheck_rise_pct",
        "precheck_p_091600",
        "precheck_quote_time_091600",
        "precheck_scan_time_091600",
        "precheck_source_091600",
        "first_rise_pct",
        "final_rise_pct",
        "order_rank",
        "rank_base_date",
        "rank_base_close",
        "rank_f_price",
        "rank_f_quote_time",
        "rank_f_minus_base",
        "rank_f_vs_base_pct",
        "prior_t2_date",
        "prior_t2_max_drop_pct",
        "prior_t1_date",
        "prior_t1_max_drop_pct",
        "prior_t1_rise_pct",
        "prior_t1_limit_up",
    ]
    for index, sample_time in enumerate(sample_times, start=1):
        label = fmt_time(sample_time).replace(":", "")
        fields.extend(
            [
                f"p{index}_{label}",
                f"rise{index}_{label}_pct",
                f"quote_time{index}_{label}",
                f"scan_time{index}_{label}",
                f"source{index}_{label}",
            ]
        )

    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        order_rank_by_code = {
            state.code: index
            for index, state in enumerate(
                sorted_order_targets(
                    [state for state in states if state.passed],
                    max_order_stocks,
                ),
                start=1,
            )
        }
        for state in states:
            samples_by_target = {
                sample.target_time: sample
                for sample in state.samples
            }
            first_sample = samples_by_target.get(sample_times[0])
            final_sample = samples_by_target.get(sample_times[-1])
            precheck_sample = state.precheck_sample
            precheck_rise = (
                (precheck_sample.price / state.pre_close - 1) * 100
                if precheck_sample is not None and state.pre_close > 0
                else ""
            )
            first_rise = (
                (first_sample.price / state.pre_close - 1) * 100
                if first_sample is not None and state.pre_close > 0
                else ""
            )
            final_rise = (
                (final_sample.price / state.pre_close - 1) * 100
                if final_sample is not None and state.pre_close > 0
                else ""
            )
            ranking_diff = rank_f_minus_base(state)
            ranking_pct = rank_f_vs_base_pct(state)
            row: dict[str, Any] = {
                "passed": state.passed,
                "reason": state.reason,
                "code": state.code,
                "name": state.name,
                "pre_close": round(state.pre_close, 4),
                "up_limit": round(state.up_limit, 4),
                "price_tick": state.price_tick,
                "preclose_source": state.preclose_source,
                "precheck_rise_pct": round(precheck_rise, 4)
                if precheck_rise != ""
                else "",
                "precheck_p_091600": round(precheck_sample.price, 4)
                if precheck_sample is not None
                else "",
                "precheck_quote_time_091600": fmt_datetime(precheck_sample.quote_time)
                if precheck_sample is not None
                else "",
                "precheck_scan_time_091600": fmt_datetime(precheck_sample.scan_time)
                if precheck_sample is not None
                else "",
                "precheck_source_091600": precheck_sample.price_source
                if precheck_sample is not None
                else "",
                "first_rise_pct": round(first_rise, 4) if first_rise != "" else "",
                "final_rise_pct": round(final_rise, 4) if final_rise != "" else "",
                "order_rank": order_rank_by_code.get(state.code, ""),
                "rank_base_date": state.rank_base_date.strftime("%Y-%m-%d")
                if state.rank_base_date is not None
                else "",
                "rank_base_close": round(state.rank_base_close, 4)
                if state.rank_base_close > 0
                else "",
                "rank_f_price": round(final_sample.price, 4)
                if final_sample is not None
                else "",
                "rank_f_quote_time": fmt_datetime(final_sample.quote_time)
                if final_sample is not None
                else "",
                "rank_f_minus_base": round(ranking_diff, 4)
                if ranking_diff is not None
                else "",
                "rank_f_vs_base_pct": round(ranking_pct, 4)
                if ranking_pct is not None
                else "",
                "prior_t2_date": state.prior_t2_date.strftime("%Y-%m-%d")
                if state.prior_t2_date is not None
                else "",
                "prior_t2_max_drop_pct": round(state.prior_t2_max_drop_pct, 4)
                if state.prior_t2_max_drop_pct is not None
                else "",
                "prior_t1_date": state.prior_t1_date.strftime("%Y-%m-%d")
                if state.prior_t1_date is not None
                else "",
                "prior_t1_max_drop_pct": round(state.prior_t1_max_drop_pct, 4)
                if state.prior_t1_max_drop_pct is not None
                else "",
                "prior_t1_rise_pct": round(state.prior_t1_rise_pct, 4)
                if state.prior_t1_rise_pct is not None
                else "",
                "prior_t1_limit_up": state.prior_t1_limit_up
                if state.prior_t1_limit_up is not None
                else "",
            }
            for index, sample_time in enumerate(sample_times, start=1):
                label = fmt_time(sample_time).replace(":", "")
                sample = samples_by_target.get(sample_time)
                if sample is not None:
                    rise_pct = (sample.price / state.pre_close - 1) * 100
                    row[f"p{index}_{label}"] = round(sample.price, 4)
                    row[f"rise{index}_{label}_pct"] = round(rise_pct, 4)
                    row[f"quote_time{index}_{label}"] = fmt_datetime(sample.quote_time)
                    row[f"scan_time{index}_{label}"] = fmt_datetime(sample.scan_time)
                    row[f"source{index}_{label}"] = sample.price_source
                else:
                    row[f"p{index}_{label}"] = ""
                    row[f"rise{index}_{label}_pct"] = ""
                    row[f"quote_time{index}_{label}"] = ""
                    row[f"scan_time{index}_{label}"] = ""
                    row[f"source{index}_{label}"] = ""
            writer.writerow(row)
    return path


def ths_sel_market_marker(code: str) -> int:
    symbol, _, market = code.upper().partition(".")
    if market == "SZ" or (not market and symbol.startswith(("0", "1", "2", "3"))):
        return 0x21
    if market == "SH" or (not market and symbol.startswith(("5", "6", "9"))):
        return 0x11
    if market == "BJ" or (not market and symbol.startswith(("4", "8"))):
        return 0x31
    raise ValueError(f"unsupported market for THS sel file: {code}")


def write_ths_sel(output_dir: Path, states: list[StockState], filename: str) -> Path:
    path = output_dir / filename
    if len(states) > 65535:
        raise ValueError(f"too many stocks for THS sel file: {len(states)}")

    payload = bytearray()
    payload.extend(len(states).to_bytes(2, "little"))
    for state in states:
        symbol, _, _market = state.code.upper().partition(".")
        if len(symbol) != 6 or not symbol.isdigit():
            raise ValueError(f"invalid stock code for THS sel file: {state.code}")
        payload.extend([0x07, ths_sel_market_marker(state.code)])
        payload.extend(symbol.encode("ascii"))

    path.write_bytes(payload)
    return path


def submit_selected_orders(
    session: TradeSession,
    selected: list[StockState],
    max_order_stocks: int,
    single_stock_asset_ratio: float,
) -> list[SubmittedOrder]:
    if not selected:
        logging.info("09:27 order skipped: no selected stocks")
        return []

    order_targets = sorted_order_targets(selected, max_order_stocks)
    if not order_targets:
        logging.info(
            "09:27 order skipped: no selected stocks with rank_base_close and f price"
        )
        return []
    logging.info(
        "09:27 order candidates: selected_total=%d order_top=%d max_order_stocks=%d "
        "sort=t1_not_limit_up_first,f_vs_rank_base_pct_asc codes=%s",
        len(selected),
        len(order_targets),
        max_order_stocks,
        ",".join(state.code for state in order_targets),
    )
    for index, state in enumerate(order_targets, start=1):
        sample = final_sample(state)
        logging.info(
            "09:27 order rank: rank=%d code=%s f_price=%.4f rank_base_date=%s "
            "rank_base_close=%.4f f_minus_base=%.4f f_vs_base_pct=%.4f "
            "prior_t1_limit_up=%s",
            index,
            state.code,
            sample.price if sample is not None else 0.0,
            state.rank_base_date.strftime("%Y-%m-%d")
            if state.rank_base_date is not None
            else "",
            state.rank_base_close,
            rank_f_minus_base(state) or 0.0,
            rank_f_vs_base_pct(state) or 0.0,
            state.prior_t1_limit_up,
        )

    asset = session.trader.query_stock_asset(session.account)
    if not asset:
        logging.error("09:27 order skipped: query_stock_asset returned empty")
        return []

    available_cash = safe_float(
        first_attr(
            asset,
            ("cash", "enable_balance", "available_cash", "m_dEnableBalance"),
            0,
        )
    )
    total_asset = safe_float(
        first_attr(asset, ("total_asset", "m_dBalance", "asset"), 0)
    )
    if available_cash <= 0 or total_asset <= 0:
        logging.error(
            "09:27 order skipped: invalid account asset available_cash=%.2f total_asset=%.2f",
            available_cash,
            total_asset,
        )
        return []

    stock_count = len(order_targets)
    single_stock_asset_cap = total_asset * single_stock_asset_ratio
    average_available_cash = available_cash / stock_count
    per_stock_budget = min(single_stock_asset_cap, average_available_cash)
    logging.info(
        "09:27 order allocation: stocks=%d available_cash=%.2f total_asset=%.2f "
        "single_stock_asset_cap=%.2f cap_ratio=%.2f%% "
        "average_available_cash=%.2f per_stock_budget=%.2f",
        stock_count,
        available_cash,
        total_asset,
        single_stock_asset_cap,
        single_stock_asset_ratio * 100,
        average_available_cash,
        per_stock_budget,
    )

    submitted = 0
    skipped = 0
    failed = 0
    submitted_orders: list[SubmittedOrder] = []
    for state in order_targets:
        order_price = round_to_tick(
            state.pre_close * (1 + ORDER_PRICE_RISE_PCT / 100),
            state.price_tick,
        )
        if order_price <= 0:
            failed += 1
            logging.error(
                "09:27 order skipped: %s %s invalid order price %.4f",
                state.code,
                state.name,
                order_price,
            )
            continue
        volume = int(per_stock_budget / order_price / LOT_SIZE) * LOT_SIZE
        order_amount = order_price * volume
        if volume < LOT_SIZE:
            skipped += 1
            logging.warning(
                "09:27 order skipped: %s %s budget=%.2f price=%.4f volume=%d",
                state.code,
                state.name,
                per_stock_budget,
                order_price,
                volume,
            )
            continue

        remark = f"{STRATEGY_NAME}_{datetime.now():%Y%m%d}_{state.code}"
        try:
            order_id = session.trader.order_stock(
                session.account,
                state.code,
                xtconstant.STOCK_BUY,
                volume,
                xtconstant.FIX_PRICE,
                order_price,
                STRATEGY_NAME,
                remark,
            )
        except Exception:
            failed += 1
            logging.exception(
                "09:27 order exception: %s %s price=%.4f volume=%d amount=%.2f",
                state.code,
                state.name,
                order_price,
                volume,
                order_amount,
            )
            continue

        success = safe_float(order_id) > 0
        if success:
            submitted += 1
            submitted_orders.append(
                SubmittedOrder(
                    order_id=safe_int(order_id),
                    code=state.code,
                    name=state.name,
                    volume=volume,
                )
            )
        else:
            failed += 1
        logging.info(
            "09:27 order: %s %s price=%.4f price_rise=%.2f%% volume=%d "
            "amount=%.2f budget=%.2f order_id=%s success=%s",
            state.code,
            state.name,
            order_price,
            ORDER_PRICE_RISE_PCT,
            volume,
            order_amount,
            per_stock_budget,
            order_id,
            success,
        )

    logging.info(
        "09:27 order summary: selected_total=%d order_targets=%d submitted=%d "
        "skipped=%d failed=%d",
        len(selected),
        stock_count,
        submitted,
        skipped,
        failed,
    )
    return submitted_orders


NON_CANCELABLE_ORDER_STATUSES = xtconstant_values(
    (
        "ORDER_REPORTED_CANCEL",
        "ORDER_PARTSUCC_CANCEL",
        "ORDER_PART_CANCEL",
        "ORDER_CANCELED",
        "ORDER_SUCCEEDED",
        "ORDER_JUNK",
        "ORDER_UNKNOWN",
    )
)


def cancel_unfilled_orders_at_1000(
    session: TradeSession,
    submitted_orders: list[SubmittedOrder],
) -> None:
    if not submitted_orders:
        logging.info("10:00 final cancel skipped: no submitted strategy orders")
        return

    try:
        queried_orders = session.trader.query_stock_orders(session.account, False) or []
    except Exception:
        logging.exception("10:00 final cancel skipped: query_stock_orders failed")
        return

    orders_by_id = {
        safe_int(first_attr(order, ("order_id", "m_nOrderID"), 0)): order
        for order in queried_orders
    }

    canceled = 0
    skipped = 0
    failed = 0
    for record in submitted_orders:
        order = orders_by_id.get(record.order_id)
        if order is None:
            skipped += 1
            logging.warning(
                "10:00 final cancel skipped: order not found order_id=%s code=%s",
                record.order_id,
                record.code,
            )
            continue

        status = first_attr(order, ("order_status", "m_nOrderStatus"), "")
        total_volume = safe_float(
            first_attr(order, ("order_volume", "m_nOrderVolume"), record.volume)
        )
        traded_volume = safe_float(
            first_attr(order, ("traded_volume", "m_nVolumeTraded"), 0)
        )
        if total_volume > 0 and traded_volume >= total_volume:
            skipped += 1
            logging.info(
                "10:00 final cancel skipped: fully traded order_id=%s code=%s "
                "status=%s traded=%.0f total=%.0f",
                record.order_id,
                record.code,
                status,
                traded_volume,
                total_volume,
            )
            continue

        if status in NON_CANCELABLE_ORDER_STATUSES:
            skipped += 1
            logging.info(
                "10:00 final cancel skipped: final status order_id=%s code=%s "
                "status=%s traded=%.0f total=%.0f",
                record.order_id,
                record.code,
                status,
                traded_volume,
                total_volume,
            )
            continue

        try:
            result = session.trader.cancel_order_stock(session.account, record.order_id)
        except Exception:
            failed += 1
            logging.exception(
                "10:00 final cancel exception: order_id=%s code=%s status=%s "
                "traded=%.0f total=%.0f",
                record.order_id,
                record.code,
                status,
                traded_volume,
                total_volume,
            )
            continue

        success = result == 0
        if success:
            canceled += 1
        else:
            failed += 1
        logging.info(
            "10:00 final cancel: order_id=%s code=%s name=%s status=%s "
            "traded=%.0f total=%.0f cancel_result=%s success=%s",
            record.order_id,
            record.code,
            record.name,
            status,
            traded_volume,
            total_volume,
            result,
            success,
        )

    logging.info(
        "10:00 final cancel summary: submitted=%d canceled=%d skipped=%d failed=%d",
        len(submitted_orders),
        canceled,
        skipped,
        failed,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MiniQMT jingjia price monitor and filter."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Jingjia QMT config path. Defaults to ./qmt_accounts_config.json",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=LOG_DIR,
        help="Directory for log and CSV outputs.",
    )
    parser.add_argument(
        "--sample-times",
        default=",".join(DEFAULT_SAMPLE_TIMES),
        help="Comma separated HH:MM:SS sample times.",
    )
    parser.add_argument(
        "--date",
        default="",
        help="Historical trade date, e.g. 20260703. Empty means live monitor mode.",
    )
    parser.add_argument(
        "--first-min-rise-pct",
        type=float,
        default=FIRST_SAMPLE_MIN_RISE_PCT,
        help="Minimum a-point rise percent versus previous close. Default is 5.",
    )
    parser.add_argument(
        "--first-rise-limit-up",
        action="store_true",
        help="Require the a-point price to be limit-up. Overrides --first-min-rise-pct.",
    )
    parser.add_argument(
        "--final-max-rise-pct",
        type=float,
        default=5.0,
        help="Deprecated; kept for compatibility and ignored by the current rule.",
    )
    parser.add_argument(
        "--max-selected",
        type=int,
        default=20,
        help="Deprecated; kept for compatibility and ignored by the current rule.",
    )
    parser.add_argument(
        "--max-order-stocks",
        type=int,
        default=MAX_ORDER_STOCKS,
        help="Max ranked selected stocks to order after T-3 percentage sorting.",
    )
    parser.add_argument(
        "--single-stock-asset-ratio-pct",
        type=float,
        default=SINGLE_STOCK_ASSET_RATIO * 100,
        help="Single-stock asset cap percent used in order allocation. Default is 30; 100 means no practical cap.",
    )
    parser.add_argument(
        "--prior-max-drop-check",
        action="store_true",
        help="Enable prior T-2/T-1 intraday max-drop check.",
    )
    parser.add_argument(
        "--prior-max-drop-threshold-pct",
        type=float,
        default=PRIOR_MAX_DROP_THRESHOLD_PCT,
        help="Minimum allowed T-2/T-1 intraday max-drop percent. Default is -9.",
    )
    parser.add_argument(
        "--prior-t1-rise-check",
        action="store_true",
        help="Enable T-1 daily rise check.",
    )
    parser.add_argument(
        "--prior-t1-max-rise-pct",
        type=float,
        default=PRIOR_T1_MAX_RISE_PCT,
        help="Maximum allowed T-1 daily rise percent. Default is 7.",
    )
    parser.add_argument(
        "--exclude-final-price-below-5",
        action="store_true",
        help="Exclude stocks whose f-point price is below 5.",
    )
    parser.add_argument(
        "--price-mode",
        choices=("last", "open", "auto"),
        default="last",
        help="Quote price field used as jingjia price. The reference strategy uses lastPrice.",
    )
    parser.add_argument(
        "--no-strict-root-check",
        action="store_true",
        help="Allow connecting to a MiniQMT root different from qmt_accounts_config.json.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.02,
        help="Scheduler sleep interval in seconds near sample time.",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=120.0,
        help="Live mode: connect and prepare this many seconds before the first sample time.",
    )
    parser.add_argument(
        "--startup-check-code",
        default="600000.SH",
        help="Live mode: fetch this stock once at startup to verify QMT data. Empty disables startup check.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Live mode: run one trade day and exit. Default keeps waiting for the next trade day.",
    )
    parser.add_argument(
        "--qmt-retry-interval",
        type=float,
        default=30.0,
        help="Live mode: seconds between QMT reconnect attempts when MiniQMT is unavailable.",
    )
    parser.add_argument(
        "--fresh-timeout",
        type=float,
        default=20.0,
        help="Live mode: seconds to wait for quote_time to reach each target time.",
    )
    parser.add_argument(
        "--fresh-retry-interval",
        type=float,
        default=0.2,
        help="Live mode: retry interval while waiting for fresh quotes.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Stock count per historical download/query chunk.",
    )
    parser.add_argument(
        "--daily-lookback-days",
        type=int,
        default=DAILY_DOWNLOAD_LOOKBACK_DAYS,
        help="Natural-day lookback window for daily bar download/check. Default is 30.",
    )
    parser.add_argument(
        "--daily-download-time",
        default="",
        help=(
            "Live mode only: when --download-daily is enabled, wait until this HH:MM:SS "
            "before downloading daily bars. Empty means download at 09:15 preparation."
        ),
    )
    parser.add_argument(
        "--download-daily-only",
        action="store_true",
        help="Only download recent daily bars for the live universe, then exit without trading.",
    )
    parser.add_argument(
        "--check-daily-ready",
        action="store_true",
        help="Only check whether live-universe T-3 ranking daily data is ready, then exit.",
    )
    parser.add_argument(
        "--max-historical-lag",
        type=float,
        default=5.0,
        help="Max seconds after each target time accepted for historical tick sampling.",
    )
    parser.add_argument(
        "--download-daily",
        dest="download_daily",
        action="store_true",
        default=True,
        help="Download daily bars before reading T-3/pre-close data. This is enabled by default.",
    )
    parser.add_argument(
        "--no-download-daily",
        dest="download_daily",
        action="store_false",
        help="Do not download daily bars before reading T-3/pre-close data.",
    )
    parser.add_argument(
        "--no-download-tick",
        action="store_true",
        help="Do not download historical tick before reading. Faster but may miss local-cache gaps.",
    )
    return parser.parse_args()


def run_historical(args: argparse.Namespace, sample_times: list[dt_time]) -> None:
    target_date = parse_target_date(args.date)
    qmt_config = load_qmt_config(args.config)
    logging.info("QMT account from config: %s", qmt_config.account_id)
    logging.info("QMT root from config: %s", qmt_config.qmt_root)
    logging.info("historical date: %s", target_date.strftime("%Y-%m-%d"))
    logging.info("precheck time: %s", fmt_time(PRECHECK_SAMPLE_TIME))
    logging.info("sample times: %s", ",".join(fmt_time(value) for value in sample_times))
    logging.info(
        "rule: precheck_limit_up %s a>=b>=c>=d>=e>=f "
        "f_rise[%.2f,%.2f] rank_f_vs_t3[%.2f,%.2f] "
        "(e-f)<(up_limit-e) exclude_f_price_below_5=%s "
        "prior_max_drop_check=%s threshold=%.2f "
        "prior_t1_rise_check=%s max=%.2f price_mode=%s",
        first_sample_rule_label(args.first_min_rise_pct, args.first_rise_limit_up),
        FINAL_MIN_RISE_PCT,
        FINAL_MAX_RISE_PCT,
        RANK_BASE_MIN_RISE_PCT,
        RANK_BASE_MAX_RISE_PCT,
        args.exclude_final_price_below_5,
        args.prior_max_drop_check,
        args.prior_max_drop_threshold_pct,
        args.prior_t1_rise_check,
        args.prior_t1_max_rise_pct,
        args.price_mode,
    )

    xtdata.enable_hello = False
    connect_market_service(qmt_config.qmt_root, strict_root_check=not args.no_strict_root_check)
    universe = load_universe()
    states = prepare_historical_stock_states(
        universe,
        target_date,
        args.chunk_size,
        args.download_daily,
    )
    if not states:
        raise RuntimeError("no usable stocks after historical preclose preparation")

    collect_historical_samples(
        target_date=target_date,
        sample_times=[PRECHECK_SAMPLE_TIME],
        states=states,
        price_mode=args.price_mode,
        chunk_size=args.chunk_size,
        max_lag_seconds=args.max_historical_lag,
        download_tick=not args.no_download_tick,
        store_as_precheck=True,
    )
    collect_historical_samples(
        target_date=target_date,
        sample_times=sample_times,
        states=states,
        price_mode=args.price_mode,
        chunk_size=args.chunk_size,
        max_lag_seconds=args.max_historical_lag,
        download_tick=not args.no_download_tick,
    )

    selected = evaluate_states(
        states,
        sample_count=len(sample_times),
        first_min_rise_pct=args.first_min_rise_pct,
        first_rise_limit_up=args.first_rise_limit_up,
        prior_max_drop_check=args.prior_max_drop_check,
        prior_max_drop_threshold_pct=args.prior_max_drop_threshold_pct,
        prior_t1_rise_check=args.prior_t1_rise_check,
        prior_t1_max_rise_pct=args.prior_t1_max_rise_pct,
        exclude_final_price_below_5=args.exclude_final_price_below_5,
    )
    order_selected = sorted_order_targets(selected, args.max_order_stocks)

    day = target_date.strftime("%Y%m%d")
    all_path = write_csv(
        args.output_dir,
        list(states.values()),
        sample_times,
        f"jingjia_filter_all_{day}.csv",
        args.max_order_stocks,
    )
    selected_path = write_csv(
        args.output_dir,
        order_selected,
        sample_times,
        f"jingjia_filter_selected_{day}.csv",
        args.max_order_stocks,
    )
    selected_sel_path = write_ths_sel(
        args.output_dir,
        order_selected,
        f"jingjia_filter_selected_{day}.sel",
    )

    logging.info("all result CSV: %s", all_path)
    logging.info("selected CSV: %s", selected_path)
    logging.info("selected THS sel: %s", selected_sel_path)
    if order_selected:
        logging.info("selected codes: %s", ",".join(item.code for item in order_selected))
    else:
        logging.info("selected codes: none")


def run_live_once(
    args: argparse.Namespace,
    sample_times: list[dt_time],
    qmt_config: QmtRuntimeConfig,
    do_startup_check: bool,
) -> date:
    clear_quote_cache()
    schedule_date = next_live_schedule_date([PRECHECK_SAMPLE_TIME] + sample_times)
    first_target = datetime.combine(schedule_date, PRECHECK_SAMPLE_TIME)
    warmup_seconds = max(0.0, args.warmup_seconds)
    warmup_target = first_target - timedelta(seconds=warmup_seconds)
    logging.info(
        "live schedule: trade_date=%s prepare_at=%s precheck_sample=%s warmup_at=%s warmup_seconds=%.1f",
        schedule_date.strftime("%Y-%m-%d"),
        fmt_datetime(datetime.combine(schedule_date, PREPARE_TIME)),
        fmt_datetime(first_target),
        fmt_datetime(warmup_target),
        warmup_seconds,
    )
    xtdata.enable_hello = False
    connected = False
    preloaded_universe: dict[str, StockMeta] | None = None
    daily_download_done = False
    if do_startup_check and str(args.startup_check_code).strip():
        connect_market_service_with_retry(
            qmt_config.qmt_root,
            strict_root_check=not args.no_strict_root_check,
            retry_interval_seconds=args.qmt_retry_interval,
        )
        connected = True
        log_startup_data_check(args.startup_check_code, args.price_mode)

    if args.download_daily and args.daily_download_time:
        download_time = parse_hhmmss(args.daily_download_time, "--daily-download-time")
        download_target = datetime.combine(schedule_date, download_time)
        if datetime.now() < download_target:
            logging.info(
                "waiting for daily download: trade_date=%s download_at=%s",
                schedule_date.strftime("%Y-%m-%d"),
                fmt_datetime(download_target),
            )
            wait_until(
                download_target,
                f"daily download {fmt_datetime(download_target)}",
                args.poll_interval,
            )
        if not connected:
            connect_market_service_with_retry(
                qmt_config.qmt_root,
                strict_root_check=not args.no_strict_root_check,
                retry_interval_seconds=args.qmt_retry_interval,
            )
            connected = True
        preloaded_universe = load_universe()
        download_daily_bars(
            list(preloaded_universe),
            schedule_date,
            args.chunk_size,
            args.daily_lookback_days,
        )
        daily_download_done = True

    if datetime.now() < warmup_target:
        logging.info(
            "waiting for next jingjia 09:16 precheck: trade_date=%s precheck_sample=%s warmup_at=%s",
            schedule_date.strftime("%Y-%m-%d"),
            fmt_datetime(first_target),
            fmt_datetime(warmup_target),
        )
        wait_until(
            warmup_target,
            f"warmup {fmt_datetime(warmup_target)}",
            args.poll_interval,
        )

    if not connected:
        connect_market_service_with_retry(
            qmt_config.qmt_root,
            strict_root_check=not args.no_strict_root_check,
            retry_interval_seconds=args.qmt_retry_interval,
        )
    prepare_target = datetime.combine(schedule_date, PREPARE_TIME)
    if datetime.now() < prepare_target:
        logging.info(
            "waiting for preclose preparation: trade_date=%s prepare_at=%s",
            schedule_date.strftime("%Y-%m-%d"),
            fmt_datetime(prepare_target),
        )
        wait_until(
            prepare_target,
            f"preclose preparation {fmt_datetime(prepare_target)}",
            args.poll_interval,
        )
    universe = preloaded_universe or load_universe()
    states = prepare_stock_states(
        universe,
        schedule_date,
        args.chunk_size,
        args.download_daily and not daily_download_done,
        args.daily_lookback_days,
    )
    if not states:
        raise RuntimeError("no usable stocks after preclose preparation")

    subscription_id = xtdata.subscribe_whole_quote(["SH", "SZ"], on_whole_quote)
    if subscription_id <= 0:
        raise RuntimeError(f"subscribe_whole_quote failed: {subscription_id}")
    logging.info("whole quote subscribed: %s", subscription_id)

    trade_session: TradeSession | None = None
    try:
        trade_session = connect_trade_account(qmt_config)
        active_codes = list(states)
        precheck_target = datetime.combine(schedule_date, PRECHECK_SAMPLE_TIME)
        wait_until(precheck_target, fmt_datetime(precheck_target), args.poll_interval)
        precheck_quote_min, precheck_quote_max = live_quote_window(
            precheck_target,
            PRECHECK_SAMPLE_TIME,
        )
        collect_sample(
            sample_time=PRECHECK_SAMPLE_TIME,
            minimum_quote_dt=precheck_quote_min,
            maximum_quote_dt=precheck_quote_max,
            collection_deadline=datetime.combine(schedule_date, PRECHECK_COLLECT_UNTIL),
            states=states,
            codes=active_codes,
            price_mode=args.price_mode,
            fresh_timeout_seconds=args.fresh_timeout,
            fresh_retry_interval=args.fresh_retry_interval,
            require_fresh_quote=True,
            store_as_precheck=True,
        )
        for sample_index, sample_time in enumerate(sample_times):
            target = datetime.combine(schedule_date, sample_time)
            wait_until(target, fmt_datetime(target), args.poll_interval)
            minimum_quote_dt, maximum_quote_dt = live_quote_window(
                target,
                sample_time,
                sample_index,
            )

            collect_sample(
                sample_time=sample_time,
                minimum_quote_dt=minimum_quote_dt,
                maximum_quote_dt=maximum_quote_dt,
                collection_deadline=(
                    datetime.combine(schedule_date, FINAL_SAMPLE_COLLECT_UNTIL)
                    if sample_index == len(sample_times) - 1
                    else None
                ),
                states=states,
                codes=active_codes,
                price_mode=args.price_mode,
                fresh_timeout_seconds=args.fresh_timeout,
                fresh_retry_interval=args.fresh_retry_interval,
                require_fresh_quote=True,
            )

        selected = evaluate_states(
            states,
            sample_count=len(sample_times),
            first_min_rise_pct=args.first_min_rise_pct,
            first_rise_limit_up=args.first_rise_limit_up,
            prior_max_drop_check=args.prior_max_drop_check,
            prior_max_drop_threshold_pct=args.prior_max_drop_threshold_pct,
            prior_t1_rise_check=args.prior_t1_rise_check,
            prior_t1_max_rise_pct=args.prior_t1_max_rise_pct,
            exclude_final_price_below_5=args.exclude_final_price_below_5,
        )
        order_selected = sorted_order_targets(selected, args.max_order_stocks)

        day = schedule_date.strftime("%Y%m%d")
        all_path = write_csv(
            args.output_dir,
            list(states.values()),
            sample_times,
            f"jingjia_filter_all_{day}.csv",
            args.max_order_stocks,
        )
        selected_path = write_csv(
            args.output_dir,
            order_selected,
            sample_times,
            f"jingjia_filter_selected_{day}.csv",
            args.max_order_stocks,
        )
        selected_sel_path = write_ths_sel(
            args.output_dir,
            order_selected,
            f"jingjia_filter_selected_{day}.sel",
        )

        logging.info("all result CSV: %s", all_path)
        logging.info("selected CSV: %s", selected_path)
        logging.info("selected THS sel: %s", selected_sel_path)
        if order_selected:
            logging.info("selected codes: %s", ",".join(item.code for item in order_selected))
            order_target = datetime.combine(schedule_date, ORDER_TIME)
            wait_until(order_target, fmt_datetime(order_target), args.poll_interval)
            submitted_orders = submit_selected_orders(
                trade_session,
                selected,
                args.max_order_stocks,
                args.single_stock_asset_ratio_pct / 100,
            )
            if submitted_orders:
                final_cancel_target = datetime.combine(schedule_date, FINAL_CANCEL_TIME)
                wait_until(
                    final_cancel_target,
                    fmt_datetime(final_cancel_target),
                    args.poll_interval,
                )
                cancel_unfilled_orders_at_1000(trade_session, submitted_orders)
        else:
            logging.info("selected codes: none")
        return schedule_date
    finally:
        if subscription_id > 0:
            try:
                xtdata.unsubscribe_quote(subscription_id)
            except Exception:
                logging.exception("unsubscribe quote failed")
        stop_trade_account(trade_session)


def run_download_daily_only(args: argparse.Namespace) -> None:
    qmt_config = load_qmt_config(args.config)
    target_date = next_live_schedule_date([PRECHECK_SAMPLE_TIME])
    logging.info("download daily only: trade_date=%s", target_date.strftime("%Y-%m-%d"))
    connect_market_service(qmt_config.qmt_root, strict_root_check=not args.no_strict_root_check)
    universe = load_universe()
    download_daily_bars(
        list(universe),
        target_date,
        args.chunk_size,
        args.daily_lookback_days,
    )


def run_check_daily_ready(args: argparse.Namespace) -> None:
    qmt_config = load_qmt_config(args.config)
    target_date = next_live_schedule_date([PRECHECK_SAMPLE_TIME])
    logging.info("check daily ready only: trade_date=%s", target_date.strftime("%Y-%m-%d"))
    connect_market_service(qmt_config.qmt_root, strict_root_check=not args.no_strict_root_check)
    universe = load_universe()
    ready = check_recent_daily_ready(
        universe,
        args.chunk_size,
        args.daily_lookback_days,
    )
    if not ready:
        raise SystemExit(2)


def main() -> None:
    args = parse_args()
    if args.first_min_rise_pct < 0:
        raise ValueError("--first-min-rise-pct must be non-negative")
    if args.max_order_stocks < 1:
        raise ValueError("--max-order-stocks must be at least 1")
    if not 0 < args.single_stock_asset_ratio_pct <= 100:
        raise ValueError("--single-stock-asset-ratio-pct must be in (0, 100]")
    if args.daily_lookback_days < 3:
        raise ValueError("--daily-lookback-days must be at least 3")
    sample_times = parse_sample_times(args.sample_times)
    if len(sample_times) != 6:
        raise ValueError("--sample-times must contain exactly 6 times for a,b,c,d,e,f")
    configure_logging(args.output_dir)

    if args.download_daily_only:
        run_download_daily_only(args)
        return
    if args.check_daily_ready:
        run_check_daily_ready(args)
        return

    if args.date:
        run_historical(args, sample_times)
        return

    qmt_config = load_qmt_config(args.config)
    logging.info("QMT account from config: %s", qmt_config.account_id)
    logging.info("QMT root from config: %s", qmt_config.qmt_root)
    logging.info("preclose preparation time: %s", fmt_time(PREPARE_TIME))
    logging.info("precheck time: %s", fmt_time(PRECHECK_SAMPLE_TIME))
    logging.info("sample times: %s", ",".join(fmt_time(value) for value in sample_times))
    logging.info(
        "live quote_time windows: precheck=09:16:00~09:18:00 "
        "a=09:20:00~09:20:20 b=09:20:55~09:21:20 "
        "c=09:21:55~09:22:20 d=09:22:55~09:23:20 "
        "e=09:23:55~09:24:20 f=09:25:00~09:26:59 "
        "precheck_collect_until=%s f_collect_until=%s "
        "fresh_timeout=%.1fs retry_interval=%.1fs",
        fmt_time(PRECHECK_COLLECT_UNTIL),
        fmt_time(FINAL_SAMPLE_COLLECT_UNTIL),
        args.fresh_timeout,
        args.fresh_retry_interval,
    )
    logging.info(
        "order rule: time=%s top=%d sort=f_vs_t3_pct_asc price=pre_close+%.2f%% per_stock_budget="
        "min(total_asset*%.2f,available_cash/order_target_count) final_cancel=%s",
        fmt_time(ORDER_TIME),
        args.max_order_stocks,
        ORDER_PRICE_RISE_PCT,
        args.single_stock_asset_ratio_pct / 100,
        fmt_time(FINAL_CANCEL_TIME),
    )
    logging.info(
        "rule: precheck_limit_up %s a>=b>=c>=d>=e>=f "
        "f_rise[%.2f,%.2f] rank_f_vs_t3[%.2f,%.2f] "
        "(e-f)<(up_limit-e) exclude_f_price_below_5=%s "
        "prior_t1_rise_check=%s max=%.2f price_mode=%s once=%s",
        first_sample_rule_label(args.first_min_rise_pct, args.first_rise_limit_up),
        FINAL_MIN_RISE_PCT,
        FINAL_MAX_RISE_PCT,
        RANK_BASE_MIN_RISE_PCT,
        RANK_BASE_MAX_RISE_PCT,
        args.exclude_final_price_below_5,
        args.prior_t1_rise_check,
        args.prior_t1_max_rise_pct,
        args.price_mode,
        args.once,
    )

    first_cycle = True
    while True:
        try:
            finished_date = run_live_once(
                args,
                sample_times,
                qmt_config,
                do_startup_check=first_cycle,
            )
            first_cycle = False
            if args.once:
                logging.info("once mode enabled; exit after trade_date=%s", finished_date)
                return
            logging.info(
                "trade_date=%s finished; keep running and wait for next trade day",
                finished_date,
            )
        except KeyboardInterrupt:
            logging.info("received Ctrl+C; exiting")
            raise
        except Exception as exc:
            first_cycle = False
            logging.exception(
                "QMT disconnected or live cycle failed: %s; wait %.1fs then reconnect",
                exc,
                args.qmt_retry_interval,
            )
            time.sleep(max(1.0, args.qmt_retry_interval))


if __name__ == "__main__":
    main()
