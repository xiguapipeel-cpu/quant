"""
策略信号分析器 - 公共模块
────────────────────────────────────────────
实时行情模式和本地数据库模式共用的 Backtrader 分析管道。

用法：
    from backtest.strategy_analyzer import analyze_stocks

    # candidates 可来自 DynamicScreener 或 LocalScreener
    results = await analyze_stocks(candidates, strategy_name="major_capital_accumulation")
    # results: [{...stock_fields..., signal_date, signal_type, match_score, signal_dates}, ...]
"""

import asyncio
from datetime import datetime, timedelta
from utils.logger import setup_logger

logger = setup_logger("strategy_analyzer")

# ── 常量 ──────────────────────────────────────────────────────
LOOKBACK_DAYS      = 120   # 拉取多少天历史日线
SIGNAL_WINDOW_DAYS = 10    # 近期信号窗口（超出此范围视为无信号）
MAX_CONCURRENT     = 8     # 最大并发分析数


# ── 工具函数 ──────────────────────────────────────────────────

def safe_end_date() -> str:
    """
    返回当前安全截止日期：
    - 盘中（09:30~15:00）→ 退回上一完整收盘日，避免使用不完整 bar
    - 其他时段 → 今日
    """
    try:
        from holidays.trading_calendar import TradingCalendar
        cal = TradingCalendar()
        if cal.is_market_open():
            return cal.prev_trading_day().strftime("%Y-%m-%d")
    except Exception:
        pass
    return datetime.now().strftime("%Y-%m-%d")


def calc_match_score(signal_dates: list[dict], latest, signal_type: str, today_str: str) -> dict:
    """
    策略贴合度评分（0~100），返回总分及各维度明细。

    维度：
      density     (0~25)  信号频率：吸筹出现次数
      confidence  (0~30)  策略内置置信度
      status      (0~20)  当前状态：BUY > 曾有BUY > WATCH
      recency     (0~25)  最新信号距今天数
    """
    n = len(signal_dates)

    density = max(0, min(25, n * 8 - 3)) if n >= 1 else 0

    conf = getattr(latest, "confidence", 0) if latest else 0
    confidence = round(conf * 30)

    has_buy = any(d["type"] == "BUY" for d in signal_dates)
    if signal_type == "BUY":
        status = 20
    elif has_buy:
        status = 14
    else:
        status = min(10, n * 3)

    try:
        today_dt  = datetime.strptime(today_str, "%Y-%m-%d")
        latest_dt = datetime.strptime(latest.date, "%Y-%m-%d") if latest else today_dt
        days_ago  = (today_dt - latest_dt).days
    except Exception:
        days_ago = 5
    recency = {0: 25, 1: 22, 2: 18, 3: 14, 4: 10}.get(days_ago, 5)

    total = max(0, min(100, density + confidence + status + recency))
    return {"total": total, "density": density, "confidence": confidence,
            "status": status, "recency": recency}


async def _fetch_bars(code: str, market: str, end_date: str) -> list[dict] | None:
    """拉取单只股票近 LOOKBACK_DAYS 天日线（优先读本地缓存/MySQL）"""
    from backtest.data_loader import BacktestDataLoader
    start_date = (
        datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")
    loader = BacktestDataLoader()
    return await loader.load_daily_bars(code, market, start_date, end_date, adjust="qfq")


async def _analyze_one(
    stock: dict,
    strategy_cls,
    today_str: str,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    """
    对单只股票执行策略信号分析。
    有近期信号则返回含 signal_date / signal_dates / match_score 的完整结果，否则返回 None。
    """
    async with semaphore:
        code   = stock["code"]
        market = stock.get("market", "SZ")
        name   = stock.get("name", "")

        bars = await _fetch_bars(code, market, today_str)
        if not bars or len(bars) < 60:
            logger.debug(f"[分析] {code} {name} 数据不足（{len(bars) if bars else 0}行），跳过")
            return None

        extra = {
            "integrity_pass":   True,
            "verified_sources": ["local_db"],
            "pe":               stock.get("pe"),
            "market_cap":       stock.get("cap_yi"),
        }

        strategy = strategy_cls()
        signals  = strategy.generate_signals(code, bars, extra)

        all_watch = [s for s in signals if s.action == "WATCH"]
        all_buys  = [s for s in signals if s.action == "BUY"]

        cutoff = (
            datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=SIGNAL_WINDOW_DAYS)
        ).strftime("%Y-%m-%d")
        recent_watch = [s for s in all_watch if s.date >= cutoff]
        recent_buys  = [s for s in all_buys  if s.date >= cutoff]

        if not recent_watch and not recent_buys:
            return None

        if recent_buys:
            latest      = recent_buys[-1]
            signal_type = "BUY"
        else:
            latest      = recent_watch[-1]
            signal_type = "WATCH"

        signal_dates = sorted(
            [{"date": s.date, "type": "WATCH", "reason": (s.reason or "")[:40]} for s in all_watch]
            + [{"date": s.date, "type": "BUY",   "reason": (s.reason or "")[:40]} for s in all_buys],
            key=lambda x: x["date"],
        )

        match_score = calc_match_score(signal_dates, latest, signal_type, today_str)

        last_bar   = bars[-1] if bars else {}
        pct_change = last_bar.get("pct_change")

        return {
            **stock,
            "pct_change":    round(pct_change, 2) if pct_change is not None else stock.get("pct_change"),
            "signal_type":   signal_type,
            "signal_date":   latest.date,
            "signal_price":  latest.price,
            "signal_reason": latest.reason,
            "confidence":    latest.confidence,
            "signal_dates":  signal_dates,
            "match_score":   match_score,
        }


# ── 主入口 ────────────────────────────────────────────────────

async def analyze_stocks(
    candidates: list[dict],
    strategy_name: str = "major_capital_accumulation",
    today_str: str | None = None,
    max_concurrent: int = MAX_CONCURRENT,
) -> list[dict]:
    """
    对候选股列表执行策略信号分析。

    参数：
        candidates    任意来源的股票列表（实时行情或本地数据库），
                      需包含字段：code, market, name
        strategy_name 策略名称，对应 BUILTIN_STRATEGIES 注册表
        today_str     数据截止日期（默认自动判断）
        max_concurrent 最大并发数

    返回：
        有近期 WATCH/BUY 信号的股票列表，每项包含：
        signal_type, signal_date, signal_dates, match_score, confidence, ...
    """
    from backtest.strategies import BUILTIN_STRATEGIES

    strategy_cls = BUILTIN_STRATEGIES.get(strategy_name)
    if strategy_cls is None:
        logger.warning(f"[分析器] 未知策略: {strategy_name}，可用: {list(BUILTIN_STRATEGIES)}")
        return []

    if today_str is None:
        today_str = safe_end_date()

    logger.info(
        f"[分析器] 策略={strategy_name} | 候选={len(candidates)}只 | "
        f"截止={today_str} | 并发={max_concurrent}"
    )

    semaphore = asyncio.Semaphore(max_concurrent)
    tasks     = [_analyze_one(s, strategy_cls, today_str, semaphore) for s in candidates]
    raw       = await asyncio.gather(*tasks, return_exceptions=True)

    results = []
    errors  = 0
    for r in raw:
        if isinstance(r, Exception):
            errors += 1
            logger.debug(f"[分析器] 分析异常: {r}")
        elif r is not None:
            results.append(r)

    buy_count   = sum(1 for r in results if r.get("signal_type") == "BUY")
    watch_count = sum(1 for r in results if r.get("signal_type") == "WATCH")
    logger.info(
        f"[分析器] 完成 | 命中={len(results)}只 "
        f"(BUY={buy_count} WATCH={watch_count}) | 异常={errors}"
    )
    return results
