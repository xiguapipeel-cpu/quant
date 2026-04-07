"""
每日主力建仓选股脚本（两阶段信号）
────────────────────────────────────────────
执行流程：
  1. 调用 DynamicScreener (major_capital_accumulation 预设) 从全A股筛选候选股
  2. 拉取每只股票最近120天日线（需要足够计算建仓期）
  3. 运行 MajorCapitalAccumulationStrategy
  4. 分别收集 WATCH（建仓中）和 BUY（建仓完毕即将拉升）信号
  5. 多渠道推送结果

独立运行：
  cd /Users/zhuzhu/Documents/quant_system
  python -m scripts.daily_major_capital_scan

被 scheduler 调用：
  from scripts.daily_major_capital_scan import run_daily_scan
  await run_daily_scan()
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from utils.logger import setup_logger

logger = setup_logger("daily_scan")

# 往前取多少天日线（建仓策略需要更长的回看期）
LOOKBACK_DAYS = 120
# 认为"近期"有信号的天数窗口（主力建仓是慢过程，窗口适当放宽）
SIGNAL_WINDOW_DAYS = 10
# 最多并发拉取的股票数（避免被限速）
MAX_CONCURRENT = 8


async def _fetch_bars(code: str, market: str, end_date: str) -> list[dict] | None:
    """拉取单只股票近 LOOKBACK_DAYS 天的日线"""
    from backtest.data_loader import BacktestDataLoader
    start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    loader = BacktestDataLoader()
    return await loader.load_daily_bars(code, market, start_date, end_date, adjust="qfq")


def _calc_match_score(signal_dates: list[dict], latest, signal_type: str, today_str: str) -> dict:
    """
    策略贴合度评分（0~100），返回总分及各维度明细。
    """
    # ── 维度1：信号密度（0~25） ──
    n_signals = len(signal_dates)
    density = min(25, int(n_signals * 8 - 3)) if n_signals >= 1 else 0
    density = max(0, density)

    # ── 维度2：策略置信度（0~30） ──
    conf = getattr(latest, "confidence", 0) if latest else 0
    confidence = round(conf * 30)

    # ── 维度3：当前状态（0~20） ──
    has_buy = any(d["type"] == "BUY" for d in signal_dates)
    if signal_type == "BUY":
        status = 20
    elif has_buy:
        status = 14
    else:
        status = min(10, n_signals * 3)

    # ── 维度4：信号时效（0~25） ──
    try:
        today_dt = datetime.strptime(today_str, "%Y-%m-%d")
        latest_dt = datetime.strptime(latest.date, "%Y-%m-%d") if latest else today_dt
        days_ago = (today_dt - latest_dt).days
    except Exception:
        days_ago = 5
    recency = {0: 25, 1: 22, 2: 18, 3: 14, 4: 10}.get(days_ago, 5)

    total = max(0, min(100, density + confidence + status + recency))
    return {
        "total":      total,
        "density":    density,
        "confidence": confidence,
        "status":     status,
        "recency":    recency,
    }


async def _analyze_stock(stock: dict, today_str: str, semaphore: asyncio.Semaphore) -> dict | None:
    """拉取日线并运行策略，有近期 WATCH/BUY 信号则返回结果"""
    async with semaphore:
        code   = stock["code"]
        market = stock.get("market", "SZ")
        name   = stock.get("name", "")

        bars = await _fetch_bars(code, market, today_str)
        if not bars or len(bars) < 60:
            logger.debug(f"[扫描] {code} {name} 数据不足，跳过")
            return None

        extra = {
            "integrity_pass":   True,
            "verified_sources": ["AKShare"],
            "pe":               stock.get("pe"),
            "market_cap":       stock.get("cap_yi"),
        }

        from backtest.strategies import MajorCapitalAccumulationStrategy
        strategy = MajorCapitalAccumulationStrategy()
        signals = strategy.generate_signals(code, bars, extra)

        # 收集全部 WATCH / BUY 信号（用于展示完整建仓时间线）
        all_watch = [s for s in signals if s.action == "WATCH"]
        all_buys  = [s for s in signals if s.action == "BUY"]

        # 至少近 SIGNAL_WINDOW_DAYS 内要有信号，才纳入标的池
        cutoff = (datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=SIGNAL_WINDOW_DAYS)).strftime("%Y-%m-%d")
        recent_watch = [s for s in all_watch if s.date >= cutoff]
        recent_buys  = [s for s in all_buys  if s.date >= cutoff]

        if not recent_watch and not recent_buys:
            return None

        # 最新信号决定当前状态（BUY 优先）
        if recent_buys:
            latest = recent_buys[-1]
            signal_type = "BUY"
        else:
            latest = recent_watch[-1]
            signal_type = "WATCH"

        # 构建全部信号日列表（按日期升序）
        signal_dates = []
        for s in all_watch:
            signal_dates.append({"date": s.date, "type": "WATCH", "reason": (s.reason or "")[:40]})
        for s in all_buys:
            signal_dates.append({"date": s.date, "type": "BUY", "reason": (s.reason or "")[:40]})
        signal_dates.sort(key=lambda x: x["date"])

        # ── 策略贴合度评分（0~100）──
        match_score = _calc_match_score(
            signal_dates=signal_dates,
            latest=latest,
            signal_type=signal_type,
            today_str=today_str,
        )

        # 从最新一根日线提取当天涨幅
        last_bar = bars[-1] if bars else {}
        pct_change = last_bar.get("pct_change")

        return {
            **stock,
            "pct_change":    round(pct_change, 2) if pct_change is not None else None,
            "signal_type":   signal_type,
            "signal_date":   latest.date,
            "signal_price":  latest.price,
            "signal_reason": latest.reason,
            "confidence":    latest.confidence,
            "signal_dates":  signal_dates,
            "match_score":   match_score,
        }


def _safe_end_date() -> str:
    """
    返回数据截止日期：
    - 盘中（09:30~15:00 交易时段内）→ 上一个已完整收盘的交易日，避免使用盘中不完整 bar
    - 其他时段（盘前/盘后/非交易日）→ 今日（当日 bar 已完整或尚未产生）
    """
    from holidays.trading_calendar import TradingCalendar
    cal = TradingCalendar()
    if cal.is_market_open():
        # 盘中：用上一个完整收盘日
        prev = cal.prev_trading_day()
        return prev.strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


async def run_daily_scan(
    trigger: str = "定时",
    notify_wechat: bool = True,
    update_web_state: bool = True,
) -> list[dict]:
    """
    执行完整的每日主力建仓选股扫描。
    Returns: 有近期 WATCH/BUY 信号的股票列表
    """
    scan_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    today_str = _safe_end_date()  # 盘中触发时自动退回上一完整收盘日
    logger.info(f"[日扫描] 开始 {trigger} 主力建仓选股 @ {scan_time} | 数据截止={today_str}")

    # ── Step 1：初筛候选股 ────────────────────────────────
    try:
        from backtest.screener import DynamicScreener, SCREEN_PRESETS
        params = SCREEN_PRESETS["major_capital_accumulation"]["params"]
        screener = DynamicScreener(**params)
        candidates = await screener.screen(use_cache_hours=2)
        logger.info(f"[日扫描] 初筛候选股 {len(candidates)} 只")
    except Exception as e:
        logger.error(f"[日扫描] 初筛失败: {e}")
        candidates = []

    if not candidates:
        logger.warning("[日扫描] 无候选股，终止")
        if notify_wechat:
            from notifications.push import pusher
            force = trigger in ("手动", "测试")
            await pusher.send_major_capital_scan([], [], scan_time, trigger, ignore_trading_day=force)
        return []

    # ── Step 2：并发拉线 + 跑策略信号 ────────────────────
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = [_analyze_stock(s, today_str, semaphore) for s in candidates]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    hit_stocks = []
    for r in raw_results:
        if isinstance(r, Exception):
            logger.warning(f"[日扫描] 分析异常: {r}")
        elif r is not None:
            hit_stocks.append(r)

    # 分成 WATCH 和 BUY 两组
    watch_stocks = [s for s in hit_stocks if s.get("signal_type") == "WATCH"]
    buy_stocks   = [s for s in hit_stocks if s.get("signal_type") == "BUY"]

    # 按信号日降序排列（最新信号排前面），同日按置信度降序
    buy_stocks.sort(key=lambda x: (x.get("signal_date", ""), x.get("confidence", 0)), reverse=True)
    watch_stocks.sort(key=lambda x: (x.get("signal_date", ""), x.get("confidence", 0)), reverse=True)

    logger.info(f"[日扫描] 命中 BUY={len(buy_stocks)} WATCH={len(watch_stocks)}")
    for s in buy_stocks[:5]:
        logger.info(f"  BUY   {s['code']} {s['name']} 信号日={s['signal_date']} 置信={s.get('confidence',0):.2f}")
    for s in watch_stocks[:5]:
        logger.info(f"  WATCH {s['code']} {s['name']} 信号日={s['signal_date']}")

    # ── Step 3：多渠道推送 ─────────────────────────────────
    if notify_wechat:
        try:
            from notifications.push import pusher
            force = trigger in ("手动", "测试")
            result = await pusher.send_major_capital_scan(
                buy_stocks, watch_stocks, scan_time, trigger, ignore_trading_day=force
            )
            if result.get("skipped"):
                logger.info("[日扫描] 非交易日静音，跳过推送")
            elif result.get("error"):
                logger.warning(f"[日扫描] 推送失败: {result['error']}")
            else:
                logger.info(f"[日扫描] 推送完成: {result.get('success_count',0)}/{result.get('total',0)} 渠道成功")
        except Exception as e:
            logger.error(f"[日扫描] 推送异常: {e}")

    # ── Step 4：更新 Web 状态（增量合并，不丢失旧标的） ───
    if update_web_state:
        try:
            from web.app import state
            from db.scan_dao import load_scan, upsert_scan

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            def _fmt(s, scan_time_override=None):
                return {
                    "code":          s.get("code", ""),
                    "name":          s.get("name", ""),
                    "market":        s.get("market", "SZ"),
                    "price":         s.get("price", 0),
                    "cap_yi":        s.get("cap_yi", 0),
                    "amount_wan":    s.get("amount_wan", 0),
                    "pe":            s.get("pe"),
                    "pct_change":    s.get("pct_change"),
                    "signal_type":   s.get("signal_type", ""),
                    "signal_date":   s.get("signal_date", ""),
                    "signal_reason": s.get("signal_reason", ""),
                    "confidence":    s.get("confidence", 0),
                    "signal_dates":  s.get("signal_dates", []),
                    "match_score":   s.get("match_score", 0),
                    "scan_time":     scan_time_override or now_str,
                    "integrity_pass": True,
                }

            # 新扫描到的标的 → scan_time = 当前时间
            new_map = {s.get("code"): _fmt(s, now_str) for s in hit_stocks}

            # 从 DB 读取上一次的标的池，保留仍在有效期内的旧标的
            # 保留条件：距上次被扫描发现（scan_time）不超过 RETAIN_DAYS 天
            RETAIN_DAYS = 7
            retain_cutoff = (datetime.strptime(today_str, "%Y-%m-%d")
                             - timedelta(days=RETAIN_DAYS)).strftime("%Y-%m-%d")
            old_rows = await load_scan("major_capital_accumulation")
            kept = 0
            for old in old_rows:
                code = old.get("code", "")
                if code in new_map:
                    # 已在新扫描中 → 合并 signal_dates（旧的多次信号日不丢失）
                    existing = new_map[code]
                    old_dates = {d.get("date") for d in (old.get("signal_dates") or [])}
                    new_dates = existing.get("signal_dates") or []
                    for od in (old.get("signal_dates") or []):
                        if od.get("date") not in {nd.get("date") for nd in new_dates}:
                            new_dates.append(od)
                    existing["signal_dates"] = sorted(new_dates, key=lambda x: x.get("date", ""))
                    continue
                # 旧标的：保留原始 scan_time，不刷新
                old_scan_time = str(old.get("scan_time", ""))[:10]
                if old_scan_time >= retain_cutoff:
                    # 保留旧标的，使用其原始 scan_time
                    old["scan_time"] = str(old.get("scan_time", now_str))
                    new_map[code] = old
                    kept += 1

            merged = list(new_map.values())
            logger.info(f"[日扫描] 合并结果: 新增/更新={len(hit_stocks)} 保留旧标的={kept} 总计={len(merged)}")

            state["scan_results"] = merged
            state["last_scan_time"] = datetime.now()
            state["scan_preset"] = "major_capital_accumulation"
            state["scan_strategy"] = "major_capital_accumulation"
            state.setdefault("scan_results_by_strategy", {})
            state["scan_results_by_strategy"]["major_capital_accumulation"] = merged
            # 持久化到 MySQL
            await upsert_scan("major_capital_accumulation", merged)
            logger.info(f"[日扫描] Web 状态已更新并持久化 | BUY={len(buy_stocks)} WATCH={len(watch_stocks)}")
        except Exception as e:
            logger.debug(f"[日扫描] 更新 Web 状态失败（可能非 Web 进程）: {e}")

    return hit_stocks


if __name__ == "__main__":
    asyncio.run(run_daily_scan(trigger="手动", notify_wechat=True, update_web_state=False))
