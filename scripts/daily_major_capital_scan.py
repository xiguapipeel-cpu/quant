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
from backtest.strategy_analyzer import analyze_stocks, safe_end_date

logger = setup_logger("daily_scan")


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
    today_str = safe_end_date()
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

    # ── Step 2：并发拉线 + 跑策略信号（公共分析器）────────
    hit_stocks = await analyze_stocks(
        candidates,
        strategy_name="major_capital_accumulation",
        today_str=today_str,
    )

    watch_stocks = [s for s in hit_stocks if s.get("signal_type") == "WATCH"]
    buy_stocks   = [s for s in hit_stocks if s.get("signal_type") == "BUY"]
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
