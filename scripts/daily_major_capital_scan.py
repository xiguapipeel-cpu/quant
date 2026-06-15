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
    scan_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

    from config.execution_rules import rank_buy_signals, rank_score

    watch_stocks = [s for s in hit_stocks if s.get("signal_type") == "WATCH"]
    buy_stocks   = [s for s in hit_stocks if s.get("signal_type") == "BUY"]
    # 排序买入：多因子综合分（confidence/RSI/watch_days/yy_ratio/bb_narrow/突破强度/成交额）
    buy_stocks   = rank_buy_signals(buy_stocks)
    watch_stocks.sort(key=lambda x: (x.get("signal_date", ""), x.get("confidence", 0)), reverse=True)

    logger.info(f"[日扫描] 命中 BUY={len(buy_stocks)} WATCH={len(watch_stocks)}")
    for s in buy_stocks[:5]:
        logger.info(f"  BUY   {s['code']} {s['name']} 信号日={s['signal_date']} "
                    f"置信={s.get('confidence',0):.2f} 排序分={s.get('rank_score',0):.3f}")
    for s in watch_stocks[:5]:
        logger.info(f"  WATCH {s['code']} {s['name']} 信号日={s['signal_date']}")

    # ── Step 2.5：完整记录所有 WATCH / BUY 信号事件 ─────────────
    # 不依赖 scan_results 的 7 日保留池；每次扫描把 signal_dates 内的每个事件直接 upsert。
    try:
        from db.pattern_dao import upsert_event
        from config.strategy_versions import (
            MAJOR_CAPITAL_FROZEN_VERSION,
            major_capital_param_snapshot,
        )
        param_snapshot = major_capital_param_snapshot()
        n_events = 0
        for s in hit_stocks:
            for ev in s.get("signal_dates") or []:
                ev_date = ev.get("date")
                ev_type = ev.get("type")
                if not ev_date or ev_type not in ("WATCH", "BUY"):
                    continue
                await upsert_event(
                    strategy="major_capital_accumulation",
                    code=s.get("code", ""),
                    name=s.get("name") or s.get("code", ""),
                    signal_date=ev_date,
                    signal_type=ev_type,
                    signal_reason=ev.get("reason") or s.get("signal_reason", ""),
                    confidence=float(ev.get("confidence", s.get("confidence", 0)) or 0),
                    strategy_version=MAJOR_CAPITAL_FROZEN_VERSION,
                    parameter_snapshot=param_snapshot,
                    scan_time=scan_time,
                    signal_meta={
                        "source": "daily_major_capital_scan",
                        "latest_signal_type": s.get("signal_type"),
                        "latest_signal_date": s.get("signal_date"),
                        "match_score": s.get("match_score") or {},
                        "price": s.get("price"),
                        "signal_price": ev.get("price"),
                        "pct_change": s.get("pct_change"),
                        "market": s.get("market", "SZ"),
                        "cap_yi": s.get("cap_yi"),
                        "amount_wan": s.get("amount_wan"),
                        # ── 逐事件 buy_meta：rank_score 选股质量审计所需 ──
                        # (rsi/yy_ratio/bb_narrow/watch_days/accumulation_days/
                        #  trigger_strength/breakout_strength)，WATCH 事件为空 dict。
                        **(ev.get("meta") or {}),
                    },
                )
                n_events += 1
        logger.info(
            f"[日扫描] pattern_outcome 记录 {n_events} 条事件 | "
            f"version={MAJOR_CAPITAL_FROZEN_VERSION}"
        )
    except Exception as e:
        logger.warning(f"[日扫描] pattern_outcome 记录失败: {e}")

    # ── Step 3：多渠道推送 ─────────────────────────────────
    # 推送 = 看板【当前标的池】全量：当日命中 + 近 RETAIN_DAYS 天保留的旧标的，剔除已离场。
    # 这样每天推送都能看到完整在手标的，不必记得昨天推过什么。
    if notify_wechat:
        try:
            from notifications.push import pusher
            from db.scan_dao import load_scan
            force = trigger in ("手动", "测试")
            # 展示顺序与看板一致：按贴合度评分 match_score.total 降序
            def _ms(s):
                m = s.get("match_score")
                return m.get("total", 0) if isinstance(m, dict) else (m or 0)

            # 合并保留池：当日 hit_stocks + DB 中近 7 天仍有效的旧标的
            RETAIN_DAYS = 7
            cutoff = (datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=RETAIN_DAYS)).strftime("%Y-%m-%d")
            pool_map = {s.get("code"): s for s in hit_stocks if s.get("code")}
            try:
                for o in await load_scan("major_capital_accumulation"):
                    c = o.get("code")
                    if c and c not in pool_map and str(o.get("scan_time", ""))[:10] >= cutoff:
                        pool_map[c] = o
            except Exception as _e:
                logger.debug(f"[日扫描] 合并保留池失败: {_e}")

            # 剔除「已离场那一轮的旧信号」（与看板 poolStocks 同口径）
            try:
                from db.mysql_pool import get_pool as _gp
                codes = list(pool_map.keys())
                if codes:
                    ph = ",".join(["%s"] * len(codes))
                    posmap: dict = {}
                    _p = await _gp()
                    async with _p.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                f"SELECT code,status,exit_date FROM position_monitor "
                                f"WHERE strategy='major_capital_accumulation' AND code IN ({ph}) "
                                f"ORDER BY signal_date ASC", tuple(codes))
                            for x in await cur.fetchall():
                                posmap[x[0]] = (x[1], x[2])
                    def _keep(s):
                        p = posmap.get(s.get("code"))
                        if p and p[0] == "exited" and p[1]:
                            dts = [d.get("date") for d in (s.get("signal_dates") or [])] \
                                  or ([s.get("signal_date")] if s.get("signal_date") else [])
                            latest = max([d for d in dts if d], default="")
                            if latest and latest <= str(p[1]):
                                return False
                        return True
                    pool_map = {c: s for c, s in pool_map.items() if _keep(s)}
            except Exception as _e:
                logger.debug(f"[日扫描] 剔除已离场失败: {_e}")

            pool = list(pool_map.values())
            push_buy = sorted([s for s in pool if s.get("signal_type") == "BUY"], key=_ms, reverse=True)
            push_watch = sorted([s for s in pool if s.get("signal_type") == "WATCH"], key=_ms, reverse=True)
            logger.info(f"[日扫描] 推送标的池全量: BUY={len(push_buy)} WATCH={len(push_watch)}（含保留池，剔除已离场）")
            result = await pusher.send_major_capital_scan(
                push_buy, push_watch, scan_time, trigger, ignore_trading_day=force
            )
            if result.get("skipped"):
                logger.info("[日扫描] 非交易日静音，跳过推送")
            elif result.get("error"):
                logger.warning(f"[日扫描] 推送失败: {result['error']}")
            else:
                logger.info(f"[日扫描] 推送完成: {result.get('success_count',0)}/{result.get('total',0)} 渠道成功")
        except Exception as e:
            logger.error(f"[日扫描] 推送异常: {e}")

    # ── Step 3.5：BUY 信号自动登记到 position_monitor（is_real=0 模拟） ───
    # 关键去重：若同一 code 已有 status='open' 的持仓，跳过（避免连续 BUY 信号产生重复记录）
    if buy_stocks:
        try:
            from db.position_dao import upsert_position, list_open
            from db.stock_dao import get_daily_history
            from config.execution_rules import (
                evaluate_next_open, MAX_NEW_ENTRIES_PER_DAY,
                STAGED_ENTRY_ENABLED, FIRST_TRANCHE_PCT,
            )
            from datetime import timedelta as _td
            # 拉当前所有 open 持仓的 code，建立 set 用于去重
            open_positions = await list_open('major_capital_accumulation')
            open_codes = {p['code'] for p in open_positions}

            # 排序买入：buy_stocks 已按多因子综合分降序。同一 signal_date 下
            # 实际入场（status='open'）最多 MAX_NEW_ENTRIES_PER_DAY 笔，其余记为 skipped。
            accepted_per_day: dict[str, int] = {}

            n_added = n_skipped_open = n_pending_entry = n_filtered = n_rank_capped = 0
            for s in buy_stocks:
                code = s.get('code')
                sd = s.get('signal_date')
                if not code or not sd:
                    continue
                if code in open_codes:
                    n_skipped_open += 1
                    continue  # 已有未离场持仓，新 BUY 视为再确认

                # 入场基线 = signal_date 次开盘价（如有数据），否则 signal_date close
                rows = await get_daily_history(
                    code, sd,
                    (datetime.strptime(sd, "%Y-%m-%d") + _td(days=15)).strftime("%Y-%m-%d"),
                )
                entry_bar = next((r for r in rows if str(r['trade_date']) > sd), None)
                signal_price = float(s.get('signal_price') or s.get('price') or 0)
                if not entry_bar:
                    n_pending_entry += 1
                    logger.info(f"  PENDING {code} {s.get('name', code)} 等待信号日后首个开盘价")
                    continue

                entry_date = str(entry_bar['trade_date'])
                entry_price = float(entry_bar['open_price'])
                if entry_price <= 0:
                    continue
                allowed, exec_reason, gap_pct = evaluate_next_open(signal_price, entry_price)
                # top-N 排序闸：执行过滤通过后，若当日已入满额，则降级为 skipped
                if allowed:
                    n_today = accepted_per_day.get(sd, 0)
                    if n_today >= MAX_NEW_ENTRIES_PER_DAY:
                        allowed = False
                        exec_reason = (
                            f"执行过滤: 当日新仓已满{MAX_NEW_ENTRIES_PER_DAY}笔, "
                            f"排序分{s.get('rank_score',0):.3f}排名靠后"
                        )
                        n_rank_capped += 1
                    else:
                        accepted_per_day[sd] = n_today + 1
                # 分批进场：通过的新仓先建半仓首批（stage=1），等待站稳突破位补满
                if allowed and STAGED_ENTRY_ENABLED:
                    entry_stage, position_pct = 1, FIRST_TRANCHE_PCT
                else:
                    entry_stage, position_pct = 2, 1.0
                await upsert_position(
                    strategy='major_capital_accumulation',
                    code=code, name=s.get('name', code),
                    signal_date=sd,
                    entry_date=entry_date,
                    entry_price=entry_price,
                    is_real=0,
                    signal_price=signal_price,
                    entry_gap_pct=gap_pct,
                    execution_reason=exec_reason,
                    status='open' if allowed else 'skipped',
                    entry_stage=entry_stage,
                    position_pct=position_pct,
                )
                if allowed:
                    n_added += 1
                else:
                    n_filtered += 1
            logger.info(
                f"[日扫描] position_monitor 登记: 新增 {n_added} 笔, "
                f"执行过滤 {n_filtered} 笔（含排序超额 {n_rank_capped}）, "
                f"等待次开 {n_pending_entry} 笔, 跳过 {n_skipped_open} 笔（已 open）"
            )
        except Exception as e:
            logger.warning(f"[日扫描] position_monitor 登记失败: {e}")

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
