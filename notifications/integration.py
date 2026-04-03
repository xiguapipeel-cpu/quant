"""
通知集成模块
将扫描结果、回测完成、节假日通知等自动接入飞书
在 main.py 和 scheduler.py 中调用
"""

import asyncio
from datetime import datetime, date, timedelta
from utils.logger import setup_logger

logger = setup_logger("notify_integration")


async def notify_scan_done(results: list, scan_type: str):
    """扫描完成后推送飞书报告"""
    try:
        from notifications.feishu import notifier
        notifier.start()
        await notifier.send_scan_report(results, scan_type)
        logger.info(f"[通知] 扫描报告已推送: {scan_type}")
    except Exception as e:
        logger.warning(f"[通知] 扫描报告推送失败: {e}")


async def notify_integrity_failures(results: list):
    """逐一推送完整性自检失败的股票"""
    try:
        from notifications.feishu import notifier
        notifier.start()
        failed = [r for r in results if not r.get("integrity_pass")]
        for r in failed[:5]:   # 最多推5条，避免刷屏
            missing = r.get("missing_fields", [])
            if missing:
                await notifier.send_integrity_fail(
                    r.get("name", "未知"), r.get("code", ""), missing
                )
                await asyncio.sleep(0.3)
    except Exception as e:
        logger.warning(f"[通知] 完整性失败推送失败: {e}")


async def notify_pe_deviations(results: list):
    """推送PE交叉验证偏差预警"""
    try:
        from notifications.feishu import notifier
        notifier.start()
        for r in results:
            val = (r.get("validation") or {}).get("pe", {})
            if val.get("needs_review") and val.get("sources"):
                srcs = val["sources"]   # ["理杏仁:20.4", "Yahoo:22.1"]
                if len(srcs) >= 2:
                    def _parse(s):
                        parts = s.split(":")
                        return parts[0], float(parts[1]) if len(parts) == 2 else (s, 0.0)
                    s1, v1 = _parse(srcs[0])
                    s2, v2 = _parse(srcs[1])
                    dev = abs(v1 - v2) / max(v1, v2, 0.001)
                    await notifier.send_pe_deviation(
                        r.get("name",""), r.get("code",""), s1, v1, s2, v2, dev
                    )
    except Exception as e:
        logger.warning(f"[通知] PE偏差推送失败: {e}")


async def notify_daily_summary(results: list):
    """盘后每日总结推送"""
    try:
        from notifications.feishu import notifier
        notifier.start()
        await notifier.send_daily_summary(results)
        logger.info("[通知] 盘后总结已推送")
    except Exception as e:
        logger.warning(f"[通知] 盘后总结推送失败: {e}")


async def notify_backtest_done(strategy: str, metrics: dict):
    """回测完成推送"""
    try:
        from notifications.feishu import notifier
        notifier.start()
        await notifier.send_backtest_done(strategy, metrics)
        logger.info(f"[通知] 回测完成推送: {strategy}")
    except Exception as e:
        logger.warning(f"[通知] 回测完成推送失败: {e}")


async def check_and_notify_upcoming_holiday(days_ahead: int = 3):
    """
    检查是否有近期节假日，提前N个交易日推送休市通知
    在每日盘后调用
    """
    try:
        from holidays.calendar import calendar as cal
        from notifications.feishu import notifier
        notifier.start()

        count, name = cal.days_until_holiday()
        if 0 < count <= days_ahead:
            upcoming = cal.upcoming_holidays(days=14)
            if upcoming:
                h = upcoming[0]
                resume = cal.next_trading_day(
                    date.fromisoformat(h["end"])
                ).isoformat()
                await notifier.send_holiday_notice(
                    holiday_name=h["name"],
                    start_date=h["start"],
                    end_date=h["end"],
                    resume_date=resume,
                    trading_days=h["days"],
                )
                logger.info(f"[通知] 节假日休市通知已推送: {h['name']}，{count}个交易日后")
    except Exception as e:
        logger.warning(f"[通知] 节假日通知检查失败: {e}")


async def notify_startup(watchlist_count: int, source_count: int):
    """系统启动通知"""
    try:
        from notifications.feishu import notifier
        notifier.start()
        await notifier.send_startup(watchlist_count, source_count)
        logger.info("[通知] 启动通知已推送")
    except Exception as e:
        logger.warning(f"[通知] 启动通知失败: {e}")
