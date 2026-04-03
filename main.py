#!/usr/bin/env python3
"""
A股量化交易系统 - 主入口
多源交叉验证 · 三次扫描 · 数据完整性自检 · 零猜测决策 · 飞书推送
"""

import asyncio
import sys
from datetime import datetime
from core.scheduler import TradingScheduler
from core.analyzer import StockAnalyzer
from config.settings import WATCHLIST, SCAN_CONFIG, DATA_SOURCES
from notifications.feishu import notifier
from utils.logger import setup_logger

logger = setup_logger("main")


async def run_scan(scan_type: str = "manual"):
    """执行一次完整扫描，结束后推送飞书报告"""
    logger.info(f"{'='*60}")
    logger.info(f"启动{scan_type}扫描 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"监控股票数: {len(WATCHLIST)} | 数据源数: {len(DATA_SOURCES)} | 搜索引擎数: 5")
    logger.info(f"{'='*60}")

    analyzer = StockAnalyzer()
    results  = await analyzer.analyze_all(WATCHLIST)

    passed   = [r for r in results if r.get("integrity_pass")]
    excluded = [r for r in results if not r.get("integrity_pass")]

    logger.info(f"\n{'='*60}")
    logger.info(f"扫描完成 | 通过: {len(passed)} | 排除: {len(excluded)}")
    logger.info(f"{'='*60}")

    # 生成本地报告
    from reports.report_generator import ReportGenerator
    reporter = ReportGenerator()
    report_path = reporter.generate(results, scan_type)
    logger.info(f"报告已生成: {report_path}")

    # 飞书推送
    await notifier.send_scan_report(results, scan_type)

    # 盘后额外发每日总结到日报群
    if scan_type == "盘后":
        await notifier.send_daily_summary(results)

    # 等待队列消费完毕（最多10秒）
    try:
        await asyncio.wait_for(notifier._queue.join(), timeout=10)
    except asyncio.TimeoutError:
        logger.warning("[飞书] 消息队列未在10s内清空，继续执行")

    return results


async def main():
    # 启动飞书消息队列 worker
    notifier.start()

    mode = sys.argv[1] if len(sys.argv) > 1 else "manual"

    if mode == "schedule":
        # 系统启动通知
        await notifier.send_startup(len(WATCHLIST), len(DATA_SOURCES))
        scheduler = TradingScheduler()
        await scheduler.start()

    elif mode in ["pre", "mid", "post"]:
        scan_names = {"pre": "盘前", "mid": "盘中", "post": "盘后"}
        await run_scan(scan_names[mode])

    else:
        await run_scan("手动")

    await notifier.stop()


if __name__ == "__main__":
    asyncio.run(main())
