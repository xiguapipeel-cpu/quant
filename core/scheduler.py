"""
每日三次自动扫描调度器
盘前09:00 / 盘中11:00(每15分钟) / 盘后15:30
"""

import asyncio
from datetime import datetime, time
from config.settings import SCAN_CONFIG, WATCHLIST
from utils.logger import setup_logger
from notifications.feishu import notifier as feishu

logger = setup_logger("scheduler")


class TradingScheduler:
    """
    A股交易日自动调度器
    - 盘前扫描：09:00
    - 盘中监测：09:30 ~ 14:57（每15分钟一次）
    - 盘后复盘：15:30
    """

    def __init__(self):
        self.running = False
        self._scan_count = 0

    async def start(self):
        """启动调度循环"""
        self.running = True
        logger.info("调度器启动，等待下一个扫描时间点...")

        while self.running:
            now = datetime.now()
            t = now.time()

            if self._is_trading_day(now):
                await self._check_and_scan(t)

            # 每30秒检查一次
            await asyncio.sleep(30)

    async def _check_and_scan(self, t: time):
        from core.analyzer import StockAnalyzer
        from reports.report_generator import ReportGenerator

        pre  = time(9, 0)
        mid  = time(11, 0)
        post = time(15, 30)

        # 盘前
        if self._in_window(t, pre, minutes=1):
            logger.info("触发盘前扫描")
            await self._run_scan("盘前")

        # 盘中（09:30 ~ 14:57，每15分钟）
        elif time(9, 30) <= t <= time(14, 57):
            if t.minute % SCAN_CONFIG["mid_interval_minutes"] == 0 and t.second < 30:
                logger.info(f"触发盘中扫描 ({t.strftime('%H:%M')})")
                await self._run_scan("盘中")

        # 盘后
        elif self._in_window(t, post, minutes=1):
            logger.info("触发盘后复盘")
            await self._run_scan("盘后")

    async def _run_scan(self, scan_type: str):
        self._scan_count += 1
        logger.info(f"[第{self._scan_count}次扫描] {scan_type} 开始")
        try:
            from core.analyzer import StockAnalyzer
            from reports.report_generator import ReportGenerator

            analyzer = StockAnalyzer()
            results = await analyzer.analyze_all(WATCHLIST)

            reporter = ReportGenerator()
            path = reporter.generate(results, scan_type)
            logger.info(f"[第{self._scan_count}次扫描] 完成 → {path}")
        except Exception as e:
            logger.error(f"[第{self._scan_count}次扫描] 异常: {e}")
            await feishu.send_alert(
                title="扫描异常：" + scan_type,
                body="扫描过程发生异常: " + str(e),
                level="critical",
            )

    @staticmethod
    def _in_window(t: time, target: time, minutes: int = 2) -> bool:
        """判断时间是否在target附近minutes分钟内"""
        t_secs = t.hour * 3600 + t.minute * 60 + t.second
        tgt_secs = target.hour * 3600 + target.minute * 60
        return abs(t_secs - tgt_secs) < minutes * 60

    @staticmethod
    def _is_trading_day(dt: datetime) -> bool:
        """判断是否是交易日（简单版：周一至周五，未考虑节假日）"""
        return dt.weekday() < 5  # 0=周一...4=周五
