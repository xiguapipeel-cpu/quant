"""
A股节假日交易日历 v2 — 权威版
─────────────────────────────────────────────────────────
数据来源：
  1. 内置静态数据（上交所/深交所官方公告，2024-2026）
  2. AKShare 实时同步（网络可用时自动更新缓存）

功能：
  • is_trading_day()        — 今天是否交易日（含调休逻辑）
  • is_market_open()        — 当前是否开盘（精确到分钟）
  • current_session()       — 当前时段（盘前/竞价/上午盘等）
  • next_trading_day()      — 下一个交易日
  • prev_trading_day()      — 上一个交易日
  • trading_days_between()  — 区间交易日列表
  • countdown_to_holiday()  — 距下一假期交易日数
  • month_calendar()        — 某月完整日历（含假期标注）
  • sync_from_akshare()     — 联网同步最新官方日历
"""

import asyncio
from datetime import date, datetime, time, timedelta
from typing import Optional
from utils.logger import setup_logger

logger = setup_logger("trading_calendar")

# ══════════════════════════════════════════════════════════════
# 静态假期数据（来源：上交所官方公告）
# ══════════════════════════════════════════════════════════════

# ── 2024年 ────────────────────────────────────────────────────
HOLIDAYS_2024 = {
    date(2024,  1,  1),                                       # 元旦
    date(2024,  2,  9), date(2024,  2, 12), date(2024,  2, 13),  # 春节
    date(2024,  2, 14), date(2024,  2, 15), date(2024,  2, 16),
    date(2024,  4,  4), date(2024,  4,  5),                   # 清明节
    date(2024,  5,  1), date(2024,  5,  2), date(2024,  5,  3),  # 劳动节
    date(2024,  6, 10),                                       # 端午节
    date(2024,  9, 16), date(2024,  9, 17),                   # 中秋节
    date(2024, 10,  1), date(2024, 10,  2), date(2024, 10,  3),  # 国庆节
    date(2024, 10,  4), date(2024, 10,  7),
}
MAKEUP_DAYS_2024 = {
    date(2024,  2,  4),   # 春节调休
    date(2024,  4, 28),   # 劳动节调休
    date(2024,  5, 11),   # 劳动节调休
    date(2024,  9, 14),   # 中秋调休
    date(2024, 10, 12),   # 国庆调休
}

# ── 2025年 ────────────────────────────────────────────────────
HOLIDAYS_2025 = {
    date(2025,  1,  1),                                       # 元旦
    date(2025,  1, 28), date(2025,  1, 29), date(2025,  1, 30),  # 春节
    date(2025,  1, 31), date(2025,  2,  3), date(2025,  2,  4),
    date(2025,  4,  4),                                       # 清明节
    date(2025,  5,  1), date(2025,  5,  2), date(2025,  5,  5),  # 劳动节
    date(2025,  5, 31), date(2025,  6,  2),                   # 端午节
    date(2025, 10,  1), date(2025, 10,  2), date(2025, 10,  3),  # 国庆+中秋
    date(2025, 10,  6), date(2025, 10,  7), date(2025, 10,  8),
}
MAKEUP_DAYS_2025 = {
    date(2025,  1, 26),   # 春节调休
    date(2025,  2,  8),   # 春节调休
    date(2025,  4, 27),   # 劳动节调休
    date(2025,  9, 28),   # 国庆调休
    date(2025, 10, 11),   # 国庆调休
}

# ── 2026年 ────────────────────────────────────────────────────
HOLIDAYS_2026 = {
    date(2026,  1,  1), date(2026,  1,  2),                   # 元旦
    date(2026,  2, 17), date(2026,  2, 18), date(2026,  2, 19),  # 春节
    date(2026,  2, 20), date(2026,  2, 23), date(2026,  2, 24),
    date(2026,  4,  6),                                       # 清明节
    date(2026,  5,  1), date(2026,  5,  4), date(2026,  5,  5),  # 劳动节
    date(2026,  6, 19),                                       # 端午节
    date(2026, 10,  1), date(2026, 10,  2), date(2026, 10,  5),  # 国庆
    date(2026, 10,  6), date(2026, 10,  7), date(2026, 10,  8),
}
MAKEUP_DAYS_2026 = {
    date(2026,  2, 15),
    date(2026,  2, 28),
    date(2026,  5,  9),
    date(2026, 10, 10),
}

# 合并
ALL_HOLIDAYS    = HOLIDAYS_2024    | HOLIDAYS_2025    | HOLIDAYS_2026
ALL_MAKEUP_DAYS = MAKEUP_DAYS_2024 | MAKEUP_DAYS_2025 | MAKEUP_DAYS_2026

# 假期元信息（用于展示名称）
HOLIDAY_NAMES = {
    # 2025
    date(2025,  1,  1): "元旦",
    date(2025,  1, 28): "春节", date(2025,  1, 29): "春节",
    date(2025,  1, 30): "春节", date(2025,  1, 31): "春节",
    date(2025,  2,  3): "春节", date(2025,  2,  4): "春节",
    date(2025,  4,  4): "清明节",
    date(2025,  5,  1): "劳动节", date(2025,  5,  2): "劳动节",
    date(2025,  5,  5): "劳动节",
    date(2025,  5, 31): "端午节", date(2025,  6,  2): "端午节",
    date(2025, 10,  1): "国庆节", date(2025, 10,  2): "国庆节",
    date(2025, 10,  3): "国庆节", date(2025, 10,  6): "国庆节",
    date(2025, 10,  7): "国庆节", date(2025, 10,  8): "国庆节",
    # 2026
    date(2026,  1,  1): "元旦",  date(2026,  1,  2): "元旦",
    date(2026,  2, 17): "春节",  date(2026,  2, 18): "春节",
    date(2026,  2, 19): "春节",  date(2026,  2, 20): "春节",
    date(2026,  2, 23): "春节",  date(2026,  2, 24): "春节",
    date(2026,  4,  6): "清明节",
    date(2026,  5,  1): "劳动节", date(2026,  5,  4): "劳动节",
    date(2026,  5,  5): "劳动节",
    date(2026,  6, 19): "端午节",
    date(2026, 10,  1): "国庆节", date(2026, 10,  2): "国庆节",
    date(2026, 10,  5): "国庆节", date(2026, 10,  6): "国庆节",
    date(2026, 10,  7): "国庆节", date(2026, 10,  8): "国庆节",
}

# 交易时段定义
SESSION_RULES = [
    (time(0,   0), time(9,   0),  "盘前准备"),
    (time(9,   0), time(9,  25),  "集合竞价"),
    (time(9,  25), time(9,  30),  "开盘撮合"),
    (time(9,  30), time(11, 30),  "上午连续竞价"),
    (time(11, 30), time(13,  0),  "午间休市"),
    (time(13,  0), time(14, 57),  "下午连续竞价"),
    (time(14, 57), time(15,  0),  "尾盘集合竞价"),
    (time(15,  0), time(15, 30),  "盘后固定价格"),
    (time(15, 30), time(23, 59),  "今日收盘"),
]


# ══════════════════════════════════════════════════════════════
class TradingCalendar:
    """A股交易日历（权威版）"""

    def __init__(self):
        # 支持运行时注入 AKShare 官方数据覆盖
        self._official_trading_days: Optional[set] = None
        self._cache: dict[str, bool] = {}

    # ─── 核心判断 ──────────────────────────────────────────────

    def is_trading_day(self, d: date = None) -> bool:
        if d is None:
            d = date.today()

        key = d.isoformat()
        if key in self._cache:
            return self._cache[key]

        # 优先使用 AKShare 官方数据（联网同步后）
        if self._official_trading_days is not None:
            result = key in self._official_trading_days
            self._cache[key] = result
            return result

        # 本地规则判断
        if d in ALL_MAKEUP_DAYS:
            result = True
        elif d in ALL_HOLIDAYS:
            result = False
        elif d.weekday() >= 5:
            result = False
        else:
            result = True

        self._cache[key] = result
        return result

    def is_market_open(self) -> bool:
        now = datetime.now()
        if not self.is_trading_day(now.date()):
            return False
        t = now.time()
        return (time(9, 30) <= t <= time(11, 30)) or (time(13, 0) <= t <= time(15, 0))

    def current_session(self) -> str:
        now = datetime.now()
        if not self.is_trading_day(now.date()):
            nxt = self.next_trading_day(now.date())
            diff = (nxt - now.date()).days
            return f"休市（{diff}天后开市，{nxt.strftime('%m/%d')}）"
        t = now.time()
        for start, end, name in SESSION_RULES:
            if start <= t < end:
                return name
        return "收盘"

    def holiday_name(self, d: date) -> Optional[str]:
        return HOLIDAY_NAMES.get(d)

    def is_makeup_day(self, d: date) -> bool:
        return d in ALL_MAKEUP_DAYS

    # ─── 导航 ─────────────────────────────────────────────────

    def next_trading_day(self, d: date = None) -> date:
        if d is None:
            d = date.today()
        d = d + timedelta(days=1)
        while not self.is_trading_day(d):
            d += timedelta(days=1)
        return d

    def prev_trading_day(self, d: date = None) -> date:
        if d is None:
            d = date.today()
        d = d - timedelta(days=1)
        while not self.is_trading_day(d):
            d -= timedelta(days=1)
        return d

    def trading_days_between(self, start: date, end: date) -> list[date]:
        days, d = [], start
        while d <= end:
            if self.is_trading_day(d):
                days.append(d)
            d += timedelta(days=1)
        return days

    def count_trading_days(self, start: date, end: date) -> int:
        return len(self.trading_days_between(start, end))

    # ─── 假期倒计时 ────────────────────────────────────────────

    def countdown_to_holiday(self) -> dict:
        """距离下一个假期（及假期名称、交易日数）"""
        today = date.today()
        cur = today + timedelta(days=1)
        trading_days = 0
        while cur <= date(2026, 12, 31):
            if cur in ALL_HOLIDAYS:
                return {
                    "holiday_date": cur.isoformat(),
                    "holiday_name": HOLIDAY_NAMES.get(cur, "节假日"),
                    "trading_days_until": trading_days,
                    "calendar_days_until": (cur - today).days,
                }
            if self.is_trading_day(cur):
                trading_days += 1
            cur += timedelta(days=1)
        return {"holiday_date": None, "holiday_name": "无", "trading_days_until": -1}

    def upcoming_holidays(self, days_ahead: int = 90) -> list[dict]:
        """未来 N 天内的所有假期"""
        today = date.today()
        end   = today + timedelta(days=days_ahead)
        result, seen_name = [], set()
        d = today
        while d <= end:
            if d in ALL_HOLIDAYS:
                name = HOLIDAY_NAMES.get(d, "节假日")
                if name not in seen_name:
                    # 找出这个假期的连续天数
                    end_d = d
                    while end_d + timedelta(days=1) in ALL_HOLIDAYS and \
                          HOLIDAY_NAMES.get(end_d + timedelta(days=1)) == name:
                        end_d += timedelta(days=1)
                    cal_days = (end_d - d).days + 1
                    result.append({
                        "name":          name,
                        "start":         d.isoformat(),
                        "end":           end_d.isoformat(),
                        "calendar_days": cal_days,
                        "trading_days_until": self.count_trading_days(today, d - timedelta(days=1)),
                    })
                    seen_name.add(name)
            d += timedelta(days=1)
        return result

    # ─── 月历 ────────────────────────────────────────────────

    def month_calendar(self, year: int = None, month: int = None) -> list[dict]:
        """
        返回某月每天的交易日状态，用于前端日历渲染
        每条记录：{date, weekday, is_trading, is_holiday, is_makeup, holiday_name, type}
        """
        import calendar as cal_mod
        if year is None:
            year = date.today().year
        if month is None:
            month = date.today().month

        _, days_in_month = cal_mod.monthrange(year, month)
        result = []
        for day in range(1, days_in_month + 1):
            d = date(year, month, day)
            is_holiday  = d in ALL_HOLIDAYS
            is_makeup   = d in ALL_MAKEUP_DAYS
            is_trading  = self.is_trading_day(d)
            h_name      = HOLIDAY_NAMES.get(d, "")

            if is_makeup:
                dtype = "makeup"       # 调休工作日
            elif is_holiday:
                dtype = "holiday"      # 法定节假日
            elif d.weekday() >= 5:
                dtype = "weekend"      # 普通周末
            else:
                dtype = "trading"      # 正常交易日

            result.append({
                "date":         d.isoformat(),
                "day":          day,
                "weekday":      d.weekday(),
                "weekday_cn":   "一二三四五六日"[d.weekday()],
                "is_trading":   is_trading,
                "is_holiday":   is_holiday,
                "is_makeup":    is_makeup,
                "is_today":     d == date.today(),
                "holiday_name": h_name,
                "type":         dtype,
            })
        return result

    # ─── AKShare 同步 ──────────────────────────────────────────

    async def sync_from_akshare(self, year: int = None) -> bool:
        """从 AKShare 同步官方交易日，成功后覆盖本地判断"""
        if year is None:
            year = date.today().year
        try:
            import akshare as ak
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(None, ak.tool_trade_date_hist_sina)
            if df is None or df.empty:
                return False

            trading_days = set()
            for row in df.itertuples():
                d_str = str(row.trade_date)
                try:
                    d = datetime.strptime(d_str, "%Y%m%d").date()
                    trading_days.add(d.isoformat())
                except ValueError:
                    pass

            if self._official_trading_days is None:
                self._official_trading_days = set()
            self._official_trading_days.update(trading_days)
            self._cache.clear()  # 清空旧缓存

            logger.info(f"[日历] AKShare 同步成功，交易日数={len(trading_days)}")
            return True

        except ImportError:
            logger.warning("[日历] akshare 未安装，使用内置数据")
        except Exception as e:
            logger.warning(f"[日历] AKShare 同步失败: {e}，使用内置数据")
        return False

    # ─── 统计 ─────────────────────────────────────────────────

    def year_stats(self, year: int = None) -> dict:
        if year is None:
            year = date.today().year
        start = date(year, 1, 1)
        end   = date(year, 12, 31)
        d, total, trading, holidays_set = start, 0, 0, set()
        while d <= end:
            total += 1
            if self.is_trading_day(d):
                trading += 1
            if d in ALL_HOLIDAYS:
                holidays_set.add(HOLIDAY_NAMES.get(d, "节假日"))
            d += timedelta(days=1)
        return {
            "year":          year,
            "total_days":    total,
            "trading_days":  trading,
            "non_trading":   total - trading,
            "holidays":      sorted(holidays_set),
            "makeup_days":   len([d for d in ALL_MAKEUP_DAYS if d.year == year]),
        }


# 全局单例
calendar = TradingCalendar()
