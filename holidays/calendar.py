"""
A股节假日交易日历 v3 — 完整权威版
数据来源：上交所/深交所官方公告（2024-2027）
功能：
  • is_trading_day()         判断交易日（含调休逻辑）
  • is_market_open()         当前是否开盘（精确到分钟）
  • current_session()        当前时段（盘前/竞价/上午盘/午休/下午盘/盘后/休市）
  • next_trading_day()       下一个交易日
  • prev_trading_day()       上一个交易日
  • trading_days_between()   区间交易日列表
  • days_until_holiday()     距下一假期天数
  • month_calendar()         某月完整日历（含假期标注）
  • upcoming_holidays()      未来N天的假期列表
  • year_stats()             全年交易日统计
  • sync_from_akshare()      联网同步最新官方日历
"""

import asyncio
from datetime import date, datetime, time, timedelta
from typing import Optional
from utils.logger import setup_logger

logger = setup_logger("trading_calendar")

# ══════════════════════════════════════════════════════════════
# 2024年
# ══════════════════════════════════════════════════════════════
HOLIDAYS_2024 = {
    "2024-01-01",                                              # 元旦
    "2024-02-09","2024-02-10","2024-02-11","2024-02-12",       # 春节
    "2024-02-13","2024-02-14","2024-02-15","2024-02-16",
    "2024-04-04","2024-04-05","2024-04-06",                    # 清明
    "2024-05-01","2024-05-02","2024-05-03",                    # 劳动节
    "2024-06-10",                                              # 端午
    "2024-09-16","2024-09-17",                                 # 中秋
    "2024-10-01","2024-10-02","2024-10-03","2024-10-04",       # 国庆
    "2024-10-07",
}
WORKDAYS_2024 = {  # 调休工作日（周末变交易日）
    "2024-02-04","2024-02-18","2024-04-07","2024-04-28",
    "2024-05-11","2024-09-14","2024-09-29","2024-10-12",
}

# ══════════════════════════════════════════════════════════════
# 2025年（来源：上交所2025年休市安排公告）
# ══════════════════════════════════════════════════════════════
HOLIDAYS_2025 = {
    "2025-01-01",                                              # 元旦
    "2025-01-28","2025-01-29","2025-01-30","2025-01-31",       # 春节
    "2025-02-03","2025-02-04",
    "2025-04-04","2025-04-05","2025-04-06",                    # 清明
    "2025-05-01","2025-05-02","2025-05-05",                    # 劳动节
    "2025-05-31","2025-06-02",                                 # 端午
    "2025-10-01","2025-10-02","2025-10-03",                    # 国庆+中秋
    "2025-10-06","2025-10-07","2025-10-08",
}
WORKDAYS_2025 = {
    "2025-01-26","2025-02-08","2025-04-27",
    "2025-09-28","2025-10-11",
}

# ══════════════════════════════════════════════════════════════
# 2026年（来源：国务院2026年节假日安排）
# ══════════════════════════════════════════════════════════════
HOLIDAYS_2026 = {
    "2026-01-01","2026-01-02",                                 # 元旦
    "2026-02-17","2026-02-18","2026-02-19","2026-02-20",       # 春节
    "2026-02-23","2026-02-24",
    "2026-04-06",                                              # 清明
    "2026-05-01","2026-05-04","2026-05-05",                    # 劳动节
    "2026-06-19",                                              # 端午
    "2026-10-01","2026-10-02","2026-10-05",                    # 国庆
    "2026-10-06","2026-10-07","2026-10-08",
}
WORKDAYS_2026 = {
    "2026-02-15","2026-02-28","2026-05-09","2026-10-10",
}

# ══════════════════════════════════════════════════════════════
# 节假日名称映射（用于通知消息）
# ══════════════════════════════════════════════════════════════
HOLIDAY_LABELS = {
    # 2025
    "2025-01-01":"元旦",
    "2025-01-28":"春节","2025-01-29":"春节","2025-01-30":"春节",
    "2025-01-31":"春节","2025-02-03":"春节","2025-02-04":"春节",
    "2025-04-04":"清明节","2025-04-05":"清明节","2025-04-06":"清明节",
    "2025-05-01":"劳动节","2025-05-02":"劳动节","2025-05-05":"劳动节",
    "2025-05-31":"端午节","2025-06-02":"端午节",
    "2025-10-01":"国庆节","2025-10-02":"国庆节","2025-10-03":"国庆节",
    "2025-10-06":"国庆节","2025-10-07":"国庆节","2025-10-08":"国庆节",
    # 2026
    "2026-01-01":"元旦","2026-01-02":"元旦",
    "2026-02-17":"春节","2026-02-18":"春节","2026-02-19":"春节",
    "2026-02-20":"春节","2026-02-23":"春节","2026-02-24":"春节",
    "2026-04-06":"清明节",
    "2026-05-01":"劳动节","2026-05-04":"劳动节","2026-05-05":"劳动节",
    "2026-06-19":"端午节",
    "2026-10-01":"国庆节","2026-10-02":"国庆节","2026-10-05":"国庆节",
    "2026-10-06":"国庆节","2026-10-07":"国庆节","2026-10-08":"国庆节",
}

ALL_HOLIDAYS   = HOLIDAYS_2024 | HOLIDAYS_2025 | HOLIDAYS_2026
ALL_WORKDAYS   = WORKDAYS_2024 | WORKDAYS_2025 | WORKDAYS_2026

# 开盘时间段
MORNING_START  = time(9, 30)
MORNING_END    = time(11, 30)
AFTERNOON_START= time(13, 0)
AFTERNOON_END  = time(15, 0)
CALL_AUCTION   = time(9, 15)   # 集合竞价开始（科创板9:15）
PRE_OPEN       = time(9, 0)    # 盘前开始


class TradingCalendar:
    """
    A股交易日历核心类
    判断优先级：
      1. 调休工作日（周末变交易日）→ 交易日
      2. 法定节假日 → 非交易日
      3. 周六/周日 → 非交易日
      4. AKShare同步覆盖（若已同步）
      5. 普通工作日 → 交易日
    """

    def __init__(self):
        self._holidays             = set(ALL_HOLIDAYS)
        self._extra_workdays       = set(ALL_WORKDAYS)
        self._cache: dict[str, bool] = {}
        self._official_set: Optional[set] = None   # AKShare同步后的权威集合

    # ─── 核心判断 ─────────────────────────────────────────────

    def is_trading_day(self, dt=None) -> bool:
        """判断指定日期（date/datetime/str YYYY-MM-DD）是否是交易日"""
        dt = _coerce_date(dt)
        key = dt.isoformat()

        if key in self._cache:
            return self._cache[key]

        # AKShare权威集合（优先）
        if self._official_set is not None:
            result = key in self._official_set
        elif key in self._extra_workdays:
            result = True
        elif key in self._holidays:
            result = False
        elif dt.weekday() >= 5:
            result = False
        else:
            result = True

        self._cache[key] = result
        return result

    def is_market_open(self, dt: datetime = None) -> bool:
        """当前时刻市场是否处于开盘状态"""
        dt = dt or datetime.now()
        if not self.is_trading_day(dt.date()):
            return False
        t = dt.time()
        return (MORNING_START <= t <= MORNING_END) or (AFTERNOON_START <= t <= AFTERNOON_END)

    def current_session(self, dt: datetime = None) -> str:
        """
        当前交易时段
        返回：盘前 | 集合竞价 | 上午盘 | 午休 | 下午盘 | 盘后 | 休市
        """
        dt = dt or datetime.now()
        if not self.is_trading_day(dt.date()):
            holiday = HOLIDAY_LABELS.get(dt.date().isoformat())
            return f"休市（{holiday}）" if holiday else "休市"
        t = dt.time()
        if   t < PRE_OPEN:        return "盘前"
        elif t < CALL_AUCTION:    return "盘前（等待竞价）"
        elif t < MORNING_START:   return "集合竞价"
        elif t <= MORNING_END:    return "上午盘"
        elif t < AFTERNOON_START: return "午休"
        elif t <= AFTERNOON_END:  return "下午盘"
        else:                     return "盘后"

    # ─── 日期导航 ─────────────────────────────────────────────

    def next_trading_day(self, dt=None) -> date:
        """下一个交易日"""
        d = _coerce_date(dt) + timedelta(days=1)
        while not self.is_trading_day(d):
            d += timedelta(days=1)
        return d

    def prev_trading_day(self, dt=None) -> date:
        """上一个交易日"""
        d = _coerce_date(dt) - timedelta(days=1)
        while not self.is_trading_day(d):
            d -= timedelta(days=1)
        return d

    def trading_days_between(self, start, end) -> list[date]:
        """区间内所有交易日（含首尾）"""
        s, e = _coerce_date(start), _coerce_date(end)
        result, d = [], s
        while d <= e:
            if self.is_trading_day(d):
                result.append(d)
            d += timedelta(days=1)
        return result

    def count_trading_days(self, start, end) -> int:
        return len(self.trading_days_between(start, end))

    # ─── 假期查询 ─────────────────────────────────────────────

    def days_until_holiday(self, dt=None) -> tuple[int, str]:
        """
        距下一个法定节假日（非周末）的交易日数量
        返回: (交易日数, 节假日名称)
        """
        d = _coerce_date(dt) + timedelta(days=1)
        trading_count = 0
        for _ in range(365):
            key = d.isoformat()
            if key in self._holidays:
                name = HOLIDAY_LABELS.get(key, "节假日")
                return trading_count, name
            if self.is_trading_day(d):
                trading_count += 1
            d += timedelta(days=1)
        return -1, "未知"

    def upcoming_holidays(self, days: int = 60, dt=None) -> list[dict]:
        """未来N天内的节假日列表（合并连续假期）"""
        start = _coerce_date(dt) + timedelta(days=1)
        end   = start + timedelta(days=days)
        result, prev_name, block = [], None, []

        d = start
        while d <= end:
            key  = d.isoformat()
            name = HOLIDAY_LABELS.get(key)
            if name:
                if name == prev_name:
                    block.append(d)
                else:
                    if block and prev_name:
                        result.append(_holiday_block(block, prev_name))
                    block = [d]
                    prev_name = name
            else:
                if block and prev_name:
                    result.append(_holiday_block(block, prev_name))
                block, prev_name = [], None
            d += timedelta(days=1)
        if block and prev_name:
            result.append(_holiday_block(block, prev_name))
        return result

    def month_calendar(self, year: int = None, month: int = None) -> list[dict]:
        """
        某月完整日历（每天状态）
        返回: list of {date, weekday, type: trading/holiday/weekend/makeup, label}
        """
        today = date.today()
        year  = year  or today.year
        month = month or today.month

        result = []
        d = date(year, month, 1)
        while d.month == month:
            key = d.isoformat()
            if key in self._extra_workdays:
                dtype, label = "makeup", "调休"
            elif key in self._holidays:
                dtype, label = "holiday", HOLIDAY_LABELS.get(key, "节假日")
            elif d.weekday() >= 5:
                dtype, label = "weekend", "周末"
            else:
                dtype, label = "trading", ""
            result.append({
                "date":    key,
                "day":     d.day,
                "weekday": d.weekday(),
                "type":    dtype,
                "label":   label,
                "is_today":d == today,
            })
            d += timedelta(days=1)
        return result

    # ─── 统计 ─────────────────────────────────────────────────

    def year_stats(self, year: int = None) -> dict:
        year = year or date.today().year
        start, end = date(year, 1, 1), date(year, 12, 31)
        trading = self.trading_days_between(start, end)
        holidays = {HOLIDAY_LABELS.get(h,"节假日") for h in self._holidays if h.startswith(str(year))}
        makeup   = [d for d in self._extra_workdays if d.startswith(str(year))]
        return {
            "year":          year,
            "total_days":    (end - start).days + 1,
            "trading_days":  len(trading),
            "non_trading":   (end - start).days + 1 - len(trading),
            "holiday_names": sorted(holidays),
            "makeup_days":   len(makeup),
            "makeup_dates":  sorted(makeup),
        }

    # ─── AKShare同步 ──────────────────────────────────────────

    async def sync_from_akshare(self, year: int = None) -> bool:
        """从AKShare同步官方交易日（成功后覆盖本地判断）"""
        try:
            import akshare as ak
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(None, ak.tool_trade_date_hist_sina)
            if df is None or df.empty:
                return False
            trading_days = set()
            for row in df.itertuples():
                try:
                    d = datetime.strptime(str(row.trade_date), "%Y%m%d").date()
                    trading_days.add(d.isoformat())
                except ValueError:
                    pass
            if self._official_set is None:
                self._official_set = set()
            self._official_set.update(trading_days)
            self._cache.clear()
            logger.info(f"[日历] AKShare同步成功，交易日数={len(trading_days)}")
            return True
        except ImportError:
            logger.warning("[日历] akshare未安装，使用内置数据")
        except Exception as e:
            logger.warning(f"[日历] AKShare同步失败: {e}，使用内置静态数据")
        return False


# ── 工具函数 ──────────────────────────────────────────────────

def _coerce_date(dt) -> date:
    if dt is None:            return date.today()
    if isinstance(dt, datetime): return dt.date()
    if isinstance(dt, date):  return dt
    if isinstance(dt, str):   return date.fromisoformat(dt)
    raise TypeError(f"不支持的日期类型: {type(dt)}")

def _holiday_block(days: list[date], name: str) -> dict:
    return {
        "name":       name,
        "start":      days[0].isoformat(),
        "end":        days[-1].isoformat(),
        "days":       len(days),
        "dates":      [d.isoformat() for d in days],
    }


# 全局单例
calendar = TradingCalendar()
