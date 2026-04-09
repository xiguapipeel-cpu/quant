"""
本地数据仓库选股器
────────────────────────────────────────────────────────────
基于 MySQL stock_snapshot 表做 SQL 过滤，替代实时 API 拉取。
数据来源：每日 15:30 由 scripts/daily_data_update.py 更新。

优势：
  - 无网络请求，毫秒级响应
  - 支持全部 5000+ A股筛选
  - 支持行业、PE、PB、换手率等多维度组合过滤
  - 与 DynamicScreener 接口兼容，可无缝替换
"""

from __future__ import annotations

from typing import Optional
from utils.logger import setup_logger

logger = setup_logger("local_screener")


class LocalScreener:
    """
    本地数据仓库选股器。
    参数与 DynamicScreener 保持一致，可直接替换。
    额外支持行业、PE、PB、换手率等字段过滤。
    """

    def __init__(
        self,
        min_cap_yi:     float = 0,
        max_cap_yi:     float = 0,
        min_amount_wan: float = 0,
        min_price:      float = 0,
        max_price:      float = 0,
        exclude_st:     bool  = True,
        min_list_days:  int   = 0,
        top_n:          int   = 500,
        # 本地选股器扩展字段
        industry:       Optional[str]   = None,
        min_pe:         Optional[float] = None,
        max_pe:         Optional[float] = None,
        min_pb:         Optional[float] = None,
        max_pb:         Optional[float] = None,
        min_turnover:   Optional[float] = None,
        order_by:       str             = "amount DESC",
        **kwargs,  # 兼容 DynamicScreener 的其他参数（忽略）
    ):
        self.min_cap_yi     = min_cap_yi
        self.max_cap_yi     = max_cap_yi
        self.min_amount_wan = min_amount_wan
        self.min_price      = min_price
        self.max_price      = max_price
        self.exclude_st     = exclude_st
        self.min_list_days  = min_list_days
        self.top_n          = top_n
        self.industry       = industry
        self.min_pe         = min_pe
        self.max_pe         = max_pe
        self.min_pb         = min_pb
        self.max_pb         = max_pb
        self.min_turnover   = min_turnover
        self.order_by       = order_by

    async def screen(self, **kwargs) -> list[dict]:
        """
        执行本地筛选，返回 [{code, name, market, price, cap_yi, amount_wan, pe, ...}, ...]
        接口与 DynamicScreener.screen() 兼容。
        """
        from db.stock_dao import query_snapshot, get_snapshot_status

        # 检查数据就绪状态
        status = await get_snapshot_status()
        if status["total"] == 0:
            logger.warning("[本地选股] stock_snapshot 为空，请先运行 scripts/daily_data_update.py")
            return []

        logger.info(
            f"[本地选股] 快照共 {status['total']} 只股票 | 数据日期 {status['last_trade_date']} | "
            f"筛选条件: 市值>{self.min_cap_yi}亿 成交>{self.min_amount_wan}万 价格{self.min_price}~{self.max_price}"
        )

        results = await query_snapshot(
            min_cap_yi     = self.min_cap_yi,
            max_cap_yi     = self.max_cap_yi,
            min_amount_wan = self.min_amount_wan,
            min_price      = self.min_price,
            max_price      = self.max_price,
            exclude_st     = self.exclude_st,
            min_list_days  = self.min_list_days,
            industry       = self.industry,
            min_pe         = self.min_pe,
            max_pe         = self.max_pe,
            min_pb         = self.min_pb,
            max_pb         = self.max_pb,
            min_turnover   = self.min_turnover,
            order_by       = self.order_by,
            top_n          = self.top_n,
        )

        logger.info(f"[本地选股] 筛选结果: {len(results)} 只")
        return results

    async def is_data_fresh(self, max_stale_days: int = 3) -> bool:
        """检查本地数据是否为最新（未超过 max_stale_days 天）"""
        from db.stock_dao import get_snapshot_status
        from datetime import date, timedelta
        status = await get_snapshot_status()
        if not status["last_trade_date"]:
            return False
        last = date.fromisoformat(status["last_trade_date"])
        return (date.today() - last).days <= max_stale_days


async def get_data_warehouse_status() -> dict:
    """返回本地数据仓库完整状态"""
    from db.stock_dao import get_snapshot_status, get_daily_status
    snapshot = await get_snapshot_status()
    daily = await get_daily_status()
    return {
        "snapshot": snapshot,
        "daily": daily,
    }


# ── 预设配置（与 DynamicScreener SCREEN_PRESETS 一致，增加扩展字段）────────

LOCAL_SCREEN_PRESETS = {
    "large_cap": {
        "label": "大盘蓝筹",
        "desc":  "市值>500亿，成交活跃",
        "params": {
            "min_cap_yi": 500, "min_amount_wan": 10000, "top_n": 100,
        },
    },
    "mid_cap": {
        "label": "中盘成长",
        "desc":  "市值100~1000亿，成长性好",
        "params": {
            "min_cap_yi": 100, "max_cap_yi": 1000, "min_amount_wan": 5000, "top_n": 300,
        },
    },
    "active": {
        "label": "活跃热门",
        "desc":  "成交额前300，不限市值",
        "params": {
            "min_cap_yi": 50, "min_amount_wan": 10000, "top_n": 300,
        },
    },
    "default": {
        "label": "默认筛选",
        "desc":  "市值>100亿，成交>5000万",
        "params": {
            "min_cap_yi": 100, "min_amount_wan": 5000, "top_n": 300,
        },
    },
    "major_capital_pump": {
        "label": "主力拉升",
        "desc":  "中小盘活跃股，主力资金偏好标的",
        "params": {
            "min_cap_yi": 30, "max_cap_yi": 500,
            "min_amount_wan": 3000,
            "min_price": 3.0, "max_price": 200.0,
            "min_list_days": 180, "top_n": 300,
        },
    },
    "major_capital_accumulation": {
        "label": "主力建仓",
        "desc":  "低位横盘中小盘股，主力悄然吸筹",
        "params": {
            "min_cap_yi": 20, "max_cap_yi": 800,
            "min_amount_wan": 1000,
            "min_price": 2.0, "max_price": 50.0,
            "min_list_days": 360, "top_n": 500,
        },
    },
    "value": {
        "label": "价值低估",
        "desc":  "低PE低PB，市值>200亿",
        "params": {
            "min_cap_yi": 200, "min_amount_wan": 3000,
            "min_pe": 1, "max_pe": 20,
            "min_pb": 0.5, "max_pb": 3.0,
            "top_n": 200, "order_by": "pe_ttm ASC",
        },
    },
    "high_turnover": {
        "label": "高换手",
        "desc":  "换手率>3%，市场热度高",
        "params": {
            "min_cap_yi": 30, "min_amount_wan": 5000,
            "min_turnover": 3.0,
            "top_n": 200, "order_by": "turnover_rate DESC",
        },
    },
}
