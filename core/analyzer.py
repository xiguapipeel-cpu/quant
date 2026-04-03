"""
股票分析器 - 核心编排模块
并行采集 → 交叉验证 → 完整性自检 → 输出决策 → 飞书推送
"""

import asyncio
from typing import Optional
from data.fetcher import AKShareFetcher, EastMoneyFetcher
from engines.search_engine import MultiEngineSearcher
from core.cross_validator import (
    CrossValidator, IntegrityChecker, DataPoint,
    extract_pe_from_text, extract_market_cap_from_text
)
from notifications.feishu import notifier as feishu
from utils.logger import setup_logger

logger = setup_logger("analyzer")


class StockAnalyzer:
    """
    单股分析器
    步骤：
    1. 并行从多源采集数据（AKShare + 东方财富 + 搜索引擎）
    2. 交叉验证每个数据点
    3. 完整性自检（四项全有才通过）
    4. 输出结构化结果
    """

    def __init__(self):
        self.akshare    = AKShareFetcher()
        self.eastmoney  = EastMoneyFetcher()
        self.searcher   = MultiEngineSearcher()
        self.validator  = CrossValidator()
        self.checker    = IntegrityChecker()

    async def analyze_stock(self, stock: dict) -> dict:
        code   = stock["code"]
        name   = stock["name"]
        market = stock["market"]

        logger.info(f"\n--- 开始分析: {name}({code}) ---")

        # ── Step 1: 并行从所有来源采集 ──────────────────────────
        tasks = [
            self.akshare.get_realtime_quote(code, market),
            self.akshare.get_fundamental(code),
            self.akshare.get_announcements(code, days=30),
            self.eastmoney.get_quote(code, market),
            self._search_pe(name, code),
            self._search_events(name),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        ak_quote, ak_fundamental, ak_announcements, em_quote, search_pe_data, search_events = [
            r if not isinstance(r, Exception) else None
            for r in results
        ]

        # ── Step 2: 汇总价格数据点 ──────────────────────────────
        price_points = []
        if ak_quote and ak_quote.get("price"):
            price_points.append(DataPoint(ak_quote["price"], "AKShare"))
        if em_quote and em_quote.get("price"):
            price_points.append(DataPoint(em_quote["price"], "东方财富API"))

        price_result = self.validator.validate_price(price_points)

        # ── Step 3: 汇总PE数据点 ────────────────────────────────
        pe_points = []
        if ak_fundamental and ak_fundamental.get("pe"):
            pe_points.append(DataPoint(ak_fundamental["pe"], "AKShare/东方财富"))
        if em_quote and em_quote.get("pe"):
            pe_points.append(DataPoint(em_quote["pe"], "东方财富API"))

        # 搜索引擎补充PE（理杏仁、Yahoo等）
        if search_pe_data:
            for src, val in search_pe_data.items():
                if val:
                    pe_points.append(DataPoint(val, src))

        pe_result = self.validator.validate_pe(pe_points)

        # ── Step 4: 汇总市值数据点 ──────────────────────────────
        cap_points = []
        if ak_fundamental and ak_fundamental.get("market_cap"):
            cap_points.append(DataPoint(ak_fundamental["market_cap"] / 1e8, "AKShare"))  # 转换为亿
        if em_quote and em_quote.get("market_cap"):
            cap_points.append(DataPoint(em_quote["market_cap"] / 1e8, "东方财富API"))

        cap_result = self.validator.validate_market_cap(cap_points)

        # ── Step 5: 汇总近期事件 ────────────────────────────────
        events = []
        if ak_announcements:
            events.extend(ak_announcements)
        if search_events:
            for e in search_events:
                if e not in events:
                    events.append(e)

        # ── Step 6: 完整性自检 ──────────────────────────────────
        check_data = {
            "price":         price_result.consensus_value,
            "pe":            pe_result.consensus_value,
            "market_cap":    cap_result.consensus_value,
            "recent_events": events,
        }
        integrity_pass, passed_fields, missing_fields = self.checker.check(check_data)

        # ── Step 6b: 飞书预警 ───────────────────────────────────
        if not integrity_pass:
            asyncio.create_task(feishu.send_integrity_fail(name, missing_fields))
        elif pe_result.needs_review:
            src_pairs = []
            for s in pe_result.sources:
                parts = s.split(':')
                if len(parts) == 2:
                    try:
                        src_pairs.append((parts[0], float(parts[1])))
                    except ValueError:
                        pass
            asyncio.create_task(
                feishu.send_pe_deviation(name, code, src_pairs, pe_result.max_deviation)
            )

        # ── Step 7: 构建输出结构 ────────────────────────────────
        result = {
            # 基本信息
            "code":   code,
            "name":   name,
            "market": market,

            # 核心数据（交叉验证后的共识值）
            "price":      price_result.consensus_value,
            "pe":         pe_result.consensus_value,
            "market_cap": cap_result.consensus_value,

            # 近期事件
            "recent_events": events[:10],  # 最多保留10条

            # 验证详情
            "validation": {
                "price":      _fmt_result(price_result),
                "pe":         _fmt_result(pe_result),
                "market_cap": _fmt_result(cap_result),
            },

            # 完整性自检
            "integrity_pass":   integrity_pass,
            "passed_fields":    passed_fields,
            "missing_fields":   missing_fields,

            # 决策结论
            "decision": "通过" if integrity_pass else "排除",
            "decision_reason": (
                f"四项完整，交叉验证通过" if integrity_pass
                else f"数据缺失: {', '.join(missing_fields)}，不猜测直接排除"
            ),
        }

        logger.info(
            f"[{name}] 结论: {result['decision']} | "
            f"PE={result['pe']} | 价格={result['price']} | "
            f"市值={result['market_cap']}亿"
        )
        return result

    async def analyze_all(self, watchlist: list) -> list:
        """并行分析所有股票"""
        logger.info(f"开始并行分析 {len(watchlist)} 只股票...")
        tasks = [self.analyze_stock(s) for s in watchlist]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        clean = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.error(f"[{watchlist[i]['name']}] 分析异常: {r}")
                clean.append({
                    "code": watchlist[i]["code"],
                    "name": watchlist[i]["name"],
                    "integrity_pass": False,
                    "decision": "排除",
                    "decision_reason": f"分析异常: {r}",
                    "missing_fields": ["全部"],
                })
            else:
                clean.append(r)
        return clean

    async def _search_pe(self, name: str, code: str) -> dict:
        """
        通过搜索引擎从理杏仁、Yahoo、搜狐等补充PE数据
        """
        queries = [
            (f"{name} {code} 市盈率 PE 理杏仁", "理杏仁"),
            (f"{name} stock PE ratio Yahoo Finance", "Yahoo Finance"),
            (f"{name} 市盈率 搜狐财经", "搜狐财经"),
        ]

        pe_map = {}
        for query, source in queries:
            result = await self.searcher.search(query)
            if result and result.success:
                pe_val = extract_pe_from_text(result.content, name)
                if pe_val:
                    pe_map[source] = pe_val
                    logger.info(f"[搜索] {source} → {name} PE={pe_val}")
                else:
                    logger.debug(f"[搜索] {source} 返回内容中未找到PE值")
            # 注意：搜不到就继续下一个，不猜测
        return pe_map

    async def _search_events(self, name: str) -> list:
        """搜索近期重要事件（研报/新闻）"""
        result = await self.searcher.search(
            f"{name} 最新消息 研报 2025",
            context="A股 近期事件 业绩"
        )
        if not result or not result.success:
            return []

        # 简单分句提取事件摘要
        events = []
        for line in result.content.split("。")[:5]:
            line = line.strip()
            if len(line) > 10 and name in line:
                events.append({"title": line[:80], "source": result.engine, "type": "搜索摘要"})
        return events


def _fmt_result(r) -> dict:
    return {
        "confirmed":       r.confirmed,
        "consensus_value": r.consensus_value,
        "sources":         r.sources,
        "max_deviation":   f"{r.max_deviation:.1%}" if r.max_deviation else "N/A",
        "needs_review":    r.needs_review,
        "note":            r.note,
    }
