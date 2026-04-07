"""
动态选股器 — 从全A股市场筛选符合条件的股票
替代固定WATCHLIST，实现动态筛选 → 回测 → 交易

数据源（三级降级）：
  1. 东方财富 xuangu API（datacenter.eastmoney.com）—— 有市值/PE，速度快
  2. 新浪财经 AKShare stock_zh_a_spot() —— 全量5500只，有成交额，无市值
  3. 本地缓存 / 固定WATCHLIST —— 离线降级

筛选维度：
  1. 成交额过滤：当日成交额 > min_amount 万
  2. 价格过滤：min_price ~ max_price
  3. 排除ST/*ST
  4. 市值过滤（若数据源有市值字段）
  5. 可选：PE过滤

技术说明：
  push2.eastmoney.com 存在 IP 封锁问题，AKShare stock_zh_a_spot_em() 不可用。
  通过清除代理环境变量 + 使用替代 API 解决。
"""

import asyncio
import contextlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from utils.logger import setup_logger

logger = setup_logger("screener")

SCREEN_CACHE = Path("backtest_cache/screen_result.json")
SCREEN_CACHE.parent.mkdir(exist_ok=True)

# 东方财富选股器 API（绕过被封锁的 push2 域名）
_EM_XUANGU_URL = "https://data.eastmoney.com/dataapi/xuangu/list"
_EM_XUANGU_FIELDS = (
    "SECUCODE,SECURITY_NAME_ABBR,NEW_PRICE,TOTAL_MARKET_CAP,"
    "DEAL_AMOUNT,PE_TTM,LISTING_DATE"
)


@contextlib.contextmanager
def _bypass_proxy():
    """
    临时清除代理环境变量，使 AKShare / requests 直连（绕过 Clash HTTP 代理）。
    退出上下文时自动恢复原始代理设置。

    背景：系统设置了 http_proxy=127.0.0.1:7890（Clash），
    但 Clash 对 push2.eastmoney.com 的 CONNECT 隧道处理异常导致连接中断。
    直连模式下会走 Clash TUN 透明代理，行为正常。
    """
    proxy_keys = [
        "http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
        "all_proxy", "ALL_PROXY",
    ]
    saved = {k: os.environ.pop(k) for k in proxy_keys if k in os.environ}
    try:
        yield
    finally:
        os.environ.update(saved)


class DynamicScreener:
    """
    动态选股器：优先调用东方财富选股 API，降级到新浪财经，最终降级到缓存。
    结果缓存到本地（默认 4 小时），避免频繁请求。
    """

    def __init__(
        self,
        min_cap_yi:     float = 100,      # 最小市值（亿）
        max_cap_yi:     float = 50000,    # 最大市值（亿），排除超大盘
        min_amount_wan: float = 5000,     # 最小日均成交额（万元）
        min_price:      float = 5.0,      # 最低股价（排除低价股）
        max_price:      float = 500.0,    # 最高股价
        exclude_st:     bool  = True,     # 排除ST
        min_list_days:  int   = 120,      # 最少上市天数
        top_n:          int   = 300,      # 最多选多少只
        trend_filter:   bool  = False,    # 是否启用趋势过滤（价格>MA60）
    ):
        self.min_cap_yi = min_cap_yi
        self.max_cap_yi = max_cap_yi
        self.min_amount_wan = min_amount_wan
        self.min_price = min_price
        self.max_price = max_price
        self.exclude_st = exclude_st
        self.min_list_days = min_list_days
        self.top_n = top_n
        self.trend_filter = trend_filter

    def _cache_path(self) -> Path:
        """根据筛选参数生成独立的缓存文件名（不同预设不共享）"""
        import hashlib
        key = f"{self.min_cap_yi}_{self.max_cap_yi}_{self.min_amount_wan}_{self.min_price}_{self.max_price}_{self.top_n}"
        h = hashlib.md5(key.encode()).hexdigest()[:8]
        return SCREEN_CACHE.parent / f"screen_{h}.json"

    async def screen(self, use_cache_hours: int = 4) -> list[dict]:
        """
        执行筛选，返回 [{code, name, market, cap_yi, amount_wan, price, pe}, ...]
        use_cache_hours: 缓存有效时间（小时），0=不用缓存
        """
        cache_file = self._cache_path()

        # 检查缓存（按预设参数隔离）
        if use_cache_hours > 0 and cache_file.exists():
            try:
                cache = json.loads(cache_file.read_text(encoding="utf-8"))
                cached_time = datetime.fromisoformat(cache["time"])
                if (datetime.now() - cached_time).total_seconds() < use_cache_hours * 3600:
                    logger.info(f"[选股] 使用缓存结果 ({len(cache['stocks'])}只, {cache['time']}, {cache_file.name})")
                    return cache["stocks"]
            except Exception:
                pass

        # 三级数据源尝试
        stocks = await self._fetch_eastmoney_xuangu()

        if not stocks:
            logger.warning("[选股] 东方财富API失败，尝试新浪财经...")
            stocks = await self._fetch_sina()

        if not stocks:
            logger.warning("[选股] 所有实时数据源失败，降级使用缓存")
            return self._load_cache_fallback()

        # 写缓存（按预设参数隔离）
        cache_data = {"time": datetime.now().isoformat(), "stocks": stocks}
        cache_file.write_text(
            json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info(f"[选股] 筛选完成: {len(stocks)}只股票，已缓存到 {cache_file.name}")
        return stocks

    # ══════════════════════════════════════════════════════════
    # 数据源 1：东方财富选股器 API（datacenter 域名，无封锁）
    # 优点：有市值/PE，速度快（1次请求可获百只股票）
    # ══════════════════════════════════════════════════════════

    async def _fetch_eastmoney_xuangu(self) -> list[dict]:
        """
        调用东方财富 xuangu/list 接口（data.eastmoney.com），绕过被封锁的 push2 域名。
        """
        try:
            import requests as _req

            # 构建服务端过滤条件（成交额的server侧过滤有数据限制，改为客户端过滤）
            server_filter = (
                f"(TOTAL_MARKET_CAP>{int(self.min_cap_yi * 1e8)})"
                f"(NEW_PRICE>{self.min_price})(NEW_PRICE<{self.max_price})"
            )

            loop = asyncio.get_event_loop()

            def _do_fetch():
                all_rows = []
                page = 1
                with _bypass_proxy():
                    while True:
                        resp = _req.get(
                            _EM_XUANGU_URL,
                            params={
                                "sty":    _EM_XUANGU_FIELDS,
                                "filter": server_filter,
                                "p":      page,
                                "ps":     200,
                                "fd":     "",
                            },
                            headers={
                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                                "Referer":    "https://data.eastmoney.com/xuangu/",
                            },
                            timeout=10,
                        )
                        result = resp.json().get("result") or {}
                        rows = result.get("data") or []
                        total = result.get("count", 0)
                        all_rows.extend(rows)
                        if len(all_rows) >= total or not rows or not result.get("nextpage"):
                            break
                        page += 1
                return all_rows

            raw = await loop.run_in_executor(None, _do_fetch)

            if not raw:
                logger.warning("[选股][东方财富] 返回空数据")
                return []

            logger.info(f"[选股][东方财富] 获取 {len(raw)} 只，开始客户端过滤...")
            return self._filter_em_rows(raw)

        except Exception as e:
            logger.error(f"[选股][东方财富] 异常: {e}")
            return []

    def _filter_em_rows(self, rows: list[dict]) -> list[dict]:
        """对东方财富 xuangu 数据进行客户端过滤"""
        from datetime import date as _date

        today = _date.today()
        result = []

        for row in rows:
            name = str(row.get("SECURITY_NAME_ABBR") or "")
            code_raw = str(row.get("SECURITY_CODE") or row.get("SECUCODE") or "")
            # SECUCODE 格式: "000001.SZ"
            if "." in code_raw:
                code = code_raw.split(".")[0]
                suffix = code_raw.split(".")[-1]
                market = "SH" if suffix == "SH" else ("BJ" if suffix == "BJ" else "SZ")
            else:
                code = code_raw.zfill(6)
                market = "SH" if code.startswith("6") else "SZ"

            price    = _safe_float(row.get("NEW_PRICE"))
            cap      = _safe_float(row.get("TOTAL_MARKET_CAP"))
            amount   = _safe_float(row.get("DEAL_AMOUNT"))
            pe       = _safe_float(row.get("PE_TTM"))
            listed   = row.get("LISTING_DATE") or ""

            # 排除 ST
            if self.exclude_st and "ST" in name.upper():
                continue

            # 价格过滤
            if price is None or price < self.min_price or price > self.max_price:
                continue

            # 市值过滤
            cap_yi = (cap / 1e8) if cap else 0
            if cap_yi < self.min_cap_yi or cap_yi > self.max_cap_yi:
                continue

            # 成交额过滤
            amount_wan = (amount / 1e4) if amount else 0
            if amount_wan < self.min_amount_wan:
                continue

            # 上市天数过滤
            if listed and self.min_list_days > 0:
                try:
                    listed_date = _date.fromisoformat(listed[:10])
                    days = (today - listed_date).days
                    if days < self.min_list_days:
                        continue
                except Exception:
                    pass

            result.append({
                "code":       code,
                "name":       name,
                "market":     market,
                "cap_yi":     round(cap_yi, 1),
                "amount_wan": round(amount_wan, 0),
                "price":      round(price, 2),
                "pe":         round(pe, 1) if pe and 0 < pe < 200 else None,
                "source":     "eastmoney_xuangu",
            })

        # 按成交额降序，取前 top_n
        result.sort(key=lambda x: x["amount_wan"], reverse=True)
        result = result[: self.top_n]

        logger.info(f"[选股][东方财富] 客户端过滤后: {len(result)} 只")
        return result

    # ══════════════════════════════════════════════════════════
    # 数据源 2：新浪财经（AKShare stock_zh_a_spot）
    # 优点：全量 5500 只，覆盖全面
    # 缺点：无市值/PE，约 30 秒拉取完成
    # ══════════════════════════════════════════════════════════

    async def _fetch_sina(self) -> list[dict]:
        """
        从新浪财经拉取全A股行情（AKShare stock_zh_a_spot）。
        新浪接口有时返回 HTML 错误页（速率限制），最多重试 2 次。
        """
        import time

        max_retries = 2
        for attempt in range(1, max_retries + 1):
            try:
                import akshare as ak
                loop = asyncio.get_event_loop()

                def _do_fetch():
                    with _bypass_proxy():
                        return ak.stock_zh_a_spot()

                df = await loop.run_in_executor(None, _do_fetch)

                if df is None or df.empty:
                    logger.warning(f"[选股][新浪] 第{attempt}次返回空数据")
                    continue

                logger.info(f"[选股][新浪] 获取全A股 {len(df)} 只，开始筛选...")
                return self._filter_sina_df(df)

            except ImportError:
                logger.error("[选股][新浪] akshare未安装")
                return []
            except Exception as e:
                err_msg = str(e)
                if "character '<'" in err_msg or "HTML" in err_msg.upper():
                    logger.warning(f"[选股][新浪] 第{attempt}次速率限制，等待3秒重试...")
                    await asyncio.sleep(3)
                else:
                    logger.error(f"[选股][新浪] 拉取异常: {e}")
                    return []

        logger.error("[选股][新浪] 重试耗尽，放弃")
        return []

    def _filter_sina_df(self, df) -> list[dict]:
        """对新浪财经数据进行过滤（无市值，按成交额+价格筛选）"""
        import pandas as pd

        col_map = {
            "代码": "code", "名称": "name",
            "最新价": "price", "成交额": "amount",
        }
        for cn, en in col_map.items():
            if cn in df.columns:
                df[en] = df[cn]

        for col in ["price", "amount"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: _safe_float(x))

        mask = df["price"].notna() & (df["price"] > 0)

        if self.exclude_st:
            mask &= ~df["name"].str.contains("ST", case=False, na=False)

        if "amount" in df.columns:
            df["amount_wan"] = df["amount"] / 1e4
            mask &= df["amount_wan"] >= self.min_amount_wan

        mask &= df["price"] >= self.min_price
        mask &= df["price"] <= self.max_price

        filtered = df[mask].copy()
        logger.info(f"[选股][新浪] 过滤后: {len(filtered)} 只")

        if filtered.empty:
            return []

        filtered = filtered.sort_values("amount", ascending=False).head(self.top_n)

        result = []
        for _, row in filtered.iterrows():
            code_raw = str(row.get("code", "")).replace("sh", "").replace("sz", "").replace("bj", "")
            code = code_raw.zfill(6) if code_raw.isdigit() else code_raw[-6:]
            # 推断市场
            raw_code = str(row.get("code", ""))
            if raw_code.startswith("sh") or code.startswith("6"):
                market = "SH"
            elif raw_code.startswith("bj"):
                market = "BJ"
            else:
                market = "SZ"

            result.append({
                "code":       code,
                "name":       str(row.get("name", "")),
                "market":     market,
                "cap_yi":     0,    # 新浪无市值数据
                "amount_wan": round(row.get("amount_wan", 0), 0),
                "price":      round(row.get("price", 0), 2),
                "pe":         None, # 新浪无PE数据
                "source":     "sina",
            })

        logger.info(f"[选股][新浪] 最终选出 {len(result)} 只: {[s['name'] for s in result[:5]]}...")
        return result

    # ══════════════════════════════════════════════════════════
    # 降级：本地缓存 / 固定WATCHLIST
    # ══════════════════════════════════════════════════════════

    def _load_cache_fallback(self) -> list[dict]:
        """缓存降级：优先读取当前预设缓存，否则读通用缓存"""
        for cache_path in [self._cache_path(), SCREEN_CACHE]:
            if cache_path.exists():
                try:
                    cache = json.loads(cache_path.read_text(encoding="utf-8"))
                    stocks = cache.get("stocks", [])
                    if stocks:
                        logger.info(f"[选股] 降级使用缓存 ({len(stocks)}只, {cache.get('time', '?')}, {cache_path.name})")
                        return stocks
                except Exception:
                    pass

        # 最终降级：使用固定WATCHLIST（补齐字段）
        from config.settings import WATCHLIST
        logger.warning("[选股] 无可用缓存，降级使用固定WATCHLIST")
        result = []
        for s in WATCHLIST:
            item = dict(s)
            item.setdefault("cap_yi", 0)
            item.setdefault("amount_wan", 0)
            item.setdefault("price", 0)
            item.setdefault("pe", None)
            item.setdefault("source", "watchlist")
            result.append(item)
        return result


def _safe_float(val) -> Optional[float]:
    try:
        v = float(val)
        return None if v != v else v  # NaN check
    except (TypeError, ValueError):
        return None


# ── 预设筛选配置 ──────────────────────────────────────────────
SCREEN_PRESETS = {
    "large_cap": {
        "label": "大盘蓝筹",
        "desc":  "市值>500亿，成交活跃",
        "params": {"min_cap_yi": 500, "min_amount_wan": 10000, "top_n": 100},
    },
    "mid_cap": {
        "label": "中盘成长",
        "desc":  "市值100~1000亿，成长性好",
        "params": {"min_cap_yi": 100, "max_cap_yi": 1000, "min_amount_wan": 5000, "top_n": 300},
    },
    "active": {
        "label": "活跃热门",
        "desc":  "成交额前300，不限市值",
        "params": {"min_cap_yi": 50, "min_amount_wan": 10000, "top_n": 300},
    },
    "default": {
        "label": "默认筛选",
        "desc":  "市值>100亿，成交>5000万",
        "params": {"min_cap_yi": 100, "min_amount_wan": 5000, "top_n": 300},
    },
    "major_capital_pump": {
        "label": "主力拉升",
        "desc":  "中小盘活跃股，主力资金偏好标的（市值30~500亿，换手活跃）",
        "params": {
            "min_cap_yi":     30,     # 主力最低操作市值门槛
            "max_cap_yi":     500,    # 超大盘主力推动难度大，排除
            "min_amount_wan": 3000,   # 日均成交3000万以上，有流动性
            "min_price":      3.0,    # 允许低价股（主力常从低价拉起）
            "max_price":      200.0,
            "exclude_st":     True,
            "min_list_days":  180,    # 上市半年以上，排除次新
            "top_n":          300,    # 大幅扩大候选池
        },
    },
    "major_capital_accumulation": {
        "label": "主力建仓",
        "desc":  "低位横盘中小盘股，主力悄然吸筹（市值20~800亿，底部放量）",
        "params": {
            "min_cap_yi":     20,     # 小盘更容易被主力控盘
            "max_cap_yi":     800,    # 放宽至800亿（建仓期市值小，拉升后膨胀）
            "min_amount_wan": 1000,   # 建仓期成交额偏低，放宽门槛
            "min_price":      2.0,    # 低价股是主力建仓标的
            "max_price":      50.0,   # 放宽至50元（建仓期低价，拉升后价格上涨）
            "exclude_st":     True,
            "min_list_days":  360,    # 上市1年以上，有足够历史数据
            "top_n":          500,    # 大幅扩大候选池
        },
    },
}
