"""
回测数据加载器
从AKShare拉取历史行情 + 基本面快照，构建回测数据集
"""

import asyncio
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
from utils.logger import setup_logger

# 东方财富行情接口不走代理（直连更稳定）
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
           "http_proxy", "https_proxy", "all_proxy"):
    os.environ.pop(_k, None)

logger = setup_logger("bt_data")

CACHE_DIR = Path("backtest_cache")
CACHE_DIR.mkdir(exist_ok=True)


class BacktestDataLoader:
    """
    回测数据加载器
    - 日线OHLCV（复权）
    - 滚动PE / 市值快照
    - 交易日历
    """

    async def load_daily_bars(
        self,
        code: str,
        market: str,
        start: str,
        end: str,
        adjust: str = "qfq",   # qfq=前复权 hfq=后复权 ""=不复权
    ) -> Optional[list[dict]]:
        """
        拉取日线数据，返回 list[dict] 每条包含:
        date / open / high / low / close / volume / amount
        """
        cache_key = f"{code}_{start}_{end}_{adjust}"
        cache_path = CACHE_DIR / f"{cache_key}.json"

        if cache_path.exists():
            logger.info(f"[数据] {code} 使用本地缓存 {cache_path.name}")
            with open(cache_path, encoding="utf-8") as f:
                return json.load(f)

        # 数据源优先级：akshare(东方财富) → 新浪财经 → 模糊缓存
        bars = await self._try_akshare(code, market, start, end, adjust)
        if bars is None:
            bars = await self._try_tencent(code, market, start, end, adjust)
        if bars is None:
            bars = self._fuzzy_cache_fallback(code, start, end, adjust)
            if bars:
                return bars
            return None

        # 写缓存
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(bars, f, ensure_ascii=False)
        logger.info(f"[数据] {code} 加载{len(bars)}条日线 ({start}~{end})")
        return bars

    async def _try_akshare(self, code, market, start, end, adjust) -> Optional[list[dict]]:
        """数据源1: akshare (东方财富)"""
        try:
            import akshare as ak
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: ak.stock_zh_a_hist(
                    symbol=code, period="daily",
                    start_date=start.replace("-", ""),
                    end_date=end.replace("-", ""),
                    adjust=adjust,
                )
            )
            if df is None or df.empty:
                return None
            col_map = {
                "日期": "date", "开盘": "open", "最高": "high",
                "最低": "low", "收盘": "close", "成交量": "volume",
                "成交额": "amount", "涨跌幅": "pct_change",
            }
            df = df.rename(columns=col_map)
            df["date"] = df["date"].astype(str)
            return df[["date", "open", "high", "low", "close", "volume", "amount", "pct_change"]].to_dict("records")
        except Exception as e:
            logger.debug(f"[数据] {code} akshare失败: {e}")
            return None

    async def _try_tencent(self, code, market, start, end, adjust="qfq") -> Optional[list[dict]]:
        """数据源2: 腾讯财经（支持前复权，直连无需代理）"""
        try:
            import requests
            prefix = "sh" if market == "SH" else "sz"
            symbol = f"{prefix}{code}"
            fq = "qfq" if adjust == "qfq" else ("hfq" if adjust == "hfq" else "")
            url = (
                f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
                f"?param={symbol},day,{start},{end},800,{fq}"
            )
            session = requests.Session()
            session.trust_env = False  # 绕过系统代理 / Clash TUN
            session.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, lambda: session.get(url, timeout=15))
            if resp.status_code != 200:
                return None
            data = resp.json().get("data", {}).get(symbol, {})
            # 前复权用 qfqday，不复权用 day
            klines = data.get("qfqday") or data.get("hfqday") or data.get("day") or []
            if not klines:
                return None
            # 腾讯格式: [date, open, close, high, low, volume]
            bars = []
            for k in klines:
                if len(k) < 6:
                    continue
                d = k[0]
                if d < start or d > end:
                    continue
                bars.append({
                    "date":       d,
                    "open":       float(k[1]),
                    "high":       float(k[3]),
                    "low":        float(k[4]),
                    "close":      float(k[2]),
                    "volume":     int(float(k[5])),
                    "amount":     0,
                    "pct_change": 0,
                })
            if not bars:
                return None
            # 计算涨跌幅
            for i in range(len(bars)):
                if i == 0:
                    bars[i]["pct_change"] = 0
                else:
                    prev = bars[i - 1]["close"]
                    if prev > 0:
                        bars[i]["pct_change"] = round((bars[i]["close"] / prev - 1) * 100, 2)
            logger.info(f"[数据] {code} 腾讯财经加载{len(bars)}条{fq}日线")
            return bars
        except Exception as e:
            logger.debug(f"[数据] {code} 腾讯失败: {e}")
            return None

    def _fuzzy_cache_fallback(self, code: str, start: str, end: str, adjust: str) -> Optional[list[dict]]:
        """
        网络不可用时，模糊匹配最接近的缓存文件。
        匹配规则：同 code + 同 adjust，start 相同或更早，end 相同或更近，
        优先选 end 最大的（覆盖范围最广）。
        """
        import glob
        pattern = f"{code}_*_{adjust}.json"
        candidates = list(CACHE_DIR.glob(pattern))
        if not candidates:
            return None

        best = None
        best_end = ""
        for p in candidates:
            parts = p.stem.split("_")  # code_start_end_adjust
            if len(parts) < 4:
                continue
            c_start, c_end = parts[1], parts[2]
            # start 不能晚于请求的 start，end 要尽可能大
            if c_start <= start and c_end > best_end:
                best = p
                best_end = c_end

        if not best:
            return None

        logger.info(f"[数据] {code} 网络不可用，使用模糊缓存 {best.name} (覆盖至{best_end})")
        with open(best, encoding="utf-8") as f:
            bars = json.load(f)

        # 按请求的 start~end 范围裁剪
        bars = [b for b in bars if start <= b.get("date", "") <= end]
        return bars if bars else None

    async def load_pe_series(self, code: str, start: str, end: str) -> Optional[list[dict]]:
        """拉取历史PE序列（用于PE过滤策略的回测）"""
        try:
            import akshare as ak
            loop = asyncio.get_event_loop()

            df = await loop.run_in_executor(
                None,
                lambda: ak.stock_a_pe(symbol=code)
            )
            if df is None or df.empty:
                return None

            df["date"] = df["date"].astype(str)
            df = df[(df["date"] >= start) & (df["date"] <= end)]
            return df[["date", "pe"]].to_dict("records")

        except Exception as e:
            logger.warning(f"[数据] {code} PE序列获取失败: {e} (将跳过PE过滤)")
            return None

    async def load_all_stocks(
        self,
        watchlist: list[dict],
        start: str,
        end: str,
    ) -> dict[str, list[dict]]:
        """并行加载所有股票日线数据"""
        tasks = {
            s["code"]: self.load_daily_bars(s["code"], s["market"], start, end)
            for s in watchlist
        }
        results = {}
        for code, coro in tasks.items():
            try:
                bars = await coro
                if bars:
                    results[code] = bars
            except Exception as e:
                logger.error(f"[数据] {code} 加载异常: {e}")
        return results
