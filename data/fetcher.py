"""
AKShare + 东方财富API 数据采集模块
打通实时行情、基本面、PE、市值
"""

import asyncio
from typing import Optional
from utils.logger import setup_logger

logger = setup_logger("data_fetcher")


class AKShareFetcher:
    """
    AKShare数据采集器
    安装: pip install akshare
    """

    async def get_realtime_quote(self, code: str, market: str) -> Optional[dict]:
        """获取实时行情（价格、涨跌幅）"""
        try:
            import akshare as ak

            full_code = f"{'sh' if market=='SH' else 'sz'}{code}"
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(None, ak.stock_zh_a_spot_em)

            row = df[df["代码"] == code]
            if row.empty:
                logger.warning(f"[AKShare] {code} 无实时行情数据")
                return None

            r = row.iloc[0]
            result = {
                "price":      float(r.get("最新价", 0)),
                "change_pct": float(r.get("涨跌幅", 0)),
                "volume":     float(r.get("成交量", 0)),
                "turnover":   float(r.get("成交额", 0)),
                "source":     "AKShare/东方财富",
            }
            logger.info(f"[AKShare] {code} 实时价格: {result['price']} ({result['change_pct']:+.2f}%)")
            return result

        except ImportError:
            logger.error("[AKShare] 未安装，请运行: pip install akshare")
            return None
        except Exception as e:
            logger.error(f"[AKShare] {code} 行情获取失败: {e}")
            return None

    async def get_fundamental(self, code: str) -> Optional[dict]:
        """获取基本面数据（PE、市值、ROE等）"""
        try:
            import akshare as ak

            loop = asyncio.get_event_loop()

            # 东方财富基本面接口
            df = await loop.run_in_executor(None, lambda: ak.stock_zh_a_spot_em())
            row = df[df["代码"] == code]

            if row.empty:
                return None

            r = row.iloc[0]
            pe = r.get("市盈率-动态", None)
            market_cap = r.get("总市值", None)

            if pe is None or str(pe) in ["--", "nan", ""]:
                logger.warning(f"[AKShare] {code} PE数据为空")
                pe = None
            else:
                pe = float(pe)

            if market_cap is None or str(market_cap) in ["--", "nan", ""]:
                logger.warning(f"[AKShare] {code} 市值数据为空")
                market_cap = None
            else:
                market_cap = float(market_cap)

            return {
                "pe":          pe,
                "market_cap":  market_cap,
                "source":      "AKShare/东方财富",
            }

        except Exception as e:
            logger.error(f"[AKShare] {code} 基本面获取失败: {e}")
            return None

    async def get_financial_report(self, code: str) -> Optional[dict]:
        """获取最新财报数据"""
        try:
            import akshare as ak

            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None, lambda: ak.stock_financial_abstract_ths(symbol=code, indicator="按年度")
            )
            if df is None or df.empty:
                return None

            latest = df.iloc[0]
            return {
                "report_date": str(latest.get("报告期", "")),
                "revenue":     latest.get("营业总收入", None),
                "net_profit":  latest.get("净利润", None),
                "roe":         latest.get("净资产收益率", None),
                "source":      "AKShare/同花顺财务",
            }
        except Exception as e:
            logger.warning(f"[AKShare] {code} 财报获取失败: {e}")
            return None

    async def get_announcements(self, code: str, days: int = 30) -> list:
        """获取近期公告"""
        try:
            import akshare as ak
            from datetime import datetime, timedelta

            loop = asyncio.get_event_loop()
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

            df = await loop.run_in_executor(
                None, lambda: ak.stock_notice_report(symbol=code, start_date=start, end_date=end)
            )
            if df is None or df.empty:
                return []

            announcements = []
            for _, row in df.head(5).iterrows():
                announcements.append({
                    "date":  str(row.get("公告日期", "")),
                    "title": str(row.get("公告标题", "")),
                    "type":  str(row.get("公告类型", "")),
                })
            logger.info(f"[AKShare] {code} 获取{len(announcements)}条近期公告")
            return announcements

        except Exception as e:
            logger.warning(f"[AKShare] {code} 公告获取失败: {e}")
            return []


class EastMoneyFetcher:
    """
    东方财富API直接调用（无需AKShare，备用）
    """

    BASE_URL = "https://push2.eastmoney.com/api/qt/stock/get"

    async def get_quote(self, code: str, market: str) -> Optional[dict]:
        """直接调用东方财富行情接口"""
        import aiohttp

        # 东方财富市场代码：0=深圳, 1=上海
        secid = f"{'1' if market == 'SH' else '0'}.{code}"
        params = {
            "secid":  secid,
            "fields": "f43,f44,f45,f46,f47,f48,f116,f117,f162",
            # f43=最新价, f44=最高, f45=最低, f116=总市值, f162=市盈率TTM
            "ut":     "fa5fd1943c7b386f172d6893dbfba10b",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        d = data.get("data", {})
                        if not d:
                            return None

                        price = d.get("f43", 0) / 100  # 东方财富返回整数需除以100
                        pe    = d.get("f162", 0) / 100
                        mcap  = d.get("f116", 0)       # 单位：元

                        if price <= 0:
                            return None

                        return {
                            "price":      price,
                            "pe":         pe if pe > 0 else None,
                            "market_cap": mcap if mcap > 0 else None,
                            "source":     "东方财富API",
                        }
        except Exception as e:
            logger.warning(f"[东方财富API] {code} 请求失败: {e}")
        return None
