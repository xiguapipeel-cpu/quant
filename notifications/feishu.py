"""
飞书推送通知模块 v4 — 生产完整版
─────────────────────────────────────────────────────────────
功能：
  1. Webhook机器人（群消息，支持主群/预警群/回测群三路由）
  2. 消息限速队列（≤4条/秒）+ 自动重试（最多3次，退避1→3→8s）
  3. 富文本消息卡片（飞书card-2.0标准）：
       send_scan_report()      扫描报告（蓝色）
       send_alert()            预警消息（橙/红色）
       send_integrity_fail()   完整性自检失败专用
       send_pe_deviation()     PE偏差人工复核通知
       send_daily_summary()    盘后每日总结（紫色）
       send_holiday_notice()   节假日休市通知（绿色）
       send_backtest_done()    回测完成通知（蓝绿色）
       send_startup()          系统启动通知
       send_text()             纯文字消息
  4. 交易日历联动（非交易日自动静音）
  5. test_connection()         一键连通性测试
─────────────────────────────────────────────────────────────
配置（.env文件）：
  FEISHU_WEBHOOK_MAIN=https://open.feishu.cn/open-apis/bot/v2/hook/xxx
  FEISHU_WEBHOOK_ALERT=https://open.feishu.cn/open-apis/bot/v2/hook/yyy
  FEISHU_WEBHOOK_BACKTEST=https://open.feishu.cn/open-apis/bot/v2/hook/zzz
  FEISHU_AT_MEMBERS=ou_xxx,ou_yyy        # @指定成员（逗号分隔OpenID）
  FEISHU_MUTE_NON_TRADING=true           # 非交易日静音（默认true）
"""

import asyncio
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from utils.logger import setup_logger

logger = setup_logger("feishu")


# ══════════════════════════════════════════════════════════════
# 配置读取
# ══════════════════════════════════════════════════════════════

def _env(key: str, default: str = "") -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=".env", override=False)
    except ImportError:
        pass
    return os.environ.get(key, default).strip()


# 三个机器人配置
BOT_CONFIG = {
    "main": {
        "webhook":          _env("FEISHU_WEBHOOK_MAIN"),
        "label":            "主群",
        "mute_non_trading": _env("FEISHU_MUTE_NON_TRADING", "true").lower() != "false",
    },
    "alert": {
        "webhook":          _env("FEISHU_WEBHOOK_ALERT") or _env("FEISHU_WEBHOOK_MAIN"),
        "label":            "预警群",
        "mute_non_trading": False,   # 预警群不静音
    },
    "backtest": {
        "webhook":          _env("FEISHU_WEBHOOK_BACKTEST") or _env("FEISHU_WEBHOOK_MAIN"),
        "label":            "回测群",
        "mute_non_trading": False,
    },
}

# @成员 OpenID列表（空=不@任何人）
AT_MEMBERS: list[str] = [
    m.strip() for m in _env("FEISHU_AT_MEMBERS").split(",") if m.strip()
]


# ══════════════════════════════════════════════════════════════
# 消息任务
# ══════════════════════════════════════════════════════════════

@dataclass
class _MsgTask:
    bot_key: str
    payload: dict
    attempt: int = 0


# ══════════════════════════════════════════════════════════════
# 飞书通知器
# ══════════════════════════════════════════════════════════════

class FeishuNotifier:
    MAX_RETRIES    = 3
    RATE_LIMIT_RPS = 4          # 飞书官方上限5条/秒，留1条余量
    RETRY_BACKOFF  = [1, 3, 8]  # 秒

    def __init__(self):
        self._queue:       asyncio.Queue          = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        self._last_send:   float                  = 0.0

    def start(self):
        """启动后台worker（在asyncio事件循环中调用）"""
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker())
            logger.info("[飞书] 消息队列worker已启动")

    async def stop(self):
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

    # ──────────────────────────────────────────────────────────
    # 公开发送接口
    # ──────────────────────────────────────────────────────────

    async def send_scan_report(self, results: list, scan_type: str = "扫描") -> bool:
        """扫描报告卡片（蓝色）→ 主群"""
        passed   = [r for r in results if r.get("integrity_pass")]
        excluded = [r for r in results if not r.get("integrity_pass")]
        now_str  = datetime.now().strftime("%m-%d %H:%M")

        # 通过股票摘要行
        passed_lines = []
        for r in passed[:8]:
            pe    = r.get("pe")
            cap   = r.get("market_cap")
            price = r.get("price")
            pe_s   = f"PE {pe:.1f}" if isinstance(pe, float) else "PE—"
            cap_s  = f"{cap:.0f}亿"  if isinstance(cap, float) else "市值—"
            prc_s  = f"¥{price:.2f} " if isinstance(price, float) else ""
            warn   = " ⚠️" if (r.get("validation") or {}).get("pe", {}).get("needs_review") else ""
            passed_lines.append(f"**{r['name']}**({r['code']}) {prc_s}| {pe_s}{warn} | {cap_s}")

        excl_lines = [
            f"{r['name']}：{r.get('decision_reason','数据缺失')[:45]}"
            for r in excluded[:5]
        ]

        payload = _build_scan_card(
            scan_type=scan_type, time_str=now_str,
            total=len(results), passed_n=len(passed), excl_n=len(excluded),
            passed_lines=passed_lines, excl_lines=excl_lines,
            at_members=AT_MEMBERS,
        )
        return await self._enqueue("main", payload)

    async def send_alert(
        self,
        title:  str,
        body:   str,
        stock:  str   = "",
        level:  str   = "warning",   # warning | critical
    ) -> bool:
        """预警卡片（橙/红）→ 预警群"""
        icon     = "🔴" if level == "critical" else "⚠️"
        template = "red" if level == "critical" else "orange"
        at       = AT_MEMBERS if level == "critical" else []
        payload  = _build_alert_card(
            title=f"{icon} {title}", body=body,
            stock=stock, template=template, at_members=at,
        )
        return await self._enqueue("alert", payload)

    async def send_integrity_fail(self, stock_name: str, code: str, missing: list) -> bool:
        """完整性自检失败专用"""
        fields_md = " · ".join(f"**{m}**" for m in missing)
        return await self.send_alert(
            title=f"完整性自检失败 — {stock_name}({code})",
            body=(
                f"缺失字段：{fields_md}\n\n"
                f"> **规则**：股价/PE/市值/近期事件四项全有才可入决策\n"
                f"> **处理**：已从决策列表移除，不猜测，不补全，等数据齐全后重试"
            ),
            stock=f"{stock_name}({code})",
            level="warning",
        )

    async def send_pe_deviation(
        self,
        stock_name: str, code: str,
        src1: str, val1: float,
        src2: str, val2: float,
        deviation: float,
    ) -> bool:
        """PE交叉验证偏差超阈值，需人工复核"""
        return await self.send_alert(
            title=f"PE数据偏差过大 — {stock_name}",
            body=(
                f"**{stock_name}**({code}) PE交叉验证偏差 **{deviation:.1%}** > 5%\n\n"
                f"| 数据源 | PE值 |\n"
                f"|--------|------|\n"
                f"| {src1} | {val1:.2f} |\n"
                f"| {src2} | {val2:.2f} |\n\n"
                f"> 系统已标记此股票为「待人工复核」，暂不参与自动决策"
            ),
            stock=stock_name,
            level="warning",
        )

    async def send_daily_summary(self, results: list) -> bool:
        """盘后每日总结（紫色）→ 主群"""
        passed   = [r for r in results if r.get("integrity_pass")]
        excluded = [r for r in results if not r.get("integrity_pass")]
        date_str = datetime.now().strftime("%Y年%m月%d日")

        table_rows = []
        for r in passed[:10]:
            pe  = r.get("pe")
            cap = r.get("market_cap")
            table_rows.append(
                f"| {r['name']}({r['code']}) "
                f"| ¥{r.get('price','—')} "
                f"| {f'{pe:.1f}' if isinstance(pe,float) else '—'} "
                f"| {f'{cap:.0f}亿' if isinstance(cap,float) else '—'} |"
            )
        table_md = (
            "| 股票 | 价格 | PE | 市值 |\n"
            "|------|------|----|------|\n"
            + "\n".join(table_rows)
        ) if table_rows else "暂无通过股票"

        excl_md = "\n".join(
            f"- {r['name']}：{r.get('decision_reason','数据缺失')[:40]}"
            for r in excluded[:5]
        ) or "无"

        payload = _build_daily_card(
            date_str=date_str,
            total=len(results), passed_n=len(passed), excl_n=len(excluded),
            table_md=table_md, excl_md=excl_md,
            at_members=AT_MEMBERS,
        )
        return await self._enqueue("main", payload)

    async def send_holiday_notice(
        self,
        holiday_name: str,
        start_date:   str,
        end_date:     str,
        resume_date:  str,
        trading_days: int,
    ) -> bool:
        """节假日休市通知（绿色）→ 主群"""
        payload = _build_holiday_card(
            holiday_name=holiday_name,
            start_date=start_date, end_date=end_date,
            resume_date=resume_date, trading_days=trading_days,
        )
        return await self._enqueue("main", payload)

    async def send_backtest_done(self, strategy: str, metrics: dict) -> bool:
        """回测完成通知（蓝绿色）→ 回测群"""
        payload = _build_backtest_card(strategy=strategy, metrics=metrics)
        return await self._enqueue("backtest", payload)

    async def send_startup(self, watchlist_count: int, source_count: int) -> bool:
        """系统启动通知（绿色）→ 主群"""
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = _build_text_card(
            title="🟢 量化系统已启动",
            body=(
                f"启动时间：{now_str}\n"
                f"监控股票：**{watchlist_count}** 只\n"
                f"数据源：**{source_count}** 个（含研报/行情/公告）\n"
                f"搜索引擎：Tavily → Serper → Firecrawl → Jina（自动切换）\n"
                f"每日扫描：盘前09:00 / 盘中11:00 / 盘后15:30"
            ),
            template="green",
        )
        return await self._enqueue("main", payload)

    async def send_text(self, text: str, bot: str = "main") -> bool:
        """纯文字消息"""
        payload = {"msg_type": "text", "content": {"text": text}}
        return await self._enqueue(bot, payload)

    async def test_connection(self, bot: str = "main") -> dict:
        """测试Webhook连通性，返回{ok, code, msg, latency_ms}"""
        import aiohttp, time as _time
        cfg = BOT_CONFIG.get(bot, {})
        webhook = cfg.get("webhook", "")
        if not webhook:
            return {"ok": False, "msg": f"[{bot}] 未配置Webhook URL", "latency_ms": 0}

        payload = {
            "msg_type": "text",
            "content": {"text": f"[量化系统] 连通性测试 {datetime.now().strftime('%H:%M:%S')}"},
        }
        t0 = _time.monotonic()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook, json=payload,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    data = await resp.json(content_type=None)
                    latency = int((_time.monotonic() - t0) * 1000)
                    code = data.get("code", data.get("StatusCode", -1))
                    ok   = code == 0
                    logger.info(f"[飞书] 连通性测试: ok={ok} code={code} {latency}ms")
                    return {"ok": ok, "code": code, "msg": data.get("msg",""), "latency_ms": latency}
        except Exception as e:
            latency = int((_time.monotonic() - t0) * 1000)
            logger.error(f"[飞书] 连通性测试失败: {e}")
            return {"ok": False, "msg": str(e), "latency_ms": latency}

    # ──────────────────────────────────────────────────────────
    # 内部：队列 + 限速 + 重试
    # ──────────────────────────────────────────────────────────

    async def _enqueue(self, bot_key: str, payload: dict) -> bool:
        cfg = BOT_CONFIG.get(bot_key, {})
        if not cfg.get("webhook"):
            logger.debug(f"[飞书] [{cfg.get('label',bot_key)}] 未配置Webhook，跳过")
            return False

        # 非交易日静音
        if cfg.get("mute_non_trading"):
            try:
                from holidays.calendar import calendar as cal
                if not cal.is_trading_day():
                    logger.info(f"[飞书] [{cfg['label']}] 非交易日静音")
                    return False
            except Exception:
                pass

        await self._queue.put(_MsgTask(bot_key=bot_key, payload=payload))
        return True

    async def _worker(self):
        import aiohttp
        while True:
            task: _MsgTask = await self._queue.get()

            # 限速
            gap = time.monotonic() - self._last_send
            min_gap = 1.0 / self.RATE_LIMIT_RPS
            if gap < min_gap:
                await asyncio.sleep(min_gap - gap)

            cfg     = BOT_CONFIG.get(task.bot_key, {})
            webhook = cfg.get("webhook", "")
            label   = cfg.get("label", task.bot_key)

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        webhook, json=task.payload,
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        data = await resp.json(content_type=None)
                        code = data.get("code", data.get("StatusCode", -1))
                        if code == 0:
                            logger.info(f"[飞书] ✓ [{label}] 推送成功")
                            self._last_send = time.monotonic()
                        elif code == 9499:
                            logger.warning(f"[飞书] [{label}] 限速，1s后重试")
                            await asyncio.sleep(1)
                            await self._queue.put(task)
                        else:
                            await self._maybe_retry(task, f"code={code}: {data.get('msg','')}")
            except asyncio.TimeoutError:
                await self._maybe_retry(task, "timeout")
            except Exception as e:
                logger.error(f"[飞书] [{label}] 异常: {e}")
                await self._maybe_retry(task, str(e))
            finally:
                self._queue.task_done()

    async def _maybe_retry(self, task: _MsgTask, reason: str):
        if task.attempt < self.MAX_RETRIES:
            wait = self.RETRY_BACKOFF[task.attempt]
            logger.info(f"[飞书] 第{task.attempt+1}次重试，等待{wait}s（{reason}）")
            await asyncio.sleep(wait)
            task.attempt += 1
            await self._queue.put(task)
        else:
            logger.error(f"[飞书] {self.MAX_RETRIES}次重试全部失败，放弃（{reason}）")


# ══════════════════════════════════════════════════════════════
# 卡片构建函数（飞书interactive card格式）
# ══════════════════════════════════════════════════════════════

def _at_elements(at_members: list) -> list:
    if not at_members:
        return []
    at_md = " ".join(f"<at id={uid}></at>" for uid in at_members)
    return [{"tag": "div", "text": {"tag": "lark_md", "content": at_md}}]


def _build_scan_card(scan_type, time_str, total, passed_n, excl_n,
                     passed_lines, excl_lines, at_members):
    elements = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**时间**：{time_str}　**总计**：{total}只\n"
                    f"**通过**：{passed_n}只　**排除**：{excl_n}只"
                )
            }
        },
        {"tag": "hr"},
    ]
    if passed_lines:
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "**✅ 通过自检（四项全有）**\n" + "\n".join(passed_lines)
            }
        })
    if excl_lines:
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "**❌ 排除（数据缺失）**\n" + "\n".join(excl_lines)
            }
        })
    elements += _at_elements(at_members)
    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title":    {"tag": "plain_text", "content": f"📊 {scan_type}扫描报告"},
                "template": "blue",
            },
            "elements": elements,
        }
    }


def _build_alert_card(title, body, stock, template, at_members):
    elements = [{"tag": "div", "text": {"tag": "lark_md", "content": body}}]
    if stock:
        elements.insert(0, {
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**相关标的**：{stock}"}
        })
    elements += _at_elements(at_members)
    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title":    {"tag": "plain_text", "content": title},
                "template": template,
            },
            "elements": elements,
        }
    }


def _build_daily_card(date_str, total, passed_n, excl_n, table_md, excl_md, at_members):
    elements = [
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**日期**：{date_str}　**分析**：{total}只　**通过**：{passed_n}只"}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": "**通过股票明细**\n" + table_md}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": "**排除股票原因**\n" + excl_md}},
    ]
    elements += _at_elements(at_members)
    return {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": "📈 盘后每日总结"}, "template": "purple"},
            "elements": elements,
        }
    }


def _build_holiday_card(holiday_name, start_date, end_date, resume_date, trading_days):
    return {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🎉 {holiday_name}休市通知"}, "template": "green"},
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**假期**：{start_date} 至 {end_date}\n"
                            f"**复市**：{resume_date}\n"
                            f"**休市时长**：{trading_days}个交易日\n\n"
                            f"> 系统已暂停自动扫描，复市日自动恢复"
                        )
                    }
                }
            ],
        }
    }


def _build_backtest_card(strategy, metrics):
    ret  = metrics.get("total_return",      "N/A")
    ann  = metrics.get("annualized_return", "N/A")
    dd   = metrics.get("max_drawdown",      "N/A")
    sh   = metrics.get("sharpe_ratio",      "N/A")
    wr   = metrics.get("win_rate",          "N/A")
    pf   = metrics.get("profit_factor",     "N/A")
    trd  = metrics.get("total_trades",      "N/A")
    return {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🔬 回测完成 — {strategy}"}, "template": "turquoise"},
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"| 指标 | 数值 |\n"
                            f"|------|------|\n"
                            f"| 总收益率 | **{ret}** |\n"
                            f"| 年化收益 | **{ann}** |\n"
                            f"| 最大回撤 | {dd} |\n"
                            f"| 夏普比率 | {sh} |\n"
                            f"| 胜率 | {wr} |\n"
                            f"| 盈亏比 | {pf} |\n"
                            f"| 总交易次数 | {trd} |\n"
                        )
                    }
                }
            ],
        }
    }


def _build_text_card(title, body, template="blue"):
    return {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": title}, "template": template},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": body}}],
        }
    }


# 全局单例
notifier = FeishuNotifier()
