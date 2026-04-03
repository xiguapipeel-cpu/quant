"""
多渠道推送通知模块
──────────────────────────────────────────────────────────
支持渠道（均免费）：

1. 企业微信机器人 Webhook（★ 最推荐）
   - 注册：https://work.weixin.qq.com  个人可免费注册
   - 创建企业 → 应用管理 → 群机器人 → 添加到群 → 复制 Webhook URL
   - .env: WECOM_WEBHOOK=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx
   - 无每日限额，支持 Markdown、文字、图文

2. 钉钉机器人 Webhook（完全免费）
   - 群设置 → 机器人 → 自定义 → 复制 Webhook URL
   - .env: DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxx
   -       DINGTALK_SECRET=SECxxx（开启加签时必填）

3. Telegram Bot（免费，需能访问 Telegram）
   - BotFather 创建 Bot → 获取 token；与 bot 对话获取 chat_id
   - .env: TG_BOT_TOKEN=xxxx:xxx  TG_CHAT_ID=123456789

4. Server酱（免费5条/天，简单快捷）
   - https://sct.ftqq.com  微信登录获取 SendKey
   - .env: SERVERCHAN_KEY=SCTxxx

配置多个渠道时，会依次推送到所有已配置的渠道。
──────────────────────────────────────────────────────────
"""

import asyncio
import hashlib
import hmac
import json as _json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from datetime import datetime
from pathlib import Path
from typing import Optional
from utils.logger import setup_logger

logger = setup_logger("push")


# ── 辅助函数 ───────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DOTENV_PATH  = _PROJECT_ROOT / ".env"

def _env(key: str, default: str = "") -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=str(_DOTENV_PATH), override=False)
    except ImportError:
        # python-dotenv 未安装时手动解析 .env
        _load_env_fallback()
    return os.environ.get(key, default).strip()


def _load_env_fallback():
    """手动解析 .env 文件（python-dotenv 不可用时的降级方案）"""
    if not _DOTENV_PATH.exists():
        return
    for line in _DOTENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip()
        if k and k not in os.environ:
            os.environ[k] = v


def _is_trading_day() -> bool:
    try:
        from holidays.calendar import calendar as cal
        return cal.is_trading_day()
    except Exception:
        return datetime.now().weekday() < 5


async def _http_post(url: str, payload: dict, headers: Optional[dict] = None,
                     timeout: int = 10) -> dict:
    """异步 HTTP POST，返回响应 dict"""
    def _do():
        data = _json.dumps(payload, ensure_ascii=False).encode("utf-8")
        h = {"Content-Type": "application/json; charset=utf-8"}
        if headers:
            h.update(headers)
        req = urllib.request.Request(url, data=data, headers=h, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
                try:
                    return _json.loads(body)
                except Exception:
                    return {"raw": body}
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8")
            except Exception:
                body = str(e)
            return {"error": str(e), "body": body}
        except Exception as e:
            return {"error": str(e)}

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _do)


# ═══════════════════════════════════════════════════════════
# 渠道 1：企业微信机器人
# ═══════════════════════════════════════════════════════════

class WeCom:
    """企业微信群机器人 Webhook"""

    @property
    def webhook(self) -> str:
        return _env("WECOM_WEBHOOK")

    @property
    def configured(self) -> bool:
        return bool(self.webhook)

    async def send_markdown(self, content: str) -> dict:
        if not self.configured:
            return {"error": "WECOM_WEBHOOK 未配置"}
        payload = {"msgtype": "markdown", "markdown": {"content": content}}
        result = await _http_post(self.webhook, payload)
        ok = result.get("errcode") == 0
        logger.info(f"[企业微信] {'成功' if ok else '失败'}: {result}")
        return result

    async def send_text(self, content: str, mentioned_list: list[str] | None = None) -> dict:
        if not self.configured:
            return {"error": "WECOM_WEBHOOK 未配置"}
        payload: dict = {"msgtype": "text", "text": {"content": content}}
        if mentioned_list:
            payload["text"]["mentioned_list"] = mentioned_list
        result = await _http_post(self.webhook, payload)
        return result

    async def test(self) -> dict:
        t0 = time.time()
        result = await self.send_text(
            f"✅ QuantSystem 连接测试\n"
            f"企业微信推送已配置成功！\n"
            f"时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        return {
            "ok":         result.get("errcode") == 0,
            "msg":        result.get("errmsg", str(result)),
            "latency_ms": int((time.time() - t0) * 1000),
        }


# ═══════════════════════════════════════════════════════════
# 渠道 2：钉钉机器人
# ═══════════════════════════════════════════════════════════

class DingTalk:
    """钉钉自定义机器人 Webhook（支持加签安全）"""

    @property
    def webhook(self) -> str:
        return _env("DINGTALK_WEBHOOK")

    @property
    def secret(self) -> str:
        return _env("DINGTALK_SECRET")

    @property
    def configured(self) -> bool:
        return bool(self.webhook)

    def _signed_url(self) -> str:
        """加签模式：在 URL 上附加 timestamp + sign"""
        if not self.secret:
            return self.webhook
        ts = str(round(time.time() * 1000))
        string_to_sign = f"{ts}\n{self.secret}"
        sign = b64encode(
            hmac.new(
                self.secret.encode("utf-8"),
                string_to_sign.encode("utf-8"),
                digestmod=hashlib.sha256,
            ).digest()
        ).decode("utf-8")
        return (
            self.webhook
            + f"&timestamp={ts}"
            + f"&sign={urllib.parse.quote_plus(sign)}"
        )

    async def send_markdown(self, title: str, text: str) -> dict:
        if not self.configured:
            return {"error": "DINGTALK_WEBHOOK 未配置"}
        payload = {
            "msgtype":  "markdown",
            "markdown": {"title": title, "text": text},
        }
        result = await _http_post(self._signed_url(), payload)
        ok = result.get("errcode") == 0
        logger.info(f"[钉钉] {'成功' if ok else '失败'}: {result}")
        return result

    async def test(self) -> dict:
        t0 = time.time()
        result = await self.send_markdown(
            title="✅ QuantSystem 连接测试",
            text=(
                "## ✅ QuantSystem 连接测试\n\n"
                "钉钉推送已配置成功！\n\n"
                f"> 时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            ),
        )
        return {
            "ok":         result.get("errcode") == 0,
            "msg":        result.get("errmsg", str(result)),
            "latency_ms": int((time.time() - t0) * 1000),
        }


# ═══════════════════════════════════════════════════════════
# 渠道 3：Telegram Bot
# ═══════════════════════════════════════════════════════════

class Telegram:
    """Telegram Bot API 推送"""

    @property
    def token(self) -> str:
        return _env("TG_BOT_TOKEN")

    @property
    def chat_id(self) -> str:
        return _env("TG_CHAT_ID")

    @property
    def configured(self) -> bool:
        return bool(self.token and self.chat_id)

    async def send_message(self, text: str, parse_mode: str = "HTML") -> dict:
        if not self.configured:
            return {"error": "TG_BOT_TOKEN / TG_CHAT_ID 未配置"}
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": parse_mode}
        result = await _http_post(url, payload)
        ok = bool(result.get("ok"))
        logger.info(f"[Telegram] {'成功' if ok else '失败'}: {result.get('description','')}")
        return result

    async def test(self) -> dict:
        t0 = time.time()
        result = await self.send_message(
            f"✅ <b>QuantSystem 连接测试</b>\n\n"
            f"Telegram 推送已配置成功！\n"
            f"时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        return {
            "ok":         bool(result.get("ok")),
            "msg":        result.get("description", "success" if result.get("ok") else str(result)),
            "latency_ms": int((time.time() - t0) * 1000),
        }


# ═══════════════════════════════════════════════════════════
# 渠道 4：Server酱（免费版每天5条）
# ═══════════════════════════════════════════════════════════

class ServerChan:
    """Server酱 SendKey 推送（微信通知）"""

    @property
    def key(self) -> str:
        return _env("SERVERCHAN_KEY")

    @property
    def configured(self) -> bool:
        return bool(self.key)

    async def send(self, title: str, desp: str = "") -> dict:
        if not self.configured:
            return {"error": "SERVERCHAN_KEY 未配置"}
        url = f"https://sctapi.ftqq.com/{self.key}.send"
        payload = {"title": title[:32], "desp": desp}
        result = await _http_post(url, payload)
        ok = result.get("data", {}).get("errno") == 0 or result.get("errno") == 0
        logger.info(f"[Server酱] {'成功' if ok else '失败'}: {result}")
        return result

    async def test(self) -> dict:
        t0 = time.time()
        result = await self.send(
            title="✅ QuantSystem 测试",
            desp=f"Server酱推送配置成功！\n\n时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        )
        ok = result.get("data", {}).get("errno") == 0 or result.get("errno") == 0
        return {
            "ok":         ok,
            "msg":        str(result),
            "latency_ms": int((time.time() - t0) * 1000),
        }


# ═══════════════════════════════════════════════════════════
# 统一推送接口（多渠道广播）
# ═══════════════════════════════════════════════════════════

class MultiChannelPusher:
    """同时向所有已配置的渠道推送"""

    def __init__(self):
        self.wecom     = WeCom()
        self.dingtalk  = DingTalk()
        self.telegram  = Telegram()
        self.serverchan = ServerChan()
        self._mute_non_trading = True

    def _check_mute(self) -> bool:
        self._mute_non_trading = _env("PUSH_MUTE_NON_TRADING", "true").lower() != "false"
        return self._mute_non_trading and not _is_trading_day()

    def channels_configured(self) -> list[str]:
        ch = []
        if self.wecom.configured:     ch.append("wecom")
        if self.dingtalk.configured:  ch.append("dingtalk")
        if self.telegram.configured:  ch.append("telegram")
        if self.serverchan.configured: ch.append("serverchan")
        return ch

    # ── 两阶段消息构建 ─────────────────────────────────────

    def _stock_line_wecom(self, s: dict, idx: int) -> str:
        pct = s.get("pct_change")
        pct_str = f"+{pct:.2f}%" if pct and pct >= 0 else (f"{pct:.2f}%" if pct else "—")
        sig = (s.get("signal_reason") or "")[:25]
        return (
            f"| **{s.get('name','')}** | {s.get('code','')} "
            f"| <font color='warning'>{pct_str}</font> "
            f"| {s.get('signal_date','')} | {sig} |"
        )

    # 企业微信 Markdown 消息体上限 4096 字节，按每行约 80 字节估算安全上限
    _WECOM_MAX_ROWS = 40

    def _build_markdown_wecom(self, buy_stocks: list[dict], watch_stocks: list[dict],
                               scan_time: str, trigger: str) -> str:
        buy_n, watch_n = len(buy_stocks), len(watch_stocks)
        if buy_n == 0 and watch_n == 0:
            return (
                f"## 📊 主力建仓选股 | {scan_time[:10]}\n\n"
                f"> {trigger}扫描 · 今日无信号，请明日继续关注"
            )
        lines = [
            f"## 📈 主力建仓选股 | {scan_time[:10]}",
            f"> {trigger}扫描 · BUY {buy_n}只 · WATCH {watch_n}只",
        ]
        remaining = self._WECOM_MAX_ROWS
        if buy_stocks:
            show_buy = buy_stocks[:remaining]
            lines += [
                "", "### 🔴 建仓完毕 — 可入场",
                "| 名称 | 代码 | 涨幅 | 信号日 | 信号 |",
                "|------|------|------|--------|------|",
            ]
            for i, s in enumerate(show_buy):
                lines.append(self._stock_line_wecom(s, i))
            if buy_n > len(show_buy):
                lines.append(f"> … 还有 {buy_n - len(show_buy)} 只，详见选股中心")
            remaining -= len(show_buy)
        if watch_stocks and remaining > 0:
            show_watch = watch_stocks[:remaining]
            lines += [
                "", "### 🟡 建仓中 — 观察名单",
                "| 名称 | 代码 | 涨幅 | 信号日 | 信号 |",
                "|------|------|------|--------|------|",
            ]
            for i, s in enumerate(show_watch):
                lines.append(self._stock_line_wecom(s, i))
            if watch_n > len(show_watch):
                lines.append(f"> … 还有 {watch_n - len(show_watch)} 只，详见选股中心")
        lines += ["", "> ⚠️ 选股结果仅供参考，不构成投资建议，注意仓位管理"]
        return "\n".join(lines)

    # 钉钉 Markdown 消息体上限约 20000 字节，宽裕很多
    _DINGTALK_MAX_ROWS = 40

    def _build_markdown_dingtalk(self, buy_stocks: list[dict], watch_stocks: list[dict],
                                  scan_time: str, trigger: str) -> tuple[str, str]:
        buy_n, watch_n = len(buy_stocks), len(watch_stocks)
        title = f"📈 主力建仓 | {scan_time[:10]} | BUY {buy_n} WATCH {watch_n}"
        if buy_n == 0 and watch_n == 0:
            return title, f"## {title}\n\n> 今日无信号"
        lines = [f"## {title}", f"> {trigger}扫描 · {scan_time}", ""]
        remaining = self._DINGTALK_MAX_ROWS
        if buy_stocks:
            show_buy = buy_stocks[:remaining]
            lines.append("### 🔴 建仓完毕 — 可入场")
            for i, s in enumerate(show_buy, 1):
                pct = s.get("pct_change")
                pct_str = f"+{pct:.2f}%" if pct and pct >= 0 else (f"{pct:.2f}%" if pct else "—")
                lines.append(
                    f"**{i}. {s.get('name','')}**（{s.get('code','')}）  "
                    f"涨幅：{pct_str}  {(s.get('signal_reason') or '')[:25]}"
                )
            if buy_n > len(show_buy):
                lines.append(f"> … 还有 {buy_n - len(show_buy)} 只")
            remaining -= len(show_buy)
        if watch_stocks and remaining > 0:
            show_watch = watch_stocks[:remaining]
            lines.append("\n### 🟡 建仓中 — 观察名单")
            for i, s in enumerate(show_watch, 1):
                lines.append(
                    f"{i}. **{s.get('name','')}**（{s.get('code','')}）  "
                    f"{(s.get('signal_reason') or '')[:25]}"
                )
            if watch_n > len(show_watch):
                lines.append(f"> … 还有 {watch_n - len(show_watch)} 只")
        lines.append("\n> ⚠️ 仅供参考，不构成投资建议")
        return title, "\n".join(lines)

    # Telegram 消息上限 4096 字符
    _TG_MAX_ROWS = 40

    def _build_telegram_html(self, buy_stocks: list[dict], watch_stocks: list[dict],
                              scan_time: str, trigger: str) -> str:
        buy_n, watch_n = len(buy_stocks), len(watch_stocks)
        if buy_n == 0 and watch_n == 0:
            return (
                f"📊 <b>主力建仓选股</b> | {scan_time[:10]}\n\n"
                f"🔍 {trigger}扫描 · 今日无信号"
            )
        lines = [
            f"📈 <b>主力建仓选股</b> | {scan_time[:10]}",
            f"🔍 {trigger}扫描 · BUY {buy_n} · WATCH {watch_n}", "",
        ]
        remaining = self._TG_MAX_ROWS
        if buy_stocks:
            show_buy = buy_stocks[:remaining]
            lines.append("🔴 <b>建仓完毕 — 可入场</b>")
            for i, s in enumerate(show_buy, 1):
                pct = s.get("pct_change")
                pct_str = f"+{pct:.2f}%" if pct and pct >= 0 else (f"{pct:.2f}%" if pct else "—")
                lines.append(f"{i}. <b>{s.get('name','')}</b>（{s.get('code','')}） {pct_str}")
            if buy_n > len(show_buy):
                lines.append(f"… 还有 {buy_n - len(show_buy)} 只")
            remaining -= len(show_buy)
        if watch_stocks and remaining > 0:
            show_watch = watch_stocks[:remaining]
            lines.append("\n🟡 <b>建仓中 — 观察名单</b>")
            for i, s in enumerate(show_watch, 1):
                lines.append(f"{i}. <b>{s.get('name','')}</b>（{s.get('code','')}）")
            if watch_n > len(show_watch):
                lines.append(f"… 还有 {watch_n - len(show_watch)} 只")
        lines.append("\n⚠️ 仅供参考，不构成投资建议")
        return "\n".join(lines)

    async def send_major_capital_scan(
        self,
        buy_stocks: list[dict],
        watch_stocks: list[dict],
        scan_time: str = "",
        trigger: str = "定时",
        ignore_trading_day: bool = False,
    ) -> dict:
        """向所有已配置渠道推送主力建仓选股报告（两阶段）"""
        if self._check_mute() and not ignore_trading_day:
            logger.info("[推送] 非交易日静音，跳过")
            return {"skipped": True, "reason": "非交易日"}

        scan_time = scan_time or datetime.now().strftime("%Y-%m-%d %H:%M")
        channels = self.channels_configured()

        if not channels:
            logger.warning("[推送] 未配置任何推送渠道")
            return {"error": "未配置任何推送渠道，请在设置中配置"}

        results = {}
        tasks = []

        if self.wecom.configured:
            md = self._build_markdown_wecom(buy_stocks, watch_stocks, scan_time, trigger)
            tasks.append(("wecom", self.wecom.send_markdown(md)))

        if self.dingtalk.configured:
            title, text = self._build_markdown_dingtalk(buy_stocks, watch_stocks, scan_time, trigger)
            tasks.append(("dingtalk", self.dingtalk.send_markdown(title, text)))

        if self.telegram.configured:
            html = self._build_telegram_html(buy_stocks, watch_stocks, scan_time, trigger)
            tasks.append(("telegram", self.telegram.send_message(html)))

        if self.serverchan.configured:
            buy_n, watch_n = len(buy_stocks), len(watch_stocks)
            sc_title = f"主力建仓 {scan_time[:10]} | BUY {buy_n} WATCH {watch_n}"
            def _fmt_pct(s):
                p = s.get('pct_change')
                if p:
                    return f" +{p:.2f}%" if p >= 0 else f" {p:.2f}%"
                return ""
            # Server酱免费版每条限 32KB，足够显示全部
            parts = []
            if buy_stocks:
                parts.append("**🔴 建仓完毕 — 可入场**")
                parts.extend(
                    f"- **{s.get('name','')}**（{s.get('code','')}）{_fmt_pct(s)}"
                    for s in buy_stocks
                )
            if watch_stocks:
                parts.append("\n**🟡 建仓中 — 观察名单**")
                parts.extend(
                    f"- {s.get('name','')}（{s.get('code','')}）"
                    for s in watch_stocks
                )
            sc_desp = "\n".join(parts) or "今日无信号"
            tasks.append(("serverchan", self.serverchan.send(sc_title, sc_desp)))

        # 并发推送
        for name, coro in tasks:
            try:
                results[name] = await coro
            except Exception as e:
                results[name] = {"error": str(e)}

        success = sum(
            1 for name, r in results.items()
            if (name == "wecom" and r.get("errcode") == 0)
            or (name == "dingtalk" and r.get("errcode") == 0)
            or (name == "telegram" and r.get("ok"))
            or (name == "serverchan" and (r.get("errno") == 0 or r.get("data", {}).get("errno") == 0))
        )
        logger.info(f"[推送] 完成 {success}/{len(tasks)} 渠道成功")
        return {"success_count": success, "total": len(tasks), "details": results}

    async def send_error_alert(self, error_msg: str) -> dict:
        """推送异常告警（忽略非交易日限制）"""
        text = f"⚠️ 选股任务异常\n时间：{datetime.now().strftime('%H:%M')}\n错误：{error_msg[:200]}"
        results = {}
        if self.wecom.configured:
            results["wecom"] = await self.wecom.send_text(text)
        if self.telegram.configured:
            results["telegram"] = await self.telegram.send_message(f"⚠️ <b>选股异常</b>\n{error_msg[:300]}")
        return results

    async def test_all(self) -> dict:
        """测试所有已配置渠道"""
        results = {}
        if self.wecom.configured:
            results["wecom"]      = await self.wecom.test()
        if self.dingtalk.configured:
            results["dingtalk"]   = await self.dingtalk.test()
        if self.telegram.configured:
            results["telegram"]   = await self.telegram.test()
        if self.serverchan.configured:
            results["serverchan"] = await self.serverchan.test()
        if not results:
            results["_none"] = {"ok": False, "msg": "请先配置至少一个推送渠道"}
        return results


# 全局单例
pusher = MultiChannelPusher()
