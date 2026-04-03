"""
微信推送通知模块 — PushPlus
─────────────────────────────────────────────────────────────
原理：PushPlus (pushplus.plus) 通过微信公众号推送消息
  1. 访问 https://www.pushplus.plus 用微信扫码登录
  2. 获取 token（个人中心 → 我的Token）
  3. 关注「pushplus推送加」公众号（否则收不到消息）
  4. 将 token 写入 .env 文件：PUSHPLUS_TOKEN=your_token

配置（.env 文件）：
  PUSHPLUS_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   # 必填
  PUSHPLUS_TOPIC=                                   # 群组推送（选填，群主才有）
  WECHAT_MUTE_NON_TRADING=true                      # 非交易日静音（默认true）

免费额度：200条/天（每条最大64KB）
─────────────────────────────────────────────────────────────
"""

import asyncio
import os
import time
from datetime import datetime
from typing import Optional
from utils.logger import setup_logger

logger = setup_logger("wechat")


def _env(key: str, default: str = "") -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=".env", override=False)
    except ImportError:
        pass
    return os.environ.get(key, default).strip()


# ══════════════════════════════════════════════════════════════
# PushPlus 推送器
# ══════════════════════════════════════════════════════════════

class WeChatPushPlus:
    API_URL = "https://www.pushplus.plus/send"
    TIMEOUT = 10

    def __init__(self):
        self._token = ""
        self._topic = ""
        self._mute_non_trading = True

    def _load_config(self):
        self._token = _env("PUSHPLUS_TOKEN")
        self._topic = _env("PUSHPLUS_TOPIC", "")
        self._mute_non_trading = _env("WECHAT_MUTE_NON_TRADING", "true").lower() != "false"

    @property
    def configured(self) -> bool:
        self._load_config()
        return bool(self._token)

    def _is_trading_day(self) -> bool:
        try:
            from holidays.calendar import calendar as cal
            return cal.is_trading_day()
        except Exception:
            return datetime.now().weekday() < 5

    async def _post(self, title: str, content: str, template: str = "html") -> dict:
        """异步 HTTP POST 到 PushPlus"""
        self._load_config()
        if not self._token:
            return {"code": -1, "msg": "PUSHPLUS_TOKEN 未配置，请在 .env 中设置"}

        import urllib.request
        import urllib.error
        import json as _json

        payload = {
            "token":    self._token,
            "title":    title,
            "content":  content,
            "template": template,
        }
        if self._topic:
            payload["topic"] = self._topic

        def _do_post():
            data = _json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self.API_URL,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=self.TIMEOUT) as resp:
                    return _json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                return {"code": e.code, "msg": str(e)}
            except Exception as e:
                return {"code": -1, "msg": str(e)}

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _do_post)

    # ── 公共发送接口 ──────────────────────────────────────

    async def send(self, title: str, content: str, template: str = "html",
                   ignore_trading_day: bool = False) -> dict:
        """发送微信消息（自动处理非交易日静音）"""
        if self._mute_non_trading and not ignore_trading_day and not self._is_trading_day():
            logger.info(f"[微信] 非交易日静音，跳过推送: {title}")
            return {"code": 0, "msg": "非交易日静音"}

        result = await self._post(title, content, template)
        if result.get("code") == 200:
            logger.info(f"[微信] 推送成功: {title}")
        else:
            logger.warning(f"[微信] 推送失败: {result.get('msg', '未知错误')}")
        return result

    async def test_connection(self) -> dict:
        """连通性测试"""
        t0 = time.time()
        result = await self.send(
            title="✅ QuantSystem 微信推送测试",
            content="<p>微信推送配置成功！</p><p>你将在每个交易日收到<b>主力拉升选股</b>推送。</p>",
            ignore_trading_day=True,
        )
        latency = int((time.time() - t0) * 1000)
        ok = result.get("code") == 200
        return {"ok": ok, "msg": result.get("msg", ""), "latency_ms": latency}

    # ── 主力拉升选股报告 ──────────────────────────────────

    async def send_major_capital_scan(
        self,
        stocks: list[dict],
        scan_time: str = "",
        trigger: str = "定时",
    ) -> dict:
        """
        发送主力拉升选股结果到微信。
        stocks: [{"code","name","price","cap_yi","amount_wan","pe","pct_change",...}, ...]
        """
        scan_time = scan_time or datetime.now().strftime("%Y-%m-%d %H:%M")
        count = len(stocks)

        if count == 0:
            title = f"📊 主力拉升选股 | {scan_time[:10]} | 暂无信号"
            content = "<p>今日主力拉升策略未发现符合条件的标的，请明日继续关注。</p>"
            return await self.send(title, content)

        title = f"📈 主力拉升选股 | {scan_time[:10]} | {count}只标的"

        # 构建 HTML 内容
        rows = ""
        for i, s in enumerate(stocks[:20], 1):
            pct = s.get("pct_change")
            pct_str = f"+{pct:.2f}%" if pct and pct >= 0 else (f"{pct:.2f}%" if pct else "—")
            pct_color = "#e74c3c" if pct and pct >= 0 else "#27ae60"  # A股涨红跌绿

            signal = s.get("signal_reason", "")
            rows += f"""
            <tr style="border-bottom:1px solid #f0f0f0;">
              <td style="padding:8px 6px;color:#666;">{i}</td>
              <td style="padding:8px 6px;font-weight:bold;">{s.get('name','')}</td>
              <td style="padding:8px 6px;color:#888;font-size:12px;">{s.get('code','')}</td>
              <td style="padding:8px 6px;font-weight:bold;color:{pct_color};">{pct_str}</td>
              <td style="padding:8px 6px;">¥{s.get('price',0):.2f}</td>
              <td style="padding:8px 6px;color:#888;">{s.get('cap_yi',0):.0f}亿</td>
              <td style="padding:8px 6px;color:#555;font-size:11px;">{signal[:30] if signal else '量价放大+MACD共振'}</td>
            </tr>"""

        more_note = f"<p style='color:#888;font-size:12px;'>仅显示前20只，共筛出 {count} 只标的</p>" if count > 20 else ""

        content = f"""
<div style="font-family:Arial,sans-serif;max-width:600px;">
  <div style="background:linear-gradient(135deg,#1a7a4a,#27ae60);padding:16px;border-radius:8px 8px 0 0;">
    <h2 style="margin:0;color:#fff;font-size:18px;">📈 主力拉升选股报告</h2>
    <p style="margin:4px 0 0;color:#a8e6c0;font-size:13px;">{trigger}扫描 · {scan_time} · 共 {count} 只</p>
  </div>
  <div style="background:#fff;padding:0;border-radius:0 0 8px 8px;box-shadow:0 2px 8px rgba(0,0,0,0.1);">
    <table style="width:100%;border-collapse:collapse;">
      <thead>
        <tr style="background:#f8f9fa;">
          <th style="padding:8px 6px;text-align:left;color:#555;font-size:12px;">#</th>
          <th style="padding:8px 6px;text-align:left;color:#555;font-size:12px;">名称</th>
          <th style="padding:8px 6px;text-align:left;color:#555;font-size:12px;">代码</th>
          <th style="padding:8px 6px;text-align:left;color:#555;font-size:12px;">涨幅</th>
          <th style="padding:8px 6px;text-align:left;color:#555;font-size:12px;">价格</th>
          <th style="padding:8px 6px;text-align:left;color:#555;font-size:12px;">市值</th>
          <th style="padding:8px 6px;text-align:left;color:#555;font-size:12px;">信号</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
    {more_note}
    <div style="padding:12px 16px;background:#f8fff8;border-top:1px solid #e8f5e9;">
      <p style="margin:0;color:#888;font-size:11px;">
        ⚠️ 选股结果仅供参考，不构成投资建议。入场请结合自身风控，注意仓位管理。<br>
        策略：中小盘活跃股 · 量比≥1.5x · 涨幅≥3% · MA20上方 · MACD多头 · RSI 50~70
      </p>
    </div>
  </div>
</div>"""

        return await self.send(title, content, template="html")

    async def send_scan_error(self, error_msg: str) -> dict:
        """推送扫描异常告警"""
        title = f"⚠️ 选股任务异常 | {datetime.now().strftime('%H:%M')}"
        content = f"<p>主力拉升定时选股任务发生异常：</p><pre>{error_msg[:500]}</pre>"
        return await self.send(title, content, ignore_trading_day=True)


# 全局单例
wechat = WeChatPushPlus()
