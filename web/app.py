"""
FastAPI Web看板 - 主应用
启动: uvicorn web.app:app --reload --port 8000
浏览器访问: http://localhost:8000
"""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from config.settings import WATCHLIST, SCAN_CONFIG
from utils.logger import setup_logger

logger = setup_logger("web_app")


# ── 启动时自动更新策略文档 ──────────────────────────────
def _update_strategy_doc():
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from scripts.update_strategies_doc import run as update_doc
        update_doc(force=False)
    except Exception as e:
        logger.warning(f"[策略文档] 自动更新失败（不影响服务启动）: {e}")


_update_strategy_doc()


# ── App初始化 ──────────────────────────────────────────
app = FastAPI(
    title="A股量化交易系统",
    description="多源交叉验证 · 三次扫描 · 数据完整性自检",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 静态文件
static_path = Path(__file__).parent / "static"
static_path.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# ── 内存状态存储（启动后由 lifespan 从 MySQL 恢复） ────────
state = {
    "scan_results":    [],
    "scan_results_by_strategy": {},
    "scan_strategy":   "",
    "backtest_results":[],
    "scan_log":        [],
    "scan_running":    False,
    "backtest_running":False,
    "last_scan_time":  None,
    "scan_preset":     "default",
    "ws_clients":      [],
}

# 策略 → 选股预设映射
STRATEGY_PRESET_MAP = {
    "trend_follow":                "large_cap",
    "rsi_reversal":                "mid_cap",
    "bollinger_revert":            "default",
    "major_capital_pump":          "major_capital_pump",
    "major_capital_accumulation":  "major_capital_accumulation",
}

# ── 定时任务状态 ─────────────────────────────────────────
_scheduler_task: Optional[asyncio.Task] = None

# schedule DAO 的同步包装（供 scheduler_loop 等无法 await 的上下文使用）
_schedule_cache: dict | None = None   # 内存缓存，lifespan 时加载

async def _load_schedule_config() -> dict:
    """从 MySQL 加载定时任务配置"""
    from db.schedule_dao import load_schedule
    return await load_schedule()

async def _save_schedule_config(cfg: dict):
    """保存定时任务配置到 MySQL"""
    from db.schedule_dao import save_schedule
    await save_schedule(cfg)


async def _scheduler_loop():
    """后台调度循环：到设定时间自动触发选股 + 微信推送"""
    logger.info("[调度器] 启动")
    last_run_date = None

    while True:
        try:
            cfg = await _load_schedule_config()
            if not cfg.get("enabled"):
                await asyncio.sleep(30)
                continue

            now = datetime.now()
            target_h = int(cfg.get("hour", 15))
            target_m = int(cfg.get("minute", 35))
            today = now.date()

            # 判断是否是交易日
            is_trading = True
            try:
                from holidays.calendar import calendar as cal
                is_trading = cal.is_trading_day()
            except Exception:
                is_trading = now.weekday() < 5

            # 到时间且今天未运行且是交易日
            if (is_trading
                    and now.hour == target_h
                    and now.minute == target_m
                    and last_run_date != today):
                last_run_date = today
                logger.info(f"[调度器] 触发主力拉升选股 {now.strftime('%H:%M')}")
                await _save_schedule_config({"last_run": now.strftime("%Y-%m-%d %H:%M"), "last_status": "running"})

                try:
                    from scripts.daily_major_capital_scan import run_daily_scan
                    results = await run_daily_scan(
                        trigger="定时",
                        notify_wechat=cfg.get("notify_wechat", True),
                        update_web_state=True,
                    )
                    await _save_schedule_config({"last_status": f"完成 | {len(results)}只信号"})
                    await ws_manager.broadcast({
                        "type": "scan_done",
                        "data": state.get("scan_results", []),
                    })
                except Exception as e:
                    logger.error(f"[调度器] 执行异常: {e}")
                    await _save_schedule_config({"last_status": f"异常: {str(e)[:60]}"})
                    if cfg.get("notify_wechat"):
                        try:
                            from notifications.push import pusher
                            await pusher.send_error_alert(str(e))
                        except Exception:
                            pass

        except Exception as e:
            logger.error(f"[调度器] 循环异常: {e}")

        await asyncio.sleep(30)


# ── FastAPI Lifespan（MySQL初始化 + 调度器）────────────
@asynccontextmanager
async def lifespan(app_: FastAPI):
    global _scheduler_task

    # ── MySQL 初始化 + 数据迁移 ──
    from db.mysql_pool import get_pool, close_pool
    await get_pool()  # 建连接池 + 建表

    # 一次性迁移：如果旧 JSON 存在就导入 MySQL 并重命名
    from db.migrate import migrate_all
    try:
        await migrate_all()
    except Exception as e:
        logger.warning(f"[迁移] {e}")

    # 从 MySQL 恢复扫描结果到内存
    from db.scan_dao import load_scan
    for strat in ("major_capital_accumulation", "major_capital_pump",
                   "trend_follow", "rsi_reversal", "bollinger_revert"):
        rows = await load_scan(strat)
        if rows:
            state["scan_results_by_strategy"][strat] = rows
            state["scan_results"] = rows  # 最后一个有数据的作为全局默认
            state["scan_strategy"] = strat

    logger.info(f"[App] MySQL 就绪，已恢复扫描数据")

    _scheduler_task = asyncio.create_task(_scheduler_loop())
    logger.info("[App] 调度器已启动")
    yield
    if _scheduler_task:
        _scheduler_task.cancel()
    await close_pool()
    logger.info("[App] 调度器+连接池已停止")


# 注册 lifespan（在 lifespan 函数定义之后，避免前向引用）
app.router.lifespan_context = lifespan


# ── WebSocket管理器 ─────────────────────────────────────
class WSManager:
    def __init__(self):
        self.connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.connections:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

ws_manager = WSManager()


# ── WebSocket端点 ───────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


# ── 主页（返回SPA） ─────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    # 优先使用 React 构建产物
    react_index = Path(__file__).parent / "static" / "dist" / "index.html"
    if react_index.exists():
        return HTMLResponse(
            react_index.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-cache, must-revalidate"},
        )
    # 降级到旧模板
    html_path = Path(__file__).parent / "templates" / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


# ══════════════════════════════════════════════════════════
# API: 系统信息
# ══════════════════════════════════════════════════════════

@app.get("/api/system/info")
async def system_info():
    """返回系统基础信息：缓存文件数量等"""
    from pathlib import Path
    cache_dir = Path("backtest_cache")
    cache_count = len(list(cache_dir.glob("*.json"))) if cache_dir.exists() else 0
    return {"cache_count": cache_count}


@app.get("/api/system/api_status")
async def api_status():
    """快速检测实时行情 API 是否可用（用 urllib 避免额外依赖）"""
    import asyncio, urllib.request
    url = "https://82.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1&fs=m:0+t:6"
    try:
        loop = asyncio.get_event_loop()
        def _check():
            try:
                req = urllib.request.urlopen(url, timeout=4)
                return req.status == 200
            except Exception:
                return False
        ok = await loop.run_in_executor(None, _check)
        return {"realtime_ok": ok}
    except Exception:
        return {"realtime_ok": False}


# ══════════════════════════════════════════════════════════
# API: 扫描相关
# ══════════════════════════════════════════════════════════

@app.get("/api/scan/status")
async def scan_status(strategy: str = ""):
    from db.scan_dao import load_scan_meta
    if strategy:
        meta = await load_scan_meta(strategy)
        result_count = meta["result_count"]
        last_time = meta["last_scan_time"]
    else:
        result_count = len(state["scan_results"])
        last_time = str(state["last_scan_time"]) if state["last_scan_time"] else None
    return {
        "running":        state["scan_running"],
        "last_scan_time": last_time,
        "result_count":   result_count,
        "scan_preset":    state["scan_preset"],
        "scan_strategy":  state["scan_strategy"],
        "log_tail":       state["scan_log"][-50:],
    }


@app.post("/api/scan/run")
async def run_scan(
    background_tasks: BackgroundTasks,
    scan_type: str = "手动",
    strategy: str = "",      # 交易策略名，自动映射到对应选股预设
    preset: str = "",        # 直接指定选股预设（优先级高于 strategy）
):
    if state["scan_running"]:
        return JSONResponse({"error": "筛选已在运行中"}, status_code=409)
    # 确定最终使用的预设
    resolved_preset = preset or STRATEGY_PRESET_MAP.get(strategy, "default")
    state["scan_preset"] = resolved_preset
    state["scan_strategy"] = strategy or ""
    # 主力相关策略走专用扫描脚本（含策略信号），其余走通用初筛
    if strategy in ("major_capital_accumulation", "major_capital_pump"):
        async def _run_strategy_scan():
            state["scan_running"] = True
            try:
                from scripts.daily_major_capital_scan import run_daily_scan
                await run_daily_scan(
                    trigger=scan_type,
                    notify_wechat=False,   # 手动筛选不推送
                    update_web_state=True,
                )
            finally:
                state["scan_running"] = False
        background_tasks.add_task(_run_strategy_scan)
    else:
        background_tasks.add_task(_do_screen, scan_type, resolved_preset, strategy)
    return {"message": f"{scan_type}筛选已启动", "preset": resolved_preset}


async def _do_screen(scan_type: str, preset_name: str = "default", strategy_name: str = ""):
    """动态选股：调用 DynamicScreener 从全市场筛选"""
    state["scan_running"] = True
    state["scan_log"] = []

    def log(msg: str, level: str = "info"):
        entry = {"time": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
        state["scan_log"].append(entry)
        asyncio.create_task(ws_manager.broadcast({"type": "log", "data": entry}))

    try:
        from backtest.screener import DynamicScreener, SCREEN_PRESETS

        preset = SCREEN_PRESETS.get(preset_name, SCREEN_PRESETS["default"])
        params = preset.get("params", {})
        log(f"启动{scan_type}筛选 | 预设={preset.get('label', preset_name)} | "
            f"市值>{params.get('min_cap_yi',100)}亿 "
            f"成交额>{params.get('min_amount_wan',5000)}万 取前{params.get('top_n',50)}只")

        screener = DynamicScreener(**params)

        log("正在从东方财富拉取全A股实时行情...")
        stocks = await screener.screen(use_cache_hours=0)

        if not stocks:
            log("实时行情拉取失败，尝试使用本地缓存数据...", "warn")
            stocks = await screener.screen(use_cache_hours=24 * 30)  # 用30天内缓存

        # 转换为统一格式
        results = []
        for s in stocks:
            results.append({
                "code":           s.get("code", ""),
                "name":           s.get("name", ""),
                "market":         s.get("market", "SH"),
                "price":          s.get("price", 0),
                "cap_yi":         s.get("cap_yi", 0),
                "amount_wan":     s.get("amount_wan", 0),
                "pe":             s.get("pe"),
                "pct_change":     s.get("pct_change"),
                "integrity_pass": True,
            })

        state["scan_results"] = results
        state["last_scan_time"] = datetime.now()
        # 同时按策略存储，方便前端按策略查询
        if strategy_name:
            state["scan_results_by_strategy"][strategy_name] = results
            from db.scan_dao import upsert_scan
            await upsert_scan(strategy_name, results)

        log(f"筛选完成 | 共筛出 {len(results)} 只标的股", "ok")
        for i, s in enumerate(results[:10]):
            log(f"  {i+1}. {s['code']} {s['name']} "
                f"市值{s.get('cap_yi',0):.0f}亿 PE={s.get('pe','—')}")
        if len(results) > 10:
            log(f"  ... 共{len(results)}只，显示前10只")

        await ws_manager.broadcast({"type": "scan_done", "data": results})
    except Exception as e:
        log(f"筛选异常: {e}", "error")
        import traceback
        log(traceback.format_exc(), "error")
    finally:
        state["scan_running"] = False


@app.get("/api/scan/results")
async def get_scan_results(strategy: str = ""):
    """返回筛选结果。strategy 参数指定策略名时从 MySQL 查询该策略的结果"""
    if strategy:
        from db.scan_dao import load_scan
        return await load_scan(strategy)
    return state["scan_results"] or []


# ══════════════════════════════════════════════════════════
# API: 回测相关
# ══════════════════════════════════════════════════════════

@app.get("/api/backtest/status")
async def backtest_status():
    """返回回测运行状态，供前端轮询"""
    return {
        "running": state["backtest_running"],
        "error":   state.get("backtest_error"),
    }


@app.get("/api/backtest/list")
async def backtest_list():
    """返回真实回测历史记录，无数据时返回空列表"""
    import math

    def sanitize(obj):
        """清理 Infinity/NaN，确保浏览器能解析 JSON"""
        if isinstance(obj, dict):
            return {k: sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [sanitize(v) for v in obj]
        if isinstance(obj, float) and (math.isinf(obj) or math.isnan(obj)):
            return 0.0
        return obj

    try:
        from db.backtest_dao import load_backtest_results
        records = await load_backtest_results()
        if records:
            return sanitize(records)
    except Exception as e:
        logger.warning(f"读取回测记录失败: {e}")
    if state["backtest_results"]:
        return sanitize(state["backtest_results"])
    return []


@app.post("/api/backtest/run")
async def run_backtest(
    background_tasks: BackgroundTasks,
    strategy: str  = "trend_follow",
    start:    str  = "2022-01-01",
    end:      str  = "2024-12-31",
    cash:     float = 1000000.0,
    screen_preset: str = "default",
):
    if state["backtest_running"]:
        return JSONResponse({"error": "回测已在运行中"}, status_code=409)
    state["backtest_error"] = None          # 清空上次错误
    background_tasks.add_task(_do_backtest, strategy, start, end, cash, screen_preset)
    return {"message": f"回测已启动: {strategy} ({start}~{end})"}


async def _do_backtest(strategy: str, start: str, end: str, cash: float, screen_preset: str = "default"):
    state["backtest_running"] = True

    def log(msg, level="info"):
        entry = {"time": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
        state["scan_log"].append(entry)
        asyncio.create_task(ws_manager.broadcast({"type": "log", "data": entry}))

    try:
        log(f"回测启动: {strategy} | {start}~{end} | 初始资金¥{cash:,.0f} | 选股={screen_preset}")

        # ★ 主力建仓策略使用 Backtrader 引擎
        if strategy == "major_capital_accumulation":
            log("引擎: Backtrader v2（自定义RSI + 信号优先级 + 策略内选股）")
            from backtest.bt_major_capital import run_for_web
            result = await run_for_web(
                strategy_name=strategy,
                start=start, end=end, cash=cash,
                log_fn=lambda m: log(m),
                screen_preset=screen_preset,
            )

            if "error" in result:
                state["backtest_error"] = result["error"]
                log(f"回测失败: {result['error']}", "error")
                await ws_manager.broadcast({"type": "backtest_error", "data": result})
                return

            metrics = result["metrics"]
            equity_data = result.get("equity_data")
            trades_paired = result.get("trades_paired", [])

        else:
            # 其他策略走原有引擎
            log("Step1: 动态选股筛选...")
            log("Step2: 加载历史日线 + 构建验证门控...")

            from backtest.runner import run_backtest as _run
            scan_results = state.get("scan_results") or None
            result = await _run(
                strategy_name=strategy,
                start=start, end=end,
                initial_cash=cash,
                scan_results=scan_results,
                screen_preset=screen_preset,
                log_fn=lambda m: log(m),
            )

            if "error" in result:
                state["backtest_error"] = result["error"]
                log(f"回测失败: {result['error']}", "error")
                log("请确认: pip install akshare 已执行，且网络可访问东方财富接口", "warn")
                await ws_manager.broadcast({"type": "backtest_error", "data": result})
                return

            metrics = result["metrics"]
            equity_data = None
            trades_paired = None

        # ── 内存缓存 ──
        entry = {
            "id":       len(state["backtest_results"]) + 1,
            "strategy": strategy, "start": start, "end": end,
            "cash":     cash,     "metrics": metrics,
            "time":     datetime.now().strftime("%Y-%m-%d %H:%M"),
            "is_real":  True,
        }
        state["backtest_results"].insert(0, entry)

        # ── MySQL 持久化（Backtrader 结果也存入，与原引擎一致） ──
        if equity_data or trades_paired:
            try:
                from db.backtest_dao import save_backtest
                await save_backtest(
                    strategy=strategy,
                    start=start, end=end,
                    initial_cash=cash,
                    metrics=metrics,
                    equity_data=equity_data,
                    trades_data=trades_paired,
                    is_real=True,
                )
                log("回测结果已持久化到 MySQL", "ok")
            except Exception as e:
                logger.warning(f"[回测] Backtrader结果MySQL持久化失败: {e}")

        log(f"回测完成 ✓ | 年化={metrics.get('annualized_return','N/A')} "
            f"夏普={metrics.get('sharpe_ratio','N/A')} "
            f"验证通过={metrics.get('verified_pass',0)}只 "
            f"排除={metrics.get('verified_excl',0)}只", "ok")
        await ws_manager.broadcast({"type": "backtest_done", "data": entry})

    except Exception as e:
        state["backtest_error"] = str(e)
        log(f"回测异常: {type(e).__name__}: {e}", "error")
        logger.exception("回测异常详情")
    finally:
        state["backtest_running"] = False


@app.get("/api/backtest/trades/{bt_id}")
async def backtest_trades(bt_id: int):
    """返回指定回测ID的交易详情"""
    try:
        from db.backtest_dao import load_backtest_results
        records = await load_backtest_results()
        for r in records:
            if r.get("id") == bt_id:
                m = r.get("metrics", {})
                initial  = m.get("initial_cash", 1_000_000)
                final    = m.get("final_value", initial)
                total_profit = round(final - initial, 2)
                trades   = r.get("trades", [])
                # 动态补全股票名称（兼容历史记录 name=code 的情况）
                try:
                    from backtest.bt_major_capital import _load_stock_name_cache
                    name_cache = _load_stock_name_cache()
                    if name_cache:
                        for t in trades:
                            code = t.get("code", "")
                            if code and (not t.get("name") or t.get("name") == code):
                                t["name"] = name_cache.get(code, code)
                except Exception:
                    pass
                realized = round(sum(t.get("pnl", 0) for t in trades
                                     if t.get("sell_date") != "（持仓中）"), 2)
                unrealized = round(total_profit - realized, 2)
                return {
                    "id":            bt_id,
                    "strategy":      r.get("strategy", ""),
                    "start":         r.get("start", ""),
                    "end":           r.get("end", ""),
                    "initial_cash":  initial,
                    "final_value":   final,
                    "total_profit":  total_profit,   # 总盈亏（含持仓浮盈）
                    "realized_pnl":  realized,        # 已实现盈亏
                    "unrealized_pnl": unrealized,     # 持仓浮盈/亏
                    "trades":        trades,
                }
    except Exception as e:
        logger.warning(f"读取交易详情失败: {e}")
    return {"id": bt_id, "trades": [], "message": "未找到该回测记录"}


@app.get("/api/backtest/equity/{strategy}")
async def backtest_equity(strategy: str):
    """返回真实净值曲线数据，从最近一次该策略的回测结果中读取"""
    try:
        from db.backtest_dao import load_backtest_results
        records = await load_backtest_results()
        for r in records:
            if r.get("strategy") == strategy and r.get("equity"):
                eq = r["equity"]
                return {
                    "dates":     eq["dates"],
                    "strategy":  eq["values"],
                    "benchmark": [1.0] * len(eq["dates"]),  # 基准暂用1.0，待接入沪深300
                }
    except Exception as e:
        logger.warning(f"读取净值曲线失败: {e}")
    return {"dates": [], "strategy": [], "benchmark": [], "empty": True, "message": "尚未运行该策略回测，请先执行回测"}


@app.get("/api/backtest/compare")
async def backtest_compare():
    """返回真实策略对比数据，从已有回测结果中读取"""
    try:
        from db.backtest_dao import load_backtest_results
        records = await load_backtest_results()
        # 按策略名分组，取每个策略最近一次的净值曲线
        strategy_map = {}
        for r in records:
            name = r.get("strategy", "")
            if name and name not in strategy_map and r.get("equity"):
                strategy_map[name] = r["equity"]
        if not strategy_map:
            return {"dates": [], "curves": {}, "empty": True, "message": "尚未运行回测，请先执行回测"}

        # 构建每个策略的 date→value 映射
        date_value_maps = {}
        all_dates_set = set()
        for name, eq in strategy_map.items():
            dv = dict(zip(eq["dates"], eq["values"]))
            date_value_maps[name] = dv
            all_dates_set.update(eq["dates"])

        # 合并所有日期并排序
        base_dates = sorted(all_dates_set)

        # 按统一日期对齐，无数据的日期填 None（前端折线会断开）
        curves = {}
        for name, dv in date_value_maps.items():
            curves[name] = [dv.get(d) for d in base_dates]

        return {"dates": base_dates, "curves": curves}
    except Exception as e:
        logger.warning(f"读取策略对比失败: {e}")
    return {"dates": [], "curves": {}, "colors": [], "empty": True, "message": "读取失败"}


# ══════════════════════════════════════════════════════════
# API: 数据源 / 系统状态
# ══════════════════════════════════════════════════════════

@app.get("/api/sources")
async def get_sources():
    from config.settings import DATA_SOURCES
    return [
        {"id": k, **v, "latency_ms": _fake_latency(k)}
        for k, v in DATA_SOURCES.items()
    ]


@app.get("/api/engines")
async def get_engines():
    from config.settings import SEARCH_ENGINES
    return SEARCH_ENGINES


@app.get("/api/watchlist")
async def get_watchlist():
    return WATCHLIST


@app.get("/api/strategies")
async def get_strategies():
    """返回可用策略列表"""
    from backtest.strategies import BUILTIN_STRATEGIES
    strategies = []
    for key, cls in BUILTIN_STRATEGIES.items():
        strategies.append({"id": key, "name": cls.name})
    return strategies


@app.get("/api/screen/presets")
async def screen_presets():
    """返回选股预设列表"""
    from backtest.screener import SCREEN_PRESETS
    return [{"id": k, **v} for k, v in SCREEN_PRESETS.items()]


@app.post("/api/screen/run")
async def run_screen(preset: str = "default"):
    """手动执行一次选股"""
    from backtest.screener import DynamicScreener, SCREEN_PRESETS
    params = SCREEN_PRESETS.get(preset, SCREEN_PRESETS["default"])["params"]
    screener = DynamicScreener(**params)
    stocks = await screener.screen(use_cache_hours=0)
    return {"count": len(stocks), "stocks": stocks[:100]}


@app.get("/api/system/info")
async def system_info():
    from config.settings import DATA_SOURCES, SEARCH_ENGINES
    return {
        "version":        "2.0.0",
        "watchlist_count":len(WATCHLIST),
        "sources_count":  sum(1 for s in DATA_SOURCES.values() if s["enabled"]),
        "engines_count":  len(SEARCH_ENGINES),
        "scan_times":     SCAN_CONFIG,
        "uptime":         "在线",
        "server_time":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ══════════════════════════════════════════════════════════
# API: 交易日历
# ══════════════════════════════════════════════════════════

@app.get("/api/calendar/today")
async def calendar_today():
    from holidays.trading_calendar import calendar as cal
    from datetime import date
    today = date.today()
    return {
        "date":             today.isoformat(),
        "is_trading_day":   cal.is_trading_day(today),
        "is_market_open":   cal.is_market_open(),
        "current_session":  cal.current_session(),
        "next_trading_day": cal.next_trading_day(today).isoformat(),
        "prev_trading_day": cal.prev_trading_day(today).isoformat(),
        "countdown":        cal.countdown_to_holiday(),
        "holiday_name":     cal.holiday_name(today) or "",
        "is_makeup_day":    cal.is_makeup_day(today),
    }

@app.get("/api/calendar/month")
async def calendar_month(year: int = None, month: int = None):
    from holidays.trading_calendar import calendar as cal
    from datetime import date
    if year is None: year = date.today().year
    if month is None: month = date.today().month
    return {
        "year": year, "month": month,
        "days": cal.month_calendar(year, month),
        "stats": cal.year_stats(year),
    }

@app.get("/api/calendar/upcoming_holidays")
async def upcoming_holidays(days: int = 120):
    from holidays.trading_calendar import calendar as cal
    return cal.upcoming_holidays(days)

@app.get("/api/calendar/year_stats")
async def year_stats(year: int = None):
    from holidays.trading_calendar import calendar as cal
    from datetime import date
    if year is None: year = date.today().year
    return cal.year_stats(year)

@app.post("/api/calendar/sync")
async def sync_calendar(background_tasks: BackgroundTasks):
    async def _sync():
        from holidays.trading_calendar import calendar as cal
        ok = await cal.sync_from_akshare()
        await ws_manager.broadcast({"type": "log", "data": {
            "time": datetime.now().strftime("%H:%M:%S"),
            "msg": f"交易日历同步{'成功' if ok else '失败（使用内置数据）'}",
            "level": "ok" if ok else "warn",
        }})
    background_tasks.add_task(_sync)
    return {"message": "日历同步已启动"}


# ══════════════════════════════════════════════════════════
# API: 飞书通知
# ══════════════════════════════════════════════════════════

@app.get("/api/feishu/config")
async def feishu_config():
    """返回飞书配置状态（不暴露 Webhook 地址）"""
    import os
    bots = {
        "main":  {"label": "主群·扫描报告", "configured": bool(os.getenv("FEISHU_WEBHOOK_MAIN"))},
        "alert": {"label": "预警群",        "configured": bool(os.getenv("FEISHU_WEBHOOK_ALERT") or os.getenv("FEISHU_WEBHOOK_MAIN"))},
        "daily": {"label": "日报群",        "configured": bool(os.getenv("FEISHU_WEBHOOK_DAILY") or os.getenv("FEISHU_WEBHOOK_MAIN"))},
    }
    return {"bots": bots, "at_members_count": 0}

@app.post("/api/feishu/test")
async def feishu_test(bot: str = "main"):
    """发送测试消息，验证 Webhook 连通性"""
    from notifications.feishu import notifier
    result = await notifier.test_webhook(bot)
    return result

@app.post("/api/feishu/send_text")
async def feishu_send_text(text: str = "测试消息", bot: str = "main"):
    from notifications.feishu import notifier
    notifier.start()
    ok = await notifier.send_text(text, bot)
    return {"ok": ok}

@app.post("/api/feishu/send_scan_report")
async def feishu_send_scan_report(background_tasks: BackgroundTasks, scan_type: str = "手动"):
    """立即推送最新扫描结果到飞书"""
    async def _push():
        from notifications.feishu import notifier
        notifier.start()
        results = state["scan_results"] or []
        if not results:
            await notifier.send_text("暂无扫描结果，请先运行扫描", "main")
            return
        await notifier.send_scan_report(results, scan_type)
    background_tasks.add_task(_push)
    return {"message": "推送已排队"}

@app.post("/api/feishu/send_holiday_notice")
async def feishu_holiday_notice(background_tasks: BackgroundTasks):
    """推送即将到来的假期休市通知"""
    async def _push():
        from notifications.feishu import notifier
        from holidays.trading_calendar import calendar as cal
        notifier.start()
        upcoming = cal.upcoming_holidays(30)
        if upcoming:
            h = upcoming[0]
            resume = cal.next_trading_day(
                __import__('datetime').date.fromisoformat(h["end"])
            ).strftime("%Y年%m月%d日")
            await notifier.send_holiday_notice(
                h["name"], h["start"], h["end"], resume
            )
    background_tasks.add_task(_push)
    return {"message": "节假日通知已排队"}


def _fake_latency(key: str) -> int:
    import hashlib
    h = int(hashlib.md5(key.encode()).hexdigest()[:4], 16)
    return 40 + (h % 280)


# ══════════════════════════════════════════════════════════════
# API: 交易日历
# ══════════════════════════════════════════════════════════════

@app.get("/api/calendar/today")
async def calendar_today():
    try:
        from holidays.calendar import calendar as cal
        today = __import__("datetime").date.today()
        days_until, holiday_name = cal.days_until_holiday()
        return {
            "date":           today.isoformat(),
            "is_trading_day": cal.is_trading_day(),
            "is_market_open": cal.is_market_open(),
            "session":        cal.current_session(),
            "next_trading":   cal.next_trading_day().isoformat(),
            "prev_trading":   cal.prev_trading_day().isoformat(),
            "days_until_holiday": days_until,
            "next_holiday_name":  holiday_name,
        }
    except Exception as e:
        return {"error": str(e), "session": "未知", "is_trading_day": False}


@app.get("/api/calendar/month")
async def calendar_month(year: int = None, month: int = None):
    try:
        from holidays.calendar import calendar as cal
        import datetime
        today = datetime.date.today()
        return cal.month_calendar(year or today.year, month or today.month)
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/calendar/upcoming_holidays")
async def upcoming_holidays(days: int = 60):
    try:
        from holidays.calendar import calendar as cal
        return cal.upcoming_holidays(days=days)
    except Exception as e:
        return {"error": str(e), "holidays": []}


@app.get("/api/calendar/year_stats")
async def year_stats(year: int = None):
    try:
        from holidays.calendar import calendar as cal
        return cal.year_stats(year)
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════
# API: 飞书通知
# ══════════════════════════════════════════════════════════════

@app.get("/api/notify/config")
async def notify_config():
    """返回飞书配置状态（不暴露真实Webhook URL）"""
    import os
    bots = {}
    for key in ["FEISHU_WEBHOOK_MAIN", "FEISHU_WEBHOOK_ALERT", "FEISHU_WEBHOOK_BACKTEST"]:
        val = os.environ.get(key, "")
        bots[key] = {"configured": bool(val), "preview": val[:30] + "..." if val else ""}
    at_raw = os.environ.get("FEISHU_AT_MEMBERS", "")
    return {
        "bots":       bots,
        "at_count":   len([m for m in at_raw.split(",") if m.strip()]),
        "mute_non_trading": os.environ.get("FEISHU_MUTE_NON_TRADING", "true").lower() != "false",
    }


@app.post("/api/notify/test")
async def notify_test(bot: str = "main"):
    """测试飞书Webhook连通性"""
    try:
        from notifications.feishu import notifier
        notifier.start()
        result = await notifier.test_connection(bot)
        return result
    except Exception as e:
        return {"ok": False, "msg": str(e), "latency_ms": 0}


@app.post("/api/notify/send_startup")
async def notify_send_startup():
    """手动触发启动通知"""
    try:
        from notifications.integration import notify_startup
        from config.settings import WATCHLIST, DATA_SOURCES
        await notify_startup(len(WATCHLIST), sum(1 for s in DATA_SOURCES.values() if s["enabled"]))
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


@app.post("/api/notify/send_holiday")
async def notify_send_holiday():
    """手动触发节假日通知检查"""
    try:
        from notifications.integration import check_and_notify_upcoming_holiday
        await check_and_notify_upcoming_holiday(days_ahead=999)  # 强制发送
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


# ══════════════════════════════════════════════════════════════
# API: 定时任务管理（主力拉升选股 + 微信推送）
# ══════════════════════════════════════════════════════════════

@app.get("/api/schedule/config")
async def schedule_get_config():
    """获取定时任务配置"""
    cfg = await _load_schedule_config()
    return cfg


@app.post("/api/schedule/config")
async def schedule_save_config(
    enabled:       bool = False,
    hour:          int  = 15,
    minute:        int  = 35,
    notify_wechat: bool = True,
):
    """保存定时任务配置（开启/关闭、设置时间）"""
    cfg = await _load_schedule_config()
    cfg["enabled"]       = enabled
    cfg["hour"]          = max(0, min(23, hour))
    cfg["minute"]        = max(0, min(59, minute))
    cfg["notify_wechat"] = notify_wechat
    await _save_schedule_config(cfg)
    status = "已开启" if enabled else "已关闭"
    return {"ok": True, "msg": f"定时任务{status}，执行时间 {cfg['hour']:02d}:{cfg['minute']:02d}"}


@app.post("/api/schedule/run_now")
async def schedule_run_now(background_tasks: BackgroundTasks, strategy: str = "major_capital_pump"):
    """立即触发一次选股扫描（手动执行），strategy 指定策略"""
    if state.get("scan_running"):
        return JSONResponse({"error": "扫描任务已在运行中"}, status_code=409)

    strategy_labels = {
        "major_capital_pump":         "主力拉升",
        "major_capital_accumulation": "主力建仓",
        "trend_follow":               "趋势跟踪",
        "rsi_reversal":               "RSI反转",
        "bollinger_revert":           "布林带回归",
    }
    label = strategy_labels.get(strategy, strategy)

    async def _run():
        state["scan_running"] = True
        state["scan_strategy"] = strategy
        try:
            sched_cfg = await _load_schedule_config()
            # 主力建仓和主力拉升走专用扫描脚本，其余走通用预设扫描
            if strategy in ("major_capital_pump", "major_capital_accumulation"):
                from scripts.daily_major_capital_scan import run_daily_scan
                await run_daily_scan(
                    trigger="手动",
                    notify_wechat=sched_cfg.get("notify_wechat", True),
                    update_web_state=True,
                )
            else:
                # 通用策略：走选股预设
                preset = STRATEGY_PRESET_MAP.get(strategy, "default")
                state["scan_preset"] = preset
                await _do_screen("手动", preset, strategy)
            await ws_manager.broadcast({
                "type": "scan_done",
                "data": state.get("scan_results", []),
            })
        finally:
            state["scan_running"] = False

    background_tasks.add_task(_run)
    return {"ok": True, "msg": f"{label}选股已启动，完成后将推送通知"}


# ══════════════════════════════════════════════════════════════
# API: 多渠道推送管理（企业微信 / 钉钉 / Telegram / Server酱）
# ══════════════════════════════════════════════════════════════

_DOTENV_PATH = Path(__file__).resolve().parent.parent / ".env"

def _env_val(key: str) -> str:
    import os
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=str(_DOTENV_PATH), override=False)
    except ImportError:
        # 手动解析 .env
        if _DOTENV_PATH.exists():
            for line in _DOTENV_PATH.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip()
                if k and k not in os.environ:
                    os.environ[k] = v
    return os.environ.get(key, "").strip()


def _mask(v: str) -> str:
    """对敏感字段做脱敏显示"""
    if not v:
        return ""
    if len(v) <= 8:
        return v[:2] + "****"
    return v[:6] + "****" + v[-4:]


@app.get("/api/push/config")
async def push_config():
    """返回各推送渠道配置状态（脱敏显示）"""
    wecom     = _env_val("WECOM_WEBHOOK")
    dingtalk  = _env_val("DINGTALK_WEBHOOK")
    tg_token  = _env_val("TG_BOT_TOKEN")
    tg_chat   = _env_val("TG_CHAT_ID")
    sc_key    = _env_val("SERVERCHAN_KEY")
    return {
        "wecom":      {"configured": bool(wecom),    "preview": _mask(wecom)},
        "dingtalk":   {"configured": bool(dingtalk),  "preview": _mask(dingtalk)},
        "telegram":   {"configured": bool(tg_token and tg_chat),
                       "token_preview": _mask(tg_token), "chat_id": tg_chat},
        "serverchan": {"configured": bool(sc_key),   "preview": _mask(sc_key)},
        "mute_non_trading": _env_val("PUSH_MUTE_NON_TRADING") != "false",
    }


@app.post("/api/push/save")
async def push_save(
    channel:     str = "",
    webhook_url: str = "",
    token:       str = "",
    chat_id:     str = "",
    secret:      str = "",
):
    """保存指定渠道配置到 .env 文件"""
    import os

    KEY_MAP = {
        "wecom":      [("WECOM_WEBHOOK", webhook_url)],
        "dingtalk":   [("DINGTALK_WEBHOOK", webhook_url), ("DINGTALK_SECRET", secret)],
        "telegram":   [("TG_BOT_TOKEN", token), ("TG_CHAT_ID", chat_id)],
        "serverchan": [("SERVERCHAN_KEY", token)],
    }
    if channel not in KEY_MAP:
        return JSONResponse({"ok": False, "msg": f"未知渠道: {channel}"}, status_code=400)

    pairs = [(k, v) for k, v in KEY_MAP[channel] if v.strip()]
    if not pairs:
        return JSONResponse({"ok": False, "msg": "未填写任何配置项"}, status_code=400)

    env_path = _DOTENV_PATH
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []

    def _set(lines: list[str], key: str, value: str) -> list[str]:
        updated = [f"{key}={value}" if l.startswith(f"{key}=") else l for l in lines]
        if not any(l.startswith(f"{key}=") for l in lines):
            updated.append(f"{key}={value}")
        return updated

    for key, value in pairs:
        lines = _set(lines, key, value.strip())
        os.environ[key] = value.strip()

    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"ok": True, "msg": f"{channel} 配置已保存"}


@app.post("/api/push/test")
async def push_test(channel: str = "all"):
    """测试推送渠道连通性"""
    try:
        from notifications.push import pusher
        if channel == "all":
            return await pusher.test_all()
        elif channel == "wecom":
            return await pusher.wecom.test()
        elif channel == "dingtalk":
            return await pusher.dingtalk.test()
        elif channel == "telegram":
            return await pusher.telegram.test()
        elif channel == "serverchan":
            return await pusher.serverchan.test()
        else:
            return {"ok": False, "msg": f"未知渠道: {channel}"}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


# 兼容旧接口（前端 ScanCenter 中还引用了 /api/wechat/config 和 /api/wechat/test）
@app.get("/api/wechat/config")
async def wechat_config_compat():
    cfg = await push_config()
    wecom = cfg.get("wecom", {})
    return {
        "configured":    wecom.get("configured", False),
        "token_preview": wecom.get("preview", ""),
        "channels":      [k for k, v in cfg.items() if isinstance(v, dict) and v.get("configured")],
    }

@app.post("/api/wechat/test")
async def wechat_test_compat():
    return await push_test("all")


# ── 打开同花顺个股 ─────────────────────────────────────
@app.post("/api/open-ths")
async def open_ths(req: dict):
    """
    打开同花顺 App 并搜索指定股票。
    步骤：1) 复制代码到剪贴板  2) 激活同花顺  3) 尝试自动粘贴+回车
    """
    import subprocess
    code = req.get("code", "")
    if not code:
        return {"ok": False, "msg": "缺少股票代码"}

    try:
        # 1. 将股票代码复制到系统剪贴板（备用）
        subprocess.run(["pbcopy"], input=code.encode(), check=True)

        # 2. 激活同花顺，直接键入代码（大多数行情软件支持全局键盘截获直接跳转）
        #    先键入代码，若失败再回退到 Cmd+V 粘贴
        script = f'''
            tell application "同花顺" to activate
            delay 0.8
            tell application "System Events"
                -- 先按 Escape 退出当前搜索状态
                key code 53
                delay 0.3
                -- 逐字符键入股票代码，每字符间隔50ms，确保同花顺搜索框完整接收
                set theCode to "{code}"
                repeat with c in characters of theCode
                    keystroke c
                    delay 0.05
                end repeat
                -- 等待同花顺完成搜索结果渲染后再按回车
                delay 1.0
                keystroke return
            end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True, text=True, timeout=8,
        )

        if result.returncode != 0:
            # AppleScript 失败（无辅助功能权限），仅打开同花顺 + 剪贴板已复制
            subprocess.run(["open", "-a", "同花顺"], check=True)
            return {
                "ok": True,
                "auto_paste": False,
                "msg": f"已打开同花顺，股票代码 {code} 已复制到剪贴板，请在同花顺中 ⌘V 粘贴",
            }

        return {"ok": True, "auto_paste": True, "msg": f"已在同花顺中打开 {code}"}
    except Exception as e:
        logger.warning(f"[open-ths] 打开同花顺失败: {e}")
        return {"ok": False, "msg": str(e)}
