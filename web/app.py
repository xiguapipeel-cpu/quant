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
    # ── 参数热力图扫描（独立于 backtest_running，不阻塞主流程）──
    "sweep_running":   False,
    "sweep_key":       None,   # "{strategy}:{start}:{end}"
    "sweep_result":    None,   # 完成后存 {p1, p2, matrix}
    "sweep_error":     None,
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
    data_source:   str = "cache",   # "cache" | "local_db"
):
    if state["backtest_running"]:
        return JSONResponse({"error": "回测已在运行中"}, status_code=409)
    state["backtest_error"] = None          # 清空上次错误
    background_tasks.add_task(_do_backtest, strategy, start, end, cash, screen_preset, data_source)
    return {"message": f"回测已启动: {strategy} ({start}~{end}) 数据源={data_source}"}


async def _do_backtest(strategy: str, start: str, end: str, cash: float, screen_preset: str = "default", data_source: str = "cache"):
    state["backtest_running"] = True

    def log(msg, level="info"):
        entry = {"time": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
        state["scan_log"].append(entry)
        asyncio.create_task(ws_manager.broadcast({"type": "log", "data": entry}))

    try:
        src_label = "本地数据库" if data_source == "local_db" else "缓存数据"
        log(f"回测启动: {strategy} | {start}~{end} | 初始资金¥{cash:,.0f} | 数据源={src_label}")

        # 所有策略统一使用 Backtrader 引擎
        log(f"引擎: Backtrader | {src_label}")
        from backtest.bt_runner import run_for_web
        result = await run_for_web(
            strategy_name=strategy,
            start=start, end=end, cash=cash,
            log_fn=lambda m: log(m),
            screen_preset=screen_preset,
            data_source=data_source,
        )

        if "error" in result:
            state["backtest_error"] = result["error"]
            log(f"回测失败: {result['error']}", "error")
            await ws_manager.broadcast({"type": "backtest_error", "data": result})
            return

        metrics = result["metrics"]
        equity_data = result.get("equity_data")
        trades_paired = result.get("trades_paired", [])

        # ── 内存缓存 ──
        entry = {
            "id":          len(state["backtest_results"]) + 1,
            "strategy":    strategy, "start": start, "end": end,
            "cash":        cash,     "metrics": metrics,
            "time":        datetime.now().strftime("%Y-%m-%d %H:%M"),
            "is_real":     True,
            "data_source": data_source,
        }
        state["backtest_results"].insert(0, entry)

        # ── MySQL 持久化（所有策略均持久化） ──
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
                data_source=data_source,
            )
            log("回测结果已持久化到 MySQL", "ok")
        except Exception as e:
            logger.warning(f"[回测] MySQL持久化失败: {e}")

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


# ── 样本外测试（OOS）──────────────────────────────────────────────────────────

@app.post("/api/backtest/run_oos")
async def run_oos_backtest(
    background_tasks: BackgroundTasks,
    strategy:    str            = "trend_follow",
    train_start: Optional[str]  = None,
    train_end:   Optional[str]  = None,
    test_start:  str            = "2022-01-01",
    test_end:    str            = "2023-12-31",
    cash:        float          = 1000000.0,
    data_source: str            = "cache",
):
    if state["backtest_running"]:
        return JSONResponse({"error": "回测已在运行中"}, status_code=409)
    state["backtest_error"] = None
    background_tasks.add_task(
        _do_oos_backtest, strategy, train_start, train_end, test_start, test_end, cash, data_source
    )
    train_desc = f"{train_start}~{train_end}" if train_start else "无"
    return {"message": f"样本外测试已启动: {strategy} 训练={train_desc} 测试={test_start}~{test_end}"}


async def _do_oos_backtest(
    strategy: str,
    train_start: Optional[str], train_end: Optional[str],
    test_start: str,  test_end: str,
    cash: float,
    data_source: str,
):
    state["backtest_running"] = True

    def log(msg, level="info"):
        entry = {"time": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
        state["scan_log"].append(entry)
        asyncio.create_task(ws_manager.broadcast({"type": "log", "data": entry}))

    try:
        src_label = "本地数据库" if data_source == "local_db" else "缓存数据"
        has_train = bool(train_start and train_end)
        log(f"样本外测试启动: {strategy} | 数据源={src_label} | 训练集={'有' if has_train else '无'}")

        from backtest.bt_runner import run_for_web

        def _pct_val(s: str) -> float:
            """'+12.34%' → 12.34"""
            try:
                return float(s.replace('%', '').replace('+', ''))
            except Exception:
                return 0.0

        train_result = None
        train_m = {}
        train_ret = None

        # ── 训练集回测（可选）──
        if has_train:
            log(f"[训练集] 加载行情数据 {train_start} ~ {train_end} ...")
            train_result = await run_for_web(
                strategy_name=strategy,
                start=train_start, end=train_end, cash=cash,
                log_fn=lambda m: log(f"[训练] {m}"),
                data_source=data_source,
            )
            if "error" in train_result:
                state["backtest_error"] = f"训练集失败: {train_result['error']}"
                log(f"训练集回测失败: {train_result['error']}", "error")
                await ws_manager.broadcast({"type": "backtest_error", "data": train_result})
                return
            train_m = train_result["metrics"]
            train_ret = _pct_val(train_m.get("total_return", "0%"))

        # ── 测试集回测（用相同初始资金，独立计算）──
        log(f"[测试集] 加载行情数据 {test_start} ~ {test_end} ...")
        test_result = await run_for_web(
            strategy_name=strategy,
            start=test_start, end=test_end, cash=cash,
            log_fn=lambda m: log(f"[测试] {m}"),
            data_source=data_source,
        )
        if "error" in test_result:
            state["backtest_error"] = f"测试集失败: {test_result['error']}"
            log(f"测试集回测失败: {test_result['error']}", "error")
            await ws_manager.broadcast({"type": "backtest_error", "data": test_result})
            return

        test_m  = test_result["metrics"]
        test_ret = _pct_val(test_m.get("total_return", "0%"))

        # ── OOS 判定逻辑（多维过拟合评分）──
        # 评分维度（每项 0–20，总分 100）：
        #   1. 年化收益一致性（test_ann / train_ann）
        #   2. 夏普比率一致性（test_sharpe / train_sharpe）
        #   3. 最大回撤恶化程度（test_dd / train_dd，越大越差）
        #   4. 胜率漂移（|test_wr - train_wr|）
        #   5. 测试集交易频次（统计显著性）
        # 综合评分 + 硬否决条件共同得出 verdict
        if not has_train:
            verdict = "N/A"
            verdict_reason = f"未提供训练集，仅展示测试区间结果（{test_start}~{test_end}），无法判断过拟合"
            oof_score = None
            score_detail = None
        else:
            # 取指标（优先用年化收益，避免区间长度偏差）
            train_ann = _pct_val(train_m.get("annualized_return", "0%"))
            test_ann  = _pct_val(test_m.get("annualized_return",  "0%"))
            train_sh  = float(train_m.get("sharpe_ratio", 0) or 0)
            test_sh   = float(test_m.get("sharpe_ratio",  0) or 0)
            train_dd  = _pct_val(train_m.get("max_drawdown", "0%"))
            test_dd   = _pct_val(test_m.get("max_drawdown",  "0%"))
            train_wr  = _pct_val(train_m.get("win_rate", "0%"))
            test_wr   = _pct_val(test_m.get("win_rate",  "0%"))
            test_n    = int(test_m.get("total_trades", 0) or 0)

            def _band(ratio: float, bands: list) -> int:
                """根据区间返回分值，bands=[(阈值, 分值), ...] 从高到低"""
                for thr, pts in bands:
                    if ratio >= thr:
                        return pts
                return 0

            # 1. 年化收益一致性
            if train_ann > 0:
                ann_ratio = test_ann / train_ann
                ann_pts = _band(ann_ratio, [(0.7, 20), (0.4, 12), (0.1, 6), (0, 3)])
            else:
                ann_ratio = 0
                ann_pts = 0

            # 2. 夏普一致性
            if train_sh > 0:
                sh_ratio = test_sh / train_sh
                sh_pts = _band(sh_ratio, [(0.7, 20), (0.4, 12), (0.1, 6), (0, 3)])
            else:
                sh_ratio = 0
                sh_pts = 0

            # 3. 回撤恶化（反向评分：越小越好）
            if train_dd > 0:
                dd_ratio = test_dd / train_dd
                if   dd_ratio <= 1.2: dd_pts = 20
                elif dd_ratio <= 1.5: dd_pts = 12
                elif dd_ratio <= 2.0: dd_pts = 6
                else:                 dd_pts = 0
            else:
                dd_ratio = 1.0 if test_dd <= 5 else 2.0
                dd_pts = 20 if test_dd <= 5 else 6

            # 4. 胜率漂移
            wr_drift = abs(test_wr - train_wr)
            if   wr_drift <= 5:  wr_pts = 20
            elif wr_drift <= 10: wr_pts = 12
            elif wr_drift <= 20: wr_pts = 6
            else:                 wr_pts = 0

            # 5. 测试集交易频次（统计显著性）
            if   test_n >= 30: n_pts = 20
            elif test_n >= 15: n_pts = 12
            elif test_n >= 5:  n_pts = 6
            else:               n_pts = 0

            oof_score = ann_pts + sh_pts + dd_pts + wr_pts + n_pts
            score_detail = {
                "ann_pts":  ann_pts, "ann_ratio":  round(ann_ratio, 3),
                "sh_pts":   sh_pts,  "sh_ratio":   round(sh_ratio,  3),
                "dd_pts":   dd_pts,  "dd_ratio":   round(dd_ratio,  3),
                "wr_pts":   wr_pts,  "wr_drift":   round(wr_drift,  2),
                "n_pts":    n_pts,   "test_n":     test_n,
            }

            # ── 硬否决：训练亏损 / 测试大亏 / 测试样本过少 ──
            if train_ann <= 0:
                verdict = "FAIL"
                verdict_reason = f"训练集年化亏损({train_ann:+.2f}%)，策略本身存在问题，非过拟合"
            elif test_ann < -5:
                verdict = "FAIL"
                verdict_reason = f"训练年化{train_ann:+.2f}% 但测试年化{test_ann:+.2f}%，典型过拟合（评分{oof_score}/100）"
            elif test_n < 3:
                verdict = "WARN"
                verdict_reason = f"测试区间仅{test_n}笔交易，样本量不足以验证（评分{oof_score}/100）"
            elif oof_score >= 75:
                verdict = "PASS"
                verdict_reason = (f"综合评分{oof_score}/100 · 年化一致性{ann_ratio*100:.0f}% · "
                                  f"夏普一致性{sh_ratio*100:.0f}% · 回撤比{dd_ratio:.2f} · "
                                  f"胜率漂移{wr_drift:.1f}pp · {test_n}笔交易，策略鲁棒性良好")
            elif oof_score >= 50:
                verdict = "WARN"
                verdict_reason = (f"综合评分{oof_score}/100 · 年化一致性{ann_ratio*100:.0f}% · "
                                  f"回撤比{dd_ratio:.2f} · 胜率漂移{wr_drift:.1f}pp，存在一定衰减")
            else:
                verdict = "FAIL"
                verdict_reason = (f"综合评分{oof_score}/100 · 年化一致性{ann_ratio*100:.0f}% · "
                                  f"回撤比{dd_ratio:.2f}，过拟合风险高")

        oos_entry = {
            "id":          len(state["backtest_results"]) + 1,
            "strategy":    strategy,
            "oos":         True,
            "train_start": train_start, "train_end": train_end,
            "test_start":  test_start,  "test_end":  test_end,
            "cash":        cash,
            "time":        datetime.now().strftime("%Y-%m-%d %H:%M"),
            "data_source": data_source,
            "train_metrics":  train_m,
            "test_metrics":   test_m,
            "train_equity":   train_result.get("equity_data") if train_result else None,
            "test_equity":    test_result.get("equity_data"),
            "verdict":        verdict,
            "verdict_reason": verdict_reason,
            "train_ret":      train_ret,
            "test_ret":       test_ret,
            "oof_score":      oof_score,
            "score_detail":   score_detail,
        }
        state["backtest_results"].insert(0, oos_entry)

        # ── MySQL 持久化 ──
        try:
            from db.backtest_dao import save_backtest
            oos_metrics = {
                "oos":            True,
                "verdict":        verdict,
                "verdict_reason": verdict_reason,
                "train_start":    train_start,
                "train_end":      train_end,
                "test_start":     test_start,
                "test_end":       test_end,
                "train_ret":      train_ret,
                "test_ret":       test_ret,
                "train_metrics":  train_m,
                "test_metrics":   test_m,
                "initial_cash":   cash,
                "oof_score":      oof_score,
                "score_detail":   score_detail,
            }
            oos_equity = {
                "train": train_result.get("equity_data") if train_result else None,
                "test":  test_result.get("equity_data"),
            }
            saved_id = await save_backtest(
                strategy=f"oos:{strategy}",
                start=train_start or test_start, end=test_end,
                initial_cash=cash,
                metrics=oos_metrics,
                equity_data=oos_equity,
                trades_data=None,
                is_real=True,
                data_source=data_source,
            )
            oos_entry["id"] = saved_id
            log(f"样本外测试结果已持久化到 MySQL id={saved_id}", "ok")
        except Exception as e:
            logger.warning(f"[OOS] MySQL 持久化失败: {e}")

        train_desc = f"训练={train_ret:+.2f}% " if train_ret is not None else ""
        log(
            f"样本外测试完成 ✓ | {train_desc}测试={test_ret:+.2f}% 结论={verdict}",
            "ok" if verdict == "PASS" else ("warn" if verdict == "WARN" else "error"),
        )
        await ws_manager.broadcast({"type": "backtest_done", "data": oos_entry})

    except Exception as e:
        state["backtest_error"] = str(e)
        log(f"样本外测试异常: {type(e).__name__}: {e}", "error")
        logger.exception("OOS回测异常详情")
    finally:
        state["backtest_running"] = False


@app.get("/api/backtest/oos_result")
async def get_oos_result():
    """返回最近一次样本外测试结果（内存优先，内存无则从 DB 读取）"""
    # 先找内存
    for r in state["backtest_results"]:
        if r.get("oos"):
            return JSONResponse(sanitize(r))
    # 再从 DB 读
    try:
        from db.backtest_dao import load_backtest_results
        records = await load_backtest_results()
        for r in records:
            if r.get("oos"):
                return JSONResponse(sanitize(r))
    except Exception:
        pass
    return JSONResponse({"error": "暂无样本外测试记录"}, status_code=404)


# ── 参数热力图扫描（OOS）────────────────────────────────────────────────────────

# 每个策略的两个可调参数及候选值（5×4 ~ 5×5 网格）
PARAM_SWEEP_CONFIG: dict = {
    "trend_follow": {
        "p1": {"name": "fast",        "label": "快EMA周期",  "values": [8, 10, 12, 15, 20]},
        "p2": {"name": "slow",        "label": "慢EMA周期",  "values": [20, 25, 30, 35, 40]},
    },
    "rsi_reversal": {
        "p1": {"name": "entry_low",   "label": "RSI超卖阈值", "values": [20, 25, 30, 35, 40]},
        "p2": {"name": "take_profit", "label": "RSI止盈阈值", "values": [55, 60, 65, 70, 75]},
    },
    "bollinger_revert": {
        "p1": {"name": "period",      "label": "布林周期",    "values": [15, 20, 25, 30]},
        "p2": {"name": "num_std",     "label": "标准差倍数",  "values": [1.5, 2.0, 2.5, 3.0]},
    },
    "major_capital_pump": {
        "p1": {"name": "pct_entry",    "label": "入场涨幅%",  "values": [2.0, 3.0, 4.0, 5.0, 6.0]},
        "p2": {"name": "trailing_pct", "label": "追踪止损%",  "values": [0.08, 0.10, 0.12, 0.15]},
    },
    "major_capital_accumulation": {
        "p1": {"name": "min_watch_days", "label": "最少观察天",  "values": [10, 15, 20, 25]},
        "p2": {"name": "stop_loss_pct",  "label": "硬止损幅度%", "values": [0.06, 0.08, 0.10, 0.12]},
    },
}


async def _do_param_sweep(strategy: str, start: str, end: str,
                          cash: float, data_source: str, sweep_key: str):
    """后台任务：参数网格扫描，结果写入 state，不阻塞主流程"""
    from backtest.bt_runner import run_for_web

    cfg = PARAM_SWEEP_CONFIG[strategy]
    p1, p2 = cfg["p1"], cfg["p2"]
    sem = asyncio.Semaphore(3)   # 最多 3 个并行，避免内存爆炸

    async def _run_one(v1, v2):
        async with sem:
            try:
                r = await run_for_web(
                    strategy_name=strategy,
                    start=start, end=end, cash=cash,
                    data_source=data_source,
                    extra_params={p1["name"]: v1, p2["name"]: v2},
                )
                if "error" in r:
                    return None
                ann = r["metrics"].get("annualized_return", "0%")
                return round(float(ann.replace("%", "").replace("+", "")), 2)
            except Exception:
                return None

    try:
        tasks = [_run_one(v1, v2) for v1 in p1["values"] for v2 in p2["values"]]
        flat = await asyncio.gather(*tasks)
        nc = len(p2["values"])
        matrix = [list(flat[i * nc:(i + 1) * nc]) for i in range(len(p1["values"]))]
        result = {"p1": p1, "p2": p2, "matrix": matrix}
        # 仅当 key 未被新任务替换时才写结果
        if state["sweep_key"] == sweep_key:
            state["sweep_result"] = result
            state["sweep_error"]  = None
        # 持久化到数据库（strategy 列用 sweep: 前缀区分）
        try:
            from db.backtest_dao import save_backtest
            await save_backtest(
                strategy=f"sweep:{strategy}",
                start=start, end=end,
                initial_cash=cash,
                metrics={"sweep_key": sweep_key, "p1": p1, "p2": p2, "matrix": matrix},
                is_real=False,
                data_source=data_source,
            )
            logger.info(f"[sweep] 结果已持久化到数据库 key={sweep_key}")
        except Exception as e:
            logger.warning(f"[sweep] 持久化失败（不影响内存结果）: {e}")
    except Exception as e:
        if state["sweep_key"] == sweep_key:
            state["sweep_error"] = str(e)
    finally:
        if state["sweep_key"] == sweep_key:
            state["sweep_running"] = False
    logger.info(f"[sweep] 完成 key={sweep_key}")


@app.post("/api/backtest/param_sweep")
async def param_sweep(
    background_tasks: BackgroundTasks,
    strategy:    str   = "trend_follow",
    start:       str   = "2022-01-01",
    end:         str   = "2023-12-31",
    cash:        float = 100000.0,
    data_source: str   = "local_db",
):
    """
    参数热力图：启动后台扫描，立即返回。
    用 GET /api/backtest/param_sweep/status 轮询进度。
    """
    cfg = PARAM_SWEEP_CONFIG.get(strategy)
    if not cfg:
        return JSONResponse({"error": f"策略 {strategy} 暂不支持参数热力图"}, status_code=400)

    sweep_key = f"{strategy}:{start}:{end}"

    # 若当前 key 已在跑或已有结果，直接告知
    if state["sweep_running"] and state["sweep_key"] == sweep_key:
        return {"status": "already_running", "key": sweep_key}
    if not state["sweep_running"] and state["sweep_key"] == sweep_key and state["sweep_result"]:
        return {"status": "already_done", "key": sweep_key}

    # 启动新任务
    state["sweep_running"] = True
    state["sweep_key"]     = sweep_key
    state["sweep_result"]  = None
    state["sweep_error"]   = None
    background_tasks.add_task(_do_param_sweep, strategy, start, end, cash, data_source, sweep_key)
    logger.info(f"[sweep] 启动后台扫描 key={sweep_key}")
    return {"status": "started", "key": sweep_key}


async def _load_sweep_from_db(sweep_key: str) -> dict | None:
    """从数据库中查找最近一次指定 key 的热力图结果"""
    try:
        from db.backtest_dao import load_backtest_results
        records = await load_backtest_results()
        strategy = sweep_key.split(":")[0]
        for r in records:
            m = r.get("metrics", {})
            if r.get("strategy") == f"sweep:{strategy}" and m.get("sweep_key") == sweep_key:
                return {"p1": m["p1"], "p2": m["p2"], "matrix": m["matrix"]}
    except Exception:
        pass
    return None


@app.get("/api/backtest/param_sweep/status")
async def param_sweep_status(key: str = ""):
    """轮询参数热力图扫描状态；若内存无结果则从 DB 兜底查询"""
    result = state["sweep_result"]
    # 内存无结果且不在运行时，尝试从 DB 恢复
    if not state["sweep_running"] and result is None and key:
        db_result = await _load_sweep_from_db(key)
        if db_result:
            # 恢复到内存（下次直接命中）
            if not state["sweep_running"]:
                state["sweep_key"]    = key
                state["sweep_result"] = db_result
            result = db_result
    return {
        "running": state["sweep_running"],
        "key":     state["sweep_key"] or key,
        "result":  result,
        "error":   state["sweep_error"],
    }


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


# ══════════════════════════════════════════════════════════
# API: 形态命中追踪 (pattern_outcome)
# ══════════════════════════════════════════════════════════

@app.get("/api/pattern_outcome/list")
async def get_pattern_outcome_list(
    strategy: str = "major_capital_accumulation",
    signal_type: str = "",          # "" / "BUY" / "WATCH"
    status: str = "",               # "" / "partial,completed" 等
    since_date: str = "",
    limit: int = 200,
):
    """返回事件明细列表，倒序"""
    from db.pattern_dao import list_events
    statuses = [s.strip() for s in status.split(",") if s.strip()] if status else None
    events = await list_events(strategy, status_in=statuses,
                                signal_type=signal_type or None,
                                since_date=since_date or None, limit=limit)
    # 数值字段转 float（避免 Decimal 序列化问题）
    out = []
    for e in events:
        ec = {}
        for k, v in e.items():
            if v is not None and not isinstance(v, (int, str, bool)):
                try:
                    if hasattr(v, 'isoformat'):
                        ec[k] = v.isoformat()
                    else:
                        ec[k] = float(v)
                except (TypeError, ValueError):
                    ec[k] = str(v)
            else:
                ec[k] = v
        out.append(ec)
    return out


@app.get("/api/pattern_outcome/stats")
async def get_pattern_outcome_stats(
    strategy: str = "major_capital_accumulation",
    signal_type: str = "BUY",       # 默认仅看 BUY 信号
    since_date: str = "",
):
    """
    返回聚合统计：
      total / win_5d / win_10d / win_30d / win_60d
      avg_5d / avg_10d / avg_30d / avg_60d
      peak_ge_10pct / peak_ge_20pct / peak_ge_30pct
      trough_le_5pct / trough_le_10pct
      monthly:[{month, n, avg_30d, win_30d, peak_avg}, ...]
    """
    from db.mysql_pool import get_pool
    pool = await get_pool()
    where = "strategy=%s AND status IN ('partial','completed')"
    args: list = [strategy]
    if signal_type:
        where += " AND signal_type=%s"
        args.append(signal_type)
    if since_date:
        where += " AND signal_date >= %s"
        args.append(since_date)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # 总览
            await cur.execute(f"""
                SELECT COUNT(*),
                       SUM(ret_5d>0)/SUM(ret_5d IS NOT NULL),  AVG(ret_5d),
                       SUM(ret_10d>0)/SUM(ret_10d IS NOT NULL), AVG(ret_10d),
                       SUM(ret_30d>0)/SUM(ret_30d IS NOT NULL), AVG(ret_30d),
                       SUM(ret_60d>0)/SUM(ret_60d IS NOT NULL), AVG(ret_60d),
                       AVG(peak_ret), AVG(trough_ret),
                       SUM(peak_ret>=0.10), SUM(peak_ret>=0.20), SUM(peak_ret>=0.30),
                       SUM(trough_ret<=-0.05), SUM(trough_ret<=-0.10)
                FROM pattern_outcome
                WHERE {where}
            """, tuple(args))
            r = await cur.fetchone()
            total = r[0] or 0
            def _f(v):
                return None if v is None else float(v)
            overview = {
                "total":         total,
                "win_5d":        _f(r[1]),  "avg_5d":  _f(r[2]),
                "win_10d":       _f(r[3]),  "avg_10d": _f(r[4]),
                "win_30d":       _f(r[5]),  "avg_30d": _f(r[6]),
                "win_60d":       _f(r[7]),  "avg_60d": _f(r[8]),
                "avg_peak":      _f(r[9]),
                "avg_trough":    _f(r[10]),
                "peak_ge_10pct": int(r[11] or 0),
                "peak_ge_20pct": int(r[12] or 0),
                "peak_ge_30pct": int(r[13] or 0),
                "trough_le_5pct":  int(r[14] or 0),
                "trough_le_10pct": int(r[15] or 0),
            }
            # 按月聚合
            await cur.execute(f"""
                SELECT DATE_FORMAT(signal_date, '%%Y-%%m') AS mon,
                       COUNT(*),
                       AVG(ret_30d), SUM(ret_30d>0)/SUM(ret_30d IS NOT NULL),
                       AVG(ret_60d), SUM(ret_60d>0)/SUM(ret_60d IS NOT NULL),
                       AVG(peak_ret), AVG(trough_ret)
                FROM pattern_outcome
                WHERE {where}
                GROUP BY mon ORDER BY mon
            """, tuple(args))
            monthly = []
            for mon, n, a30, w30, a60, w60, peak, trough in await cur.fetchall():
                monthly.append({
                    "month": mon, "n": int(n),
                    "avg_30d": _f(a30), "win_30d": _f(w30),
                    "avg_60d": _f(a60), "win_60d": _f(w60),
                    "avg_peak": _f(peak), "avg_trough": _f(trough),
                })
    return {"overview": overview, "monthly": monthly}


# ══════════════════════════════════════════════════════════
# API: 持仓监控 (position_monitor) — 双轨：模拟 + 真实
# ══════════════════════════════════════════════════════════

@app.get("/api/position/list")
async def position_list(
    strategy: str = "major_capital_accumulation",
    status: str = "open",         # "open" / "exited" / ""
    is_real: int = -1,            # -1 全部，0 仅模拟，1 仅真实
    limit: int = 200,
):
    """返回持仓列表。开仓中的会附 current_price + unrealized_pnl_pct（浮赢/浮亏）。"""
    from db.position_dao import list_open, list_exited, list_skipped

    is_real_arg = None if is_real == -1 else int(is_real)
    if status == "open":
        rows = await list_open(strategy, is_real=is_real_arg)
    elif status == "exited":
        rows = await list_exited(strategy, limit=limit, is_real=is_real_arg)
    elif status == "skipped":
        rows = await list_skipped(strategy, limit=limit, is_real=is_real_arg)
    else:
        rows = await list_open(strategy, is_real=is_real_arg)
        rows += await list_exited(strategy, limit=limit, is_real=is_real_arg)
        rows += await list_skipped(strategy, limit=limit, is_real=is_real_arg)

    # ── 开仓持仓附加 current_price + unrealized_pnl_pct ──
    open_codes = [r['code'] for r in rows if r.get('status') == 'open']
    latest_map: dict[str, dict] = {}
    if open_codes:
        from db.mysql_pool import get_pool
        pool = await get_pool()
        placeholders = ",".join(["%s"] * len(open_codes))
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # stock_snapshot 是每日最新快照（每只股票 1 行）
                await cur.execute(
                    f"SELECT code, price, trade_date, pct_change FROM stock_snapshot "
                    f"WHERE code IN ({placeholders})",
                    tuple(open_codes),
                )
                for r in await cur.fetchall():
                    latest_map[r[0]] = {
                        "price":      float(r[1]) if r[1] is not None else None,
                        "trade_date": r[2].isoformat() if r[2] else None,
                        "pct_change": float(r[3]) if r[3] is not None else None,
                    }

    # 数值字段标准化（Decimal/date → 可序列化）
    out = []
    for r in rows:
        ec = {}
        for k, v in r.items():
            if v is None or isinstance(v, (int, str, bool)):
                ec[k] = v
            elif hasattr(v, 'isoformat'):
                ec[k] = v.isoformat()
            else:
                try: ec[k] = float(v)
                except (TypeError, ValueError): ec[k] = str(v)
        # 附加：开仓中的当前价 + 浮赢
        if ec.get('status') == 'open':
            latest = latest_map.get(ec['code'])
            if latest and latest['price']:
                ec['current_price'] = latest['price']
                ec['current_date']  = latest['trade_date']
                # stock_snapshot.pct_change 单位是 %（如 4.52 = 4.52%），归一化到 fraction
                if latest['pct_change'] is not None:
                    ec['day_change'] = latest['pct_change'] / 100.0
                ep = ec.get('entry_price')
                if ep is not None:
                    ec['unrealized_pnl_pct'] = latest['price'] / float(ep) - 1.0
        out.append(ec)
    return out


@app.post("/api/position/add")
async def position_add(
    code: str,
    entry_date: str,
    entry_price: float,
    shares: int,
    name: str = "",
    strategy: str = "major_capital_accumulation",
):
    """手动新增真实持仓（is_real=1）。signal_date 默认与 entry_date 相同。"""
    from db.position_dao import upsert_position
    if not code or not entry_date or entry_price <= 0 or shares <= 0:
        return {"ok": False, "error": "参数缺失或非法"}
    await upsert_position(
        strategy=strategy, code=code, name=name or code,
        signal_date=entry_date, entry_date=entry_date,
        entry_price=entry_price, is_real=1, shares=shares,
    )
    return {"ok": True}


@app.post("/api/position/mark_real")
async def position_mark_real(position_id: int, shares: int):
    """把模拟持仓标记为真实持仓（接收离场推送）"""
    from db.position_dao import mark_as_real
    if shares <= 0:
        return {"ok": False, "error": "shares 必须 > 0"}
    ok = await mark_as_real(position_id, shares)
    return {"ok": ok}


@app.post("/api/position/unmark_real")
async def position_unmark_real(position_id: int):
    """把真实持仓改回模拟（不再推送，但保留记录）"""
    from db.mysql_pool import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE position_monitor SET is_real=0, shares=NULL WHERE id=%s",
                (position_id,),
            )
            return {"ok": cur.rowcount > 0}


@app.post("/api/position/delete")
async def position_delete(position_id: int):
    """删除持仓记录（误录或重复时用）"""
    from db.position_dao import delete_position
    ok = await delete_position(position_id)
    return {"ok": ok}


@app.post("/api/position/run_exit_scan")
async def position_run_exit_scan(background_tasks: BackgroundTasks):
    """手动触发一次持仓离场扫描。"""
    if state.get("exit_scan_running"):
        return JSONResponse({"ok": False, "error": "离场扫描正在运行"}, status_code=409)

    async def _run():
        state["exit_scan_running"] = True
        try:
            import sys
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "scripts.daily_exit_scan",
                cwd=str(Path(__file__).resolve().parent.parent),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await proc.communicate()
            text = (out or b"").decode("utf-8", errors="replace")
            logger.info(f"[Web] 离场扫描完成 rc={proc.returncode}\n{text[-4000:]}")
            await ws_manager.broadcast({
                "type": "exit_scan_done",
                "data": {"returncode": proc.returncode, "tail": text[-4000:]},
            })
        except Exception as e:
            logger.error(f"[Web] 离场扫描失败: {e}")
            await ws_manager.broadcast({"type": "exit_scan_error", "data": str(e)})
        finally:
            state["exit_scan_running"] = False

    background_tasks.add_task(_run)
    return {"ok": True, "message": "离场扫描已启动"}


@app.get("/api/position/stats")
async def position_stats(
    strategy: str = "major_capital_accumulation",
    is_real: int = -1,
):
    """聚合统计：开仓中 / 已离场 / 各 exit_reason 分布 / PnL 桶"""
    from db.mysql_pool import get_pool
    pool = await get_pool()
    real_clause = "" if is_real == -1 else f" AND is_real={int(is_real)}"
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                SELECT
                  SUM(status='open') AS n_open,
                  SUM(status='exited') AS n_exited,
                  SUM(status='skipped') AS n_skipped,
                  AVG(CASE WHEN status='exited' THEN exit_pnl_pct END) AS avg_pnl,
                  SUM(CASE WHEN status='exited' AND exit_pnl_pct > 0 THEN 1 ELSE 0 END) AS n_win,
                  AVG(CASE WHEN status='exited' THEN days_held END) AS avg_days,
                  MAX(exit_pnl_pct) AS max_pnl,
                  MIN(exit_pnl_pct) AS min_pnl
                FROM position_monitor
                WHERE strategy=%s {real_clause}
            """, (strategy,))
            r = await cur.fetchone()
            def f(v): return None if v is None else float(v)
            overview = {
                "n_open":   int(r[0] or 0),
                "n_exited": int(r[1] or 0),
                "n_skipped": int(r[2] or 0),
                "avg_pnl":  f(r[3]),
                "n_win":    int(r[4] or 0),
                "win_rate": (int(r[4] or 0) / int(r[1] or 1)) if r[1] else 0,
                "avg_days": f(r[5]),
                "max_pnl":  f(r[6]),
                "min_pnl":  f(r[7]),
            }
            # exit_reason 分布
            await cur.execute(f"""
                SELECT
                  CASE
                    WHEN exit_reason LIKE '%%硬止损%%' THEN '硬止损'
                    WHEN exit_reason LIKE '%%stage1%%' THEN 'stage1(<5%%)'
                    WHEN exit_reason LIKE '%%stage2%%' THEN 'stage2(5~15%%)'
                    WHEN exit_reason LIKE '%%stage3%%' THEN 'stage3(≥15%%)'
                    WHEN exit_reason LIKE '%%MA20%%' THEN '破MA20'
                    ELSE 'other'
                  END AS cat,
                  COUNT(*), AVG(exit_pnl_pct), SUM(exit_pnl_pct > 0) / COUNT(*) AS win
                FROM position_monitor
                WHERE strategy=%s AND status='exited' {real_clause}
                GROUP BY cat ORDER BY COUNT(*) DESC
            """, (strategy,))
            by_reason = [{
                "reason": rr[0], "n": int(rr[1]),
                "avg_pnl": f(rr[2]), "win_rate": f(rr[3]),
            } for rr in await cur.fetchall()]
    return {"overview": overview, "by_reason": by_reason}


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


# ══════════════════════════════════════════════════════════
# API: 本地数据仓库（选股中心）
# ══════════════════════════════════════════════════════════

@app.get("/api/market/status")
async def market_status():
    """返回本地数据仓库状态：股票数量、最新数据日期、K线覆盖情况"""
    try:
        from backtest.local_screener import get_data_warehouse_status
        status = await get_data_warehouse_status()
        return {"ok": True, **status}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/market/update")
async def trigger_market_update(
    background_tasks: BackgroundTasks,
    skip_kline: bool = False,
):
    """
    手动触发每日行情数据更新（通常由 crontab 自动调用，也可在此手动触发）。
    skip_kline=true 时只更新快照（约30秒），skip_kline=false 时同时更新K线（约20分钟）。
    """
    if state.get("market_update_running"):
        return JSONResponse({"error": "数据更新已在进行中"}, status_code=409)

    async def _do_update():
        state["market_update_running"] = True
        try:
            from scripts.daily_data_update import run_daily_update
            result = await run_daily_update(skip_kline=skip_kline)
            await ws_manager.broadcast({"type": "market_update_done", "data": result})
            logger.info(f"[Web] 行情更新完成: {result}")
        except Exception as e:
            logger.error(f"[Web] 行情更新失败: {e}")
            await ws_manager.broadcast({"type": "market_update_error", "data": str(e)})
        finally:
            state["market_update_running"] = False

    state["market_update_running"] = False  # 确保 key 存在
    background_tasks.add_task(_do_update)
    return {"message": "行情数据更新已启动", "skip_kline": skip_kline}


@app.get("/api/market/presets")
async def market_screen_presets():
    """返回本地选股预设列表（含扩展预设如 value、high_turnover）"""
    from backtest.local_screener import LOCAL_SCREEN_PRESETS
    return [{"id": k, **v} for k, v in LOCAL_SCREEN_PRESETS.items()]


@app.post("/api/market/screen")
async def market_screen(
    preset:         str   = "default",
    min_cap_yi:     float = 0,
    max_cap_yi:     float = 0,
    min_amount_wan: float = 0,
    min_price:      float = 0,
    max_price:      float = 0,
    exclude_st:     bool  = True,
    min_list_days:  int   = 0,
    industry:       str   = "",
    min_pe:         Optional[float] = None,
    max_pe:         Optional[float] = None,
    min_pb:         Optional[float] = None,
    min_turnover:   Optional[float] = None,
    order_by:       str   = "amount DESC",
    top_n:          int   = 500,
    with_signals:   bool  = False,   # 是否追加 Backtrader 策略信号分析
    strategy:       str   = "",      # with_signals=True 时使用的策略名
):
    """
    从本地数据仓库执行 SQL 选股。
    with_signals=True 时，额外对筛选结果跑 Backtrader 策略分析，
    返回 signal_date / signal_dates / match_score（与实时行情模式完全一致）。
    """
    from backtest.local_screener import LocalScreener, LOCAL_SCREEN_PRESETS

    base_params = {}
    if preset and preset in LOCAL_SCREEN_PRESETS:
        base_params = dict(LOCAL_SCREEN_PRESETS[preset].get("params", {}))

    overrides = {
        "min_cap_yi":     min_cap_yi,
        "max_cap_yi":     max_cap_yi,
        "min_amount_wan": min_amount_wan,
        "min_price":      min_price,
        "max_price":      max_price,
        "exclude_st":     exclude_st,
        "min_list_days":  min_list_days,
        "top_n":          top_n,
        "order_by":       order_by,
    }
    if industry:
        overrides["industry"] = industry
    if min_pe is not None:
        overrides["min_pe"] = min_pe
    if max_pe is not None:
        overrides["max_pe"] = max_pe
    if min_pb is not None:
        overrides["min_pb"] = min_pb
    if min_turnover is not None:
        overrides["min_turnover"] = min_turnover

    for k, v in overrides.items():
        if isinstance(v, (int, float)) and v != 0:
            base_params[k] = v
        elif isinstance(v, str) and v and k not in ("order_by",):
            base_params[k] = v
        elif k in ("exclude_st", "order_by", "industry", "min_pe", "max_pe", "min_pb", "min_turnover"):
            base_params[k] = v

    screener = LocalScreener(**base_params)
    try:
        candidates = await screener.screen()
    except Exception as e:
        return JSONResponse({"error": str(e), "stocks": []}, status_code=500)

    # 可选：追加 Backtrader 策略信号分析
    if with_signals and candidates:
        from backtest.strategy_analyzer import analyze_stocks
        strategy_name = strategy or preset  # 策略名默认与 preset 对齐
        stocks = await analyze_stocks(candidates, strategy_name=strategy_name)
    else:
        stocks = candidates

    return {
        "count":        len(stocks),
        "preset":       preset,
        "with_signals": with_signals,
        "stocks":       stocks,
    }


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


# ══════════════════════════════════════════════════════════
# SPA 路由兜底：任何非 API、非静态资源、非 WS 的 GET 请求都返回 index.html
# 让 React 前端通过 history API 处理客户端路由（/scan /backtest /equity 等）
# 必须放在所有 @app.get 路由 *之后*，让 FastAPI 优先匹配明确的路由
# ══════════════════════════════════════════════════════════

@app.get("/{full_path:path}", response_class=HTMLResponse, include_in_schema=False)
async def spa_catch_all(full_path: str):
    # 仅排除明显的 API/资源前缀；不在白名单里的全部返回 SPA
    if full_path.startswith(("api/", "static/", "ws", "favicon.ico", "icons.svg")):
        # 不应走到这里——明确的 @app.get 路由已优先匹配；走到这里说明确实没找到
        # 返回 404 让前端知道路径无效
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not Found")

    react_index = Path(__file__).parent / "static" / "dist" / "index.html"
    if react_index.exists():
        return HTMLResponse(
            react_index.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-cache, must-revalidate"},
        )
    html_path = Path(__file__).parent / "templates" / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))
