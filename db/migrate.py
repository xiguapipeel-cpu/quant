"""
一次性迁移：将现有 JSON 文件数据导入 MySQL
"""

import asyncio
import json
from pathlib import Path
from db.mysql_pool import get_pool
from utils.logger import setup_logger

logger = setup_logger("migrate")

PROJECT_ROOT = Path(__file__).resolve().parent.parent


async def migrate_all():
    """迁移所有 JSON → MySQL"""
    pool = await get_pool()

    await _migrate_scan_results(pool)
    await _migrate_backtest_results(pool)
    await _migrate_schedule_config(pool)

    logger.info("[迁移] 全部完成")


async def _migrate_scan_results(pool):
    f = PROJECT_ROOT / "scan_results.json"
    if not f.exists():
        logger.info("[迁移] scan_results.json 不存在，跳过")
        return
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[迁移] scan_results.json 解析失败: {e}")
        return

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for strategy, v in data.items():
                results = v.get("results", []) if isinstance(v, dict) else []
                scan_time = v.get("scan_time", "2026-01-01 00:00") if isinstance(v, dict) else "2026-01-01 00:00"
                if not results:
                    continue
                # 先清理
                await cur.execute("DELETE FROM scan_results WHERE strategy=%s", (strategy,))
                sql = """
                    INSERT INTO scan_results
                        (strategy, code, name, market, price, cap_yi, amount_wan,
                         pe, pct_change, signal_type, signal_date, signal_reason,
                         confidence, scan_time)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                rows = [
                    (
                        strategy,
                        r.get("code", ""),
                        r.get("name", ""),
                        r.get("market", "SZ"),
                        r.get("price", 0) or 0,
                        r.get("cap_yi", 0) or 0,
                        r.get("amount_wan", 0) or 0,
                        r.get("pe"),
                        r.get("pct_change"),
                        r.get("signal_type", ""),
                        r.get("signal_date", ""),
                        r.get("signal_reason", ""),
                        r.get("confidence", 0) or 0,
                        scan_time,
                    )
                    for r in results
                ]
                await cur.executemany(sql, rows)
                logger.info(f"[迁移] scan_results/{strategy}: {len(rows)} 条")
    # 备份
    f.rename(f.with_suffix(".json.bak"))
    logger.info("[迁移] scan_results.json → .bak")


async def _migrate_backtest_results(pool):
    f = PROJECT_ROOT / "backtest_results.json"
    if not f.exists():
        logger.info("[迁移] backtest_results.json 不存在，跳过")
        return
    try:
        records = json.loads(f.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[迁移] backtest_results.json 解析失败: {e}")
        return

    if not isinstance(records, list):
        logger.warning("[迁移] backtest_results.json 格式异常")
        return

    import math

    def sanitize(obj):
        if isinstance(obj, dict):
            return {k: sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [sanitize(v) for v in obj]
        if isinstance(obj, float) and (math.isinf(obj) or math.isnan(obj)):
            return 0.0
        return obj

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            count = 0
            for rec in records:
                strategy = rec.get("strategy", "")
                start    = rec.get("start", "")
                end      = rec.get("end", "")
                cash     = rec.get("cash", 1000000)
                metrics  = sanitize(rec.get("metrics", {}))
                equity   = sanitize(rec.get("equity"))
                trades   = sanitize(rec.get("trades"))
                is_real  = rec.get("is_real", True)

                metrics_s = json.dumps(metrics, ensure_ascii=False)
                equity_s  = json.dumps(equity, ensure_ascii=False) if equity else None
                trades_s  = json.dumps(trades, ensure_ascii=False) if trades else None

                await cur.execute(
                    """
                    INSERT INTO backtest_results
                        (strategy, start_date, end_date, initial_cash,
                         metrics_json, equity_json, trades_json, is_real)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (strategy, start, end, cash, metrics_s, equity_s, trades_s, int(is_real)),
                )
                count += 1
            logger.info(f"[迁移] backtest_results: {count} 条")
    f.rename(f.with_suffix(".json.bak"))
    logger.info("[迁移] backtest_results.json → .bak")


async def _migrate_schedule_config(pool):
    f = PROJECT_ROOT / "config" / "schedule.json"
    if not f.exists():
        logger.info("[迁移] schedule.json 不存在，跳过")
        return
    try:
        cfg = json.loads(f.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[迁移] schedule.json 解析失败: {e}")
        return

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO schedule_config (id, enabled, hour, minute, notify_wechat, last_run, last_status)
                VALUES (1, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    enabled=VALUES(enabled), hour=VALUES(hour), minute=VALUES(minute),
                    notify_wechat=VALUES(notify_wechat), last_run=VALUES(last_run),
                    last_status=VALUES(last_status)
                """,
                (
                    int(bool(cfg.get("enabled", False))),
                    cfg.get("hour", 15),
                    cfg.get("minute", 35),
                    int(bool(cfg.get("notify_wechat", True))),
                    cfg.get("last_run"),
                    cfg.get("last_status"),
                ),
            )
    logger.info(f"[迁移] schedule_config: enabled={cfg.get('enabled')}")


if __name__ == "__main__":
    asyncio.run(migrate_all())
