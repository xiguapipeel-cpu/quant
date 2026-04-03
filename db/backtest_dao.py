"""
回测结果 DAO
"""

import json
import math
from db.mysql_pool import get_pool
from utils.logger import setup_logger

logger = setup_logger("bt_dao")


def _sanitize(obj):
    """清理 NaN / Inf，确保 JSON 可序列化"""
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, float):
        if math.isinf(obj) or math.isnan(obj):
            return 0.0
    return obj


async def save_backtest(strategy: str, start: str, end: str,
                        initial_cash: float, metrics: dict,
                        equity_data: dict = None, trades_data: list = None,
                        is_real: bool = True) -> int:
    """保存一次回测结果，返回自增 id"""
    pool = await get_pool()
    metrics_s  = json.dumps(_sanitize(metrics), ensure_ascii=False)
    equity_s   = json.dumps(_sanitize(equity_data), ensure_ascii=False) if equity_data else None
    trades_s   = json.dumps(_sanitize(trades_data), ensure_ascii=False) if trades_data else None

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO backtest_results
                    (strategy, start_date, end_date, initial_cash,
                     metrics_json, equity_json, trades_json, is_real)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (strategy, start, end, initial_cash,
                 metrics_s, equity_s, trades_s, int(is_real)),
            )
            new_id = cur.lastrowid

    # 只保留最近 50 条（清理旧记录）
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM backtest_results
                WHERE id NOT IN (
                    SELECT id FROM (
                        SELECT id FROM backtest_results ORDER BY created_at DESC LIMIT 50
                    ) AS keep_ids
                )
                """
            )

    logger.info(f"[bt_dao] 回测结果已入库 id={new_id} strategy={strategy}")
    return new_id


async def load_backtest_results() -> list[dict]:
    """读取所有回测结果（前端展示用），按时间倒序"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT * FROM backtest_results ORDER BY created_at DESC LIMIT 50"
            )
            rows = await cur.fetchall()

    results = []
    for r in rows:
        record = {
            "id":       r["id"],
            "strategy": r["strategy"],
            "start":    r["start_date"],
            "end":      r["end_date"],
            "cash":     r["initial_cash"],
            "metrics":  json.loads(r["metrics_json"]) if r["metrics_json"] else {},
            "time":     r["created_at"].strftime("%Y-%m-%d %H:%M") if r["created_at"] else "",
            "is_real":  bool(r["is_real"]),
        }
        if r.get("equity_json"):
            record["equity"] = json.loads(r["equity_json"])
        if r.get("trades_json"):
            record["trades"] = json.loads(r["trades_json"])
        results.append(record)
    return results


import aiomysql
