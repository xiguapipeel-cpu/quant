"""
扫描结果 DAO — 按策略存取
"""

import json
from datetime import datetime
from db.mysql_pool import get_pool
from utils.logger import setup_logger

logger = setup_logger("scan_dao")


def _parse_json(raw):
    """安全解析 JSON 字段，兼容 str / dict / int / None"""
    if raw is None:
        return {}
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, (int, float)):
        # 旧数据是 INT 类型，转为 {total: N}
        return {"total": int(raw)} if raw else {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


async def upsert_scan(strategy: str, results: list[dict]):
    """
    写入/更新某策略的扫描结果（全量替换）。
    先删除该策略旧数据，再批量插入新数据。
    """
    pool = await get_pool()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM scan_results WHERE strategy=%s", (strategy,))
            if not results:
                return
            sql = """
                INSERT INTO scan_results
                    (strategy, code, name, market, price, cap_yi, amount_wan,
                     pe, pct_change, signal_type, signal_date, signal_dates,
                     match_score, signal_reason, confidence, scan_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            rows = [
                (
                    strategy,
                    r.get("code", ""),
                    r.get("name", ""),
                    r.get("market", "SZ"),
                    r.get("price", 0),
                    r.get("cap_yi", 0),
                    r.get("amount_wan", 0),
                    r.get("pe"),
                    r.get("pct_change"),
                    r.get("signal_type", ""),
                    r.get("signal_date", ""),
                    json.dumps(r.get("signal_dates", []), ensure_ascii=False),
                    json.dumps(r.get("match_score") or {}, ensure_ascii=False),
                    r.get("signal_reason", ""),
                    r.get("confidence", 0),
                    r.get("scan_time") or now,   # 优先使用行级 scan_time，保留旧标的原始发现时间
                )
                for r in results
            ]
            await cur.executemany(sql, rows)
    logger.info(f"[scan_dao] {strategy} 写入 {len(results)} 条扫描结果")


async def load_scan(strategy: str) -> list[dict]:
    """读取某策略的扫描结果"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT * FROM scan_results WHERE strategy=%s ORDER BY confidence DESC",
                (strategy,),
            )
            rows = await cur.fetchall()
    # 转为前端可用格式
    results = []
    for r in rows:
        # signal_dates 可能是 JSON 字符串或已经是 list
        sd_raw = r.get("signal_dates") or "[]"
        if isinstance(sd_raw, str):
            try:
                signal_dates = json.loads(sd_raw)
            except (json.JSONDecodeError, TypeError):
                signal_dates = []
        else:
            signal_dates = sd_raw
        results.append({
            "code":           r["code"],
            "name":           r["name"],
            "market":         r["market"],
            "price":          r["price"],
            "cap_yi":         r["cap_yi"],
            "amount_wan":     r["amount_wan"],
            "pe":             r["pe"],
            "pct_change":     r["pct_change"],
            "signal_type":    r["signal_type"],
            "signal_date":    r["signal_date"],
            "signal_dates":   signal_dates,
            "match_score":    _parse_json(r.get("match_score")),
            "signal_reason":  r["signal_reason"],
            "confidence":     r["confidence"],
            "scan_time":      str(r.get("scan_time", "")) if r.get("scan_time") else "",
            "integrity_pass": True,
        })
    return results


async def load_scan_meta(strategy: str) -> dict:
    """获取某策略扫描的元信息（条数、时间）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) AS cnt, MAX(scan_time) AS last_time "
                "FROM scan_results WHERE strategy=%s",
                (strategy,),
            )
            row = await cur.fetchone()
    return {
        "result_count": row[0] if row else 0,
        "last_scan_time": str(row[1]) if row and row[1] else None,
    }


import aiomysql
