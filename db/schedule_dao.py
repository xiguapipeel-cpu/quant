"""
定时任务配置 DAO（单行记录）
"""

from db.mysql_pool import get_pool
from utils.logger import setup_logger

logger = setup_logger("sched_dao")

_DEFAULTS = {
    "enabled": False,
    "hour": 15,
    "minute": 35,
    "notify_wechat": True,
    "last_run": None,
    "last_status": None,
}


async def load_schedule() -> dict:
    """读取定时任务配置，不存在则插入默认行"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM schedule_config WHERE id=1")
            row = await cur.fetchone()
            if not row:
                # 插入默认行
                await cur.execute(
                    """
                    INSERT INTO schedule_config (id, enabled, hour, minute, notify_wechat)
                    VALUES (1, 0, 15, 35, 1)
                    """
                )
                return dict(_DEFAULTS)
            # row: (id, enabled, hour, minute, notify_wechat, last_run, last_status, updated_at)
            return {
                "enabled":       bool(row[1]),
                "hour":          row[2],
                "minute":        row[3],
                "notify_wechat": bool(row[4]),
                "last_run":      row[5],
                "last_status":   row[6],
            }


async def save_schedule(patch: dict):
    """更新定时任务配置（只更新传入的字段）"""
    pool = await get_pool()
    allowed = ("enabled", "hour", "minute", "notify_wechat", "last_run", "last_status")
    sets = []
    vals = []
    for k, v in patch.items():
        if k in allowed:
            sets.append(f"{k}=%s")
            if k in ("enabled", "notify_wechat"):
                vals.append(int(bool(v)))
            else:
                vals.append(v)
    if not sets:
        return
    sql = f"UPDATE schedule_config SET {', '.join(sets)} WHERE id=1"
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, vals)
    logger.debug(f"[sched_dao] 配置已更新: {patch}")
