"""
MySQL 连接池 + 表结构管理
─────────────────────────────────────
全局唯一连接池，懒加载（第一次 get_pool 时创建）。
"""

import asyncio
import aiomysql
from utils.logger import setup_logger

logger = setup_logger("mysql")

_pool: aiomysql.Pool | None = None
_lock = asyncio.Lock()

DB_CONFIG = {
    "host":       "127.0.0.1",
    "port":       3306,
    "user":       "root",
    "password":   "",
    "db":         "quant_system",
    "charset":    "utf8mb4",
    "minsize":    2,
    "maxsize":    10,
    "autocommit": True,
}

# ── 建表 DDL ─────────────────────────────────────────────

_DDL = [
    # 1. 扫描结果（按策略分组，每条是一只股票）
    """
    CREATE TABLE IF NOT EXISTS scan_results (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        strategy      VARCHAR(64)  NOT NULL,
        code          VARCHAR(16)  NOT NULL,
        name          VARCHAR(32)  NOT NULL DEFAULT '',
        market        VARCHAR(8)   NOT NULL DEFAULT 'SZ',
        price         DOUBLE       NOT NULL DEFAULT 0,
        cap_yi        DOUBLE       NOT NULL DEFAULT 0,
        amount_wan    DOUBLE       NOT NULL DEFAULT 0,
        pe            DOUBLE       DEFAULT NULL,
        pct_change    DOUBLE       DEFAULT NULL,
        signal_type   VARCHAR(16)  NOT NULL DEFAULT '',
        signal_date   VARCHAR(16)  NOT NULL DEFAULT '',
        signal_dates  TEXT         DEFAULT NULL,
        match_score   TEXT         DEFAULT NULL,
        signal_reason TEXT,
        confidence    DOUBLE       NOT NULL DEFAULT 0,
        scan_time     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_strategy_code (strategy, code),
        KEY idx_strategy (strategy),
        KEY idx_scan_time (scan_time)
    ) ENGINE=InnoDB
    """,

    # 2. 回测结果
    """
    CREATE TABLE IF NOT EXISTS backtest_results (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        strategy      VARCHAR(64)  NOT NULL,
        start_date    VARCHAR(16)  NOT NULL,
        end_date      VARCHAR(16)  NOT NULL,
        initial_cash  DOUBLE       NOT NULL DEFAULT 1000000,
        metrics_json  MEDIUMTEXT   NOT NULL,
        equity_json   MEDIUMTEXT,
        trades_json   MEDIUMTEXT,
        is_real       TINYINT(1)   NOT NULL DEFAULT 1,
        created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        KEY idx_strategy (strategy),
        KEY idx_created (created_at)
    ) ENGINE=InnoDB
    """,

    # 3. 定时任务配置（单行）
    """
    CREATE TABLE IF NOT EXISTS schedule_config (
        id             INT          NOT NULL DEFAULT 1 PRIMARY KEY,
        enabled        TINYINT(1)   NOT NULL DEFAULT 0,
        hour           INT          NOT NULL DEFAULT 15,
        minute         INT          NOT NULL DEFAULT 35,
        notify_wechat  TINYINT(1)   NOT NULL DEFAULT 1,
        last_run       VARCHAR(32)  DEFAULT NULL,
        last_status    VARCHAR(128) DEFAULT NULL,
        updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
]


async def get_pool() -> aiomysql.Pool:
    """获取全局连接池（懒加载 + 建表）"""
    global _pool
    if _pool is not None and not _pool._closed:
        return _pool
    async with _lock:
        if _pool is not None and not _pool._closed:
            return _pool
        logger.info("[MySQL] 创建连接池...")
        _pool = await aiomysql.create_pool(**DB_CONFIG)
        await _ensure_tables()
        logger.info("[MySQL] 连接池就绪，表结构已检查")
        return _pool


async def _ensure_tables():
    """建表（IF NOT EXISTS）+ 增量迁移"""
    async with _pool.acquire() as conn:
        async with conn.cursor() as cur:
            for ddl in _DDL:
                await cur.execute(ddl)
            # 增量迁移：scan_results 新增 signal_dates 列
            await cur.execute(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='scan_results' "
                "AND COLUMN_NAME='signal_dates'",
                (DB_CONFIG["db"],),
            )
            (cnt,) = await cur.fetchone()
            if cnt == 0:
                await cur.execute(
                    "ALTER TABLE scan_results ADD COLUMN signal_dates TEXT DEFAULT NULL AFTER signal_date"
                )
                logger.info("[MySQL] scan_results 新增 signal_dates 列")
            # 增量迁移：scan_results 新增 match_score 列（TEXT 存 JSON）
            await cur.execute(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='scan_results' "
                "AND COLUMN_NAME='match_score'",
                (DB_CONFIG["db"],),
            )
            (cnt2,) = await cur.fetchone()
            if cnt2 == 0:
                await cur.execute(
                    "ALTER TABLE scan_results ADD COLUMN match_score TEXT DEFAULT NULL AFTER signal_dates"
                )
                logger.info("[MySQL] scan_results 新增 match_score 列")
            else:
                # 如果已存在但类型是 INT，改为 TEXT
                await cur.execute(
                    "SELECT DATA_TYPE FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='scan_results' "
                    "AND COLUMN_NAME='match_score'",
                    (DB_CONFIG["db"],),
                )
                (dtype,) = await cur.fetchone()
                if dtype == "int":
                    await cur.execute(
                        "ALTER TABLE scan_results MODIFY COLUMN match_score TEXT DEFAULT NULL"
                    )
                    logger.info("[MySQL] match_score 列类型 INT→TEXT")


async def close_pool():
    global _pool
    if _pool and not _pool._closed:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
        logger.info("[MySQL] 连接池已关闭")
