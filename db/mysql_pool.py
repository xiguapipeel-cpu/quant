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
    # ── 本地行情仓库 ──────────────────────────────────────────

    # A. 股票基础信息（代码、名称、行业、上市日期）
    """
    CREATE TABLE IF NOT EXISTS stock_basic (
        code        VARCHAR(10)  NOT NULL PRIMARY KEY,
        name        VARCHAR(50)  NOT NULL DEFAULT '',
        market      VARCHAR(5)   NOT NULL DEFAULT 'SZ',
        industry    VARCHAR(50)  DEFAULT NULL,
        list_date   DATE         DEFAULT NULL,
        is_st       TINYINT(1)   NOT NULL DEFAULT 0,
        updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,

    # B. 每日行情快照（每日15:30全量更新，供选股直接查询）
    """
    CREATE TABLE IF NOT EXISTS stock_snapshot (
        code          VARCHAR(10)    NOT NULL PRIMARY KEY,
        name          VARCHAR(50)    NOT NULL DEFAULT '',
        market        VARCHAR(5)     NOT NULL DEFAULT 'SZ',
        price         DECIMAL(10,3)  DEFAULT NULL,
        pct_change    DECIMAL(8,4)   DEFAULT NULL,
        volume        BIGINT         DEFAULT NULL,
        amount        DECIMAL(20,2)  DEFAULT NULL,
        market_cap    DECIMAL(20,2)  DEFAULT NULL,
        float_cap     DECIMAL(20,2)  DEFAULT NULL,
        pe_ttm        DECIMAL(10,3)  DEFAULT NULL,
        pb            DECIMAL(10,3)  DEFAULT NULL,
        turnover_rate DECIMAL(8,4)   DEFAULT NULL,
        amplitude     DECIMAL(8,4)   DEFAULT NULL,
        high          DECIMAL(10,3)  DEFAULT NULL,
        low           DECIMAL(10,3)  DEFAULT NULL,
        open_price    DECIMAL(10,3)  DEFAULT NULL,
        prev_close    DECIMAL(10,3)  DEFAULT NULL,
        vol_ratio     DECIMAL(8,4)   DEFAULT NULL,
        pct_60d       DECIMAL(8,4)   DEFAULT NULL,
        pct_ytd       DECIMAL(8,4)   DEFAULT NULL,
        industry      VARCHAR(50)    DEFAULT NULL,
        list_date     DATE           DEFAULT NULL,
        is_st         TINYINT(1)     NOT NULL DEFAULT 0,
        trade_date    DATE           DEFAULT NULL,
        updated_at    DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_market_cap (market_cap),
        KEY idx_trade_date (trade_date),
        KEY idx_industry (industry),
        KEY idx_pe_ttm (pe_ttm)
    ) ENGINE=InnoDB
    """,

    # C. 日线K线数据（增量更新，用于技术指标计算）
    """
    CREATE TABLE IF NOT EXISTS stock_daily (
        id            BIGINT         AUTO_INCREMENT PRIMARY KEY,
        code          VARCHAR(10)    NOT NULL,
        trade_date    DATE           NOT NULL,
        open_price    DECIMAL(10,3)  DEFAULT NULL,
        high          DECIMAL(10,3)  DEFAULT NULL,
        low           DECIMAL(10,3)  DEFAULT NULL,
        close         DECIMAL(10,3)  DEFAULT NULL,
        volume        BIGINT         DEFAULT NULL,
        amount        DECIMAL(20,2)  DEFAULT NULL,
        pct_change    DECIMAL(8,4)   DEFAULT NULL,
        turnover_rate DECIMAL(8,4)   DEFAULT NULL,
        UNIQUE KEY uk_code_date (code, trade_date),
        KEY idx_trade_date (trade_date),
        KEY idx_code (code)
    ) ENGINE=InnoDB
    """,

    # D. 个股资金流向（主力/超大单/大单/中单/小单 净流入）
    # 来源：akshare.stock_individual_fund_flow（东方财富）
    # 限制：API 仅返回最近 ~120 个交易日，需要每日增量采集才能积累历史
    """
    CREATE TABLE IF NOT EXISTS stock_fund_flow (
        code              VARCHAR(10)    NOT NULL,
        trade_date        DATE           NOT NULL,
        close_price       DECIMAL(10,3)  DEFAULT NULL,
        pct_change        DECIMAL(8,4)   DEFAULT NULL,
        main_net_amount   DECIMAL(20,2)  DEFAULT NULL,   -- 主力净流入-净额（元）
        main_net_pct      DECIMAL(8,4)   DEFAULT NULL,   -- 主力净流入-净占比（%）
        super_large_net   DECIMAL(20,2)  DEFAULT NULL,
        super_large_pct   DECIMAL(8,4)   DEFAULT NULL,
        large_net         DECIMAL(20,2)  DEFAULT NULL,
        large_pct         DECIMAL(8,4)   DEFAULT NULL,
        medium_net        DECIMAL(20,2)  DEFAULT NULL,
        medium_pct        DECIMAL(8,4)   DEFAULT NULL,
        small_net         DECIMAL(20,2)  DEFAULT NULL,
        small_pct         DECIMAL(8,4)   DEFAULT NULL,
        updated_at        DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_code_date (code, trade_date),
        KEY idx_trade_date (trade_date),
        KEY idx_code (code)
    ) ENGINE=InnoDB
    """,

    # ── 原有业务表 ────────────────────────────────────────────

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

    # 5. 持仓监控（双轨：模拟 / 真实）+ 离场跟踪（daily_exit_scan.py 维护）
    """
    CREATE TABLE IF NOT EXISTS position_monitor (
        id              BIGINT       AUTO_INCREMENT PRIMARY KEY,
        strategy        VARCHAR(64)  NOT NULL,
        code            VARCHAR(16)  NOT NULL,
        name            VARCHAR(32)  NOT NULL DEFAULT '',
        -- 入场（signal_date 次开盘）
        signal_date     DATE         NOT NULL,
        entry_date      DATE         NOT NULL,
        entry_price     DECIMAL(10,3) NOT NULL,
        -- 双轨：is_real=0 模拟（BUY 信号自动登记，不推送），1 真实（手工标记，离场推送）
        is_real         TINYINT(1)   NOT NULL DEFAULT 0,
        shares          INT          DEFAULT NULL,            -- 仅真实持仓有意义
        -- trail 状态（每日 update）
        highest_price   DECIMAL(10,3) DEFAULT NULL,
        highest_date    DATE         DEFAULT NULL,
        lowest_price    DECIMAL(10,3) DEFAULT NULL,
        lowest_date     DATE         DEFAULT NULL,
        days_held       INT          NOT NULL DEFAULT 0,
        last_check_date DATE         DEFAULT NULL,
        -- 状态
        status          VARCHAR(20)  NOT NULL DEFAULT 'open', -- open/exited
        exit_date       DATE         DEFAULT NULL,      -- 信号触发日（今日收盘判定）
        exit_price      DECIMAL(10,3) DEFAULT NULL,     -- 信号触发收盘价（参考）
        exit_reason     VARCHAR(255) DEFAULT NULL,
        actual_exit_date  DATE         DEFAULT NULL,    -- 实际成交日（次交易日）
        actual_exit_price DECIMAL(10,3) DEFAULT NULL,   -- 实际成交价（次日 open）
        actual_filled     TINYINT(1)   NOT NULL DEFAULT 0,  -- 1=已用次日 open 填充 PnL
        exit_pnl_pct    DECIMAL(8,4) DEFAULT NULL,      -- 优先用 actual_exit_price 算
        notified        TINYINT(1)   NOT NULL DEFAULT 0,      -- 推送状态（避免重发）
        created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_strat_code_signal (strategy, code, signal_date),
        KEY idx_status_real (status, is_real),
        KEY idx_strategy (strategy)
    ) ENGINE=InnoDB
    """,

    # 4. 形态命中事件 + 后续走势（pattern_tracker.py 维护）
    """
    CREATE TABLE IF NOT EXISTS pattern_outcome (
        id             BIGINT       AUTO_INCREMENT PRIMARY KEY,
        strategy       VARCHAR(64)  NOT NULL,
        code           VARCHAR(16)  NOT NULL,
        name           VARCHAR(32)  NOT NULL DEFAULT '',
        signal_date    DATE         NOT NULL,
        signal_type    VARCHAR(16)  NOT NULL,                  -- BUY / WATCH
        signal_reason  VARCHAR(255) DEFAULT NULL,
        confidence     DOUBLE       NOT NULL DEFAULT 0,
        -- 入场基线（信号日次交易日开盘价；若 stock_daily 无次日数据则 NULL）
        buy_price      DECIMAL(10,3) DEFAULT NULL,
        buy_date       DATE         DEFAULT NULL,
        -- 后续 N 个交易日收益（基于 buy_price 后第 5/10/30/60 个交易日 close）
        ret_5d         DECIMAL(8,4) DEFAULT NULL,
        ret_10d        DECIMAL(8,4) DEFAULT NULL,
        ret_30d        DECIMAL(8,4) DEFAULT NULL,
        ret_60d        DECIMAL(8,4) DEFAULT NULL,
        -- 60 交易日内极值（基于 buy_price）
        peak_ret       DECIMAL(8,4) DEFAULT NULL,
        trough_ret     DECIMAL(8,4) DEFAULT NULL,
        peak_date      DATE         DEFAULT NULL,
        trough_date    DATE         DEFAULT NULL,
        -- 跟踪进度
        bars_seen      INT          NOT NULL DEFAULT 0,        -- buy_date 后已观察到的 bar 数
        status         VARCHAR(20)  NOT NULL DEFAULT 'pending',-- pending/partial/completed/no_data
        updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_strat_code_date (strategy, code, signal_date),
        KEY idx_strategy (strategy),
        KEY idx_signal_date (signal_date),
        KEY idx_status (status)
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
            # 增量迁移：backtest_results 新增 data_source 列
            await cur.execute(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='backtest_results' "
                "AND COLUMN_NAME='data_source'",
                (DB_CONFIG["db"],),
            )
            (cnt3,) = await cur.fetchone()
            if cnt3 == 0:
                await cur.execute(
                    "ALTER TABLE backtest_results ADD COLUMN data_source VARCHAR(32) NOT NULL DEFAULT 'cache' AFTER is_real"
                )
                logger.info("[MySQL] backtest_results 新增 data_source 列")


async def close_pool():
    global _pool
    if _pool and not _pool._closed:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
        logger.info("[MySQL] 连接池已关闭")
