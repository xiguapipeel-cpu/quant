"""
系统配置 - 监控股票列表、数据源、搜索引擎
"""

# ============================================================
# 监控股票列表（10只）
# ============================================================
WATCHLIST = [
    {"code": "600519", "name": "贵州茅台", "market": "SH"},
    {"code": "300750", "name": "宁德时代", "market": "SZ"},
    {"code": "600036", "name": "招商银行", "market": "SH"},
    {"code": "002594", "name": "比亚迪",   "market": "SZ"},
    {"code": "601318", "name": "中国平安", "market": "SH"},
    {"code": "000858", "name": "五粮液",   "market": "SZ"},
    {"code": "002371", "name": "北方华创", "market": "SZ"},
    {"code": "601088", "name": "中国神华", "market": "SH"},
    {"code": "603288", "name": "海天味业", "market": "SH"},
    {"code": "300760", "name": "迈瑞医疗", "market": "SZ"},
]

# ============================================================
# 数据源配置（23个）
# ============================================================
DATA_SOURCES = {
    # --- 行情数据源 ---
    "eastmoney":   {"name": "东方财富",  "type": "market",     "priority": 1, "enabled": True},
    "akshare":     {"name": "AKShare",   "type": "market",     "priority": 1, "enabled": True},
    "sina":        {"name": "新浪财经",  "type": "market",     "priority": 2, "enabled": True},
    "tonghuashun": {"name": "同花顺",    "type": "market",     "priority": 2, "enabled": True},

    # --- 基本面数据源 ---
    "lixinger":    {"name": "理杏仁",    "type": "fundamental","priority": 1, "enabled": True},
    "yinianwang":  {"name": "亿牛网",    "type": "fundamental","priority": 2, "enabled": True},
    "sohu_finance":{"name": "搜狐财经",  "type": "fundamental","priority": 2, "enabled": True},
    "yahoo":       {"name": "Yahoo Finance","type": "international","priority": 2, "enabled": True},
    "choice":      {"name": "Choice数据","type": "fundamental", "priority": 2, "enabled": True},

    # --- 研报数据源 ---
    "dongwu":      {"name": "东吴证券研报","type": "report",   "priority": 3, "enabled": True},
    "cj_research": {"name": "中金研报",  "type": "report",     "priority": 3, "enabled": True},
    "guotai":      {"name": "国泰君安",  "type": "report",     "priority": 3, "enabled": True},
    "shenwanyh":   {"name": "申万宏源",  "type": "report",     "priority": 3, "enabled": True},
    "zhaoshang":   {"name": "招商证券",  "type": "report",     "priority": 3, "enabled": True},
    "huatai":      {"name": "华泰证券",  "type": "report",     "priority": 3, "enabled": True},
    "citic":       {"name": "中信证券",  "type": "report",     "priority": 3, "enabled": True},

    # --- 公告/监管数据源 ---
    "cninfo":      {"name": "巨潮资讯",  "type": "announcement","priority": 2, "enabled": True},
    "sse":         {"name": "上交所官网","type": "announcement","priority": 2, "enabled": True},
    "szse":        {"name": "深交所官网","type": "announcement","priority": 2, "enabled": True},
    "csrc":        {"name": "证监会EDGAR","type": "regulatory","priority": 3, "enabled": True},

    # --- 社区/综合数据源 ---
    "xueqiu":      {"name": "雪球网",    "type": "community",  "priority": 3, "enabled": True},
    "wind":        {"name": "万得Wind",  "type": "terminal",   "priority": 1, "enabled": False},  # 需付费授权
    "bloomberg":   {"name": "Bloomberg", "type": "international","priority": 1,"enabled": False},  # 需付费授权
}

# ============================================================
# MCP搜索引擎配置（5个，带故障切换链）
# ============================================================
SEARCH_ENGINES = [
    {
        "name":    "tavily",
        "display": "Tavily",
        "api_key_env": "TAVILY_API_KEY",
        "max_retries": 1,
        "timeout":  10,
        "priority": 1,
    },
    {
        "name":    "serper",
        "display": "Serper",
        "api_key_env": "SERPER_API_KEY",
        "max_retries": 1,
        "timeout":  10,
        "priority": 2,
    },
    {
        "name":    "firecrawl",
        "display": "Firecrawl",
        "api_key_env": "FIRECRAWL_API_KEY",
        "max_retries": 1,
        "timeout":  15,
        "priority": 3,
    },
    {
        "name":    "jina",
        "display": "Jina",
        "api_key_env": "JINA_API_KEY",
        "max_retries": 1,
        "timeout":  10,
        "priority": 4,
    },
    {
        "name":    "feishu",
        "display": "飞书搜索",
        "api_key_env": "FEISHU_APP_SECRET",
        "max_retries": 1,
        "timeout":  12,
        "priority": 5,
    },
]

# ============================================================
# 扫描时间配置
# ============================================================
SCAN_CONFIG = {
    "pre_market":  {"hour": 9,  "minute": 0,  "label": "盘前"},
    "mid_market":  {"hour": 11, "minute": 0,  "label": "盘中"},
    "post_market": {"hour": 15, "minute": 30, "label": "盘后"},
    "mid_interval_minutes": 15,  # 盘中扫描间隔
}

# ============================================================
# 数据完整性自检配置
# ============================================================
INTEGRITY_FIELDS = ["price", "pe", "market_cap", "recent_events"]
# 四项全有才算通过，缺一不可

# PE交叉验证容忍误差（超过此误差需人工复核）
PE_CROSS_TOLERANCE = 0.05  # 5%

# 每个数据点至少需要几个独立来源确认
MIN_SOURCES_FOR_CONFIRMATION = 2

# 搜索引擎最大重试次数（3次全失败才排除）
MAX_SEARCH_RETRIES = 3
