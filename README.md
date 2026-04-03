# A股量化交易系统

多源交叉验证 · 三次扫描 · 数据完整性自检 · 零猜测决策

---

## 系统架构

```
quant_system/
├── main.py                    # 主入口
├── requirements.txt
├── .env.example               # API Key模板
├── config/
│   └── settings.py            # 股票列表、数据源、引擎配置
├── core/
│   ├── analyzer.py            # 主分析编排（并行采集→验证→自检）
│   ├── cross_validator.py     # 交叉验证 + 完整性自检
│   └── scheduler.py           # 盘前/盘中/盘后三次扫描调度器
├── data/
│   └── fetcher.py             # AKShare + 东方财富API采集
├── engines/
│   └── search_engine.py       # 多引擎搜索（Tavily→Serper→Jina，最多3次）
├── reports/
│   └── report_generator.py    # 结构化报告生成（文本+JSON）
└── utils/
    └── logger.py              # 日志工具
```

---

## 快速开始

### 1. 安装依赖

```bash
cd quant_system
pip install -r requirements.txt
```

### 2. 配置API Key

```bash
cp .env.example .env
# 编辑 .env，填入至少一个搜索引擎Key
# 推荐优先配置：TAVILY_API_KEY（免费额度充足）
```

### 3. 运行方式

```bash
# 立即手动分析（测试用）
python main.py

# 指定扫描类型
python main.py pre    # 盘前扫描
python main.py mid    # 盘中扫描
python main.py post   # 盘后扫描

# 启动自动调度（09:00/11:00/15:30 自动触发）
python main.py schedule
```

启动 Web 看板
# 方式1：直接启动（推荐开发用）
uvicorn web.app:app --reload --port 8000

# 方式2：从项目根目录启动
python -m uvicorn web.app:app --reload --port 8000


---

## 五大核心原则

### 原则1：多源交叉验证
每个数据点至少2个独立来源确认。

| 股票 | 来源1 | 数值1 | 来源2 | 数值2 | 结论 |
|------|-------|-------|-------|-------|------|
| 茅台PE | 理杏仁 | 20.40 | Yahoo | 20.70 | ✓ 一致 |
| 宁德PE | 理杏仁 | 26.14 | 搜狐 | 26.37 | ✓ 一致 |

误差 > 5% 时自动标记「需人工复核」，不自动决策。

### 原则2：搜索失败必须重试
```
Tavily → 失败 → Serper → 失败 → Jina → 失败 → 标记数据缺失
（最多3次，全失败才排除，不提前放弃）
```

### 原则3：空数据禁止进决策
```python
if not all([price, pe, market_cap, recent_events]):
    decision = "排除"  # 不猜测，不推断
```

排除示例：
- 北方华创：半导体龙头，但PE/市值三次搜索全缺 → 排除
- 中国神华：只有港股数据，无A股实时价格 → 排除

### 原则4：出报告前先自检

```
股价  ✓/✗
PE   ✓/✗
市值 ✓/✗
近期事件 ✓/✗
→ 四项全有才输出报告，缺一不可
```

### 原则5：决策必须基于事实
报告中禁止出现：「众所周知」「一般认为」「市场普遍预期」
每个结论必须标注：数据来源 + 数值 + 来源时间

---

## 数据源清单（23个）

| 类型 | 数据源 |
|------|--------|
| 行情 | 东方财富、AKShare、新浪财经、同花顺 |
| 基本面 | 理杏仁、亿牛网、搜狐财经、Yahoo Finance、Choice数据 |
| 研报 | 东吴证券、中金、国泰君安、申万宏源、招商、华泰、中信 |
| 公告 | 巨潮资讯、上交所、深交所、证监会EDGAR |
| 社区 | 雪球网 |
| 付费(可选) | 万得Wind、Bloomberg |

## 搜索引擎（5个，自动故障切换）

| 优先级 | 引擎 | ENV变量 |
|--------|------|---------|
| 1 | Tavily | TAVILY_API_KEY |
| 2 | Serper | SERPER_API_KEY |
| 3 | Firecrawl | FIRECRAWL_API_KEY |
| 4 | Jina | JINA_API_KEY |
| 5 | 飞书 | FEISHU_APP_SECRET |

---

## 扫描时间表

| 时间 | 类型 | 任务 |
|------|------|------|
| 09:00 | 盘前 | 全量数据采集 + 完整性自检 + 晨报输出 |
| 09:30~14:57 | 盘中 | 每15分钟增量更新 + 异常波动预警 |
| 15:30 | 盘后 | 全天复盘 + 明日候选股预筛 |

---

## 修改监控股票

编辑 `config/settings.py` 中的 `WATCHLIST`：

```python
WATCHLIST = [
    {"code": "600519", "name": "贵州茅台", "market": "SH"},
    {"code": "300750", "name": "宁德时代", "market": "SZ"},
    # ... 最多建议20只，过多会导致并发请求超限
]
```
