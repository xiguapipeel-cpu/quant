"""
回填 idx_sh 历史（2018-01 ~ 2022-01）到 stock_daily 表，code='idx_sh'。
"""
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 移除代理（akshare 走直连）
for _k in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY',
          'all_proxy', 'ALL_PROXY']:
    os.environ.pop(_k, None)

from db.stock_dao import upsert_daily_batch
from db.mysql_pool import close_pool, get_pool


async def main():
    import akshare as ak
    print("[1/3] 拉取上证综指（新浪接口）...")
    df = ak.stock_zh_index_daily(symbol="sh000001")
    print(f"  总行数: {len(df)}, 范围: {df['date'].min()} ~ {df['date'].max()}")

    # 过滤 2018-01-01 ~ 2022-01-04（已有数据起点）
    import pandas as pd
    df['date'] = pd.to_datetime(df['date'])
    mask = (df['date'] >= '2018-01-01') & (df['date'] < '2022-01-04')
    df_fill = df[mask].copy()
    print(f"\n[2/3] 待回填: {len(df_fill)} 行（2018-01-01 ~ 2022-01-03）")

    # 转换为 upsert 所需 schema
    rows = []
    for _, r in df_fill.iterrows():
        rows.append({
            'code':          'idx_sh',
            'trade_date':    r['date'].date(),
            'open_price':    float(r['open']),
            'high':          float(r['high']),
            'low':           float(r['low']),
            'close':         float(r['close']),
            'volume':        float(r['volume']),
            'amount':        None,
            'pct_change':    None,
            'turnover_rate': None,
        })
    print(f"  转换完成，准备 upsert")

    print(f"\n[3/3] 批量入库...")
    n = await upsert_daily_batch(rows)
    print(f"  入库 {n} 行（INSERT IGNORE，已存在的跳过）")

    # 验证
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT MIN(trade_date), MAX(trade_date), COUNT(*) "
                "FROM stock_daily WHERE code='idx_sh'"
            )
            row = await cur.fetchone()
    print(f"\n  验证: idx_sh 现在 起={row[0]} 止={row[1]} 共 {row[2]} 行")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
