"""
回填 stock_daily 最近几天缺失的数据（用 akshare 单股逐只拉）
仅用于补 daily_data_update cron 中断造成的零散缺口。
"""
import asyncio
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# bypass proxy
for _k in ['http_proxy','https_proxy','HTTP_PROXY','HTTPS_PROXY','all_proxy','ALL_PROXY']:
    os.environ.pop(_k, None)

from db.stock_dao import upsert_daily_batch
from db.mysql_pool import get_pool, close_pool


def fetch_one(code: str, start_str: str, end_str: str) -> list[dict]:
    """单股拉历史 akshare → 标准化 rows"""
    try:
        import akshare as ak
        df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                 start_date=start_str, end_date=end_str, adjust="qfq")
        if df is None or len(df) == 0:
            return []
        rows = []
        for _, r in df.iterrows():
            try:
                rows.append({
                    'code':          code,
                    'trade_date':    str(r['日期']),
                    'open_price':    float(r['开盘']),
                    'high':          float(r['最高']),
                    'low':           float(r['最低']),
                    'close':         float(r['收盘']),
                    'volume':        int(r['成交量']),
                    'amount':        float(r['成交额']),
                    'pct_change':    float(r['涨跌幅']),
                    'turnover_rate': float(r['换手率']),
                })
            except Exception:
                continue
        return rows
    except Exception as e:
        return []


async def main(codes: list[str], start: str, end: str, workers: int = 8):
    start_str = start.replace('-','')
    end_str = end.replace('-','')
    print(f"[1/2] 并发拉取 {len(codes)} 只股票 {start} ~ {end}（{workers} workers）...")
    loop = asyncio.get_event_loop()
    pool_exec = ThreadPoolExecutor(max_workers=workers)
    sem = asyncio.Semaphore(workers)
    all_rows: list[dict] = []
    ok_count = fail_count = 0

    async def _one(code, idx):
        nonlocal ok_count, fail_count
        async with sem:
            rows = await loop.run_in_executor(pool_exec, fetch_one, code, start_str, end_str)
            if rows:
                all_rows.extend(rows)
                ok_count += 1
            else:
                fail_count += 1
            if (idx + 1) % 20 == 0 or (idx + 1) == len(codes):
                print(f"  进度 {idx+1}/{len(codes)} 成功={ok_count} 失败={fail_count} 累计={len(all_rows)}行")

    tasks = [_one(c, i) for i, c in enumerate(codes)]
    await asyncio.gather(*tasks)
    pool_exec.shutdown(wait=False)

    if all_rows:
        print(f"[2/2] 批量入库 {len(all_rows)} 行...")
        n = await upsert_daily_batch(all_rows)
        print(f"  入库 {n} 行（INSERT IGNORE）")
    print(f"完成：成功 {ok_count}/{len(codes)}，新增 {len(all_rows)} 行")


async def _from_pattern_outcome_codes():
    """从 pattern_outcome 拿独有 code"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT DISTINCT code FROM pattern_outcome ORDER BY code")
            rows = await cur.fetchall()
    return [r[0] for r in rows]


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2026-05-14')
    ap.add_argument('--end',   default='2026-05-20')
    ap.add_argument('--source', default='pattern_outcome',
                    choices=['pattern_outcome', 'all'])
    ap.add_argument('--workers', type=int, default=8)
    args = ap.parse_args()

    async def run():
        if args.source == 'pattern_outcome':
            codes = await _from_pattern_outcome_codes()
        else:
            pool = await get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT code FROM stock_basic")
                    codes = [r[0] for r in await cur.fetchall()]
        print(f"目标 code 数: {len(codes)}")
        await main(codes, args.start, args.end, args.workers)
        await close_pool()

    asyncio.run(run())
