"""
补拉 2022-01-01 ~ 2024-04-08 缺口历史数据（单进程，每500只重登录防超时）
用法: python3 -m scripts.fetch_history_gap
"""
import sys, time, asyncio, signal
sys.path.insert(0, '/Users/zhuzhu/Documents/quant_system')

class _Timeout(Exception): pass

def _timeout_handler(sig, frame):
    raise _Timeout()

def query_with_timeout(bs, code, start, end, seconds=15):
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(seconds)
    try:
        rs = bs.query_history_k_data_plus(
            code,
            'date,open,high,low,close,volume,amount,turn,pctChg',
            start_date=start, end_date=end,
            frequency='d', adjustflag='3'
        )
        rows = []
        while rs.error_code == '0' and rs.next():
            r = rs.get_row_data()
            if r[4]:
                rows.append(r)
        return rows
    finally:
        signal.alarm(0)

import baostock as bs
import pymysql
from db.mysql_pool import DB_CONFIG, get_pool

START = '2018-01-01'
END   = '2021-12-31'
RESUME_FROM = 0   # 从头开始

def get_conn():
    return pymysql.connect(
        host=DB_CONFIG['host'], port=int(DB_CONFIG['port']),
        user=DB_CONFIG['user'], password=DB_CONFIG['password'],
        database=DB_CONFIG['db'], charset=DB_CONFIG.get('charset', 'utf8mb4'),
        connect_timeout=10,
    )

def bs_code(code):
    return ('sh.' if code.startswith('6') else 'sz.') + code

async def get_stock_list():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                'SELECT DISTINCT code FROM stock_daily ORDER BY code'
            )
            rows = await cur.fetchall()
            return [r[0] for r in rows]

def main():
    lg = bs.login()
    print(f'[BaoStock] {lg.error_code} {lg.error_msg}', flush=True)
    if lg.error_code != '0':
        raise RuntimeError('BaoStock 登录失败')

    stocks = asyncio.run(get_stock_list())
    print(f'[股票列表] 共 {len(stocks)} 只，拉取范围 {START} ~ {END}', flush=True)

    conn = get_conn()
    cursor = conn.cursor()
    inserted = 0
    errors = 0
    t0 = time.time()

    for i, code in enumerate(stocks):
        if i < RESUME_FROM:
            continue   # 跳过已处理的
        try:
            raw = query_with_timeout(bs, bs_code(code), START, END, seconds=15)
            rows = []
            for r in raw:
                rows.append((
                    code, r[0],
                    float(r[1]) if r[1] else 0,
                    float(r[2]) if r[2] else 0,
                    float(r[3]) if r[3] else 0,
                    float(r[4]),
                    int(float(r[5])) if r[5] else 0,
                    float(r[6]) if r[6] else 0,
                    float(r[7]) if r[7] else 0,
                    float(r[8]) if r[8] else 0,
                ))
            if rows:
                cursor.executemany('''
                    INSERT IGNORE INTO stock_daily
                    (code, trade_date, open_price, high, low, close,
                     volume, amount, turnover_rate, pct_change)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ''', rows)
                conn.commit()
                inserted += len(rows)
        except Exception as e:
            errors += 1
            try:
                conn = get_conn()
                cursor = conn.cursor()
            except Exception:
                pass

        # 每200只打一次进度
        if (i + 1) % 200 == 0:
            elapsed = time.time() - t0
            speed = (i + 1) / elapsed * 60
            eta = (len(stocks) - i - 1) / (speed / 60) / 60
            print(f'[进度] {i+1}/{len(stocks)} 只 | 写入 {inserted:,} 条 | '
                  f'错误 {errors} 只 | 速度 {speed:.0f}只/分 | 剩余约 {eta:.0f}分钟',
                  flush=True)

        # 每500只重新登录防超时
        if (i + 1) % 500 == 0:
            bs.logout()
            time.sleep(2)
            lg = bs.login()
            print(f'[重登录] {lg.error_code} {lg.error_msg}', flush=True)

    bs.logout()
    cursor.close()
    conn.close()
    elapsed = (time.time() - t0) / 60
    print(f'[完成] 总写入 {inserted:,} 条，错误 {errors} 只，耗时 {elapsed:.1f} 分钟', flush=True)

if __name__ == '__main__':
    main()
