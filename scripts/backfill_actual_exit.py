"""
一次性回填：把已离场记录的 actual_exit_date / actual_exit_price 补上
（按信号日后第一个交易日的开盘价）
重新基于 actual 价格计算 exit_pnl_pct。
"""
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool, close_pool
from db.stock_dao import get_daily_history
from db.position_dao import fill_actual_exit, list_pending_actual_fill


STRATEGY_ID = "major_capital_accumulation"


async def main():
    pending = await list_pending_actual_fill(STRATEGY_ID)
    print(f"[backfill] 待填充 {len(pending)} 笔")

    n_filled = n_nodata = 0
    for i, pos in enumerate(pending, 1):
        sig_exit_date = str(pos['exit_date'])
        entry_price = float(pos['entry_price'])
        try:
            start = (date.fromisoformat(sig_exit_date) + timedelta(days=1)).isoformat()
            end = (date.fromisoformat(sig_exit_date) + timedelta(days=15)).isoformat()
            rows = await get_daily_history(pos['code'], start, end)
            next_bar = rows[0] if rows else None
            if next_bar:
                actual_date = str(next_bar['trade_date'])
                actual_price = float(next_bar['open_price'])
                actual_pnl = (actual_price - entry_price) / entry_price
                await fill_actual_exit(pos['id'], actual_date, actual_price, actual_pnl)
                n_filled += 1
            else:
                n_nodata += 1
        except Exception as e:
            n_nodata += 1
            if i % 500 == 0:
                print(f"  err sample: {pos['code']} {sig_exit_date} {e}")
        if i % 1000 == 0:
            print(f"  进度 {i}/{len(pending)} 填充={n_filled} 无数据={n_nodata}")

    print(f"[backfill] 完成: 填充={n_filled}, 无次日数据={n_nodata}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
