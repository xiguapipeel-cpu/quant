"""
迷你回测 debug：用实际 Backtrader 跑 5 只股票，打印每日详细状态
"""
import asyncio, sys
from pathlib import Path
from datetime import datetime as _dt, date

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import backtrader as bt
from backtest.bt_major_capital import MajorCapitalBT, load_all_db_data, get_all_db_stocks

START       = "2025-01-01"
END         = "2026-04-09"
WARMUP      = "2024-01-01"
CASH        = 1_000_000
TEST_CODES  = ["000001", "600276", "601187", "603515", "000858"]


class DebugStrategy(MajorCapitalBT):
    """在 MajorCapitalBT 基础上添加逐日打印"""

    def next(self):
        # 只打印 2025 年之后的内容
        cur_date = self.datetime.date(0)

        # 调用父类 next 前先记录每只股票的状态
        for d in self.datas:
            name = d._name
            if name in self.order_dict:
                continue
            state = self.stock_state.get(name, {})
            ind   = self.indicators.get(name, {})
            if not ind:
                continue
            if len(d) < 60:
                continue

            # 只在 2025 年后打印
            if cur_date.year < 2025:
                continue

            # 只打印有建仓计数变化的
            acc = state.get('accumulation_days', 0)

            # 计算关键指标
            try:
                close   = float(d.close[0])
                rsi     = float(ind['rsi'][0]) if ind.get('rsi') else float('nan')
                ma20    = float(ind['ma20'][0]) if ind.get('ma20') else float('nan')
                ma60    = float(ind['ma60'][0]) if ind.get('ma60') else float('nan')

                # near_low
                lookback = 60
                start_i  = max(0, len(d) - lookback)
                lows = [float(d.low[-j]) for j in range(min(lookback, len(d)))]
                lo = min(lows) if lows else None
                nl = (close - lo) / lo * 100 if lo and lo > 0 else None

                # MA convergence
                ma5 = float(ind['ma5'][0]) if ind.get('ma5') else float('nan')
                ma10= float(ind['ma10'][0]) if ind.get('ma10') else float('nan')
                if ma5==ma5 and ma10==ma10 and ma20==ma20:
                    mid = (ma5+ma10+ma20)/3
                    conv = max(abs(ma5-mid),abs(ma10-mid),abs(ma20-mid))/mid*100 if mid>0 else None
                else:
                    conv = None

                if acc > 0 or (nl is not None and nl <= 25 and conv is not None and conv <= 6):
                    print(f"  {cur_date} {name:8s} acc={acc:3d} near_low={nl:.1f}% conv={conv:.2f}% rsi={rsi:.1f} MA20={ma20:.2f} MA60={ma60:.2f}")
            except Exception as e:
                pass

        # 调用父类
        super().next()


async def main():
    print(f"加载DB数据 {WARMUP}~{END} ...")
    db_data = await load_all_db_data(TEST_CODES, WARMUP, END)
    print(f"加载到 {len(db_data)} 只股票")
    for code, df in db_data.items():
        print(f"  {code}: {len(df)} bars  {df.index[0].date()} ~ {df.index[-1].date()}")

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(CASH)
    cerebro.broker.setcommission(commission=0.0003)

    for code, df in db_data.items():
        data = bt.feeds.PandasData(
            dataname=df, name=code, datetime=None,
            open='open', high='high', low='low', close='close',
            volume='volume', openinterest=-1,
        )
        cerebro.adddata(data)

    trade_start = _dt.strptime(START, '%Y-%m-%d').date()
    cerebro.addstrategy(
        DebugStrategy,
        max_positions=3,
        position_pct=0.33,
        screen_enabled=True,   # 开启选股过滤，模拟真实回测
        trade_start_date=trade_start,
    )

    print(f"\n--- 运行回测 (trade_start_date={trade_start}) ---\n")
    results = cerebro.run()
    strat = results[0]

    print(f"\n=== 交易记录 ===")
    for t in strat.trade_log:
        print(f"  {t}")

    print(f"\n最终资产: {cerebro.broker.getvalue():.0f}")


if __name__ == '__main__':
    asyncio.run(main())
