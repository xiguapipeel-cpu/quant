"""
P2-A：在新默认（tight_trail + F=False）之上加硬性入场过滤
   signal_a_min_rsi = 55  AND  signal_a_min_ma_diverge_pct = 0

直接跑 4 折 OOS，对比 VAR_A baseline（已知 +12.29%/+12.31%）。
"""
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.bt_runner import run_for_web
from db.mysql_pool import close_pool


# 新默认已包含 tight_trail + F=False，本次只需附加质量过滤
P2A = {
    'signal_a_min_rsi': 55.0,
    'signal_a_min_ma_diverge_pct': 0.0,
}

FOLDS = [
    (1, '2025-01-01', '2025-06-30'),
    (2, '2025-04-01', '2025-09-30'),
    (3, '2025-07-01', '2025-12-31'),
    (4, '2025-10-01', '2026-03-31'),
]


async def run_one(fold, start, end, params):
    print(f"\n--- Fold {fold} {start}~{end} ---")
    result = await run_for_web(
        strategy_name='major_capital_accumulation',
        start=start, end=end,
        cash=100000.0, log_fn=None,
        screen_preset='default',
        data_source='local_db',
        extra_params=params,
    )
    m = result.get('metrics', {})
    print(f"  ret={m.get('total_return')} sharpe={m.get('sharpe_ratio')} "
          f"win={m.get('win_rate')} trades={m.get('total_trades')}")
    return {
        'fold': fold, 'period': f'{start}~{end}',
        'metrics': m,
        'trades_paired': result.get('trades_paired', []),
    }


async def main():
    print(f'P2A overrides: {P2A}')
    results = []
    for idx, s, e in FOLDS:
        r = await run_one(idx, s, e, P2A)
        results.append(r)
    out = ROOT / 'logs' / 'p2a_filter.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'overrides': P2A,
            'baseline_VAR_A': {
                'fold1': {'ret': '-5.61%', 'sharpe': -1.89, 'trades': 6},
                'fold2': {'ret': '+8.32%', 'sharpe': 0.39, 'trades': 17},
                'fold3': {'ret': '+9.60%', 'sharpe': 0.48, 'trades': 14},
                'fold4': {'ret': '+0.00%', 'sharpe': 0.0, 'trades': 0},
                'cum': '+12.31%',
            },
            'results': results,
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
