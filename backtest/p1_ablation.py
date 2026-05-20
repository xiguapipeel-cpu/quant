"""
P1-B Ablation：跑 4 折 OOS，对比两个变体 vs tight_trail baseline。

变体 A：关掉 enable_signal_f（去掉短期看着对、最终输的信号）
变体 B：在 A_breakout 上加质量过滤（rsi≥53 & ma_diverge_pct≥0）
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


BASE = {
    'atr_stop_k': 2.0, 'atr_trail_k': 2.0,
    'min_watch_days': 10, 'vol_ratio_percentile': 0.85,
    'trail_stage2_gain': 0.05, 'trail_stage2_k': 1.5,
    'trail_stage3_gain': 0.15, 'trail_stage3_k': 1.0,
    'lock_floor_t1_gain': 0.10, 'lock_floor_t1_pct': 0.03,
    'lock_floor_t2_gain': 0.20, 'lock_floor_t2_pct': 0.10,
    'lock_floor_t3_gain': 0.35, 'lock_floor_t3_pct': 0.22,
    'pyramid_require_highest_gain': 0.05,
}

VAR_A = {**BASE, 'enable_signal_f': False}
VAR_B = {**BASE, 'signal_a_min_rsi': 53.0, 'signal_a_min_ma_diverge_pct': 0.0}

FOLDS = [
    (1, '2025-01-01', '2025-06-30'),
    (2, '2025-04-01', '2025-09-30'),
    (3, '2025-07-01', '2025-12-31'),
    (4, '2025-10-01', '2026-03-31'),
]


async def run_one(label, fold, start, end, params):
    print(f"\n--- {label} | Fold {fold} {start}~{end} ---")
    result = await run_for_web(
        strategy_name='major_capital_accumulation',
        start=start, end=end,
        cash=100000.0, log_fn=None,
        screen_preset='default',
        data_source='local_db',
        extra_params=params,
    )
    m = result.get('metrics', {})
    print(f"  指标: ret={m.get('total_return')} sharpe={m.get('sharpe_ratio')} "
          f"win={m.get('win_rate')} trades={m.get('total_trades')}")
    return {
        'label': label, 'fold': fold, 'period': f'{start}~{end}',
        'metrics': m,
    }


async def main():
    results = []
    for label, params in [('VAR_A_no_F', VAR_A), ('VAR_B_filter_A', VAR_B)]:
        for idx, s, e in FOLDS:
            r = await run_one(label, idx, s, e, params)
            results.append(r)
    out = ROOT / 'logs' / 'p1_ablation.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({'baseline_tight': {
            'note': '已知 4 折 OOS（tight_trail）',
            'fold1': {'ret': '-5.6%', 'sharpe': -1.89, 'trades': 6},
            'fold2': {'ret': '-3.2%', 'sharpe': -2.55, 'trades': 19},
            'fold3': {'ret': '+5.8%', 'sharpe': 0.13,  'trades': 15},
            'fold4': {'ret': '-4.8%', 'sharpe': -1.82, 'trades': 4},
            'cum_ret_pp': -7.84,
        }, 'results': results}, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
