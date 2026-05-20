"""
P3：大盘环境过滤 ablation
===========================

当前默认（baseline）= MA20>MA60 单独过滤 → 已知 4 折 OOS +22.60%

新增可选过滤（默认 off，通过 extra_params 启用）：
  - MR_RSI    : MA20>MA60 + 指数 RSI ≥ 50
  - MR_BREADTH: MA20>MA60 + 涨跌家数比 ≥ 1.0
  - MR_ALL    : MA20>MA60 + RSI ≥ 50 + 涨跌家数比 ≥ 1.0

预期：在 Fold 1（4 月关税黑天鹅）和 Fold 4（弱市）削减低质量入场。
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


VARIANTS = {
    'MR_RSI':     {'market_rsi_min': 50.0, 'market_breadth_min': 0.0},
    'MR_BREADTH': {'market_rsi_min': 0.0,  'market_breadth_min': 1.0},
    'MR_ALL':     {'market_rsi_min': 50.0, 'market_breadth_min': 1.0},
}

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
    print(f"  ret={m.get('total_return')} sharpe={m.get('sharpe_ratio')} "
          f"win={m.get('win_rate')} trades={m.get('total_trades')}")
    return {'label': label, 'fold': fold, 'period': f'{start}~{end}', 'metrics': m}


async def main():
    results = []
    for label, params in VARIANTS.items():
        print(f'\n{"="*60}\n{label}: {params}\n{"="*60}')
        for idx, s, e in FOLDS:
            r = await run_one(label, idx, s, e, params)
            results.append(r)
    out = ROOT / 'logs' / 'p3_market_regime.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'baseline_known': {  # 当前默认（MA20>MA60 only）
                'fold1': {'ret': '-0.53%', 'trades': 1},
                'fold2': {'ret': '+13.64%', 'trades': 10},
                'fold3': {'ret': '+9.49%',  'trades': 9},
                'fold4': {'ret': '+0.00%',  'trades': 0},
                'cum': '+22.60%',
            },
            'variants': VARIANTS,
            'results': results,
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
