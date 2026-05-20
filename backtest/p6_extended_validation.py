"""
P6：扩展 walk-forward 验证（2023 起 10 折）
================================================

目的：在更长历史（含 2023 熊市、2024-09 924 反弹、2025 多空切换）上验证：
  1. 当前默认配方（P0+P1+P2+P4）是否仍然稳定 → baseline
  2. P5 ATR1.5+V80 是否能在长历史上稳定 +3.53pp → P5_ATR15

10 折覆盖 OOS：2023-07-01 ~ 2026-03-31（约 33 个月）
  - 后 4 折与原 P4/P5 已知结果对照
  - 前 6 折是全新数据，结构性检验
"""
import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.bt_runner import run_for_web
from db.mysql_pool import close_pool


# 10 折定义：start=2022-07-01, train=12mo, test=6mo, step=3mo, end=2026-03-31
FOLDS = [
    (1,  '2023-07-01', '2023-12-31'),
    (2,  '2023-10-01', '2024-03-31'),
    (3,  '2024-01-01', '2024-06-30'),
    (4,  '2024-04-01', '2024-09-30'),
    (5,  '2024-07-01', '2024-12-31'),
    (6,  '2024-10-01', '2025-03-31'),
    (7,  '2025-01-01', '2025-06-30'),    # = 原 Fold 1
    (8,  '2025-04-01', '2025-09-30'),    # = 原 Fold 2
    (9,  '2025-07-01', '2025-12-31'),    # = 原 Fold 3
    (10, '2025-10-01', '2026-03-31'),    # = 原 Fold 4
]

VARIANTS = {
    'baseline':  {},  # 当前默认（P0+P1+P2+P4）
    'P5_ATR15':  {'breakout_atr_k': 1.5, 'vol_raw_percentile': 0.80},
}


async def run_one(label, fold, start, end, params):
    print(f"\n--- {label} | Fold {fold:>2} {start}~{end} ---")
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
          f"win={m.get('win_rate')} trades={m.get('total_trades')} mdd={m.get('max_drawdown')}")
    return {'label': label, 'fold': fold, 'period': f'{start}~{end}', 'metrics': m}


async def main():
    results = []
    for label, params in VARIANTS.items():
        print(f'\n{"="*70}\n{label}: {params or "(use defaults)"}\n{"="*70}')
        for fold, s, e in FOLDS:
            r = await run_one(label, fold, s, e, params)
            results.append(r)
    out = ROOT / 'logs' / 'p6_extended_validation.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'config': {
                'folds': len(FOLDS),
                'oos_span': f'{FOLDS[0][1]} ~ {FOLDS[-1][2]}',
                'overlap_with_p4_p5': '后 4 折（Fold 7-10）对应原 4 折',
            },
            'variants': VARIANTS,
            'results': results,
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
