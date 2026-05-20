"""
P1-A：dump 4 折 OOS 全部交易（含 buy_meta），用于入场信号质量分析。

每个 fold 跑一次 OOS 回测，使用 tight_trail+lock_floor+pyramid 加固参数（与 walk-forward 一致），
保存 trades_paired 到 logs/p1_oos_fold{N}.json。
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


PARAMS = {
    'atr_stop_k': 2.0, 'atr_trail_k': 2.0,
    'min_watch_days': 10, 'vol_ratio_percentile': 0.85,
    'trail_stage2_gain': 0.05, 'trail_stage2_k': 1.5,
    'trail_stage3_gain': 0.15, 'trail_stage3_k': 1.0,
    'lock_floor_t1_gain': 0.10, 'lock_floor_t1_pct': 0.03,
    'lock_floor_t2_gain': 0.20, 'lock_floor_t2_pct': 0.10,
    'lock_floor_t3_gain': 0.35, 'lock_floor_t3_pct': 0.22,
    'pyramid_require_highest_gain': 0.05,
}

FOLDS = [
    (1, '2025-01-01', '2025-06-30'),
    (2, '2025-04-01', '2025-09-30'),
    (3, '2025-07-01', '2025-12-31'),
    (4, '2025-10-01', '2026-03-31'),
]


async def run_fold(idx, start, end):
    print(f"\n=== Fold {idx} OOS  {start} ~ {end} ===")
    result = await run_for_web(
        strategy_name='major_capital_accumulation',
        start=start, end=end,
        cash=100000.0, log_fn=None,
        screen_preset='default',
        data_source='local_db',
        extra_params=PARAMS,
    )
    m = result.get('metrics', {})
    print(f"  指标: ret={m.get('total_return')} sharpe={m.get('sharpe_ratio')} "
          f"win={m.get('win_rate')} trades={m.get('total_trades')}")
    out = {
        'fold': idx,
        'period': f'{start}~{end}',
        'metrics': m,
        'trades_paired': result.get('trades_paired', []),
    }
    out_path = ROOT / 'logs' / f'p1_oos_fold{idx}.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"  [保存] {out_path}  trades={len(out['trades_paired'])}")


async def main():
    for idx, s, e in FOLDS:
        await run_fold(idx, s, e)
    await close_pool()
    print("\n所有 fold 完成")


if __name__ == '__main__':
    asyncio.run(main())
