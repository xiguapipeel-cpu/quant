"""
P0 修复实验：仅修改 trail stop 参数，跑同一 fold 对照。

基线（baseline）= Fold 3 OOS 已知结果（sharpe=-2.31 ret=-3.81% 胜率 30%）。
本次（tight_trail）= 反转 trail stop 哲学：越赚越紧。
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


# 基础参数 = Fold 3 训练选中的 best_params
BASE = {
    'atr_stop_k': 2.0,
    'min_watch_days': 10,
    'trail_stage2_gain': 0.15,
    'vol_ratio_percentile': 0.85,
}

# 仅 trail 收紧（其他不动）
TIGHT_TRAIL = {
    **BASE,
    'atr_trail_k': 2.0,           # 原 3.0 → 2.0（stage1 收紧）
    'trail_stage2_gain': 0.05,    # 原 0.15 → 0.05（更早启动 stage2）
    'trail_stage2_k': 1.5,        # 原 5.0 → 1.5（stage2 反向收紧）
    'trail_stage3_gain': 0.15,    # 原 0.30 → 0.15（更早启动 stage3）
    'trail_stage3_k': 1.0,        # 原 7.0 → 1.0（stage3 锁利）
}


async def run(label: str, params: dict) -> dict:
    print(f"\n=== {label} ===")
    print(f"params: {params}")
    result = await run_for_web(
        strategy_name='major_capital_accumulation',
        start='2025-07-01', end='2025-12-31',
        cash=100000.0, log_fn=None,
        screen_preset='default',
        data_source='local_db',
        extra_params=params,
    )
    m = result.get('metrics', {})
    print(f"  收益={m.get('total_return')} 夏普={m.get('sharpe_ratio')} "
          f"胜率={m.get('win_rate')} 交易={m.get('total_trades')} 最大回撤={m.get('max_drawdown')}")
    return result


async def main():
    res_tight = await run('TIGHT_TRAIL（trail 反向收紧）', TIGHT_TRAIL)
    out = {
        'period': '2025-07-01 ~ 2025-12-31 (Fold 3 OOS)',
        'baseline_known': {  # 已有结果，避免重跑基线
            'total_return': '-3.81%', 'sharpe_ratio': -2.31,
            'win_rate': '30.0%', 'total_trades': 13, 'max_drawdown': '10.36%',
        },
        'tight_trail_params': TIGHT_TRAIL,
        'tight_trail_metrics': res_tight.get('metrics'),
        'tight_trail_trades': res_tight.get('trades_paired', []),
    }
    out_path = ROOT / 'logs' / 'p0_trail_compare.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out_path}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
