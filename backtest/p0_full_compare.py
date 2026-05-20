"""
P0 #2 实验：tight_trail（已验证）+ 分层锁利 + 加仓加固，跑同一 fold 对照。

baseline_tight = tight_trail（已知 +5.78% / 0.13 sharpe / 50% / 15 笔 / 4.25% MDD）
full_p0        = tight_trail + lock floor + pyramid_require_highest_gain
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


# tight_trail 配方（P0 #1 已验证）
TIGHT = {
    'atr_stop_k': 2.0,
    'min_watch_days': 10,
    'vol_ratio_percentile': 0.85,
    'atr_trail_k': 2.0,
    'trail_stage2_gain': 0.05,
    'trail_stage2_k': 1.5,
    'trail_stage3_gain': 0.15,
    'trail_stage3_k': 1.0,
}

# P0 #2 全套：tight_trail + 分层锁利 + 加仓加固
FULL = {
    **TIGHT,
    # 分层锁利：峰值达到 tier 后，止损不得低于 buy_price * (1 + pct)
    'lock_floor_t1_gain': 0.10, 'lock_floor_t1_pct': 0.03,   # 峰值≥10% 锁+3%
    'lock_floor_t2_gain': 0.20, 'lock_floor_t2_pct': 0.10,   # 峰值≥20% 锁+10%
    'lock_floor_t3_gain': 0.35, 'lock_floor_t3_pct': 0.22,   # 峰值≥35% 锁+22%
    # 加仓加固：要求峰值浮盈先到 stage2_gain（即 trail 已收紧）
    'pyramid_require_highest_gain': 0.05,
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
          f"胜率={m.get('win_rate')} 交易={m.get('total_trades')} 最大回撤={m.get('max_drawdown')} PF={m.get('profit_factor')}")
    return result


async def main():
    res_full = await run('FULL_P0（tight_trail + lock floor + pyramid 加固）', FULL)
    out = {
        'period': '2025-07-01 ~ 2025-12-31 (Fold 3 OOS)',
        'baseline_tight_trail': {  # 已知，避免重跑
            'total_return': '+5.78%', 'sharpe_ratio': 0.13,
            'win_rate': '50.0%', 'total_trades': 15,
            'max_drawdown': '4.25%', 'profit_factor': 3.16,
        },
        'full_p0_params': FULL,
        'full_p0_metrics': res_full.get('metrics'),
        'full_p0_trades': res_full.get('trades_paired', []),
    }
    out_path = ROOT / 'logs' / 'p0_full_compare.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out_path}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
