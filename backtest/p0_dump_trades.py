"""
P0 诊断：rerun 一个 fold 的 OOS 测试，dump trades_paired + buy_meta 详情。

目标：分析 13 笔真实交易的入场理由 vs 后续走势，找出为什么胜率只有 14.6%。
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


async def main():
    # Fold 3 OOS：训练段选中的最优参数
    params = {
        'atr_stop_k': 2.0,
        'min_watch_days': 10,
        'trail_stage2_gain': 0.15,
        'vol_ratio_percentile': 0.85,
    }
    result = await run_for_web(
        strategy_name='major_capital_accumulation',
        start='2025-07-01',
        end='2025-12-31',
        cash=100000.0,
        log_fn=print,
        screen_preset='default',
        data_source='local_db',
        extra_params=params,
    )
    out = {
        'period': '2025-07-01 ~ 2025-12-31 (Fold 3 OOS)',
        'params': params,
        'metrics': result.get('metrics'),
        'trades_paired': result.get('trades_paired', []),
    }
    out_path = ROOT / 'logs' / 'p0_trades_fold3_oos.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[P0] 已保存 {len(out['trades_paired'])} 笔交易 → {out_path}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
