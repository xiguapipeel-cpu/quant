"""
P9b：用极度宽松参数测试 2020 H2 V 复苏大牛是否能交易
若仍 0 trades → 不是过滤太紧，是 WATCH 阶段 / 数据层结构问题
若有交易 → 当前默认对 2020-2023 过严，校准只匹配 2025
"""
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.bt_runner import run_for_web
from db.mysql_pool import close_pool


# 测试 3 个组合
VARIANTS = {
    'current_default':  {},   # 现默认
    'pre_P0_legacy': {        # 完全回退到原始基线（前期 P0 之前的默认）
        'enable_signal_f': True,
        'signal_a_min_rsi': 0.0,
        'signal_a_break_high_lookback': 0,
        'atr_trail_k': 3.0,
        'trail_stage2_gain': 0.15,
        'trail_stage2_k': 5.0,
        'trail_stage3_gain': 0.30,
        'trail_stage3_k': 7.0,
    },
    'super_loose': {          # 关掉所有"质量"过滤
        'enable_signal_f': True,
        'signal_a_min_rsi': 0.0,
        'signal_a_break_high_lookback': 0,
        'market_filter': False,         # 关 L1
        'min_watch_days': 5,            # WATCH 大降
    },
}


async def run_one(label, start, end, params):
    print(f"\n=== {label} | {start}~{end} ===")
    print(f"   params: {params or '(use defaults)'}")
    result = await run_for_web(
        strategy_name='major_capital_accumulation',
        start=start, end=end,
        cash=100000.0, log_fn=None,
        screen_preset='default',
        data_source='local_db',
        extra_params=params,
    )
    m = result.get('metrics', {})
    print(f"   ret={m.get('total_return')} sharpe={m.get('sharpe_ratio')} "
          f"win={m.get('win_rate')} trades={m.get('total_trades')}")


async def main():
    PERIOD = ('2020-07-01', '2020-12-31')
    for label, params in VARIANTS.items():
        await run_one(label, *PERIOD, params)
    print('\n--- 对照：2025-07~12 (active zone known-active) ---')
    await run_one('current_default', '2025-07-01', '2025-12-31', {})
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
