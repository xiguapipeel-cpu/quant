"""
P2-A 拆解：rsi 过滤 vs mdiv 过滤独立测试，看哪个是真正起作用的。

baseline (P2-A 联合): rsi≥55 AND mdiv≥0  → +22.60%
RSI_ONLY              : rsi≥55,  mdiv off    → ?
MDIV_ONLY             : rsi off, mdiv≥0     → ?
NONE   (= VAR_A 已知) : 都 off              → +12.31%
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


# 当前默认 = P2-A（rsi=55 + mdiv=0），通过 extra_params 单独关闭一个
RSI_ONLY = {
    'signal_a_min_rsi': 55.0,
    'signal_a_min_ma_diverge_pct': -999.0,   # 关掉 mdiv 过滤
}

MDIV_ONLY = {
    'signal_a_min_rsi': 0.0,                 # 关掉 rsi 过滤
    'signal_a_min_ma_diverge_pct': 0.0,
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
    for label, params in [('RSI_ONLY', RSI_ONLY), ('MDIV_ONLY', MDIV_ONLY)]:
        print(f'\n{"="*60}\n{label}: {params}\n{"="*60}')
        for idx, s, e in FOLDS:
            r = await run_one(label, idx, s, e, params)
            results.append(r)
    out = ROOT / 'logs' / 'p2a_dissect.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'known_baselines': {
                'NONE_VAR_A': '+12.31% cumulative',  # 都不过滤
                'BOTH_P2A':   '+22.60% cumulative',  # 现默认
            },
            'results': results,
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
