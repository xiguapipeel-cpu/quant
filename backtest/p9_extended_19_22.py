"""
P9：扩展验证回测到 2020-2023 H1（7 个新 fold）
====================================================
idx_sh 已回填到 2018-01-02。补充 7 个新 fold 测试 baseline + P5_ATR15。

7 个新 fold 覆盖：
  - 2020 COVID V 反转
  - 2020 H2 全面复苏牛
  - 2021 H1 新能源高峰
  - 2021 H2 顶部 + 回落
  - 2022 H1 熊市启动
  - 2022 H2 熊市底部+反弹
  - 2023 H1 弱反弹

合并现有 P6 的 Fold 7-10（2025 活跃区）共 11 折做 bootstrap。
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


FOLDS = [
    ('B', '2020-01-01', '2020-06-30', 'COVID 暴跌+V 反转'),
    ('C', '2020-07-01', '2020-12-31', 'V 后复苏大牛'),
    ('D', '2021-01-01', '2021-06-30', '新能源/医药抱团高峰'),
    ('E', '2021-07-01', '2021-12-31', '抱团瓦解+小盘反弹'),
    ('F', '2022-01-01', '2022-06-30', '熊市初段+俄乌'),
    ('G', '2022-07-01', '2022-12-31', '熊市末段+稳增长'),
    ('H', '2023-01-01', '2023-06-30', '23 春季反弹+萎缩'),
]

VARIANTS = {
    'baseline': {},
    'P5_ATR15': {'breakout_atr_k': 1.5, 'vol_raw_percentile': 0.80},
}


async def run_one(label, fold_id, start, end, params):
    print(f"\n--- {label} | Fold {fold_id} {start}~{end} ---")
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
    return {'label': label, 'fold': fold_id, 'period': f'{start}~{end}',
            'metrics': m, 'trades_paired': result.get('trades_paired', [])}


async def main():
    results = []
    for label, params in VARIANTS.items():
        print(f'\n{"="*70}\n{label}: {params or "(use defaults)"}\n{"="*70}')
        for fold_id, s, e, note in FOLDS:
            print(f'\n  备注: {note}')
            r = await run_one(label, fold_id, s, e, params)
            results.append(r)
    out = ROOT / 'logs' / 'p9_extended_19_22.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'config': {
                'description': '7 pre-2023 folds for P5 validation',
                'folds': [(fid, s, e, n) for fid, s, e, n in FOLDS],
                'note': '合并 P6 logs/p6_extended_validation.json Fold 7-10（active）共 11 折做 bootstrap',
            },
            'variants': VARIANTS,
            'results': results,
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
