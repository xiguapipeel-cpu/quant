"""
P1-A 分析器：聚合 4 折 OOS 全部入场信号，输出每个 trigger / 特征区间的
胜率 / 5日收益 / 最终PnL 统计，定位"噪音入场"。
"""
import asyncio
import json
import statistics as st
import sys
from pathlib import Path
from collections import defaultdict
from datetime import timedelta, date

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.stock_dao import get_daily_history
from db.mysql_pool import close_pool


FOLD_FILES = [ROOT / 'logs' / f'p1_oos_fold{i}.json' for i in (1, 2, 3, 4)]


def parse_pct(s):
    if s is None: return 0.0
    if isinstance(s, (int, float)): return float(s)
    s = str(s).rstrip('%').strip('+')
    try: return float(s) / 100.0
    except: return 0.0


def classify_trigger(buy_reason: str) -> str:
    """从 buy_reason 字符串识别入场信号类型。"""
    if '加仓' in buy_reason: return 'PYRAMID'
    if '放量突破' in buy_reason: return 'A_breakout'
    if '量先萎缩后温和放大' in buy_reason or '缩量比' in buy_reason: return 'F_vol_pattern'
    return 'other'


async def fetch_5d_return(code: str, buy_date: str, buy_price: float) -> float | None:
    """查询买入后 5 个交易日的收盘价，计算 5d 收益。"""
    bd = date.fromisoformat(str(buy_date)[:10])
    end = bd + timedelta(days=15)  # 留余量覆盖周末/节假日
    rows = await get_daily_history(code, bd.isoformat(), end.isoformat())
    if not rows or len(rows) < 6:
        return None
    # rows[0] is buy_date itself; rows[5] is +5 trading days
    fifth = rows[5] if len(rows) > 5 else rows[-1]
    close5 = float(fifth['close'])
    return close5 / buy_price - 1.0


def pct_pos(values):
    return sum(1 for v in values if v > 0) / len(values) if values else 0.0


def fmt_summary(label, count, pos5d, pos_final, ret5d, retfin):
    return (f"{label:<22} n={count:<3}  "
            f"5d 胜={pos5d*100:>5.1f}%  avg={ret5d*100:+5.2f}%  "
            f"final 胜={pos_final*100:>5.1f}%  avg={retfin*100:+6.2f}%")


async def main():
    # 聚合所有 entry trades
    entries = []
    for fp in FOLD_FILES:
        if not fp.exists():
            print(f"⚠ 缺文件 {fp}"); continue
        data = json.load(open(fp, encoding='utf-8'))
        fold = data.get('fold', '?')
        for t in data.get('trades_paired', []):
            br = t.get('buy_reason') or ''
            trig = classify_trigger(br)
            if trig == 'PYRAMID':
                continue  # 只看新建仓
            bp = float(t.get('buy_price', 0))
            if bp <= 0: continue
            entries.append({
                'fold': fold,
                'code': t.get('code'),
                'buy_date': t.get('buy_date'),
                'buy_price': bp,
                'pnl_pct': float(t.get('pnl_pct', 0)) / 100.0,
                'trigger': trig,
                'buy_reason': br,
                'buy_meta': t.get('buy_meta') or {},
                'sell_reason': t.get('sell_reason', '') or '',
            })

    print(f'\n总入场数(去除加仓): {len(entries)}')
    by_fold = defaultdict(int)
    for e in entries: by_fold[e['fold']] += 1
    print(f'分布: {dict(by_fold)}\n')

    # 拉 5 日收益
    print('查询 5日后收盘价...')
    for e in entries:
        e['ret5d'] = await fetch_5d_return(e['code'], e['buy_date'], e['buy_price'])
    valid = [e for e in entries if e['ret5d'] is not None]
    print(f'  有效 5d 数据: {len(valid)}/{len(entries)}\n')

    # 按 trigger 聚合
    print('='*90)
    print('【按入场信号类型】')
    print('='*90)
    by_trig = defaultdict(list)
    for e in valid: by_trig[e['trigger']].append(e)
    print(fmt_summary('全部', len(valid),
                      pct_pos([e['ret5d'] for e in valid]),
                      pct_pos([e['pnl_pct'] for e in valid]),
                      st.mean([e['ret5d'] for e in valid]),
                      st.mean([e['pnl_pct'] for e in valid])))
    print()
    for trig, items in sorted(by_trig.items()):
        if not items: continue
        r5 = [e['ret5d'] for e in items]
        rf = [e['pnl_pct'] for e in items]
        print(fmt_summary(trig, len(items), pct_pos(r5), pct_pos(rf), st.mean(r5), st.mean(rf)))

    # 按 fold × trigger
    print('\n'+'='*90)
    print('【按 fold × trigger】')
    print('='*90)
    by_ft = defaultdict(list)
    for e in valid: by_ft[(e['fold'], e['trigger'])].append(e)
    for (fold, trig), items in sorted(by_ft.items()):
        r5 = [e['ret5d'] for e in items]
        rf = [e['pnl_pct'] for e in items]
        print(fmt_summary(f'F{fold} {trig}', len(items), pct_pos(r5), pct_pos(rf),
                          st.mean(r5), st.mean(rf)))

    # 按 buy_meta 关键特征分桶（只看有效数据）
    print('\n'+'='*90)
    print('【按特征区间分桶（5日收益 与 final PnL）】')
    print('='*90)
    feat_keys = ['rsi', 'near_low_pct', 'ma_converge_pct', 'yy_ratio', 'bb_narrow', 'ma_diverge_pct']
    for fk in feat_keys:
        vals = [(e['buy_meta'].get(fk), e['ret5d'], e['pnl_pct'])
                for e in valid if e['buy_meta'].get(fk) is not None]
        if len(vals) < 8: continue
        vals.sort(key=lambda x: x[0])
        n = len(vals)
        # 三分位
        thirds = [vals[:n//3], vals[n//3:2*n//3], vals[2*n//3:]]
        labels = [f'低 (<={vals[n//3-1][0]:.2f})',
                  f'中 ({vals[n//3][0]:.2f}~{vals[2*n//3-1][0]:.2f})',
                  f'高 (>={vals[2*n//3][0]:.2f})']
        print(f'\n  {fk}:')
        for lab, group in zip(labels, thirds):
            r5s = [g[1] for g in group]
            rfs = [g[2] for g in group]
            if not r5s: continue
            print(f'    {lab:<28} n={len(group):<3} 5d avg={st.mean(r5s)*100:+.2f}% '
                  f'final avg={st.mean(rfs)*100:+.2f}%  5d胜={pct_pos(r5s)*100:.0f}%')

    # 保存详细 entries 给后续
    out = ROOT / 'logs' / 'p1_signal_analysis.json'
    with open(out, 'w', encoding='utf-8') as f:
        # 去掉 buy_meta 里嵌套的不可序列化对象
        safe = []
        for e in valid:
            ec = dict(e)
            ec['buy_meta'] = {k: v for k, v in (e['buy_meta'] or {}).items()
                              if isinstance(v, (int, float, str, bool, type(None)))}
            safe.append(ec)
        json.dump({'entries': safe, 'count': len(safe)}, f, ensure_ascii=False, indent=2, default=str)
    print(f'\n[保存] {out}')

    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
