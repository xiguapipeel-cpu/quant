"""
P8：Bootstrap 置信区间 — 检验 P5 ATR1.5_V80 +3.53pp 是否统计显著

两种 bootstrap：
  1. 交易级 (trade-level)：把 baseline 18 笔、P5 16 笔的单笔 pnl_pct 当独立样本
                            有放回抽 N=各原始大小，1000 次迭代，计算 sum(P5)-sum(baseline) 的 CI
  2. 折级 (fold-level)：把 4 折 OOS 收益当 4 个独立 block，有放回抽 4 个
                          这种更严格，反映 regime 相关性，但只有 4 个块统计力弱

输出 95% CI 和 P(P5>baseline)；如 CI 不含 0 → P5 真显著。
"""
import json
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 加载数据
p4 = json.load(open(ROOT / 'logs' / 'p4_break_high.json'))
p5 = json.load(open(ROOT / 'logs' / 'p5_adaptive_threshold.json'))


def extract_trades(results, label):
    """返回 [(fold, pnl_pct), ...] 用于 trade-level bootstrap，过滤未平仓和加仓"""
    out = []
    for r in results:
        if r['label'] != label:
            continue
        fold = r['fold']
        for t in r.get('trades_paired', []):
            # 跳过未平仓
            sell_date = str(t.get('sell_date', '') or '')
            sell_reason = str(t.get('sell_reason', '') or '')
            if '持仓中' in sell_reason or '持仓中' in sell_date or not sell_date or sell_date in ('None', '（持仓中）'):
                continue
            # pnl_pct 是百分比形式（如 -8.84 表示 -8.84%）
            pnl = t.get('pnl_pct')
            if pnl is None:
                continue
            out.append((fold, float(pnl)))
    return out


def extract_fold_returns(results, label):
    """返回 [fold_return_pp, ...] 用于 fold-level bootstrap（百分点形式）"""
    fold_map = {}
    for r in results:
        if r['label'] != label:
            continue
        ret_str = str(r['metrics'].get('total_return', '0%')).rstrip('%').strip('+')
        try:
            fold_map[r['fold']] = float(ret_str)
        except:
            fold_map[r['fold']] = 0.0
    # 按 fold 排序返回
    return [fold_map[f] for f in sorted(fold_map.keys())]


def bootstrap_diff(samples_a, samples_b, n_iter=10000, agg='sum'):
    """有放回抽样，计算 sum(b) - sum(a) 的分布"""
    diffs = []
    for _ in range(n_iter):
        a_sample = [random.choice(samples_a) for _ in range(len(samples_a))]
        b_sample = [random.choice(samples_b) for _ in range(len(samples_b))]
        if agg == 'sum':
            diff = sum(b_sample) - sum(a_sample)
        elif agg == 'mean':
            diff = sum(b_sample)/len(b_sample) - sum(a_sample)/len(a_sample)
        diffs.append(diff)
    diffs.sort()
    return diffs


def ci(diffs, alpha=0.05):
    """返回 alpha/2 和 1-alpha/2 分位"""
    n = len(diffs)
    lo = diffs[int(n * alpha/2)]
    hi = diffs[int(n * (1 - alpha/2))]
    return lo, hi


def p_positive(diffs):
    return sum(1 for d in diffs if d > 0) / len(diffs)


# ─── 1. Trade-level bootstrap ───
baseline_trades = extract_trades(p4['results'], 'BH_30')
p5_trades = extract_trades(p5['results'], 'ATR1.5_V80')
print(f"\n{'='*70}")
print(f"  Trade-level bootstrap")
print(f"{'='*70}")
print(f"baseline 闭仓: {len(baseline_trades)} 笔, pnl 单笔: {[f'{p:+.1f}%' for _, p in baseline_trades]}")
print(f"P5_ATR15 闭仓: {len(p5_trades)} 笔, pnl 单笔: {[f'{p:+.1f}%' for _, p in p5_trades]}")
print(f"原始累计: baseline={sum(p for _, p in baseline_trades):+.2f}%, P5={sum(p for _, p in p5_trades):+.2f}%")
print(f"原始差异: {sum(p for _, p in p5_trades) - sum(p for _, p in baseline_trades):+.2f}pp")

random.seed(42)
a = [p for _, p in baseline_trades]
b = [p for _, p in p5_trades]
diffs_sum = bootstrap_diff(a, b, n_iter=10000, agg='sum')
lo, hi = ci(diffs_sum)
print(f"\n累计差异（sum）bootstrap 10000 次:")
print(f"  95% CI: [{lo:+.2f}pp, {hi:+.2f}pp]")
print(f"  P(P5 > baseline): {p_positive(diffs_sum)*100:.1f}%")
print(f"  中位差异: {sorted(diffs_sum)[5000]:+.2f}pp")
print(f"  结论: {'✅ CI 不含 0，统计显著' if lo > 0 or hi < 0 else '⚠️ CI 含 0，不显著（无法排除偶然）'}")

# 也算单笔均值差异
diffs_mean = bootstrap_diff(a, b, n_iter=10000, agg='mean')
lo, hi = ci(diffs_mean)
print(f"\n单笔均值差异（mean）bootstrap:")
print(f"  原始: baseline avg={sum(a)/len(a):+.2f}%/笔, P5 avg={sum(b)/len(b):+.2f}%/笔, 差={sum(b)/len(b)-sum(a)/len(a):+.2f}pp")
print(f"  95% CI: [{lo:+.2f}pp, {hi:+.2f}pp]")
print(f"  P(P5 单笔均 > baseline 单笔均): {p_positive(diffs_mean)*100:.1f}%")

# ─── 2. Fold-level bootstrap ───
b_folds = extract_fold_returns(p4['results'], 'BH_30')
p_folds = extract_fold_returns(p5['results'], 'ATR1.5_V80')
print(f"\n{'='*70}")
print(f"  Fold-level bootstrap (block)")
print(f"{'='*70}")
print(f"baseline 4 折: {[f'{r:+.2f}%' for r in b_folds]}, sum={sum(b_folds):+.2f}%")
print(f"P5 4 折:       {[f'{r:+.2f}%' for r in p_folds]}, sum={sum(p_folds):+.2f}%")

random.seed(42)
# Paired bootstrap: 抽 fold index，同时取两组该 fold 的数据
n_folds = len(b_folds)
diffs_fold = []
for _ in range(10000):
    sampled_idx = [random.randint(0, n_folds-1) for _ in range(n_folds)]
    b_sum = sum(b_folds[i] for i in sampled_idx)
    p_sum = sum(p_folds[i] for i in sampled_idx)
    diffs_fold.append(p_sum - b_sum)
diffs_fold.sort()
lo, hi = ci(diffs_fold)
print(f"\nPaired fold bootstrap 10000 次（同 fold 索引同时抽）:")
print(f"  累计差异: {sum(p_folds) - sum(b_folds):+.2f}pp")
print(f"  95% CI: [{lo:+.2f}pp, {hi:+.2f}pp]")
print(f"  P(P5 > baseline): {p_positive(diffs_fold)*100:.1f}%")
print(f"  结论: {'✅ CI 不含 0，统计显著' if lo > 0 or hi < 0 else '⚠️ CI 含 0，4 折样本不足以分辨真假'}")
