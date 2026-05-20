"""
主力建仓策略 — Walk-Forward 验证框架

设计目标：
  代替"单段全期回测"，用滚动训练-测试窗口检验参数的外推稳健性。
  每个 fold：在训练段做参数 grid search → 用最优参数在测试段评估。
  汇总各 fold 的样本外表现 + 参数漂移度，给出稳定性诊断。

数据源：
  本期实现仅支持 `cache`（backtest_cache/*_qfq.json）。
  后续可扩展 local_db。

用法：
  python -m backtest.walk_forward \\
      --start 2024-01-01 --end 2026-04-30 \\
      --train-months 12 --test-months 3 --step-months 3 \\
      --cash 100000 \\
      --metric sharpe \\
      --grid default

输出：
  - 控制台汇总
  - backtest_cache/walk_forward_report/{timestamp}/folds.csv
  - backtest_cache/walk_forward_report/{timestamp}/summary.json
"""

import argparse
import asyncio
import csv
import itertools
import json
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.bt_runner import run_for_web

REPORT_ROOT = ROOT / "backtest_cache" / "walk_forward_report"

# 默认参数网格 — 围绕 P2-A 已固化的默认值做稳定性扫描
# （trail / lock_floor / F-off 已写入策略默认；本网格只扫 P2-A 入场过滤 + 建仓严苛度）
# 2×2 = 4 combos × 4 folds × (4 train + 1 test) ≈ 20 backtests，本地 DB 模式约 3-4 小时
DEFAULT_GRID: Dict[str, List[Any]] = {
    "signal_a_min_rsi":       [55.0, 60.0],   # 围绕 P2-A 选定的 55
    "min_watch_days":         [10, 15],        # P0 选定的 10
}

# 冒烟网格 — 单组合（仅复现当前默认）
QUICK_GRID: Dict[str, List[Any]] = {
    "signal_a_min_rsi":       [55.0],
    "min_watch_days":         [10],
}


# ──────────────────────────────────────────────────────────────
# 配置与 fold 生成
# ──────────────────────────────────────────────────────────────

@dataclass
class WalkForwardConfig:
    start: date
    end: date
    train_months: int = 12
    test_months: int = 3
    step_months: int = 3
    cash: float = 100_000.0
    strategy: str = "major_capital_accumulation"
    data_source: str = "cache"
    metric: str = "sharpe"        # sharpe | total_return | calmar
    grid: Dict[str, List[Any]] = field(default_factory=lambda: dict(DEFAULT_GRID))


@dataclass
class Fold:
    idx: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date


def generate_folds(cfg: WalkForwardConfig) -> List[Fold]:
    folds: List[Fold] = []
    cursor = cfg.start
    idx = 1
    while True:
        train_start = cursor
        train_end = train_start + relativedelta(months=cfg.train_months) - relativedelta(days=1)
        test_start = train_end + relativedelta(days=1)
        test_end = test_start + relativedelta(months=cfg.test_months) - relativedelta(days=1)
        if test_end > cfg.end:
            break
        folds.append(Fold(idx, train_start, train_end, test_start, test_end))
        cursor = cursor + relativedelta(months=cfg.step_months)
        idx += 1
    return folds


# ──────────────────────────────────────────────────────────────
# 单次回测 + 指标提取
# ──────────────────────────────────────────────────────────────

_PCT_RE = re.compile(r"([+-]?\d+(?:\.\d+)?)%")


def _parse_pct(s: Any) -> float:
    """把 '+12.34%' / '12.34%' 转成 0.1234；空值返回 0.0。"""
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    m = _PCT_RE.search(str(s))
    return float(m.group(1)) / 100.0 if m else 0.0


def extract_numeric_metrics(result: Dict[str, Any]) -> Dict[str, float]:
    """从 run_for_web 返回 dict 抽取数值指标。失败时返回全 0。"""
    if not result or "error" in result:
        return {
            "total_return": 0.0, "annualized_return": 0.0,
            "max_drawdown": 0.0, "sharpe": 0.0,
            "win_rate": 0.0, "profit_factor": 0.0,
            "total_trades": 0, "calmar": 0.0,
        }
    m = result.get("metrics", {})
    total_ret = _parse_pct(m.get("total_return"))
    ann_ret = _parse_pct(m.get("annualized_return"))
    mdd = _parse_pct(m.get("max_drawdown"))
    sharpe = float(m.get("sharpe_ratio") or 0.0)
    win_rate = _parse_pct(m.get("win_rate"))
    pf = float(m.get("profit_factor") or 0.0)
    n_trades = int(m.get("total_trades") or 0)
    # Calmar = 年化 / 最大回撤（避免除0）
    calmar = ann_ret / mdd if mdd > 1e-6 else 0.0
    return {
        "total_return": total_ret,
        "annualized_return": ann_ret,
        "max_drawdown": mdd,
        "sharpe": sharpe,
        "win_rate": win_rate,
        "profit_factor": pf,
        "total_trades": n_trades,
        "calmar": calmar,
    }


def selection_score(metrics: Dict[str, float], metric_name: str) -> float:
    """根据用户选择的指标返回单一打分（越大越好）。"""
    if metric_name == "sharpe":
        return metrics["sharpe"]
    if metric_name == "total_return":
        return metrics["total_return"]
    if metric_name == "calmar":
        return metrics["calmar"]
    if metric_name == "annualized_return":
        return metrics["annualized_return"]
    raise ValueError(f"未知 metric: {metric_name}")


async def run_one(
    cfg: WalkForwardConfig,
    start: date,
    end: date,
    extra_params: Dict[str, Any],
    silent: bool = True,
) -> Dict[str, float]:
    """跑一次回测并返回数值指标。"""
    log_fn = None if silent else print
    result = await run_for_web(
        strategy_name=cfg.strategy,
        start=start.isoformat(),
        end=end.isoformat(),
        cash=cfg.cash,
        log_fn=log_fn,
        screen_preset="default",
        data_source=cfg.data_source,
        extra_params=extra_params,
    )
    metrics = extract_numeric_metrics(result)
    metrics["_error"] = result.get("error", "") if isinstance(result, dict) else ""
    return metrics


# ──────────────────────────────────────────────────────────────
# 网格搜索
# ──────────────────────────────────────────────────────────────

def expand_grid(grid: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    keys = sorted(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    return [dict(zip(keys, c)) for c in combos]


async def grid_search_train(
    cfg: WalkForwardConfig,
    fold: Fold,
    log_progress: bool = True,
) -> Tuple[Dict[str, Any], Dict[str, float], List[Dict[str, Any]]]:
    """
    在训练段对参数网格做穷举，返回：
      (best_params, best_metrics, all_results)
    all_results: [{"params": {...}, "metrics": {...}}, ...]
    """
    combos = expand_grid(cfg.grid)
    all_results: List[Dict[str, Any]] = []
    best_params: Optional[Dict[str, Any]] = None
    best_score = float("-inf")
    best_metrics: Dict[str, float] = {}

    for i, params in enumerate(combos, 1):
        t0 = time.time()
        m = await run_one(cfg, fold.train_start, fold.train_end, params)
        elapsed = time.time() - t0
        score = selection_score(m, cfg.metric)
        all_results.append({"params": dict(params), "metrics": dict(m), "score": score})
        if log_progress:
            print(f"  [Fold{fold.idx} TRAIN {i}/{len(combos)}] "
                  f"params={params} {cfg.metric}={score:+.3f} "
                  f"ret={m['total_return']:+.1%} mdd={m['max_drawdown']:.1%} "
                  f"trades={m['total_trades']} ({elapsed:.0f}s)")
        if score > best_score:
            best_score = score
            best_params = params
            best_metrics = m

    if best_params is None:
        best_params = combos[0]
    return best_params, best_metrics, all_results


# ──────────────────────────────────────────────────────────────
# Walk-forward 主流程
# ──────────────────────────────────────────────────────────────

async def run_walk_forward(cfg: WalkForwardConfig) -> Dict[str, Any]:
    folds = generate_folds(cfg)
    if not folds:
        raise ValueError(
            f"无法生成 fold：检查 start/end/train/test/step 配置。"
            f" start={cfg.start} end={cfg.end} train={cfg.train_months}mo "
            f"test={cfg.test_months}mo step={cfg.step_months}mo"
        )

    print(f"\n{'='*70}")
    print(f"  Walk-Forward 验证 | 策略: {cfg.strategy}")
    print(f"  数据源: {cfg.data_source}  |  选择指标: {cfg.metric}")
    print(f"  全期: {cfg.start} ~ {cfg.end}")
    print(f"  Train={cfg.train_months}mo Test={cfg.test_months}mo Step={cfg.step_months}mo")
    print(f"  生成 {len(folds)} 个 fold | 网格组合数: {len(expand_grid(cfg.grid))}")
    print(f"{'='*70}\n")

    fold_records: List[Dict[str, Any]] = []
    overall_t0 = time.time()

    for fold in folds:
        print(f"\n── Fold {fold.idx} ──")
        print(f"  Train: {fold.train_start} ~ {fold.train_end}")
        print(f"  Test:  {fold.test_start} ~ {fold.test_end}")

        # 1. 训练段网格搜索
        best_params, best_train_metrics, all_train = await grid_search_train(cfg, fold)
        print(f"  → 选中参数: {best_params}")
        print(f"     训练段 {cfg.metric}={selection_score(best_train_metrics, cfg.metric):+.3f}")

        # 2. 测试段（样本外）评估
        oos_metrics = await run_one(cfg, fold.test_start, fold.test_end, best_params)
        print(f"  → 测试段 {cfg.metric}={selection_score(oos_metrics, cfg.metric):+.3f} "
              f"ret={oos_metrics['total_return']:+.1%} "
              f"mdd={oos_metrics['max_drawdown']:.1%} "
              f"trades={oos_metrics['total_trades']}")

        fold_records.append({
            "fold":          fold.idx,
            "train_start":   fold.train_start.isoformat(),
            "train_end":     fold.train_end.isoformat(),
            "test_start":    fold.test_start.isoformat(),
            "test_end":      fold.test_end.isoformat(),
            "best_params":   best_params,
            "is_metrics":    best_train_metrics,
            "oos_metrics":   oos_metrics,
            "all_train_runs": all_train,
        })

    total_elapsed = time.time() - overall_t0
    print(f"\n[完成] 总耗时 {total_elapsed/60:.1f} 分钟")

    summary = analyze_stability(fold_records, cfg)
    return {
        "config":  serialize_cfg(cfg),
        "folds":   fold_records,
        "summary": summary,
    }


# ──────────────────────────────────────────────────────────────
# 稳定性分析
# ──────────────────────────────────────────────────────────────

def _mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _std(xs: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    mu = _mean(xs)
    return (sum((x - mu) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def analyze_stability(folds: List[Dict[str, Any]], cfg: WalkForwardConfig) -> Dict[str, Any]:
    """汇总样本外表现 + 参数漂移度。"""
    if not folds:
        return {}

    is_scores = [selection_score(f["is_metrics"], cfg.metric) for f in folds]
    oos_scores = [selection_score(f["oos_metrics"], cfg.metric) for f in folds]
    oos_returns = [f["oos_metrics"]["total_return"] for f in folds]
    oos_mdd = [f["oos_metrics"]["max_drawdown"] for f in folds]
    oos_trades = [f["oos_metrics"]["total_trades"] for f in folds]
    oos_winrate = [f["oos_metrics"]["win_rate"] for f in folds]

    # IS/OOS 衰减比
    is_mean = _mean(is_scores)
    oos_mean = _mean(oos_scores)
    if abs(is_mean) > 1e-6:
        decay_ratio = oos_mean / is_mean
    else:
        decay_ratio = 0.0

    # 参数漂移度：每个参数被选中值的频率分布
    param_drift: Dict[str, Dict[str, int]] = {}
    for f in folds:
        for k, v in f["best_params"].items():
            param_drift.setdefault(k, {})
            key = str(v)
            param_drift[k][key] = param_drift[k].get(key, 0) + 1
    # 主导参数比例（最高频值占比）
    param_dominance = {}
    for k, dist in param_drift.items():
        total = sum(dist.values())
        top = max(dist.values()) if dist else 0
        param_dominance[k] = round(top / total, 2) if total else 0.0

    # 样本外累计收益（链式复利）
    oos_cum_return = 1.0
    for r in oos_returns:
        oos_cum_return *= (1.0 + r)
    oos_cum_return -= 1.0

    return {
        "n_folds":                  len(folds),
        "is_mean_score":            round(is_mean, 4),
        "oos_mean_score":           round(oos_mean, 4),
        "oos_std_score":            round(_std(oos_scores), 4),
        "decay_ratio":              round(decay_ratio, 3),
        "oos_cumulative_return":    round(oos_cum_return, 4),
        "oos_avg_return_per_fold":  round(_mean(oos_returns), 4),
        "oos_avg_mdd":              round(_mean(oos_mdd), 4),
        "oos_avg_winrate":          round(_mean(oos_winrate), 4),
        "oos_avg_trades":           round(_mean(oos_trades), 1),
        "param_drift":              param_drift,
        "param_dominance":          param_dominance,
    }


# ──────────────────────────────────────────────────────────────
# 序列化 + 报告输出
# ──────────────────────────────────────────────────────────────

def serialize_cfg(cfg: WalkForwardConfig) -> Dict[str, Any]:
    d = asdict(cfg)
    d["start"] = cfg.start.isoformat()
    d["end"] = cfg.end.isoformat()
    return d


def save_report(report: Dict[str, Any], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    # folds.csv：每行一个 fold（含训练段最优参数 + 样本外指标）
    csv_path = out_dir / "folds.csv"
    if report["folds"]:
        # 收集所有出现的参数 key
        param_keys = sorted({k for f in report["folds"] for k in f["best_params"].keys()})
        metric_keys = ["total_return", "annualized_return", "max_drawdown",
                       "sharpe", "win_rate", "profit_factor", "total_trades", "calmar"]
        header = (["fold", "train_start", "train_end", "test_start", "test_end"]
                  + [f"param_{k}" for k in param_keys]
                  + [f"is_{k}" for k in metric_keys]
                  + [f"oos_{k}" for k in metric_keys])
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(header)
            for f in report["folds"]:
                row = [f["fold"], f["train_start"], f["train_end"],
                       f["test_start"], f["test_end"]]
                row += [f["best_params"].get(k, "") for k in param_keys]
                row += [f["is_metrics"].get(k, 0) for k in metric_keys]
                row += [f["oos_metrics"].get(k, 0) for k in metric_keys]
                w.writerow(row)

    # summary.json：完整报告（含全部网格结果，便于离线分析）
    json_path = out_dir / "summary.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2, default=str)

    return out_dir


def print_summary(report: Dict[str, Any]) -> None:
    s = report["summary"]
    cfg = report["config"]
    print(f"\n{'='*70}")
    print(f"  样本外稳定性诊断")
    print(f"{'='*70}")
    print(f"  Folds 数:                 {s['n_folds']}")
    print(f"  样本内 {cfg['metric']} 均值:    {s['is_mean_score']:+.3f}")
    print(f"  样本外 {cfg['metric']} 均值:    {s['oos_mean_score']:+.3f}")
    print(f"  样本外 {cfg['metric']} 标准差:  {s['oos_std_score']:.3f}")
    print(f"  IS→OOS 衰减比:            {s['decay_ratio']:+.2f}  "
          f"({'稳健' if s['decay_ratio'] >= 0.5 else '过拟合明显' if s['decay_ratio'] < 0.2 else '中等衰减'})")
    print(f"  样本外累计收益:           {s['oos_cumulative_return']*100:+.2f}%")
    print(f"  样本外单段平均收益:       {s['oos_avg_return_per_fold']*100:+.2f}%")
    print(f"  样本外平均最大回撤:       {s['oos_avg_mdd']*100:.2f}%")
    print(f"  样本外平均胜率:           {s['oos_avg_winrate']*100:.1f}%")
    print(f"  样本外平均交易数:         {s['oos_avg_trades']:.1f}")
    print(f"\n  参数稳定性（最高频值占比，越接近 1 越稳定）:")
    for k, v in s["param_dominance"].items():
        dist = s["param_drift"][k]
        print(f"    {k}: {v:.0%}  分布={dist}")
    print(f"{'='*70}\n")


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def parse_grid_arg(value: str) -> Dict[str, List[Any]]:
    if value == "default":
        return dict(DEFAULT_GRID)
    if value == "quick":
        return dict(QUICK_GRID)
    # 允许 JSON 文件路径
    p = Path(value)
    if p.exists():
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)
    # 允许内联 JSON
    return json.loads(value)


def main() -> int:
    ap = argparse.ArgumentParser(description="Walk-forward 验证主力建仓策略")
    ap.add_argument("--start", type=str, default="2024-01-01")
    ap.add_argument("--end", type=str, default="2026-04-30")
    ap.add_argument("--train-months", type=int, default=12)
    ap.add_argument("--test-months", type=int, default=3)
    ap.add_argument("--step-months", type=int, default=3)
    ap.add_argument("--cash", type=float, default=100_000.0)
    ap.add_argument("--strategy", type=str, default="major_capital_accumulation")
    ap.add_argument("--data-source", type=str, default="cache",
                    choices=["cache", "local_db"])
    ap.add_argument("--metric", type=str, default="sharpe",
                    choices=["sharpe", "total_return", "calmar", "annualized_return"])
    ap.add_argument("--grid", type=str, default="default",
                    help='"default" | "quick" | path/to/grid.json | inline-JSON')
    args = ap.parse_args()

    cfg = WalkForwardConfig(
        start=datetime.strptime(args.start, "%Y-%m-%d").date(),
        end=datetime.strptime(args.end, "%Y-%m-%d").date(),
        train_months=args.train_months,
        test_months=args.test_months,
        step_months=args.step_months,
        cash=args.cash,
        strategy=args.strategy,
        data_source=args.data_source,
        metric=args.metric,
        grid=parse_grid_arg(args.grid),
    )

    report = asyncio.run(run_walk_forward(cfg))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = REPORT_ROOT / ts
    save_report(report, out_dir)
    print_summary(report)
    print(f"[报告] 已保存到 {out_dir}/")
    print(f"  - folds.csv      逐 fold 训练参数 + IS/OOS 指标")
    print(f"  - summary.json   完整结构（含训练段全部网格结果）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
