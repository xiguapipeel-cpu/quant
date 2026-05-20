import { useEffect, useState, useMemo, useContext, useRef } from 'react';
import { Card, Row, Col, Button, Select, DatePicker, InputNumber, Table, Typography, Modal, Statistic, message, Space, Empty, Input, Radio, Popover, Tag, Tooltip, Progress, Divider, Alert, Switch, Tabs, Spin } from 'antd';
import { PlayCircleOutlined, EyeOutlined, LineChartOutlined, LoadingOutlined, SearchOutlined, CheckCircleOutlined, SyncOutlined, ExperimentOutlined, CheckOutlined, WarningOutlined, CloseCircleOutlined, FileTextOutlined, HeatMapOutlined, FallOutlined, BarChartOutlined } from '@ant-design/icons';
import { apiFetch } from '../api/client';
import { WsContext } from '../App';
import dayjs from 'dayjs';
import ReactECharts from 'echarts-for-react';

const { Title, Text } = Typography;

const strategies = [
  { value: 'trend_follow', label: '趋势跟踪' },
  { value: 'rsi_reversal', label: 'RSI反转' },
  { value: 'bollinger_revert', label: '布林带回归' },
  { value: 'major_capital_pump', label: '主力拉升' },
  { value: 'major_capital_accumulation', label: '主力建仓' },
];

/* ── 统一格式化工具 ─────────────────────────────── */
const fmtMoney = (v: any): string => {
  if (v == null) return '--';
  const n = Number(v);
  return (n >= 0 ? '+' : '') + n.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' 元';
};
const fmtAsset = (v: any): string => {
  if (v == null) return '--';
  return Number(v).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' 元';
};
const pctColor = (v: any) => {
  if (v == null) return undefined;
  const s = String(v);
  return s.startsWith('-') ? '#ef4444' : '#10b981';
};
const moneyColor = (v: number) => v >= 0 ? '#10b981' : '#ef4444';

/* ── 进度阶段推断 ───────────────────────────────── */
const inferStage = (logs: string[]): { pct: number; label: string } => {
  const joined = logs.join(' ');
  // ── OOS 专用阶段（优先判断，防止被普通规则误判）──
  if (joined.includes('样本外测试完成')) return { pct: 100, label: '样本外测试完成 ✓' };
  if (joined.includes('[测试] 回测完成') || (joined.includes('[测试]') && joined.includes('回测完成'))) return { pct: 95, label: '测试集完成，汇总结果...' };
  if (joined.includes('[测试] 正在运行') || (joined.includes('[测试]') && joined.includes('正在运行'))) return { pct: 80, label: '测试集：运行回测引擎...' };
  if (joined.includes('[测试] 实际加载') || (joined.includes('[测试]') && joined.includes('实际加载'))) return { pct: 70, label: '测试集：数据加载完毕...' };
  if (joined.includes('[测试] 批量加载') || joined.includes('[测试集]')) return { pct: 60, label: '测试集：加载行情数据...' };
  if (joined.includes('[训练] 回测完成') || (joined.includes('[训练]') && joined.includes('回测完成'))) return { pct: 55, label: '训练集完成，启动测试集...' };
  if (joined.includes('[训练] 正在运行') || (joined.includes('[训练]') && joined.includes('正在运行'))) return { pct: 40, label: '训练集：运行回测引擎...' };
  if (joined.includes('[训练] 实际加载') || (joined.includes('[训练]') && joined.includes('实际加载'))) return { pct: 30, label: '训练集：数据加载完毕...' };
  if (joined.includes('[训练] 批量加载') || joined.includes('[训练集]')) return { pct: 20, label: '训练集：加载行情数据...' };
  if (joined.includes('样本外测试启动')) return { pct: 10, label: '初始化样本外测试...' };
  // ── 普通回测阶段 ──
  if (joined.includes('回测完成')) return { pct: 100, label: '回测完成 ✓' };
  if (joined.includes('已持久化') || joined.includes('持久化')) return { pct: 98, label: '保存结果...' };
  if (joined.includes('正在运行 Backtrader') || joined.includes('正在运行回测')) return { pct: 65, label: '运行回测引擎...' };
  if (joined.includes('实际加载') && joined.includes('只股票')) return { pct: 50, label: '数据加载完毕，准备引擎...' };
  if (joined.includes('正在加载') || joined.includes('批量加载')) return { pct: 30, label: '批量加载行情数据...' };
  if (joined.includes('发现') && joined.includes('只股票')) return { pct: 20, label: '查询股票列表...' };
  if (joined.includes('引擎') || joined.includes('回测启动')) return { pct: 10, label: '初始化引擎...' };
  return { pct: 5, label: '连接服务中...' };
};

export default function BacktestCenter() {
  const { logs: wsLogs } = useContext(WsContext);
  const [btList, setBtList] = useState<any[]>([]);
  const [running, setRunning] = useState(false);
  const [mode, setMode] = useState<'normal' | 'oos'>('normal');
  // 结束日期默认上一个完整交易日（避免今日盘中数据混入回测）
  const [form, setForm] = useState({ strategy: 'major_capital_accumulation', start: '2025-01-01', end: dayjs().subtract(1, 'day').format('YYYY-MM-DD'), cash: 100000, data_source: 'cache' });
  const [oosForm, setOosForm] = useState({
    strategy: 'major_capital_accumulation',
    train_start: '2022-01-01', train_end: '2023-12-31',
    test_start:  '2018-01-01', test_end:  '2021-12-31',
    cash: 100000, data_source: 'local_db',
  });
  const [oosHasTrain, setOosHasTrain] = useState(true);
  const [oosResult, setOosResult] = useState<any>(null);
  const startDayjs = useMemo(() => dayjs(form.start), [form.start]);
  const endDayjs   = useMemo(() => dayjs(form.end),   [form.end]);
  const [tradeModal, setTradeModal] = useState<any>(null);
  const [equityModal, setEquityModal] = useState<any>(null);
  const [tradeSearch, setTradeSearch] = useState('');
  const [tradePnlFilter, setTradePnlFilter] = useState<'all' | 'win' | 'loss' | 'holding'>('all');
  const [reportModal, setReportModal] = useState<any>(null);
  const [sweepLoading, setSweepLoading] = useState(false);
  const [sweepResult, setSweepResult] = useState<any>(null);
  const [_sweepKey, setSweepKey] = useState<string | null>(null);
  const sweepKeyRef = useRef<string | null>(null);   // 同步版本，供 showReport 判断
  const sweepPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // ── 进度相关 ──
  const [progressLogs, setProgressLogs] = useState<string[]>([]);
  const [elapsed, setElapsed] = useState(0);
  const startTimeRef = useRef<number>(0);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // 回测运行期间收集 WS 日志
  useEffect(() => {
    if (!running) return;
    const last = wsLogs[wsLogs.length - 1];
    if (!last) return;
    setProgressLogs(prev => [...prev.slice(-20), last.msg]);
  }, [wsLogs, running]);

  // 计时器
  useEffect(() => {
    if (running) {
      startTimeRef.current = Date.now();
      setElapsed(0);
      timerRef.current = setInterval(() => {
        setElapsed(Math.floor((Date.now() - startTimeRef.current) / 1000));
      }, 1000);
    } else {
      if (timerRef.current) clearInterval(timerRef.current);
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  }, [running]);

  const fmtElapsed = (s: number) =>
    s < 60 ? `${s}秒` : `${Math.floor(s / 60)}分${s % 60}秒`;

  const load = async () => {
    const data = await apiFetch('/api/backtest/list').catch(() => []);
    // 最新回测排在前面（按 ID 降序）
    const sorted = (data || []).sort((a: any, b: any) => (b.id ?? 0) - (a.id ?? 0));
    setBtList(sorted);
  };

  // ── 页面刷新后恢复运行状态 ──────────────────────────────
  // 若刷新时后端仍在跑回测，自动重入轮询，避免卡死在进度面板消失状态
  useEffect(() => {
    load();
    let poll: ReturnType<typeof setInterval> | null = null;

    const checkRunning = async () => {
      const st = await apiFetch('/api/backtest/status').catch(() => null);
      if (!st?.running) return;          // 没在跑，正常加载
      // 已在跑：恢复 running 状态，切到 OOS 标签并开始轮询
      setRunning(true);
      setMode('oos');
      poll = setInterval(async () => {
        const s = await apiFetch('/api/backtest/status').catch(() => null);
        if (s && !s.running) {
          clearInterval(poll!);
          setRunning(false);
          if (s.error) {
            message.error(`回测失败: ${s.error}`);
          } else {
            const res = await apiFetch('/api/backtest/oos_result').catch(() => null);
            if (res && !res.error) setOosResult(res);
            message.success('回测已在后台完成');
          }
          load();
        }
      }, 2000);
    };

    checkRunning();
    return () => { if (poll) clearInterval(poll); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const runBacktest = async () => {
    setProgressLogs([]);
    setRunning(true);
    try {
      await apiFetch(`/api/backtest/run?strategy=${form.strategy}&start=${form.start}&end=${form.end}&cash=${form.cash}&data_source=${form.data_source}`, 'POST');
      const poll = setInterval(async () => {
        const st = await apiFetch('/api/backtest/status').catch(() => null);
        if (st && !st.running) {
          clearInterval(poll);
          setRunning(false);
          if (st.error) {
            message.error(`回测失败: ${st.error}`);
          } else {
            load();
            message.success('回测完成');
          }
        }
      }, 2000);
    } catch {
      message.error('回测启动失败');
      setRunning(false);
    }
  };

  const runOosBacktest = async () => {
    setProgressLogs([]);
    setRunning(true);
    setOosResult(null);
    try {
      const { strategy, train_start, train_end, test_start, test_end, cash, data_source } = oosForm;
      const params = new URLSearchParams({ strategy, test_start, test_end, cash: String(cash), data_source });
      if (oosHasTrain) { params.set('train_start', train_start); params.set('train_end', train_end); }
      await apiFetch(`/api/backtest/run_oos?${params}`, 'POST');
      const poll = setInterval(async () => {
        const st = await apiFetch('/api/backtest/status').catch(() => null);
        if (st && !st.running) {
          clearInterval(poll);
          setRunning(false);
          if (st.error) {
            message.error(`样本外测试失败: ${st.error}`);
          } else {
            const res = await apiFetch('/api/backtest/oos_result').catch(() => null);
            if (res && !res.error) setOosResult(res);
            message.success('样本外测试完成');
          }
        }
      }, 2000);
    } catch {
      message.error('样本外测试启动失败');
      setRunning(false);
    }
  };

  const showTrades = async (id: number) => {
    const data = await apiFetch(`/api/backtest/trades/${id}`).catch(() => null);
    if (data) {
      setTradeModal(data);
      setTradeSearch('');
      setTradePnlFilter('all');
    }
  };

  const showEquity = (record: any) => {
    const eq = record.equity;
    if (!eq || !eq.dates) {
      message.warning('该记录无资产走势数据');
      return;
    }
    const m = record.metrics || {};
    const cash = m.initial_cash || 1000000;
    // 优先使用精确的 abs_values，旧数据降级用 values * cash
    const absValues: number[] = eq.abs_values
      ? eq.abs_values
      : (eq.values || []).map((v: number) => Number((v * cash).toFixed(2)));
    // 强制最后一个点与 metrics.final_value 精确一致
    if (absValues.length > 0 && m.final_value != null) {
      absValues[absValues.length - 1] = Number(m.final_value);
    }
    setEquityModal({
      strategy: record.strategy,
      start: record.start,
      end: record.end,
      dates: eq.dates,
      values: absValues,
      initialCash: cash,
      annualized_return: m.annualized_return,
      total_return: m.total_return,
      max_drawdown: m.max_drawdown,
      sharpe_ratio: m.sharpe_ratio,
      period_profit: m.period_profit,
      final_value: m.final_value,
    });
  };

  // ── 报告弹窗 ──────────────────────────────────────────
  const showReport = async (record: any) => {
    const eq = record.equity;
    if (!eq || !eq.dates || eq.dates.length === 0) {
      message.warning('该记录无资产走势数据，无法生成报告');
      return;
    }
    const m = record.metrics || {};
    const cash = m.initial_cash || 100000;
    const absValues: number[] = eq.abs_values
      ? eq.abs_values
      : (eq.values || []).map((v: number) => Number((v * cash).toFixed(2)));
    if (absValues.length > 0 && m.final_value != null) {
      absValues[absValues.length - 1] = Number(m.final_value);
    }

    // 分年度收益
    const annual: Record<string, { start_val: number; end_val: number; ret: number }> = {};
    eq.dates.forEach((dt: string, i: number) => {
      const yr = dt.slice(0, 4);
      if (!annual[yr]) annual[yr] = { start_val: i > 0 ? absValues[i - 1] : cash, end_val: absValues[i], ret: 0 };
      annual[yr].end_val = absValues[i];
    });
    Object.keys(annual).forEach(yr => {
      annual[yr].ret = Number(((annual[yr].end_val / annual[yr].start_val - 1) * 100).toFixed(2));
    });

    // 最大回撤期分析（Top 3，不重叠）
    const vals = absValues;
    const dates = eq.dates as string[];
    const n = vals.length;
    let peak = vals[0]; let pkI = 0;
    const peaks: number[] = [peak]; const pkIdxs: number[] = [0];
    for (let i = 1; i < n; i++) {
      if (vals[i] > peak) { peak = vals[i]; pkI = i; }
      peaks.push(peak); pkIdxs.push(pkI);
    }
    const ddSeries = peaks.map((p, i) => p > 0 ? (p - vals[i]) / p : 0);
    const used = new Array(n).fill(false);
    const ddPeriods: any[] = [];
    for (let iter = 0; iter < 3; iter++) {
      let maxDd = 0; let maxIdx = -1;
      for (let i = 0; i < n; i++) { if (!used[i] && ddSeries[i] > maxDd) { maxDd = ddSeries[i]; maxIdx = i; } }
      if (maxIdx < 0 || maxDd < 0.005) break;
      const pk = pkIdxs[maxIdx];
      const target = peaks[maxIdx];
      let recIdx: number | null = null;
      for (let i = maxIdx; i < n; i++) { if (vals[i] >= target) { recIdx = i; break; } }
      const endMark = recIdx !== null ? recIdx : n - 1;
      for (let i = pk; i <= endMark; i++) used[i] = true;
      ddPeriods.push({
        peak_date: dates[pk], trough_date: dates[maxIdx],
        recovery_date: recIdx !== null ? dates[recIdx] : null,
        drawdown_pct: Number((maxDd * 100).toFixed(2)),
        down_days: maxIdx - pk,
        recovery_days: recIdx !== null ? recIdx - maxIdx : null,
      });
    }

    const newKey = `${record.strategy}:${record.start}:${record.end}`;

    // 停掉旧轮询，防止旧回调在 await 期间写入 state
    if (sweepPollRef.current) {
      clearInterval(sweepPollRef.current);
      sweepPollRef.current = null;
    }
    // 先重置，后由后台状态决定是否恢复
    setSweepResult(null);
    setSweepLoading(false);
    setSweepKey(newKey);
    sweepKeyRef.current = newKey;

    setReportModal({ record, strategy: record.strategy, start: record.start, end: record.end, dates, absValues, cash, metrics: m, annual, ddPeriods });

    // 从后台查询真实状态并恢复（key 必须匹配才处理，带 key 参数使后端可从 DB 兜底）
    try {
      const st = await apiFetch(`/api/backtest/param_sweep/status?key=${encodeURIComponent(newKey)}`).catch(() => null);
      if (st?.key === newKey) {
        if (st.result) {
          setSweepResult(st.result);
        } else if (st.running) {
          setSweepLoading(true);
          _startSweepPoll(newKey);
        }
      }
    } catch { /* ignore */ }
  };

  /** 启动轮询（不依赖 reportModal 是否打开，关闭弹窗也继续跑） */
  const _startSweepPoll = (key: string) => {
    if (sweepPollRef.current) clearInterval(sweepPollRef.current);
    sweepPollRef.current = setInterval(async () => {
      try {
        const st = await apiFetch('/api/backtest/param_sweep/status');
        if (st?.key !== key) { clearInterval(sweepPollRef.current!); return; }
        if (!st.running) {
          clearInterval(sweepPollRef.current!);
          sweepPollRef.current = null;
          setSweepLoading(false);
          if (st.error) { message.error(`参数热力图失败: ${st.error}`); }
          else if (st.result) { setSweepResult(st.result); }
        }
      } catch { /* ignore transient errors */ }
    }, 2000);
  };

  const runParamSweep = async () => {
    if (!reportModal) return;
    const { strategy, start, end, cash, record } = reportModal;
    const ds = record?.data_source || 'local_db';
    const key = `${strategy}:${start}:${end}`;
    try {
      const res = await apiFetch(
        `/api/backtest/param_sweep?strategy=${strategy}&start=${start}&end=${end}&cash=${cash}&data_source=${ds}`,
        'POST'
      );
      if (res?.error) { message.error(res.error); return; }
      if (res?.status === 'already_done' && res.key === key) {
        // 已有结果，直接从 status 接口取
        const st = await apiFetch('/api/backtest/param_sweep/status');
        if (st?.result) { setSweepResult(st.result); return; }
      }
      setSweepKey(key);
      sweepKeyRef.current = key;
      setSweepResult(null);
      setSweepLoading(true);
      _startSweepPoll(key);
    } catch { message.error('参数热力图生成失败'); }
  };

  // ── Table columns ─────────────────────────────────────
  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 50 },
    { title: '策略', key: 'strategy', render: (_: any, r: any) => {
      const label = strategies.find(s => s.value === r.strategy)?.label ?? r.strategy;
      if (r.oos) return (
        <Space size={4}>
          <ExperimentOutlined style={{ color: '#7c3aed' }} />
          <Text strong style={{ color: '#7c3aed' }}>{label}</Text>
          <Tag color="purple" style={{ fontSize: 10, padding: '0 4px' }}>OOS</Tag>
        </Space>
      );
      return <Text strong>{label}</Text>;
    }},
    { title: '数据来源', key: 'data_source', render: (_: any, r: any) => {
      const label = r.data_source === 'local_db' ? '本地数据库' : '本地缓存';
      return <Text type="secondary">{label}</Text>;
    }},
    { title: '区间', key: 'period', render: (_: any, r: any) => {
      if (r.oos) return (
        <div style={{ fontSize: 11, lineHeight: '18px' }}>
          {r.train_start && <div><Text style={{ color: '#3b82f6' }}>训练: {r.train_start}~{r.train_end}</Text></div>}
          <div><Text style={{ color: '#f59e0b' }}>测试: {r.test_start}~{r.test_end}</Text></div>
        </div>
      );
      return <Text type="secondary" style={{ fontSize: 12 }}>{r.start}~{r.end}</Text>;
    }},
    { title: '初始资金', key: 'cash', render: (_: any, r: any) => {
      const v = r.metrics?.initial_cash ?? r.cash;
      return <Text>{v != null ? fmtAsset(v) : '--'}</Text>;
    }},
    { title: '年化收益', key: 'ann', render: (_: any, r: any) => {
      if (r.oos) {
        const verdict = r.verdict;
        const cfg = verdict === 'PASS'
          ? { color: '#10b981', icon: <CheckOutlined />, label: '通过' }
          : verdict === 'WARN'
          ? { color: '#f59e0b', icon: <WarningOutlined />, label: '警告' }
          : verdict === 'N/A'
          ? { color: '#6366f1', icon: <ExperimentOutlined />, label: '仅测试' }
          : { color: '#ef4444', icon: <CloseCircleOutlined />, label: '过拟合' };
        const tagColor = verdict === 'PASS' ? 'success' : verdict === 'WARN' ? 'warning' : verdict === 'N/A' ? 'processing' : 'error';
        return <Tag color={tagColor} icon={cfg.icon}>{cfg.label}</Tag>;
      }
      const v = r.metrics?.annualized_return;
      return <Text style={{ color: pctColor(v), fontWeight: 600 }}>{v || '--'}</Text>;
    }},
    { title: '区间收益', key: 'totalret', render: (_: any, r: any) => {
      if (r.oos) return (
        <div style={{ fontSize: 11, lineHeight: '18px' }}>
          {r.train_start != null
            ? <div><Text style={{ color: '#3b82f6' }}>训练: <b style={{ color: pctColor(`${r.train_ret}`) }}>{r.train_ret >= 0 ? '+' : ''}{Number(r.train_ret).toFixed(2)}%</b></Text></div>
            : null}
          <div><Text style={{ color: '#f59e0b' }}>测试: <b style={{ color: pctColor(`${r.test_ret}`) }}>{r.test_ret >= 0 ? '+' : ''}{Number(r.test_ret).toFixed(2)}%</b></Text></div>
        </div>
      );
      const v = r.metrics?.total_return;
      return <Text style={{ color: pctColor(v) }}>{v || '--'}</Text>;
    }},
    { title: '区间盈亏(元)', key: 'profit', render: (_: any, r: any) => {
      const v = r.metrics?.period_profit ?? 0;
      return <Text style={{ color: moneyColor(v), fontWeight: 600 }}>{fmtMoney(v)}</Text>;
    }},
    { title: '总资产(元)', key: 'final', render: (_: any, r: any) => <Text strong>{fmtAsset(r.metrics?.final_value)}</Text> },
    { title: '最大回撤', dataIndex: ['metrics', 'max_drawdown'], key: 'dd', render: (v: string) => <Text type="danger">{v || '--'}</Text> },
    { title: '夏普', dataIndex: ['metrics', 'sharpe_ratio'], key: 'sharpe' },
    { title: '胜率', dataIndex: ['metrics', 'win_rate'], key: 'wr' },
    { title: '盈亏比', dataIndex: ['metrics', 'profit_factor'], key: 'pf' },
    { title: '交易', dataIndex: ['metrics', 'total_trades'], key: 'trades' },
    { title: '操作', key: 'action', width: 260, render: (_: any, r: any) => (
      <Space size={4}>
        <Button type="link" size="small" icon={<EyeOutlined />} onClick={() => showTrades(r.id)}>交易详情</Button>
        <Button type="link" size="small" icon={<LineChartOutlined />} onClick={() => showEquity(r)} style={{ color: '#f59e0b' }}>走势图</Button>
        {!r.oos && <Button type="link" size="small" icon={<FileTextOutlined />} onClick={() => showReport(r)} style={{ color: '#10b981' }}>完整报告</Button>}
      </Space>
    )},
  ];

  // ── Trade detail calculations ─────────────────────────
  const td = tradeModal;
  const totalProfit = td?.total_profit ?? 0;
  const realizedPnl = td?.realized_pnl ?? 0;
  const unrealizedPnl = td?.unrealized_pnl ?? 0;

  // 全量排序（时间倒序，持仓中排最前）
  const tdTrades = [...(td?.trades || [])].sort((a: any, b: any) => {
    const da = a.sell_date === '（持仓中）' ? '9999' : (a.buy_date || '');
    const db = b.sell_date === '（持仓中）' ? '9999' : (b.buy_date || '');
    return db.localeCompare(da);
  });

  // 全量统计（不受搜索/筛选影响）
  const wins = tdTrades.filter((t: any) => t.pnl > 0).length;
  const losses = tdTrades.filter((t: any) => t.pnl < 0 && t.sell_date !== '（持仓中）').length;
  const holding = tdTrades.filter((t: any) => t.sell_date === '（持仓中）').length;
  const closedCount = tdTrades.length - holding;
  const winRate = closedCount > 0 ? (wins / closedCount * 100).toFixed(1) : '0.0';

  // 过滤后列表（用于表格展示）
  const filteredTrades = useMemo(() => {
    let list = tdTrades;
    // 名称/代码搜索
    if (tradeSearch.trim()) {
      const kw = tradeSearch.trim().toLowerCase();
      list = list.filter((t: any) =>
        (t.name || '').toLowerCase().includes(kw) ||
        (t.code || '').toLowerCase().includes(kw)
      );
    }
    // 盈亏筛选
    if (tradePnlFilter === 'win') {
      list = list.filter((t: any) => t.pnl > 0);
    } else if (tradePnlFilter === 'loss') {
      list = list.filter((t: any) => t.pnl < 0 && t.sell_date !== '（持仓中）');
    } else if (tradePnlFilter === 'holding') {
      list = list.filter((t: any) => t.sell_date === '（持仓中）');
    }
    return list;
  }, [tdTrades, tradeSearch, tradePnlFilter]);

  const tradeColumns = [
    { title: '#', key: 'idx', width: 40, render: (_: any, __: any, i: number) => i + 1 },
    { title: '股票', key: 'stock', render: (_: any, t: any) => (
      <div
        style={{ cursor: 'pointer' }}
        onClick={() => {
          apiFetch('/api/open-ths', 'POST', { code: t.code })
            .then((r: any) => {
              if (r.ok) message.success(r.auto_paste ? `已在同花顺中打开 ${t.code}` : `已打开同花顺，${t.code} 已复制到剪贴板，⌘V 粘贴`);
              else message.error(r.msg || '打开失败');
            })
            .catch(() => message.error('请求失败'));
        }}
      >
        <Text strong style={{ color: '#60a5fa' }}>{t.name && t.name !== t.code ? t.name : t.code}</Text>
        {t.name && t.name !== t.code && (
          <><br /><Text type="secondary" style={{ fontSize: 11 }}>{t.code}</Text></>
        )}
      </div>
    )},
    { title: '买入日期', dataIndex: 'buy_date', key: 'bd', render: (v: string) => <Text style={{ color: '#10b981', fontSize: 12 }}>{v}</Text> },
    { title: '买入价', dataIndex: 'buy_price', key: 'bp', render: (v: number) => <Text style={{ color: '#10b981' }}>{v?.toFixed(2)}</Text> },
    { title: '卖出日期', dataIndex: 'sell_date', key: 'sd', render: (v: string) => <Text style={{ color: v === '（持仓中）' ? '#f59e0b' : '#ef4444', fontSize: 12 }}>{v}</Text> },
    { title: '卖出价', dataIndex: 'sell_price', key: 'sp', render: (v: number, t: any) => t.sell_date === '（持仓中）' ? '—' : <Text style={{ color: '#ef4444' }}>{v?.toFixed(2)}</Text> },
    { title: '数量', dataIndex: 'shares', key: 'sh', render: (v: number) => v?.toLocaleString() },
    { title: '盈亏(元)', dataIndex: 'pnl', key: 'pnl', render: (v: number, t: any) => {
      if (t.sell_date === '（持仓中）') return '—';
      return <Text style={{ color: moneyColor(v), fontWeight: 600 }}>{(v >= 0 ? '+' : '') + v?.toFixed(2)}</Text>;
    }},
    { title: '盈亏%', dataIndex: 'pnl_pct', key: 'pp', render: (v: number, t: any) => {
      if (t.sell_date === '（持仓中）') return '—';
      return <Text style={{ color: moneyColor(v) }}>{(v >= 0 ? '+' : '') + v?.toFixed(2)}%</Text>;
    }},
    { title: '买入详情', key: 'br', width: 160, render: (_: any, t: any) => {
      const meta = t.buy_meta || {};
      const conf = meta.confidence ?? t.confidence ?? 0;
      const hasMeta = !!(meta.trigger || meta.accumulation_days);

      const confColor = conf >= 0.8 ? '#e74c3c' : conf >= 0.6 ? '#f39c12' : '#8892a4';

      // 信号日时间线：倒序排列，最多显示6条，溢出 hover tooltip 展示全部
      const sigDates: string[] = Array.isArray(meta.watch_signal_dates)
        ? [...meta.watch_signal_dates].sort((a, b) => b.localeCompare(a))
        : [];
      const showDates = sigDates.slice(0, 7);
      const hiddenDates = sigDates.slice(7);

      const popContent = (
        <div style={{ fontSize: 12, lineHeight: '22px', maxWidth: 300 }}>
          {meta.trigger && (
            <div style={{ marginBottom: 6 }}>
              <Tag color="blue" style={{ fontSize: 11 }}>{meta.trigger}</Tag>
            </div>
          )}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2px 12px' }}>
            {meta.accumulation_days != null && <><span style={{ color: '#8892a4' }}>观察期</span><span><strong>{meta.accumulation_days}</strong> 天</span></>}
            {conf > 0 && <><span style={{ color: '#8892a4' }}>贴合度</span><span style={{ color: confColor, fontWeight: 600 }}>{Math.round(conf * 100)} 分</span></>}
            {meta.rsi != null && <><span style={{ color: '#8892a4' }}>RSI</span><span><strong>{meta.rsi}</strong></span></>}
            {meta.yy_ratio != null && <><span style={{ color: '#8892a4' }}>阳阴量比</span><span><strong>{meta.yy_ratio}</strong></span></>}
            {meta.near_low_pct != null && <><span style={{ color: '#8892a4' }}>距近期低点</span><span><strong>{meta.near_low_pct}%</strong></span></>}
            {meta.ma_converge_pct != null && <><span style={{ color: '#8892a4' }}>均线粘合</span><span><strong>{meta.ma_converge_pct}%</strong></span></>}
            {meta.sbv_count > 0 && <><span style={{ color: '#8892a4' }}>缩幅放量</span><span><strong style={{ color: '#f39c12' }}>{meta.sbv_count}</strong> 次</span></>}
            {meta.bb_narrow && <><span style={{ color: '#8892a4' }}>布林带</span><span style={{ color: '#10b981' }}>已收窄</span></>}
          </div>
          {showDates.length > 0 && (
            <div style={{ marginTop: 8, borderTop: '1px solid rgba(255,255,255,0.08)', paddingTop: 6 }}>
              <div style={{ color: '#8892a4', marginBottom: 4 }}>建仓信号日（共{sigDates.length}条）</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
                {showDates.map((d, i) => (
                  <span key={i} style={{ fontSize: 10, background: 'rgba(96,165,250,0.12)', color: '#60a5fa', padding: '1px 5px', borderRadius: 3 }}>{d}</span>
                ))}
                {hiddenDates.length > 0 && (
                  <Tooltip
                    title={
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3, maxWidth: 240 }}>
                        {hiddenDates.map((d, i) => (
                          <span key={i} style={{ fontSize: 10, background: 'rgba(96,165,250,0.12)', color: '#60a5fa', padding: '1px 5px', borderRadius: 3 }}>{d}</span>
                        ))}
                      </div>
                    }
                    placement="bottom"
                  >
                    <span style={{ fontSize: 10, color: '#8892a4', cursor: 'help', padding: '1px 5px', borderRadius: 3, background: 'rgba(255,255,255,0.06)' }}>
                      +{hiddenDates.length} 更多
                    </span>
                  </Tooltip>
                )}
              </div>
            </div>
          )}
          {!hasMeta && t.buy_reason && <div style={{ color: '#c0c8d8' }}>{t.buy_reason}</div>}
        </div>
      );

      if (!hasMeta && !t.buy_reason) return <Text type="secondary" style={{ fontSize: 11 }}>—</Text>;

      return (
        <Popover content={popContent} title="买入分析" trigger="click" overlayStyle={{ maxWidth: 320 }}>
          <div style={{ cursor: 'pointer' }}>
            <Text style={{ color: '#60a5fa', fontSize: 11 }}>
              {meta.trigger || (t.buy_reason || '').split('|')[0].replace('建仓完毕: ', '') || '查看'}
            </Text>
            {conf > 0 && (
              <><br /><Text style={{ fontSize: 10, color: confColor }}>{Math.round(conf * 100)}分</Text>
              {meta.accumulation_days > 0 && <Text type="secondary" style={{ fontSize: 10 }}> · {meta.accumulation_days}天</Text>}</>
            )}
          </div>
        </Popover>
      );
    }},
    { title: '卖出原因', dataIndex: 'sell_reason', key: 'sr', width: 140, render: (v: string) => (
      <Tooltip title={v} placement="topLeft">
        <Text style={{ fontSize: 11, color: '#8892a4', cursor: 'default' }}
          ellipsis>{v || '—'}</Text>
      </Tooltip>
    )},
  ];

  // ── ECharts option（useMemo 避免缩放重置）───────────
  const equityOption = useMemo(() => {
    if (!equityModal) return {};
    const { dates, values, initialCash } = equityModal;
    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(22,27,37,0.95)',
        borderColor: 'rgba(255,255,255,0.1)',
        textStyle: { color: '#e6edf3', fontSize: 12 },
        formatter: (params: any) => {
          const p = params[0];
          const asset = Number(p.value);
          const pnl = asset - initialCash;
          const pct = ((asset / initialCash - 1) * 100).toFixed(2);
          const fmtV = (v: number) => v.toLocaleString('zh-CN', {minimumFractionDigits:2, maximumFractionDigits:2});
          return `<div style="font-size:12px">
            <div style="color:#8892a4;margin-bottom:4px">${p.axisValue}</div>
            <div>总资产: <b style="color:#fff">¥${fmtV(asset)}</b></div>
            <div>盈亏: <b style="color:${pnl >= 0 ? '#10b981' : '#ef4444'}">${pnl >= 0 ? '+' : ''}¥${fmtV(pnl)} (${pnl >= 0 ? '+' : ''}${pct}%)</b></div>
          </div>`;
        },
      },
      grid: { top: 40, right: 30, bottom: 70, left: 80 },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: { color: '#556070', fontSize: 10, interval: Math.floor(dates.length / 8) },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        splitLine: { show: false },
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          color: '#556070', fontSize: 10,
          formatter: (v: number) => v >= 10000 ? (v / 10000).toFixed(1) + '万' : v.toLocaleString(),
        },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } },
      },
      series: [{
        name: '总资产',
        type: 'line',
        data: values,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 2, color: '#3b82f6' },
        areaStyle: {
          color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(59,130,246,0.25)' },
              { offset: 1, color: 'rgba(59,130,246,0.02)' },
            ],
          },
        },
        markLine: {
          silent: true, symbol: 'none',
          lineStyle: { color: '#f59e0b', type: 'dashed', width: 1 },
          data: [{ yAxis: initialCash, label: { formatter: `初始 ¥${initialCash.toLocaleString()}`, color: '#f59e0b', fontSize: 11 } }],
        },
      }],
      dataZoom: [
        { type: 'slider', xAxisIndex: 0, bottom: 10, height: 24,
          borderColor: 'rgba(255,255,255,0.1)', fillerColor: 'rgba(59,130,246,0.15)',
          textStyle: { color: '#556070', fontSize: 10 } },
        { type: 'inside', xAxisIndex: 0 },
      ],
    };
  }, [equityModal]);

  // ── 报告弹窗：年度收益柱图 ────────────────────────────
  const annualBarOption = useMemo(() => {
    if (!reportModal?.annual) return {};
    const years = Object.keys(reportModal.annual).sort();
    const rets = years.map(y => reportModal.annual[y].ret);
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis', formatter: (p: any) => `${p[0].name}年: ${p[0].value >= 0 ? '+' : ''}${p[0].value}%` },
      grid: { left: 50, right: 20, top: 30, bottom: 30 },
      xAxis: { type: 'category', data: years, axisLabel: { color: '#8892a4', fontSize: 12 } },
      yAxis: { type: 'value', axisLabel: { color: '#8892a4', fontSize: 11, formatter: (v: number) => `${v}%` }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } } },
      series: [{
        type: 'bar', data: rets.map(v => ({
          value: v,
          itemStyle: { color: v >= 0 ? '#10b981' : '#ef4444', borderRadius: [4, 4, 0, 0] },
          label: { show: true, position: v >= 0 ? 'top' : 'bottom', formatter: (p: any) => `${p.value >= 0 ? '+' : ''}${p.value}%`, color: '#e2e8f0', fontSize: 11 },
        })),
      }],
    };
  }, [reportModal?.annual]);

  // ── 报告弹窗：参数热力图 ──────────────────────────────
  const sweepHeatmapOption = useMemo(() => {
    if (!sweepResult?.matrix) return {};
    const { p1, p2, matrix } = sweepResult;
    const flat: [number, number, number | null][] = [];
    matrix.forEach((row: (number | null)[], xi: number) => {
      row.forEach((v: number | null, yi: number) => flat.push([xi, yi, v]));
    });
    const validVals = flat.map(d => d[2]).filter(v => v !== null) as number[];
    const minV = Math.min(...validVals);
    const maxV = Math.max(...validVals);
    return {
      backgroundColor: 'transparent',
      tooltip: { formatter: (p: any) => `${p1.label}=${p1.values[p.data[0]]}  ${p2.label}=${p2.values[p.data[1]]}<br/>年化收益: ${p.data[2] !== null ? `${p.data[2] >= 0 ? '+' : ''}${p.data[2]}%` : 'N/A'}` },
      grid: { left: 80, right: 20, top: 60, bottom: 60 },
      xAxis: { type: 'category', data: p1.values.map(String), name: p1.label, nameLocation: 'middle', nameGap: 30, axisLabel: { color: '#8892a4', fontSize: 11 } },
      yAxis: { type: 'category', data: p2.values.map(String), name: p2.label, nameLocation: 'middle', nameGap: 40, axisLabel: { color: '#8892a4', fontSize: 11 } },
      visualMap: { min: minV, max: maxV, calculable: true, orient: 'horizontal', left: 'center', top: 10, textStyle: { color: '#8892a4', fontSize: 11 }, inRange: { color: ['#ef4444', '#374151', '#10b981'] } },
      series: [{ type: 'heatmap', data: flat, label: { show: true, formatter: (p: any) => p.data[2] !== null ? `${p.data[2] >= 0 ? '+' : ''}${p.data[2]}%` : '--', color: '#e2e8f0', fontSize: 10 } }],
    };
  }, [sweepResult]);

  // ── 报告弹窗：全样本曲线（带回撤标注）─────────────────
  const reportEquityOption = useMemo(() => {
    if (!reportModal?.dates) return {};
    const { dates, absValues, cash, ddPeriods } = reportModal;
    const markAreas = (ddPeriods || []).slice(0, 3).map((dd: any, i: number) => ([
      { xAxis: dd.peak_date, itemStyle: { color: `rgba(239,68,68,${0.12 - i * 0.03})` }, name: `回撤${i + 1}: -${dd.drawdown_pct}%` },
      { xAxis: dd.recovery_date || dates[dates.length - 1] },
    ]));
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis', backgroundColor: 'rgba(22,27,37,0.95)', borderColor: '#2d3748', textStyle: { color: '#e2e8f0', fontSize: 12 }, formatter: (p: any) => `${p[0].axisValue}<br/>资产: ¥${Number(p[0].value).toLocaleString('zh-CN', { minimumFractionDigits: 2 })}` },
      grid: { left: 70, right: 20, top: 40, bottom: 60 },
      xAxis: { type: 'category', data: dates, axisLabel: { color: '#8892a4', fontSize: 10, interval: Math.floor(dates.length / 8) } },
      yAxis: { type: 'value', axisLabel: { color: '#8892a4', fontSize: 11, formatter: (v: number) => `¥${(v / 10000).toFixed(0)}w` }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } } },
      dataZoom: [{ type: 'slider', bottom: 10, height: 20, borderColor: 'rgba(255,255,255,0.1)', fillerColor: 'rgba(59,130,246,0.15)', textStyle: { color: '#556070', fontSize: 10 } }, { type: 'inside' }],
      series: [{
        type: 'line', data: absValues, smooth: true, symbol: 'none',
        lineStyle: { color: '#3b82f6', width: 2 },
        areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: 'rgba(59,130,246,0.3)' }, { offset: 1, color: 'rgba(59,130,246,0.02)' }] } },
        markLine: { symbol: 'none', data: [{ yAxis: cash, lineStyle: { color: 'rgba(255,255,255,0.2)', type: 'dashed' }, label: { formatter: '初始资金', color: '#8892a4', fontSize: 10 } }] },
        markArea: markAreas.length > 0 ? { data: markAreas, label: { show: true, position: 'insideTopLeft', color: '#ef4444', fontSize: 10 } } : undefined,
      }],
    };
  }, [reportModal]);

  // ── 走势图弹窗中的 KPI（直接使用后端数据，保证一致）──
  const eq = equityModal;

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>回测中心</Title>
        <Radio.Group value={mode} onChange={e => setMode(e.target.value)} size="small" buttonStyle="solid">
          <Radio.Button value="normal"><PlayCircleOutlined /> 普通回测</Radio.Button>
          <Radio.Button value="oos"><ExperimentOutlined /> 样本外测试</Radio.Button>
        </Radio.Group>
      </div>

      {/* ── 普通回测参数 ── */}
      {mode === 'normal' && (
        <Card size="small" style={{ marginBottom: 16 }}>
          <Space wrap size="middle">
            <div>
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>策略</Text>
              <Select value={form.strategy} onChange={v => setForm({ ...form, strategy: v })} options={strategies} style={{ width: 140 }} />
            </div>
            <div>
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>开始日期</Text>
              <DatePicker
                value={startDayjs}
                onChange={d => d && setForm({ ...form, start: d.format('YYYY-MM-DD') })}
                allowClear={false}
                style={{ width: 140 }}
                minDate={dayjs('2000-01-01')}
                maxDate={dayjs('2099-12-31')}
              />
            </div>
            <div>
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>结束日期</Text>
              <DatePicker
                value={endDayjs}
                onChange={d => d && setForm({ ...form, end: d.format('YYYY-MM-DD') })}
                allowClear={false}
                style={{ width: 140 }}
                minDate={dayjs('2000-01-01')}
                maxDate={dayjs('2099-12-31')}
              />
            </div>
            <div>
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>初始资金</Text>
              <InputNumber value={form.cash} onChange={v => v && setForm({ ...form, cash: v })} formatter={v => `¥ ${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')} style={{ width: 160 }} />
            </div>
            <div>
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>数据来源</Text>
              <Select
                value={form.data_source}
                onChange={v => setForm({ ...form, data_source: v })}
                style={{ width: 150 }}
                options={[
                  { value: 'cache',    label: '📁 本地缓存' },
                  { value: 'local_db', label: '🗄️ 本地数据库' },
                ]}
              />
            </div>
            <div style={{ alignSelf: 'flex-end' }}>
              <Button type="primary" icon={running ? <LoadingOutlined /> : <PlayCircleOutlined />} loading={running} onClick={runBacktest}>
                {running ? `回测中 ${fmtElapsed(elapsed)}` : '启动回测'}
              </Button>
            </div>
          </Space>
        </Card>
      )}

      {/* ── 样本外测试参数 ── */}
      {mode === 'oos' && (
        <Card
          size="small"
          style={{ marginBottom: 16, borderColor: '#7c3aed', background: 'rgba(124,58,237,0.04)' }}
          title={
            <Space wrap>
              <ExperimentOutlined style={{ color: '#7c3aed' }} />
              <Text style={{ color: '#7c3aed', fontWeight: 600 }}>样本外测试 — 防过拟合验证</Text>
              <Text type="secondary" style={{ fontSize: 12, fontWeight: 400 }}>
                {oosHasTrain ? '用训练集调参，用测试集验证，两段均正收益才是真策略' : '仅运行测试区间，不做过拟合判定'}
              </Text>
              <Tag color="orange" style={{ fontSize: 11 }}>需使用本地数据库（缓存无历史数据）</Tag>
            </Space>
          }
        >
          <Space wrap size="middle">
            <div>
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>策略</Text>
              <Select value={oosForm.strategy} onChange={v => setOosForm({ ...oosForm, strategy: v })} options={strategies} style={{ width: 140 }} />
            </div>
            {/* 训练集（可选） */}
            <div style={{ borderLeft: `3px solid ${oosHasTrain ? '#3b82f6' : '#d1d5db'}`, paddingLeft: 10 }}>
              <Space size={6} style={{ marginBottom: 4 }}>
                <Switch size="small" checked={oosHasTrain} onChange={setOosHasTrain} />
                <Text style={{ fontSize: 12, color: oosHasTrain ? '#3b82f6' : '#9ca3af', fontWeight: 600 }}>
                  训练集{oosHasTrain ? '（用于调参）' : '（已关闭）'}
                </Text>
              </Space>
              {oosHasTrain && (
                <Space size={6}>
                  <DatePicker
                    value={dayjs(oosForm.train_start)}
                    onChange={d => d && setOosForm({ ...oosForm, train_start: d.format('YYYY-MM-DD') })}
                    allowClear={false} style={{ width: 130 }} placeholder="训练开始"
                    minDate={dayjs('2000-01-01')} maxDate={dayjs('2099-12-31')}
                  />
                  <Text type="secondary">~</Text>
                  <DatePicker
                    value={dayjs(oosForm.train_end)}
                    onChange={d => d && setOosForm({ ...oosForm, train_end: d.format('YYYY-MM-DD') })}
                    allowClear={false} style={{ width: 130 }} placeholder="训练结束"
                    minDate={dayjs('2000-01-01')} maxDate={dayjs('2099-12-31')}
                  />
                </Space>
              )}
            </div>
            {/* 测试集 */}
            <div style={{ borderLeft: '3px solid #f59e0b', paddingLeft: 10 }}>
              <Text style={{ fontSize: 12, color: '#f59e0b', display: 'block', marginBottom: 4, fontWeight: 600 }}>测试集（样本外验证）</Text>
              <Space size={6}>
                <DatePicker
                  value={dayjs(oosForm.test_start)}
                  onChange={d => d && setOosForm({ ...oosForm, test_start: d.format('YYYY-MM-DD') })}
                  allowClear={false} style={{ width: 130 }} placeholder="测试开始"
                  minDate={dayjs('2000-01-01')} maxDate={dayjs('2099-12-31')}
                />
                <Text type="secondary">~</Text>
                <DatePicker
                  value={dayjs(oosForm.test_end)}
                  onChange={d => d && setOosForm({ ...oosForm, test_end: d.format('YYYY-MM-DD') })}
                  allowClear={false} style={{ width: 130 }} placeholder="测试结束"
                  minDate={dayjs('2000-01-01')} maxDate={dayjs('2099-12-31')}
                />
              </Space>
            </div>
            <div>
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>初始资金</Text>
              <InputNumber value={oosForm.cash} onChange={v => v && setOosForm({ ...oosForm, cash: v })} formatter={v => `¥ ${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')} style={{ width: 160 }} />
            </div>
            <div>
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>数据来源</Text>
              <Select
                value={oosForm.data_source}
                onChange={v => setOosForm({ ...oosForm, data_source: v })}
                style={{ width: 150 }}
                options={[
                  { value: 'cache',    label: '📁 本地缓存' },
                  { value: 'local_db', label: '🗄️ 本地数据库' },
                ]}
              />
            </div>
            <div style={{ alignSelf: 'flex-end' }}>
              <Button
                type="primary"
                style={{ background: '#7c3aed', borderColor: '#7c3aed' }}
                icon={running ? <LoadingOutlined /> : <ExperimentOutlined />}
                loading={running}
                onClick={runOosBacktest}
              >
                {running ? `测试中 ${fmtElapsed(elapsed)}` : '启动样本外测试'}
              </Button>
            </div>
          </Space>
        </Card>
      )}

      {/* 回测进度面板 */}
      {running && (() => {
        const { pct, label } = inferStage(progressLogs);
        const recentLogs = progressLogs.slice(-5);
        return (
          <Card
            size="small"
            style={{ marginBottom: 16, borderColor: '#3b82f6', background: 'rgba(59,130,246,0.04)' }}
            title={
              <Space>
                <SyncOutlined spin style={{ color: '#3b82f6' }} />
                <Text style={{ color: '#3b82f6', fontWeight: 600 }}>回测进行中</Text>
                <Text type="secondary" style={{ fontSize: 12 }}>已用时 {fmtElapsed(elapsed)}</Text>
              </Space>
            }
          >
            <Progress
              percent={pct}
              status={pct === 100 ? 'success' : 'active'}
              strokeColor={{ '0%': '#3b82f6', '100%': '#10b981' }}
              style={{ marginBottom: 12 }}
              format={() => <Text style={{ fontSize: 12, color: pct === 100 ? '#10b981' : '#3b82f6' }}>{label}</Text>}
            />
            {recentLogs.length > 0 && (
              <div style={{
                background: 'rgba(0,0,0,0.25)',
                borderRadius: 6,
                padding: '8px 12px',
                fontFamily: 'monospace',
                fontSize: 12,
                maxHeight: 120,
                overflowY: 'auto',
              }}>
                {recentLogs.map((log, i) => (
                  <div key={i} style={{
                    color: log.includes('失败') || log.includes('错误') ? '#ef4444'
                         : log.includes('完成') || log.includes('✓')   ? '#10b981'
                         : '#94a3b8',
                    lineHeight: '1.6',
                  }}>
                    {i === recentLogs.length - 1
                      ? <><SyncOutlined spin style={{ marginRight: 6, color: '#3b82f6' }} />{log}</>
                      : <><CheckCircleOutlined style={{ marginRight: 6, color: '#475569' }} />{log}</>
                    }
                  </div>
                ))}
              </div>
            )}
          </Card>
        );
      })()}

      {/* ── 样本外测试结果 ── */}
      {mode === 'oos' && oosResult && (() => {
        const tm = oosResult.train_metrics || {};
        const sm = oosResult.test_metrics  || {};
        const verdict: string = oosResult.verdict || 'FAIL';
        const verdictConfig = verdict === 'PASS'
          ? { color: '#10b981', bg: 'rgba(16,185,129,0.08)', border: '#10b981', icon: <CheckOutlined />, label: '通过' }
          : verdict === 'WARN'
          ? { color: '#f59e0b', bg: 'rgba(245,158,11,0.08)', border: '#f59e0b', icon: <WarningOutlined />, label: '警告' }
          : verdict === 'N/A'
          ? { color: '#6366f1', bg: 'rgba(99,102,241,0.08)', border: '#6366f1', icon: <ExperimentOutlined />, label: '仅测试' }
          : { color: '#ef4444', bg: 'rgba(239,68,68,0.08)', border: '#ef4444', icon: <CloseCircleOutlined />, label: '过拟合' };
        const hasTrain = !!(oosResult.train_start && oosResult.train_end);
        const stratLabel = strategies.find(s => s.value === oosResult.strategy)?.label ?? oosResult.strategy;

        const MetricCell = ({ title, value, color }: { title: string; value: string | number; color?: string }) => (
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 11, color: '#556070', marginBottom: 2 }}>{title}</div>
            <div style={{ fontSize: 14, fontWeight: 600, color: color || '#e2e8f0' }}>{value ?? '--'}</div>
          </div>
        );

        return (
          <Card
            size="small"
            style={{ marginBottom: 16, borderColor: verdictConfig.border, background: verdictConfig.bg }}
            title={
              <Space>
                <ExperimentOutlined style={{ color: '#7c3aed' }} />
                <Text style={{ color: '#7c3aed', fontWeight: 600 }}>样本外测试结果 — {stratLabel}</Text>
                <Tag
                  color={verdict === 'PASS' ? 'success' : verdict === 'WARN' ? 'warning' : 'error'}
                  icon={verdictConfig.icon}
                  style={{ fontSize: 13, padding: '0 10px', fontWeight: 700 }}
                >
                  {verdictConfig.label}
                </Tag>
              </Space>
            }
          >
            <Alert
              type={verdict === 'PASS' ? 'success' : verdict === 'WARN' ? 'warning' : verdict === 'N/A' ? 'info' : 'error'}
              message={oosResult.verdict_reason}
              style={{ marginBottom: 16, fontSize: 13 }}
              showIcon
            />
            {/* 过拟合评分面板（5 维度） */}
            {hasTrain && oosResult.oof_score != null && oosResult.score_detail && (() => {
              const d = oosResult.score_detail;
              const score = oosResult.oof_score as number;
              const scoreColor = score >= 75 ? '#10b981' : score >= 50 ? '#f59e0b' : '#ef4444';
              const dims = [
                { label: '年化一致性', pts: d.ann_pts, tip: `test/train = ${(d.ann_ratio * 100).toFixed(0)}%`, full: 20 },
                { label: '夏普一致性', pts: d.sh_pts,  tip: `test/train = ${(d.sh_ratio  * 100).toFixed(0)}%`, full: 20 },
                { label: '回撤恶化',   pts: d.dd_pts,  tip: `test/train = ${d.dd_ratio.toFixed(2)}×`, full: 20 },
                { label: '胜率漂移',   pts: d.wr_pts,  tip: `差值 ${d.wr_drift.toFixed(1)}pp`, full: 20 },
                { label: '样本充分性', pts: d.n_pts,   tip: `${d.test_n} 笔交易`, full: 20 },
              ];
              return (
                <div style={{ marginBottom: 16, padding: '12px 16px', border: `1px solid ${scoreColor}44`, borderRadius: 8, background: 'rgba(148,163,184,0.04)' }}>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: 12, marginBottom: 10 }}>
                    <Text strong style={{ fontSize: 13 }}>过拟合评分</Text>
                    <Text style={{ fontSize: 22, fontWeight: 700, color: scoreColor, lineHeight: 1 }}>{score}</Text>
                    <Text type="secondary" style={{ fontSize: 12 }}>/ 100</Text>
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      ≥75 PASS · 50–75 WARN · &lt;50 FAIL
                    </Text>
                  </div>
                  <Row gutter={[16, 8]}>
                    {dims.map(({ label, pts, tip, full }) => {
                      const pct = (pts / full) * 100;
                      const color = pct >= 75 ? '#10b981' : pct >= 50 ? '#f59e0b' : pct >= 25 ? '#fb923c' : '#ef4444';
                      return (
                        <Col span={8} key={label}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11 }}>
                            <Text style={{ fontSize: 11, width: 70, flexShrink: 0 }}>{label}</Text>
                            <div style={{ flex: 1, height: 6, background: 'rgba(148,163,184,0.15)', borderRadius: 3, overflow: 'hidden' }}>
                              <div style={{ width: `${pct}%`, height: '100%', background: color, transition: 'width .3s' }} />
                            </div>
                            <Text style={{ fontSize: 11, color, width: 32, textAlign: 'right', fontWeight: 600 }}>{pts}/{full}</Text>
                            <Tooltip title={tip}><Text type="secondary" style={{ fontSize: 10, width: 60 }}>{tip}</Text></Tooltip>
                          </div>
                        </Col>
                      );
                    })}
                  </Row>
                </div>
              );
            })()}
            <Row gutter={16}>
              {/* 训练集（有才展示） */}
              {hasTrain && (
                <Col span={12}>
                  <div style={{ border: '1px solid rgba(59,130,246,0.3)', borderRadius: 8, padding: '12px 16px', background: 'rgba(59,130,246,0.05)' }}>
                    <div style={{ color: '#3b82f6', fontWeight: 600, marginBottom: 12, fontSize: 13 }}>
                      训练集 &nbsp;<Text type="secondary" style={{ fontSize: 12, fontWeight: 400 }}>{oosResult.train_start} ~ {oosResult.train_end}</Text>
                    </div>
                    <Row gutter={[8, 12]}>
                      <Col span={8}><MetricCell title="区间收益" value={tm.total_return || '--'} color={pctColor(tm.total_return)} /></Col>
                      <Col span={8}><MetricCell title="年化收益" value={tm.annualized_return || '--'} color={pctColor(tm.annualized_return)} /></Col>
                      <Col span={8}><MetricCell title="最大回撤" value={tm.max_drawdown || '--'} color="#ef4444" /></Col>
                      <Col span={8}><MetricCell title="夏普比率" value={tm.sharpe_ratio ?? '--'} /></Col>
                      <Col span={8}><MetricCell title="胜率" value={tm.win_rate || '--'} /></Col>
                      <Col span={8}><MetricCell title="盈亏比" value={tm.profit_factor ?? '--'} /></Col>
                      <Col span={12}><MetricCell title="区间盈亏" value={fmtMoney(tm.period_profit)} color={moneyColor(tm.period_profit ?? 0)} /></Col>
                      <Col span={12}><MetricCell title="总交易" value={`${tm.total_trades ?? 0} 笔`} /></Col>
                    </Row>
                  </div>
                </Col>
              )}
              {/* 测试集 */}
              <Col span={hasTrain ? 12 : 24}>
                <div style={{ border: `1px solid ${verdictConfig.border}44`, borderRadius: 8, padding: '12px 16px', background: `${verdictConfig.bg}` }}>
                  <div style={{ color: verdictConfig.color, fontWeight: 600, marginBottom: 12, fontSize: 13 }}>
                    测试集{hasTrain ? '（样本外）' : ''}&nbsp;<Text type="secondary" style={{ fontSize: 12, fontWeight: 400 }}>{oosResult.test_start} ~ {oosResult.test_end}</Text>
                  </div>
                  <Row gutter={[8, 12]}>
                    <Col span={hasTrain ? 8 : 4}><MetricCell title="区间收益" value={sm.total_return || '--'} color={pctColor(sm.total_return)} /></Col>
                    <Col span={hasTrain ? 8 : 4}><MetricCell title="年化收益" value={sm.annualized_return || '--'} color={pctColor(sm.annualized_return)} /></Col>
                    <Col span={hasTrain ? 8 : 4}><MetricCell title="最大回撤" value={sm.max_drawdown || '--'} color="#ef4444" /></Col>
                    <Col span={hasTrain ? 8 : 4}><MetricCell title="夏普比率" value={sm.sharpe_ratio ?? '--'} /></Col>
                    <Col span={hasTrain ? 8 : 4}><MetricCell title="胜率" value={sm.win_rate || '--'} /></Col>
                    <Col span={hasTrain ? 8 : 4}><MetricCell title="盈亏比" value={sm.profit_factor ?? '--'} /></Col>
                    <Col span={hasTrain ? 12 : 4}><MetricCell title="区间盈亏" value={fmtMoney(sm.period_profit)} color={moneyColor(sm.period_profit ?? 0)} /></Col>
                    <Col span={hasTrain ? 12 : 4}><MetricCell title="总交易" value={`${sm.total_trades ?? 0} 笔`} /></Col>
                  </Row>
                </div>
              </Col>
            </Row>
            {/* 指标对比条（仅有训练集时展示） */}
            {hasTrain && (
            <><Divider style={{ margin: '16px 0 12px' }} />
            <Row gutter={[16, 8]}>
              {[
                { label: '区间收益', train: oosResult.train_ret, test: oosResult.test_ret, unit: '%' },
                { label: '夏普比率', train: tm.sharpe_ratio, test: sm.sharpe_ratio, unit: '' },
              ].map(({ label, train, test, unit }) => {
                const t = Number(train) || 0;
                const s = Number(test)  || 0;
                const decay = t !== 0 ? ((s - t) / Math.abs(t) * 100).toFixed(1) : '--';
                return (
                  <Col span={12} key={label}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <Text type="secondary" style={{ fontSize: 12, width: 60, flexShrink: 0 }}>{label}</Text>
                      <Text style={{ fontSize: 12, color: '#3b82f6', width: 60, textAlign: 'right' }}>
                        {t >= 0 ? '+' : ''}{t.toFixed(2)}{unit}
                      </Text>
                      <Text type="secondary" style={{ fontSize: 11 }}>→</Text>
                      <Text style={{ fontSize: 12, color: s >= 0 ? '#10b981' : '#ef4444', width: 60, textAlign: 'right' }}>
                        {s >= 0 ? '+' : ''}{s.toFixed(2)}{unit}
                      </Text>
                      <Text style={{ fontSize: 11, color: '#556070' }}>
                        变化 {decay !== '--' ? `${Number(decay) >= 0 ? '+' : ''}${decay}%` : '--'}
                      </Text>
                    </div>
                  </Col>
                );
              })}
            </Row></>
            )}
          </Card>
        );
      })()}

      {/* 回测历史 */}
      {mode === 'normal' && (
        <Card size="small" title="回测历史" style={{ marginBottom: 16 }}>
          <Table
            dataSource={btList.filter((r: any) => !r.oos)}
            columns={columns}
            rowKey="id"
            size="small"
            pagination={{ pageSize: 10 }}
            scroll={{ x: 1500 }}
            locale={{ emptyText: <Empty description="暂无回测记录，点击「启动回测」开始" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
          />
        </Card>
      )}

      {/* 样本外测试历史 */}
      {mode === 'oos' && (
        <Card size="small" title="样本外测试历史" style={{ marginBottom: 16 }}>
          <Table
            dataSource={btList.filter((r: any) => r.oos)}
            columns={columns}
            rowKey="id"
            size="small"
            pagination={{ pageSize: 10 }}
            scroll={{ x: 1500 }}
            locale={{ emptyText: <Empty description="暂无样本外测试记录，点击「启动测试」开始" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
          />
        </Card>
      )}

      {/* 资产走势弹窗 */}
      <Modal
        title={eq ? `资产走势 — ${eq.strategy} (${eq.start} ~ ${eq.end})` : ''}
        open={!!equityModal}
        onCancel={() => setEquityModal(null)}
        footer={null}
        width={960}
        centered
        destroyOnClose
      >
        {eq && (
          <>
            <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
              <Col span={4}><Statistic title="初始资金" value={fmtAsset(eq.initialCash)} valueStyle={{ fontSize: 14 }} /></Col>
              <Col span={4}><Statistic title="最终资产" value={fmtAsset(eq.final_value)} valueStyle={{ fontSize: 14 }} /></Col>
              <Col span={4}><Statistic title="区间盈亏" value={fmtMoney(eq.period_profit)} valueStyle={{ fontSize: 14, color: moneyColor(eq.period_profit ?? 0) }} /></Col>
              <Col span={3}><Statistic title="区间收益" value={eq.total_return || '--'} valueStyle={{ fontSize: 14, color: pctColor(eq.total_return) }} /></Col>
              <Col span={3}><Statistic title="年化收益" value={eq.annualized_return || '--'} valueStyle={{ fontSize: 14, color: pctColor(eq.annualized_return), fontWeight: 600 }} /></Col>
              <Col span={3}><Statistic title="最大回撤" value={eq.max_drawdown || '--'} valueStyle={{ fontSize: 14, color: '#ef4444' }} /></Col>
              <Col span={3}><Statistic title="夏普比率" value={eq.sharpe_ratio ?? '--'} valueStyle={{ fontSize: 14 }} /></Col>
            </Row>
            <ReactECharts
              option={equityOption}
              style={{ height: 420 }}
              theme="dark"
              notMerge={false}
              lazyUpdate={true}
            />
          </>
        )}
      </Modal>

      {/* 交易详情弹窗 */}
      <Modal
        title={td ? `交易详情 — ${td.strategy} (${td.start} ~ ${td.end})` : ''}
        open={!!tradeModal}
        onCancel={() => setTradeModal(null)}
        footer={null}
        width={1100}
        centered
        destroyOnClose
      >
        {td && (
          <>
            <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
              <Col span={3}><Statistic title="总交易" value={tdTrades.length} suffix="笔" valueStyle={{ fontSize: 16 }} /></Col>
              <Col span={3}><Statistic title="盈利" value={wins} suffix="笔" valueStyle={{ fontSize: 16, color: '#10b981' }} /></Col>
              <Col span={3}><Statistic title="亏损" value={losses} suffix="笔" valueStyle={{ fontSize: 16, color: '#ef4444' }} /></Col>
              {holding > 0 && <Col span={2}><Statistic title="持仓中" value={holding} suffix="笔" valueStyle={{ fontSize: 16, color: '#f59e0b' }} /></Col>}
              <Col span={3}><Statistic title="胜率" value={winRate + '%'} valueStyle={{ fontSize: 16 }} /></Col>
              <Col span={4}><Statistic title="已实现盈亏" value={fmtMoney(realizedPnl)} valueStyle={{ fontSize: 14, color: moneyColor(realizedPnl) }} /></Col>
              {holding > 0 && <Col span={3}><Statistic title="持仓浮盈亏" value={fmtMoney(unrealizedPnl)} valueStyle={{ fontSize: 13, color: moneyColor(unrealizedPnl) }} /></Col>}
              <Col span={4}><Statistic title="总盈亏" value={fmtMoney(totalProfit)} valueStyle={{ fontSize: 16, fontWeight: 700, color: moneyColor(totalProfit) }} /></Col>
            </Row>
            {/* 搜索 + 盈亏筛选工具栏 */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 10, flexWrap: 'wrap' }}>
              <Input
                placeholder="搜索股票名称 / 代码"
                prefix={<SearchOutlined style={{ color: '#556070' }} />}
                allowClear
                value={tradeSearch}
                onChange={e => setTradeSearch(e.target.value)}
                style={{ width: 200 }}
                size="small"
              />
              <Radio.Group
                size="small"
                value={tradePnlFilter}
                onChange={e => setTradePnlFilter(e.target.value)}
              >
                <Radio.Button value="all">全部 {tdTrades.length}</Radio.Button>
                <Radio.Button value="win">
                  <span style={{ color: '#10b981' }}>盈利 {wins}</span>
                </Radio.Button>
                <Radio.Button value="loss">
                  <span style={{ color: '#ef4444' }}>亏损 {losses}</span>
                </Radio.Button>
                {holding > 0 && (
                  <Radio.Button value="holding">
                    <span style={{ color: '#f59e0b' }}>持仓中 {holding}</span>
                  </Radio.Button>
                )}
              </Radio.Group>
              {(tradeSearch || tradePnlFilter !== 'all') && (
                <Text type="secondary" style={{ fontSize: 12 }}>
                  显示 {filteredTrades.length} / {tdTrades.length} 条
                </Text>
              )}
            </div>
            <Table
              dataSource={filteredTrades}
              columns={tradeColumns}
              rowKey={(_: any, i: any) => i!}
              size="small"
              pagination={{ pageSize: 20, showSizeChanger: false, showTotal: (t) => `共 ${t} 笔` }}
              scroll={{ x: 1000, y: 380 }}
              locale={{ emptyText: <Empty description="无匹配记录" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
            />
          </>
        )}
      </Modal>
      {/* ── 完整回测报告弹窗 ── */}
      {reportModal && (() => {
        const rm = reportModal;
        const m = rm.metrics;
        const stratLabel = strategies.find(s => s.value === rm.strategy)?.label ?? rm.strategy;
        const annualYears = Object.keys(rm.annual || {}).sort();
        return (
          <Modal
            title={
              <Space>
                <FileTextOutlined style={{ color: '#10b981' }} />
                <Text style={{ color: '#10b981', fontWeight: 600 }}>回测报告 — {stratLabel}</Text>
                <Text type="secondary" style={{ fontSize: 12, fontWeight: 400 }}>{rm.start} ~ {rm.end}</Text>
              </Space>
            }
            open={!!reportModal}
            onCancel={() => { setReportModal(null); setSweepResult(null); }}
            footer={null}
            width={1100}
            centered
            destroyOnClose
          >
            {/* KPI 行 */}
            <Row gutter={[12, 8]} style={{ marginBottom: 16 }}>
              {[
                { title: '年化收益', value: m?.annualized_return || '--', color: pctColor(m?.annualized_return) },
                { title: '区间收益', value: m?.total_return || '--', color: pctColor(m?.total_return) },
                { title: '最大回撤', value: m?.max_drawdown || '--', color: '#ef4444' },
                { title: '夏普比率', value: m?.sharpe_ratio ?? '--' },
                { title: '胜率', value: m?.win_rate || '--' },
                { title: '盈亏比', value: m?.profit_factor ?? '--' },
              ].map(({ title, value, color }) => (
                <Col span={4} key={title}>
                  <Statistic title={title} value={value} valueStyle={{ fontSize: 16, fontWeight: 700, color: color || '#e2e8f0' }} />
                </Col>
              ))}
            </Row>

            <Tabs
              defaultActiveKey="curve"
              size="small"
              items={[
                {
                  key: 'curve',
                  label: <span><LineChartOutlined /> 全样本曲线</span>,
                  children: (
                    <div>
                      <Alert
                        type="info" showIcon style={{ marginBottom: 12, fontSize: 12 }}
                        message="红色阴影区域为最大回撤期，拖动底部滑块可缩放查看细节"
                      />
                      <ReactECharts option={reportEquityOption} style={{ height: 400 }} theme="dark" notMerge />
                      {/* 回撤事件说明 */}
                      {rm.ddPeriods?.length > 0 && (
                        <div style={{ marginTop: 12, display: 'flex', gap: 12, flexWrap: 'wrap' }}>
                          {rm.ddPeriods.map((dd: any, i: number) => (
                            <div key={i} style={{ fontSize: 11, color: '#8892a4', border: '1px solid rgba(239,68,68,0.3)', borderRadius: 6, padding: '4px 10px', background: 'rgba(239,68,68,0.05)' }}>
                              <Text style={{ color: '#ef4444', fontWeight: 600 }}>回撤{i + 1}  -{dd.drawdown_pct}%</Text>
                              &nbsp;&nbsp;{dd.peak_date} → {dd.trough_date}
                              {dd.recovery_date ? <span style={{ color: '#10b981' }}> → 修复 {dd.recovery_date}</span> : <span style={{ color: '#f59e0b' }}> → 未修复</span>}
                              &nbsp;（下跌{dd.down_days}天{dd.recovery_days != null ? `，修复${dd.recovery_days}天` : ''}）
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ),
                },
                {
                  key: 'annual',
                  label: <span><BarChartOutlined /> 分年度收益</span>,
                  children: (
                    <div>
                      <ReactECharts option={annualBarOption} style={{ height: 280 }} theme="dark" notMerge />
                      <Table
                        size="small"
                        style={{ marginTop: 12 }}
                        pagination={false}
                        dataSource={annualYears.map(yr => ({ yr, ...rm.annual[yr] }))}
                        rowKey="yr"
                        columns={[
                          { title: '年份', dataIndex: 'yr', width: 80 },
                          { title: '年初资产', dataIndex: 'start_val', render: (v: number) => fmtAsset(v) },
                          { title: '年末资产', dataIndex: 'end_val', render: (v: number) => fmtAsset(v) },
                          { title: '年度收益', dataIndex: 'ret', render: (v: number) => (
                            <Text style={{ fontWeight: 700, color: v >= 0 ? '#10b981' : '#ef4444' }}>{v >= 0 ? '+' : ''}{v}%</Text>
                          )},
                          { title: '盈亏', key: 'pnl', render: (_: any, row: any) => {
                            const pnl = row.end_val - row.start_val;
                            return <Text style={{ color: pnl >= 0 ? '#10b981' : '#ef4444' }}>{fmtMoney(pnl)}</Text>;
                          }},
                        ]}
                      />
                      <div style={{ marginTop: 10, fontSize: 12, color: '#556070' }}>
                        {annualYears.filter(y => rm.annual[y].ret > 0).length} 年盈利 / {annualYears.filter(y => rm.annual[y].ret <= 0).length} 年亏损
                        &nbsp;·&nbsp;最佳年份: {annualYears.reduce((a, b) => rm.annual[a].ret > rm.annual[b].ret ? a : b)} ({Math.max(...annualYears.map(y => rm.annual[y].ret)) >= 0 ? '+' : ''}{Math.max(...annualYears.map(y => rm.annual[y].ret))}%)
                        &nbsp;·&nbsp;最差年份: {annualYears.reduce((a, b) => rm.annual[a].ret < rm.annual[b].ret ? a : b)} ({Math.min(...annualYears.map(y => rm.annual[y].ret)) >= 0 ? '+' : ''}{Math.min(...annualYears.map(y => rm.annual[y].ret))}%)
                      </div>
                    </div>
                  ),
                },
                {
                  key: 'heatmap',
                  label: <span><HeatMapOutlined /> 参数热力图</span>,
                  children: (
                    <div>
                      <Alert
                        type="warning" showIcon style={{ marginBottom: 12, fontSize: 12 }}
                        message={`将对「${stratLabel}」的 2 个核心参数做网格扫描（${rm.start}~${rm.end}），运行约 ${rm.strategy === 'trend_follow' || rm.strategy === 'rsi_reversal' ? '25' : '16'} 次回测，耗时较长。数据源：${rm.record?.data_source === 'local_db' ? '本地数据库' : '本地缓存'}`}
                      />
                      {!sweepResult && (
                        <div style={{ textAlign: 'center', padding: '40px 0' }}>
                          <Button
                            type="primary"
                            size="large"
                            icon={sweepLoading ? <LoadingOutlined /> : <HeatMapOutlined />}
                            loading={sweepLoading}
                            onClick={runParamSweep}
                            style={{ background: '#7c3aed', borderColor: '#7c3aed' }}
                          >
                            {sweepLoading ? '扫描中，请稍候…' : '生成参数热力图'}
                          </Button>
                          {sweepLoading && <div style={{ marginTop: 16 }}><Spin tip="正在运行参数网格扫描..." /></div>}
                        </div>
                      )}
                      {sweepResult?.matrix && (
                        <>
                          <ReactECharts option={sweepHeatmapOption} style={{ height: 380 }} theme="dark" notMerge />
                          <div style={{ marginTop: 8, fontSize: 12, color: '#556070', textAlign: 'center' }}>
                            绿色=年化收益高，红色=年化收益低。稳定策略应在参数变化时保持绿色区域连续（不突变）。
                          </div>
                        </>
                      )}
                    </div>
                  ),
                },
                {
                  key: 'drawdown',
                  label: <span><FallOutlined /> 最大回撤分析</span>,
                  children: rm.ddPeriods?.length > 0 ? (
                    <div>
                      {rm.ddPeriods.map((dd: any, i: number) => (
                        <div key={i} style={{ marginBottom: 16, border: `1px solid rgba(239,68,68,${0.4 - i * 0.1})`, borderRadius: 8, padding: '12px 16px', background: 'rgba(239,68,68,0.04)' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 10 }}>
                            <Tag color="red" style={{ fontSize: 13, fontWeight: 700 }}>第 {i + 1} 大回撤 -{dd.drawdown_pct}%</Tag>
                            <Text type="secondary" style={{ fontSize: 12 }}>
                              {dd.peak_date} 见顶 → {dd.trough_date} 触底（{dd.down_days} 个交易日）
                              {dd.recovery_date
                                ? <span style={{ color: '#10b981' }}> → {dd.recovery_date} 修复（{dd.recovery_days} 个交易日）</span>
                                : <span style={{ color: '#f59e0b' }}> → 回测期内未完全修复</span>}
                            </Text>
                          </div>
                          <Row gutter={16}>
                            <Col span={6}><Statistic title="回撤幅度" value={`-${dd.drawdown_pct}%`} valueStyle={{ color: '#ef4444', fontSize: 18, fontWeight: 700 }} /></Col>
                            <Col span={6}><Statistic title="下跌历时" value={dd.down_days} suffix="交易日" valueStyle={{ fontSize: 16 }} /></Col>
                            <Col span={6}><Statistic title="修复历时" value={dd.recovery_days ?? '—'} suffix={dd.recovery_days ? '交易日' : ''} valueStyle={{ fontSize: 16, color: dd.recovery_days ? '#10b981' : '#f59e0b' }} /></Col>
                            <Col span={6}><Statistic title="是否修复" value={dd.recovery_date ? '已修复' : '未修复'} valueStyle={{ fontSize: 16, color: dd.recovery_date ? '#10b981' : '#f59e0b', fontWeight: 600 }} /></Col>
                          </Row>
                          <div style={{ marginTop: 10, fontSize: 12, color: '#556070', lineHeight: '20px' }}>
                            <Text type="secondary">分析提示：</Text>
                            {dd.down_days <= 20 && ' 下跌速度快，可能源于系统性风险或突发事件。'}
                            {dd.down_days > 20 && dd.down_days <= 60 && ' 中等持续时间的调整，属于正常市场修正。'}
                            {dd.down_days > 60 && ' 长期缓慢下跌，需检查策略是否在趋势行情中持续逆势操作。'}
                            {dd.recovery_date && dd.recovery_days && dd.recovery_days < dd.down_days && ' 修复速度快于下跌，策略反弹能力良好。'}
                            {dd.recovery_date && dd.recovery_days && dd.recovery_days >= dd.down_days && ' 修复耗时较长，策略在低谷期可能存在过度持仓。'}
                            {!dd.recovery_date && ' 未修复意味着回测结束时仍未回到历史高点，需关注是否处于持续亏损区间。'}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <Empty description="回测期内未检测到显著回撤（>0.5%）" />
                  ),
                },
              ]}
            />
          </Modal>
        );
      })()}
    </div>
  );
}
