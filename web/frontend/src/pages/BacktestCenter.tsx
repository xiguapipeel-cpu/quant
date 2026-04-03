import { useEffect, useState, useMemo } from 'react';
import { Card, Row, Col, Button, Select, DatePicker, InputNumber, Table, Typography, Modal, Statistic, message, Space, Empty, Input, Radio } from 'antd';
import { PlayCircleOutlined, EyeOutlined, LineChartOutlined, LoadingOutlined, SearchOutlined } from '@ant-design/icons';
import { apiFetch } from '../api/client';
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

export default function BacktestCenter() {
  const [btList, setBtList] = useState<any[]>([]);
  const [running, setRunning] = useState(false);
  const [form, setForm] = useState({ strategy: 'major_capital_accumulation', start: '2025-01-01', end: dayjs().format('YYYY-MM-DD'), cash: 100000 });
  const [tradeModal, setTradeModal] = useState<any>(null);
  const [equityModal, setEquityModal] = useState<any>(null);
  const [tradeSearch, setTradeSearch] = useState('');
  const [tradePnlFilter, setTradePnlFilter] = useState<'all' | 'win' | 'loss' | 'holding'>('all');

  const load = async () => {
    const data = await apiFetch('/api/backtest/list').catch(() => []);
    // 最新回测排在前面（按 ID 降序）
    const sorted = (data || []).sort((a: any, b: any) => (b.id ?? 0) - (a.id ?? 0));
    setBtList(sorted);
  };
  useEffect(() => { load(); }, []);

  const runBacktest = async () => {
    setRunning(true);
    try {
      await apiFetch(`/api/backtest/run?strategy=${form.strategy}&start=${form.start}&end=${form.end}&cash=${form.cash}`, 'POST');
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

  // ── Table columns ─────────────────────────────────────
  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 50 },
    { title: '策略', key: 'strategy', render: (_: any, r: any) => <Text strong>{r.strategy}</Text> },
    { title: '区间', key: 'period', render: (_: any, r: any) => <Text type="secondary" style={{ fontSize: 12 }}>{r.start}~{r.end}</Text> },
    { title: '初始资金', key: 'cash', render: (_: any, r: any) => {
      const v = r.metrics?.initial_cash;
      return <Text>{v != null ? fmtAsset(v) : '--'}</Text>;
    }},
    { title: '年化收益', key: 'ann', render: (_: any, r: any) => {
      const v = r.metrics?.annualized_return;
      return <Text style={{ color: pctColor(v), fontWeight: 600 }}>{v || '--'}</Text>;
    }},
    { title: '区间收益', key: 'totalret', render: (_: any, r: any) => {
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
    { title: '操作', key: 'action', width: 200, render: (_: any, r: any) => (
      <Space size={4}>
        <Button type="link" size="small" icon={<EyeOutlined />} onClick={() => showTrades(r.id)}>交易详情</Button>
        <Button type="link" size="small" icon={<LineChartOutlined />} onClick={() => showEquity(r)} style={{ color: '#f59e0b' }}>走势图</Button>
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
    { title: '股票', key: 'stock', render: (_: any, t: any) => <><Text strong>{t.name}</Text><br /><Text type="secondary" style={{ fontSize: 11 }}>{t.code}</Text></> },
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
    { title: '买入原因', dataIndex: 'buy_reason', key: 'br', ellipsis: true, width: 140 },
    { title: '卖出原因', dataIndex: 'sell_reason', key: 'sr', ellipsis: true, width: 140 },
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

  // ── 走势图弹窗中的 KPI（直接使用后端数据，保证一致）──
  const eq = equityModal;

  return (
    <div>
      <Title level={4}>回测中心</Title>

      {/* 回测参数 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <div>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>策略</Text>
            <Select value={form.strategy} onChange={v => setForm({ ...form, strategy: v })} options={strategies} style={{ width: 140 }} />
          </div>
          <div>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>开始日期</Text>
            <DatePicker
              value={dayjs(form.start)}
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
              value={dayjs(form.end)}
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
          <div style={{ alignSelf: 'flex-end' }}>
            <Button type="primary" icon={running ? <LoadingOutlined /> : <PlayCircleOutlined />} loading={running} onClick={runBacktest}>
              {running ? '回测中...' : '启动回测'}
            </Button>
          </div>
        </Space>
      </Card>

      {/* 回测历史 */}
      <Card size="small" title="回测历史" style={{ marginBottom: 16 }}>
        <Table
          dataSource={btList}
          columns={columns}
          rowKey="id"
          size="small"
          pagination={{ pageSize: 10 }}
          scroll={{ x: 1500 }}
          locale={{ emptyText: <Empty description="暂无回测记录，点击「启动回测」开始" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
        />
      </Card>

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
    </div>
  );
}
