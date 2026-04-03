import { useEffect, useState, useMemo } from 'react';
import { Card, Table, Typography, Empty } from 'antd';
import { apiFetch } from '../api/client';
import ReactECharts from 'echarts-for-react';

const { Title } = Typography;

const COLORS = ['#10b981', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899'];

export default function StrategyCompare() {
  const [data, setData] = useState<any>(null);
  const [btList, setBtList] = useState<any[]>([]);

  useEffect(() => {
    apiFetch('/api/backtest/compare').then(setData).catch(() => {});
    apiFetch('/api/backtest/list').then(setBtList).catch(() => {});
  }, []);

  const dates = data?.dates || [];
  const curves = data?.curves || {};
  const strategyNames = Object.keys(curves);

  const option = useMemo(() => {
    if (!dates.length || !strategyNames.length) return {};

    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(22,27,37,0.95)',
        borderColor: 'rgba(255,255,255,0.1)',
        textStyle: { color: '#e6edf3', fontSize: 12 },
        axisPointer: { type: 'cross', crossStyle: { color: 'rgba(255,255,255,0.1)' } },
        formatter: (params: any) => {
          const valid = params.filter((p: any) => p.value != null);
          if (!valid.length) return '';
          let html = `<div style="font-size:12px"><div style="color:#8892a4;margin-bottom:6px">${valid[0].axisValue}</div>`;
          valid.forEach((p: any) => {
            const v = p.value;
            const c = p.color || '#fff';
            html += `<div style="margin:2px 0"><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:${c};margin-right:6px"></span>${p.seriesName}: <b style="color:#fff">${v >= 0 ? '+' : ''}${v.toFixed(2)}%</b></div>`;
          });
          return html + '</div>';
        },
      },
      legend: {
        top: 4, right: 20,
        textStyle: { color: '#8892a4', fontSize: 12 },
        itemWidth: 20, itemHeight: 3,
      },
      grid: { top: 40, right: 20, bottom: 60, left: 60 },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: {
          color: '#556070', fontSize: 10,
          rotate: 30,
          interval: Math.max(Math.floor(dates.length / 12) - 1, 0),
        },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        splitLine: { show: false },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: '#556070', fontSize: 10, formatter: (v: number) => v.toFixed(1) + '%' },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.04)' } },
      },
      series: strategyNames.map((name, idx) => ({
        name,
        type: 'line',
        data: (curves[name] as (number | null)[]).map((v: number | null) =>
          v != null ? Number(((v - 1) * 100).toFixed(2)) : null
        ),
        smooth: true,
        symbol: 'none',
        connectNulls: false,
        lineStyle: { width: 2, color: COLORS[idx % COLORS.length] },
        itemStyle: { color: COLORS[idx % COLORS.length] },
        emphasis: { lineStyle: { width: 3 } },
      })),
      dataZoom: [
        { type: 'slider', bottom: 8, height: 22,
          borderColor: 'rgba(255,255,255,0.1)', fillerColor: 'rgba(59,130,246,0.12)',
          textStyle: { color: '#556070', fontSize: 10 } },
        { type: 'inside' },
      ],
    };
  }, [dates, curves, strategyNames]);

  // Comparison table
  const strategyMap = new Map<string, any>();
  for (const r of btList) {
    if (!strategyMap.has(r.strategy)) strategyMap.set(r.strategy, r);
  }
  const tableData = Array.from(strategyMap.values());

  const columns = [
    { title: '策略', dataIndex: 'strategy', key: 'strategy',
      render: (v: string) => {
        const idx = strategyNames.indexOf(v);
        const color = idx >= 0 ? COLORS[idx % COLORS.length] : '#fff';
        return <span style={{ fontWeight: 600 }}><span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: 2, background: color, marginRight: 8 }} />{v}</span>;
      },
    },
    { title: '年化收益', dataIndex: ['metrics', 'annualized_return'], key: 'ann', render: (v: string) => <span style={{ color: (v || '').toString().includes('-') ? '#ef4444' : '#10b981', fontWeight: 600 }}>{v || '--'}</span> },
    { title: '夏普比率', dataIndex: ['metrics', 'sharpe_ratio'], key: 'sharpe' },
    { title: '最大回撤', dataIndex: ['metrics', 'max_drawdown'], key: 'dd', render: (v: string) => <span style={{ color: '#ef4444' }}>{v || '--'}</span> },
    { title: '胜率', dataIndex: ['metrics', 'win_rate'], key: 'wr' },
    { title: '盈亏比', dataIndex: ['metrics', 'profit_factor'], key: 'pf' },
    { title: '交易次数', dataIndex: ['metrics', 'total_trades'], key: 'trades' },
  ];

  return (
    <div>
      <Title level={4}>策略对比</Title>

      <Card size="small" style={{ marginBottom: 16 }}>
        {!strategyNames.length ? (
          <Empty description="暂无对比数据，请先运行多个策略的回测" style={{ padding: 60 }} />
        ) : (
          <ReactECharts option={option} style={{ height: 400 }} theme="dark" notMerge={false} lazyUpdate />
        )}
      </Card>

      <Card size="small" title="策略指标对比">
        <Table
          dataSource={tableData}
          columns={columns}
          rowKey="strategy"
          size="small"
          pagination={false}
          locale={{ emptyText: <Empty description="暂无回测数据" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
        />
      </Card>
    </div>
  );
}
