import { useEffect, useState, useMemo } from 'react';
import { Card, Row, Col, Select, Statistic, Typography, Empty } from 'antd';
import { apiFetch } from '../api/client';
import ReactECharts from 'echarts-for-react';

const { Title } = Typography;

const strategies = [
  { value: 'trend_follow', label: '趋势跟踪' },
  { value: 'rsi_reversal', label: 'RSI反转' },
  { value: 'bollinger_revert', label: '布林带回归' },
  { value: 'major_capital_pump', label: '主力拉升' },
];

export default function EquityCurve() {
  const [strategy, setStrategy] = useState('trend_follow');
  const [data, setData] = useState<any>(null);

  const load = async (s: string) => {
    const res = await apiFetch(`/api/backtest/equity/${s}`).catch(() => null);
    setData(res);
  };

  useEffect(() => { load(strategy); }, [strategy]);

  const values = data?.strategy || [];
  const dates = data?.dates || [];
  const benchmarks = data?.benchmark || [];
  const finalReturn = values.length > 0 ? ((values[values.length - 1] - 1) * 100).toFixed(2) + '%' : '--';
  let maxDD = 0;
  let peak = 1;
  for (const v of values) {
    if (v > peak) peak = v;
    const dd = (peak - v) / peak;
    if (dd > maxDD) maxDD = dd;
  }

  const option = useMemo(() => {
    if (!dates.length) return {};
    const strategyPct = values.map((v: number) => Number(((v - 1) * 100).toFixed(2)));
    const benchPct = benchmarks.map((v: number) => Number(((v - 1) * 100).toFixed(2)));
    const hasBench = benchPct.some((v: number) => v !== 0);

    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(22,27,37,0.95)',
        borderColor: 'rgba(255,255,255,0.1)',
        textStyle: { color: '#e6edf3', fontSize: 12 },
        formatter: (params: any) => {
          let html = `<div style="font-size:12px"><div style="color:#8892a4;margin-bottom:6px">${params[0].axisValue}</div>`;
          params.forEach((p: any) => {
            html += `<div>${p.marker} ${p.seriesName}: <b style="color:#fff">${p.value >= 0 ? '+' : ''}${p.value.toFixed(2)}%</b></div>`;
          });
          return html + '</div>';
        },
      },
      legend: {
        show: hasBench,
        top: 4, right: 20,
        textStyle: { color: '#8892a4', fontSize: 11 },
        itemWidth: 16, itemHeight: 2,
      },
      grid: { top: 36, right: 20, bottom: 60, left: 60 },
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
      series: [
        {
          name: '策略净值',
          type: 'line',
          data: strategyPct,
          smooth: true,
          symbol: 'none',
          lineStyle: { width: 2, color: '#3b82f6' },
          areaStyle: {
            color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(59,130,246,0.2)' },
                { offset: 1, color: 'rgba(59,130,246,0.01)' },
              ],
            },
          },
        },
        ...(hasBench ? [{
          name: '基准',
          type: 'line',
          data: benchPct,
          smooth: true,
          symbol: 'none',
          lineStyle: { width: 1.5, color: '#6b7280', type: 'dashed' as const },
        }] : []),
      ],
      dataZoom: [
        { type: 'slider', bottom: 8, height: 22,
          borderColor: 'rgba(255,255,255,0.1)', fillerColor: 'rgba(59,130,246,0.12)',
          textStyle: { color: '#556070', fontSize: 10 } },
        { type: 'inside' },
      ],
    };
  }, [dates, values, benchmarks]);

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
        <Title level={4} style={{ margin: 0 }}>净值曲线</Title>
        <Select value={strategy} onChange={setStrategy} options={strategies} style={{ width: 160 }} />
      </div>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={12} sm={6}>
          <Card size="small"><Statistic title="累计收益" value={finalReturn} valueStyle={{ color: finalReturn.includes('-') ? '#ef4444' : '#10b981' }} /></Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small"><Statistic title="最大回撤" value={(maxDD * 100).toFixed(2) + '%'} valueStyle={{ color: '#ef4444' }} /></Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small"><Statistic title="交易天数" value={values.length} suffix="天" /></Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small"><Statistic title="最终净值" value={values.length > 0 ? values[values.length - 1].toFixed(4) : '--'} /></Card>
        </Col>
      </Row>

      <Card size="small">
        {dates.length === 0 ? (
          <Empty description="尚未运行该策略回测，请先在回测中心执行" style={{ padding: 60 }} />
        ) : (
          <ReactECharts option={option} style={{ height: 420 }} theme="dark" notMerge={false} lazyUpdate />
        )}
      </Card>
    </div>
  );
}
