import { useEffect, useState, useContext } from 'react';
import { Card, Row, Col, Statistic, Tag, Typography, Empty } from 'antd';
import { StockOutlined, ExperimentOutlined } from '@ant-design/icons';
import { apiFetch } from '../api/client';
import { WsContext } from '../App';

const { Text } = Typography;

export default function Dashboard() {
  const [scanResults, setScanResults] = useState<any[]>([]);
  const [btList, setBtList] = useState<any[]>([]);
  const { scanDone } = useContext(WsContext);

  const load = async () => {
    const [scan, bt] = await Promise.all([
      apiFetch('/api/scan/results').catch(() => []),
      apiFetch('/api/backtest/list').catch(() => []),
    ]);
    setScanResults(scan || []);
    setBtList(bt || []);
  };

  useEffect(() => { load(); }, [scanDone]);

  const latest = btList[0];
  const m = latest?.metrics || {};

  return (
    <div>
      <Typography.Title level={4} style={{ marginBottom: 20 }}>总览仪表盘</Typography.Title>

      <Row gutter={[16, 16]}>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="标的池"
              value={scanResults.length}
              suffix="只"
              prefix={<StockOutlined />}
              valueStyle={{ color: '#10b981' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="回测记录"
              value={btList.length}
              suffix="条"
              prefix={<ExperimentOutlined />}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="最新年化"
              value={m.annualized_return || '--'}
              valueStyle={{ color: (m.annualized_return || '').toString().includes('-') ? '#ef4444' : '#10b981' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="最新夏普"
              value={m.sharpe_ratio ?? '--'}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col xs={24} md={12}>
          <Card title="最新标的池概览" size="small">
            {scanResults.length === 0 ? (
              <Empty description="暂无数据，请在选股中心执行筛选" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {scanResults.slice(0, 20).map((s: any) => (
                  <Tag key={s.code} color="blue">{s.name} {s.code}</Tag>
                ))}
                {scanResults.length > 20 && <Tag>...共{scanResults.length}只</Tag>}
              </div>
            )}
          </Card>
        </Col>
        <Col xs={24} md={12}>
          <Card title="最近回测" size="small">
            {btList.length === 0 ? (
              <Empty description="暂无回测记录" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {btList.slice(0, 5).map((r: any) => {
                  const rm = r.metrics || {};
                  const isPos = !(rm.annualized_return || '').toString().includes('-');
                  return (
                    <div key={r.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '6px 0', borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                      <div>
                        <Text strong>{r.strategy}</Text>
                        <Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>{r.start}~{r.end}</Text>
                      </div>
                      <Text style={{ color: isPos ? '#10b981' : '#ef4444', fontWeight: 600 }}>
                        {rm.annualized_return || '--'}
                      </Text>
                    </div>
                  );
                })}
              </div>
            )}
          </Card>
        </Col>
      </Row>
    </div>
  );
}
