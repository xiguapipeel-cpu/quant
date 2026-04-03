import { useEffect, useState } from 'react';
import { Card, Row, Col, Button, Statistic, Typography, Tag, List } from 'antd';
import { LeftOutlined, RightOutlined } from '@ant-design/icons';
import { apiFetch } from '../api/client';
import dayjs from 'dayjs';

const { Title, Text } = Typography;

const dayColors: Record<string, string> = {
  trading: '#10b981',
  holiday: '#ef4444',
  weekend: '#6b7280',
  makeup: '#f59e0b',
};
const dayLabels: Record<string, string> = {
  trading: '交易日', holiday: '节假日', weekend: '周末', makeup: '补班',
};

export default function TradingCalendar() {
  const [today, setToday] = useState<any>(null);
  const [month, setMonth] = useState<any>(null);
  const [holidays, setHolidays] = useState<any[]>([]);
  const [yearStats, setYearStats] = useState<any>(null);
  const [currentMonth, setCurrentMonth] = useState(dayjs());

  const loadMonth = async (m: dayjs.Dayjs) => {
    const res = await apiFetch(`/api/calendar/month?year=${m.year()}&month=${m.month() + 1}`).catch(() => null);
    setMonth(res);
  };

  useEffect(() => {
    apiFetch('/api/calendar/today').then(setToday).catch(() => {});
    apiFetch('/api/calendar/upcoming_holidays').then(d => setHolidays(d || [])).catch(() => {});
    apiFetch('/api/calendar/year_stats').then(setYearStats).catch(() => {});
    loadMonth(currentMonth);
  }, []);

  const changeMonth = (delta: number) => {
    const m = currentMonth.add(delta, 'month');
    setCurrentMonth(m);
    loadMonth(m);
  };

  // Build calendar grid
  const days: any[] = month?.days || [];
  const firstDay = days[0] ? dayjs(days[0].date).day() : 0;
  const padding = Array.from({ length: firstDay === 0 ? 6 : firstDay - 1 }, () => null);
  const gridDays = [...padding, ...days];

  return (
    <div>
      <Title level={4}>交易日历</Title>

      {/* Today status */}
      {today && (
        <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
          <Col xs={8} sm={6}>
            <Card size="small">
              <Statistic title="今日" value={today.date} valueStyle={{ fontSize: 14 }} />
              <Tag color={dayColors[today.type]}>{dayLabels[today.type] || today.type}</Tag>
            </Card>
          </Col>
          <Col xs={8} sm={6}>
            <Card size="small">
              <Statistic title="下一交易日" value={today.next_trading || '--'} valueStyle={{ fontSize: 14 }} />
            </Card>
          </Col>
          {yearStats && (
            <>
              <Col xs={8} sm={6}>
                <Card size="small">
                  <Statistic title="全年交易日" value={yearStats.trading_days} suffix="天" />
                </Card>
              </Col>
              <Col xs={8} sm={6}>
                <Card size="small">
                  <Statistic title="已过交易日" value={yearStats.elapsed_trading} suffix="天" />
                </Card>
              </Col>
            </>
          )}
        </Row>
      )}

      <Row gutter={[16, 16]}>
        {/* Calendar grid */}
        <Col xs={24} md={16}>
          <Card size="small">
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
              <Button icon={<LeftOutlined />} onClick={() => changeMonth(-1)} />
              <Title level={5} style={{ margin: 0 }}>{currentMonth.format('YYYY年M月')}</Title>
              <Button icon={<RightOutlined />} onClick={() => changeMonth(1)} />
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 4, textAlign: 'center' }}>
              {['一', '二', '三', '四', '五', '六', '日'].map(d => (
                <div key={d} style={{ padding: 6, fontSize: 12, color: '#8892a4', fontWeight: 600 }}>{d}</div>
              ))}
              {gridDays.map((d, i) => {
                if (!d) return <div key={`pad-${i}`} />;
                const isToday = d.date === dayjs().format('YYYY-MM-DD');
                return (
                  <div
                    key={d.date}
                    style={{
                      padding: '8px 4px',
                      borderRadius: 8,
                      background: isToday ? 'rgba(59,130,246,0.2)' : d.type === 'holiday' ? 'rgba(239,68,68,0.08)' : d.type === 'makeup' ? 'rgba(245,158,11,0.08)' : 'transparent',
                      border: isToday ? '1px solid #3b82f6' : '1px solid transparent',
                    }}
                  >
                    <div style={{ fontSize: 14, fontWeight: isToday ? 700 : 400, color: dayColors[d.type] || '#fff' }}>
                      {dayjs(d.date).date()}
                    </div>
                    {d.label && <div style={{ fontSize: 9, color: dayColors[d.type], marginTop: 2 }}>{d.label}</div>}
                  </div>
                );
              })}
            </div>
          </Card>
        </Col>

        {/* Upcoming holidays */}
        <Col xs={24} md={8}>
          <Card size="small" title="近期假日">
            <List
              size="small"
              dataSource={holidays.slice(0, 8)}
              renderItem={(h: any) => (
                <List.Item>
                  <div>
                    <Text strong>{h.name}</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: 11 }}>{h.start} ~ {h.end}</Text>
                  </div>
                  <Tag color="red">{h.days}天</Tag>
                </List.Item>
              )}
              locale={{ emptyText: '暂无假期数据' }}
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
}
