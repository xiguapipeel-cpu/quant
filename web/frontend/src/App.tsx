import { useState, useEffect, createContext } from 'react';
import { ConfigProvider, Layout, Menu, Typography, Badge } from 'antd';
import {
  DashboardOutlined, SearchOutlined,
  ExperimentOutlined, LineChartOutlined, BarChartOutlined,
  CalendarOutlined, CodeOutlined,
} from '@ant-design/icons';
import darkTheme from './theme';
import { useWebSocket, type LogEntry } from './hooks/useWebSocket';
import Dashboard from './pages/Dashboard';
import ScanCenter from './pages/ScanCenter';
import BacktestCenter from './pages/BacktestCenter';
import EquityCurve from './pages/EquityCurve';
import StrategyCompare from './pages/StrategyCompare';
import TradingCalendar from './pages/TradingCalendar';
import LiveLog from './pages/LiveLog';

const { Sider, Content } = Layout;
const { Title, Text } = Typography;

export const WsContext = createContext<{ logs: LogEntry[]; scanDone: number; clearLogs: () => void }>({
  logs: [], scanDone: 0, clearLogs: () => {},
});

const menuItems = [
  { type: 'group' as const, label: '主面板', children: [
    { key: 'dashboard', icon: <DashboardOutlined />, label: '总览仪表盘' },
    { key: 'scan',      icon: <SearchOutlined />,    label: '选股中心' },
  ]},
  { type: 'group' as const, label: '策略回测', children: [
    { key: 'backtest', icon: <ExperimentOutlined />, label: '回测中心' },
    { key: 'equity',   icon: <LineChartOutlined />,  label: '净值曲线' },
    { key: 'compare',  icon: <BarChartOutlined />,   label: '策略对比' },
  ]},
  { type: 'group' as const, label: '工具', children: [
    { key: 'calendar', icon: <CalendarOutlined />, label: '交易日历' },
  ]},
  { type: 'group' as const, label: '系统', children: [
    { key: 'log', icon: <CodeOutlined />, label: '实时日志' },
  ]},
];

export default function App() {
  const [page, setPage] = useState('dashboard');
  const [collapsed, setCollapsed] = useState(false);
  const [clock, setClock] = useState('');
  const ws = useWebSocket();

  useEffect(() => {
    const t = setInterval(() => setClock(new Date().toLocaleTimeString('zh-CN', { hour12: false })), 1000);
    return () => clearInterval(t);
  }, []);

  const renderPage = () => {
    switch (page) {
      case 'dashboard': return <Dashboard />;
      case 'scan':      return <ScanCenter />;
      case 'backtest':  return <BacktestCenter />;
      case 'equity':    return <EquityCurve />;
      case 'compare':   return <StrategyCompare />;
      case 'calendar':  return <TradingCalendar />;
      case 'log':       return <LiveLog />;
      default:          return <Dashboard />;
    }
  };

  return (
    <ConfigProvider theme={darkTheme}>
      <WsContext.Provider value={ws}>
        <Layout style={{ minHeight: '100vh' }}>
          <Sider
            collapsible
            collapsed={collapsed}
            onCollapse={setCollapsed}
            width={220}
            style={{ borderRight: '1px solid rgba(255,255,255,0.06)' }}
          >
            <div style={{ padding: collapsed ? '16px 8px' : '20px', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
              {!collapsed && (
                <>
                  <Title level={5} style={{ margin: 0, color: '#fff', fontSize: 15 }}>
                    <Badge status="success" /> QuantSystem
                  </Title>
                  <Text type="secondary" style={{ fontSize: 11 }}>A股 · 多源验证 · v2.0</Text>
                </>
              )}
              {collapsed && <Badge status="success" />}
            </div>
            <Menu
              theme="dark"
              mode="inline"
              selectedKeys={[page]}
              onClick={({ key }) => setPage(key)}
              items={menuItems}
              style={{ borderRight: 'none' }}
            />
            {!collapsed && (
              <div style={{ padding: '12px 20px', borderTop: '1px solid rgba(255,255,255,0.06)', position: 'absolute', bottom: 40, width: '100%' }}>
                <Text type="secondary" style={{ fontSize: 13, fontVariantNumeric: 'tabular-nums' }}>{clock}</Text>
              </div>
            )}
          </Sider>
          <Layout>
            <Content style={{ padding: 24, overflow: 'auto', background: '#0d1117' }}>
              {renderPage()}
            </Content>
          </Layout>
        </Layout>
      </WsContext.Provider>
    </ConfigProvider>
  );
}
