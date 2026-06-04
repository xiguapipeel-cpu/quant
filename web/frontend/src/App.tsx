import { useState, useEffect, createContext } from 'react';
import { ConfigProvider, Layout, Menu, Typography, Badge } from 'antd';
import {
  DashboardOutlined, SearchOutlined,
  ExperimentOutlined, LineChartOutlined, BarChartOutlined,
  CalendarOutlined, CodeOutlined, FileTextOutlined,
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
import ReviewReports from './pages/ReviewReports';

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
  { type: 'group' as const, label: '复盘', children: [
    { key: 'reports', icon: <FileTextOutlined />, label: '复盘报告' },
  ]},
  { type: 'group' as const, label: '工具', children: [
    { key: 'calendar', icon: <CalendarOutlined />, label: '交易日历' },
  ]},
  { type: 'group' as const, label: '系统', children: [
    { key: 'log', icon: <CodeOutlined />, label: '实时日志' },
  ]},
];

// 合法页面路径（与 menuItems 的 key 一一对应）
const VALID_PAGES = new Set(['dashboard', 'scan', 'backtest', 'equity', 'compare', 'reports', 'calendar', 'log']);

function pageFromUrl(): string {
  const path = window.location.pathname.replace(/^\/+/, '').split('/')[0] || 'dashboard';
  return VALID_PAGES.has(path) ? path : 'dashboard';
}

function setUrl(page: string) {
  const target = page === 'dashboard' ? '/' : `/${page}`;
  if (window.location.pathname !== target) {
    window.history.pushState({ page }, '', target);
  }
}

export default function App() {
  const [page, setPageRaw] = useState<string>(() => pageFromUrl());
  const [collapsed, setCollapsed] = useState(false);
  const [clock, setClock] = useState('');
  const ws = useWebSocket();

  // 包装 setPage：同时更新 URL，使刷新/分享链接都能落到对应页面
  const setPage = (next: string) => {
    setPageRaw(next);
    setUrl(next);
  };

  // 监听浏览器前进/后退按钮
  useEffect(() => {
    const onPop = () => setPageRaw(pageFromUrl());
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, []);

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
      case 'reports':   return <ReviewReports />;
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
            style={{
              borderRight: '1px solid rgba(255,255,255,0.06)',
              position: 'fixed',
              top: 0,
              bottom: 0,
              left: 0,
              height: '100vh',
              overflow: 'auto',
              zIndex: 100,
            }}
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
          <Layout style={{ marginLeft: collapsed ? 80 : 220, transition: 'margin-left 0.2s' }}>
            <Content style={{ padding: 24, overflow: 'auto', background: '#0d1117' }}>
              {renderPage()}
            </Content>
          </Layout>
        </Layout>
      </WsContext.Provider>
    </ConfigProvider>
  );
}
