import { theme, type ThemeConfig } from 'antd';

const darkTheme: ThemeConfig = {
  algorithm: theme.darkAlgorithm,
  token: {
    colorPrimary: '#3b82f6',
    colorSuccess: '#10b981',
    colorError: '#ef4444',
    colorWarning: '#f59e0b',
    colorBgContainer: '#161b25',
    colorBgElevated: '#1e2535',
    colorBorder: 'rgba(255,255,255,0.08)',
    borderRadius: 10,
    fontFamily: "'SF Pro Display', -apple-system, 'Segoe UI', sans-serif",
  },
  components: {
    Layout: { siderBg: '#111827', headerBg: '#111827', bodyBg: '#0d1117' },
    Menu: { darkItemBg: '#111827', darkSubMenuItemBg: '#111827' },
    Table: { headerBg: '#1e2535', rowHoverBg: 'rgba(59,130,246,0.08)' },
    Card: { colorBgContainer: '#161b25' },
    Modal: { contentBg: '#1e2535', headerBg: '#1e2535' },
  },
};

export default darkTheme;
