import { useContext, useRef, useEffect } from 'react';
import { Card, Button, Typography, Empty } from 'antd';
import { ClearOutlined } from '@ant-design/icons';
import { WsContext } from '../App';

const { Title, Text } = Typography;

const levelColors: Record<string, string> = {
  info: '#8892a4',
  ok: '#10b981',
  error: '#ef4444',
  warn: '#f59e0b',
};

export default function LiveLog() {
  const { logs, clearLogs } = useContext(WsContext);
  const boxRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (boxRef.current) {
      boxRef.current.scrollTop = boxRef.current.scrollHeight;
    }
  }, [logs]);

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
        <div>
          <Title level={4} style={{ margin: 0 }}>实时日志</Title>
          <Text type="secondary">WebSocket 实时推送 · 筛选/回测/验证全程记录</Text>
        </div>
        <Button icon={<ClearOutlined />} onClick={clearLogs}>清空</Button>
      </div>

      <Card size="small">
        <div
          ref={boxRef}
          style={{
            background: '#0d1117',
            borderRadius: 8,
            padding: 16,
            height: 'calc(100vh - 220px)',
            overflowY: 'auto',
            fontFamily: "'SF Mono', 'Fira Code', monospace",
            fontSize: 12,
            lineHeight: 1.8,
          }}
        >
          {logs.length === 0 ? (
            <Empty description="等待日志..." image={Empty.PRESENTED_IMAGE_SIMPLE} />
          ) : (
            logs.map((l, i) => (
              <div key={i} style={{ color: levelColors[l.level] || '#8892a4' }}>
                <span style={{ color: '#556070', marginRight: 8 }}>{l.time}</span>
                {l.level === 'error' && <span style={{ color: '#ef4444', marginRight: 4 }}>[ERROR]</span>}
                {l.level === 'warn' && <span style={{ color: '#f59e0b', marginRight: 4 }}>[WARN]</span>}
                {l.msg}
              </div>
            ))
          )}
        </div>
      </Card>
    </div>
  );
}
