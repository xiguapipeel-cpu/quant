import { useEffect, useRef, useState, useCallback } from 'react';
import { wsUrl } from '../api/client';

export interface LogEntry {
  time: string;
  msg: string;
  level: string;
}

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [scanDone, setScanDone] = useState(0); // increment to trigger refresh

  const connect = useCallback(() => {
    const ws = new WebSocket(wsUrl());
    wsRef.current = ws;
    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === 'log' && msg.data) {
          setLogs(prev => [...prev.slice(-200), msg.data]);
        }
        if (msg.type === 'scan_done') {
          setScanDone(n => n + 1);
        }
      } catch {}
    };
    ws.onclose = () => { setTimeout(connect, 2000); };
    ws.onerror = () => { ws.close(); };
  }, []);

  useEffect(() => {
    connect();
    return () => { wsRef.current?.close(); };
  }, [connect]);

  const clearLogs = useCallback(() => setLogs([]), []);

  return { logs, scanDone, clearLogs };
}
