import { useEffect, useMemo, useState } from 'react';
import {
  Card, Segmented, Select, Input, Button, List, Tag, Typography, Spin, Alert, Space, Empty,
} from 'antd';
import { FileTextOutlined, ReloadOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { apiFetch } from '../api/client';

const { Title, Text } = Typography;

type ReportType = 'dual_track' | 'monthly';
type Scope = 'weeks' | 'month' | 'all';

interface Archive {
  type: ReportType;
  title: string;
  name: string;
  size: number;
  mtime: string;
}

// ── 迷你 Markdown 渲染器（覆盖报告语法：标题/表格/列表/引用/粗体/hr） ──
function renderMarkdown(md: string): string {
  const esc = (t: string) =>
    t.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const inline = (t: string) =>
    esc(t)
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/`(.+?)`/g, '<code>$1</code>');
  const colorize = (t: string) =>
    t.replace(/([+-]\d+(?:\.\d+)?%)/g, (m) =>
      m.startsWith('-') ? `<span class="rr-neg">${m}</span>` : `<span class="rr-pos">${m}</span>`);

  const lines = md.split('\n');
  let html = '';
  let i = 0;
  while (i < lines.length) {
    const ln = lines[i];
    if (/^\s*\|.*\|\s*$/.test(ln)) {
      const tbl: string[] = [];
      while (i < lines.length && /^\s*\|.*\|\s*$/.test(lines[i])) { tbl.push(lines[i]); i++; }
      const cells = (r: string) => r.trim().replace(/^\||\|$/g, '').split('|').map((c) => c.trim());
      let t = '<table class="rr-table"><thead><tr>';
      t += cells(tbl[0]).map((c) => `<th>${inline(c)}</th>`).join('');
      t += '</tr></thead><tbody>';
      for (let r = 1; r < tbl.length; r++) {
        if (/^[\s:|-]+$/.test(tbl[r])) continue;
        t += '<tr>' + cells(tbl[r]).map((c) => `<td>${colorize(inline(c))}</td>`).join('') + '</tr>';
      }
      t += '</tbody></table>';
      html += t;
      continue;
    }
    if (/^###\s/.test(ln)) { html += `<h3>${inline(ln.replace(/^###\s/, ''))}</h3>`; i++; continue; }
    if (/^##\s/.test(ln)) { html += `<h2>${inline(ln.replace(/^##\s/, ''))}</h2>`; i++; continue; }
    if (/^#\s/.test(ln)) { html += `<h1>${inline(ln.replace(/^#\s/, ''))}</h1>`; i++; continue; }
    if (/^>\s?/.test(ln)) {
      const q: string[] = [];
      while (i < lines.length && /^>\s?/.test(lines[i])) { q.push(lines[i].replace(/^>\s?/, '')); i++; }
      html += `<blockquote>${colorize(inline(q.join(' ')))}</blockquote>`;
      continue;
    }
    if (/^---+\s*$/.test(ln)) { html += '<hr/>'; i++; continue; }
    if (/^\s*[-*]\s/.test(ln)) {
      const li: string[] = [];
      while (i < lines.length && /^\s*[-*]\s/.test(lines[i])) { li.push(lines[i].replace(/^\s*[-*]\s/, '')); i++; }
      html += '<ul>' + li.map((x) => `<li>${colorize(inline(x))}</li>`).join('') + '</ul>';
      continue;
    }
    if (ln.trim() === '') { i++; continue; }
    html += `<p>${colorize(inline(ln))}</p>`;
    i++;
  }
  return html;
}

export default function ReviewReports() {
  const [type, setType] = useState<ReportType>('dual_track');
  const [scope, setScope] = useState<Scope>('weeks');
  const [value, setValue] = useState('6');
  const [loading, setLoading] = useState(false);
  const [md, setMd] = useState('');
  const [err, setErr] = useState('');
  const [archives, setArchives] = useState<Archive[]>([]);

  const loadArchives = async () => {
    try {
      const list = await apiFetch<Archive[]>('/api/reports/archives');
      setArchives(list);
    } catch { /* ignore */ }
  };
  useEffect(() => { loadArchives(); }, []);

  // 切到双轨时月份范围无效，回退到周
  useEffect(() => {
    if (type !== 'monthly' && scope === 'month') setScope('weeks');
  }, [type]);

  // scope 改变时给默认值
  useEffect(() => {
    if (scope === 'month' && !/^\d{4}-\d{2}$/.test(value)) setValue('2025-09');
    if (scope === 'weeks' && !/^\d+$/.test(value)) setValue('6');
  }, [scope]);

  const generate = async () => {
    setLoading(true); setErr(''); setMd('');
    try {
      const qs = `type=${type}&scope=${scope}&value=${encodeURIComponent(value)}`;
      const res = await fetch(`/api/reports/generate?${qs}`, { method: 'POST' });
      const j = await res.json();
      if (j.ok) { setMd(j.markdown); loadArchives(); }
      else setErr(j.error || `HTTP ${res.status}`);
    } catch (e: any) { setErr(String(e)); }
    setLoading(false);
  };

  const openArchive = async (name: string) => {
    setLoading(true); setErr(''); setMd('');
    try {
      const j = await apiFetch<{ ok: boolean; markdown: string; error?: string }>(
        `/api/reports/archive?name=${encodeURIComponent(name)}`);
      if (j.ok) setMd(j.markdown); else setErr(j.error || '读取失败');
    } catch (e: any) { setErr(String(e)); }
    setLoading(false);
  };

  const html = useMemo(() => (md ? renderMarkdown(md) : ''), [md]);

  return (
    <div>
      <style>{`
        .rr-md h1{font-size:20px;border-bottom:1px solid rgba(255,255,255,.12);padding-bottom:8px;color:#e6e8eb}
        .rr-md h2{font-size:16px;margin-top:22px;color:#4c8bf5}
        .rr-md h3{font-size:14px;margin-top:16px;color:#cfd3da}
        .rr-md p{color:#cfd3da;margin:6px 0}
        .rr-md ul{padding-left:20px;color:#cfd3da}
        .rr-md hr{border:0;border-top:1px solid rgba(255,255,255,.12);margin:14px 0}
        .rr-md code{background:rgba(255,255,255,.08);padding:1px 5px;border-radius:4px}
        .rr-md blockquote{border-left:3px solid #4c8bf5;margin:10px 0;padding:6px 12px;color:#9aa0aa;background:rgba(76,139,245,.08)}
        .rr-table{border-collapse:collapse;width:100%;margin:10px 0;font-size:12.5px}
        .rr-table th,.rr-table td{border:1px solid rgba(255,255,255,.12);padding:5px 8px;text-align:left;white-space:nowrap}
        .rr-table th{background:rgba(255,255,255,.04);color:#9aa0aa}
        .rr-pos{color:#34c759}.rr-neg{color:#ff5b5b}
      `}</style>

      <Title level={4} style={{ marginBottom: 4 }}>
        <FileTextOutlined /> 复盘报告
      </Title>
      <Text type="secondary">
        双轨执行复盘（position_monitor 执行轨）· 月度 Outcome（pattern_outcome 信号轨，含未交易信号）
      </Text>

      <div style={{ display: 'flex', gap: 16, marginTop: 16, alignItems: 'flex-start' }}>
        {/* 左侧控制面板：sticky，不跟随右侧报告滚动 */}
        <Card
          size="small"
          title="生成 / 查看"
          style={{
            width: 300, flex: '0 0 300px',
            position: 'sticky', top: 0, alignSelf: 'flex-start',
            maxHeight: 'calc(100vh - 48px)', overflowY: 'auto',
          }}
        >
          <Segmented
            block
            value={type}
            onChange={(v) => setType(v as ReportType)}
            options={[
              { label: '双轨执行复盘', value: 'dual_track' },
              { label: '月度 Outcome', value: 'monthly' },
            ]}
          />
          <div style={{ marginTop: 12 }}>
            <Text type="secondary" style={{ fontSize: 12 }}>统计范围</Text>
            <Select
              style={{ width: '100%', marginTop: 4 }}
              value={scope}
              onChange={(v) => setScope(v as Scope)}
              options={[
                { label: '近 N 周', value: 'weeks' },
                { label: '指定月份 (YYYY-MM)', value: 'month', disabled: type !== 'monthly' },
                { label: '全部历史', value: 'all' },
              ]}
            />
          </div>
          {scope !== 'all' && (
            <div style={{ marginTop: 12 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {scope === 'month' ? '月份 (YYYY-MM)' : '周数'}
              </Text>
              <Input
                style={{ marginTop: 4 }}
                value={value}
                onChange={(e) => setValue(e.target.value)}
                placeholder={scope === 'month' ? '2025-09' : '6'}
              />
            </div>
          )}
          <Button
            type="primary" block icon={<ThunderboltOutlined />}
            style={{ marginTop: 16 }} loading={loading} onClick={generate}
          >
            生成报告
          </Button>

          <div style={{ marginTop: 18, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Text type="secondary" style={{ fontSize: 12 }}>历史存档</Text>
            <Button size="small" type="text" icon={<ReloadOutlined />} onClick={loadArchives} />
          </div>
          <List
            size="small"
            style={{ maxHeight: 360, overflow: 'auto' }}
            dataSource={archives}
            locale={{ emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无存档" /> }}
            renderItem={(a) => (
              <List.Item
                style={{ cursor: 'pointer', padding: '6px 4px' }}
                onClick={() => openArchive(a.name)}
              >
                <Space size={6} style={{ width: '100%', justifyContent: 'space-between' }}>
                  <Text style={{ fontSize: 12 }} ellipsis>{a.name.replace(/\.md$/, '')}</Text>
                  <Tag color={a.type === 'dual_track' ? 'blue' : 'gold'} style={{ margin: 0 }}>
                    {a.type === 'dual_track' ? '双轨' : '月度'}
                  </Tag>
                </Space>
              </List.Item>
            )}
          />
        </Card>

        {/* 右侧结果区 */}
        <Card size="small" style={{ flex: 1, minWidth: 0 }}>
          {err && <Alert type="error" showIcon message="生成失败" description={err} style={{ marginBottom: 12 }} />}
          {loading && (
            <div style={{ textAlign: 'center', padding: '60px 0' }}>
              <Spin tip="报告生成中，全历史可能需数十秒…" size="large"><div style={{ height: 1 }} /></Spin>
            </div>
          )}
          {!loading && !md && !err && (
            <Empty style={{ padding: '60px 0' }} description="选择范围后点「生成报告」，或从左侧历史存档中打开" />
          )}
          {!loading && md && (
            <div className="rr-md" dangerouslySetInnerHTML={{ __html: html }} />
          )}
        </Card>
      </div>
    </div>
  );
}
