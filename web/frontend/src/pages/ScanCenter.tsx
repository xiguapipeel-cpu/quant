import { useEffect, useState, useContext } from 'react';
import {
  Card, Row, Col, Button, Typography, Steps, Empty, Statistic, Tooltip,
  Select, Tag, message, Switch, InputNumber, Form, Alert, Input, Modal, Space, Radio,
} from 'antd';
import {
  SearchOutlined, CheckCircleOutlined, LoadingOutlined,
  ThunderboltOutlined, LineChartOutlined, BarChartOutlined, RiseOutlined,
  ClockCircleOutlined, WechatOutlined, PlayCircleOutlined, SettingOutlined,
} from '@ant-design/icons';
import { apiFetch } from '../api/client';
import { WsContext } from '../App';

const { Title, Text } = Typography;

// ── 策略配置 ──────────────────────────────────────────────────
const STRATEGY_CONFIG: Record<string, {
  label: string; preset: string; color: string;
  icon: React.ReactNode; desc: string;
  steps: { title: string; description: string }[];
  tags: string[];
}> = {
  trend_follow: {
    label: '趋势跟踪', preset: 'large_cap', color: '#3b82f6',
    icon: <LineChartOutlined />,
    desc: '筛选大盘蓝筹，EMA金叉 + 大趋势向上，顺势做多',
    steps: [
      { title: '大盘初筛', description: '市值 > 500亿 · 日成交 > 1亿 · 排除ST' },
      { title: '趋势确认', description: '价格 > EMA60 · 大趋势向上' },
      { title: 'EMA金叉', description: 'EMA10 上穿 EMA30 入场信号' },
      { title: '进入标的池', description: '追踪止损保护，8% 回撤出场' },
    ],
    tags: ['大盘蓝筹', '市值>500亿', 'EMA金叉', '追踪止损'],
  },
  rsi_reversal: {
    label: 'RSI反转', preset: 'mid_cap', color: '#f59e0b',
    icon: <BarChartOutlined />,
    desc: '筛选中盘成长股，RSI超卖后反弹，快进快出',
    steps: [
      { title: '中盘成长筛选', description: '市值 100~1000亿 · 成交 > 5000万 · 排除ST' },
      { title: '超卖确认', description: 'RSI < 25 出现超卖信号' },
      { title: '反弹入场', description: 'RSI 回升 > 30 + 收阳线确认' },
      { title: '进入标的池', description: 'RSI > 60 止盈 或 止损 6%' },
    ],
    tags: ['中盘成长', '市值100~1000亿', 'RSI超卖', '快进快出'],
  },
  bollinger_revert: {
    label: '布林带回归', preset: 'default', color: '#8b5cf6',
    icon: <ThunderboltOutlined />,
    desc: '价格触碰布林下轨，均值回归到中轨止盈',
    steps: [
      { title: '市场初筛', description: '市值 > 100亿 · 成交 > 5000万 · 排除ST' },
      { title: '数据完整性', description: '股价 / PE / 市值 / 成交量均有效' },
      { title: '布林下轨触碰', description: '收盘价 ≤ 布林带下轨(2σ) + 收阳确认' },
      { title: '进入标的池', description: '回归中轨止盈，止损 5%' },
    ],
    tags: ['全市场', '市值>100亿', '布林下轨', '均值回归'],
  },
  major_capital_pump: {
    label: '主力拉升', preset: 'major_capital_pump', color: '#10b981',
    icon: <RiseOutlined />,
    desc: '中小盘活跃股，量价放大+MACD+RSI共振，捕捉主力资金拉升',
    steps: [
      { title: '中小盘活跃筛选', description: '市值 30~500亿 · 成交 > 3000万 · 上市 > 180天' },
      { title: '量价共振验证', description: '涨幅 ≥ 3% · 量比 ≥ 1.5x · 价格 > MA20' },
      { title: 'MACD + RSI 共振', description: 'MACD DIF > 0 · RSI 在 50~70 强势区' },
      { title: '进入标的池', description: '追踪止损 10% · RSI>85+长上影线出货信号' },
    ],
    tags: ['中小盘', '市值30~500亿', '量价放大', 'MACD共振', '主力资金'],
  },
  major_capital_accumulation: {
    label: '主力建仓', preset: 'major_capital_accumulation', color: '#f59e0b',
    icon: <SearchOutlined />,
    desc: '低位横盘中小盘股，均线粘合+布林收窄+阳量>阴量，捕捉主力低位吸筹',
    steps: [
      { title: '底部区域筛选', description: '市值 20~300亿 · 价格距60日低点 ≤15% · 上市>1年' },
      { title: '横盘吸筹确认', description: '均线粘合≤3% · MA20平走 · 布林带收窄' },
      { title: '资金暗流验证', description: '阳线成交量 > 阴线成交量 · RSI 35~55 低位区' },
      { title: '临界入场信号', description: 'MACD金叉 / 放量突破3% / 站上布林中轨' },
    ],
    tags: ['低位吸筹', '市值20~300亿', '均线粘合', '布林收窄', '阳量>阴量'],
  },
};

// ── 渠道配置信息 ─────────────────────────────────────────────
const CHANNEL_INFO: Record<string, {
  label: string; color: string; free: string;
  icon: string; guide: React.ReactNode;
  fields: { key: string; label: string; placeholder: string; isWebhook?: boolean }[];
}> = {
  wecom: {
    label: '企业微信机器人', color: '#07c160', free: '完全免费·无限制',
    icon: '💬',
    guide: (
      <ol style={{ margin: '6px 0', paddingLeft: 18, fontSize: 12, lineHeight: '22px' }}>
        <li>访问 <b>work.weixin.qq.com</b>，个人可免费注册企业</li>
        <li>「应用管理」→ 任意群聊 → 「添加群机器人」</li>
        <li>复制机器人的 <b>Webhook URL</b> 粘贴到下方</li>
        <li>手机/电脑安装企业微信即可收到通知</li>
      </ol>
    ),
    fields: [{ key: 'webhook_url', label: 'Webhook URL', placeholder: 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...', isWebhook: true }],
  },
  dingtalk: {
    label: '钉钉机器人', color: '#1677ff', free: '完全免费·无限制',
    icon: '📎',
    guide: (
      <ol style={{ margin: '6px 0', paddingLeft: 18, fontSize: 12, lineHeight: '22px' }}>
        <li>钉钉群 → 群设置 → 「机器人」→ 添加「自定义」</li>
        <li>安全设置选「加签」，复制 <b>Secret</b></li>
        <li>复制 <b>Webhook URL</b>，两个都粘贴到下方</li>
      </ol>
    ),
    fields: [
      { key: 'webhook_url', label: 'Webhook URL', placeholder: 'https://oapi.dingtalk.com/robot/send?access_token=...', isWebhook: true },
      { key: 'secret',      label: '加签 Secret（可选）', placeholder: 'SECxxx（开启加签时填写）' },
    ],
  },
  telegram: {
    label: 'Telegram Bot', color: '#0088cc', free: '完全免费',
    icon: '✈️',
    guide: (
      <ol style={{ margin: '6px 0', paddingLeft: 18, fontSize: 12, lineHeight: '22px' }}>
        <li>Telegram 搜索 <b>@BotFather</b> → /newbot → 获取 Bot Token</li>
        <li>与你的 Bot 发一条消息，然后访问：<br />
          <code style={{ fontSize: 11 }}>api.telegram.org/bot&lt;TOKEN&gt;/getUpdates</code></li>
        <li>从返回 JSON 中找到 <b>chat.id</b> 填入下方</li>
      </ol>
    ),
    fields: [
      { key: 'token',   label: 'Bot Token', placeholder: '1234567890:ABCDEFxxxx' },
      { key: 'chat_id', label: 'Chat ID',   placeholder: '123456789' },
    ],
  },
  serverchan: {
    label: 'Server酱（微信）', color: '#fa541c', free: '免费5条/天',
    icon: '📨',
    guide: (
      <ol style={{ margin: '6px 0', paddingLeft: 18, fontSize: 12, lineHeight: '22px' }}>
        <li>访问 <b>sct.ftqq.com</b>，微信扫码登录</li>
        <li>绑定微信号 → 复制 <b>SendKey</b></li>
        <li>粘贴到下方（免费版每天5条，足够日报使用）</li>
      </ol>
    ),
    fields: [{ key: 'token', label: 'SendKey', placeholder: 'SCTxxx...' }],
  },
};

// ── 定时任务 + 推送配置面板 ───────────────────────────────────
function SchedulePanel({ strategy, strategyCfg }: { strategy: string; strategyCfg: typeof STRATEGY_CONFIG[string] }) {
  const [cfg,      setCfg]      = useState<any>(null);
  const [pushCfg,  setPushCfg]  = useState<any>(null);
  const [saving,   setSaving]   = useState(false);
  const [testing,  setTesting]  = useState<string | null>(null);  // channel name
  const [running,  setRunning]  = useState(false);
  const [modal,    setModal]    = useState<string | null>(null);  // channel name
  const [formVals, setFormVals] = useState<Record<string, string>>({});
  const [savingCh, setSavingCh] = useState(false);

  const load = async () => {
    const [s, p] = await Promise.all([
      apiFetch('/api/schedule/config').catch(() => null),
      apiFetch('/api/push/config').catch(() => null),
    ]);
    if (s) setCfg(s);
    if (p) setPushCfg(p);
  };

  useEffect(() => { load(); }, []);

  const saveSchedule = async (patch: any) => {
    const next = { ...cfg, ...patch };
    setSaving(true);
    try {
      const params = new URLSearchParams({
        enabled:       String(next.enabled),
        hour:          String(next.hour),
        minute:        String(next.minute),
        notify_wechat: String(next.notify_wechat),
      });
      const res = await apiFetch(`/api/schedule/config?${params}`, 'POST');
      setCfg(next);
      message.success(res.msg || '保存成功');
    } catch { message.error('保存失败'); }
    finally { setSaving(false); }
  };

  const testChannel = async (ch: string) => {
    setTesting(ch);
    try {
      const res = await apiFetch(`/api/push/test?channel=${ch}`, 'POST');
      // res 可能是单渠道 {ok, msg, latency_ms} 或多渠道 {wecom: {...}, ...}
      const ok = res.ok ?? Object.values(res).some((r: any) => r?.ok);
      if (ok) message.success(`${CHANNEL_INFO[ch]?.label || ch} 推送成功，请查收通知！`);
      else     message.error(`推送失败：${res.msg || JSON.stringify(res)}`);
    } catch { message.error('测试失败'); }
    finally { setTesting(null); }
  };

  const testAll = async () => {
    setTesting('all');
    try {
      const res = await apiFetch('/api/push/test?channel=all', 'POST');
      const successes = Object.entries(res)
        .filter(([k, v]: any) => !k.startsWith('_') && v?.ok)
        .map(([k]) => CHANNEL_INFO[k]?.label || k);
      if (successes.length > 0)
        message.success(`${successes.join('、')} 推送成功！`);
      else
        message.warning('所有渠道推送失败，请检查配置');
    } catch { message.error('测试失败'); }
    finally { setTesting(null); }
  };

  const runNow = async () => {
    setRunning(true);
    try {
      const res = await apiFetch(`/api/schedule/run_now?strategy=${strategy}`, 'POST');
      if (res.ok) message.success(res.msg);
      else         message.error(res.error || '启动失败');
    } catch { message.error('启动失败'); }
    finally { setTimeout(() => setRunning(false), 3000); }
  };

  const openModal = (ch: string) => {
    setFormVals({});
    setModal(ch);
  };

  const saveChannel = async () => {
    if (!modal) return;
    setSavingCh(true);
    try {
      const params = new URLSearchParams({ channel: modal, ...formVals });
      const res = await apiFetch(`/api/push/save?${params}`, 'POST');
      if (res.ok) {
        message.success(res.msg);
        setModal(null);
        load();
      } else {
        message.error(res.msg || '保存失败');
      }
    } catch { message.error('保存失败'); }
    finally { setSavingCh(false); }
  };

  if (!cfg) return null;

  const configuredChannels = pushCfg
    ? Object.entries(CHANNEL_INFO)
        .filter(([k]) => pushCfg[k]?.configured)
        .map(([k]) => k)
    : [];

  const timeStr = `${String(cfg.hour ?? 15).padStart(2,'0')}:${String(cfg.minute ?? 35).padStart(2,'0')}`;
  const modalInfo = modal ? CHANNEL_INFO[modal] : null;

  return (
    <Card
      size="small"
      title={<span><ClockCircleOutlined style={{ color: strategyCfg.color, marginRight: 6 }} />定时任务 · 每日{strategyCfg.label}选股</span>}
      extra={
        <Switch
          checked={cfg.enabled}
          checkedChildren="已开启"
          unCheckedChildren="已关闭"
          onChange={v => saveSchedule({ enabled: v })}
          loading={saving}
          style={cfg.enabled ? { background: '#10b981' } : undefined}
        />
      }
      style={{ marginBottom: 16, borderLeft: `3px solid ${cfg?.enabled ? strategyCfg.color : '#555'}` }}
    >
      {cfg.enabled && (
        <Alert
          message={
            configuredChannels.length > 0
              ? `定时任务已开启，每个交易日 ${timeStr} 自动执行选股并推送到：${configuredChannels.map(k => CHANNEL_INFO[k].label).join('、')}`
              : `定时任务已开启（${timeStr}），请配置至少一个推送渠道以接收结果`
          }
          type={configuredChannels.length > 0 ? 'success' : 'warning'}
          showIcon
          style={{ marginBottom: 12, fontSize: 12 }}
        />
      )}

      {/* 执行时间 + 推送开关 + 操作 */}
      <Row gutter={[16, 12]} align="middle" style={{ marginBottom: 14 }}>
        <Col xs={24} sm={12} md={9}>
          <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 6 }}>执行时间（每个交易日）</Text>
          <Space>
            <InputNumber min={0} max={23} value={cfg.hour ?? 15}
              onChange={v => setCfg({ ...cfg, hour: v })}
              formatter={v => `${v}`.padStart(2,'0')} style={{ width: 60 }} />
            <Text>:</Text>
            <InputNumber min={0} max={59} value={cfg.minute ?? 35}
              onChange={v => setCfg({ ...cfg, minute: v })}
              formatter={v => `${v}`.padStart(2,'0')} style={{ width: 60 }} />
            <Button size="small" onClick={() => saveSchedule({ hour: cfg.hour, minute: cfg.minute })} loading={saving}>
              保存
            </Button>
          </Space>
          <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 4 }}>建议 15:35（收盘后）</Text>
        </Col>
        <Col xs={24} sm={12} md={7}>
          <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 6 }}>推送开关</Text>
          <Switch
            checked={cfg.notify_wechat}
            checkedChildren="推送开启"
            unCheckedChildren="推送关闭"
            onChange={v => saveSchedule({ notify_wechat: v })}
            style={cfg.notify_wechat ? { background: '#07c160' } : undefined}
          />
        </Col>
        <Col xs={24} sm={24} md={8}>
          <Space wrap>
            <Button
              type="primary" size="small"
              icon={running ? <LoadingOutlined /> : <PlayCircleOutlined />}
              loading={running} onClick={runNow}
              style={{ background: '#10b981', borderColor: '#10b981' }}
            >立即执行一次</Button>
            <Button size="small"
              icon={testing === 'all' ? <LoadingOutlined /> : <WechatOutlined />}
              loading={testing === 'all'} onClick={testAll}
              disabled={configuredChannels.length === 0}
            >测试所有渠道</Button>
          </Space>
        </Col>
      </Row>

      {/* 推送渠道配置卡片 */}
      <div style={{ marginBottom: 4 }}>
        <Text type="secondary" style={{ fontSize: 12 }}>推送渠道（均免费，配置一个或多个）：</Text>
      </div>
      <Row gutter={[10, 10]}>
        {Object.entries(CHANNEL_INFO).map(([key, info]) => {
          const chCfg  = pushCfg?.[key] ?? {};
          const isConf = chCfg.configured;
          return (
            <Col xs={24} sm={12} md={6} key={key}>
              <div style={{
                padding: '10px 12px',
                background: isConf ? `${info.color}15` : '#1a2035',
                border: `1px solid ${isConf ? info.color : '#2a3555'}`,
                borderRadius: 8,
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                  <Text strong style={{ fontSize: 13, color: info.color }}>
                    {info.icon} {info.label}
                  </Text>
                  {isConf
                    ? <Tag color="success" style={{ fontSize: 10 }}>已配置</Tag>
                    : <Tag color="default" style={{ fontSize: 10, opacity: 0.6 }}>未配置</Tag>
                  }
                </div>
                <Text type="secondary" style={{ fontSize: 11, display: 'block', marginBottom: 8 }}>
                  {info.free}
                </Text>
                <Space size={6}>
                  <Button size="small" icon={<SettingOutlined />} onClick={() => openModal(key)} style={{ fontSize: 11 }}>
                    {isConf ? '修改' : '配置'}
                  </Button>
                  {isConf && (
                    <Button size="small"
                      icon={testing === key ? <LoadingOutlined /> : <CheckCircleOutlined />}
                      loading={testing === key}
                      onClick={() => testChannel(key)}
                      style={{ fontSize: 11, color: info.color, borderColor: info.color }}
                    >测试</Button>
                  )}
                </Space>
              </div>
            </Col>
          );
        })}
      </Row>

      {/* 上次执行状态 */}
      {cfg.last_run && (
        <div style={{ marginTop: 12, padding: '8px 12px', background: '#0d1117', borderRadius: 6 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            上次执行：<span style={{ color: '#e6edf3' }}>{cfg.last_run}</span>
            {cfg.last_status && (
              <span style={{ marginLeft: 12, color: cfg.last_status.includes('异常') ? '#ef4444' : '#10b981' }}>
                {cfg.last_status}
              </span>
            )}
          </Text>
        </div>
      )}

      {/* 渠道配置弹窗 */}
      <Modal
        title={modalInfo ? `${modalInfo.icon} 配置 ${modalInfo.label}` : '配置推送渠道'}
        open={!!modal}
        onCancel={() => setModal(null)}
        onOk={saveChannel}
        confirmLoading={savingCh}
        okText="保存"
        width={540}
        destroyOnClose
      >
        {modalInfo && (
          <>
            <Alert
              message={<span>配置步骤 · <Tag color="success" style={{ fontSize: 11 }}>{modalInfo.free}</Tag></span>}
              description={modalInfo.guide}
              type="info"
              style={{ marginBottom: 16 }}
            />
            <Form layout="vertical" size="small">
              {modalInfo.fields.map(f => (
                <Form.Item key={f.key} label={f.label} required={!f.key.includes('secret')}>
                  <Input.Password
                    value={formVals[f.key] || ''}
                    onChange={e => setFormVals(prev => ({ ...prev, [f.key]: e.target.value }))}
                    placeholder={f.placeholder}
                    visibilityToggle
                  />
                </Form.Item>
              ))}
            </Form>
          </>
        )}
      </Modal>
    </Card>
  );
}


// ── 主组件 ────────────────────────────────────────────────────
export default function ScanCenter() {
  const [stocks, setStocks] = useState<any[]>([]);
  const [scanning, setScanning] = useState(false);
  const [status, setStatus] = useState<any>(null);
  const [apiOk, setApiOk] = useState<boolean | null>(null);
  const [cacheCount, setCacheCount] = useState<number | null>(null);
  const [strategy, setStrategy] = useState('major_capital_pump');
  const [signalFilter, setSignalFilter] = useState<'all' | 'BUY' | 'WATCH'>('all');
  const { scanDone } = useContext(WsContext);

  const cfg = STRATEGY_CONFIG[strategy];
  // 切换策略时重置信号过滤
  useEffect(() => { setSignalFilter('all'); }, [strategy]);

  const load = async (strat: string = strategy) => {
    const [res, st, info, api] = await Promise.all([
      apiFetch(`/api/scan/results?strategy=${strat}`).catch(() => []),
      apiFetch(`/api/scan/status?strategy=${strat}`).catch(() => null),
      apiFetch('/api/system/info').catch(() => null),
      apiFetch('/api/system/api_status').catch(() => null),
    ]);
    setStocks(res || []);
    setStatus(st);
    if (info?.cache_count != null) setCacheCount(info.cache_count);
    if (api) setApiOk(api.realtime_ok);
  };

  useEffect(() => { load(strategy); }, [scanDone, strategy]);

  const triggerScan = async () => {
    setScanning(true);
    try {
      await apiFetch(`/api/scan/run?scan_type=手动&strategy=${strategy}`, 'POST');
      const poll = setInterval(async () => {
        const st = await apiFetch('/api/scan/status').catch(() => null);
        if (st && !st.running) {
          clearInterval(poll);
          setScanning(false);
          load();
        }
      }, 1500);
    } catch {
      message.error('筛选启动失败');
      setScanning(false);
    }
  };

  // 结果是否来自当前策略（有结果即视为匹配）
  const presetMatch = stocks.length > 0 || !status?.last_scan_time;

  return (
    <div>
      {/* 标题栏 */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 20 }}>
        <div>
          <Title level={4} style={{ margin: 0 }}>选股中心</Title>
          <Text type="secondary">根据交易策略动态筛选标的股，进入回测与实盘标的池</Text>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <Select
            value={strategy}
            onChange={setStrategy}
            style={{ width: 140 }}
            options={Object.entries(STRATEGY_CONFIG).map(([k, v]) => ({
              value: k,
              label: <span style={{ color: v.color }}>{v.icon} {v.label}</span>,
            }))}
          />
          <Button
            type="primary"
            icon={scanning ? <LoadingOutlined /> : <SearchOutlined />}
            loading={scanning}
            onClick={triggerScan}
            style={{ background: cfg.color, borderColor: cfg.color }}
          >
            {scanning ? '筛选中...' : '立即筛选'}
          </Button>
        </div>
      </div>

      {/* 定时任务面板（主力相关策略时展开显示） */}
      {(strategy === 'major_capital_pump' || strategy === 'major_capital_accumulation') && (
        <SchedulePanel strategy={strategy} strategyCfg={cfg} />
      )}

      {/* 策略说明 */}
      <Card
        size="small"
        style={{
          marginBottom: 16,
          background: `linear-gradient(135deg, ${cfg.color}12 0%, transparent 100%)`,
          borderLeft: `3px solid ${cfg.color}`,
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 8 }}>
          <div>
            <Text strong style={{ color: cfg.color, fontSize: 14 }}>{cfg.icon} {cfg.label} 选股策略</Text>
            <br />
            <Text type="secondary" style={{ fontSize: 12 }}>{cfg.desc}</Text>
          </div>
          <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
            {cfg.tags.map(t => (
              <Tag key={t} style={{ background: `${cfg.color}20`, borderColor: `${cfg.color}50`, color: cfg.color, fontSize: 11 }}>{t}</Tag>
            ))}
          </div>
        </div>
      </Card>

      {/* 选股流程 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Steps size="small" items={cfg.steps} />
      </Card>

      {/* 数据源状态 */}
      <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '3px solid #10b981' }}>
            <Statistic title="历史行情" value="可用" valueStyle={{ color: '#10b981', fontSize: 15 }} prefix={<CheckCircleOutlined />} />
            <Text type="secondary" style={{ fontSize: 11 }}>AKShare · 本地缓存</Text>
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: `3px solid ${apiOk ? '#10b981' : '#ef4444'}` }}>
            <Statistic
              title="实时行情"
              value={apiOk === null ? '检测中' : apiOk ? '可用' : '受限'}
              valueStyle={{ color: apiOk ? '#10b981' : '#ef4444', fontSize: 15 }}
            />
            <Text type="secondary" style={{ fontSize: 11 }}>东方财富 API</Text>
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '3px solid #3b82f6' }}>
            <Statistic title="缓存数据" value={cacheCount ?? '—'} suffix="只" valueStyle={{ fontSize: 15, color: '#3b82f6' }} />
            <Text type="secondary" style={{ fontSize: 11 }}>本地日线文件</Text>
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: `3px solid ${presetMatch ? '#8b5cf6' : '#f59e0b'}` }}>
            <Statistic
              title="上次筛选"
              value={status?.last_scan_time ? status.last_scan_time.substring(0, 16) : '从未运行'}
              valueStyle={{ fontSize: 13 }}
            />
            <Text type="secondary" style={{ fontSize: 11 }}>
              {status?.result_count != null ? `共${status.result_count}只` : '—'}
              {!presetMatch && <span style={{ color: '#f59e0b', marginLeft: 4 }}>（暂无{cfg.label}结果，请点击筛选）</span>}
            </Text>
          </Card>
        </Col>
      </Row>

      {/* 标的池 */}
      <Card
        size="small"
        title={
          <span>
            当前标的池
            <Tag
              style={{
                marginLeft: 8, fontSize: 11,
                background: `${cfg.color}20`,
                borderColor: `${cfg.color}60`,
                color: cfg.color,
              }}
            >
              {cfg.icon} {cfg.label}
            </Tag>
          </span>
        }
        extra={<Text type="secondary">{stocks.length ? `共 ${stocks.length} 只` : '尚未筛选'}</Text>}
        style={{ marginBottom: 16 }}
      >
        {/* 信号过滤栏（主力建仓策略专属） */}
        {strategy === 'major_capital_accumulation' && stocks.length > 0 && (() => {
          const buyCount   = stocks.filter(s => s.signal_type === 'BUY').length;
          const watchCount = stocks.filter(s => s.signal_type === 'WATCH').length;
          return (
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12, flexWrap: 'wrap' }}>
              <Radio.Group
                size="small"
                value={signalFilter}
                onChange={e => setSignalFilter(e.target.value)}
                optionType="button"
                buttonStyle="solid"
              >
                <Radio.Button value="all">全部 {stocks.length}</Radio.Button>
                <Radio.Button value="BUY">
                  <span style={{ color: signalFilter === 'BUY' ? '#fff' : '#e74c3c' }}>
                    🔴 可入场 {buyCount}
                  </span>
                </Radio.Button>
                <Radio.Button value="WATCH">
                  <span style={{ color: signalFilter === 'WATCH' ? '#fff' : '#f39c12' }}>
                    🟡 建仓中 {watchCount}
                  </span>
                </Radio.Button>
              </Radio.Group>
              {buyCount > 0 && signalFilter !== 'WATCH' && (
                <Text type="secondary" style={{ fontSize: 11 }}>
                  🔴 可入场：建仓完毕，突破信号，可介入
                </Text>
              )}
              {watchCount > 0 && signalFilter === 'WATCH' && (
                <Text type="secondary" style={{ fontSize: 11 }}>
                  🟡 建仓中：主力吸筹未完毕，加入观察
                </Text>
              )}
            </div>
          );
        })()}
        {stocks.length === 0 ? (
          <Empty
            description={`暂无筛选结果。选择「${cfg.label}」策略后点击「立即筛选」。`}
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          />
        ) : (<>
          {strategy === 'major_capital_accumulation' && (
            <div style={{
              display: 'flex', gap: 24, flexWrap: 'wrap', marginBottom: 10, padding: '6px 12px',
              background: '#161d2a', borderRadius: 6, fontSize: 11, color: '#8892a4',
            }}>
              <span style={{ color: '#ffffffcc', fontWeight: 600 }}>策略贴合度评分：</span>
              <span>信号密度<span style={{ opacity: 0.5 }}>（0~25）</span>反复吸筹次数</span>
              <span>策略置信度<span style={{ opacity: 0.5 }}>（0~30）</span>触发强度+量能+布林</span>
              <span>当前状态<span style={{ opacity: 0.5 }}>（0~20）</span>BUY &gt; WATCH</span>
              <span>信号时效<span style={{ opacity: 0.5 }}>（0~25）</span>近期信号加分</span>
              <span style={{ marginLeft: 'auto' }}>
                <span style={{ color: '#e74c3c' }}>≥70 高</span>
                {' · '}
                <span style={{ color: '#f39c12' }}>≥50 中</span>
                {' · '}
                <span>&lt;50 弱</span>
              </span>
            </div>
          )}
          <Row gutter={[10, 10]}>
            {stocks.filter(s =>
              signalFilter === 'all' || s.signal_type === signalFilter || !s.signal_type
            ).sort((a: any, b: any) => {
              const sa = typeof a.match_score === 'object' ? (a.match_score?.total ?? 0) : (a.match_score ?? 0);
              const sb = typeof b.match_score === 'object' ? (b.match_score?.total ?? 0) : (b.match_score ?? 0);
              return sb - sa;
            }).map((s: any) => {
              const pct = s.pct_change;
              const hasSignal = !!s.signal_date;
              const isBuy = s.signal_type === 'BUY';
              const isWatch = s.signal_type === 'WATCH';
              // 策略贴合度评分（兼容旧 int 和新 object 格式）
              const scoreObj = typeof s.match_score === 'object' ? (s.match_score || {}) : { total: s.match_score || 0 };
              const score = scoreObj.total ?? 0;
              const scoreColor = score >= 70 ? '#e74c3c' : score >= 50 ? '#f39c12' : '#8892a4';
              // 全部信号日时间线（按日期升序）
              const allDates: { date: string; type: string; reason?: string }[] =
                Array.isArray(s.signal_dates) && s.signal_dates.length > 0
                  ? [...s.signal_dates].sort((a: any, b: any) => a.date.localeCompare(b.date))
                  : hasSignal ? [{ date: s.signal_date, type: s.signal_type }] : [];
              return (
                <Col xs={12} sm={12} md={8} lg={24/5} xl={24/5} key={s.code}
                     style={{ maxWidth: '20%', flex: '0 0 20%' }}>
                  <Card
                    size="small"
                    hoverable
                    onClick={() => {
                      apiFetch('/api/open-ths', 'POST', { code: s.code })
                        .then((r: any) => {
                          if (r.ok) {
                            message.success(r.auto_paste ? `已在同花顺中打开 ${s.code}` : `已打开同花顺，${s.code} 已复制到剪贴板，⌘V 粘贴`);
                          } else {
                            message.error(r.msg || '打开失败');
                          }
                        })
                        .catch(() => message.error('请求失败'));
                    }}
                    style={{ background: '#1e2535', border: `1px solid ${isBuy ? '#e74c3c80' : isWatch ? '#f39c1260' : cfg.color + '40'}`, cursor: 'pointer' }}
                    bodyStyle={{ padding: '10px 12px' }}
                  >
                    {/* 第一行：名称(代码) + 涨幅 */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                      <Text strong style={{ fontSize: 13 }}>
                        {s.name}<Text type="secondary" style={{ fontSize: 11, fontWeight: 'normal' }}>({s.code})</Text>
                      </Text>
                      {pct != null && (
                        <Text style={{ color: pct >= 0 ? '#e74c3c' : '#27ae60', fontWeight: 600, fontSize: 12, flexShrink: 0, marginLeft: 4 }}>
                          {pct >= 0 ? '+' : ''}{pct.toFixed(2)}%
                        </Text>
                      )}
                    </div>
                    {/* 第二行：价格 / 市值 / PE / 策略评分 */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: 11, color: '#8892a4' }}>
                      <div style={{ display: 'flex', gap: 8 }}>
                        {s.price > 0 && <span>¥{Number(s.price).toFixed(2)}</span>}
                        {s.cap_yi > 0 && <span>{Number(s.cap_yi).toFixed(0)}亿</span>}
                        {s.pe > 0 && <span>PE {Number(s.pe).toFixed(1)}</span>}
                      </div>
                      {score > 0 && (
                        <Tooltip
                          title={
                            <div style={{ fontSize: 12, lineHeight: '22px' }}>
                              <div style={{ fontWeight: 600, marginBottom: 6, fontSize: 13 }}>策略贴合度 {score} 分</div>
                              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                                <span>信号密度</span>
                                <span style={{ fontWeight: 600 }}>{scoreObj.density ?? '-'}<span style={{ opacity: 0.5 }}>/25</span></span>
                              </div>
                              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                                <span>策略置信度</span>
                                <span style={{ fontWeight: 600 }}>{scoreObj.confidence ?? '-'}<span style={{ opacity: 0.5 }}>/30</span></span>
                              </div>
                              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                                <span>当前状态</span>
                                <span style={{ fontWeight: 600 }}>{scoreObj.status ?? '-'}<span style={{ opacity: 0.5 }}>/20</span></span>
                              </div>
                              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                                <span>信号时效</span>
                                <span style={{ fontWeight: 600 }}>{scoreObj.recency ?? '-'}<span style={{ opacity: 0.5 }}>/25</span></span>
                              </div>
                              <div style={{ marginTop: 6, opacity: 0.6, fontSize: 11 }}>≥70 高贴合 · ≥50 中等 · &lt;50 偏弱</div>
                            </div>
                          }
                          placement="left"
                        >
                          <span
                            style={{
                              fontSize: 12, fontWeight: 700, color: scoreColor,
                              cursor: 'help', flexShrink: 0,
                            }}
                          >
                            {score}<span style={{ fontSize: 10, fontWeight: 400, opacity: 0.7 }}>分</span>
                          </span>
                        </Tooltip>
                      )}
                    </div>
                    {/* 第三行：信号日时间线 */}
                    {allDates.length > 0 && (
                      <div style={{ marginTop: 6, display: 'flex', flexWrap: 'wrap', gap: 3 }}>
                        {allDates.map((d, idx) => {
                          const dIsBuy = d.type === 'BUY';
                          const dColor = dIsBuy ? '#e74c3c' : '#f39c12';
                          const dLabel = dIsBuy ? '▲' : '●';
                          return (
                            <Tag
                              key={idx}
                              style={{
                                fontSize: 10, lineHeight: '16px', padding: '0 4px',
                                background: `${dColor}15`, borderColor: `${dColor}50`, color: dColor,
                                cursor: d.reason ? 'help' : 'default',
                              }}
                              title={d.reason || ''}
                            >
                              {dLabel} {d.date}
                            </Tag>
                          );
                        })}
                      </div>
                    )}
                  </Card>
                </Col>
              );
            })}
          </Row>
        </>)}
      </Card>

    </div>
  );
}
