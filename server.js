// ============================================================
// Dev Monitoring Suite - server.js
// Full-stack uptime monitoring & alerting platform
// Deploy on Railway: https://railway.app
// ============================================================

require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const { createClient } = require('redis');
const cron = require('node-cron');
const axios = require('axios');
const nodemailer = require('nodemailer');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const morgan = require('morgan');
const { v4: uuidv4 } = require('uuid');

// ─── App Setup ───────────────────────────────────────────────
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

const PORT = process.env.PORT || 3000;

// ─── Middleware ───────────────────────────────────────────────
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(morgan('combined'));

const limiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 200 });
app.use('/api/', limiter);

// ─── Database Setup ───────────────────────────────────────────
const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// ─── Redis Setup ─────────────────────────────────────────────
let redisClient = null;
async function connectRedis() {
  if (!process.env.REDIS_URL) {
    console.log('⚠️  REDIS_URL not set, running without cache');
    return;
  }
  try {
    redisClient = createClient({ url: process.env.REDIS_URL });
    redisClient.on('error', (err) => console.log('Redis error:', err));
    await redisClient.connect();
    console.log('✅ Redis connected');
  } catch (err) {
    console.log('⚠️  Redis unavailable, running without cache:', err.message);
    redisClient = null;
  }
}

// ─── Database Initialization ──────────────────────────────────
async function initDB() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS monitors (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name VARCHAR(255) NOT NULL,
      url TEXT NOT NULL,
      interval_seconds INTEGER DEFAULT 60,
      timeout_ms INTEGER DEFAULT 10000,
      alert_threshold INTEGER DEFAULT 2,
      alert_email VARCHAR(255),
      webhook_url TEXT,
      is_active BOOLEAN DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS checks (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      monitor_id UUID REFERENCES monitors(id) ON DELETE CASCADE,
      status VARCHAR(20) NOT NULL,
      response_time_ms INTEGER,
      status_code INTEGER,
      error_message TEXT,
      checked_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS incidents (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      monitor_id UUID REFERENCES monitors(id) ON DELETE CASCADE,
      title VARCHAR(255) NOT NULL,
      description TEXT,
      status VARCHAR(50) DEFAULT 'open',
      started_at TIMESTAMPTZ DEFAULT NOW(),
      resolved_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS custom_metrics (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name VARCHAR(255) NOT NULL,
      value NUMERIC NOT NULL,
      unit VARCHAR(50),
      tags JSONB DEFAULT '{}',
      recorded_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_checks_monitor_id ON checks(monitor_id);
    CREATE INDEX IF NOT EXISTS idx_checks_checked_at ON checks(checked_at DESC);
    CREATE INDEX IF NOT EXISTS idx_incidents_monitor_id ON incidents(monitor_id);
    CREATE INDEX IF NOT EXISTS idx_metrics_recorded_at ON custom_metrics(recorded_at DESC);
  `);
  console.log('✅ Database initialized');
}

// ─── Monitor State ────────────────────────────────────────────
const monitorJobs = new Map();
const monitorFailCounts = new Map();

// ─── Alert Helpers ────────────────────────────────────────────
async function sendEmailAlert(monitor, subject, message) {
  if (!process.env.SMTP_HOST || !process.env.SMTP_USER) return;
  try {
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: parseInt(process.env.SMTP_PORT || '587'),
      secure: false,
      auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
    });
    await transporter.sendMail({
      from: process.env.ALERT_FROM_EMAIL || process.env.SMTP_USER,
      to: monitor.alert_email || process.env.ALERT_TO_EMAIL,
      subject,
      html: `<h2>${subject}</h2><p>${message}</p><hr><p><b>Monitor:</b> ${monitor.name}<br><b>URL:</b> ${monitor.url}<br><b>Time:</b> ${new Date().toISOString()}</p>`
    });
  } catch (err) {
    console.error('Email alert failed:', err.message);
  }
}

async function sendWebhookAlert(monitor, message) {
  const webhookUrl = monitor.webhook_url || process.env.WEBHOOK_URL;
  if (!webhookUrl) return;
  try {
    await axios.post(webhookUrl, {
      text: message,
      attachments: [{
        color: message.includes('DOWN') ? '#FF0000' : '#36A64F',
        fields: [
          { title: 'Monitor', value: monitor.name, short: true },
          { title: 'URL', value: monitor.url, short: true },
          { title: 'Time', value: new Date().toISOString(), short: false }
        ]
      }]
    }, { timeout: 5000 });
  } catch (err) {
    console.error('Webhook alert failed:', err.message);
  }
}

async function createIncident(monitorId, title, description) {
  try {
    const result = await db.query(
      `INSERT INTO incidents (monitor_id, title, description) VALUES ($1, $2, $3) RETURNING *`,
      [monitorId, title, description]
    );
    io.emit('incident:created', result.rows[0]);
    return result.rows[0];
  } catch (err) {
    console.error('Failed to create incident:', err.message);
  }
}

async function resolveIncident(monitorId) {
  try {
    const result = await db.query(
      `UPDATE incidents SET status = 'resolved', resolved_at = NOW()
       WHERE monitor_id = $1 AND status = 'open' RETURNING *`,
      [monitorId]
    );
    if (result.rows.length > 0) {
      io.emit('incident:resolved', result.rows[0]);
    }
  } catch (err) {
    console.error('Failed to resolve incident:', err.message);
  }
}

// ─── Core Check Function ──────────────────────────────────────
async function runCheck(monitor) {
  const startTime = Date.now();
  let status = 'down';
  let responseTime = null;
  let statusCode = null;
  let errorMessage = null;

  try {
    const response = await axios.get(monitor.url, {
      timeout: monitor.timeout_ms || 10000,
      validateStatus: () => true,
      maxRedirects: 5,
      headers: { 'User-Agent': 'DevMonitoringSuite/1.0' }
    });
    responseTime = Date.now() - startTime;
    statusCode = response.status;
    status = response.status < 400 ? 'up' : 'down';
  } catch (err) {
    responseTime = Date.now() - startTime;
    errorMessage = err.message;
    status = 'down';
  }

  // Save check result
  const check = await db.query(
    `INSERT INTO checks (monitor_id, status, response_time_ms, status_code, error_message)
     VALUES ($1, $2, $3, $4, $5) RETURNING *`,
    [monitor.id, status, responseTime, statusCode, errorMessage]
  );

  // Cache latest status in Redis
  if (redisClient) {
    await redisClient.setEx(
      `monitor:${monitor.id}:status`,
      300,
      JSON.stringify({ status, responseTime, statusCode, checkedAt: new Date() })
    );
  }

  // Broadcast to connected clients
  io.emit('check:result', { monitorId: monitor.id, status, responseTime, statusCode, checkedAt: new Date() });

  // Handle alerting logic
  const threshold = monitor.alert_threshold || parseInt(process.env.ALERT_THRESHOLD || '2');
  if (status === 'down') {
    const fails = (monitorFailCounts.get(monitor.id) || 0) + 1;
    monitorFailCounts.set(monitor.id, fails);
    if (fails === threshold) {
      const msg = `🚨 Monitor DOWN: ${monitor.name} (${monitor.url}) — ${errorMessage || `HTTP ${statusCode}`}`;
      await sendEmailAlert(monitor, `[DOWN] ${monitor.name}`, msg);
      await sendWebhookAlert(monitor, msg);
      await createIncident(monitor.id, `${monitor.name} is down`, msg);
      console.log(`🔴 ALERT: ${monitor.name} is DOWN`);
    }
  } else {
    const prevFails = monitorFailCounts.get(monitor.id) || 0;
    if (prevFails >= threshold) {
      const msg = `✅ Monitor RECOVERED: ${monitor.name} (${monitor.url}) — Response: ${responseTime}ms`;
      await sendEmailAlert(monitor, `[RECOVERED] ${monitor.name}`, msg);
      await sendWebhookAlert(monitor, msg);
      await resolveIncident(monitor.id);
      console.log(`🟢 RECOVERED: ${monitor.name}`);
    }
    monitorFailCounts.set(monitor.id, 0);
  }

  return check.rows[0];
}

// ─── Scheduler ────────────────────────────────────────────────
function scheduleMonitor(monitor) {
  if (monitorJobs.has(monitor.id)) {
    monitorJobs.get(monitor.id).stop();
    monitorJobs.delete(monitor.id);
  }
  if (!monitor.is_active) return;

  const intervalSeconds = Math.max(30, monitor.interval_seconds || 60);
  const cronExpr = intervalSeconds < 60
    ? `*/${intervalSeconds} * * * * *`
    : `*/${Math.floor(intervalSeconds / 60)} * * * *`;

  const job = cron.schedule(cronExpr, () => runCheck(monitor), { scheduled: true });
  monitorJobs.set(monitor.id, job);
  console.log(`📡 Scheduled: ${monitor.name} every ${intervalSeconds}s`);
}

async function startAllMonitors() {
  const { rows } = await db.query('SELECT * FROM monitors WHERE is_active = true');
  rows.forEach(scheduleMonitor);
  console.log(`🚀 Started ${rows.length} monitor(s)`);
}

// ─── HTML Dashboard ───────────────────────────────────────────
const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>🤖 Dev Monitoring Suite</title>
  <script src="https://cdn.socket.io/4.6.0/socket.io.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #e6edf3; min-height: 100vh; }
    header { background: #161b22; border-bottom: 1px solid #30363d; padding: 16px 32px; display: flex; align-items: center; gap: 12px; }
    header h1 { font-size: 20px; font-weight: 600; }
    .badge { background: #238636; color: white; font-size: 11px; padding: 2px 8px; border-radius: 12px; }
    .container { max-width: 1200px; margin: 0 auto; padding: 32px 16px; }
    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 32px; }
    .stat-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; }
    .stat-card .label { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; }
    .stat-card .value { font-size: 32px; font-weight: 700; margin-top: 4px; }
    .value.green { color: #3fb950; }
    .value.red { color: #f85149; }
    .value.blue { color: #58a6ff; }
    .value.yellow { color: #d29922; }
    .section { background: #161b22; border: 1px solid #30363d; border-radius: 8px; margin-bottom: 24px; }
    .section-header { padding: 16px 20px; border-bottom: 1px solid #30363d; display: flex; align-items: center; justify-content: space-between; }
    .section-header h2 { font-size: 15px; font-weight: 600; }
    .btn { background: #238636; color: white; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 500; }
    .btn:hover { background: #2ea043; }
    .btn.secondary { background: #21262d; border: 1px solid #30363d; }
    .btn.secondary:hover { background: #30363d; }
    .btn.danger { background: #da3633; }
    .btn.danger:hover { background: #f85149; }
    table { width: 100%; border-collapse: collapse; }
    th { text-align: left; padding: 12px 20px; font-size: 12px; font-weight: 600; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; border-bottom: 1px solid #30363d; }
    td { padding: 14px 20px; border-bottom: 1px solid #21262d; font-size: 14px; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #1c2128; }
    .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 8px; }
    .dot-up { background: #3fb950; }
    .dot-down { background: #f85149; }
    .dot-pending { background: #8b949e; }
    .response-bar { display: inline-block; height: 4px; border-radius: 2px; background: #3fb950; vertical-align: middle; margin-left: 8px; }
    .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.7); z-index: 100; align-items: center; justify-content: center; }
    .modal-overlay.active { display: flex; }
    .modal { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 24px; width: 480px; max-width: 95vw; }
    .modal h3 { margin-bottom: 16px; font-size: 16px; }
    .form-group { margin-bottom: 16px; }
    .form-group label { display: block; margin-bottom: 6px; font-size: 13px; color: #8b949e; }
    .form-group input, .form-group select { width: 100%; background: #0d1117; border: 1px solid #30363d; color: #e6edf3; padding: 8px 12px; border-radius: 6px; font-size: 14px; }
    .form-group input:focus, .form-group select:focus { outline: none; border-color: #58a6ff; }
    .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .modal-footer { display: flex; justify-content: flex-end; gap: 8px; margin-top: 20px; }
    .incident-item { padding: 14px 20px; border-bottom: 1px solid #21262d; display: flex; align-items: flex-start; gap: 12px; }
    .incident-item:last-child { border-bottom: none; }
    .incident-status { width: 8px; height: 8px; border-radius: 50%; margin-top: 6px; flex-shrink: 0; }
    .incident-open { background: #f85149; }
    .incident-resolved { background: #3fb950; }
    .incident-title { font-weight: 500; font-size: 14px; }
    .incident-meta { font-size: 12px; color: #8b949e; margin-top: 2px; }
    .chart-container { padding: 20px; height: 200px; }
    .empty-state { text-align: center; padding: 48px 20px; color: #8b949e; }
    .empty-state p { margin-top: 8px; font-size: 14px; }
    .tag { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 500; }
    .tag-up { background: #1b4332; color: #3fb950; }
    .tag-down { background: #3d1c1c; color: #f85149; }
    .tag-pending { background: #21262d; color: #8b949e; }
    .uptime-bar { display: flex; gap: 2px; }
    .uptime-tick { width: 4px; height: 20px; border-radius: 2px; flex-shrink: 0; }
    .live-indicator { width: 8px; height: 8px; border-radius: 50%; background: #3fb950; display: inline-block; margin-right: 6px; animation: pulse 2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .4; } }
  </style>
</head>
<body>
  <header>
    <span>🤖</span>
    <h1>Dev Monitoring Suite</h1>
    <span class="badge">LIVE</span>
    <span style="margin-left: auto; font-size: 13px; color: #8b949e;">
      <span class="live-indicator"></span>Real-time
    </span>
  </header>

  <div class="container">
    <!-- Stats -->
    <div class="stats-grid">
      <div class="stat-card">
        <div class="label">Total Monitors</div>
        <div class="value blue" id="stat-total">—</div>
      </div>
      <div class="stat-card">
        <div class="label">Online</div>
        <div class="value green" id="stat-up">—</div>
      </div>
      <div class="stat-card">
        <div class="label">Down</div>
        <div class="value red" id="stat-down">—</div>
      </div>
      <div class="stat-card">
        <div class="label">Avg Response</div>
        <div class="value blue" id="stat-response">—</div>
      </div>
      <div class="stat-card">
        <div class="label">Open Incidents</div>
        <div class="value yellow" id="stat-incidents">—</div>
      </div>
    </div>

    <!-- Monitors -->
    <div class="section">
      <div class="section-header">
        <h2>📡 Monitors</h2>
        <button class="btn" onclick="openAddMonitor()">+ Add Monitor</button>
      </div>
      <div id="monitors-container">
        <div class="empty-state">
          <div style="font-size:32px">📡</div>
          <p>No monitors yet. Add one to start tracking uptime.</p>
        </div>
      </div>
    </div>

    <!-- Incidents -->
    <div class="section">
      <div class="section-header">
        <h2>🚨 Incidents</h2>
        <span id="incident-count" style="font-size:13px;color:#8b949e"></span>
      </div>
      <div id="incidents-container">
        <div class="empty-state">
          <div style="font-size:32px">✅</div>
          <p>No incidents. Everything looks good!</p>
        </div>
      </div>
    </div>

    <!-- Response Time Chart -->
    <div class="section">
      <div class="section-header">
        <h2>📈 Response Times (last 24h)</h2>
        <select id="chart-monitor-select" onchange="loadChart()" style="background:#0d1117;border:1px solid #30363d;color:#e6edf3;padding:6px 10px;border-radius:6px;font-size:13px;"></select>
      </div>
      <div class="chart-container">
        <canvas id="responseChart"></canvas>
      </div>
    </div>
  </div>

  <!-- Add Monitor Modal -->
  <div class="modal-overlay" id="modal-add">
    <div class="modal">
      <h3>+ Add Monitor</h3>
      <div class="form-group">
        <label>Monitor Name</label>
        <input type="text" id="m-name" placeholder="e.g. My Website" />
      </div>
      <div class="form-group">
        <label>URL to Monitor</label>
        <input type="url" id="m-url" placeholder="https://example.com" />
      </div>
      <div class="form-row">
        <div class="form-group">
          <label>Check Interval</label>
          <select id="m-interval">
            <option value="60">Every 1 minute</option>
            <option value="300" selected>Every 5 minutes</option>
            <option value="600">Every 10 minutes</option>
            <option value="1800">Every 30 minutes</option>
          </select>
        </div>
        <div class="form-group">
          <label>Alert After (failures)</label>
          <select id="m-threshold">
            <option value="1">1 failure</option>
            <option value="2" selected>2 failures</option>
            <option value="3">3 failures</option>
          </select>
        </div>
      </div>
      <div class="form-group">
        <label>Alert Email (optional)</label>
        <input type="email" id="m-email" placeholder="you@example.com" />
      </div>
      <div class="form-group">
        <label>Webhook URL (Slack/Discord, optional)</label>
        <input type="url" id="m-webhook" placeholder="https://hooks.slack.com/..." />
      </div>
      <div class="modal-footer">
        <button class="btn secondary" onclick="closeModal()">Cancel</button>
        <button class="btn" onclick="addMonitor()">Add Monitor</button>
      </div>
    </div>
  </div>

  <script>
    const socket = io();
    let monitors = [];
    let chart = null;

    // ── Socket Events ──────────────────────────────────────────
    socket.on('check:result', ({ monitorId, status, responseTime }) => {
      const row = document.getElementById('row-' + monitorId);
      if (!row) return;
      const dot = row.querySelector('.status-dot');
      const tag = row.querySelector('.status-tag');
      const rtEl = row.querySelector('.rt');
      dot.className = 'status-dot dot-' + status;
      tag.className = 'tag tag-' + status;
      tag.textContent = status.toUpperCase();
      if (rtEl && responseTime) rtEl.textContent = responseTime + 'ms';
      const m = monitors.find(m => m.id === monitorId);
      if (m) { m.last_status = status; m.last_response_time = responseTime; }
      updateStats();
    });

    socket.on('incident:created', () => { loadIncidents(); loadStats(); });
    socket.on('incident:resolved', () => { loadIncidents(); loadStats(); });

    // ── Stats ──────────────────────────────────────────────────
    async function loadStats() {
      const res = await fetch('/api/stats');
      const s = await res.json();
      document.getElementById('stat-total').textContent = s.total;
      document.getElementById('stat-up').textContent = s.up;
      document.getElementById('stat-down').textContent = s.down;
      document.getElementById('stat-response').textContent = s.avgResponse ? s.avgResponse + 'ms' : '—';
      document.getElementById('stat-incidents').textContent = s.openIncidents;
    }

    function updateStats() {
      const up = monitors.filter(m => m.last_status === 'up').length;
      const down = monitors.filter(m => m.last_status === 'down').length;
      document.getElementById('stat-total').textContent = monitors.length;
      document.getElementById('stat-up').textContent = up;
      document.getElementById('stat-down').textContent = down;
    }

    // ── Monitors ───────────────────────────────────────────────
    async function loadMonitors() {
      const res = await fetch('/api/monitors');
      monitors = await res.json();
      renderMonitors();
      updateChartSelect();
    }

    function renderMonitors() {
      const container = document.getElementById('monitors-container');
      if (!monitors.length) {
        container.innerHTML = '<div class="empty-state"><div style="font-size:32px">📡</div><p>No monitors yet. Add one to start tracking uptime.</p></div>';
        return;
      }
      container.innerHTML = '<table><thead><tr><th>Name</th><th>URL</th><th>Status</th><th>Response</th><th>Interval</th><th>Actions</th></tr></thead><tbody>' +
        monitors.map(m => \`
          <tr id="row-\${m.id}">
            <td><span class="status-dot dot-\${m.last_status || 'pending'}"></span>\${esc(m.name)}</td>
            <td style="color:#58a6ff;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">\${esc(m.url)}</td>
            <td><span class="tag tag-\${m.last_status || 'pending'} status-tag">\${(m.last_status || 'pending').toUpperCase()}</span></td>
            <td class="rt">\${m.last_response_time ? m.last_response_time + 'ms' : '—'}</td>
            <td>\${formatInterval(m.interval_seconds)}</td>
            <td>
              <button class="btn secondary" onclick="checkNow('\${m.id}')" style="padding:4px 10px;font-size:12px;margin-right:4px">Check Now</button>
              <button class="btn danger" onclick="deleteMonitor('\${m.id}')" style="padding:4px 10px;font-size:12px">Delete</button>
            </td>
          </tr>
        \`).join('') + '</tbody></table>';
    }

    function formatInterval(s) {
      if (s < 60) return s + 's';
      if (s < 3600) return (s / 60) + 'm';
      return (s / 3600) + 'h';
    }

    function esc(str) {
      const d = document.createElement('div');
      d.textContent = str || '';
      return d.innerHTML;
    }

    async function addMonitor() {
      const name = document.getElementById('m-name').value.trim();
      const url = document.getElementById('m-url').value.trim();
      if (!name || !url) { alert('Name and URL are required'); return; }
      await fetch('/api/monitors', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name, url,
          interval_seconds: parseInt(document.getElementById('m-interval').value),
          alert_threshold: parseInt(document.getElementById('m-threshold').value),
          alert_email: document.getElementById('m-email').value.trim() || null,
          webhook_url: document.getElementById('m-webhook').value.trim() || null
        })
      });
      closeModal();
      loadMonitors();
      loadStats();
    }

    async function deleteMonitor(id) {
      if (!confirm('Delete this monitor?')) return;
      await fetch('/api/monitors/' + id, { method: 'DELETE' });
      loadMonitors();
      loadStats();
    }

    async function checkNow(id) {
      await fetch('/api/monitors/' + id + '/check', { method: 'POST' });
    }

    // ── Incidents ──────────────────────────────────────────────
    async function loadIncidents() {
      const res = await fetch('/api/incidents');
      const incidents = await res.json();
      const container = document.getElementById('incidents-container');
      document.getElementById('stat-incidents').textContent = incidents.filter(i => i.status === 'open').length;
      if (!incidents.length) {
        container.innerHTML = '<div class="empty-state"><div style="font-size:32px">✅</div><p>No incidents. Everything looks good!</p></div>';
        return;
      }
      container.innerHTML = incidents.map(i => \`
        <div class="incident-item">
          <div class="incident-status \${i.status === 'open' ? 'incident-open' : 'incident-resolved'}"></div>
          <div>
            <div class="incident-title">\${esc(i.title)}</div>
            <div class="incident-meta">\${i.status === 'open' ? '🔴 Open' : '✅ Resolved'} · \${new Date(i.started_at).toLocaleString()}\${i.resolved_at ? ' → ' + new Date(i.resolved_at).toLocaleString() : ''}</div>
          </div>
          \${i.status === 'open' ? '<button class="btn secondary" onclick="resolveIncident(\\'' + i.id + '\\')" style="margin-left:auto;padding:4px 10px;font-size:12px">Resolve</button>' : ''}
        </div>
      \`).join('');
    }

    async function resolveIncident(id) {
      await fetch('/api/incidents/' + id + '/resolve', { method: 'POST' });
      loadIncidents();
    }

    // ── Chart ──────────────────────────────────────────────────
    function updateChartSelect() {
      const sel = document.getElementById('chart-monitor-select');
      sel.innerHTML = monitors.length
        ? monitors.map(m => \`<option value="\${m.id}">\${esc(m.name)}</option>\`).join('')
        : '<option>No monitors</option>';
      if (monitors.length) loadChart();
    }

    async function loadChart() {
      const id = document.getElementById('chart-monitor-select').value;
      if (!id) return;
      const res = await fetch('/api/monitors/' + id + '/history?hours=24');
      const data = await res.json();
      const ctx = document.getElementById('responseChart').getContext('2d');
      if (chart) chart.destroy();
      chart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: data.map(d => new Date(d.checked_at).toLocaleTimeString()),
          datasets: [{
            label: 'Response Time (ms)',
            data: data.map(d => d.response_time_ms),
            borderColor: '#58a6ff',
            backgroundColor: 'rgba(88,166,255,.1)',
            tension: 0.3,
            fill: true,
            pointRadius: 2
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: '#8b949e', maxTicksLimit: 8 }, grid: { color: '#21262d' } },
            y: { ticks: { color: '#8b949e', callback: v => v + 'ms' }, grid: { color: '#21262d' } }
          }
        }
      });
    }

    // ── Modal ──────────────────────────────────────────────────
    function openAddMonitor() { document.getElementById('modal-add').classList.add('active'); }
    function closeModal() { document.getElementById('modal-add').classList.remove('active'); }
    document.getElementById('modal-add').addEventListener('click', e => { if (e.target === e.currentTarget) closeModal(); });

    // ── Init ───────────────────────────────────────────────────
    loadMonitors();
    loadStats();
    loadIncidents();
    setInterval(loadStats, 30000);
  </script>
</body>
</html>`;

// ─── API Routes ───────────────────────────────────────────────

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', uptime: process.uptime(), timestamp: new Date() });
});

// Dashboard
app.get('/', (req, res) => res.send(dashboardHTML));

// Stats
app.get('/api/stats', async (req, res) => {
  try {
    const [monitorsRes, incidentsRes, responseRes] = await Promise.all([
      db.query('SELECT * FROM monitors WHERE is_active = true'),
      db.query("SELECT COUNT(*) FROM incidents WHERE status = 'open'"),
      db.query(`
        SELECT ROUND(AVG(response_time_ms)) as avg
        FROM checks
        WHERE checked_at > NOW() - INTERVAL '1 hour' AND status = 'up'
      `)
    ]);

    const monitorIds = monitorsRes.rows.map(m => m.id);
    const statuses = new Map();

    if (monitorIds.length > 0) {
      const latestChecks = await db.query(`
        SELECT DISTINCT ON (monitor_id) monitor_id, status
        FROM checks
        WHERE monitor_id = ANY($1)
        ORDER BY monitor_id, checked_at DESC
      `, [monitorIds]);
      latestChecks.rows.forEach(r => statuses.set(r.monitor_id, r.status));
    }

    let up = 0, down = 0;
    monitorsRes.rows.forEach(m => {
      if (statuses.get(m.id) === 'up') up++;
      else if (statuses.get(m.id) === 'down') down++;
    });

    res.json({
      total: monitorsRes.rows.length,
      up, down,
      avgResponse: responseRes.rows[0].avg ? parseInt(responseRes.rows[0].avg) : null,
      openIncidents: parseInt(incidentsRes.rows[0].count)
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get all monitors (with latest status)
app.get('/api/monitors', async (req, res) => {
  try {
    const { rows: monitors } = await db.query('SELECT * FROM monitors ORDER BY created_at DESC');
    if (monitors.length === 0) return res.json([]);

    const ids = monitors.map(m => m.id);
    const { rows: latest } = await db.query(`
      SELECT DISTINCT ON (monitor_id) monitor_id, status, response_time_ms
      FROM checks WHERE monitor_id = ANY($1)
      ORDER BY monitor_id, checked_at DESC
    `, [ids]);

    const statusMap = new Map(latest.map(r => [r.monitor_id, r]));
    const result = monitors.map(m => ({
      ...m,
      last_status: statusMap.get(m.id)?.status || null,
      last_response_time: statusMap.get(m.id)?.response_time_ms || null
    }));
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Add monitor
app.post('/api/monitors', async (req, res) => {
  try {
    const { name, url, interval_seconds, timeout_ms, alert_threshold, alert_email, webhook_url } = req.body;
    if (!name || !url) return res.status(400).json({ error: 'name and url required' });

    const { rows } = await db.query(
      `INSERT INTO monitors (name, url, interval_seconds, timeout_ms, alert_threshold, alert_email, webhook_url)
       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [name, url, interval_seconds || 300, timeout_ms || 10000, alert_threshold || 2, alert_email || null, webhook_url || null]
    );
    const monitor = rows[0];
    scheduleMonitor(monitor);
    runCheck(monitor).catch(console.error);
    io.emit('monitor:created', monitor);
    res.status(201).json(monitor);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Delete monitor
app.delete('/api/monitors/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (monitorJobs.has(id)) { monitorJobs.get(id).stop(); monitorJobs.delete(id); }
    await db.query('DELETE FROM monitors WHERE id = $1', [id]);
    io.emit('monitor:deleted', { id });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Trigger manual check
app.post('/api/monitors/:id/check', async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM monitors WHERE id = $1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Monitor not found' });
    const result = await runCheck(rows[0]);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get check history
app.get('/api/monitors/:id/history', async (req, res) => {
  try {
    const hours = Math.min(parseInt(req.query.hours || '24'), 168);
    const { rows } = await db.query(
      `SELECT * FROM checks WHERE monitor_id = $1 AND checked_at > NOW() - INTERVAL '${hours} hours'
       ORDER BY checked_at ASC LIMIT 500`,
      [req.params.id]
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get all incidents
app.get('/api/incidents', async (req, res) => {
  try {
    const { rows } = await db.query(
      `SELECT i.*, m.name as monitor_name FROM incidents i
       LEFT JOIN monitors m ON i.monitor_id = m.id
       ORDER BY i.started_at DESC LIMIT 50`
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Resolve incident
app.post('/api/incidents/:id/resolve', async (req, res) => {
  try {
    const { rows } = await db.query(
      `UPDATE incidents SET status = 'resolved', resolved_at = NOW() WHERE id = $1 RETURNING *`,
      [req.params.id]
    );
    if (rows.length) io.emit('incident:resolved', rows[0]);
    res.json(rows[0] || {});
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Record custom metric
app.post('/api/metrics', async (req, res) => {
  try {
    const { name, value, unit, tags } = req.body;
    if (!name || value === undefined) return res.status(400).json({ error: 'name and value required' });
    const { rows } = await db.query(
      `INSERT INTO custom_metrics (name, value, unit, tags) VALUES ($1, $2, $3, $4) RETURNING *`,
      [name, value, unit || null, tags ? JSON.stringify(tags) : '{}']
    );
    res.status(201).json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get custom metrics
app.get('/api/metrics', async (req, res) => {
  try {
    const { name, hours } = req.query;
    const h = Math.min(parseInt(hours || '24'), 720);
    let query = `SELECT * FROM custom_metrics WHERE recorded_at > NOW() - INTERVAL '${h} hours'`;
    const params = [];
    if (name) { params.push(name); query += ` AND name = $${params.length}`; }
    query += ' ORDER BY recorded_at DESC LIMIT 1000';
    const { rows } = await db.query(query, params);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Socket.io connection
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  socket.on('disconnect', () => console.log('Client disconnected:', socket.id));
});

// ─── Startup ──────────────────────────────────────────────────
async function start() {
  try {
    await connectRedis();
    await initDB();
    await startAllMonitors();
    server.listen(PORT, () => {
      console.log(`\n🚀 Dev Monitoring Suite running on port ${PORT}`);
      console.log(`📊 Dashboard: http://localhost:${PORT}`);
      console.log(`🏥 Health:    http://localhost:${PORT}/health\n`);
    });
  } catch (err) {
    console.error('❌ Startup failed:', err);
    process.exit(1);
  }
}

start();
