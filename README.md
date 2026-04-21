# 🤖 Dev Monitoring Suite

**Full-stack uptime monitoring & alerting platform — deploy in 2 minutes on Railway.**

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/dev-monitoring-suite)

---

## What It Does

Dev Monitoring Suite is a self-hosted monitoring platform that keeps an eye on your websites and APIs. When something goes down, you'll know about it — via email, Slack, Discord, or any webhook.

- **Uptime monitoring** — check any URL on a schedule you control
- **Real-time dashboard** — live status updates via WebSockets
- **Incident management** — automatic incident creation on failure, auto-resolution on recovery
- **Email & webhook alerts** — Slack, Discord, Teams, or any HTTP endpoint
- **Response time charts** — visualize performance over the last 24 hours
- **Custom metrics API** — push your own data points for tracking
- **Self-hosted** — your data stays yours

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Runtime | Node.js 18+ |
| Framework | Express.js |
| Database | PostgreSQL |
| Cache | Redis |
| Real-time | Socket.io |
| Charts | Chart.js |
| HTTP checks | Axios |
| Alerts | Nodemailer + Webhooks |

---

## Deploy on Railway

Click the button above, or:

1. Go to [railway.app](https://railway.app) and create a new project
2. Select **"Deploy from GitHub repo"** and choose this repo
3. Add a **PostgreSQL** database plugin
4. Add a **Redis** database plugin
5. Railway auto-wires `DATABASE_URL` and `REDIS_URL`
6. Generate a domain under **Settings → Domains**
7. Visit your URL — the dashboard is live!

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | ✅ Auto | Set by Railway PostgreSQL plugin |
| `REDIS_URL` | ✅ Auto | Set by Railway Redis plugin |
| `SESSION_SECRET` | Recommended | Random string for security |
| `SMTP_HOST` | Optional | Email server for alerts |
| `SMTP_PORT` | Optional | Email port (default: 587) |
| `SMTP_USER` | Optional | Email username |
| `SMTP_PASS` | Optional | Email password/app key |
| `ALERT_FROM_EMAIL` | Optional | Sender email address |
| `ALERT_TO_EMAIL` | Optional | Default alert recipient |
| `WEBHOOK_URL` | Optional | Slack/Discord/Teams webhook |
| `DEFAULT_CHECK_INTERVAL` | Optional | Seconds between checks (default: 60) |
| `REQUEST_TIMEOUT` | Optional | Check timeout ms (default: 10000) |
| `ALERT_THRESHOLD` | Optional | Failures before alerting (default: 2) |

---

## API Reference

### Monitors

```
GET    /api/monitors              — List all monitors with latest status
POST   /api/monitors              — Create a new monitor
DELETE /api/monitors/:id          — Delete a monitor
POST   /api/monitors/:id/check    — Trigger an immediate check
GET    /api/monitors/:id/history  — Get check history (query: ?hours=24)
```

### Incidents

```
GET  /api/incidents           — List all incidents
POST /api/incidents/:id/resolve — Manually resolve an incident
```

### Custom Metrics

```
POST /api/metrics   — Record a custom metric
GET  /api/metrics   — Query metrics (query: ?name=cpu&hours=24)
```

### System

```
GET /health   — Health check endpoint
GET /         — Dashboard
```

### Example: Add a Monitor via API

```bash
curl -X POST https://your-app.up.railway.app/api/monitors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My Website",
    "url": "https://example.com",
    "interval_seconds": 300,
    "alert_email": "you@example.com"
  }'
```

### Example: Record a Custom Metric

```bash
curl -X POST https://your-app.up.railway.app/api/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cpu_usage",
    "value": 72.5,
    "unit": "percent",
    "tags": { "server": "prod-01" }
  }'
```

---

## Running Locally

```bash
# Clone the repo
git clone https://github.com/Jeah84/dev-monitoring-suite.git
cd dev-monitoring-suite

# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Edit .env with your database URLs and settings

# Start the server
npm start

# Or with auto-reload during development
npm run dev
```

You'll need a local PostgreSQL and Redis instance, or you can use Railway's private networking to connect to cloud instances.

---

## Estimated Railway Hosting Cost

| Service | Estimated Cost |
|---------|---------------|
| App (Node.js) | ~$5/month |
| PostgreSQL | ~$5/month |
| Redis | ~$5/month |
| **Total** | **~$15/month** |

Costs scale with usage. Idle apps sleep automatically on Railway's Hobby plan.

---

## Support

- **Issues:** [GitHub Issues](https://github.com/Jeah84/dev-monitoring-suite/issues)
- **Railway Docs:** [docs.railway.com](https://docs.railway.com)
- **Email:** jeahrauch84@gmail.com

---

## License

MIT © 2026 Jeah84 — Free to use, modify, and distribute.
