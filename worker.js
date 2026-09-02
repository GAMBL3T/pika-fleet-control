'use strict'

const mineflayer = require('mineflayer')
const fs = require('node:fs')
const path = require('node:path')
const { spawn } = require('node:child_process')
const readline = require('node:readline')
const http = require('node:http')
const { WebSocketServer, WebSocket } = require('ws')

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const CONFIG_FILE = path.join(__dirname, 'config.json')

function loadConfigFile () {
  try {
    return JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'))
  } catch (err) {
    console.error('Could not read config.json: ' + err.message)
    return {}
  }
}

const FILE_CONFIG = loadConfigFile()
const configNumber = (envName, fileValue, fallback) => Number(process.env[envName] ?? fileValue ?? fallback)
const configBoolean = (envName, fileValue, fallback) => process.env[envName] === undefined
  ? fileValue ?? fallback
  : process.env[envName] !== '0'
const configAutoCommands = (commands) => Array.isArray(commands)
  ? commands.map((entry) => {
    const command = typeof entry === 'string' ? entry : entry && entry.command
    const delay = typeof entry === 'object' && entry !== null ? Number(entry.delay ?? 0) : 0
    return { command, delay }
  }).filter(({ command, delay }) => typeof command === 'string' && command.startsWith('/') && Number.isFinite(delay) && delay >= 0)
  : []

// Environment variables override config.json for one-off runs.
const CONFIG = {
  host: process.env.HOST || FILE_CONFIG.host || 'localhost',
  port: configNumber('PORT', FILE_CONFIG.port, 25565),
  // Pin the protocol version. Letting mineflayer auto-detect costs an extra
  // handshake per bot and can pick a different version per bot on a proxy.
  version: process.env.MC_VERSION || FILE_CONFIG.version || '1.20.1',
  count: configNumber('COUNT', FILE_CONFIG.accountCount ?? FILE_CONFIG.count, 10),
  prefix: process.env.PREFIX || FILE_CONFIG.prefix || 'bot',
  dashboardHost: process.env.DASHBOARD_HOST || FILE_CONFIG.dashboardHost || '127.0.0.1',
  dashboardPort: configNumber('DASHBOARD_PORT', FILE_CONFIG.dashboardPort, 3000),
  controlHost: process.env.CONTROL_HOST || FILE_CONFIG.controlHost || '127.0.0.1',
  controlPort: configNumber('CONTROL_PORT', FILE_CONFIG.controlPort, 3001),
  openDashboard: configBoolean('OPEN_DASHBOARD', FILE_CONFIG.openDashboard, true),
  openVerification: configBoolean('OPEN_VERIFICATION', FILE_CONFIG.openVerification, true),
  maxVerificationLinks: configNumber('MAX_VERIFICATION_LINKS', FILE_CONFIG.maxVerificationLinks, 3) > 0
    ? Math.floor(configNumber('MAX_VERIFICATION_LINKS', FILE_CONFIG.maxVerificationLinks, 3))
    : 3,
  // Offline mode only. Premium auth is per-account and rate limited; you cannot
  // stand up 100 authenticated sessions from one machine.
  auth: 'offline',

  // Launch accounts in controlled batches. Environment values override the
  // matching config.json values for a one-off run.
  batchSize: Math.max(1, Math.floor(configNumber('BATCH_SIZE', FILE_CONFIG.batchSize, 10))),
  batchAccountDelay: Math.max(0, configNumber('BATCH_ACCOUNT_DELAY', FILE_CONFIG.batchAccountDelay ?? FILE_CONFIG.joinDelay, 40000)),
  batchCooldown: Math.max(0, configNumber('BATCH_COOLDOWN', FILE_CONFIG.batchCooldown, 180000)),
  autoCommands: configAutoCommands(FILE_CONFIG.autoCommands),

  // Reconnect backoff, in ms: first retry, cap, and multiplier.
  reconnectBase: 5000,
  reconnectMax: 120000,
  reconnectFactor: 2,
}

const LOG_DIR = path.join(__dirname, 'logs')

function updateAutoCommands (commands) {
  if (!Array.isArray(commands)) throw new Error('Auto commands must be a list.')
  const normalized = configAutoCommands(commands)
  if (normalized.length !== commands.length) {
    throw new Error('Each auto command needs a slash command and a non-negative delay.')
  }
  CONFIG.autoCommands = normalized
  FILE_CONFIG.autoCommands = normalized
  fs.writeFileSync(CONFIG_FILE, `${JSON.stringify(FILE_CONFIG, null, 2)}\n`)
  return normalized
}

// ---------------------------------------------------------------------------
// Fleet state
// ---------------------------------------------------------------------------
const bots = new Map() // username -> { bot, attempts, timers, logFile, status, logs }
let shuttingDown = false
const startupTimers = new Set()
const pendingUsernames = []
const startupInFlight = new Set()
let connectionDispatchPaused = false
let nextScheduledStartAt = 0
let batchPosition = 0
let batchGateTimer = null
let commandConsole = null
const commandTimers = new Set()
let dashboardServer = null
const dashboardClients = new Set()
let controlServer = null
const controlClients = new Set()
const activeVerifications = new Set()
let telemetryTimer = null
let lastCpuUsage = process.cpuUsage()
let lastCpuSampleAt = process.hrtime.bigint()
let processCpuPercent = 0

function processTelemetry () {
  const now = process.hrtime.bigint()
  const elapsedMicros = Number(now - lastCpuSampleAt) / 1000
  if (elapsedMicros > 0) {
    const cpuUsage = process.cpuUsage(lastCpuUsage)
    processCpuPercent = ((cpuUsage.user + cpuUsage.system) / elapsedMicros) * 100
    lastCpuUsage = process.cpuUsage()
    lastCpuSampleAt = now
  }
  const memory = process.memoryUsage()
  return {
    cpuPercent: processCpuPercent,
    ramMb: memory.rss / (1024 * 1024),
  }
}

function newBotState (username) {
  const safeName = username.replace(/[^a-zA-Z0-9._-]/g, '_')
  return {
    bot: null,
    attempts: 0,
    timers: new Set(),
    logFile: path.join(LOG_DIR, `${safeName}.log`),
    status: 'stopped',
    manuallyStopped: false,
    scheduledStartAt: null,
    logs: [],
    lastVerificationUrl: null,
    verificationPending: false,
    queuedVerificationUrl: null,
    waitingForVerificationSlot: false,
    retryDelayOverride: null,
    claimingPlaytimeRewards: false,
    lastChatMessage: null,
    suppressedChatRepeats: 0,
  }
}

function log (username, msg) {
  const line = `[${new Date().toISOString()}] ${username.padEnd(12)} ${msg}`

  const state = bots.get(username)
  if (state) {
    state.logs.push({ time: new Date().toISOString(), message: msg })
    if (state.logs.length > 200) state.logs.shift()
    try {
      fs.appendFileSync(state.logFile, `${line}\n`)
    } catch (err) {
      console.error(`Could not write ${username}'s log: ${err.message}`)
    }
  }
  publishDashboard()
}

// Servers often repeat lobby tips every second. Keep the first occurrence in
// the per-bot log and collapse consecutive duplicates until a new chat message
// arrives.
function logChat (username, state, msg) {
  if (state.lastChatMessage === msg) {
    state.suppressedChatRepeats += 1
    return
  }

  if (state.suppressedChatRepeats > 0) {
    log(username, `chat: previous message repeated ${state.suppressedChatRepeats} time(s), suppressed`)
  }
  state.lastChatMessage = msg
  state.suppressedChatRepeats = 0
  log(username, `chat: ${msg}`)
}

function launchVerificationUrl (username, state, url) {
  const wasWaitingForSlot = state.waitingForVerificationSlot
  state.waitingForVerificationSlot = false
  state.queuedVerificationUrl = null
  state.verificationPending = true
  state.lastVerificationUrl = url
  startupInFlight.delete(username)
  activeVerifications.add(username)
  log(username, `action: opening verification URL in browser: ${url}`)
  const browser = spawn('cmd.exe', ['/d', '/c', 'start', '', url], {
    detached: true,
    stdio: 'ignore',
    windowsHide: true,
  })
  browser.on('error', (err) => {
    activeVerifications.delete(username)
    state.verificationPending = false
    state.queuedVerificationUrl = url
    log(username, `verification browser error: ${err.message}`)
  })
  browser.unref()

  // Bots that were paused because all slots were occupied need a fresh
  // connection attempt after their browser verification is opened.
  if (wasWaitingForSlot && state.bot === null && !shuttingDown) {
    scheduleReconnect(username, state, 'verification slot opened')
  }
}

function drainVerificationQueue () {
  if (!CONFIG.openVerification || process.platform !== 'win32' || shuttingDown) return
  for (const [username, state] of bots) {
    if (activeVerifications.size >= CONFIG.maxVerificationLinks) return
    if (!state.verificationPending && state.queuedVerificationUrl) {
      launchVerificationUrl(username, state, state.queuedVerificationUrl)
    }
  }
}

function openVerificationUrl (username, state, reason) {
  if (!CONFIG.openVerification || process.platform !== 'win32') return

  // The server's formatted message can append Minecraft color codes directly
  // after the URL, so only accept the URL token itself.
  const match = reason.match(/https:\/\/v4g\.to\/[A-Za-z0-9_-]+/)
  const url = match && match[0]
  if (!url || state.lastVerificationUrl === url || state.verificationPending) return

  if (activeVerifications.size >= CONFIG.maxVerificationLinks) {
    const isNewlyQueued = state.queuedVerificationUrl === null
    state.queuedVerificationUrl = url
    state.waitingForVerificationSlot = true
    if (isNewlyQueued) log(username, `verification link queued; ${CONFIG.maxVerificationLinks} browser slots are active`)
    return
  }
  launchVerificationUrl(username, state, url)
}

function releaseVerificationSlot (username, state) {
  if (!activeVerifications.delete(username)) return false
  state.verificationPending = false
  state.queuedVerificationUrl = null
  log(username, 'verification browser slot released')
  drainVerificationQueue()
  fillInitialConnectionSlots()
  return true
}

function dashboardState () {
  const telemetry = processTelemetry()
  const accounts = [...bots.entries()]
      .sort(([a], [b]) => a.localeCompare(b, undefined, { numeric: true }))
      .map(([username, state]) => ({
        username,
        status: state.status,
        isConnected: Boolean(state.bot),
        manuallyStopped: state.manuallyStopped,
        scheduledStartAt: state.scheduledStartAt,
        attempts: state.attempts,
        verificationPending: state.verificationPending,
        waitingForVerificationSlot: state.waitingForVerificationSlot,
        lastActivity: state.scheduledStartAt
          ? `Scheduled to start at ${new Date(state.scheduledStartAt).toLocaleTimeString()}`
          : state.logs.length ? state.logs[state.logs.length - 1].message : 'Queued for connection',
        logs: state.logs,
      }))
  return {
    fleet: {
      host: CONFIG.host,
      port: CONFIG.port,
      version: CONFIG.version,
      accountCapacity: CONFIG.count,
      verificationCapacity: CONFIG.maxVerificationLinks,
      activeVerifications: activeVerifications.size,
      queuedConnections: pendingUsernames.length,
      connectionDispatchPaused,
      batchSize: CONFIG.batchSize,
      batchAccountDelay: CONFIG.batchAccountDelay,
      batchCooldown: CONFIG.batchCooldown,
      nextScheduledStartAt,
      autoCommands: CONFIG.autoCommands,
      cpuPercent: telemetry.cpuPercent,
      ramMb: telemetry.ramMb,
    },
    accounts,
  }
}

function publishDashboard () {
  const message = JSON.stringify({ type: 'state', state: dashboardState() })
  for (const client of controlClients) {
    if (client.readyState !== WebSocket.OPEN) {
      controlClients.delete(client)
      continue
    }
    try { client.send(message) } catch { controlClients.delete(client) }
  }
}

function handleControlAction (action, payload) {
  switch (action) {
    case 'command': return sendBotCommand(payload.target, payload.command, payload.delay ?? 0)
    case 'claimPlaytimeRewards': return claimPlaytimeRewardsForTargets(payload.target, payload.delay ?? 0)
    case 'toggleConnections':
      connectionDispatchPaused = !connectionDispatchPaused
      if (!connectionDispatchPaused) fillInitialConnectionSlots()
      return connectionDispatchPaused ? 'New account connections paused.' : 'New account connections resumed.'
    case 'startFleet': return `Scheduled ${startAccounts(payload.count)} account(s) using the configured batch rate.`
    case 'stopFleet': return `Stopped ${stopAllAccounts()} account(s).`
    case 'startAccount':
      if (!enqueueAccount(payload.target)) throw new Error('Account cannot be started in its current state.')
      return `Scheduled ${payload.target}.`
    case 'stopAccount':
      if (!stopAccount(payload.target)) throw new Error('Account cannot be stopped in its current state.')
      return `Stopped ${payload.target}.`
    case 'releaseVerification': {
      const state = bots.get(payload.target)
      if (!state || !releaseVerificationSlot(payload.target, state)) throw new Error(`No active verification slot for "${payload.target}".`)
      return `Released ${payload.target}'s verification slot.`
    }
    case 'setAutoCommands': return `Saved ${updateAutoCommands(payload.commands).length} auto command(s).`
    case 'ping': return 'Worker is online.'
    default: throw new Error(`Unknown worker action: ${action}`)
  }
}

function startControlServer () {
  controlServer = new WebSocketServer({ host: CONFIG.controlHost, port: CONFIG.controlPort })
  controlServer.on('connection', (socket) => {
    controlClients.add(socket)
    try { socket.send(JSON.stringify({ type: 'state', state: dashboardState() })) } catch {}
    socket.on('message', (raw) => {
      let request
      try {
        request = JSON.parse(raw.toString())
        if (!request || typeof request.action !== 'string') throw new Error('Invalid control request.')
        const message = handleControlAction(request.action, request.payload || {})
        socket.send(JSON.stringify({ type: 'response', id: request.id, ok: true, message }))
        publishDashboard()
      } catch (err) {
        try { socket.send(JSON.stringify({ type: 'response', id: request && request.id, ok: false, message: err.message })) } catch {}
      }
    })
    socket.on('close', () => controlClients.delete(socket))
    socket.on('error', () => controlClients.delete(socket))
  })
  controlServer.on('listening', () => console.log(`Worker control socket: ws://${CONFIG.controlHost}:${CONFIG.controlPort}`))
  controlServer.on('error', (err) => console.error(`Worker control socket error: ${err.message}`))
}

const DASHBOARD_HTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Fleet control</title>
  <style>
    /* ---------------------------------------------------------------
       Design tokens. One calm dark theme, one accent, no gradients.
       --------------------------------------------------------------- */
    :root {
      color-scheme: dark;

      --bg: #0b0e13;
      --surface: #12161d;
      --surface-raised: #171c25;
      --surface-sunken: #0a0d12;
      --surface-hover: #1b212c;

      --border: #232b37;
      --border-strong: #313b4a;

      --text: #e7ecf3;
      --text-muted: #9aa6b7;
      --text-faint: #6d7889;

      --accent: #4c8dff;
      --accent-hover: #6b9fff;
      --accent-press: #3d7ae6;
      --accent-soft: #16233a;
      --accent-text: #a8c6ff;

      --ok: #4fb37a;
      --ok-soft: #12261b;
      --warn: #d9a441;
      --warn-soft: #2a2113;
      --danger: #e06c6c;
      --danger-soft: #2c1719;
      --neutral: #7c8899;
      --neutral-soft: #191e27;

      --radius-sm: 4px;
      --radius: 6px;
      --radius-lg: 8px;

      --gap: 12px;
      --pad: 16px;

      --font: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
      --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;

      --focus: 0 0 0 2px var(--bg), 0 0 0 4px var(--accent);

      font-family: var(--font);
      background: var(--bg);
      color: var(--text);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font-size: 14px;
      line-height: 1.45;
      -webkit-font-smoothing: antialiased;
    }

    h1, h2, h3, p { margin: 0; }

    /* ---------------------------------------------------------------
       Form controls
       --------------------------------------------------------------- */
    button, input, select, textarea { font: inherit; color: inherit; }

    input, select, textarea {
      min-width: 0;
      padding: 8px 10px;
      border: 1px solid var(--border-strong);
      border-radius: var(--radius);
      background: var(--surface-sunken);
      color: var(--text);
      outline: none;
    }
    input::placeholder, textarea::placeholder { color: var(--text-faint); }
    input:hover, select:hover, textarea:hover { border-color: #3d4a5c; }
    input:focus-visible, select:focus-visible, textarea:focus-visible,
    input:focus, select:focus, textarea:focus { border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-soft); }
    input[type="number"] { font-variant-numeric: tabular-nums; }
    select { appearance: none; padding-right: 28px; background-image: none; }

    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      padding: 8px 12px;
      border: 1px solid transparent;
      border-radius: var(--radius);
      font-weight: 600;
      white-space: nowrap;
      cursor: pointer;
      transition: background-color .12s ease, border-color .12s ease, color .12s ease;
    }
    .btn-primary { color: #fff; background: var(--accent); }
    .btn-primary:hover { background: var(--accent-hover); }
    .btn-primary:active { background: var(--accent-press); }
    .btn-secondary { color: var(--text); border-color: var(--border-strong); background: var(--surface-raised); }
    .btn-secondary:hover { border-color: #435063; background: var(--surface-hover); }
    .btn-secondary:active { background: #212836; }
    .btn-quiet { color: var(--text-muted); border-color: transparent; background: transparent; }
    .btn-quiet:hover { color: var(--text); background: var(--surface-hover); }
    .btn.active, .btn[aria-pressed="true"] { color: var(--accent-text); border-color: #33507f; background: var(--accent-soft); }
    .btn:disabled { cursor: not-allowed; opacity: .42; }
    .btn:disabled:hover { background: var(--surface-raised); border-color: var(--border-strong); }
    .btn-primary:disabled:hover { background: var(--accent); }
    .btn-sm { padding: 6px 9px; font-size: .78rem; }

    :focus-visible { outline: none; box-shadow: var(--focus); border-radius: var(--radius-sm); }

    /* ---------------------------------------------------------------
       Shell
       --------------------------------------------------------------- */
    .app { max-width: 1560px; margin: 0 auto; padding: 22px 24px 32px; }

    .topbar {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 24px;
      padding-bottom: 18px;
      border-bottom: 1px solid var(--border);
    }
    .eyebrow {
      color: var(--text-faint);
      font-size: .7rem;
      font-weight: 700;
      letter-spacing: .1em;
      text-transform: uppercase;
    }
    h1 { margin-top: 4px; font-size: 1.5rem; font-weight: 650; letter-spacing: -.015em; }
    .subtitle { margin-top: 4px; color: var(--text-muted); font-size: .875rem; }

    .environment {
      display: grid;
      gap: 8px;
      min-width: 280px;
      padding: 12px 14px;
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      background: var(--surface);
    }
    .environment-label { color: var(--text-faint); font-size: .68rem; font-weight: 700; letter-spacing: .1em; text-transform: uppercase; }
    .environment-value { font-family: var(--mono); font-size: .85rem; font-weight: 500; overflow-wrap: anywhere; }
    .stream { display: inline-flex; align-items: center; gap: 7px; color: var(--text-muted); font-size: .78rem; }
    .stream::before { content: ""; flex: none; width: 7px; height: 7px; border-radius: 50%; background: var(--warn); }
    .stream.live { color: var(--text-muted); }
    .stream.live::before { background: var(--ok); }
    .stream.down { color: #d99b9b; }
    .stream.down::before { background: var(--danger); }
    .reload-dashboard { width: 100%; }

    /* ---------------------------------------------------------------
       Metrics
       --------------------------------------------------------------- */
    .metrics {
      display: grid;
      grid-template-columns: repeat(7, minmax(0, 1fr));
      gap: var(--gap);
      margin: 18px 0;
      padding: 0;
      border: 0;
    }
    .metric {
      display: grid;
      gap: 6px;
      padding: 12px 14px;
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      background: var(--surface);
    }
    .metric-label { color: var(--text-muted); font-size: .7rem; font-weight: 700; letter-spacing: .07em; text-transform: uppercase; }
    .metric-value { font-size: 1.4rem; font-weight: 650; letter-spacing: -.02em; font-variant-numeric: tabular-nums; }

    /* ---------------------------------------------------------------
       Panels
       --------------------------------------------------------------- */
    .panel {
      padding: var(--pad);
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      background: var(--surface);
    }
    .panel-primary { border-color: var(--border-strong); background: var(--surface-raised); }
    .panel-secondary { background: var(--surface); }
    .panel-secondary .panel-heading h2 { font-size: .82rem; color: var(--text-muted); letter-spacing: .06em; text-transform: uppercase; }

    .panel-heading {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }
    .panel-heading h2 { font-size: .95rem; font-weight: 650; }
    .panel-note { color: var(--text-muted); font-size: .78rem; text-align: right; }

    .fleet-controls { display: grid; grid-template-columns: 110px auto auto auto 1fr; gap: 10px; align-items: center; }
    .fleet-capacity { color: var(--text-muted); font-size: .78rem; text-align: right; font-variant-numeric: tabular-nums; }

    .fleet-ops { display: grid; grid-template-columns: 1.35fr 1fr; gap: var(--gap); margin-top: var(--gap); }

    .controls { display: grid; grid-template-columns: minmax(130px, .8fr) minmax(200px, 2fr) 110px auto; gap: 10px; }
    .history-row { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 10px; align-items: center; margin-top: 10px; }
    .quick-actions { display: flex; flex-wrap: wrap; align-items: center; gap: 7px; margin-top: 10px; padding-top: 12px; border-top: 1px solid var(--border); }
    .quick-actions input { width: 150px; padding: 6px 9px; font-size: .78rem; }

    .auto-command-form { display: grid; gap: 10px; }
    .auto-command-form textarea { min-height: 104px; resize: vertical; font: .82rem/1.5 var(--mono); }
    .auto-command-form .btn { justify-self: start; }

    .notice { min-height: 1.35em; margin-top: 10px; color: var(--text-muted); font-size: .82rem; }
    .notice.is-ok { color: var(--ok); }
    .notice.is-error { color: var(--danger); }

    .shortcut { color: var(--text-faint); font-size: .75rem; white-space: nowrap; }

    /* ---------------------------------------------------------------
       Status chips
       --------------------------------------------------------------- */
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 2px 8px;
      border: 1px solid var(--border-strong);
      border-radius: 999px;
      background: var(--neutral-soft);
      color: var(--text-muted);
      font-size: .72rem;
      font-weight: 600;
      line-height: 1.6;
      white-space: nowrap;
    }
    .badge::before { content: ""; flex: none; width: 6px; height: 6px; border-radius: 50%; background: var(--neutral); }
    .badge.online { color: #8fd8ac; border-color: #23503a; background: var(--ok-soft); }
    .badge.online::before { background: var(--ok); }
    .badge.connecting, .badge.awaiting-welcome { color: var(--accent-text); border-color: #2c4470; background: var(--accent-soft); }
    .badge.connecting::before, .badge.awaiting-welcome::before { background: var(--accent); }
    .badge.reconnecting, .badge.stopping { color: #e8c887; border-color: #56471f; background: var(--warn-soft); }
    .badge.reconnecting::before, .badge.stopping::before { background: var(--warn); }
    .badge.kicked, .badge.waiting-verification-slot { color: #eda1a1; border-color: #5a2c2e; background: var(--danger-soft); }
    .badge.kicked::before, .badge.waiting-verification-slot::before { background: var(--danger); }
    .badge.queued::before, .badge.scheduled::before, .badge.stopped::before { background: var(--neutral); }

    /* ---------------------------------------------------------------
       Workspace: account list + selected account
       --------------------------------------------------------------- */
    .workspace { display: grid; grid-template-columns: minmax(360px, .8fr) minmax(520px, 1.6fr); gap: var(--gap); align-items: start; margin-top: var(--gap); }

    .account-toolbar { display: grid; grid-template-columns: minmax(0, 1fr) auto auto; gap: 8px; }
    .account-toolbar-row { display: grid; grid-template-columns: auto auto; gap: 8px; margin-top: 8px; justify-content: start; }

    .accounts { display: grid; gap: 5px; margin-top: 12px; }

    .account {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 148px;
      gap: 2px 12px;
      width: 100%;
      padding: 9px 11px;
      border: 1px solid var(--border);
      border-left: 3px solid transparent;
      border-radius: var(--radius);
      background: var(--surface-raised);
      color: var(--text);
      text-align: left;
      cursor: pointer;
      transition: background-color .12s ease, border-color .12s ease;
    }
    .account:hover { background: var(--surface-hover); border-color: var(--border-strong); }
    .account.selected { border-color: var(--border-strong); border-left-color: var(--accent); background: var(--accent-soft); }
    .account-name { font-weight: 650; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .account-activity { grid-column: 1; color: var(--text-muted); font-size: .76rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .account-flag { grid-column: 2; align-self: center; color: var(--text-faint); font-size: .72rem; white-space: nowrap; }
    .account-flag.attention { color: var(--warn); }
    .account .badge { grid-column: 2; grid-row: 1; justify-self: start; }

    .placeholder {
      display: grid;
      gap: 4px;
      padding: 26px 16px;
      border: 1px dashed var(--border-strong);
      border-radius: var(--radius);
      color: var(--text-muted);
      text-align: center;
    }
    .placeholder strong { color: var(--text); font-weight: 600; }
    .placeholder span { font-size: .8rem; }
    .placeholder.is-error { border-color: #5a2c2e; color: #eda1a1; }

    /* Selected account panel ---------------------------------------- */
    .account-panel { display: grid; gap: 12px; }
    .account-panel-heading { align-items: flex-start; margin-bottom: 0; }
    .account-identity { display: grid; gap: 2px; min-width: 0; }
    .account-identity h2 { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .log-meta { color: var(--text-muted); font-size: .78rem; }

    .account-controls {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 7px;
      padding: 10px;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      background: var(--surface-sunken);
    }
    .control-group { display: inline-flex; align-items: center; gap: 7px; }
    .control-group + .control-group { padding-left: 10px; border-left: 1px solid var(--border); }
    .control-spacer { flex: 1 1 auto; }

    .log-toolbar { display: grid; grid-template-columns: minmax(0, 1fr) 130px repeat(5, auto); gap: 8px; }

    .logs {
      display: block;
      min-height: 380px;
      max-height: 56vh;
      margin: 0;
      padding: 10px 12px;
      overflow: auto;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      background: var(--surface-sunken);
      color: var(--text);
      font: .8rem/1.55 var(--mono);
      white-space: normal;
    }
    .log-line { display: grid; grid-template-columns: 4.6rem minmax(0, 1fr); gap: 10px; padding: 1px 0; }
    .log-line + .log-line { border-top: 1px solid rgba(255, 255, 255, .03); }
    .log-time { color: var(--text-faint); font-variant-numeric: tabular-nums; }
    .log-message { white-space: pre-wrap; overflow-wrap: anywhere; }
    .logs.nowrap { overflow-x: auto; }
    .logs.nowrap .log-message { white-space: pre; overflow-wrap: normal; }
    .log-line.alert .log-message { color: #eda1a1; }
    .log-line.warn .log-message { color: #e8c887; }
    .log-line.chat .log-message { color: #b9c6d6; }
    .log-empty { display: block; padding: 22px 0; color: var(--text-faint); text-align: center; font-family: var(--font); }

    .log-summary { display: flex; justify-content: space-between; gap: 12px; color: var(--text-muted); font-size: .75rem; font-variant-numeric: tabular-nums; }
    .client-command { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 10px; }

    .footer { display: flex; justify-content: space-between; gap: 12px; margin-top: 18px; padding-top: 14px; border-top: 1px solid var(--border); color: var(--text-faint); font-size: .75rem; }

    .visually-hidden { position: absolute; width: 1px; height: 1px; margin: -1px; padding: 0; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; border: 0; }

    /* ---------------------------------------------------------------
       Responsive
       --------------------------------------------------------------- */
    @media (min-width: 1101px) {
      .workspace > .account-panel { position: sticky; top: 16px; }
      .account-panel .logs { max-height: calc(100vh - 380px); }
    }
    @media (max-width: 1300px) {
      .metrics { grid-template-columns: repeat(4, minmax(0, 1fr)); }
      .log-toolbar { grid-template-columns: minmax(0, 1fr) 130px; grid-auto-flow: column dense; }
    }
    @media (max-width: 1100px) {
      .workspace { grid-template-columns: minmax(0, 1fr); }
      .fleet-ops { grid-template-columns: minmax(0, 1fr); }
      .account-panel { position: static; }
    }
    @media (max-width: 860px) {
      .app { padding: 16px; }
      .topbar { display: grid; }
      .environment { min-width: 0; }
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .controls, .client-command, .history-row, .fleet-controls, .account-toolbar { grid-template-columns: minmax(0, 1fr); }
      .log-toolbar { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .fleet-capacity, .panel-note { text-align: left; }
      .panel-heading { display: grid; gap: 4px; }
      .account { grid-template-columns: minmax(0, 1fr); }
      .account .badge, .account-flag { grid-column: 1; justify-self: start; }
      .account .badge { grid-row: auto; margin-top: 4px; }
      .logs { min-height: 300px; max-height: 60vh; }
      .control-group + .control-group { padding-left: 0; border-left: 0; }
      .footer { display: grid; gap: 4px; }
    }
    @media (prefers-reduced-motion: reduce) {
      * { transition: none !important; }
    }
  </style>
</head>
<body>
  <div class="app">
    <header class="topbar">
      <div>
        <p class="eyebrow">Local dashboard</p>
        <h1>Fleet control</h1>
        <p class="subtitle">Start accounts, send commands, and review client activity.</p>
      </div>
      <div class="environment">
        <span class="environment-label">Server target</span>
        <strong id="fleet-target" class="environment-value">Connecting dashboard…</strong>
        <span id="stream-status" class="stream">Syncing live stream</span>
        <button type="button" id="reload-dashboard" class="btn btn-secondary btn-sm reload-dashboard">Reload dashboard</button>
      </div>
    </header>

    <section class="metrics" aria-label="Account summary">
      <div class="metric"><span class="metric-label">Accounts</span><strong id="metric-total" class="metric-value">0</strong></div>
      <div class="metric"><span class="metric-label">Online</span><strong id="metric-online" class="metric-value">0</strong></div>
      <div class="metric"><span class="metric-label">Connecting</span><strong id="metric-connecting" class="metric-value">0</strong></div>
      <div class="metric"><span class="metric-label">Verification</span><strong id="metric-verification" class="metric-value">0 / 0</strong></div>
      <div class="metric"><span class="metric-label">Queued</span><strong id="metric-queued" class="metric-value">0</strong></div>
      <div class="metric"><span class="metric-label">CPU</span><strong id="metric-cpu" class="metric-value">0%</strong></div>
      <div class="metric"><span class="metric-label">RAM</span><strong id="metric-ram" class="metric-value">0 MB</strong></div>
    </section>

    <section class="panel panel-primary" aria-labelledby="start-heading">
      <div class="panel-heading">
        <h2 id="start-heading">Start and stop accounts</h2>
        <span class="panel-note">Accounts only start after you select an action.</span>
      </div>
      <div class="fleet-controls">
        <input id="fleet-start-count" aria-label="Number of accounts to start" type="number" min="1" value="1">
        <button type="button" id="fleet-start" class="btn btn-primary">Start selected count</button>
        <button type="button" id="fleet-start-remaining" class="btn btn-secondary">Start remaining</button>
        <button type="button" id="fleet-stop-all" class="btn btn-secondary">Stop all</button>
        <span id="fleet-capacity" class="fleet-capacity">0 available</span>
      </div>
    </section>

    <div class="fleet-ops">
      <section class="panel panel-secondary" aria-labelledby="dispatch-heading">
        <div class="panel-heading">
          <h2 id="dispatch-heading">Fleet command</h2>
          <span class="panel-note">Send to one account or space commands across connected accounts.</span>
        </div>
        <form id="command-form" class="controls">
          <select id="target" aria-label="Target account"><option value="all">All connected bots</option></select>
          <input id="command" aria-label="Minecraft command" placeholder="/register password password" autocomplete="off" required>
          <input id="delay" aria-label="Delay between bots in milliseconds" type="number" min="0" value="0">
          <button type="submit" class="btn btn-primary">Dispatch</button>
        </form>
        <div class="history-row">
          <select id="command-history" aria-label="Recent command history"><option value="">Recent safe commands</option></select>
          <span id="command-counter" class="shortcut">0 characters · Ctrl/Cmd + K focuses selected-client command</span>
        </div>
        <div class="quick-actions">
          <button type="button" id="claim-playtime-rewards" class="btn btn-secondary btn-sm">Claim playtime rewards</button>
          <input id="claim-delay" aria-label="Delay between account reward claims in milliseconds" type="number" min="0" value="0" placeholder="Claim spacing (ms)">
          <button type="button" class="btn btn-quiet btn-sm" data-delay-preset="0">No delay</button>
          <button type="button" class="btn btn-quiet btn-sm" data-delay-preset="1000">1s spacing</button>
          <button type="button" class="btn btn-quiet btn-sm" data-delay-preset="5000">5s spacing</button>
          <button type="button" id="toggle-dispatch" class="btn btn-secondary btn-sm">Pause new connections</button>
          <button type="button" id="export-fleet" class="btn btn-secondary btn-sm">Export fleet snapshot</button>
        </div>
        <p id="notice" class="notice" role="status" aria-live="polite"></p>
      </section>

      <section class="panel panel-secondary" aria-labelledby="auto-heading">
        <div class="panel-heading">
          <h2 id="auto-heading">Auto commands after login</h2>
        </div>
        <form id="auto-command-form" class="auto-command-form">
          <textarea id="auto-commands" aria-label="Auto commands after login" aria-describedby="auto-commands-help" placeholder="1000 /register password password&#10;4000 /ptr" spellcheck="false"></textarea>
          <span id="auto-commands-help" class="shortcut">One command per line: optional delay in ms, then the slash command.</span>
          <button type="submit" class="btn btn-primary">Save auto commands</button>
        </form>
      </section>
    </div>

    <main class="workspace">
      <section class="panel" aria-labelledby="accounts-heading">
        <div class="panel-heading">
          <h2 id="accounts-heading">Accounts</h2>
          <span id="account-count" class="panel-note">0 tracked</span>
        </div>
        <div class="account-toolbar">
          <input id="account-filter" aria-label="Filter accounts" placeholder="Search accounts or status">
          <select id="status-filter" aria-label="Filter account status"><option value="all">All statuses</option><option value="active">Active</option><option value="verification">Verification</option><option value="queued">Queued</option><option value="issues">Needs attention</option></select>
          <select id="sort-accounts" aria-label="Sort accounts"><option value="pinned">Pinned first</option><option value="name">Name</option><option value="status">Status</option><option value="activity">Recent activity</option></select>
        </div>
        <div class="account-toolbar-row">
          <button type="button" id="pinned-only" class="btn btn-secondary btn-sm" aria-pressed="false">Pinned only</button>
          <button type="button" id="reset-filters" class="btn btn-quiet btn-sm">Reset filters</button>
        </div>
        <div id="accounts" class="accounts">
          <p class="placeholder"><strong>Loading accounts</strong><span>Waiting for the first worker update.</span></p>
        </div>
      </section>

      <section class="panel account-panel" aria-labelledby="log-title">
        <div class="panel-heading account-panel-heading">
          <div class="account-identity">
            <h2 id="log-title">Selected account</h2>
            <span id="log-meta" class="log-meta">Select an account</span>
          </div>
          <span id="selected-status" class="badge">No account</span>
        </div>

        <div class="account-controls" role="group" aria-label="Selected account controls">
          <div class="control-group">
            <button type="button" id="selected-start" class="btn btn-primary btn-sm" disabled>Start</button>
            <button type="button" id="selected-stop" class="btn btn-secondary btn-sm" disabled>Stop</button>
          </div>
          <div class="control-group">
            <button type="button" id="selected-pin" class="btn btn-secondary btn-sm" aria-pressed="false" disabled>Pin</button>
            <button type="button" id="release-verification" class="btn btn-secondary btn-sm">Release verification slot</button>
          </div>
          <span class="control-spacer"></span>
          <div class="control-group">
            <button type="button" id="previous-account" class="btn btn-quiet btn-sm">Previous</button>
            <button type="button" id="next-account" class="btn btn-quiet btn-sm">Next</button>
          </div>
        </div>

        <div class="log-toolbar">
          <input id="log-search" aria-label="Search selected logs" placeholder="Search selected logs">
          <select id="log-filter" aria-label="Filter selected logs"><option value="all">All events</option><option value="chat">Chat only</option><option value="actions">Actions</option><option value="alerts">Alerts</option></select>
          <button type="button" id="toggle-scroll" class="btn btn-secondary btn-sm" aria-pressed="true">Auto-scroll: on</button>
          <button type="button" id="toggle-wrap" class="btn btn-secondary btn-sm" aria-pressed="true">Wrap: on</button>
          <button type="button" id="jump-latest" class="btn btn-quiet btn-sm">Latest</button>
          <button type="button" id="copy-log" class="btn btn-quiet btn-sm">Copy</button>
          <button type="button" id="download-log" class="btn btn-quiet btn-sm">Export</button>
        </div>

        <pre id="logs" class="logs" tabindex="0" aria-label="Selected account activity"><span class="log-empty">Select an account to view its activity.</span></pre>

        <div class="log-summary"><span id="log-count">0 visible events</span><span id="alert-count">0 alerts</span></div>

        <form id="client-command-form" class="client-command">
          <input id="client-command" aria-label="Command for selected account" placeholder="/register password password" autocomplete="off" required>
          <button type="submit" class="btn btn-primary">Send to selected</button>
        </form>
      </section>
    </main>

    <footer class="footer">
      <span>Runs only on this computer · command activity is saved per account</span>
      <span id="last-updated">Waiting for live data</span>
    </footer>
  </div>
  <script>
    const accountsElement = document.getElementById('accounts')
    const targetElement = document.getElementById('target')
    const logsElement = document.getElementById('logs')
    const logTitle = document.getElementById('log-title')
    const logMeta = document.getElementById('log-meta')
    const notice = document.getElementById('notice')
    const accountCount = document.getElementById('account-count')
    const accountFilter = document.getElementById('account-filter')
    const statusFilter = document.getElementById('status-filter')
    const sortAccounts = document.getElementById('sort-accounts')
    const logSearch = document.getElementById('log-search')
    const logFilter = document.getElementById('log-filter')
    const autoScrollButton = document.getElementById('toggle-scroll')
    const streamStatus = document.getElementById('stream-status')
    const commandHistory = document.getElementById('command-history')
    const commandInput = document.getElementById('command')
    const delayInput = document.getElementById('delay')
    const pinnedOnlyButton = document.getElementById('pinned-only')
    const dispatchButton = document.getElementById('toggle-dispatch')
    const logCount = document.getElementById('log-count')
    const alertCount = document.getElementById('alert-count')
    const fleetStartCount = document.getElementById('fleet-start-count')
    const fleetCapacity = document.getElementById('fleet-capacity')
    const autoCommandsInput = document.getElementById('auto-commands')
    const selectedStartButton = document.getElementById('selected-start')
    const selectedStopButton = document.getElementById('selected-stop')
    const selectedPinButton = document.getElementById('selected-pin')
    const selectedStatus = document.getElementById('selected-status')
    let state = { accounts: [] }
    let selected = null
    let autoCommandsDirty = false
    let filter = ''
    let autoScroll = true
    let statusScope = 'all'
    let sortMode = 'pinned'
    let logQuery = ''
    let logScope = 'all'
    let pinsOnly = false
    let wrapLogs = true
    let received = false
    let pinned = new Set()
    let history = []
    try { pinned = new Set(JSON.parse(localStorage.getItem('fleet-pins') || '[]')) } catch {}
    try { history = JSON.parse(localStorage.getItem('fleet-command-history') || '[]') } catch {}
    const escapeHtml = (value) => String(value).replace(/[&<>'"]/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' })[char])
    const statusClass = (status) => String(status).replace(/\\s+/g, '-')
    const setText = (id, value) => { document.getElementById(id).textContent = value }
    const setNotice = (message, tone) => {
      notice.textContent = message || ''
      notice.className = 'notice' + (message && tone ? ' is-' + tone : '')
    }
    const shortTime = (value) => {
      const parsed = new Date(value)
      return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toLocaleTimeString([], { hour12: false })
    }
    const logTone = (message) => {
      const text = message.toLowerCase()
      if (text.includes('kicked') || text.includes('error') || text.includes('denied')) return 'alert'
      if (text.includes('rate limit') || text.includes('verification')) return 'warn'
      if (text.startsWith('chat:')) return 'chat'
      return ''
    }
    const placeholder = (title, detail, tone) => '<p class="placeholder' + (tone ? ' is-' + tone : '') + '"><strong>' + escapeHtml(title) + '</strong><span>' + escapeHtml(detail) + '</span></p>'
    const savePreferences = () => {
      try {
        localStorage.setItem('fleet-pins', JSON.stringify([...pinned]))
        localStorage.setItem('fleet-command-history', JSON.stringify(history))
      } catch {}
    }
    const formatAutoCommands = (commands) => (commands || []).map(item => String(item.delay || 0) + ' ' + item.command).join(String.fromCharCode(10))
    const parseAutoCommands = (value) => value.split(String.fromCharCode(10)).map(line => line.trim()).filter(Boolean).map((line) => {
      const match = line.match(/^(?:(\\d+)\\s+)?(\\/.+)$/)
      if (!match) throw new Error('Use one command per line: optional delay in ms, then /command.')
      return { delay: Number(match[1] || 0), command: match[2].trim() }
    })
    const logMatches = (entry) => {
      const message = entry.message.toLowerCase()
      if (logQuery && !message.includes(logQuery)) return false
      if (logScope === 'chat') return message.startsWith('chat:')
      if (logScope === 'actions') return message.includes('action:')
      if (logScope === 'alerts') return message.includes('kicked') || message.includes('error') || message.includes('denied') || message.includes('rate limit')
      return true
    }
    const accountMatches = (account) => {
      const textMatch = account.username.toLowerCase().includes(filter) || account.status.toLowerCase().includes(filter) || account.lastActivity.toLowerCase().includes(filter)
      if (!textMatch) return false
      if (pinsOnly && !pinned.has(account.username)) return false
      if (statusScope === 'active') return account.status === 'online' || account.status === 'awaiting welcome' || account.status === 'connecting'
      if (statusScope === 'verification') return account.verificationPending || account.waitingForVerificationSlot
      if (statusScope === 'queued') return account.status === 'queued' || account.status === 'scheduled'
      if (statusScope === 'issues') return account.status === 'kicked' || account.status === 'reconnecting' || account.status === 'waiting verification slot'
      return true
    }
    const accountSorter = (left, right) => {
      if (sortMode === 'pinned' && pinned.has(left.username) !== pinned.has(right.username)) return pinned.has(left.username) ? -1 : 1
      if (sortMode === 'status') return left.status.localeCompare(right.status) || left.username.localeCompare(right.username)
      if (sortMode === 'activity') return right.lastActivity.localeCompare(left.lastActivity) || left.username.localeCompare(right.username)
      return left.username.localeCompare(right.username, undefined, { numeric: true })
    }
    function moveSelectedAccount (offset) {
      const accounts = (state.accounts || []).filter(accountMatches).sort(accountSorter)
      if (!accounts.length) return
      const current = accounts.findIndex(account => account.username === selected)
      selected = accounts[(current + offset + accounts.length) % accounts.length].username
      render()
    }
    function renderHistory () {
      commandHistory.innerHTML = '<option value="">Recent safe commands</option>' + history.map(command => '<option value="' + escapeHtml(command) + '">' + escapeHtml(command) + '</option>').join('')
    }
    function rememberCommand (command) {
      if (!command || command.toLowerCase().startsWith('/register ')) return
      history = [command, ...history.filter(item => item !== command)].slice(0, 10)
      savePreferences()
      renderHistory()
    }
    function accountMarkup (account) {
      const flag = account.verificationPending ? 'verification open' : account.waitingForVerificationSlot ? 'waiting for slot' : ''
      const pin = pinned.has(account.username) ? '★ ' : ''
      return '<button type="button" class="account ' + (account.username === selected ? 'selected' : '') + '"' +
        ' data-account="' + escapeHtml(account.username) + '" aria-pressed="' + String(account.username === selected) + '">' +
        '<span class="account-name">' + pin + escapeHtml(account.username) + '</span>' +
        '<span class="badge ' + statusClass(account.status) + '">' + escapeHtml(account.status) + '</span>' +
        '<span class="account-activity">' + escapeHtml(account.lastActivity) + '</span>' +
        '<span class="account-flag' + (flag ? ' attention' : '') + '">' + flag + '</span>' +
        '</button>'
    }
    function render () {
      const fleet = state.fleet || {}
      const accounts = state.accounts || []
      const online = accounts.filter(account => account.status === 'online').length
      const connecting = accounts.filter(account => account.status === 'connecting' || account.status === 'awaiting welcome' || account.status === 'reconnecting').length
      setText('fleet-target', (fleet.host || 'localhost') + ':' + (fleet.port || '—') + ' · ' + (fleet.version || '—'))
      setText('metric-total', accounts.length)
      setText('metric-online', online)
      setText('metric-connecting', connecting)
      setText('metric-verification', (fleet.activeVerifications || 0) + ' / ' + (fleet.verificationCapacity || 0))
      setText('metric-queued', fleet.queuedConnections || 0)
      setText('metric-cpu', (Number(fleet.cpuPercent) || 0).toFixed(1) + '%')
      setText('metric-ram', (Number(fleet.ramMb) || 0).toFixed(1) + ' MB')
      if (!autoCommandsDirty && document.activeElement !== autoCommandsInput) autoCommandsInput.value = formatAutoCommands(fleet.autoCommands)
      dispatchButton.textContent = fleet.connectionDispatchPaused ? 'Resume new connections' : 'Pause new connections'
      dispatchButton.setAttribute('aria-pressed', String(Boolean(fleet.connectionDispatchPaused)))
      accountCount.textContent = accounts.length + ' tracked'
      const availableAccounts = accounts.filter(account => account.status === 'stopped').length
      fleetStartCount.max = fleet.accountCapacity || accounts.length
      fleetCapacity.textContent = availableAccounts + ' available · batches of ' + (fleet.batchSize || 0) + ' / ' + Math.round((fleet.batchAccountDelay || 0) / 1000) + 's · ' + (fleet.verificationCapacity || 0) + ' verification slots'
      const previousTarget = targetElement.value
      targetElement.innerHTML = '<option value="all">All connected bots</option>' + accounts.map(account => '<option value="' + escapeHtml(account.username) + '">' + escapeHtml(account.username) + '</option>').join('')
      targetElement.value = accounts.some(account => account.username === previousTarget) ? previousTarget : 'all'
      if (!selected && accounts.length) selected = accounts[0].username
      if (!accounts.some(account => account.username === selected)) selected = accounts[0] ? accounts[0].username : null
      const filteredAccounts = accounts.filter(accountMatches).sort(accountSorter)
      accountsElement.innerHTML = filteredAccounts.map(accountMarkup).join('') ||
        (!received
          ? placeholder('Loading accounts', 'Waiting for the first worker update.')
          : !accounts.length
            ? placeholder('No accounts yet', 'The worker has not registered any accounts.')
            : placeholder('No matching accounts', 'Adjust the search, status, or pinned filters.'))
      const account = accounts.find(item => item.username === selected)
      const wasAtBottom = logsElement.scrollHeight - logsElement.scrollTop - logsElement.clientHeight < 32
      logTitle.textContent = account ? account.username : 'Selected account'
      logMeta.textContent = account ? account.status + ' · ' + account.attempts + ' reconnect attempt(s)' : 'Select an account'
      selectedStatus.textContent = account ? account.status : 'No account'
      selectedStatus.className = 'badge ' + (account ? statusClass(account.status) : '')
      selectedStartButton.disabled = !account || account.status !== 'stopped'
      selectedStopButton.disabled = !account || account.status === 'stopped'
      selectedPinButton.disabled = !account
      selectedPinButton.textContent = account && pinned.has(account.username) ? 'Unpin' : 'Pin'
      selectedPinButton.classList.toggle('active', Boolean(account && pinned.has(account.username)))
      selectedPinButton.setAttribute('aria-pressed', String(Boolean(account && pinned.has(account.username))))
      document.getElementById('release-verification').disabled = !account
      const visibleLogs = account ? account.logs.filter(logMatches) : []
      const alerts = visibleLogs.filter(entry => logMatches({ message: entry.message }) && (entry.message.toLowerCase().includes('kicked') || entry.message.toLowerCase().includes('error') || entry.message.toLowerCase().includes('denied'))).length
      logsElement.innerHTML = visibleLogs.length
        ? visibleLogs.map(entry => '<span class="log-line ' + logTone(entry.message) + '"><time class="log-time" datetime="' + escapeHtml(entry.time) + '" title="' + escapeHtml(entry.time) + '">' + escapeHtml(shortTime(entry.time)) + '</time><span class="log-message">' + escapeHtml(entry.message) + '</span></span>').join('')
        : '<span class="log-empty">' + (!account ? 'Select an account to view its activity.' : 'No log entries match the current filters.') + '</span>'
      logCount.textContent = visibleLogs.length + ' visible event' + (visibleLogs.length === 1 ? '' : 's')
      alertCount.textContent = alerts + ' alert' + (alerts === 1 ? '' : 's')
      if (autoScroll && (wasAtBottom || logsElement.scrollTop === 0)) logsElement.scrollTop = logsElement.scrollHeight
    }
    accountsElement.addEventListener('click', event => {
      const button = event.target.closest('[data-account]')
      if (button) { selected = button.dataset.account; render() }
    })
    autoCommandsInput.addEventListener('input', () => { autoCommandsDirty = true })
    document.getElementById('auto-command-form').addEventListener('submit', async event => {
      event.preventDefault()
      try {
        const commands = parseAutoCommands(autoCommandsInput.value)
        if (await request('/auto-commands', { commands })) {
          autoCommandsDirty = false
          state.fleet = state.fleet || {}
          state.fleet.autoCommands = commands
        }
      } catch (err) {
        setNotice(err.message, 'error')
      }
    })
    accountFilter.addEventListener('input', event => { filter = event.target.value.trim().toLowerCase(); render() })
    statusFilter.addEventListener('change', event => { statusScope = event.target.value; render() })
    sortAccounts.addEventListener('change', event => { sortMode = event.target.value; render() })
    pinnedOnlyButton.addEventListener('click', () => {
      pinsOnly = !pinsOnly
      pinnedOnlyButton.textContent = pinsOnly ? 'Showing pinned' : 'Pinned only'
      pinnedOnlyButton.setAttribute('aria-pressed', String(pinsOnly))
      render()
    })
    document.getElementById('reset-filters').addEventListener('click', () => {
      filter = ''
      statusScope = 'all'
      sortMode = 'pinned'
      pinsOnly = false
      accountFilter.value = ''
      statusFilter.value = 'all'
      sortAccounts.value = 'pinned'
      pinnedOnlyButton.textContent = 'Pinned only'
      pinnedOnlyButton.setAttribute('aria-pressed', 'false')
      render()
    })
    logSearch.addEventListener('input', event => { logQuery = event.target.value.trim().toLowerCase(); render() })
    logFilter.addEventListener('change', event => { logScope = event.target.value; render() })
    autoScrollButton.addEventListener('click', () => {
      autoScroll = !autoScroll
      autoScrollButton.textContent = 'Auto-scroll: ' + (autoScroll ? 'on' : 'off')
      autoScrollButton.setAttribute('aria-pressed', String(autoScroll))
    })
    commandHistory.addEventListener('change', event => {
      if (event.target.value) {
        commandInput.value = event.target.value
        commandInput.dispatchEvent(new Event('input'))
      }
    })
    commandInput.addEventListener('input', () => {
      document.getElementById('command-counter').textContent = commandInput.value.length + ' characters · Ctrl/Cmd + K focuses selected-client command'
    })
    document.querySelectorAll('[data-delay-preset]').forEach(button => {
      button.addEventListener('click', () => { delayInput.value = button.dataset.delayPreset })
    })
    dispatchButton.addEventListener('click', async () => {
      await request('/toggle-connection-dispatch', {})
    })
    document.getElementById('claim-playtime-rewards').addEventListener('click', async () => {
      await request('/claim-playtime-rewards', { target: targetElement.value, delay: Number(document.getElementById('claim-delay').value) })
    })
    document.getElementById('jump-latest').addEventListener('click', () => { logsElement.scrollTop = logsElement.scrollHeight })
    document.getElementById('toggle-wrap').addEventListener('click', event => {
      wrapLogs = !wrapLogs
      logsElement.classList.toggle('nowrap', !wrapLogs)
      event.currentTarget.textContent = 'Wrap: ' + (wrapLogs ? 'on' : 'off')
      event.currentTarget.setAttribute('aria-pressed', String(wrapLogs))
    })
    document.getElementById('previous-account').addEventListener('click', () => moveSelectedAccount(-1))
    document.getElementById('next-account').addEventListener('click', () => moveSelectedAccount(1))
    selectedStartButton.addEventListener('click', async () => {
      if (selected) await request('/account-start', { target: selected })
    })
    selectedStopButton.addEventListener('click', async () => {
      if (selected) await request('/account-stop', { target: selected })
    })
    selectedPinButton.addEventListener('click', () => {
      if (!selected) return
      if (pinned.has(selected)) pinned.delete(selected)
      else pinned.add(selected)
      savePreferences()
      render()
    })
    const selectedLogText = () => {
      const account = (state.accounts || []).find(item => item.username === selected)
      return account ? account.logs.filter(logMatches).map(entry => '[' + entry.time + '] ' + entry.message).join('\\n') : ''
    }
    async function request (path, payload) {
      try {
        const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
        const result = await response.json()
        setNotice(result.message, response.ok ? 'ok' : 'error')
        return response.ok
      } catch {
        setNotice('Dashboard request failed. Check the local service and try again.', 'error')
        return false
      }
    }
    document.getElementById('fleet-start').addEventListener('click', async () => {
      await request('/fleet-start', { count: Number(fleetStartCount.value) })
    })
    document.getElementById('fleet-start-remaining').addEventListener('click', async () => {
      await request('/fleet-start', { count: (state.accounts || []).filter(account => account.status === 'stopped').length })
    })
    document.getElementById('fleet-stop-all').addEventListener('click', async () => {
      await request('/fleet-stop', {})
    })
    document.getElementById('reload-dashboard').addEventListener('click', () => {
      window.location.reload()
    })
    document.getElementById('copy-log').addEventListener('click', async () => {
      try {
        await navigator.clipboard.writeText(selectedLogText())
        setNotice('Visible log entries copied.', 'ok')
      } catch {
        setNotice('Copy was blocked by the browser.', 'error')
      }
    })
    document.getElementById('download-log').addEventListener('click', () => {
      const content = selectedLogText()
      const link = document.createElement('a')
      link.href = URL.createObjectURL(new Blob([content], { type: 'text/plain' }))
      link.download = (selected || 'fleet') + '-activity.log'
      link.click()
      URL.revokeObjectURL(link.href)
      setNotice('Visible log entries exported.', 'ok')
    })
    document.getElementById('export-fleet').addEventListener('click', () => {
      const content = JSON.stringify({ exportedAt: new Date().toISOString(), fleet: state.fleet, accounts: state.accounts }, null, 2)
      const link = document.createElement('a')
      link.href = URL.createObjectURL(new Blob([content], { type: 'application/json' }))
      link.download = 'fleet-snapshot.json'
      link.click()
      URL.revokeObjectURL(link.href)
      setNotice('Fleet snapshot exported.', 'ok')
    })
    document.getElementById('command-form').addEventListener('submit', async event => {
      event.preventDefault()
      const command = document.getElementById('command').value
      if (await request('/command', { target: targetElement.value, command, delay: Number(document.getElementById('delay').value) })) rememberCommand(command)
    })
    document.getElementById('release-verification').addEventListener('click', async () => {
      if (!selected) return
      await request('/release-verification', { target: selected })
    })
    document.getElementById('client-command-form').addEventListener('submit', async event => {
      event.preventDefault()
      if (!selected) { setNotice('Select an account first.', 'error'); return }
      const input = document.getElementById('client-command')
      if (await request('/command', { target: selected, command: input.value, delay: 0 })) {
        rememberCommand(input.value)
        input.value = ''
      }
    })
    document.addEventListener('keydown', event => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        document.getElementById('client-command').focus()
      }
      if (event.altKey && event.key === 'ArrowUp') {
        event.preventDefault()
        moveSelectedAccount(-1)
      }
      if (event.altKey && event.key === 'ArrowDown') {
        event.preventDefault()
        moveSelectedAccount(1)
      }
    })
    renderHistory()
    const events = new EventSource('/events')
    events.onopen = () => {
      streamStatus.textContent = 'Live event stream connected'
      streamStatus.classList.add('live')
      streamStatus.classList.remove('down')
    }
    events.onmessage = event => {
      state = JSON.parse(event.data)
      received = true
      document.getElementById('last-updated').textContent = 'Updated ' + new Date().toLocaleTimeString()
      render()
    }
    events.onerror = () => {
      streamStatus.textContent = 'Reconnecting live stream'
      streamStatus.classList.remove('live')
      streamStatus.classList.add('down')
      setNotice('Dashboard connection lost; retrying…', 'error')
    }
  </script>
</body>
</html>`

function sendDashboardResponse (res, statusCode, body) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' })
  res.end(JSON.stringify(body))
}

function handleDashboardRequest (req, res) {
  const url = new URL(req.url, `http://${CONFIG.dashboardHost}:${CONFIG.dashboardPort}`)
  if (req.method === 'GET' && url.pathname === '/') {
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
      'X-Content-Type-Options': 'nosniff',
      'Content-Security-Policy': "default-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'",
    })
    res.end(DASHBOARD_HTML)
    return
  }
  if (req.method === 'GET' && url.pathname === '/events') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive', 'X-Content-Type-Options': 'nosniff' })
    dashboardClients.add(res)
    res.write(`data: ${JSON.stringify(dashboardState())}\n\n`)
    req.on('close', () => dashboardClients.delete(res))
    return
  }
  if (req.method === 'POST' && url.pathname === '/command') {
    let body = ''
    req.on('data', (chunk) => {
      body += chunk
      if (body.length > 10_000) req.destroy()
    })
    req.on('end', () => {
      try {
        const { target, command, delay } = JSON.parse(body)
        const message = sendBotCommand(target, command, delay)
        sendDashboardResponse(res, 200, { message })
      } catch (err) {
        sendDashboardResponse(res, 400, { message: err.message })
      }
    })
    return
  }
  if (req.method === 'POST' && url.pathname === '/auto-commands') {
    let body = ''
    req.on('data', (chunk) => {
      body += chunk
      if (body.length > 20_000) req.destroy()
    })
    req.on('end', () => {
      try {
        const { commands } = JSON.parse(body)
        const saved = updateAutoCommands(commands)
        publishDashboard()
        sendDashboardResponse(res, 200, { message: 'Saved ' + saved.length + ' auto command(s).' })
      } catch (err) {
        sendDashboardResponse(res, 400, { message: err.message })
      }
    })
    return
  }
  if (req.method === 'POST' && url.pathname === '/claim-playtime-rewards') {
    let body = ''
    req.on('data', (chunk) => {
      body += chunk
      if (body.length > 10_000) req.destroy()
    })
    req.on('end', () => {
      try {
        const { target, delay } = JSON.parse(body)
        sendDashboardResponse(res, 200, { message: claimPlaytimeRewardsForTargets(target, delay) })
      } catch (err) {
        sendDashboardResponse(res, 400, { message: err.message })
      }
    })
    return
  }
  if (req.method === 'POST' && url.pathname === '/toggle-connection-dispatch') {
    connectionDispatchPaused = !connectionDispatchPaused
    if (!connectionDispatchPaused) fillInitialConnectionSlots()
    publishDashboard()
    sendDashboardResponse(res, 200, { message: connectionDispatchPaused ? 'New account connections paused.' : 'New account connections resumed.' })
    return
  }
  if (req.method === 'POST' && url.pathname === '/fleet-start') {
    let body = ''
    req.on('data', (chunk) => { body += chunk })
    req.on('end', () => {
      try {
        const { count } = JSON.parse(body)
        const started = startAccounts(count)
        sendDashboardResponse(res, 200, { message: 'Scheduled ' + started + ' account(s) using the configured batch rate.' })
      } catch (err) {
        sendDashboardResponse(res, 400, { message: err.message })
      }
    })
    return
  }
  if (req.method === 'POST' && url.pathname === '/fleet-stop') {
    const stopped = stopAllAccounts()
    sendDashboardResponse(res, 200, { message: 'Stopped ' + stopped + ' account(s).' })
    return
  }
  if (req.method === 'POST' && (url.pathname === '/account-start' || url.pathname === '/account-stop')) {
    let body = ''
    req.on('data', (chunk) => { body += chunk })
    req.on('end', () => {
      try {
        const { target } = JSON.parse(body)
        const changed = url.pathname === '/account-start' ? enqueueAccount(target) : stopAccount(target)
        if (!changed) throw new Error('Account cannot be changed in its current state.')
        sendDashboardResponse(res, 200, { message: (url.pathname === '/account-start' ? 'Scheduled ' : 'Stopped ') + target + '.' })
      } catch (err) {
        sendDashboardResponse(res, 400, { message: err.message })
      }
    })
    return
  }
  if (req.method === 'POST' && url.pathname === '/release-verification') {
    let body = ''
    req.on('data', (chunk) => { body += chunk })
    req.on('end', () => {
      try {
        const { target } = JSON.parse(body)
        const state = bots.get(target)
        if (!state || !releaseVerificationSlot(target, state)) throw new Error('No active verification slot for "' + target + '".')
        sendDashboardResponse(res, 200, { message: 'Released ' + target + "'s verification slot." })
      } catch (err) {
        sendDashboardResponse(res, 400, { message: err.message })
      }
    })
    return
  }
  res.writeHead(404)
  res.end()
}

function startDashboard () {
  dashboardServer = http.createServer(handleDashboardRequest)
  dashboardServer.on('error', (err) => console.error(`Dashboard error: ${err.message}`))
  dashboardServer.listen(CONFIG.dashboardPort, CONFIG.dashboardHost, () => {
    const url = `http://${CONFIG.dashboardHost}:${CONFIG.dashboardPort}`
    console.log(`Dashboard: ${url}`)
    if (CONFIG.openDashboard && process.platform === 'win32') {
      const browser = spawn('cmd.exe', ['/d', '/c', 'start', '', url], { detached: true, stdio: 'ignore', windowsHide: true })
      browser.on('error', (err) => console.error(`Could not open dashboard: ${err.message}`))
      browser.unref()
    }
  })
}

// Track timers per bot so a reconnect never leaves an orphaned interval behind.
function track (state, timer) {
  state.timers.add(timer)
  return timer
}

function clearTimers (state) {
  for (const t of state.timers) clearTimeout(t), clearInterval(t)
  state.timers.clear()
}

// ---------------------------------------------------------------------------
// Bot lifecycle
// ---------------------------------------------------------------------------
function createBot (username) {
  const state = bots.get(username) || newBotState(username)
  bots.set(username, state)
  state.manuallyStopped = false
  state.hasReceivedWelcome = false
  state.status = 'connecting'
  publishDashboard()

  const bot = mineflayer.createBot({
    host: CONFIG.host,
    port: CONFIG.port,
    username,
    auth: CONFIG.auth,
    version: CONFIG.version,

    // --- memory knobs -----------------------------------------------------
    // These are the difference between ~30MB and ~150MB per bot at scale.
    // 'tiny' keeps the chunk cache small; physicsEnabled: false skips the
    // per-tick simulation entirely. Turn physics back on if you ever want the
    // bots to actually walk, pathfind, or fall correctly.
    viewDistance: 'tiny',
    physicsEnabled: false,

    // Bail on a dead connection instead of hanging a socket forever.
    checkTimeoutInterval: 30000,
    // Skip loading the (large) plugin set you aren't using. Comment out to get
    // the full default plugin list back.
    // plugins: { ... }
  })

  state.bot = bot

  // --- events ---------------------------------------------------------------

  bot.once('spawn', () => {
    state.status = 'awaiting welcome'
    state.attempts = 0 // a successful spawn resets the backoff
    log(username, 'spawned')
    onSpawn(username, bot, state)
  })

  bot.on('kicked', (reason) => {
    state.status = 'kicked'
    // `reason` is a chat component or a JSON string depending on version.
    const formattedReason = stringifyReason(reason)
    log(username, `kicked: ${formattedReason}`)
    if (formattedReason.includes("We're receiving too many connections")) {
      state.retryDelayOverride = 120000
      log(username, 'server rate limit detected; next retry will wait 2 minutes')
    }
    openVerificationUrl(username, state, formattedReason)
  })

  // MUST be handled. An unhandled 'error' on an EventEmitter throws and takes
  // the whole process — and with it all 99 other bots — down with it.
  bot.on('error', (err) => {
    log(username, `error: ${err.code || err.message}`)
  })

  // 'end' fires for every disconnect: kick, error, network drop, quit.
  bot.on('end', (reason) => {
    clearTimers(state)
    state.claimingPlaytimeRewards = false
    const releasedStartupSlot = startupInFlight.delete(username)
    state.bot = null
    state.status = shuttingDown || state.manuallyStopped
      ? 'stopped'
      : state.waitingForVerificationSlot ? 'waiting verification slot' : 'reconnecting'
    publishDashboard()
    if (shuttingDown || state.manuallyStopped) {
      if (releasedStartupSlot) fillInitialConnectionSlots()
      return
    }
    if (state.waitingForVerificationSlot) {
      log(username, 'reconnect paused until a verification browser slot is available')
      if (releasedStartupSlot) fillInitialConnectionSlots()
      return
    }
    scheduleReconnect(username, state, reason)
    if (releasedStartupSlot) fillInitialConnectionSlots()
  })

  bot.on('messagestr', (msg) => {
    logChat(username, state, msg)
    if (!state.hasReceivedWelcome && msg.includes('Welcome to PikaNetwork')) {
      state.hasReceivedWelcome = true
      state.status = 'online'
      log(username, 'received PikaNetwork welcome')
      releaseVerificationSlot(username, state)
      if (startupInFlight.delete(username)) fillInitialConnectionSlots()
      scheduleAutoCommands(username, state)
    }
  })

  return bot
}

// Everything you actually want the bots to DO goes here.
function onSpawn (username, bot, state) {
  // e.g. bot.chat('/register hunter2 hunter2')
  // e.g. track(state, setTimeout(() => bot.chat('/spawn'), 3000))
}

function scheduleAutoCommands (username, state) {
  if (CONFIG.autoCommands.length === 0) return
  log(username, `action: scheduling ${CONFIG.autoCommands.length} post-login command(s)`)
  for (const { command, delay } of CONFIG.autoCommands) {
    const timer = setTimeout(() => {
      state.timers.delete(timer)
      if (!shuttingDown && !state.manuallyStopped) sendCommandIfConnected(username, state, command)
    }, Math.floor(delay))
    track(state, timer)
  }
}

function scheduleReconnect (username, state, reason) {
  const delay = state.retryDelayOverride ?? Math.min(
    CONFIG.reconnectBase * Math.pow(CONFIG.reconnectFactor, state.attempts),
    CONFIG.reconnectMax
  )
  state.retryDelayOverride = null
  state.attempts += 1
  log(username, `disconnected (${stringifyReason(reason)}) — joining the connection queue in ${Math.round(delay / 1000)}s`)
  const timer = setTimeout(() => {
    state.timers.delete(timer)
    if (!shuttingDown && !state.manuallyStopped) {
      enqueueReconnect(username, state)
    }
  }, delay)
  track(state, timer)
}

function stringifyReason (reason) {
  if (!reason) return 'unknown'
  if (typeof reason === 'string') return reason
  if (typeof reason.toString === 'function' && reason.toString !== Object.prototype.toString) return reason.toString()
  return JSON.stringify(reason)
}

function isGreyGlassPane (item) {
  if (!item) return false
  return item.name === 'gray_stained_glass_pane' ||
    item.name === 'light_gray_stained_glass_pane' ||
    (item.name === 'stained_glass_pane' && [7, 8].includes(item.metadata))
}

function isPlaytimePlaceholder (item) {
  return !item || item.name === 'air' || item.name === 'compass' || item.name.endsWith('_stained_glass_pane') || item.name === 'stained_glass_pane'
}

function isPlaytimeRewardsWindow (window) {
  return Boolean(window && /playtime rewards/i.test(stringifyReason(window.title)))
}

async function clickPlaytimeRewards (username, state, bot, window) {
  const firstGreyPane = window.slots.findIndex(isGreyGlassPane)
  const menuEnd = Number.isInteger(window.inventoryStart) ? window.inventoryStart : window.slots.length
  if (firstGreyPane === -1 || firstGreyPane >= menuEnd - 1) {
    log(username, 'playtime rewards skipped: grey glass separator was not found')
    return
  }

  const rewardSlots = []
  for (let slot = firstGreyPane + 1; slot < menuEnd; slot += 1) {
    const item = window.slots[slot]
    if (!isPlaytimePlaceholder(item)) rewardSlots.push(slot)
  }

  if (rewardSlots.length === 0) {
    log(username, 'playtime rewards: no eligible reward slots found')
    return
  }

  log(username, `playtime rewards: claiming ${rewardSlots.length} eligible slot(s)`)
  for (const slot of rewardSlots) {
    if (shuttingDown || state.manuallyStopped || state.bot !== bot || bot.currentWindow !== window) {
      log(username, 'playtime rewards stopped: the reward window is no longer open')
      return
    }
    try {
      await bot.clickWindow(slot, 0, 0)
      log(username, `action: claimed playtime reward at menu slot ${slot}`)
    } catch (err) {
      log(username, `playtime reward slot ${slot} failed: ${err.message}`)
    }
  }
}

function claimPlaytimeRewardsForAccount (username, state) {
  const bot = state.bot
  const canChat = bot && bot._client && typeof bot._client.chat === 'function'
  if (!canChat) return false
  if (state.claimingPlaytimeRewards) return false

  state.claimingPlaytimeRewards = true
  let timeout
  const cleanup = () => {
    if (timeout) {
      clearTimeout(timeout)
      state.timers.delete(timeout)
    }
    bot.removeListener('windowOpen', onWindowOpen)
  }
  const startClaiming = (window) => {
    cleanup()
    clickPlaytimeRewards(username, state, bot, window)
      .catch((err) => log(username, `playtime rewards failed: ${err.message}`))
      .finally(() => { state.claimingPlaytimeRewards = false })
  }
  const onWindowOpen = (window) => {
    if (isPlaytimeRewardsWindow(window)) startClaiming(window)
  }

  if (isPlaytimeRewardsWindow(bot.currentWindow)) {
    startClaiming(bot.currentWindow)
    return true
  }

  bot.on('windowOpen', onWindowOpen)
  timeout = track(state, setTimeout(() => {
    cleanup()
    state.claimingPlaytimeRewards = false
    log(username, 'playtime rewards timed out waiting for the menu')
  }, 10000))
  try {
    bot.chat('/ptr')
    log(username, 'action: sent command /ptr')
    return true
  } catch (err) {
    cleanup()
    state.claimingPlaytimeRewards = false
    log(username, `playtime rewards could not open: ${err.message}`)
    return false
  }
}

function claimPlaytimeRewardsForTargets (target, delay = 0) {
  if (typeof target !== 'string') throw new Error('Choose an account or all connected accounts.')
  const spacing = Number(delay)
  if (!Number.isSafeInteger(spacing) || spacing < 0) throw new Error('Reward-claim spacing must be a non-negative whole number of milliseconds.')
  const targets = target === 'all'
    ? [...bots.entries()].filter(([, state]) => state.bot)
    : [[target, bots.get(target)]]
  const readyTargets = targets.filter(([, state]) => state && state.bot && !state.claimingPlaytimeRewards)
  if (readyTargets.length === 0) throw new Error('No connected account is ready to claim playtime rewards.')

  if (spacing === 0) {
    const started = readyTargets.filter(([username, state]) => claimPlaytimeRewardsForAccount(username, state)).length
    if (started === 0) throw new Error('No connected account is ready to claim playtime rewards.')
    return `Opening /ptr for ${started} account(s). Every eligible reward slot will be claimed immediately as the menu responds.`
  }

  for (const [index, [username, state]] of readyTargets.entries()) {
    const claim = () => {
      commandTimers.delete(timer)
      if (!claimPlaytimeRewardsForAccount(username, state)) log(username, 'playtime rewards skipped: client is no longer ready')
    }
    if (index === 0) {
      claimPlaytimeRewardsForAccount(username, state)
      continue
    }
    const timer = setTimeout(claim, spacing * index)
    commandTimers.add(timer)
  }
  return `Opening /ptr for ${readyTargets.length} account(s), ${spacing}ms apart. Every eligible reward slot will be claimed immediately as each menu responds.`
}

function commandForLog (command) {
  return command.startsWith('/register ') ? '/register [redacted]' : command
}

function sendCommandIfConnected (username, state, command) {
  if (shuttingDown) return
  const bot = state.bot
  const canChat = bot && bot._client && typeof bot._client.chat === 'function'
  if (!canChat) {
    log(username, `action skipped: client is not connected for ${commandForLog(command)}`)
    return
  }

  try {
    bot.chat(command)
    log(username, `action: sent command ${commandForLog(command)}`)
  } catch (err) {
    log(username, `action failed for ${commandForLog(command)}: ${err.message}`)
  }
}

function sendBotCommand (target, command, delay = 0) {
  if (typeof target !== 'string' || typeof command !== 'string') throw new Error('Target and command are required.')
  if (!command.startsWith('/')) throw new Error('Minecraft commands must start with /.')
  if (!Number.isSafeInteger(delay) || delay < 0) throw new Error('Delay must be a non-negative whole number of milliseconds.')

  const targets = target === 'all'
    ? [...bots.entries()].filter(([, state]) => state.bot)
    : [[target, bots.get(target)]]
  const connectedTargets = targets.filter(([, state]) => state && state.bot)
  if (connectedTargets.length === 0) throw new Error(`No connected bot found for "${target}".`)

  for (const [index, [username, state]] of connectedTargets.entries()) {
    const send = () => {
      commandTimers.delete(timer)
      sendCommandIfConnected(username, state, command)
    }
    const timer = setTimeout(send, delay * index)
    commandTimers.add(timer)
  }
  return `Queued ${commandForLog(command)} for ${connectedTargets.length} bot(s)${delay ? `, ${delay}ms apart` : ''}.`
}

function printCommandHelp () {
  console.log('Commands: <bot name> <minecraft command>, all [--delay <ms>] <minecraft command>, status, help, quit')
  console.log('Example: gob_1 /register password password')
  console.log('Example: all --delay 1000 /lobby')
}

function runBotCommand (line) {
  const input = line.trim()
  if (!input) return
  if (input === 'help') return printCommandHelp()
  if (input === 'status') {
    for (const [username, state] of bots) console.log(`${username}: ${state.bot ? 'connected' : 'disconnected'}`)
    return
  }
  if (input === 'quit') return shutdown()

  const firstSpace = input.indexOf(' ')
  if (firstSpace === -1) {
    console.log('Use: <bot name> <minecraft command> (or type help)')
    return
  }

  const target = input.slice(0, firstSpace)
  let command = input.slice(firstSpace + 1).trim()
  let delay = 0
  if (target === 'all' && command.startsWith('--delay')) {
    const delayMatch = command.match(/^--delay\s+(\d+)\s+(.+)$/)
    if (!delayMatch) {
      console.log('Use: all --delay <milliseconds> <minecraft command>')
      return
    }
    delay = Number(delayMatch[1])
    command = delayMatch[2].trim()
  }
  try {
    console.log(sendBotCommand(target, command, delay))
  } catch (err) {
    console.log(err.message)
  }
}

function startCommandConsole () {
  if (!process.stdin.isTTY) return
  commandConsole = readline.createInterface({ input: process.stdin, output: process.stdout })
  commandConsole.on('line', runBotCommand)
  commandConsole.on('SIGINT', shutdown)
  printCommandHelp()
}

// ---------------------------------------------------------------------------
// Startup / shutdown
// ---------------------------------------------------------------------------
function reserveBatchStart () {
  const now = Date.now()
  if (nextScheduledStartAt < now) {
    nextScheduledStartAt = now
    batchPosition = 0
  }

  const startAt = nextScheduledStartAt
  batchPosition += 1
  if (batchPosition >= CONFIG.batchSize) {
    nextScheduledStartAt += CONFIG.batchCooldown
    batchPosition = 0
  } else {
    nextScheduledStartAt += CONFIG.batchAccountDelay
  }
  return startAt
}

function scheduleBatchGate (delay) {
  if (batchGateTimer || shuttingDown) return
  batchGateTimer = setTimeout(() => {
    startupTimers.delete(batchGateTimer)
    batchGateTimer = null
    fillInitialConnectionSlots()
  }, Math.max(0, delay))
  startupTimers.add(batchGateTimer)
}

function resetBatchSchedule () {
  if (batchGateTimer) {
    clearTimeout(batchGateTimer)
    startupTimers.delete(batchGateTimer)
    batchGateTimer = null
  }
  nextScheduledStartAt = 0
  batchPosition = 0
}

function enqueueAccount (username) {
  const state = bots.get(username)
  if (!state || state.status !== 'stopped' || state.bot || pendingUsernames.includes(username) || startupInFlight.has(username)) return false
  state.manuallyStopped = false
  state.scheduledStartAt = reserveBatchStart()
  state.status = 'scheduled'
  pendingUsernames.push(username)
  fillInitialConnectionSlots()
  return true
}

// Reconnects deliberately use the very same batch scheduler as manual starts.
// They are always appended, so a disconnect can never bypass accounts already
// queued for a connection slot.
function enqueueReconnect (username, state) {
  if (
    !state ||
    state.bot ||
    state.manuallyStopped ||
    pendingUsernames.includes(username) ||
    startupInFlight.has(username)
  ) return false

  state.scheduledStartAt = reserveBatchStart()
  state.status = 'scheduled'
  pendingUsernames.push(username)
  log(username, `reconnect queued at the end of the connection queue (${pendingUsernames.length} waiting)`)
  fillInitialConnectionSlots()
  return true
}

function startAccounts (count) {
  const available = [...bots.entries()]
    .filter(([username, state]) => state.status === 'stopped' && !state.bot && !pendingUsernames.includes(username) && !startupInFlight.has(username))
    .map(([username]) => username)
  const limit = Math.max(0, Math.min(Number(count) || 0, available.length))
  let started = 0
  for (const username of available.slice(0, limit)) {
    if (enqueueAccount(username)) started += 1
  }
  return started
}

function stopAccount (username) {
  const state = bots.get(username)
  if (!state || state.status === 'stopped') return false
  state.manuallyStopped = true
  const pendingIndex = pendingUsernames.indexOf(username)
  if (pendingIndex !== -1) pendingUsernames.splice(pendingIndex, 1)
  startupInFlight.delete(username)
  state.scheduledStartAt = null
  clearTimers(state)
  releaseVerificationSlot(username, state)
  if (state.bot) {
    state.status = 'stopping'
    state.bot.quit('manual stop')
  } else {
    state.status = 'stopped'
  }
  publishDashboard()
  return true
}

function stopAllAccounts () {
  let stopped = 0
  for (const username of bots.keys()) {
    if (stopAccount(username)) stopped += 1
  }
  resetBatchSchedule()
  return stopped
}

function fillInitialConnectionSlots () {
  if (shuttingDown || connectionDispatchPaused) return

  while (
    pendingUsernames.length > 0 &&
    activeVerifications.size + startupInFlight.size < CONFIG.maxVerificationLinks
  ) {
    const username = pendingUsernames[0]
    const state = bots.get(username)
    if (!state || state.manuallyStopped || state.status !== 'scheduled') {
      pendingUsernames.shift()
      continue
    }

    const wait = state.scheduledStartAt - Date.now()
    if (wait > 0) {
      scheduleBatchGate(wait)
      break
    }

    pendingUsernames.shift()
    startupInFlight.add(username)
    state.scheduledStartAt = null
    try {
      createBot(username)
    } catch (err) {
      startupInFlight.delete(username)
      state.status = 'stopped'
      log(username, `could not start client: ${err.message}`)
    }
  }
  publishDashboard()
}

function start () {
  fs.mkdirSync(LOG_DIR, { recursive: true })
  console.log(`Worker ready for ${CONFIG.count} accounts -> ${CONFIG.host}:${CONFIG.port} (${CONFIG.version}). Waiting for dashboard commands; batches: ${CONFIG.batchSize}, ${CONFIG.batchAccountDelay}ms apart, ${CONFIG.batchCooldown}ms cooldown.`)
  for (let i = 1; i <= CONFIG.count; i++) {
    const username = `${CONFIG.prefix}${i}`
    bots.set(username, newBotState(username))
  }
  startControlServer()
  telemetryTimer = setInterval(publishDashboard, 1000)
  startCommandConsole()
}

function shutdown () {
  if (shuttingDown) return
  shuttingDown = true
  console.log('\nShutting down...')
  if (commandConsole) commandConsole.close()
  if (telemetryTimer) clearInterval(telemetryTimer)
  telemetryTimer = null
  for (const timer of startupTimers) clearTimeout(timer)
  startupTimers.clear()
  batchGateTimer = null
  startupInFlight.clear()
  pendingUsernames.length = 0
  for (const timer of commandTimers) clearTimeout(timer)
  commandTimers.clear()
  for (const client of dashboardClients) client.end()
  dashboardClients.clear()
  if (dashboardServer) dashboardServer.close()
  for (const client of controlClients) client.close()
  controlClients.clear()
  if (controlServer) controlServer.close()
  for (const [, state] of bots) {
    state.status = 'stopping'
    clearTimers(state)
    if (state.bot) state.bot.quit('shutdown')
  }
  // Give the quit packets a moment to flush.
  setTimeout(() => process.exit(0), 1000)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

// One bot's bug should not kill the fleet.
process.on('unhandledRejection', (err) => console.error('unhandledRejection:', err))

start()
