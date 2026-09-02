'use strict'

const fs = require('node:fs')
const path = require('node:path')
const http = require('node:http')
const { spawn } = require('node:child_process')
const { WebSocket } = require('ws')

const CONFIG_FILE = path.join(__dirname, 'config.json')
const WORKER_FILE = path.join(__dirname, 'worker.js')
const config = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'))
const DASHBOARD_HOST = process.env.DASHBOARD_HOST || config.dashboardHost || '127.0.0.1'
const DASHBOARD_PORT = Number(process.env.DASHBOARD_PORT || config.dashboardPort || 3000)
const CONTROL_HOST = process.env.CONTROL_HOST || config.controlHost || '127.0.0.1'
const CONTROL_PORT = Number(process.env.CONTROL_PORT || config.controlPort || 3001)
const WORKER_URL = 'ws://' + CONTROL_HOST + ':' + CONTROL_PORT
const OPEN_DASHBOARD = process.env.OPEN_DASHBOARD === undefined ? (config.openDashboard ?? true) : process.env.OPEN_DASHBOARD !== '0'

let worker = null
let reconnectTimer = null
let nextRequestId = 1
const pending = new Map()
const browserClients = new Set()
let workerStatus = 'Worker offline — retrying'
let workerState = {
  fleet: { host: config.host || 'localhost', port: config.port || 25565, version: config.version || '—' },
  accounts: [],
}

function loadDashboardHtml () {
  const source = fs.readFileSync(WORKER_FILE, 'utf8')
  const tick = String.fromCharCode(96)
  const marker = 'const DASHBOARD_HTML = ' + tick
  const start = source.indexOf(marker) + marker.length
  const end = source.lastIndexOf(tick, source.indexOf('function sendDashboardResponse'))
  if (start < marker.length || end < start) throw new Error('Could not load dashboard template from worker.js.')
  return Function('return ' + tick + source.slice(start, end) + tick)()
}

function stateForBrowser () {
  return { ...workerState, workerStatus }
}

function publish () {
  const event = 'data: ' + JSON.stringify(stateForBrowser()) + '\n\n'
  for (const client of browserClients) {
    try { client.write(event) } catch { browserClients.delete(client) }
  }
}

function connectWorker () {
  if (worker && (worker.readyState === WebSocket.OPEN || worker.readyState === WebSocket.CONNECTING)) return
  worker = new WebSocket(WORKER_URL)
  worker.on('open', () => {
    workerStatus = 'Worker online'
    publish()
  })
  worker.on('message', (raw) => {
    try {
      const message = JSON.parse(raw.toString())
      if (message.type === 'state') {
        workerState = message.state
        publish()
        return
      }
      if (message.type === 'response') {
        const request = pending.get(message.id)
        if (!request) return
        pending.delete(message.id)
        clearTimeout(request.timer)
        message.ok ? request.resolve(message.message) : request.reject(new Error(message.message))
      }
    } catch {}
  })
  worker.on('close', () => {
    worker = null
    workerStatus = 'Worker offline — retrying'
    publish()
    reconnectTimer = setTimeout(connectWorker, 1000)
  })
  worker.on('error', () => {})
}

function workerRequest (action, payload) {
  if (!worker || worker.readyState !== WebSocket.OPEN) {
    return Promise.reject(new Error('The persistent worker is offline. Start it with npm run worker.'))
  }
  const id = nextRequestId++
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id)
      reject(new Error('The worker did not respond in time.'))
    }, 10000)
    pending.set(id, { resolve, reject, timer })
    worker.send(JSON.stringify({ id, action, payload }))
  })
}

function readBody (req) {
  return new Promise((resolve, reject) => {
    let body = ''
    req.on('data', (chunk) => {
      body += chunk
      if (body.length > 20_000) req.destroy()
    })
    req.on('end', () => {
      try { resolve(JSON.parse(body || '{}')) } catch { reject(new Error('Invalid request body.')) }
    })
  })
}

function reply (res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' })
  res.end(JSON.stringify(body))
}

const routeActions = {
  '/command': 'command',
  '/claim-playtime-rewards': 'claimPlaytimeRewards',
  '/toggle-connection-dispatch': 'toggleConnections',
  '/fleet-start': 'startFleet',
  '/fleet-stop': 'stopFleet',
  '/account-start': 'startAccount',
  '/account-stop': 'stopAccount',
  '/release-verification': 'releaseVerification',
  '/auto-commands': 'setAutoCommands',
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://' + DASHBOARD_HOST + ':' + DASHBOARD_PORT)
  if (req.method === 'GET' && url.pathname === '/') {
    try {
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store',
        'X-Content-Type-Options': 'nosniff',
        'Content-Security-Policy': "default-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'",
      })
      res.end(loadDashboardHtml())
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' })
      res.end(err.message)
    }
    return
  }
  if (req.method === 'GET' && url.pathname === '/events') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive', 'X-Content-Type-Options': 'nosniff' })
    browserClients.add(res)
    res.write('data: ' + JSON.stringify(stateForBrowser()) + '\n\n')
    req.on('close', () => browserClients.delete(res))
    return
  }
  if (req.method === 'POST' && routeActions[url.pathname]) {
    try {
      const payload = await readBody(req)
      const message = await workerRequest(routeActions[url.pathname], payload)
      reply(res, 200, { message })
    } catch (err) {
      reply(res, 400, { message: err.message })
    }
    return
  }
  res.writeHead(404)
  res.end()
})

server.listen(DASHBOARD_PORT, DASHBOARD_HOST, () => {
  console.log('Dashboard: http://' + DASHBOARD_HOST + ':' + DASHBOARD_PORT)
  connectWorker()
  if (OPEN_DASHBOARD && process.platform === 'win32') {
    const browser = spawn('cmd.exe', ['/d', '/c', 'start', '', 'http://' + DASHBOARD_HOST + ':' + DASHBOARD_PORT], { detached: true, stdio: 'ignore', windowsHide: true })
    browser.unref()
  }
})

function shutdown () {
  if (reconnectTimer) clearTimeout(reconnectTimer)
  for (const request of pending.values()) {
    clearTimeout(request.timer)
    request.reject(new Error('Dashboard is restarting.'))
  }
  pending.clear()
  if (worker) worker.close()
  for (const client of browserClients) client.end()
  browserClients.clear()
  server.close(() => process.exit(0))
  setTimeout(() => process.exit(0), 1000)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
