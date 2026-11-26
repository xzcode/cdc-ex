#!/usr/bin/env node
// Real browser acceptance against an isolated CDC EX server and MySQL fixture.
// Invoked by security-smoke.sh with CDC_TEST_UI=true; no API mocking.
import { createRequire } from 'node:module'
import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
const require = createRequire(new URL('../cdc-web/package.json', import.meta.url))
const { chromium, expect } = require('@playwright/test')

function required(name) { const value = process.env[name]; if (!value) throw new Error(`Missing ${name}`); return value }
const base = required('CDC_UI_BASE_URL').replace(/\/$/, '')
const token = required('CDC_UI_TOKEN')
const port = required('CDC_UI_DB_PORT')
const user = required('CDC_UI_DB_USER')
const password = required('CDC_UI_DB_PASSWORD')
const sourceDatabase = process.env.CDC_UI_SOURCE_DATABASE || 'ui_src'
const targetDatabase = process.env.CDC_UI_TARGET_DATABASE || 'ui_dst'
const secondTargetDatabase = process.env.CDC_UI_SECOND_TARGET_DATABASE
const prefix = `ui-${Date.now()}`
const sourceId = `${prefix}-source`, sinkId = `${prefix}-sink`, taskId = `${prefix}-task`
const secondSourceId = `${prefix}-source2`, secondSinkId = `${prefix}-sink2`, secondTaskId = `${prefix}-task2`
const replicationId = Number(process.env.CDC_UI_REPLICATION_ID || '47654')
const screenshots = process.env.CDC_UI_ARTIFACT_DIR || process.env.CDC_UI_SCREENSHOT_DIR || join(process.cwd(), 'ui-artifacts')
mkdirSync(screenshots, { recursive: true })
const executablePath = process.env.CDC_UI_CHROMIUM || (existsSync('/usr/lib/chromium/chromium') ? '/usr/lib/chromium/chromium' : undefined)
const browser = await chromium.launch({ executablePath, headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] })
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, locale: 'en-US' })
const page = await context.newPage()
const errors = []
page.on('pageerror', error => errors.push(error.message))
page.on('console', message => { if (message.type() === 'error' && !message.text().includes('favicon')) errors.push(message.text()) })
page.on('request', request => {
  if (request.url().includes('/api/')) {
    if (request.url().includes(token) || request.url().includes(password)) errors.push('Credential appeared in request URL')
    if (request.headers().authorization !== `Bearer ${token}`) errors.push('Authenticated UI request omitted authorization header')
  }
})
async function api(path) {
  const response = await fetch(`${base}/api${path}`, { headers: { Authorization: `Bearer ${token}` } })
  if (!response.ok) throw new Error(`UI fixture API returned HTTP ${response.status}`)
  return response.json()
}
async function task(id = taskId) { return (await api('/tasks')).find(item => item.id === id) }
function taskRow(id) { return page.locator('.task-table tbody tr').filter({ has: page.getByText(id, { exact: true }) }) }
async function refresh() {
  if (await page.getByRole('dialog').isVisible()) {
    await page.waitForResponse(response => response.url().endsWith('/api/tasks') && response.request().method() === 'GET', { timeout: 20000 })
  } else await page.getByRole('button', { name: 'Refresh', exact: true }).click()
}
async function waitState(accepted, id = taskId) {
  await expect.poll(async () => (await task(id))?.state, { timeout: 90000, intervals: [500, 1000, 2000] }).toMatch(new RegExp(`^(${accepted.join('|')})$`))
  await refresh()
}
async function waitDelivered(id = taskId) {
  await expect.poll(async () => {
    const data = await task(id); const journal = data?.health?.journal
    return !!journal && journal.total >= 1 && journal.pending === 0
  }, { timeout: 120000, intervals: [500, 1000, 2000] }).toBe(true)
}
async function closeDialog() { await page.getByRole('dialog').getByRole('button', { name: 'Close', exact: true }).first().click() }
async function fillNameAndIdentifier(modal, name, id) {
  const nameInput = modal.getByLabel('Name', { exact: true })
  const identifier = modal.getByLabel('Identifier', { exact: true })
  await nameInput.fill(name)
  // Name blur renders the suggested identifier. Let that render finish before
  // selecting and replacing its contents with this fixture's recovery namespace.
  await nameInput.press('Tab')
  await expect(identifier).not.toHaveValue('')
  await identifier.fill(id)
  await expect(identifier).toHaveValue(id)
}
async function addConnection(id, name, database, source, serverId = replicationId) {
  await page.getByRole('button', { name: 'Add connection', exact: true }).first().click()
  const modal = page.getByRole('dialog')
  await fillNameAndIdentifier(modal, name, id)
  await modal.getByLabel('Connector', { exact: true }).selectOption('MYSQL')
  await modal.getByLabel('Host', { exact: true }).fill('127.0.0.1')
  await modal.getByLabel('Port', { exact: true }).fill(port)
  await modal.getByLabel('Database', { exact: true }).fill(database)
  await modal.getByLabel('Table / collection', { exact: true }).fill('items')
  await modal.getByLabel('Username', { exact: true }).fill(user)
  await modal.getByLabel('Password', { exact: true }).fill(password)
  await modal.getByLabel('TLS mode', { exact: true }).selectOption(process.env.CDC_UI_DB_SSL_MODE || 'REQUIRED')
  if (source) await modal.getByLabel('Replication client ID (source)', { exact: true }).fill(String(serverId))
  await modal.getByRole('button', { name: 'Save connection', exact: true }).click()
  await expect(modal).not.toBeVisible({ timeout: 30000 })
  const card = page.locator('.connection-card').filter({ has: page.getByRole('heading', { name, exact: true }) })
  await expect(card).toBeVisible()
  await card.getByRole('button', { name: source ? 'Test as source' : 'Test as destination', exact: true }).click()
  await expect(page.getByRole('dialog').getByText('Connection preflight passed', { exact: true })).toBeVisible({ timeout: 30000 })
  await closeDialog()
}
async function createTask(id, name, source, sink, screenshot = false) {
  await page.getByRole('button', { name: 'New task', exact: true }).first().click()
  const modal = page.getByRole('dialog')
  await fillNameAndIdentifier(modal, name, id)
  await modal.getByLabel('Source', { exact: true }).selectOption(source)
  await modal.getByLabel('Destination', { exact: true }).selectOption(sink)
  await modal.getByLabel('Capture mode', { exact: true }).selectOption('initial')
  await modal.getByRole('button', { name: 'Continue', exact: true }).click()
  await modal.getByRole('button', { name: 'Discover fields', exact: true }).click()
  await expect(modal.getByLabel('Source field 1', { exact: true })).toHaveValue('id', { timeout: 30000 })
  await expect(modal.getByLabel('Include id', { exact: true })).toBeDisabled()
  if (screenshot) await page.screenshot({ path: join(screenshots, 'mapping.png'), fullPage: true })
  await modal.getByRole('button', { name: 'Continue', exact: true }).click()
  await modal.getByRole('button', { name: 'Create task', exact: true }).click()
  await expect(modal).not.toBeVisible({ timeout: 30000 })
  await expect(taskRow(id)).toBeVisible()
}
try {
  await page.goto(base, { waitUntil: 'networkidle' })
  await expect(page.getByRole('heading', { name: 'Welcome to your data workspace' })).toBeVisible()
  await page.screenshot({ path: join(screenshots, 'login.png'), fullPage: true })
  await page.getByLabel('Administrator token', { exact: true }).fill(token)
  await page.getByRole('button', { name: 'Open workspace', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'Your data, in sync.' })).toBeVisible({ timeout: 30000 })
  await page.getByRole('navigation').getByRole('button', { name: 'Connections', exact: true }).click()
  await addConnection(sourceId, 'UI acceptance source', sourceDatabase, true)
  await addConnection(sinkId, 'UI acceptance destination', targetDatabase, false)
  if (secondTargetDatabase) {
    await addConnection(secondSourceId, 'UI concurrent source', sourceDatabase, true, replicationId + 1)
    await addConnection(secondSinkId, 'UI concurrent destination', secondTargetDatabase, false)
  }
  const listed = await api('/connections')
  const persisted = listed.filter(item => [sourceId, sinkId].includes(item.id))
  expect(persisted).toHaveLength(2)
  expect(JSON.stringify(persisted)).not.toContain(password)
  expect(persisted.every(item => item.configuredSecrets.includes('password'))).toBe(true)
  // Editing without a new secret must preserve the server-side credential.
  const sourceCard = page.locator('.connection-card').filter({ has: page.getByRole('heading', { name: 'UI acceptance source', exact: true }) })
  await sourceCard.getByRole('button', { name: 'Edit connection UI acceptance source', exact: true }).click()
  await expect(page.getByRole('dialog').getByLabel('Password', { exact: true })).toHaveValue('')
  await page.getByRole('dialog').getByRole('button', { name: 'Save connection', exact: true }).click()
  await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 30000 })
  await page.getByRole('navigation').getByRole('button', { name: /^Sync tasks/ }).click()
  await createTask(taskId, 'UI acceptance pipeline', sourceId, sinkId, true)
  if (secondTargetDatabase) await createTask(secondTaskId, 'UI concurrent pipeline', secondSourceId, secondSinkId)
  const row = taskRow(taskId)
  await expect(row).toBeVisible()
  await row.getByRole('button', { name: 'Details UI acceptance pipeline', exact: true }).click()
  const modal = page.getByRole('dialog')
  await modal.getByRole('button', { name: 'Validate', exact: true }).click()
  await expect(modal.getByRole('heading', { name: 'Source schema', exact: true })).toBeVisible({ timeout: 30000 })
  await modal.getByRole('button', { name: 'Start', exact: true }).click()
  await waitState(['CAPTURING', 'STREAMING', 'RUNNING'])
  await waitDelivered()
  await refresh()
  await closeDialog()
  if (secondTargetDatabase) {
    await taskRow(secondTaskId).getByRole('button', { name: 'Start UI concurrent pipeline', exact: true }).click()
    await waitState(['CAPTURING', 'STREAMING', 'RUNNING'], secondTaskId)
    await waitDelivered(secondTaskId)
    expect(['CAPTURING', 'STREAMING', 'RUNNING']).toContain((await task()).state)
    await refresh()
    await page.screenshot({ path: join(screenshots, 'concurrent-tasks.png'), fullPage: true })
  }
  await page.getByRole('navigation').getByRole('button', { name: 'Overview', exact: true }).click()
  await page.screenshot({ path: join(screenshots, 'overview.png'), fullPage: true })
  await page.getByRole('navigation').getByRole('button', { name: /^Sync tasks/ }).click()
  await taskRow(taskId).getByRole('button', { name: 'Pause UI acceptance pipeline', exact: true }).click()
  await waitState(['PAUSED'])
  await taskRow(taskId).getByRole('button', { name: 'Start UI acceptance pipeline', exact: true }).click()
  await waitState(['CAPTURING', 'STREAMING', 'RUNNING'])
  await taskRow(taskId).getByRole('button', { name: 'Pause UI acceptance pipeline', exact: true }).click()
  await waitState(['PAUSED'])
  if (secondTargetDatabase) {
    expect(['CAPTURING', 'STREAMING', 'RUNNING']).toContain((await task(secondTaskId)).state)
    await taskRow(secondTaskId).getByRole('button', { name: 'Pause UI concurrent pipeline', exact: true }).click()
    await waitState(['PAUSED'], secondTaskId)
  }
  await taskRow(taskId).getByRole('button', { name: 'Details UI acceptance pipeline', exact: true }).click()
  await page.getByRole('dialog').getByRole('button', { name: 'Compare data', exact: true }).click()
  await page.getByRole('dialog').getByLabel('Source writes are stopped and the task has finished delivering.', { exact: true }).check()
  const comparisonResponse = page.waitForResponse(response => response.url().endsWith(`/api/tasks/${taskId}/compare`) && response.request().method() === 'POST')
  await page.getByRole('dialog').getByRole('button', { name: 'Run comparison', exact: true }).click()
  const comparison = await (await comparisonResponse).json()
  expect(comparison.complete).toBe(true)
  expect(comparison.consistent).toBe(true)
  expect(comparison.sourceCount).toBe(1)
  expect(comparison.targetCount).toBe(1)
  await page.screenshot({ path: join(screenshots, 'comparison.png'), fullPage: true })
  await closeDialog()
  await page.getByRole('button', { name: '中文', exact: true }).click()
  await expect(page.getByRole('heading', { name: '单向同步任务', exact: true })).toBeVisible()
  await page.setViewportSize({ width: 390, height: 844 })
  await page.getByRole('button', { name: '工作空间', exact: true }).click()
  await page.getByRole('navigation').getByRole('button', { name: '概览', exact: true }).click()
  await expect(page.getByRole('heading', { name: '数据同步，清晰可见。', exact: true })).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1)).toBe(true)
  await page.screenshot({ path: join(screenshots, 'mobile-zh.png'), fullPage: true })
  const html = await page.content()
  expect(html).not.toContain(password)
  expect(html).not.toContain(token)
  expect(errors).toEqual([])
  console.log(`Real UI acceptance passed: authenticated login, protected connections, preflight, field wizard, MySQL snapshot, pause/resume${secondTargetDatabase ? ', concurrent tasks with separate replication identities' : ''}, exact comparison, EN/ZH, mobile layout and no browser errors.`)
} catch (error) {
  // Preserve useful UI failure evidence without serializing credential values.
  const redact = text => String(text).replaceAll(password, '[redacted]').replaceAll(token, '[redacted]')
  try {
    await page.screenshot({ path: join(screenshots, 'failure.png'), fullPage: true })
    const controls = await page.locator('select').evaluateAll(elements => elements.map(element => ({
      ariaLabel: element.getAttribute('aria-label'),
      labels: Array.from(element.labels || []).map(label => label.textContent),
      disabled: element.disabled,
      choices: Array.from(element.options).map(option => option.text),
    })))
    writeFileSync(join(screenshots, 'failure-controls.json'), redact(JSON.stringify({ controls, browserErrors: errors }, null, 2)))
  } catch { /* Reporting must not hide the original acceptance failure. */ }
  throw new Error(redact(error?.stack || error))
} finally { await browser.close() }
