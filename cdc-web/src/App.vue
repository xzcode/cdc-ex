<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import Icon from './components/Icon.vue'
import BatchTaskForm from './components/BatchTaskForm.vue'
import LiveMonitor from './components/LiveMonitor.vue'
import { message } from './i18n'
import { connectionFields, connectorNames, connectorMarks, defaultOptions, conversions, isSecret } from './connections'
import type { Audit, Connection, ConnectorCaps, Inspection, Locale, Mapping, MappingRow, Task, View, BatchResult } from './types'

const locale = ref<Locale>(localStorage.getItem('cdc-ex-locale') === 'zh' ? 'zh' : 'en')
const t = (key: string, values: Record<string, string | number> = {}) => message(locale.value, key, values)
const token = ref(sessionStorage.getItem('cdc-ex-token') || '')
const tokenInput = ref('')
const authenticated = ref(false)
const loggingIn = ref(false)
const refreshing = ref(false)
const busy = ref('')
const view = ref<View>('overview')
const navOpen = ref(false)
const error = ref('')
const toast = ref('')
const connections = ref<Connection[]>([])
const tasks = ref<Task[]>([])
const audits = ref<Audit[]>([])
const connectors = ref<Record<string, ConnectorCaps>>({})
const serverOnline = ref(false)
const search = ref('')
const statusFilter = ref('')
const groupFilter=ref(''), selection=ref<string[]>([]), groupDraft=ref('')
const bulkAction=ref<'start'|'pause'|'stop'|'group'>('group')
const batchResults=ref<BatchResult[]>([])
const batchFailed=computed(()=>batchResults.value.filter(r=>r.outcome==='FAILED').length)
const now = ref(Date.now())
let pollTimer: ReturnType<typeof setInterval> | undefined
let toastTimer: ReturnType<typeof setTimeout> | undefined
const dialog = ref<HTMLDialogElement | null>(null)
const modal = ref<'connection' | 'task' | 'details' | 'inspection' | 'compare' | 'ddl' | 'batch' | 'bulk' | 'batch-result' | ''>('')
const modalError = ref('')
const selectedTask = ref<Task | null>(null)
const inspected = ref<{ source?: Inspection; sink?: Inspection }>({})
const inspectionResult = ref<Inspection | null>(null)
const comparisonResult = ref<unknown>(null)
const comparisonSummary = computed(() => {
  const data = comparisonResult.value
  if (!data || typeof data !== 'object') return null
  const result = data as Record<string, unknown>
  if (typeof result.complete !== 'boolean') return null
  return {
    complete: result.complete, consistent: result.complete && result.consistent === true,
    sourceCount: typeof result.sourceCount === 'number' ? result.sourceCount : undefined,
    targetCount: typeof result.targetCount === 'number' ? result.targetCount : undefined,
  }
})
const ddlText = ref('')
const copyDone = ref(false)
const comparison = reactive({ sourceQuiescent: false, maxRows: 10000 })
const connectionDraft = reactive({ id: '', name: '', type: 'MYSQL', options: {} as Record<string, string>, extra: '{}', editing: false, savedSecrets: [] as string[] })
const taskDraft = reactive({ id: '', name: '', group: '', sourceConnectionId: '', sinkConnectionId: '', snapshotMode: 'initial', journalMiB: 256, journalEvents: 1000000, batchSize: 128, maxFailures: 12 })
const wizardStep = ref(1)
const mappingRows = ref<MappingRow[]>([])
const names = connectorNames
const marks = connectorMarks
const navigation: { id: View; icon: string; label: string }[] = [{ id: 'overview', icon: 'grid', label: 'overview' }, { id: 'tasks', icon: 'flow', label: 'tasks' }, { id: 'connections', icon: 'database', label: 'connections' }, { id: 'audit', icon: 'audit', label: 'audit' }]
const activeStates = new Set(['STARTING', 'CAPTURING', 'RUNNING', 'SNAPSHOTTING', 'STREAMING', 'RETRYING', 'PAUSING', 'STOPPING'])
const state = (task: Task) => task.state || task.health?.state || 'DRAFT'
const active = (task: Task) => activeStates.has(state(task))
const currentTask = computed(() => tasks.value.find(item => item.id === selectedTask.value?.id) || selectedTask.value)
const connectionMap = computed(() => Object.fromEntries(connections.value.map(item => [item.id, item])))
const sourceConnection = computed(() => connectionMap.value[taskDraft.sourceConnectionId])
const sinkConnection = computed(() => connectionMap.value[taskDraft.sinkConnectionId])
const fields = computed(() => connectionFields(connectionDraft.type))
const sourceModes = computed(() => {
  const connection = sourceConnection.value
  const modes = (connectors.value[connection?.type || '']?.sourceModes || ['initial', 'no_data']).filter(mode => mode !== 'resume')
  return connection?.type === 'CLICKHOUSE' && modes.includes(connection.options.mode || '') ? [connection.options.mode!] : modes
})
const visibleTasks = computed(() => tasks.value.filter(task => {
  const text = `${task.name} ${task.id} ${task.group||''} ${task.sourceResource||''} ${task.sinkResource||''} ${connectionName(task.sourceConnectionId)} ${connectionName(task.sinkConnectionId)}`.toLowerCase()
  return text.includes(search.value.toLowerCase()) && (!statusFilter.value || state(task) === statusFilter.value) && (!groupFilter.value || (groupFilter.value==='ungrouped'?!task.group:groupFilter.value==='group:'+task.group))
}))
const groups=computed(()=>[...new Set(tasks.value.map(task=>task.group||'').filter(Boolean))].sort())
const selectedVisible=computed(()=>visibleTasks.value.filter(task=>selection.value.includes(task.id)).map(task=>task.id))
const allVisibleSelected=computed(()=>visibleTasks.value.length>0&&selectedVisible.value.length===visibleTasks.value.length)
const listedTasks = computed(() => view.value === 'overview' ? visibleTasks.value.slice(0, 6) : visibleTasks.value)
const totals = computed(() => ({ active: tasks.value.filter(active).length, pending: tasks.value.reduce((n, task) => n + (task.health?.journal?.pending || 0), 0), bytes: tasks.value.reduce((n, task) => n + (task.health?.journal?.bytes || 0), 0) }))
const schemaFields = computed(() => Object.keys(inspected.value.source?.fields || {}).filter(field => field !== '*'))
const knownKeys = computed(() => inspected.value.source?.keyFields || [])
const number = (value: number) => new Intl.NumberFormat(locale.value === 'zh' ? 'zh-CN' : 'en-US').format(value)
function size(value: number) { if (value < 1024) return `${value} B`; if (value < 1048576) return `${(value / 1024).toFixed(1)} KiB`; return `${(value / 1048576).toFixed(1)} MiB` }
function relative(value?: number) { if (!value) return t('never'); const seconds = Math.max(0, Math.floor((now.value - value) / 1000)); return seconds < 60 ? t('secondsAgo', { n: seconds }) : seconds < 3600 ? t('minutesAgo', { n: Math.floor(seconds / 60) }) : t('hoursAgo', { n: Math.floor(seconds / 3600) }) }
function date(value?: string | number) { if (!value) return '—'; const time = new Date(value); return Number.isNaN(time.getTime()) ? '—' : time.toLocaleString(locale.value === 'zh' ? 'zh-CN' : 'en-GB', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) }
function connectionName(id: string) { return connectionMap.value[id]?.name || id }
function taskEndpoint(task:Task,role:'source'|'sink'){
  const id=role==='source'?task.sourceConnectionId:task.sinkConnectionId,resource=role==='source'?task.sourceResource:task.sinkResource,c=connectionMap.value[id]
  return resource&&resource!==(c?.options.topic||c?.options.table)?connectionName(id)+' / '+resource:connectionName(id)
}
function rate(value?:number|null){return value==null||!Number.isFinite(value)?'—':number(Math.round(value*10)/10)}
function duration(value?:number|null){return value==null||!Number.isFinite(value)?'—':value<1000?number(Math.round(value))+' ms':number(Math.round(value/100)/10)+' s'}
function count(task:Task,kind:'pending'|'delivered'){const j=task.health?.journal;return j?number(kind==='pending'?j.pending:j.total-j.pending):state(task)==='DRAFT'?'0':'—'}
function toggleTask(id:string,checked:boolean){selection.value=checked?[...new Set([...selection.value,id])]:selection.value.filter(x=>x!==id)}
function selectVisible(checked:boolean){selection.value=checked?visibleTasks.value.map(task=>task.id):[]}
function openBulk(action:'start'|'pause'|'stop'|'group'){if(!selectedVisible.value.length)return;bulkAction.value=action;groupDraft.value='';batchResults.value=[];modal.value='bulk'}
async function applyBulk(){await perform('bulk',async()=>{const ids=[...selectedVisible.value];if(!ids.length)throw new Error(t('batchNoSelection'));batchResults.value=await api<BatchResult[]>('/tasks/bulk','POST',{ids,action:bulkAction.value,group:groupDraft.value},120000);selection.value=[];modal.value='batch-result';await refresh()})}
function outcomeLabel(outcome:string){return t(({CREATED:'outcomeCreated',UNCHANGED:'outcomeUnchanged',REQUESTED:'outcomeRequested',UPDATED:'outcomeUpdated',FAILED:'outcomeFailed'} as Record<string,string>)[outcome]||outcome)}
async function batchComplete(results:BatchResult[],group:string){batchResults.value=results;navigate('tasks');await refresh();groupFilter.value=group?'group:'+group:'';modal.value='batch-result'}
function connectorName(type: string) { return names[type] || type }
function endpoint(connection: Connection) {
  const options = connection.options || {}
  if (options.host) {
    const host = options.host.includes(':') && !options.host.startsWith('[') ? '[' + options.host + ']' : options.host
    return options.port ? host + ':' + options.port : host
  }
  return options.bootstrapServers || options.url || '—'
}
function namespace(connection: Connection) { return connection.options?.topic || [connection.options?.database, connection.options?.schema, connection.options?.table].filter(Boolean).join('.') || '—' }
function statusLabel(value: string) {
  const keys: Record<string, string> = { DRAFT: 'statusDraft', STARTING: 'statusStarting', CAPTURING: 'statusRunning', RUNNING: 'statusRunning', RETRYING: 'statusRetrying', BLOCKED: 'statusBlocked', FAILED: 'statusFailed', PAUSED: 'statusPaused', STOPPED: 'statusStopped', STOPPING: 'statusStopping', PAUSING: 'statusStopping', SNAPSHOTTING: 'statusSnapshotting', STREAMING: 'statusStreaming', VALIDATING: 'statusValidating' }
  return t(keys[value] || value)
}
function statusClass(value: string) { return ['BLOCKED', 'FAILED'].includes(value) ? 'danger' : value === 'RETRYING' ? 'warning' : activeStates.has(value) ? 'success' : 'neutral' }
function modeLabel(mode: string, type?: string) {
  if (type === 'KAFKA' && mode === 'initial') return t('streamInitialMode')
  return t(({ initial: 'initialMode', no_data: 'noDataMode', snapshot: 'snapshotOnlyMode', cursor: 'cursorMode', changelog: 'changelogMode', earliest: 'earliestMode', latest: 'latestMode' } as Record<string, string>)[mode] || mode)
}
function notify(text: string) { toast.value = text; if (toastTimer) clearTimeout(toastTimer); toastTimer = setTimeout(() => { toast.value = '' }, 4500) }
async function api<T>(path: string, method = 'GET', body?: unknown, timeout = 30000): Promise<T> {
  let response: Response
  try {
    response = await fetch(`/api${path}`, { method, headers: { Authorization: `Bearer ${token.value}`, ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}) }, body: body === undefined ? undefined : JSON.stringify(body), credentials: 'same-origin', signal: AbortSignal.timeout(timeout) })
  } catch { throw new Error(t('networkError')) }
  if (response.status === 401 || response.status === 403) { if (authenticated.value) signOut(); throw new Error(t('authExpired')) }
  const text = await response.text()
  let result: unknown = null
  try { result = text ? JSON.parse(text) : null } catch { throw new Error(t('requestFailed')) }
  if (!response.ok) {
    const data = result as { message?: string; error?: string; code?: string } | null
    throw new Error(data?.message || data?.code || data?.error || t('requestFailed'))
  }
  return result as T
}
async function refresh(initial = false) {
  if (refreshing.value || (!authenticated.value && !initial)) return
  refreshing.value = true
  try {
    const [connectionData, taskData, auditData, connectorData] = await Promise.all([api<Connection[]>('/connections'), api<Task[]>('/tasks'), api<Audit[]>('/audit'), api<Record<string, ConnectorCaps>>('/connectors')])
    connections.value = connectionData; tasks.value = taskData; audits.value = auditData; connectors.value = connectorData; serverOnline.value = true; now.value = Date.now()
    if (initial) authenticated.value = true
  } catch (e) { serverOnline.value = false; error.value = (e as Error).message; if (initial) throw e }
  finally { refreshing.value = false }
}
async function login() {
  if (!tokenInput.value.trim()) return
  loggingIn.value = true; error.value = ''; token.value = tokenInput.value.trim()
  try { await refresh(true); sessionStorage.setItem('cdc-ex-token', token.value); tokenInput.value = '' }
  catch { token.value = ''; error.value = t('loginError') }
  finally { loggingIn.value = false }
}
function signOut() { token.value = ''; tokenInput.value = ''; authenticated.value = false; sessionStorage.removeItem('cdc-ex-token'); tasks.value = []; connections.value = []; audits.value = []; selection.value=[]; closeModal() }
function navigate(target: View) { view.value = target; navOpen.value = false; search.value = ''; statusFilter.value = ''; groupFilter.value=''; selection.value=[] }
function closeModal() { modal.value = ''; modalError.value = ''; connectionDraft.options = {}; connectionDraft.extra = '{}'; comparisonResult.value = null; copyDone.value = false }
async function perform(key: string, action: () => Promise<void>) {
  if (busy.value) return
  busy.value = key; error.value = ''; modalError.value = ''
  try { await action() } catch (e) { if (modal.value) modalError.value = (e as Error).message; else error.value = (e as Error).message }
  finally { busy.value = '' }
}
function newConnection(connection?: Connection) {
  const type = connection?.type || 'MYSQL'
  connectionDraft.id = connection?.id || ''; connectionDraft.name = connection?.name || ''; connectionDraft.type = type; connectionDraft.editing = !!connection; connectionDraft.savedSecrets = connection?.configuredSecrets || []
  const options = { ...defaultOptions(type), ...(connection?.options || {}) }
  for (const key of Object.keys(options)) if (isSecret(key)) delete options[key]
  connectionDraft.options = options
  const known = new Set([...connectionFields(type).map(field => field.key), 'allowInsecure', 'tls', 'directConnection', 'exclusiveWriter'])
  connectionDraft.extra = JSON.stringify(Object.fromEntries(Object.entries(options).filter(([key]) => !known.has(key))), null, 2)
  modal.value = 'connection'
}
function changeConnector() { connectionDraft.options = defaultOptions(connectionDraft.type); connectionDraft.extra = '{}'; connectionDraft.savedSecrets = [] }
function suggestId(target: 'connection' | 'task') {
  const draft = target === 'connection' ? connectionDraft : taskDraft
  if (!draft.id) draft.id = draft.name.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-|-$/g, '').slice(0, 64)
}
function boolOption(name: string) { return connectionDraft.options[name] === 'true' }
function setBool(name: string, event: Event) { connectionDraft.options[name] = String((event.target as HTMLInputElement).checked) }
async function saveConnection() {
  await perform('connection-save', async () => {
    if (!connectionDraft.name.trim() || !/^[A-Za-z0-9_-]{1,64}$/.test(connectionDraft.id)) throw new Error(t('requiredName'))
    let extra: Record<string, string>
    try { extra = JSON.parse(connectionDraft.extra || '{}'); if (!extra || Array.isArray(extra) || typeof extra !== 'object' || Object.values(extra).some(value => typeof value !== 'string')) throw new Error() } catch { throw new Error(t('invalidJson')) }
    const known = new Set([...fields.value.map(field => field.key), 'allowInsecure', 'tls', 'directConnection', 'exclusiveWriter'])
    const options = { ...extra, ...Object.fromEntries(Object.entries(connectionDraft.options).filter(([key]) => known.has(key))) }
    for (const [key, value] of Object.entries(options)) if (value === '' || value === '***' || value === '[redacted]') delete options[key]
    const body = { id: connectionDraft.id, name: connectionDraft.name.trim(), endpoint: { type: connectionDraft.type, options } }
    await api(connectionDraft.editing ? `/connections/${encodeURIComponent(connectionDraft.id)}` : '/connections', connectionDraft.editing ? 'PUT' : 'POST', body)
    closeModal(); await refresh(); notify(t('saved'))
  })
}
async function testConnection(connection: Connection, role: 'SOURCE' | 'SINK') {
  await perform(`test-${connection.id}-${role}`, async () => { inspectionResult.value = await api<Inspection>(`/connections/${encodeURIComponent(connection.id)}/test?role=${role}`, 'POST'); modal.value = 'inspection'; notify(t('testPassed')) })
}
function newTask() {
  Object.assign(taskDraft, { id: '', name: '', group: '', sourceConnectionId: '', sinkConnectionId: '', snapshotMode: 'initial', journalMiB: 256, journalEvents: 1000000, batchSize: 128, maxFailures: 12 })
  mappingRows.value = []; inspected.value = {}; wizardStep.value = 1; modal.value = 'task'
}
function mapping(): Mapping {
  const result: Mapping = { rename: {}, conversions: {}, exclude: [] }; const seen = new Set<string>()
  for (const row of mappingRows.value) {
    const source = row.source.trim(), target = row.target.trim()
    if (!source || !target) throw new Error(t('invalidMapping'))
    if (seen.has(source)) throw new Error(t('duplicateMapping')); seen.add(source)
    if (row.excluded) result.exclude.push(source)
    else { if (source !== target) result.rename[source] = target; if (row.conversion) result.conversions[target] = row.conversion }
  }
  return result
}
function nextStep() {
  modalError.value = ''
  try {
    if (!taskDraft.name.trim() || !/^[A-Za-z0-9_-]{1,64}$/.test(taskDraft.id)) throw new Error(t('requiredName'))
    if (!sourceConnection.value || !sinkConnection.value) throw new Error(t('connectionNeeded'))
    if (sourceConnection.value.id === sinkConnection.value.id) throw new Error(t('differentEndpoints'))
    if (wizardStep.value === 2) mapping()
    wizardStep.value++
  } catch (e) { modalError.value = (e as Error).message }
}
async function discoverFields() {
  await perform('discover', async () => {
    if (!sourceConnection.value || !sinkConnection.value) throw new Error(t('connectionNeeded'))
    const [source, sink] = await Promise.all([api<Inspection>(`/connections/${encodeURIComponent(sourceConnection.value.id)}/test?role=SOURCE`, 'POST'), api<Inspection>(`/connections/${encodeURIComponent(sinkConnection.value.id)}/test?role=SINK`, 'POST')])
    inspected.value = { source, sink }
    if (!mappingRows.value.length && !source.fields['*']) mappingRows.value = Object.keys(source.fields).map(field => ({ source: field, target: field, conversion: '', excluded: false }))
    notify(t('discovered'))
  })
}
async function createTask() {
  await perform('create-task', async () => {
    const body = { ...taskDraft, journalBytes: Math.round(taskDraft.journalMiB * 1048576), mapping: mapping() }
    const { journalMiB: unused, ...payload } = body; void unused
    await api('/tasks', 'POST', payload); closeModal(); view.value = 'tasks'; await refresh(); notify(t('created'))
  })
}
async function taskAction(task: Task, action: 'start' | 'pause' | 'stop' | 'validate') {
  await perform(`${task.id}-${action}`, async () => {
    const result = await api<{ source: Inspection; sink: Inspection }>(`/tasks/${encodeURIComponent(task.id)}/${action}`, 'POST')
    if (action === 'validate') { inspected.value = result; selectedTask.value = task; modal.value = 'details'; notify(t('validated')) }
    else notify(t(action === 'start' ? 'started' : action === 'pause' ? 'paused' : 'stopped'))
    await refresh()
  })
}
function taskDetails(task: Task) { selectedTask.value = task; inspected.value = {}; modal.value = 'details' }
function openCompare(task: Task) { selectedTask.value = task; comparison.sourceQuiescent = false; comparison.maxRows = 10000; comparisonResult.value = null; modal.value = 'compare' }
async function compareTask() { if (!currentTask.value || !comparison.sourceQuiescent) return; await perform('compare', async () => { comparisonResult.value = await api(`/tasks/${encodeURIComponent(currentTask.value!.id)}/compare`, 'POST', comparison) }) }
async function openDdl(task: Task) { await perform('ddl', async () => { selectedTask.value = task; const result = await api<{ sql: string; executable: boolean }>(`/tasks/${encodeURIComponent(task.id)}/ddl`); ddlText.value = result.sql; modal.value = 'ddl' }) }
async function copyDdl() { try { await navigator.clipboard.writeText(ddlText.value); copyDone.value = true } catch { modalError.value = t('clipboardError') } }
function resultJson(value: unknown) { return JSON.stringify(value, null, 2) }
watch([search,statusFilter,groupFilter],()=>{selection.value=[]})
watch(locale, value => { localStorage.setItem('cdc-ex-locale', value); toast.value = ''; document.documentElement.lang = value === 'zh' ? 'zh-CN' : 'en' }, { immediate: true })
watch(() => taskDraft.sourceConnectionId, () => { inspected.value = {}; mappingRows.value = []; const modes = sourceModes.value; taskDraft.snapshotMode = modes.includes('initial') ? 'initial' : modes[0] || 'initial' })
watch(() => taskDraft.sinkConnectionId, () => { inspected.value = {} })
watch(modal, async value => { await nextTick(); if (value && !dialog.value?.open) dialog.value?.showModal(); else if (!value && dialog.value?.open) dialog.value.close() })
onMounted(async () => {
  if (token.value) { loggingIn.value = true; try { await refresh(true) } catch { sessionStorage.removeItem('cdc-ex-token'); token.value = '' } finally { loggingIn.value = false } }
  pollTimer = setInterval(() => { now.value = Date.now(); if (authenticated.value && !document.hidden && !busy.value) void refresh() }, 10000)
})
onBeforeUnmount(() => { if (pollTimer) clearInterval(pollTimer); if (toastTimer) clearTimeout(toastTimer) })
</script>

<template>
  <div v-if="!authenticated" class="login-shell">
    <aside class="login-story">
      <a class="brand" href="/" aria-label="CDC EX"><span class="brand-symbol"><i></i><i></i><i></i></span><span>CDC <b>EX</b></span></a>
      <div class="login-story-content"><span class="eyebrow">{{ t('dataInMotion') }}</span><h1>{{ t('loginHero') }}</h1><p>{{ t('loginHeroSub') }}</p>
        <div class="login-graph" aria-hidden="true"><span class="graph-db"><Icon name="database" :size="32" /></span><span class="graph-line"></span><span class="graph-core">EX</span><span class="graph-line"></span><span class="graph-db"><Icon name="flow" :size="32" /></span></div>
        <ul class="login-benefits"><li v-for="item in [{ title: 'reliable', sub: 'reliableSub', icon: 'shield' }, { title: 'explicit', sub: 'explicitSub', icon: 'flow' }, { title: 'visibility', sub: 'visibilitySub', icon: 'grid' }]" :key="item.title"><Icon :name="item.icon" /><div><strong>{{ t(item.title) }}</strong><span>{{ t(item.sub) }}</span></div></li></ul>
      </div><div class="login-story-footer"><span class="status-dot"></span>{{ t('selfHosted') }} · {{ t('singleNode') }}</div>
    </aside>
    <main class="login-main"><button class="language-toggle" @click="locale = locale === 'en' ? 'zh' : 'en'"><Icon name="globe" :size="17" />{{ locale === 'en' ? '简体中文' : 'English' }}</button>
      <form class="login-card" @submit.prevent="login"><div class="login-icon"><Icon name="lock" :size="26" /></div><h2>{{ t('loginTitle') }}</h2><p>{{ t('loginSub') }}</p>
        <div v-if="error" class="notice danger" role="alert"><Icon name="warning" /><span>{{ error }}</span></div>
        <label class="field"><span>{{ t('token') }}</span><input v-model="tokenInput" type="password" autocomplete="current-password" :placeholder="t('tokenPlaceholder')" required autofocus :disabled="loggingIn" /></label>
        <button class="button primary full" type="submit" :disabled="loggingIn || !tokenInput.trim()"><span>{{ t(loggingIn ? 'loggingIn' : 'login') }}</span><Icon v-if="!loggingIn" name="arrow" :size="18" /><span v-else class="spinner"></span></button>
        <p class="login-hint"><Icon name="shield" :size="16" />{{ t('tokenHint') }}</p>
      </form><span class="login-copyright">CDC EX · {{ t('workspace') }}</span>
    </main>
  </div>
  <div v-else class="app-shell">
    <aside class="sidebar" :class="{ open: navOpen }"><a class="brand" href="#" @click.prevent="navigate('overview')"><span class="brand-symbol"><i></i><i></i><i></i></span><span>CDC <b>EX</b></span></a><div class="workspace-label">{{ t('workspace') }}</div>
      <nav :aria-label="t('workspace')"><button v-for="item in navigation" :key="item.id" :class="{ selected: view === item.id }" :aria-current="view === item.id ? 'page' : undefined" @click="navigate(item.id)"><Icon :name="item.icon" /><span>{{ t(item.label) }}</span><span v-if="item.id === 'tasks'" class="nav-count">{{ tasks.length }}</span></button></nav>
      <div class="sidebar-footer"><a href="https://github.com/xzcode/cdc-ex/tree/master/docs" target="_blank" rel="noopener noreferrer"><Icon name="external" :size="17" />{{ t('docs') }}</a><div class="instance"><span class="instance-icon"><Icon name="shield" :size="19" /></span><div><strong>{{ t('singleNode') }}</strong><span>{{ t('localJournal') }}</span></div></div></div>
    </aside>
    <button v-if="navOpen" class="sidebar-shade" :aria-label="t('close')" @click="navOpen = false"></button>
    <div class="main-shell"><header class="topbar"><div class="breadcrumb"><button class="icon-button mobile-only" :aria-label="t('workspace')" @click="navOpen = !navOpen"><Icon name="menu" /></button><span>{{ t('workspace') }}</span><Icon name="chevron" :size="13" /><strong>{{ t(view) }}</strong></div><div class="topbar-actions"><span class="server-status" :class="{ offline: !serverOnline }"><span class="status-dot"></span>{{ t(serverOnline ? 'connected' : 'unavailable') }}</span><button class="icon-button" :class="{ spinning: refreshing }" :title="t('refresh')" :aria-label="t('refresh')" @click="refresh()"><Icon name="refresh" :size="17" /></button><button class="language-toggle" @click="locale = locale === 'en' ? 'zh' : 'en'">{{ locale === 'en' ? '中文' : 'EN' }}</button><span class="topbar-divider"></span><button class="icon-button" :title="t('signOut')" :aria-label="t('signOut')" @click="signOut"><Icon name="logout" :size="18" /></button></div></header>
      <main class="workspace-content">
        <div v-if="error" class="notice danger page-notice" role="alert"><Icon name="warning" /><span>{{ error }}</span><button class="icon-button" :aria-label="t('dismiss')" @click="error = ''"><Icon name="close" :size="16" /></button></div>
        <section class="page-heading"><div><span class="eyebrow">{{ t('selfHosted') }} / CDC EX</span><h1>{{ t(view === 'overview' ? 'welcome' : `${view}Title`) }}</h1><p>{{ t(view === 'overview' ? 'welcomeSub' : `${view}Sub`) }}</p></div><div v-if="view !== 'audit'" class="heading-actions"><button v-if="view === 'tasks'" class="button secondary" :disabled="!!busy || !connections.length" @click="modal='batch'"><Icon name="flow" :size="17"/>{{ t('batchTasks') }}</button><button class="button primary" @click="view === 'connections' ? newConnection() : newTask()"><Icon name="plus" :size="18" />{{ t(view === 'connections' ? 'newConnection' : 'newTask') }}</button></div><span v-else class="subtle-label"><Icon name="refresh" :size="15" />{{ t('refreshHint') }}</span></section>
        <section v-if="view === 'overview'" class="metrics-grid" :aria-label="t('overview')"><article class="metric-card"><div class="metric-top"><span>{{ t('totalTasks') }}</span><span class="metric-icon blue"><Icon name="flow" :size="18" /></span></div><strong>{{ number(tasks.length) }}</strong><p>{{ t('totalCaption') }}</p></article><article class="metric-card"><div class="metric-top"><span>{{ t('activeTasks') }}</span><span class="metric-icon green"><Icon name="bolt" :size="18" /></span></div><strong>{{ number(totals.active) }}<span class="live-pulse" v-if="totals.active"></span></strong><p>{{ t('activeCaption') }}</p></article><article class="metric-card"><div class="metric-top"><span>{{ t('pendingEvents') }}</span><span class="metric-icon amber"><Icon name="clock" :size="18" /></span></div><strong>{{ number(totals.pending) }}</strong><p>{{ t('pendingCaption') }}</p></article><article class="metric-card"><div class="metric-top"><span>{{ t('journalPayload') }}</span><span class="metric-icon violet"><Icon name="database" :size="18" /></span></div><strong class="compact-value">{{ size(totals.bytes) }}</strong><p>{{ t('payloadCaption') }}</p></article></section>
        <LiveMonitor v-if="view==='overview'" :tasks="tasks" :now="now" :t="t" :active="active" :rate="rate" :duration="duration"/>
        <section v-if="view === 'overview' || view === 'tasks'" class="panel task-panel"><div class="panel-header"><div><h2>{{ t(view === 'overview' ? 'recentTasks' : 'tasks') }}<span class="count-pill">{{ tasks.length }}</span></h2></div><button v-if="view === 'overview' && tasks.length" class="text-button" @click="navigate('tasks')">{{ t('viewAll') }}<Icon name="arrow" :size="16" /></button><span v-else-if="view === 'overview'" class="subtle-label"><span class="status-dot"></span>{{ t('refreshHint') }}</span></div>
          <div v-if="view === 'tasks' && tasks.length" class="table-toolbar"><label class="search-box"><Icon name="search" :size="17" /><input v-model="search" :placeholder="t('searchTasks')" :aria-label="t('searchTasks')" /></label><select v-model="statusFilter" :aria-label="t('state')"><option value="">{{ t('allStates') }}</option><option v-for="value in [...new Set(tasks.map(state))]" :key="value" :value="value">{{ statusLabel(value) }}</option></select><select v-model="groupFilter" :aria-label="t('taskGroup')"><option value="">{{ t('allGroups') }}</option><option value="ungrouped">{{ t('ungrouped') }}</option><option v-for="group in groups" :key="group" :value="'group:'+group">{{ group }}</option></select></div>
          <div v-if="view==='tasks'&&tasks.length" class="bulk-toolbar"><label class="check-field"><input type="checkbox" :checked="allVisibleSelected" :aria-label="t('selectVisible')" @change="selectVisible(($event.target as HTMLInputElement).checked)"/>{{ t('selectVisible') }}</label><span class="subtle-label">{{ t('selectedTasks',{n:selectedVisible.length}) }}</span><div class="bulk-actions"><button v-for="action in (['start','pause','stop','group'] as const)" :key="action" class="button secondary small" :disabled="!!busy||!selectedVisible.length" @click="openBulk(action)"><Icon v-if="action!=='group'" :name="action==='start'?'play':action" :size="14"/>{{ t(action==='group'?'assignGroup':action==='start'?'batchStart':action==='pause'?'batchPause':'batchStop') }}</button></div></div>
          <div v-if="!tasks.length" class="empty-state"><span class="empty-art"><Icon name="flow" :size="30" /></span><h3>{{ t('noTasks') }}</h3><p>{{ t('noTasksSub') }}</p><button class="button secondary" @click="connections.length < 2 ? newConnection() : newTask()"><Icon name="plus" :size="16" />{{ t(connections.length < 2 ? 'newConnection' : 'newTask') }}</button></div>
          <div v-else-if="!listedTasks.length" class="empty-state short"><Icon name="search" :size="25" /><h3>{{ t('noResults') }}</h3></div>
          <div v-else class="table-scroll"><table class="task-table"><thead><tr><th>{{ t('task') }}</th><th>{{ t('source') }} <span class="muted">→</span> {{ t('destination') }}</th><th>{{ t('state') }}</th><th class="number-cell">{{ t('pending') }}</th><th class="number-cell">{{ t('delivered') }}</th><th class="actions-cell">{{ t('actions') }}</th></tr></thead><tbody><tr v-for="task in listedTasks" :key="task.id"><td><div class="task-identity"><input v-if="view==='tasks'" type="checkbox" :checked="selection.includes(task.id)" :aria-label="t('selectTask',{name:task.name})" @change="toggleTask(task.id,($event.target as HTMLInputElement).checked)"/><button class="task-name" @click="taskDetails(task)"><span class="task-icon"><Icon name="flow" :size="18" /></span><span><strong>{{ task.name || task.id }}</strong><small>{{ task.id }}</small><small v-if="task.group" class="group-chip">{{ task.group }}</small></span></button></div></td><td><div class="route-cell"><div class="route-endpoint"><span class="connector-mark route-mark" :class="connectionMap[task.sourceConnectionId]?.type.toLowerCase()">{{ marks[connectionMap[task.sourceConnectionId]?.type || ''] || 'DB' }}</span><span class="route-name" :title="taskEndpoint(task,'source')">{{ taskEndpoint(task,'source') }}</span></div><Icon name="arrow" :size="16" /><div class="route-endpoint"><span class="connector-mark route-mark" :class="connectionMap[task.sinkConnectionId]?.type.toLowerCase()">{{ marks[connectionMap[task.sinkConnectionId]?.type || ''] || 'DB' }}</span><span class="route-name" :title="taskEndpoint(task,'sink')">{{ taskEndpoint(task,'sink') }}</span></div></div><small class="table-caption">{{ modeLabel(task.snapshotMode, connectionMap[task.sourceConnectionId]?.type) }}</small></td><td><span class="badge" :class="statusClass(state(task))"><span></span>{{ statusLabel(state(task)) }}</span></td><td class="number-cell" :data-label="t('pending')">{{ count(task,'pending') }}</td><td class="number-cell" :data-label="t('delivered')">{{ count(task,'delivered') }}<small v-if="active(task)&&task.health?.deliveredPerSecond!=null" class="table-caption rate-caption">{{ rate(task.health.deliveredPerSecond) }}/s</small></td><td class="actions-cell"><div class="row-actions"><button v-if="!active(task)" class="icon-button" :title="t(['BLOCKED', 'FAILED'].includes(state(task)) ? 'retry' : 'start')" :aria-label="`${t('start')} ${task.name}`" :disabled="!!busy" @click="taskAction(task, 'start')"><Icon name="play" :size="16" /></button><button v-else class="icon-button" :title="t('pause')" :aria-label="`${t('pause')} ${task.name}`" :disabled="!!busy || ['PAUSING', 'STOPPING'].includes(state(task))" @click="taskAction(task, 'pause')"><Icon name="pause" :size="16" /></button><button class="icon-button" :title="t('details')" :aria-label="`${t('details')} ${task.name}`" @click="taskDetails(task)"><Icon name="chevron" :size="17" /></button></div></td></tr></tbody></table></div>
        </section>
        <div v-if="view === 'overview'" class="overview-bottom"><section class="panel connector-panel"><div class="panel-header"><h2>{{ t('connectorsTitle') }}</h2><span class="count-pill">{{ Object.keys(connectors).length }}</span></div><p class="panel-description">{{ t('connectorsSub') }}</p><div class="connector-grid"><button v-for="(_, type) in connectors" :key="type" class="connector-tile" @click="newConnection(); connectionDraft.type = String(type); changeConnector()"><span class="connector-mark" :class="String(type).toLowerCase()">{{ marks[type] || String(type).slice(0, 2) }}</span><span>{{ connectorName(String(type)) }}</span><Icon name="plus" :size="15" /></button></div></section><section class="panel activity-panel"><div class="panel-header"><h2>{{ t('recentActivity') }}</h2><button class="text-button" @click="navigate('audit')"><Icon name="arrow" :size="17" /><span class="sr-only">{{ t('viewAll') }}</span></button></div><div v-if="!audits.length" class="quiet-empty"><Icon name="clock" :size="25" /><p>{{ t('noActivity') }}</p></div><ol v-else class="activity-list"><li v-for="(audit, index) in audits.slice(0, 4)" :key="index"><span class="activity-point"></span><div><strong>{{ audit.action }}</strong><p>{{ audit.resource }}</p><time>{{ date(audit.timestamp) }}</time></div><span class="audit-result">{{ audit.result }}</span></li></ol></section></div>
        <section v-if="view === 'connections'" class="connections-section"><div v-if="!connections.length" class="panel empty-state"><span class="empty-art"><Icon name="database" :size="30" /></span><h3>{{ t('noConnections') }}</h3><p>{{ t('noConnectionsSub') }}</p><button class="button secondary" @click="newConnection()"><Icon name="plus" :size="17" />{{ t('newConnection') }}</button></div><div v-else class="connection-grid"><article v-for="connection in connections" :key="connection.id" class="connection-card"><div class="connection-top"><span class="connector-mark large" :class="connection.type.toLowerCase()">{{ marks[connection.type] || connection.type.slice(0, 2) }}</span><button class="icon-button" :title="t('editConnection')" :aria-label="`${t('editConnection')} ${connection.name}`" :disabled="!!busy" @click="newConnection(connection)"><Icon name="edit" :size="17" /></button></div><h2 :title="connection.name">{{ connection.name }}</h2><p class="connection-type">{{ connectorName(connection.type) }}<span>·</span>{{ connection.id }}</p><dl><div><dt>{{ t('endpoint') }}</dt><dd :title="endpoint(connection)">{{ endpoint(connection) }}</dd></div><div><dt>{{ t('namespace') }}</dt><dd :title="namespace(connection)">{{ namespace(connection) }}</dd></div></dl><div class="connection-security"><Icon name="lock" :size="13" />{{ t('savedSecrets') }}<span>{{ connection.configuredSecrets?.length || 0 }}</span></div><div class="connection-actions"><button class="button secondary small" :disabled="!!busy" @click="testConnection(connection, 'SOURCE')">{{ t('testSource') }}</button><button class="button secondary small" :disabled="!!busy" @click="testConnection(connection, 'SINK')">{{ t('testSink') }}</button></div></article></div></section>
        <section v-if="view === 'audit'" class="panel"><div class="panel-header"><h2>{{ t('recentActivity') }}<span class="count-pill">{{ audits.length }}</span></h2><Icon name="shield" :size="19" /></div><div v-if="!audits.length" class="empty-state"><span class="empty-art"><Icon name="audit" :size="30" /></span><h3>{{ t('noActivity') }}</h3><p>{{ t('noActivitySub') }}</p></div><div v-else class="table-scroll"><table><thead><tr><th>{{ t('timestamp') }}</th><th>{{ t('action') }}</th><th>{{ t('resource') }}</th><th>{{ t('outcome') }}</th></tr></thead><tbody><tr v-for="(audit, index) in audits" :key="index"><td class="muted">{{ date(audit.timestamp) }}</td><td><strong>{{ audit.action }}</strong></td><td class="mono">{{ audit.resource }}</td><td><span class="badge neutral">{{ audit.result }}</span></td></tr></tbody></table></div></section>
        <footer class="page-footer"><span>CDC EX</span><span>{{ t('durabilityNote') }}</span></footer>
      </main>
    </div>
  </div>
  <Transition name="toast"><div v-if="toast" class="toast-message" role="status"><span class="toast-check"><Icon name="check" :size="15" /></span>{{ toast }}</div></Transition>
  <dialog ref="dialog" class="modal" :class="{ wide: ['task', 'details', 'batch', 'batch-result'].includes(modal) }" @cancel.prevent="!busy && closeModal()" @click="!busy && ($event.target === dialog) && closeModal()">
    <div v-if="modal" class="modal-content"><header class="modal-header"><div><span class="eyebrow">CDC EX / {{ t('workspace') }}</span><h2>{{ modal === 'connection' ? t(connectionDraft.editing ? 'editConnection' : 'createConnection') : modal === 'task' ? t('newTask') : modal === 'batch' ? t('batchTitle') : modal === 'bulk' ? t(bulkAction==='group'?'assignGroup':bulkAction==='start'?'batchStart':bulkAction==='pause'?'batchPause':'batchStop') : modal === 'batch-result' ? t('batchResult') : modal === 'details' ? currentTask?.name : t(modal === 'inspection' ? 'inspectResult' : modal === 'compare' ? 'compareTitle' : 'ddlTitle') }}</h2></div><button class="icon-button" :aria-label="t('close')" :disabled="!!busy" @click="closeModal"><Icon name="close" /></button></header>
      <div v-if="modalError" class="notice danger modal-notice" role="alert"><Icon name="warning" :size="18" /><span>{{ modalError }}</span></div>
      <form v-if="modal === 'connection'" @submit.prevent="saveConnection"><div class="modal-body"><div class="form-grid"><label class="field"><span>{{ t('name') }}</span><input v-model="connectionDraft.name" required maxlength="120" :placeholder="t('connectionNamePlaceholder')" @blur="suggestId('connection')" /></label><label class="field"><span>{{ t('id') }}</span><input v-model="connectionDraft.id" required pattern="[A-Za-z0-9_\-]{1,64}" maxlength="64" :disabled="connectionDraft.editing" /></label><label class="field span-2"><span>{{ t('connector') }}</span><select :aria-label="t('connector')" v-model="connectionDraft.type" :disabled="connectionDraft.editing" @change="changeConnector"><option v-for="type in Object.keys(connectors)" :key="type" :value="type">{{ connectorName(type) }}</option></select></label></div><div class="section-divider"><span>{{ t('settings') }}</span></div><div class="form-grid"><label v-for="field in fields" :key="field.key" class="field" :class="{ 'span-2': field.full }"><span>{{ t(field.label) }}<span v-if="field.secret" class="field-lock"><Icon name="lock" :size="12" /></span></span><select v-if="field.choices" :aria-label="t(field.label)" v-model="connectionDraft.options[field.key]"><option v-for="choice in field.choices" :key="choice" :value="choice">{{ choice }}</option></select><input v-else v-model="connectionDraft.options[field.key]" :type="field.secret ? 'password' : 'text'" :required="field.required && !(connectionDraft.type === 'MONGODB' && ['host', 'port'].includes(field.key) && (connectionDraft.options.uri || connectionDraft.savedSecrets.includes('uri')))" :placeholder="field.secret && connectionDraft.editing ? t('passwordSaved') : field.placeholder" :autocomplete="field.secret ? 'new-password' : 'off'" spellcheck="false" /></label></div><div v-if="['MONGODB', 'CLICKHOUSE'].includes(connectionDraft.type)" class="option-group"><label class="check-field"><input type="checkbox" :checked="boolOption('tls')" @change="setBool('tls', $event)" />{{ t('tls') }}</label><label v-if="connectionDraft.type === 'MONGODB'" class="check-field"><input type="checkbox" :checked="boolOption('directConnection')" @change="setBool('directConnection', $event)" />{{ t('directConnection') }}</label><label v-if="connectionDraft.type === 'CLICKHOUSE'" class="check-field"><input type="checkbox" :checked="boolOption('exclusiveWriter')" @change="setBool('exclusiveWriter', $event)" />{{ t('exclusiveWriter') }}</label></div><label class="check-field insecure-option"><input type="checkbox" :checked="boolOption('allowInsecure')" @change="setBool('allowInsecure', $event)" /><span>{{ t('allowInsecure') }}<small>{{ t('insecureHint') }}</small></span></label><details class="advanced-options"><summary>{{ t('advancedOptions') }}</summary><p>{{ t('advancedHint') }}</p><textarea v-model="connectionDraft.extra" rows="5" spellcheck="false" :aria-label="t('advancedOptions')"></textarea></details><p class="inline-hint"><Icon name="shield" :size="16" />{{ t('credentialsHint') }}</p></div><footer class="modal-footer"><span class="subtle-label">{{ t('connectionSavedHint') }}</span><button class="button secondary" type="button" :disabled="!!busy" @click="closeModal">{{ t('cancel') }}</button><button class="button primary" type="submit" :disabled="!!busy"><span v-if="busy === 'connection-save'" class="spinner"></span>{{ t(busy ? 'saving' : 'save') }}</button></footer></form>
      <div v-else-if="modal === 'task'"><ol class="wizard-steps"><li v-for="(key, index) in ['chooseEndpoints', 'mapFields', 'reviewTask']" :key="key" :class="{ current: wizardStep === index + 1, complete: wizardStep > index + 1 }"><span><Icon v-if="wizardStep > index + 1" name="check" :size="14" /><template v-else>{{ index + 1 }}</template></span>{{ t(key) }}</li></ol><div class="modal-body wizard-body">
        <div v-if="wizardStep === 1"><div v-if="connections.length < 2" class="notice warning"><Icon name="info" /><span>{{ t('missingEndpoints') }}</span></div><div class="form-grid"><label class="field"><span>{{ t('name') }}</span><input v-model="taskDraft.name" maxlength="120" :placeholder="t('namePlaceholder')" @blur="suggestId('task')" /></label><label class="field"><span>{{ t('id') }}</span><input v-model="taskDraft.id" pattern="[A-Za-z0-9_\-]{1,64}" maxlength="64" /></label><p class="field-hint span-2">{{ t('idHint') }}</p><label class="field span-2"><span>{{ t('taskGroup') }}</span><input v-model="taskDraft.group" maxlength="80"/></label></div><div class="endpoint-selection"><div class="endpoint-picker"><span class="picker-icon source-icon"><Icon name="database" /></span><label class="field"><span>{{ t('source') }}</span><select :aria-label="t('source')" v-model="taskDraft.sourceConnectionId"><option value="">{{ t('selectConnection') }}</option><option v-for="connection in connections" :key="connection.id" :value="connection.id">{{ connection.name }} · {{ connectorName(connection.type) }}</option></select></label><small v-if="sourceConnection">{{ namespace(sourceConnection) }}</small></div><span class="endpoint-arrow"><Icon name="arrow" :size="21" /></span><div class="endpoint-picker"><span class="picker-icon sink-icon"><Icon name="database" /></span><label class="field"><span>{{ t('destination') }}</span><select :aria-label="t('destination')" v-model="taskDraft.sinkConnectionId"><option value="">{{ t('selectConnection') }}</option><option v-for="connection in connections" :key="connection.id" :value="connection.id" :disabled="connection.id === taskDraft.sourceConnectionId">{{ connection.name }} · {{ connectorName(connection.type) }}</option></select></label><small v-if="sinkConnection">{{ namespace(sinkConnection) }}</small></div></div><label class="field"><span>{{ t('snapshotMode') }}</span><select :aria-label="t('snapshotMode')" v-model="taskDraft.snapshotMode"><option v-for="mode in sourceModes" :key="mode" :value="mode">{{ modeLabel(mode, sourceConnection?.type) }}</option></select></label><p class="field-hint">{{ t('modeHint') }}</p></div>
        <div v-if="wizardStep === 2"><div class="mapping-top"><p>{{ t('mappingHint') }}</p><button class="button secondary small" :disabled="!!busy" @click="discoverFields"><span v-if="busy === 'discover'" class="spinner dark"></span><Icon v-else name="database" :size="15" />{{ t('inspectFields') }}</button></div><p v-if="inspected.source?.fields['*']" class="inline-hint"><Icon name="info" :size="16" />{{ t('wildcardSchema') }}</p><div v-if="!mappingRows.length" class="mapping-empty">{{ t('mappingEmpty') }}</div><div v-else class="table-scroll mapping-table"><table><thead><tr><th>{{ t('sourceField') }}</th><th></th><th>{{ t('targetField') }}</th><th>{{ t('conversion') }}</th><th>{{ t('include') }}</th><th></th></tr></thead><tbody><tr v-for="(row, index) in mappingRows" :key="index" :class="{ excluded: row.excluded }"><td><div class="mapping-source"><input v-model="row.source" list="source-field-options" :aria-label="`${t('sourceField')} ${index + 1}`" /><span v-if="knownKeys.includes(row.source)" class="key-tag">{{ t('key') }}</span></div></td><td><Icon name="arrow" :size="14" /></td><td><input v-model="row.target" :disabled="row.excluded" :aria-label="`${t('targetField')} ${index + 1}`" /></td><td><select v-model="row.conversion" :disabled="row.excluded" :aria-label="`${t('conversion')} ${index + 1}`"><option value="">{{ t('preserveType') }}</option><option v-for="conversion in conversions" :key="conversion" :value="conversion">{{ conversion }}</option></select></td><td class="check-cell"><input type="checkbox" :checked="!row.excluded" :disabled="knownKeys.includes(row.source)" :aria-label="`${t('include')} ${row.source}`" @change="row.excluded = !($event.target as HTMLInputElement).checked" /></td><td><button class="icon-button" :aria-label="t('remove')" @click="mappingRows.splice(index, 1)"><Icon name="close" :size="15" /></button></td></tr></tbody></table></div><datalist id="source-field-options"><option v-for="field in schemaFields" :key="field" :value="field" /></datalist><button class="text-button add-mapping" @click="mappingRows.push({ source: '', target: '', conversion: '', excluded: false })"><Icon name="plus" :size="16" />{{ t('addRule') }}</button><div v-if="inspected.sink" class="target-fields"><strong>{{ t('sinkSchema') }}</strong><span v-for="(type, field) in inspected.sink.fields" :key="field"><b>{{ field }}</b>{{ type }}</span></div></div>
        <div v-if="wizardStep === 3"><div class="task-review"><span class="review-icon"><Icon name="flow" :size="25" /></span><h3>{{ taskDraft.name }}</h3><p>{{ taskDraft.id }}</p><div class="review-route"><div><small>{{ t('source') }}</small><strong>{{ sourceConnection?.name }}</strong><span>{{ connectorName(sourceConnection?.type || '') }}</span></div><Icon name="arrow" :size="24" /><div><small>{{ t('destination') }}</small><strong>{{ sinkConnection?.name }}</strong><span>{{ connectorName(sinkConnection?.type || '') }}</span></div></div><div class="review-badges"><span>{{ modeLabel(taskDraft.snapshotMode, sourceConnection?.type) }}</span><span>{{ mappingRows.length ? t('mapCount', { n: mappingRows.length }) : t('identityMapping') }}</span></div></div><details class="advanced-options"><summary>{{ t('resourceLimits') }}</summary><div class="form-grid resource-grid"><label class="field"><span>{{ t('journalMiB') }}</span><input v-model.number="taskDraft.journalMiB" type="number" min="1" max="1048576" /></label><label class="field"><span>{{ t('journalEvents') }}</span><input v-model.number="taskDraft.journalEvents" type="number" min="1" /></label><label class="field"><span>{{ t('batchSize') }}</span><input v-model.number="taskDraft.batchSize" type="number" min="1" max="10000" /></label><label class="field"><span>{{ t('maxFailures') }}</span><input v-model.number="taskDraft.maxFailures" type="number" min="1" max="1000" /></label></div></details><div class="notice information"><Icon name="info" /><span>{{ t('taskReviewHint') }}</span></div></div>
      </div><footer class="modal-footer"><span class="subtle-label">{{ t('step', { n: wizardStep }) }}</span><button class="button secondary" :disabled="!!busy" @click="wizardStep > 1 ? wizardStep-- : closeModal()">{{ t(wizardStep > 1 ? 'back' : 'cancel') }}</button><button v-if="wizardStep < 3" class="button primary" :disabled="!!busy || connections.length < 2" @click="nextStep">{{ t('next') }}<Icon name="arrow" :size="16" /></button><button v-else class="button primary" :disabled="!!busy" @click="createTask"><span v-if="busy" class="spinner"></span>{{ t('createTask') }}</button></footer></div>
      <BatchTaskForm v-else-if="modal==='batch'" :connections="connections" :connectors="connectors" :api="api" :t="t" :connector-name="connectorName" :mode-label="modeLabel" @busy="busy=$event?'batch':''" @complete="batchComplete" @cancel="closeModal"/>
      <div v-else-if="modal==='bulk'"><div class="modal-body"><p class="modal-description">{{ t('bulkConfirm',{n:selectedVisible.length}) }}</p><label v-if="bulkAction==='group'" class="field"><span>{{ t('taskGroup') }}</span><input v-model="groupDraft" list="existing-groups" maxlength="80"/><datalist id="existing-groups"><option v-for="group in groups" :key="group" :value="group"/></datalist></label><p class="field-hint">{{ t(bulkAction==='group'?'groupHint':'batchReadOnly') }}</p></div><footer class="modal-footer"><button class="button secondary" :disabled="!!busy" @click="closeModal">{{ t('cancel') }}</button><button class="button primary" :disabled="!!busy" @click="applyBulk">{{ t(bulkAction==='group'?'assignGroup':bulkAction==='start'?'batchStart':bulkAction==='pause'?'batchPause':'batchStop') }}</button></footer></div>
      <div v-else-if="modal==='batch-result'"><div class="modal-body"><div class="notice" :class="batchFailed?'warning':'success'"><Icon :name="batchFailed?'warning':'check'"/>{{ t('batchSummary',{ok:batchResults.length-batchFailed,failed:batchFailed}) }}</div><div class="table-scroll batch-results"><table><thead><tr><th>{{ t('id') }}</th><th>{{ t('outcome') }}</th><th>{{ t('details') }}</th></tr></thead><tbody><tr v-for="row in batchResults" :key="row.id"><td class="mono">{{ row.id }}</td><td><span class="badge" :class="row.outcome==='FAILED'?'danger':'success'">{{ outcomeLabel(row.outcome) }}</span></td><td>{{ row.code||'—' }}</td></tr></tbody></table></div></div><footer class="modal-footer"><button class="button primary" @click="closeModal">{{ t('close') }}</button></footer></div>
      <div v-else-if="modal === 'details' && currentTask"><div class="modal-body"><div class="detail-heading"><span class="badge" :class="statusClass(state(currentTask))"><span></span>{{ statusLabel(state(currentTask)) }}</span><span class="mono muted">{{ currentTask.id }}</span></div><div v-if="currentTask.error || currentTask.health?.errorCode" class="notice danger"><Icon name="warning" /><div><strong>{{ t('taskError') }}</strong><p>{{ currentTask.error || currentTask.health?.errorCode }}</p></div></div><div class="detail-metrics"><div><span>{{ t('pending') }}</span><strong>{{ count(currentTask,'pending') }}</strong></div><div><span>{{ t('delivered') }}</span><strong>{{ count(currentTask,'delivered') }}</strong></div><div><span>{{ t('retries') }}</span><strong>{{ currentTask.health?.retries || 0 }}</strong></div><div><span>{{ t('journalPayload') }}</span><strong>{{ size(currentTask.health?.journal?.bytes || 0) }}</strong></div></div><div class="detail-metrics runtime-rates"><div><span>{{ t('captureRate') }}</span><strong>{{ rate(currentTask.health?.capturedPerSecond) }}</strong></div><div><span>{{ t('deliveryRate') }}</span><strong>{{ rate(currentTask.health?.deliveredPerSecond) }}</strong></div><div :title="t('delayHint')"><span>{{ t('deliveryDelay') }}</span><strong>{{ duration(currentTask.health?.lastDeliveryDelayMillis) }}</strong></div><div><span>{{ t('sampleWindow') }}</span><strong>{{ duration(currentTask.health?.rateWindowMillis) }}</strong></div></div><p class="field-hint">{{ t('delayHint') }}</p><dl class="detail-list"><div><dt>{{ t('taskGroup') }}</dt><dd>{{ currentTask.group||t('ungrouped') }}</dd></div><div><dt>{{ t('source') }}</dt><dd>{{ taskEndpoint(currentTask,'source') }}</dd></div><div><dt>{{ t('destination') }}</dt><dd>{{ taskEndpoint(currentTask,'sink') }}</dd></div><div><dt>{{ t('snapshotMode') }}</dt><dd>{{ modeLabel(currentTask.snapshotMode, connectionMap[currentTask.sourceConnectionId]?.type) }}</dd></div><div><dt>{{ t('lastSource') }}</dt><dd>{{ relative(currentTask.health?.sourceTimeMillis) }}</dd></div><div><dt>{{ t('lastProgress') }}</dt><dd>{{ relative(currentTask.health?.lastProgressMillis) }}</dd></div></dl><div class="detail-tools"><button class="button secondary small" :disabled="!!busy || active(currentTask)" @click="taskAction(currentTask, 'validate')"><Icon name="shield" :size="15" />{{ t('validate') }}</button><button class="button secondary small" :disabled="!!busy || active(currentTask)" @click="openCompare(currentTask)"><Icon name="compare" :size="15" />{{ t('compare') }}</button><button class="button secondary small" :disabled="!!busy" @click="openDdl(currentTask)"><Icon name="code" :size="15" />{{ t('ddl') }}</button></div><p v-if="active(currentTask)" class="field-hint">{{ t('runningTaskHint') }}</p><div v-if="inspected.source || inspected.sink" class="schema-pair"><section v-for="role in ['source', 'sink'] as const" :key="role"><h3>{{ t(role === 'source' ? 'sourceSchema' : 'sinkSchema') }}</h3><div v-for="(type, field) in inspected[role]?.fields || {}" :key="field" class="schema-row"><span>{{ field }}<small v-if="inspected[role]?.keyFields.includes(String(field))" class="key-tag">{{ t('key') }}</small></span><code>{{ type }}</code></div></section></div><details class="advanced-options"><summary>{{ t('preview') }}</summary><pre>{{ resultJson(currentTask.mapping) }}</pre></details></div><footer class="modal-footer"><span class="subtle-label">{{ t('stateNote') }}</span><button v-if="active(currentTask)" class="button secondary" :disabled="!!busy || ['PAUSING', 'STOPPING'].includes(state(currentTask))" @click="taskAction(currentTask, 'stop')"><Icon name="stop" :size="15" />{{ t('stop') }}</button><button class="button primary" :disabled="!!busy || ['PAUSING', 'STOPPING'].includes(state(currentTask))" @click="taskAction(currentTask, active(currentTask) ? 'pause' : 'start')"><Icon :name="active(currentTask) ? 'pause' : 'play'" :size="15" />{{ t(active(currentTask) ? 'pause' : 'start') }}</button></footer></div>
      <div v-else-if="modal === 'inspection' && inspectionResult"><div class="modal-body"><div class="notice success"><Icon name="check" /><span>{{ t('testPassed') }}</span></div><div class="schema-row" v-for="(type, field) in inspectionResult.fields" :key="field"><span>{{ field }}<small v-if="inspectionResult.keyFields.includes(String(field))" class="key-tag">{{ t('key') }}</small></span><code>{{ type }}</code></div><details class="advanced-options"><summary>{{ t('details') }}</summary><pre>{{ resultJson(inspectionResult.details) }}</pre></details></div><footer class="modal-footer"><button class="button primary" @click="closeModal">{{ t('close') }}</button></footer></div>
      <div v-else-if="modal === 'compare'"><div class="modal-body"><p class="modal-description">{{ t('compareHint') }}</p><label class="check-field comparison-confirm"><input v-model="comparison.sourceQuiescent" type="checkbox" />{{ t('sourceQuiescent') }}</label><label class="field"><span>{{ t('maxRows') }}</span><input v-model.number="comparison.maxRows" type="number" min="1" max="100000" /></label><section v-if="comparisonResult" class="comparison-result"><h3>{{ t('comparisonResult') }}</h3><template v-if="comparisonSummary"><div class="comparison-verdict" :class="comparisonSummary.complete ? (comparisonSummary.consistent ? 'match' : 'mismatch') : 'partial'" role="status"><Icon :name="comparisonSummary.consistent ? 'check' : 'warning'" :size="21" /><div><strong>{{ t(comparisonSummary.complete ? (comparisonSummary.consistent ? 'comparisonMatch' : 'comparisonMismatch') : 'comparisonIncomplete') }}</strong><p>{{ t(comparisonSummary.complete ? 'comparisonScope' : 'comparisonPartialHint') }}</p></div></div><div class="comparison-counts"><div><span>{{ t('sourceRows') }}</span><strong data-testid="comparison-source-count">{{ comparisonSummary.sourceCount === undefined ? '—' : number(comparisonSummary.sourceCount) }}</strong></div><div><span>{{ t('targetRows') }}</span><strong data-testid="comparison-target-count">{{ comparisonSummary.targetCount === undefined ? '—' : number(comparisonSummary.targetCount) }}</strong></div></div><details class="advanced-options comparison-raw"><summary>{{ t('comparisonRaw') }}</summary><pre>{{ resultJson(comparisonResult) }}</pre></details></template><pre v-else>{{ resultJson(comparisonResult) }}</pre></section></div><footer class="modal-footer"><button class="button secondary" :disabled="!!busy" @click="closeModal">{{ t('close') }}</button><button class="button primary" :disabled="!!busy || !comparison.sourceQuiescent" @click="compareTask"><span v-if="busy" class="spinner"></span>{{ t('runCompare') }}</button></footer></div>
      <div v-else-if="modal === 'ddl'"><div class="modal-body"><p class="modal-description">{{ t('ddlHint') }}</p><pre class="ddl-preview">{{ ddlText }}</pre></div><footer class="modal-footer"><button class="button secondary" @click="closeModal">{{ t('close') }}</button><button class="button primary" @click="copyDdl"><Icon :name="copyDone ? 'check' : 'copy'" :size="16" />{{ t(copyDone ? 'copied' : 'copy') }}</button></footer></div>
    </div>
  </dialog>
</template>
