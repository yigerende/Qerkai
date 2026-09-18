<script setup lang="ts">
import { computed, defineAsyncComponent, onMounted, onUnmounted, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import Icon from '@/components/icons/Icon.vue'
import { stateKeeperAPI, type StateKeeperDetail, type StateKeeperFileDetail, type StateKeeperSettings, type StateKeeperSnapshot } from '@/api/admin/openaiStateKeeper'
import { useClipboard } from '@/composables/useClipboard'
import * as accountsAPI from '@/api/admin/accounts'
import * as groupsAPI from '@/api/admin/groups'
import * as proxiesAPI from '@/api/admin/proxies'
import type { AccountListItem, AdminGroup, Proxy, CreateProxyRequest } from '@/types'

const ImportAccounts = defineAsyncComponent(() => import('@/components/admin/account/ImportDataModal.vue'))
const CreateAccount = defineAsyncComponent(() => import('@/components/account/CreateAccountModal.vue'))
const form = ref<StateKeeperSettings | null>(null)
const allowedLengthsInput = ref('')
const degradedLengthsInput = ref('')
const modelsInput = ref('')
const selectedProxyIDs = ref<number[]>([])
const proxyIDs = (settings: StateKeeperSettings) => settings.proxy_ids ?? (settings.proxy_id ? [settings.proxy_id] : [])
const defaultModels = ['gpt-5.5', 'gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-6-astra']
const modelLines = (settings: StateKeeperSettings) => (settings.models ?? (settings.model ? [settings.model] : defaultModels)).join('\n')
const snapshot = ref<StateKeeperSnapshot | null>(null)
const accounts = ref<AccountListItem[]>([])
const groups = ref<AdminGroup[]>([])
const proxies = ref<Proxy[]>([])
const busy = ref(false)
const loading = ref(false)
const message = ref('')
const messageError = ref(false)
const loadError = ref('')
const search = ref('')
const accountGroupID = ref<number | ''>('')
const activeTab = ref<'settings' | 'states' | 'history'>('settings')
const expandedAccounts = ref(new Set<number>())
const showImport = ref(false)
const showCreate = ref(false)
const showProxy = ref(false)
const detailAccountID = ref<number | null>(null)
const detailMode = ref<'header' | 'file'>('header')
const detailModel = ref('')
const detail = ref<StateKeeperDetail | null>(null)
const fileDetail = ref<StateKeeperFileDetail | null>(null)
const detailLoading = ref(false)
const detailError = ref('')
const detailHeader = computed(() => detail.value ? `${detail.value.header_name}: ${detail.value.header_value}` : '')
const fileContent = computed(() => fileDetail.value ? JSON.stringify(fileDetail.value.content, null, 2) : '')
const { copied, copyToClipboard } = useClipboard()
let detailRequest = 0
const proxyBusy = ref(false)
const newProxy = ref<CreateProxyRequest>({ name: '', protocol: 'http', host: '', port: 8080, username: '', password: '' })
const dirty = computed(() => !!form.value && !!snapshot.value && (
  JSON.stringify(form.value) !== JSON.stringify(snapshot.value.settings) ||
  allowedLengthsInput.value !== (snapshot.value.settings.allowed_state_lengths || []).join(',') ||
  degradedLengthsInput.value !== (snapshot.value.settings.degraded_state_lengths || []).join(',') ||
  modelsInput.value !== modelLines(snapshot.value.settings) ||
  JSON.stringify(selectedProxyIDs.value) !== JSON.stringify(proxyIDs(snapshot.value.settings))
))
const accountChoices = computed(() => {
  const choices = new Map(accounts.value.map(a => [a.id, { id: a.id, name: a.name, group_ids: a.group_ids }]))
  for (const row of snapshot.value?.rows || []) {
    const existing = choices.get(row.account_id)
    choices.set(row.account_id, { id: row.account_id, name: row.account_name || existing?.name || `账号 #${row.account_id}`, group_ids: row.account_group_ids || existing?.group_ids })
  }
  return [...choices.values()]
})
const visibleAccounts = computed(() => accountChoices.value.filter(a =>
  (accountGroupID.value === '' || a.group_ids?.includes(accountGroupID.value)) &&
  `${a.id} ${a.name}`.toLowerCase().includes(search.value.toLowerCase())
))
const groupSelectedAccounts = computed(() => {
  const selected = new Set(form.value?.collection_group_ids || [])
  return new Set(accountChoices.value.filter(a => a.group_ids?.some(id => selected.has(id))).map(a => a.id))
})
const selectedAccounts = computed(() => new Set([...(form.value?.account_ids || []), ...groupSelectedAccounts.value]))
const selectableAccounts = computed(() => visibleAccounts.value.filter(a => !groupSelectedAccounts.value.has(a.id)))
const visibleSelectedCount = computed(() => {
  return visibleAccounts.value.filter(account => selectedAccounts.value.has(account.id)).length
})
const allVisibleSelected = computed(() => visibleAccounts.value.length > 0 && visibleSelectedCount.value === visibleAccounts.value.length)
function selectVisibleAccounts(event: Event) {
  if (!form.value) return
  const visible = new Set(selectableAccounts.value.map(account => account.id))
  form.value.account_ids = (event.target as HTMLInputElement).checked
    ? [...new Set([...form.value.account_ids, ...visible])]
    : form.value.account_ids.filter(id => !visible.has(id))
}
function selectAccount(id: number, event: Event) {
  if (!form.value || groupSelectedAccounts.value.has(id)) return
  form.value.account_ids = (event.target as HTMLInputElement).checked
    ? [...new Set([...form.value.account_ids, id])]
    : form.value.account_ids.filter(value => value !== id)
}
const readyCount = computed(() => snapshot.value?.rows.filter(r => (r.models || [r]).every(m => m.state_file_saved)).length || 0)
const injectionCount = computed(() => snapshot.value?.rows.reduce((sum, r) => sum + r.injections, 0) || 0)
const collectingCount = computed(() => snapshot.value?.rows.filter(r => r.collecting || r.queued).length || 0)
const accountStates = computed(() => (snapshot.value?.rows || []).map(account => {
  const models = account.models || [account]
  const collectionTimes = models.map(row => row.last_collection_at || row.collected_at).filter((value): value is string => !!value)
  return {
    ...account,
    models,
    savedCount: models.filter(row => row.state_file_saved).length,
    pausedCount: models.filter(row => !account.account_unavailable && !row.account_unavailable && row.paused && !row.queued && !row.collecting).length,
    activeCount: models.filter(row => row.queued || row.collecting).length,
    lastCollection: collectionTimes.sort().at(-1),
    httpStatuses: [...new Set(models.map(row => row.http_status).filter(Boolean))].join(' / ') || '—',
    stateLengths: [...new Set(models.map(row => row.turn_state_length).filter(Boolean))].join(' / ') || '—',
  }
}))
const pausedCount = computed(() => accountStates.value.reduce((sum, account) => sum + account.pausedCount, 0))
function toggleAccount(id: number) {
  if (expandedAccounts.value.has(id)) expandedAccounts.value.delete(id)
  else expandedAccounts.value.add(id)
}
const accountName = (id: number) => snapshot.value?.rows.find(a => a.account_id === id)?.account_name || accounts.value.find(a => a.id === id)?.name || `账号 #${id}`
const proxyName = (id: number) => proxies.value.find(p => p.id === id)?.name || `代理 #${id}`
function moveProxy(index: number, offset: number) {
  const target = index + offset
  if (target < 0 || target >= selectedProxyIDs.value.length) return
  const ids = [...selectedProxyIDs.value]
  ;[ids[index], ids[target]] = [ids[target], ids[index]]
  selectedProxyIDs.value = ids
}
const date = (value?: string) => value ? new Date(value).toLocaleString() : '—'
const errorText = (error: unknown) => {
  const e = error as { response?: { data?: { message?: string } }; message?: string }
  return e.response?.data?.message || e.message || '请求失败，请稍后重试'
}
const statusText: Record<string, string> = { empty: '尚未采集', ready: '已采集', filtered: '长度不符，未保存', refresh_failed: '刷新失败，保留上次 State', not_observed: '未取得目标状态', upstream_error: '上游错误', failed: '采集失败' }
function notify(text: string, error = false) { message.value = text; messageError.value = error }

async function load(reset = false) {
  if (loading.value) return
  loading.value = true
  try {
    const next = await stateKeeperAPI.get()
    const preserveDraft = dirty.value
    const nextProxyIDs = new Set(proxyIDs(next.settings))
    const removedProxyIDs = new Set(snapshot.value ? proxyIDs(snapshot.value.settings).filter(id => !nextProxyIDs.has(id)) : [])
    const nextAccountIDs = new Set(next.settings.account_ids)
    const removedAccountIDs = new Set(snapshot.value ? snapshot.value.settings.account_ids.filter(id => !nextAccountIDs.has(id)) : [])
    snapshot.value = next
    const rowIDs = new Set(next.rows.map(row => row.account_id))
    for (const id of expandedAccounts.value) if (!rowIDs.has(id)) expandedAccounts.value.delete(id)
    if (reset || !form.value || !preserveDraft) {
      form.value = structuredClone(next.settings)
      allowedLengthsInput.value = (next.settings.allowed_state_lengths || []).join(',')
      degradedLengthsInput.value = (next.settings.degraded_state_lengths || []).join(',')
      modelsInput.value = modelLines(next.settings)
      selectedProxyIDs.value = [...proxyIDs(next.settings)]
    } else {
      selectedProxyIDs.value = selectedProxyIDs.value.filter(id => !removedProxyIDs.has(id))
      form.value.account_ids = form.value.account_ids.filter(id => !removedAccountIDs.has(id))
    }
    loadError.value = ''
  } catch (e) { loadError.value = errorText(e) }
  finally { loading.value = false }
}

async function loadChoices() {
  const [accountResult, groupResult, proxyResult] = await Promise.allSettled([
    (async () => {
      const items: AccountListItem[] = []
      for (let page = 1; page <= 5; page++) {
        const result = await accountsAPI.list(page, 100, { platform: 'openai', type: 'oauth', lite: 'true' })
        items.push(...result.items)
        if (items.length >= result.total || !result.items.length) break
      }
      return items
    })(), groupsAPI.getAll(), proxiesAPI.getAll(),
  ])
  if (accountResult.status === 'fulfilled') accounts.value = accountResult.value
  if (groupResult.status === 'fulfilled') groups.value = groupResult.value
  if (proxyResult.status === 'fulfilled') proxies.value = proxyResult.value
  for (const result of [accountResult, groupResult, proxyResult]) {
    if (result.status === 'rejected') notify(`选项加载失败：${errorText(result.reason)}`, true)
  }
}

async function save() {
  if (!form.value) return
  busy.value = true
  try {
    const parts = allowedLengthsInput.value.split(/[,，]/).map(value => value.trim()).filter(Boolean)
    if (parts.some(value => !/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 16384)) {
      throw new Error('响应头长度须为 1–16384 的整数，用逗号分隔')
    }
    const lengths = [...new Set(parts.map(Number))]
    if (lengths.length > 100) throw new Error('允许保存的响应头长度最多填写 100 个')
    const degradedParts = degradedLengthsInput.value.split(/[,，]/).map(value => value.trim()).filter(Boolean)
    if (degradedParts.some(value => !/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 16384)) throw new Error('降智响应头长度须为 1–16384 的整数，用逗号分隔')
    const degraded = [...new Set(degradedParts.map(Number))]
    if (degraded.length > 100) throw new Error('降智响应头长度最多填写 100 个')
    if (degraded.some(value => lengths.includes(value))) throw new Error('允许保存与降智响应头长度不能重叠')
    const models = [...new Set(modelsInput.value.split(/\r?\n/).map(value => value.trim()).filter(Boolean))]
    if (!models.length || models.length > 20 || models.some(model => model.length > 200 || /\s/.test(model))) throw new Error('请填写 1–20 个模型，每行一个，模型名称不能包含空白')
    if (selectedProxyIDs.value.length > 20) throw new Error('最多选择 20 个采集代理')
    const data = await stateKeeperAPI.save({ ...form.value, proxy_ids: [...selectedProxyIDs.value], proxy_id: selectedProxyIDs.value[0] || 0, model: models[0], models, allowed_state_lengths: lengths, degraded_state_lengths: degraded })
    snapshot.value = data; form.value = structuredClone(data.settings)
    allowedLengthsInput.value = (data.settings.allowed_state_lengths || []).join(',')
    degradedLengthsInput.value = (data.settings.degraded_state_lengths || []).join(',')
    modelsInput.value = modelLines(data.settings)
    selectedProxyIDs.value = [...proxyIDs(data.settings)]
    notify('配置已保存')
  } catch (e) { notify(errorText(e), true) }
  finally { busy.value = false }
}

async function collect(id?: number, model?: string) {
  if (dirty.value) { notify('请先保存配置，再开始采集', true); return }
  busy.value = true
  try {
    await stateKeeperAPI.collect(id ? [id] : [], model)
    notify('采集已排队，可在状态列表查看结果'); activeTab.value = 'states'; await load()
  } catch (e) { notify(errorText(e), true) }
  finally { busy.value = false }
}

async function collectPaused() {
  if (busy.value || !pausedCount.value || !snapshot.value?.settings.enabled) return
  if (dirty.value) { notify('请先保存配置，再开始采集', true); return }
  busy.value = true
  try {
    const result = await stateKeeperAPI.collectPaused()
    notify(result.scheduled_count ? `已排队 ${result.scheduled_count} 个待人工账号模型` : '当前没有需要人工重试的账号模型')
    await load()
  } catch (e) { notify(errorText(e), true) }
  finally { busy.value = false }
}

async function openDetail(id: number, mode: 'header' | 'file' = 'header', model = '') {
  const request = ++detailRequest
  detailAccountID.value = id
  detailMode.value = mode
  detailModel.value = model
  detail.value = null
  fileDetail.value = null
  detailError.value = ''
  detailLoading.value = true
  copied.value = false
  try {
    if (mode === 'file') {
      const data = await stateKeeperAPI.fileDetail(id, model)
      if (request === detailRequest) fileDetail.value = data
    } else {
      const data = await stateKeeperAPI.detail(id, model)
      if (request === detailRequest) detail.value = data
    }
  } catch (e) {
    if (request === detailRequest) detailError.value = errorText(e)
  } finally {
    if (request === detailRequest) detailLoading.value = false
  }
}

function closeDetail() {
  detailRequest++
  detailAccountID.value = null
  detail.value = null
  fileDetail.value = null
  detailError.value = ''
  detailLoading.value = false
}

async function createProxy() {
  if (!newProxy.value.name.trim() || !newProxy.value.host.trim() || !newProxy.value.port) {
    notify('请填写代理名称、主机和端口', true); return
  }
  proxyBusy.value = true
  try {
    const proxy = await proxiesAPI.create(newProxy.value)
    await loadChoices()
    selectedProxyIDs.value = [...selectedProxyIDs.value, proxy.id]
    newProxy.value = { name: '', protocol: 'http', host: '', port: 8080, username: '', password: '' }
    showProxy.value = false
    notify('代理已添加并选中，请保存采集配置')
  } catch (e) { notify(errorText(e), true) }
  finally { proxyBusy.value = false }
}

let timer: ReturnType<typeof setTimeout> | undefined
let disposed = false
async function poll() {
  await load()
  if (!disposed) timer = setTimeout(() => void poll(), 5000)
}
onMounted(() => { void poll(); void loadChoices() })
onUnmounted(() => { disposed = true; if (timer) clearTimeout(timer); closeDetail() })
</script>

<template>
  <AppLayout>
    <div class="space-y-5 pb-8">
      <header class="flex flex-wrap items-center justify-between gap-4 border-b border-gray-200 pb-5 dark:border-dark-700">
        <div class="flex items-center gap-3">
          <span class="flex h-10 w-10 items-center justify-center rounded-xl bg-primary-50 text-primary-600 dark:bg-primary-900/20"><Icon name="sync" size="md" /></span>
          <div><h1 class="text-xl font-semibold text-gray-900 dark:text-white">上游状态管理</h1><p class="mt-1 text-sm text-gray-500">Turn-State 采集与注入</p></div>
        </div>
        <button class="btn btn-secondary" :disabled="loading" @click="load(); loadChoices()"><Icon name="refresh" size="sm" />刷新</button>
      </header>

      <p v-if="loadError || snapshot?.config_error" role="alert" class="rounded-xl bg-red-50 px-4 py-3 text-sm text-red-700 dark:bg-red-950/30 dark:text-red-300">{{ loadError || snapshot?.config_error }}</p>
      <p v-if="message" role="status" class="rounded-xl px-4 py-3 text-sm" :class="messageError ? 'bg-red-50 text-red-700 dark:bg-red-950/30 dark:text-red-300' : 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/30 dark:text-emerald-300'">{{ message }}</p>

      <div class="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <article class="state-card"><span class="text-sm text-gray-500">采集开关</span><strong class="mt-2 block text-xl">{{ snapshot?.settings.enabled ? '已开启' : '已关闭' }}</strong></article>
        <article class="state-card"><span class="text-sm text-gray-500">全部模型已采集 / 选中账号</span><strong class="mt-2 block text-xl">{{ readyCount }} / {{ snapshot?.rows.length || 0 }}</strong></article>
        <article class="state-card"><span class="text-sm text-gray-500">排队与采集中</span><strong class="mt-2 block text-xl">{{ collectingCount }}</strong></article>
        <article class="state-card"><span class="text-sm text-gray-500">已注入请求次数</span><strong class="mt-2 block text-xl">{{ injectionCount }}</strong></article>
      </div>

      <nav class="flex gap-1 border-b border-gray-200 dark:border-dark-700" aria-label="状态管理页面">
        <button v-for="tab in ([['settings', '采集配置'], ['states', '账号状态'], ['history', '采集记录']] as const)" :key="tab[0]" class="border-b-2 px-4 py-3 text-sm font-medium" :class="activeTab === tab[0] ? 'border-primary-500 text-primary-600' : 'border-transparent text-gray-500'" @click="activeTab = tab[0]">{{ tab[1] }}</button>
      </nav>

      <div v-if="form && activeTab === 'settings'" class="space-y-5">
        <section class="state-card space-y-4">
          <div class="flex flex-wrap gap-x-8 gap-y-3">
            <label class="state-check"><input v-model="form.enabled" type="checkbox">启用 State 采集</label>
            <label class="state-check"><input v-model="form.auto_refresh" type="checkbox">定时自动采集</label>
            <label class="state-check"><input v-model="form.degradation_scan_enabled" type="checkbox">定时降智扫描</label>
            <label class="state-check"><input v-model="form.injection_enabled" type="checkbox">启用请求注入</label>
            <label class="state-check"><input v-model="form.response_refresh_enabled" type="checkbox" aria-label="请求响应触发采集">请求响应触发采集</label>
          </div>
          <div class="grid gap-4 md:grid-cols-3">
            <label class="state-label">采集总并发数<input v-model.number="form.concurrency" class="input" type="number" min="1" max="500" step="1"></label>
            <label class="state-label">单账号采集并发数<input v-model.number="form.account_concurrency" class="input" type="number" min="1" max="100" step="1"></label>
            <label class="state-label">每轮最多请求次数（含首次）<input v-model.number="form.max_attempts" class="input" type="number" min="1" max="100" step="1"></label>
            <label class="state-label">失败后重试轮数<input v-model.number="form.retry_count" class="input" type="number" min="0" max="100" step="1" aria-label="失败后重试轮数"></label>
            <label class="state-label">重试间隔（s）<input v-model.number="form.retry_interval_seconds" class="input" type="number" min="1" max="86400" step="1" aria-label="重试间隔（s）"></label>
          </div>
          <label class="state-label max-w-md">允许保存的响应头长度<input v-model="allowedLengthsInput" class="input" type="text" maxlength="2000" placeholder="292,332；留空不限制" aria-label="允许保存的响应头长度"></label>
          <label class="state-label max-w-md">降智响应头长度<input v-model="degradedLengthsInput" class="input" type="text" maxlength="2000" placeholder="356" aria-label="降智响应头长度"></label>
          <div v-if="form.auto_refresh" class="space-y-2">
            <label class="state-label max-w-xs">自动采集间隔（s）<input v-model.number="form.auto_collect_interval_seconds" class="input" type="number" min="0" max="86400" step="1" placeholder="输入秒数"></label>
            <p class="text-xs text-gray-500">0 表示不定时采集。</p>
          </div>
          <label v-if="form.degradation_scan_enabled" class="state-label max-w-xs">降智扫描间隔（s）<input v-model.number="form.degradation_scan_interval_seconds" class="input" type="number" min="1" max="86400" step="1" aria-label="降智扫描间隔（s）"></label>
          <dl class="text-sm text-gray-500"><div><dt class="inline">响应头：</dt><dd class="inline"><code>x-codex-turn-state</code></dd></div></dl>
        </section>

        <section class="state-card space-y-4">
          <h2 class="font-semibold">采集出口与模型</h2>
          <div class="grid gap-4 md:grid-cols-2">
            <div class="min-w-0 space-y-3">
              <h3 class="text-sm font-medium">采集代理</h3>
              <div class="max-h-48 space-y-2 overflow-y-auto border-b border-gray-200 pb-3 dark:border-dark-600">
                <label v-for="p in proxies" :key="p.id" class="state-check w-full"><input v-model="selectedProxyIDs" class="shrink-0" type="checkbox" :value="p.id"><span class="min-w-0 break-all">{{ p.name }} <span class="text-xs text-gray-400">{{ p.host }}:{{ p.port }}</span></span></label>
                <p v-if="!proxies.length" class="text-sm text-gray-400">暂无代理</p>
              </div>
              <h3 class="text-sm font-medium">代理优先级</h3>
              <ol class="divide-y divide-gray-100 dark:divide-dark-700" aria-label="代理优先级">
                <li v-for="(id, index) in selectedProxyIDs" :key="id" class="flex min-h-10 items-center gap-2 py-1">
                  <span class="w-6 shrink-0 text-center text-xs tabular-nums text-gray-400">{{ index + 1 }}</span>
                  <span class="min-w-0 flex-1 break-words text-sm">{{ proxyName(id) }}</span>
                  <button type="button" class="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded hover:bg-gray-100 disabled:opacity-30 dark:hover:bg-dark-700" :disabled="index === 0" title="提高优先级" :aria-label="`提高 ${proxyName(id)} 优先级`" @click="moveProxy(index, -1)"><Icon name="arrowUp" size="sm" /></button>
                  <button type="button" class="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded hover:bg-gray-100 disabled:opacity-30 dark:hover:bg-dark-700" :disabled="index === selectedProxyIDs.length - 1" title="降低优先级" :aria-label="`降低 ${proxyName(id)} 优先级`" @click="moveProxy(index, 1)"><Icon name="arrowDown" size="sm" /></button>
                </li>
              </ol>
              <p v-if="!selectedProxyIDs.length" class="text-sm text-gray-400">尚未选择采集代理</p>
            </div>
            <label class="state-label">采集模型（每行一个）<textarea v-model="modelsInput" class="input min-h-32 resize-y font-mono text-sm" rows="4" maxlength="4020" :placeholder="defaultModels.join('\n')" aria-label="采集模型（每行一个）"></textarea></label>
          </div>
          <p class="text-xs text-gray-500">本地采集入口：<code>{{ snapshot?.collection_path || '服务更新中' }}</code></p>
          <p class="text-xs text-gray-500">上游地址：<code class="break-all">{{ snapshot?.collection_endpoint || '服务更新中' }}</code></p>
          <p class="text-xs text-gray-500">通过本地 Chat Completions 兼容逻辑发送 hi，固定所选 OAuth 账号和采集代理，检查原始上游状态码与响应头。</p>
          <button class="btn btn-secondary" @click="showProxy = !showProxy"><Icon name="plus" size="sm" />{{ showProxy ? '收起代理配置' : '添加住宅代理' }}</button>
          <div v-if="showProxy" class="grid gap-3 rounded-lg bg-gray-50 p-4 dark:bg-dark-900 md:grid-cols-3">
            <label class="state-label">名称<input v-model="newProxy.name" class="input" placeholder="住宅采集出口"></label>
            <label class="state-label">协议<select v-model="newProxy.protocol" class="input"><option value="http">HTTP</option><option value="https">HTTPS</option><option value="socks5">SOCKS5</option></select></label>
            <label class="state-label">主机<input v-model="newProxy.host" class="input" placeholder="IP 或域名"></label>
            <label class="state-label">端口<input v-model.number="newProxy.port" class="input" type="number" min="1" max="65535"></label>
            <label class="state-label">用户名<input v-model="newProxy.username" class="input" autocomplete="off"></label>
            <label class="state-label">密码<input v-model="newProxy.password" class="input" type="password" autocomplete="new-password"></label>
            <button class="btn btn-primary justify-self-start" :disabled="proxyBusy" @click="createProxy">{{ proxyBusy ? '添加中…' : '添加并选中' }}</button>
          </div>
        </section>

        <section class="state-card space-y-4">
          <div class="flex flex-wrap items-center justify-between gap-3"><h2 class="font-semibold">采集账号 <span class="font-normal text-gray-500">{{ selectedAccounts.size }} 个</span></h2><div class="flex gap-2"><button class="btn btn-secondary" @click="showCreate = true">添加账号</button><button class="btn btn-secondary" @click="showImport = true">导入账号</button></div></div>
          <div class="space-y-3 border-b border-gray-200 pb-4 dark:border-dark-600">
            <h3 class="text-sm font-medium">采集分组 <span class="font-normal text-gray-500">自动纳入</span></h3>
            <div class="flex flex-wrap gap-4">
              <label v-for="g in groups" :key="g.id" class="state-check min-w-0"><input v-model="form.collection_group_ids" type="checkbox" :value="g.id" :aria-label="`采集分组 ${g.name}`"><span class="break-all">{{ g.name }}</span></label>
              <span v-if="!groups.length" class="text-sm text-gray-500">暂无可选分组</span>
            </div>
          </div>
          <div class="flex flex-wrap items-center gap-3">
            <input v-model="search" class="input max-w-md" placeholder="搜索账号名称或 ID" aria-label="搜索采集账号">
            <select v-model="accountGroupID" class="input max-w-xs" aria-label="按分组筛选采集账号"><option value="">全部分组</option><option v-for="g in groups" :key="g.id" :value="g.id">{{ g.name }}</option></select>
            <label class="state-check shrink-0"><input type="checkbox" aria-label="全选采集账号" :checked="allVisibleSelected" :indeterminate="visibleSelectedCount > 0 && !allVisibleSelected" :disabled="busy || !selectableAccounts.length" @change="selectVisibleAccounts">{{ search ? '全选搜索结果' : accountGroupID !== '' ? '全选当前分组' : '全选' }}</label>
          </div>
          <div class="grid max-h-64 grid-cols-1 gap-2 overflow-y-auto md:grid-cols-2 xl:grid-cols-3">
            <label v-for="a in visibleAccounts" :key="a.id" class="state-check min-w-0 rounded-lg border border-gray-200 p-3 dark:border-dark-600"><input type="checkbox" :value="a.id" :checked="selectedAccounts.has(a.id)" :disabled="groupSelectedAccounts.has(a.id)" :aria-label="`采集账号 ${a.name}`" @change="selectAccount(a.id, $event)"><span class="min-w-0 flex-1 truncate" :title="`${a.name} #${a.id}`">{{ a.name }} <span class="text-gray-400">#{{ a.id }}</span></span><span v-if="groupSelectedAccounts.has(a.id)" class="shrink-0 text-xs text-primary-600">分组</span></label>
            <p v-if="!visibleAccounts.length" class="py-5 text-sm text-gray-500">暂无匹配的 OpenAI OAuth 账号，请添加或导入。</p>
          </div>
        </section>

        <section class="state-card space-y-4">
          <h2 class="font-semibold">注入范围</h2>
          <label class="state-check"><input v-model="form.all_groups" type="checkbox">适用于全部分组</label>
          <div v-if="!form.all_groups" class="flex flex-wrap gap-4"><label v-for="g in groups" :key="g.id" class="state-check"><input v-model="form.group_ids" type="checkbox" :value="g.id">{{ g.name }}</label><span v-if="!groups.length" class="text-sm text-gray-500">暂无可选分组</span></div>
        </section>
        <div class="flex flex-wrap items-center gap-3"><button class="btn btn-primary" :disabled="busy" @click="save">{{ busy ? '处理中…' : '保存配置' }}</button><button class="btn btn-secondary" :disabled="busy || dirty || !snapshot?.settings.enabled" @click="collect()">立即采集</button><span v-if="dirty" class="text-sm text-amber-600">有未保存的修改</span></div>
      </div>

      <section v-if="activeTab === 'states'" class="state-card space-y-4">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <h2 class="font-semibold">账号状态</h2>
          <div class="flex flex-wrap items-center gap-2">
            <button class="btn btn-secondary" :disabled="busy || dirty || !snapshot?.settings.enabled || !pausedCount" @click="collectPaused"><Icon name="refresh" size="sm" />一键重试待人工项<span class="tabular-nums">({{ pausedCount }})</span></button>
            <button class="btn btn-primary" :disabled="busy || dirty || !snapshot?.settings.enabled" @click="collect()">采集全部选中账号</button>
          </div>
        </div>
        <div class="overflow-x-auto"><table class="state-table"><thead><tr><th>账号</th><th>模型 / 状态</th><th>状态值</th><th>返回码</th><th>响应头值长度</th><th>最近采集</th><th>采集 / 成功 / 注入</th><th>操作</th></tr></thead>
          <template v-for="account in accountStates" :key="account.account_id">
          <tbody class="state-account-group">
            <tr class="state-account-summary" :class="account.account_unavailable ? 'state-account-unavailable' : 'bg-gray-50/60 dark:bg-dark-900/30'">
              <td class="min-w-52">
                <button type="button" class="flex w-full max-w-64 items-center gap-2 text-left font-medium text-primary-700 dark:text-primary-400" :aria-expanded="expandedAccounts.has(account.account_id)" :aria-controls="`state-models-${account.account_id}`" :title="accountName(account.account_id)" @click="toggleAccount(account.account_id)">
                  <Icon :name="expandedAccounts.has(account.account_id) ? 'chevronDown' : 'chevronRight'" size="sm" class="shrink-0" />
                  <span class="truncate">{{ accountName(account.account_id) }}</span>
                </button>
                <p class="mt-1 pl-6 text-xs text-gray-500" :title="account.quality_reason">#{{ account.account_id }} · 综合：{{ account.quality_status === 'degraded' ? '降智' : account.quality_status === 'normal' ? '无降智' : '待检测' }}</p>
                <p v-if="account.account_unavailable" class="mt-1 max-w-64 pl-6 text-xs text-red-600 dark:text-red-300">{{ account.account_unavailable_reason || '账号不可用，已停止采集' }}</p>
              </td>
              <td class="min-w-40">
                <span class="text-xs text-gray-500">{{ account.models.length }} 个模型</span>
                <div class="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs">
                  <span v-if="account.account_unavailable" class="text-red-600 dark:text-red-300">账号不可用 · 停止采集</span>
                  <span v-else-if="account.pausedCount" class="text-amber-600">待人工 {{ account.pausedCount }}</span>
                  <span v-if="account.activeCount" class="text-primary-600">排队 / 采集中 {{ account.activeCount }}</span>
                  <span v-if="!account.account_unavailable && !account.pausedCount && !account.activeCount" class="text-gray-500">{{ account.savedCount === account.models.length ? '已采集' : '待采集' }}</span>
                </div>
              </td>
              <td class="whitespace-nowrap text-xs" :class="account.savedCount ? 'text-emerald-600' : 'text-gray-400'">已保存 {{ account.savedCount }} / {{ account.models.length }}</td>
              <td class="max-w-36 break-words text-xs tabular-nums">{{ account.httpStatuses }}</td>
              <td class="max-w-40 break-words text-xs tabular-nums">{{ account.stateLengths }}</td>
              <td class="whitespace-nowrap text-xs">{{ date(account.lastCollection) }}</td>
              <td class="whitespace-nowrap tabular-nums">{{ account.attempts }} / {{ account.successes }} / {{ account.injections }}</td>
              <td><button class="whitespace-nowrap text-xs text-primary-600 disabled:opacity-40" :disabled="busy || dirty || account.account_unavailable || account.collecting || account.queued || !snapshot?.settings.enabled" @click="collect(account.account_id)">采集全部模型</button></td>
            </tr>
          </tbody>
          <tbody v-if="expandedAccounts.has(account.account_id)" :id="`state-models-${account.account_id}`" class="state-model-group">
            <tr v-for="row in account.models" :key="row.model" :class="{ 'state-account-unavailable': account.account_unavailable }">
              <td aria-hidden="true" class="bg-gray-50/30 dark:bg-dark-900/10"></td>
              <td class="min-w-44 max-w-60">
                <div class="break-all font-medium">{{ row.model }}</div>
                <span class="mt-1 inline-block rounded px-2 py-1 text-xs" :class="row.paused ? 'bg-amber-50 text-amber-700' : row.state_file_saved ? 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/30 dark:text-emerald-300' : 'bg-gray-100 text-gray-600 dark:bg-dark-700 dark:text-gray-300'">{{ row.next_retry_at ? '等待重试' : row.collecting ? '采集中' : row.queued ? '已排队' : row.paused ? '已暂停，待人工' : statusText[row.status] || row.status }}</span>
                <p class="mt-1 max-w-60 break-words text-xs text-gray-500">{{ row.paused ? row.pause_reason : row.message }}</p>
              </td>
              <td class="min-w-44">
                <button class="block max-w-48 text-left font-mono text-xs text-gray-800 disabled:cursor-default dark:text-gray-200" :disabled="!row.state_file_saved" title="查看此模型已保存的完整 State" @click="openDetail(row.account_id, 'file', row.model)">{{ row.state_preview || '—' }}</button>
                <div v-if="row.fingerprint" class="mt-1 max-w-44 truncate font-mono text-[10px] text-gray-400" :title="row.fingerprint">{{ row.fingerprint }}</div>
                <span v-if="row.state_file_saved" class="mt-1 block text-xs text-emerald-600">已保存 · {{ row.saved_state_length || '—' }} 字符</span>
              </td>
              <td class="tabular-nums">{{ row.http_status || '—' }}</td>
              <td class="tabular-nums">{{ row.turn_state_length || '—' }}</td>
              <td class="whitespace-nowrap text-xs">{{ date(row.last_collection_at || row.collected_at) }}<p v-if="row.next_retry_at" class="mt-1 text-amber-600">重试 {{ date(row.next_retry_at) }}</p><p v-else-if="row.next_attempt_at" class="mt-1 text-gray-500">下次 {{ date(row.next_attempt_at) }}</p><p v-if="row.collection_proxy_id" class="mt-1 max-w-40 whitespace-normal break-words text-gray-500">{{ proxyName(row.collection_proxy_id) }} · {{ row.proxy_attempt || 1 }}/{{ row.proxy_count || 1 }}</p></td>
              <td class="whitespace-nowrap tabular-nums">{{ row.attempts }} / {{ row.successes }} / {{ row.injections }}<p class="mt-1 text-xs text-gray-500">本轮已请求 {{ row.round_attempts || 0 }} 次</p><p class="mt-1 text-xs text-gray-500">重试 {{ row.retry_attempt || 0 }} / {{ row.retry_limit || 0 }}</p></td>
              <td><div class="flex flex-col items-start gap-2 whitespace-nowrap">
                <button class="inline-flex items-center gap-1 text-primary-600 disabled:opacity-40" :disabled="!row.has_details" @click="openDetail(row.account_id, 'header', row.model)"><Icon name="eye" size="sm" />详情</button>
                <button class="inline-flex items-center gap-1 text-primary-600 disabled:opacity-40" :disabled="!row.state_file_saved" title="查看已写入的 State 文件" @click="openDetail(row.account_id, 'file', row.model)"><Icon name="eye" size="sm" />查看</button>
                <button class="text-primary-600 disabled:opacity-40" :disabled="busy || dirty || account.account_unavailable || row.account_unavailable || row.queued || row.collecting || !snapshot?.settings.enabled" @click="collect(row.account_id, row.model)">{{ row.paused ? '手动重试' : '采集' }}</button>
              </div></td>
            </tr>
          </tbody>
          </template>
          <tbody v-if="!snapshot?.rows.length"><tr><td colspan="8" class="py-12 text-center text-gray-500">先在采集配置中选择账号并保存。</td></tr></tbody>
        </table></div>
      </section>

      <section v-if="activeTab === 'history'" class="state-card space-y-4"><div><h2 class="font-semibold">采集记录</h2><p class="mt-1 text-xs text-gray-500">显示本次服务运行期间最近 200 条记录。</p></div><div class="overflow-x-auto"><table class="state-table"><thead><tr><th>时间</th><th>账号</th><th>模型</th><th>返回码</th><th class="whitespace-nowrap">响应头值长度</th><th>结果</th></tr></thead><tbody><tr v-for="(event, index) in snapshot?.events || []" :key="index"><td class="whitespace-nowrap">{{ date(event.at) }}</td><td>{{ accountName(event.account_id) }}</td><td>{{ event.model }}</td><td>{{ event.http_status || '—' }}</td><td class="tabular-nums">{{ event.turn_state_length || '—' }}</td><td class="min-w-64">{{ event.message }}</td></tr><tr v-if="!snapshot?.events.length"><td colspan="6" class="py-12 text-center text-gray-500">暂无采集记录</td></tr></tbody></table></div></section>

      <BaseDialog :show="detailAccountID !== null" :title="detailMode === 'file' ? 'State 文件详情' : 'Turn-State 详情'" width="wide" @close="closeDetail">
        <div class="min-h-48 space-y-4" :aria-busy="detailLoading">
          <p class="break-all font-medium">{{ detailAccountID !== null ? accountName(detailAccountID) : '' }}</p>
          <p class="break-all text-sm text-gray-500">{{ detailModel }}</p>
          <p v-if="detailLoading" role="status" class="py-8 text-center text-sm text-gray-500">加载中…</p>
          <div v-else-if="detailError" class="space-y-3">
            <p role="alert" class="text-sm text-red-600 dark:text-red-400">{{ detailError }}</p>
            <button class="btn btn-secondary" @click="detailAccountID !== null && openDetail(detailAccountID, detailMode, detailModel)"><Icon name="refresh" size="sm" />重新加载</button>
          </div>
          <template v-else-if="fileDetail">
            <dl class="grid grid-cols-1 gap-3 text-sm sm:grid-cols-2">
              <div><dt class="text-gray-500">文件名</dt><dd class="mt-1 break-all">{{ fileDetail.file_name }}</dd></div>
              <div><dt class="text-gray-500">响应头值长度</dt><dd class="mt-1 tabular-nums">{{ fileDetail.content.turn_state.length }} 字符</dd></div>
              <div><dt class="text-gray-500">采集时间</dt><dd class="mt-1">{{ date(fileDetail.content.collected_at) }}</dd></div>
            </dl>
            <div class="space-y-2">
              <h4 class="text-sm font-medium">完整文件内容（解密后）</h4>
              <pre aria-label="完整 State 文件内容" tabindex="0" class="max-h-96 min-h-48 overflow-y-auto whitespace-pre-wrap break-all rounded-lg border border-gray-200 bg-gray-50 p-3 font-mono text-xs leading-6 dark:border-dark-600 dark:bg-dark-900">{{ fileContent }}</pre>
            </div>
          </template>
          <template v-else-if="detail">
            <dl class="grid grid-cols-1 gap-3 text-sm sm:grid-cols-2">
              <div><dt class="text-gray-500">采集模型</dt><dd class="mt-1 break-all">{{ detail.model }}</dd></div>
              <div><dt class="text-gray-500">HTTP 返回码</dt><dd class="mt-1 tabular-nums">{{ detail.http_status }}</dd></div>
              <div><dt class="text-gray-500">响应头值长度</dt><dd class="mt-1 tabular-nums">{{ detail.turn_state_length }} 字符</dd></div>
              <div><dt class="text-gray-500">当前保存规则</dt><dd class="mt-1" :class="detail.save_allowed ? 'text-emerald-600' : 'text-amber-600'">{{ detail.save_allowed ? '长度符合' : '长度不符' }}</dd></div>
              <div><dt class="text-gray-500">本次采集文件</dt><dd class="mt-1">{{ detail.state_file_saved ? '已写入 State 文件' : '未写入 State 文件' }}</dd></div>
              <div><dt class="text-gray-500">采集时间</dt><dd class="mt-1">{{ date(detail.collected_at) }}</dd></div>
            </dl>
            <div class="space-y-2">
              <h4 class="text-sm font-medium">完整响应头</h4>
              <pre aria-label="完整响应头" tabindex="0" class="max-h-72 min-h-32 overflow-y-auto whitespace-pre-wrap break-all rounded-lg border border-gray-200 bg-gray-50 p-3 font-mono text-xs leading-6 dark:border-dark-600 dark:bg-dark-900">{{ detailHeader }}</pre>
            </div>
          </template>
        </div>
        <template #footer>
          <button class="btn btn-secondary" @click="closeDetail">关闭</button>
          <button class="btn btn-primary min-w-40" :disabled="(!detail && !fileDetail) || detailLoading" @click="copyToClipboard(detailMode === 'file' ? fileContent : detailHeader, detailMode === 'file' ? '完整文件内容已复制' : '完整响应头已复制')"><Icon :name="copied ? 'check' : 'copy'" size="sm" />{{ copied ? '已复制' : detailMode === 'file' ? '复制文件内容' : '复制完整响应头' }}</button>
        </template>
      </BaseDialog>

      <ImportAccounts v-if="showImport" :show="showImport" @close="showImport = false" @imported="loadChoices" />
      <CreateAccount v-if="showCreate" :show="showCreate" :proxies="proxies" :groups="groups" @close="showCreate = false" @created="loadChoices" />
    </div>
  </AppLayout>
</template>

<style scoped>
.state-card { @apply rounded-xl border border-gray-200 bg-white p-5 text-gray-900 dark:border-dark-700 dark:bg-dark-800 dark:text-gray-100; }
.state-label { @apply flex flex-col gap-2 text-sm text-gray-600 dark:text-gray-300; }
.state-check { @apply inline-flex items-center gap-2 text-sm; }
.state-check input { @apply h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500; }
.state-table { @apply w-full text-left text-sm; }
.state-table th { @apply whitespace-nowrap border-b border-gray-200 px-3 py-3 text-xs font-medium text-gray-500 dark:border-dark-700; }
.state-table td { @apply border-b border-gray-100 px-3 py-4 align-top dark:border-dark-700; }
.state-table td > span, .state-table td > button { @apply whitespace-nowrap; }
.state-account-group + .state-account-group { @apply border-t-2 border-gray-200 dark:border-dark-600; }
.state-account-unavailable { @apply bg-red-50 dark:bg-red-950/30; }
</style>
