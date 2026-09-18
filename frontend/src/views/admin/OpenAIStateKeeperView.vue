<script setup lang="ts">
import { computed, defineAsyncComponent, onMounted, onUnmounted, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import Icon from '@/components/icons/Icon.vue'
import { stateKeeperAPI, type StateKeeperSettings, type StateKeeperSnapshot } from '@/api/admin/openaiStateKeeper'
import * as accountsAPI from '@/api/admin/accounts'
import * as groupsAPI from '@/api/admin/groups'
import * as proxiesAPI from '@/api/admin/proxies'
import type { AccountListItem, AdminGroup, Proxy, CreateProxyRequest } from '@/types'

const ImportAccounts = defineAsyncComponent(() => import('@/components/admin/account/ImportDataModal.vue'))
const CreateAccount = defineAsyncComponent(() => import('@/components/account/CreateAccountModal.vue'))
const form = ref<StateKeeperSettings | null>(null)
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
const activeTab = ref<'settings' | 'states' | 'history'>('settings')
const showImport = ref(false)
const showCreate = ref(false)
const showProxy = ref(false)
const proxyBusy = ref(false)
const newProxy = ref<CreateProxyRequest>({ name: '', protocol: 'http', host: '', port: 8080, username: '', password: '' })
const dirty = computed(() => !!form.value && !!snapshot.value && JSON.stringify(form.value) !== JSON.stringify(snapshot.value.settings))
const visibleAccounts = computed(() => accounts.value.filter(a => `${a.id} ${a.name}`.toLowerCase().includes(search.value.toLowerCase())))
const readyCount = computed(() => snapshot.value?.rows.filter(r => r.status === 'ready' || r.status === 'refresh_failed').length || 0)
const injectionCount = computed(() => snapshot.value?.rows.reduce((sum, r) => sum + r.injections, 0) || 0)
const collectingCount = computed(() => snapshot.value?.rows.filter(r => r.collecting || r.queued).length || 0)
const accountName = (id: number) => accounts.value.find(a => a.id === id)?.name || `账号 #${id}`
const date = (value?: string) => value ? new Date(value).toLocaleString() : '—'
const errorText = (error: unknown) => {
  const e = error as { response?: { data?: { message?: string } }; message?: string }
  return e.response?.data?.message || e.message || '请求失败，请稍后重试'
}
const statusText: Record<string, string> = { empty: '尚未采集', ready: '已采集', expired: '已过期', refresh_failed: '刷新失败，旧值仍在缓存期', not_observed: '未取得目标状态', upstream_error: '上游错误', failed: '采集失败' }
function notify(text: string, error = false) { message.value = text; messageError.value = error }

async function load(reset = false) {
  if (loading.value) return
  loading.value = true
  try {
    const next = await stateKeeperAPI.get()
    snapshot.value = next
    if (reset || !form.value) form.value = structuredClone(next.settings)
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
    const data = await stateKeeperAPI.save(form.value)
    snapshot.value = data; form.value = structuredClone(data.settings)
    notify('配置已保存')
  } catch (e) { notify(errorText(e), true) }
  finally { busy.value = false }
}

async function collect(id?: number) {
  if (dirty.value) { notify('请先保存配置，再开始采集', true); return }
  busy.value = true
  try {
    await stateKeeperAPI.collect(id ? [id] : [])
    notify('采集已排队，可在状态列表查看结果'); activeTab.value = 'states'; await load()
  } catch (e) { notify(errorText(e), true) }
  finally { busy.value = false }
}

async function createProxy() {
  if (!newProxy.value.name.trim() || !newProxy.value.host.trim() || !newProxy.value.port) {
    notify('请填写代理名称、主机和端口', true); return
  }
  proxyBusy.value = true
  try {
    const proxy = await proxiesAPI.create(newProxy.value)
    await loadChoices()
    if (form.value) form.value.proxy_id = proxy.id
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
onUnmounted(() => { disposed = true; if (timer) clearTimeout(timer) })
</script>

<template>
  <AppLayout>
    <div class="space-y-5 pb-8">
      <header class="flex flex-wrap items-center justify-between gap-4 border-b border-gray-200 pb-5 dark:border-dark-700">
        <div class="flex items-center gap-3">
          <span class="flex h-10 w-10 items-center justify-center rounded-xl bg-primary-50 text-primary-600 dark:bg-primary-900/20"><Icon name="sync" size="md" /></span>
          <div><h1 class="text-xl font-semibold text-gray-900 dark:text-white">上游状态管理</h1><p class="mt-1 text-sm text-gray-500">按账号采集 State，管理自动刷新与请求注入。</p></div>
        </div>
        <button class="btn btn-secondary" :disabled="loading" @click="load(); loadChoices()"><Icon name="refresh" size="sm" />刷新</button>
      </header>

      <p v-if="loadError || snapshot?.config_error" role="alert" class="rounded-xl bg-red-50 px-4 py-3 text-sm text-red-700 dark:bg-red-950/30 dark:text-red-300">{{ loadError || snapshot?.config_error }}</p>
      <p v-if="message" role="status" class="rounded-xl px-4 py-3 text-sm" :class="messageError ? 'bg-red-50 text-red-700 dark:bg-red-950/30 dark:text-red-300' : 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/30 dark:text-emerald-300'">{{ message }}</p>

      <div class="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <article class="state-card"><span class="text-sm text-gray-500">采集开关</span><strong class="mt-2 block text-xl">{{ snapshot?.settings.enabled ? '已开启' : '已关闭' }}</strong></article>
        <article class="state-card"><span class="text-sm text-gray-500">已采集 / 选中账号</span><strong class="mt-2 block text-xl">{{ readyCount }} / {{ snapshot?.rows.length || 0 }}</strong></article>
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
            <label class="state-check"><input v-model="form.auto_refresh" type="checkbox">自动采集与刷新</label>
            <label class="state-check"><input v-model="form.injection_enabled" type="checkbox">启用请求注入</label>
          </div>
          <label class="state-label max-w-xs">采集并发数<input v-model.number="form.concurrency" class="input" type="number" min="1" max="500" step="1" placeholder="输入并发数"></label>
          <div v-if="form.auto_refresh" class="space-y-2">
            <label class="state-label max-w-xs">自动采集间隔（s）<input v-model.number="form.auto_collect_interval_seconds" class="input" type="number" min="0" max="86400" step="1" placeholder="输入秒数"></label>
            <p class="text-xs text-gray-500">保存后生效。大于 0 时，每次采集结束后等待该秒数再采集，失败也按此间隔重试；0 沿用原有自动续期。</p>
          </div>
          <p class="text-sm leading-6 text-gray-500">先保存配置并采集，查看结果后再启用注入。仅接收实际返回的 HTTP 292 与 current_turn_state 响应头，成功后写入各账号独立的 State 文件。采集会消耗上游账号额度，改善效果需实际验证。</p>
        </section>

        <section class="state-card space-y-4">
          <h2 class="font-semibold">采集出口与模型</h2>
          <div class="grid gap-4 md:grid-cols-2">
            <label class="state-label">采集代理<select v-model.number="form.proxy_id" class="input"><option :value="0">请选择采集代理</option><option v-for="p in proxies" :key="p.id" :value="p.id">{{ p.name }} · {{ p.host }}:{{ p.port }}</option></select></label>
            <label class="state-label">采集模型<input v-model="form.model" class="input" maxlength="200" placeholder="填写实际使用的模型"></label>
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
          <div class="flex flex-wrap items-center justify-between gap-3"><h2 class="font-semibold">采集账号 <span class="font-normal text-gray-500">{{ form.account_ids.length }} 个</span></h2><div class="flex gap-2"><button class="btn btn-secondary" @click="showCreate = true">添加账号</button><button class="btn btn-secondary" @click="showImport = true">导入账号</button></div></div>
          <input v-model="search" class="input max-w-md" placeholder="搜索账号名称或 ID" aria-label="搜索采集账号">
          <div class="grid max-h-64 gap-2 overflow-y-auto md:grid-cols-2 xl:grid-cols-3">
            <label v-for="a in visibleAccounts" :key="a.id" class="state-check rounded-lg border border-gray-200 p-3 dark:border-dark-600"><input v-model="form.account_ids" type="checkbox" :value="a.id"><span class="min-w-0 truncate">{{ a.name }} <span class="text-gray-400">#{{ a.id }}</span></span></label>
            <p v-if="!visibleAccounts.length" class="py-5 text-sm text-gray-500">暂无匹配的 OpenAI OAuth 账号，请添加或导入。</p>
          </div>
          <p class="text-xs text-gray-500">同一账号不重复采集；并发数以已保存配置为准，超出部分排队。最多选择 500 个账号。</p>
        </section>

        <section class="state-card space-y-4">
          <h2 class="font-semibold">注入范围与刷新</h2>
          <label class="state-check"><input v-model="form.all_groups" type="checkbox">适用于全部分组</label>
          <div v-if="!form.all_groups" class="flex flex-wrap gap-4"><label v-for="g in groups" :key="g.id" class="state-check"><input v-model="form.group_ids" type="checkbox" :value="g.id">{{ g.name }}</label><span v-if="!groups.length" class="text-sm text-gray-500">暂无可选分组</span></div>
          <p class="text-xs text-gray-500">只有选中账号、选中分组和相同模型的 Responses 请求才会注入。状态缺失或过期时照常转发。</p>
          <div class="grid gap-4 md:grid-cols-3">
            <label class="state-label">缓存时长（秒）<input v-model.number="form.ttl_seconds" class="input" type="number" min="300" max="86400"></label>
            <label v-if="!form.auto_collect_interval_seconds" class="state-label">提前刷新（秒）<input v-model.number="form.refresh_before_seconds" class="input" type="number" min="30" :max="form.ttl_seconds - 1"></label>
            <label v-if="!form.auto_collect_interval_seconds" class="state-label">失败重试起始间隔（秒）<input v-model.number="form.retry_seconds" class="input" type="number" min="30" max="3600"></label>
          </div>
          <p class="text-xs text-gray-500">缓存时长是本地实验参数，不代表上游承诺的有效期。未设置定时间隔时，按缓存到期时间提前刷新，连续失败会延长重试间隔。State 文件加密保存，重启后恢复有效状态。</p>
        </section>
        <div class="flex flex-wrap items-center gap-3"><button class="btn btn-primary" :disabled="busy" @click="save">{{ busy ? '处理中…' : '保存配置' }}</button><button class="btn btn-secondary" :disabled="busy || dirty || !snapshot?.settings.enabled" @click="collect()">立即采集</button><span v-if="dirty" class="text-sm text-amber-600">有未保存的修改</span></div>
      </div>

      <section v-if="activeTab === 'states'" class="state-card space-y-4">
        <div class="flex items-center justify-between gap-3"><h2 class="font-semibold">账号状态</h2><button class="btn btn-primary" :disabled="busy || dirty || !snapshot?.settings.enabled" @click="collect()">采集全部选中账号</button></div>
        <div class="overflow-x-auto"><table class="state-table"><thead><tr><th>账号 / 模型</th><th>状态</th><th>返回码</th><th>最近采集 / 缓存截止</th><th>采集 / 成功 / 注入</th><th>说明</th><th>操作</th></tr></thead><tbody>
          <tr v-for="row in snapshot?.rows || []" :key="row.account_id"><td><div class="font-medium">{{ accountName(row.account_id) }}</div><div class="text-xs text-gray-500">{{ row.model }}</div></td><td><span class="inline-block rounded-full px-2.5 py-1 text-xs" :class="row.status === 'ready' ? 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/30 dark:text-emerald-300' : 'bg-gray-100 text-gray-600 dark:bg-dark-700 dark:text-gray-300'">{{ row.collecting ? '采集中' : row.queued ? '已排队' : statusText[row.status] || row.status }}</span></td><td>{{ row.http_status || '—' }}</td><td class="whitespace-nowrap"><div>{{ date(row.last_attempt_at) }}</div><div class="text-xs text-gray-500">{{ date(row.expires_at) }}</div></td><td>{{ row.attempts }} / {{ row.successes }} / {{ row.injections }}</td><td class="min-w-56 max-w-96"><p class="whitespace-normal break-words">{{ row.message || '等待采集' }}</p><span v-if="row.fingerprint" class="text-xs text-gray-400">状态标识 {{ row.fingerprint }}</span></td><td><button class="text-primary-600 disabled:opacity-40" :disabled="busy || dirty || row.queued || row.collecting || !snapshot?.settings.enabled" @click="collect(row.account_id)">采集</button></td></tr>
          <tr v-if="!snapshot?.rows.length"><td colspan="7" class="py-12 text-center text-gray-500">先在采集配置中选择账号并保存。</td></tr>
        </tbody></table></div>
      </section>

      <section v-if="activeTab === 'history'" class="state-card space-y-4"><div><h2 class="font-semibold">采集记录</h2><p class="mt-1 text-xs text-gray-500">显示本次服务运行期间最近 200 条记录。</p></div><div class="overflow-x-auto"><table class="state-table"><thead><tr><th>时间</th><th>账号</th><th>模型</th><th>返回码</th><th>结果</th></tr></thead><tbody><tr v-for="(event, index) in snapshot?.events || []" :key="index"><td class="whitespace-nowrap">{{ date(event.at) }}</td><td>{{ accountName(event.account_id) }}</td><td>{{ event.model }}</td><td>{{ event.http_status || '—' }}</td><td class="min-w-64">{{ event.message }}</td></tr><tr v-if="!snapshot?.events.length"><td colspan="5" class="py-12 text-center text-gray-500">暂无采集记录</td></tr></tbody></table></div></section>

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
.state-table th { @apply border-b border-gray-200 px-3 py-3 text-xs font-medium text-gray-500 dark:border-dark-700; }
.state-table td { @apply border-b border-gray-100 px-3 py-4 align-top dark:border-dark-700; }
</style>
