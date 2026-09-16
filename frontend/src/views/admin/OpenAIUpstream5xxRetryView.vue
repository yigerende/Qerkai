<template>
  <AppLayout>
    <div class="w-full min-w-0 space-y-5 pb-8">
      <header class="flex flex-col gap-4 border-b border-gray-200 pb-5 dark:border-dark-700 sm:flex-row sm:items-end sm:justify-between">
        <div class="flex items-center gap-3">
          <span class="flex h-9 w-9 items-center justify-center rounded-lg bg-emerald-50 text-emerald-600 dark:bg-emerald-950/50 dark:text-emerald-400"><Icon name="sync" size="md" /></span>
          <div>
            <h1 class="text-xl font-bold text-gray-900 dark:text-white">{{ t('admin.openaiUpstream5xxRetry.title') }}</h1>
            <p class="mt-0.5 text-xs text-gray-500 dark:text-gray-400">{{ t('admin.openaiUpstream5xxRetry.description') }}</p>
          </div>
        </div>
        <div class="flex items-center gap-3">
          <label class="inline-flex cursor-pointer items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400">
            <input v-model="autoRefresh" type="checkbox" class="h-3.5 w-3.5 rounded border-gray-300 text-emerald-600 focus:ring-emerald-500 dark:border-dark-600 dark:bg-dark-700" />
            {{ t('admin.openaiUpstream5xxRetry.autoRefresh') }}
          </label>
          <span class="text-xs text-gray-400">{{ updatedText }}</span>
          <button class="btn btn-secondary h-9 w-9 p-0" :disabled="loading" :title="t('admin.openaiUpstream5xxRetry.refresh')" @click="load">
            <Icon name="refresh" size="sm" :class="loading ? 'animate-spin' : ''" />
          </button>
          <button class="btn btn-secondary h-9 gap-1.5 px-3 text-xs" :disabled="clearing || !page.total" @click="showClearDialog = true">
            <Icon name="trash" size="sm" />{{ t('admin.openaiUpstream5xxRetry.clear') }}
          </button>
        </div>
      </header>

      <div v-if="loadError" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/30 dark:text-red-300">
        {{ t('admin.openaiUpstream5xxRetry.loadFailed') }}
      </div>

      <div v-if="page.config && !page.config.enabled" class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
        {{ t('admin.openaiUpstream5xxRetry.disabledHint') }}
      </div>

      <section class="rounded-lg border border-gray-200 bg-white px-4 py-3 dark:border-dark-700 dark:bg-dark-800">
        <div class="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs">
          <span class="font-semibold text-gray-900 dark:text-white">{{ t('admin.openaiUpstream5xxRetry.configLabel') }}</span>
          <span class="inline-flex items-center gap-1.5 text-gray-500 dark:text-gray-400">
            <span class="h-2 w-2 rounded-full" :class="page.config?.enabled ? 'bg-emerald-500' : 'bg-gray-400'" />
            {{ page.config?.enabled ? t('common.enabled') : t('common.disabled') }}
          </span>
          <span class="text-gray-500 dark:text-gray-400">{{ t('admin.openaiUpstream5xxRetry.config.sameAccount', { n: page.config?.same_account ?? 0 }) }}</span>
          <span class="text-gray-500 dark:text-gray-400">{{ t('admin.openaiUpstream5xxRetry.config.total', { n: page.config?.total ?? 0 }) }}</span>
          <span class="text-gray-500 dark:text-gray-400">{{ t('admin.openaiUpstream5xxRetry.config.delay', { n: page.config?.delay_ms ?? 0 }) }}</span>
        </div>
      </section>

      <div class="grid grid-cols-2 gap-3 lg:grid-cols-4 xl:grid-cols-8">
        <div v-for="item in summary" :key="item.label" class="rounded-lg border border-gray-200 bg-white px-4 py-3 dark:border-dark-700 dark:bg-dark-800">
          <div class="text-xs text-gray-500 dark:text-gray-400">{{ item.label }}</div>
          <div class="mt-1 text-2xl font-semibold tabular-nums" :class="item.class || 'text-gray-900 dark:text-white'">{{ item.value }}</div>
        </div>
      </div>

      <section class="rounded-lg border border-gray-200 bg-white px-4 py-3 dark:border-dark-700 dark:bg-dark-800">
        <div class="flex flex-wrap items-end gap-3">
          <label class="block">
            <span class="mb-1 block text-xs text-gray-500 dark:text-gray-400">{{ t('admin.openaiUpstream5xxRetry.filters.event') }}</span>
            <select v-model="filters.event" class="input h-9 w-40 text-sm">
              <option value="">{{ t('admin.openaiUpstream5xxRetry.filters.all') }}</option>
              <option v-for="ev in eventOptions" :key="ev" :value="ev">{{ t(`admin.openaiUpstream5xxRetry.events.${ev}`) }}</option>
            </select>
          </label>
          <label class="block">
            <span class="mb-1 block text-xs text-gray-500 dark:text-gray-400">{{ t('admin.openaiUpstream5xxRetry.filters.statusCode') }}</span>
            <select v-model="filters.statusCode" class="input h-9 w-32 text-sm">
              <option value="">{{ t('admin.openaiUpstream5xxRetry.filters.all') }}</option>
              <option value="502">502</option>
              <option value="503">503</option>
            </select>
          </label>
          <label class="block">
            <span class="mb-1 block text-xs text-gray-500 dark:text-gray-400">{{ t('admin.openaiUpstream5xxRetry.filters.accountId') }}</span>
            <input v-model="filters.accountId" type="number" min="1" class="input h-9 w-40 text-sm" :placeholder="t('admin.openaiUpstream5xxRetry.filters.accountIdPlaceholder')" />
          </label>
          <button class="btn btn-secondary h-9 px-3 text-xs" @click="resetFilters">{{ t('admin.openaiUpstream5xxRetry.filters.reset') }}</button>
        </div>
      </section>

      <section class="overflow-hidden rounded-lg border border-gray-200 bg-white dark:border-dark-700 dark:bg-dark-800">
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200 text-sm dark:divide-dark-700">
            <thead class="whitespace-nowrap bg-gray-50 text-left text-xs font-medium text-gray-500 dark:bg-dark-700/60 dark:text-gray-400">
              <tr>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.time') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.event') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.upstreamStatus') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.clientStatus') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.account') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.model') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.transport') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.attempt') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.wsRetry') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.rule') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.delay') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.extraLatency') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.requestId') }}</th>
                <th class="px-3 py-3">{{ t('admin.openaiUpstream5xxRetry.columns.message') }}</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-100 dark:divide-dark-700">
              <tr v-for="entry in page.entries" :key="entry.seq" class="hover:bg-gray-50 dark:hover:bg-dark-700/40">
                <td class="whitespace-nowrap px-3 py-3 tabular-nums text-gray-500 dark:text-gray-400">{{ formatTime(entry.at_unix_ms) }}</td>
                <td class="px-3 py-3">
                  <span class="whitespace-nowrap rounded px-2 py-1 text-xs" :class="eventClass(entry.event)" :title="t(`admin.openaiUpstream5xxRetry.eventHints.${entry.event}`)">
                    {{ t(`admin.openaiUpstream5xxRetry.events.${entry.event}`) }}
                  </span>
                </td>
                <td class="px-3 py-3 tabular-nums text-amber-600 dark:text-amber-300">{{ entry.upstream_status || '-' }}</td>
                <td class="px-3 py-3 tabular-nums" :class="clientStatusClass(entry)">{{ isTerminal(entry.event) ? entry.status_code : '-' }}</td>
                <td class="px-3 py-3">
                  <div class="max-w-[12rem] truncate text-gray-900 dark:text-white" :title="entry.account_name">
                    {{ entry.account_name || t('admin.openaiUpstream5xxRetry.accountFallback', { id: entry.account_id }) }}
                  </div>
                  <div class="text-xs text-gray-400">{{ t('admin.openaiUpstream5xxRetry.idLabel', { id: entry.account_id }) }}</div>
                </td>
                <td class="px-3 py-3"><div class="max-w-[12rem] truncate" :title="entry.model">{{ entry.model || '-' }}</div></td>
                <td class="whitespace-nowrap px-3 py-3 text-xs text-gray-500 dark:text-gray-400">{{ transportText(entry.transport) }}</td>
                <td class="whitespace-nowrap px-3 py-3 text-xs tabular-nums">{{ attemptText(entry) }}</td>
                <td class="whitespace-nowrap px-3 py-3 text-xs tabular-nums" :title="entry.ws_retry_reason">{{ wsRetryText(entry) }}</td>
                <td class="px-3 py-3"><div class="max-w-[12rem] truncate text-xs" :title="entry.rule_name">{{ entry.rule_name || '-' }}</div></td>
                <td class="whitespace-nowrap px-3 py-3 tabular-nums text-gray-500 dark:text-gray-400">{{ entry.retry_delay_ms ? `${entry.retry_delay_ms} ms` : '-' }}</td>
                <td class="whitespace-nowrap px-3 py-3 tabular-nums" :class="entry.extra_latency_ms ? 'text-amber-600 dark:text-amber-300' : 'text-gray-400'">
                  {{ entry.extra_latency_ms ? `${entry.extra_latency_ms} ms` : '-' }}
                </td>
                <td class="px-3 py-3">
                  <div class="max-w-[10rem] truncate font-mono text-xs text-gray-500" :title="entry.request_id || entry.client_request_id">
                    {{ entry.request_id || entry.client_request_id || '-' }}
                  </div>
                </td>
                <td class="px-3 py-3"><div class="max-w-[18rem] truncate text-xs text-gray-500 dark:text-gray-400" :title="messageText(entry)">{{ messageText(entry) }}</div></td>
              </tr>
              <tr v-if="!loading && !page.entries.length">
                <td colspan="14" class="px-4 py-12 text-center text-gray-400">
                  {{ hasFilters ? t('admin.openaiUpstream5xxRetry.emptyFiltered') : t('admin.openaiUpstream5xxRetry.empty') }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-if="page.total > 0" class="flex flex-wrap items-center justify-between gap-3 border-t border-gray-200 px-4 py-3 dark:border-dark-700">
          <span class="text-xs text-gray-500 dark:text-gray-400">{{ t('admin.openaiUpstream5xxRetry.pagination', { from: rangeFrom, to: rangeTo, total: page.total }) }}</span>
          <div class="flex items-center gap-2">
            <button class="btn btn-secondary h-8 w-8 p-0" :disabled="currentPage <= 1" @click="currentPage--"><Icon name="chevronLeft" size="sm" /></button>
            <span class="text-xs tabular-nums text-gray-500 dark:text-gray-400">{{ currentPage }} / {{ totalPages }}</span>
            <button class="btn btn-secondary h-8 w-8 p-0" :disabled="currentPage >= totalPages" @click="currentPage++"><Icon name="chevronRight" size="sm" /></button>
          </div>
        </div>
      </section>
    </div>

    <ConfirmDialog
      :show="showClearDialog"
      :title="t('admin.openaiUpstream5xxRetry.clear')"
      :message="t('admin.openaiUpstream5xxRetry.clearConfirm')"
      :danger="true"
      @confirm="handleClear"
      @cancel="showClearDialog = false"
    />
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import AppLayout from '@/components/layout/AppLayout.vue'
import Icon from '@/components/icons/Icon.vue'
import ConfirmDialog from '@/components/common/ConfirmDialog.vue'
import { useAppStore } from '@/stores/app'
import {
  getOpenAIUpstream5xxRetryLog,
  clearOpenAIUpstream5xxRetryLog,
  type OpenAIUpstream5xxRetryEvent,
  type OpenAIUpstream5xxRetryLogEntry,
  type OpenAIUpstream5xxRetryLogResponse,
} from '@/api/admin/ops'

const { t, te } = useI18n()
const appStore = useAppStore()

const PAGE_SIZE = 50
const eventOptions: OpenAIUpstream5xxRetryEvent[] = ['intercepted', 'succeeded', 'exhausted', 'skipped']

const emptyPage = (): OpenAIUpstream5xxRetryLogResponse => ({
  entries: [],
  total: 0,
  capacity: 0,
  stats: { total: 0, intercepted: 0, succeeded: 0, exhausted: 0, skipped: 0, extra_latency_ms_total: 0, avg_extra_latency_ms: 0, max_extra_latency_ms: 0 },
  config: { enabled: false, same_account: 0, total: 0, delay_ms: 0 },
})

const page = ref<OpenAIUpstream5xxRetryLogResponse>(emptyPage())
const loading = ref(false)
const clearing = ref(false)
const loadError = ref(false)
const autoRefresh = ref(true)
const showClearDialog = ref(false)
const currentPage = ref(1)
const lastLoadedAt = ref<number | null>(null)
const filters = ref<{ event: '' | OpenAIUpstream5xxRetryEvent; statusCode: string; accountId: string }>({
  event: '',
  statusCode: '',
  accountId: '',
})

let timer: number | undefined

const hasFilters = computed(() => Boolean(filters.value.event || filters.value.statusCode || filters.value.accountId))
const totalPages = computed(() => Math.max(1, Math.ceil(page.value.total / PAGE_SIZE)))
const rangeFrom = computed(() => (page.value.total ? (currentPage.value - 1) * PAGE_SIZE + 1 : 0))
const rangeTo = computed(() => Math.min(currentPage.value * PAGE_SIZE, page.value.total))

const summary = computed(() => {
  const s = page.value.stats
  return [
    { label: t('admin.openaiUpstream5xxRetry.stats.total'), value: s.total },
    { label: t('admin.openaiUpstream5xxRetry.stats.intercepted'), value: s.intercepted },
    { label: t('admin.openaiUpstream5xxRetry.stats.succeeded'), value: s.succeeded, class: 'text-emerald-600 dark:text-emerald-400' },
    { label: t('admin.openaiUpstream5xxRetry.stats.exhausted'), value: s.exhausted, class: s.exhausted ? 'text-red-600 dark:text-red-400' : undefined },
    { label: t('admin.openaiUpstream5xxRetry.stats.skipped'), value: s.skipped },
    { label: t('admin.openaiUpstream5xxRetry.stats.avgExtraLatency'), value: `${s.avg_extra_latency_ms} ms` },
    { label: t('admin.openaiUpstream5xxRetry.stats.maxExtraLatency'), value: `${s.max_extra_latency_ms} ms` },
    { label: t('admin.openaiUpstream5xxRetry.stats.capacity'), value: page.value.capacity },
  ]
})

const updatedText = computed(() => (lastLoadedAt.value ? t('admin.openaiUpstream5xxRetry.updatedAt', { time: new Date(lastLoadedAt.value).toLocaleTimeString() }) : ''))

const load = async () => {
  if (loading.value) return
  loading.value = true
  try {
    const accountId = Number(filters.value.accountId)
    page.value = await getOpenAIUpstream5xxRetryLog({
      event: filters.value.event || undefined,
      status_code: filters.value.statusCode ? Number(filters.value.statusCode) : undefined,
      account_id: Number.isFinite(accountId) && accountId > 0 ? accountId : undefined,
      page: currentPage.value,
      page_size: PAGE_SIZE,
    })
    lastLoadedAt.value = Date.now()
    loadError.value = false
  } catch {
    loadError.value = true
  } finally {
    loading.value = false
  }
}

const handleClear = async () => {
  showClearDialog.value = false
  clearing.value = true
  try {
    await clearOpenAIUpstream5xxRetryLog()
    currentPage.value = 1
    await load()
    appStore.showSuccess(t('admin.openaiUpstream5xxRetry.cleared'))
  } catch {
    appStore.showError(t('admin.openaiUpstream5xxRetry.clearFailed'))
  } finally {
    clearing.value = false
  }
}

const resetFilters = () => {
  filters.value = { event: '', statusCode: '', accountId: '' }
}

// 筛选条件变化时回到第一页；页码变化直接重新拉取。
watch(filters, () => {
  if (currentPage.value !== 1) {
    currentPage.value = 1
    return
  }
  void load()
}, { deep: true })
watch(currentPage, () => void load())

const tick = () => {
  if (autoRefresh.value && document.visibilityState === 'visible') void load()
}
const onVisibility = () => {
  if (document.visibilityState === 'visible') void load()
}

onMounted(() => {
  void load()
  timer = window.setInterval(tick, 5000)
  document.addEventListener('visibilitychange', onVisibility)
})
onBeforeUnmount(() => {
  if (timer) window.clearInterval(timer)
  document.removeEventListener('visibilitychange', onVisibility)
})

const isTerminal = (event: string) => event === 'succeeded' || event === 'exhausted'

const formatTime = (ms: number) => (ms ? new Date(ms).toLocaleString() : '-')

const transportText = (transport: string) => {
  if (transport === 'http_sse') return t('admin.openaiUpstream5xxRetry.transports.http_sse')
  if (transport === 'responses_websockets_v2') return t('admin.openaiUpstream5xxRetry.transports.responses_websockets_v2')
  return t('admin.openaiUpstream5xxRetry.transports.unknown')
}

// intercepted/skipped 展示「本次拦截序号 + 同账号进度」；终态事件只有累计次数有意义。
const attemptText = (entry: OpenAIUpstream5xxRetryLogEntry) => {
  if (isTerminal(entry.event)) return t('admin.openaiUpstream5xxRetry.attemptTerminal', { n: entry.retry_count })
  if (entry.event === 'skipped') return '-'
  if (!entry.same_account_attempt) return t('admin.openaiUpstream5xxRetry.attemptSwitch', { attempt: entry.attempt })
  return t('admin.openaiUpstream5xxRetry.attemptValue', {
    attempt: entry.attempt,
    same: entry.same_account_attempt,
    max: entry.same_account_max,
  })
}

const messageText = (entry: OpenAIUpstream5xxRetryLogEntry) => {
  const messages = [entry.final_message, entry.upstream_message].filter((value, index, values) => value && values.indexOf(value) === index).join('; ')
  if (!entry.stop_reason) return messages || '-'
  const [reason, ...details] = entry.stop_reason.split(':')
  const key = `admin.openaiUpstream5xxRetry.stopReasons.${reason}`
  const label = te(key) ? t(key) : reason
  const stop = [label, ...details].join(': ')
  return messages ? `${stop}; ${messages}` : stop
}

const wsRetryText = (entry: OpenAIUpstream5xxRetryLogEntry) => {
  if (!entry.ws_retry_count) return t('admin.openaiUpstream5xxRetry.wsRetry.none')
  const status = entry.ws_retry_status || 'retrying'
  return t(`admin.openaiUpstream5xxRetry.wsRetry.${status}`, { n: entry.ws_retry_count })
}

const eventClass = (event: string) => {
  switch (event) {
    case 'succeeded':
      return 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-300'
    case 'exhausted':
      return 'bg-red-50 text-red-700 dark:bg-red-950/40 dark:text-red-300'
    case 'skipped':
      return 'bg-gray-100 text-gray-600 dark:bg-dark-700 dark:text-gray-300'
    default:
      return 'bg-amber-50 text-amber-700 dark:bg-amber-950/40 dark:text-amber-300'
  }
}

const clientStatusClass = (entry: OpenAIUpstream5xxRetryLogEntry) => {
  if (!isTerminal(entry.event)) return 'text-gray-400'
  return entry.status_code >= 200 && entry.status_code < 300
    ? 'text-emerald-600 dark:text-emerald-400'
    : 'text-red-600 dark:text-red-400'
}
</script>
