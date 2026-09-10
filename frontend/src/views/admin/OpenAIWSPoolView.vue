<template>
  <AppLayout>
    <div class="w-full min-w-0 space-y-5 pb-8">
      <header class="flex flex-col gap-4 border-b border-gray-200 pb-5 dark:border-dark-700 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <div class="flex items-center gap-3">
            <span class="flex h-9 w-9 items-center justify-center rounded-lg bg-emerald-50 text-emerald-600 dark:bg-emerald-950/50 dark:text-emerald-400"><Icon name="server" size="md" /></span>
            <div>
              <h1 class="text-xl font-bold text-gray-900 dark:text-white">{{ t('admin.openaiWSPool.title') }}</h1>
              <p class="mt-0.5 text-xs text-gray-500 dark:text-gray-400">{{ t('admin.openaiWSPool.description') }}</p>
            </div>
          </div>
        </div>
        <div class="flex items-center gap-3">
          <span class="inline-flex items-center gap-1.5 text-xs text-gray-500"><span class="h-2 w-2 rounded-full" :class="snapshot?.optimization_enabled ? 'bg-emerald-500' : 'bg-gray-400'" />{{ modeText }}</span>
          <span class="text-xs text-gray-400">{{ updatedText }}</span>
          <button class="btn btn-secondary h-9 w-9 p-0" :disabled="loading" :title="t('admin.openaiWSPool.refresh')" @click="load"><Icon name="refresh" size="sm" :class="loading ? 'animate-spin' : ''" /></button>
        </div>
      </header>

      <div class="grid grid-cols-2 gap-3 lg:grid-cols-4 xl:grid-cols-7">
        <div v-for="item in summary" :key="item.label" class="rounded-lg border border-gray-200 bg-white px-4 py-3 dark:border-dark-700 dark:bg-dark-800">
          <div class="text-xs text-gray-500 dark:text-gray-400">{{ item.label }}</div>
          <div class="mt-1 text-2xl font-semibold tabular-nums text-gray-900 dark:text-white">{{ item.value }}</div>
        </div>
      </div>

      <div v-if="loadError" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/30 dark:text-red-300">{{ t('admin.openaiWSPool.loadFailed') }}</div>

      <section class="rounded-lg border border-gray-200 bg-white px-4 py-4 dark:border-dark-700 dark:bg-dark-800">
        <h2 class="mb-3 text-sm font-semibold text-gray-900 dark:text-white">{{ t('admin.openaiWSPool.handshake.title') }}</h2>
        <div class="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3 xl:grid-cols-6">
          <div v-for="item in handshakeSummary" :key="item.label" class="min-w-0 border-l-2 border-gray-200 pl-3 dark:border-dark-600">
            <div class="text-xs text-gray-500 dark:text-gray-400">{{ item.label }}</div>
            <div class="mt-0.5 text-lg font-semibold tabular-nums text-gray-900 dark:text-white">{{ item.value }}</div>
          </div>
        </div>
      </section>

      <div v-if="!snapshot?.force_ws_enabled" class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300">
        {{ t('admin.openaiWSPool.forceDisabled') }}
      </div>
      <div v-else-if="!snapshot?.optimization_enabled" class="rounded-lg border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-800 dark:border-blue-900 dark:bg-blue-950/30 dark:text-blue-300">
        {{ t('admin.openaiWSPool.optimizationDisabled') }}
      </div>

      <section class="overflow-hidden rounded-lg border border-gray-200 bg-white dark:border-dark-700 dark:bg-dark-800">
        <div class="flex items-center justify-between border-b border-gray-200 px-4 py-3 dark:border-dark-700">
          <h2 class="text-sm font-semibold text-gray-900 dark:text-white">{{ t('admin.openaiWSPool.accounts.title') }}</h2>
          <span class="text-xs text-gray-400">{{ t('admin.openaiWSPool.accounts.activeCount', { count: snapshot?.accounts?.length || 0 }) }}</span>
        </div>
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200 text-sm dark:divide-dark-700">
            <thead class="bg-gray-50 text-left text-xs font-medium text-gray-500 dark:bg-dark-700/60 dark:text-gray-400">
              <tr><th class="w-10 px-3 py-3"></th><th class="px-3 py-3">{{ t('admin.openaiWSPool.columns.account') }}</th><th class="px-3 py-3">{{ t('admin.openaiWSPool.columns.status') }}</th><th class="px-3 py-3">{{ t('admin.openaiWSPool.columns.connections') }}</th><th class="px-3 py-3">{{ t('admin.openaiWSPool.columns.inUse') }}</th><th class="px-3 py-3">{{ t('admin.openaiWSPool.columns.session') }}</th><th class="px-3 py-3">{{ t('admin.openaiWSPool.columns.inventory') }}</th><th class="px-3 py-3">{{ t('admin.openaiWSPool.columns.creating') }}</th><th class="px-3 py-3">{{ t('admin.openaiWSPool.columns.queue') }}</th></tr>
            </thead>
            <tbody class="divide-y divide-gray-100 dark:divide-dark-700">
              <template v-for="account in snapshot?.accounts || []" :key="account.account_id">
                <tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-dark-700/40" @click="toggle(account.account_id)">
                  <td class="px-3 py-3 text-gray-400"><Icon name="chevronRight" size="sm" class="transition-transform" :class="expanded.has(account.account_id) ? 'rotate-90' : ''" /></td>
                  <td class="px-3 py-3"><div class="font-medium text-gray-900 dark:text-white">{{ account.account_name || t('admin.openaiWSPool.accountFallback', { id: account.account_id }) }}</div><div class="text-xs text-gray-400">{{ t('admin.openaiWSPool.idLabel', { id: account.account_id }) }}</div></td>
                  <td class="px-3 py-3"><span class="rounded px-2 py-1 text-xs" :class="statusClass(account.status)">{{ statusText(account.status) }}</span></td>
                  <td class="px-3 py-3 tabular-nums">{{ account.total }}</td><td class="px-3 py-3 tabular-nums">{{ account.in_use }}</td><td class="px-3 py-3 tabular-nums">{{ account.session_primary }} / {{ account.session_standby }}</td><td class="px-3 py-3 tabular-nums">{{ account.prewarm }} / {{ account.standby }}</td><td class="px-3 py-3 tabular-nums">{{ account.creating }}</td><td class="px-3 py-3 tabular-nums">{{ account.waiters }}</td>
                </tr>
                <tr v-if="expanded.has(account.account_id)"><td colspan="9" class="bg-gray-50 px-5 py-4 dark:bg-dark-900/30">
                  <div class="overflow-x-auto rounded border border-gray-200 dark:border-dark-700">
                    <table class="min-w-full text-xs"><thead class="bg-white text-gray-500 dark:bg-dark-800"><tr><th class="px-3 py-2 text-left">{{ t('admin.openaiWSPool.columns.connectionId') }}</th><th class="px-3 py-2 text-left">{{ t('admin.openaiWSPool.columns.role') }}</th><th class="px-3 py-2 text-left">{{ t('admin.openaiWSPool.columns.sessionId') }}</th><th class="px-3 py-2 text-left">{{ t('admin.openaiWSPool.columns.status') }}</th><th class="px-3 py-2 text-left">{{ t('admin.openaiWSPool.columns.age') }}</th><th class="px-3 py-2 text-left">{{ t('admin.openaiWSPool.columns.idle') }}</th><th class="px-3 py-2 text-left">{{ t('admin.openaiWSPool.columns.unbindIn') }}</th><th class="px-3 py-2 text-left">{{ t('admin.openaiWSPool.columns.waiters') }}</th></tr></thead>
                      <tbody class="divide-y divide-gray-200 bg-white dark:divide-dark-700 dark:bg-dark-800"><tr v-for="conn in account.connections" :key="conn.id"><td class="px-3 py-2 font-mono text-gray-700 dark:text-gray-200">{{ conn.id }}</td><td class="px-3 py-2">{{ roleText(conn.role) }}</td><td class="px-3 py-2 font-mono text-gray-500">{{ conn.session || '-' }}</td><td class="px-3 py-2"><span :class="conn.state === 'in_use' ? 'text-emerald-600' : 'text-gray-500'">{{ t(`admin.openaiWSPool.connectionState.${conn.state === 'in_use' ? 'inUse' : 'idle'}`) }}</span></td><td class="px-3 py-2 tabular-nums">{{ duration(conn.age_seconds) }}</td><td class="px-3 py-2 tabular-nums">{{ duration(conn.idle_seconds) }}</td><td class="px-3 py-2 tabular-nums" :class="conn.unbind_at ? 'text-amber-600 dark:text-amber-300' : 'text-gray-400'">{{ conn.unbind_at ? countdown(conn.unbind_in_seconds) : '-' }}</td><td class="px-3 py-2 tabular-nums">{{ conn.waiters }}</td></tr><tr v-if="!account.connections.length"><td colspan="8" class="px-3 py-6 text-center text-gray-400">{{ t('admin.openaiWSPool.noConnections') }}</td></tr></tbody>
                    </table>
                  </div>
                </td></tr>
              </template>
              <tr v-if="!loading && !(snapshot?.accounts?.length)"><td colspan="9" class="px-4 py-12 text-center text-gray-400">{{ t('admin.openaiWSPool.accounts.empty') }}</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import AppLayout from '@/components/layout/AppLayout.vue'
import Icon from '@/components/icons/Icon.vue'
import { settingsAPI, type OpenAIWSPoolOps } from '@/api/admin/settings'

const snapshot = ref<OpenAIWSPoolOps | null>(null)
const loading = ref(false)
const loadError = ref(false)
const expanded = ref(new Set<number>())
const { t } = useI18n()
let timer: number | undefined

const load = async () => { if (loading.value) return; loading.value = true; try { snapshot.value = await settingsAPI.getOpenAIWSPoolOps(); loadError.value = false } catch { loadError.value = true } finally { loading.value = false } }
const tick = () => { if (document.visibilityState === 'visible') void load() }
const onVisibility = () => { if (document.visibilityState === 'visible') void load() }
onMounted(() => { void load(); timer = window.setInterval(tick, 5000); document.addEventListener('visibilitychange', onVisibility) })
onBeforeUnmount(() => { if (timer) window.clearInterval(timer); document.removeEventListener('visibilitychange', onVisibility) })
const toggle = (id: number) => { const next = new Set(expanded.value); next.has(id) ? next.delete(id) : next.add(id); expanded.value = next }
const reuseRate = computed(() => { const m = snapshot.value?.metrics; return m?.AcquireTotal ? `${((m.AcquireReuseTotal / m.AcquireTotal) * 100).toFixed(1)}%` : '0%' })
const averageQueue = computed(() => { const m = snapshot.value?.metrics; return m?.AcquireQueueWaitTotal ? `${Math.round(m.AcquireQueueWaitMsTotal / m.AcquireQueueWaitTotal)} ms` : '0 ms' })
const summary = computed(() => [{ label: t('admin.openaiWSPool.summary.totalConnections'), value: snapshot.value?.total_connections || 0 }, { label: t('admin.openaiWSPool.summary.inUse'), value: snapshot.value?.total_in_use || 0 }, { label: t('admin.openaiWSPool.summary.sessionOwned'), value: snapshot.value?.total_session || 0 }, { label: t('admin.openaiWSPool.summary.prewarm'), value: snapshot.value?.total_prewarm || 0 }, { label: t('admin.openaiWSPool.summary.standby'), value: snapshot.value?.total_standby || 0 }, { label: t('admin.openaiWSPool.summary.reuseRate'), value: reuseRate.value }, { label: t('admin.openaiWSPool.summary.averageQueue'), value: averageQueue.value }])
const handshakeSummary = computed(() => { const m = snapshot.value?.metrics; return [{ label: t('admin.openaiWSPool.handshake.total'), value: m?.DialTotal || 0 }, { label: t('admin.openaiWSPool.handshake.success'), value: m?.DialSuccessTotal || 0 }, { label: t('admin.openaiWSPool.handshake.failure'), value: m?.DialFailureTotal || 0 }, { label: t('admin.openaiWSPool.handshake.forbidden'), value: m?.Dial403Total || 0 }, { label: t('admin.openaiWSPool.handshake.rateLimited'), value: m?.Dial429Total || 0 }, { label: t('admin.openaiWSPool.handshake.average'), value: m?.DialTotal ? `${Math.round(m.DialMsTotal / m.DialTotal)} ms` : '0 ms' }] })
const modeText = computed(() => !snapshot.value?.force_ws_enabled ? t('admin.openaiWSPool.modes.forceDisabled') : snapshot.value.optimization_enabled ? t('admin.openaiWSPool.modes.optimized') : t('admin.openaiWSPool.modes.legacy'))
const updatedText = computed(() => snapshot.value?.generated_at ? t('admin.openaiWSPool.updatedAt', { time: new Date(snapshot.value.generated_at * 1000).toLocaleTimeString() }) : '')
const duration = (seconds: number) => seconds < 60 ? `${seconds}s` : seconds < 3600 ? `${Math.floor(seconds / 60)}m` : `${Math.floor(seconds / 3600)}h`
const countdown = (seconds: number) => seconds < 60 ? `${seconds}s` : seconds < 3600 ? `${Math.floor(seconds / 60)}m ${seconds % 60}s` : `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`
const roleText = (role: string) => ({ prewarm: t('admin.openaiWSPool.roles.prewarm'), standby: t('admin.openaiWSPool.roles.standby'), session_primary: t('admin.openaiWSPool.roles.sessionPrimary'), session_standby: t('admin.openaiWSPool.roles.sessionStandby'), legacy: t('admin.openaiWSPool.roles.legacy') }[role] || role)
const statusText = (status: string) => ({ healthy: t('admin.openaiWSPool.status.healthy'), replenishing: t('admin.openaiWSPool.status.replenishing'), cooldown: t('admin.openaiWSPool.status.cooldown') }[status] || status)
const statusClass = (status: string) => status === 'healthy' ? 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-300' : status === 'replenishing' ? 'bg-blue-50 text-blue-700 dark:bg-blue-950/40 dark:text-blue-300' : 'bg-amber-50 text-amber-700 dark:bg-amber-950/40 dark:text-amber-300'
</script>
