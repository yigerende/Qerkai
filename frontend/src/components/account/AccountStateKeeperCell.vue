<script setup lang="ts">
import { computed, ref } from 'vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import type { StateKeeperEvent, StateKeeperRecent } from '@/api/admin/openaiStateKeeper'
import { formatDateTime } from '@/utils/format'

const props = defineProps<{ accountId: number; recent?: StateKeeperRecent; loading?: boolean; error?: boolean }>()
const opened = ref(false)
const validity = computed(() => props.recent?.scheduling_state)
const validityText = computed(() => {
  const state = validity.value
  if (!state) return ''
  if (state.status !== 'valid') return ({ missing: '未采集', expired: '已过期', unavailable: '不可用' })[state.status]
  const minutes = Math.max(0, Math.ceil((Date.parse(state.expires_at!) - Date.parse(state.checked_at)) / 60000))
  return `有效，剩余 ${minutes} 分钟`
})
const lanes = computed(() => [
  { key: 'collection', label: '采集', events: props.recent?.collections || [] },
  { key: 'injection', label: '注入', events: props.recent?.injections || [] },
])
const slots = (events: StateKeeperEvent[]) => [...Array<null>(10 - Math.min(10, events.length)).fill(null), ...events.slice(0, 10).reverse()]
const resultName = (result: string) => ({ collected: '已保存', sent: '已携带发送', degraded_signal: '降智长度信号', filtered: '长度不符', failed: '失败', upstream_error: '上游错误', not_observed: '未取得响应头', cancelled: '已取消' }[result] || result)
const sourceName = (source: string) => ({ manual: '手动', timer: '定时', degradation_scan: '降智扫描', response: '返回信号', automatic_retry: '自动恢复', credentials_updated: '凭据更新', http: 'HTTP', ws: 'WS', quality_http: '降智检测' }[source] || source)
const describe = (event: StateKeeperEvent) => [formatDateTime(event.at), event.model, resultName(event.result), sourceName(event.source), event.attempt ? `本轮第 ${event.attempt} 次` : '', event.injected_length ? `注入长度 ${event.injected_length}` : '', `返回长度 ${event.turn_state_length || '-'}`, event.message].filter(Boolean).join(' | ')
const color = (event: StateKeeperEvent | null) => !event ? 'bg-gray-200 dark:bg-dark-600' : event.result === 'collected' ? 'bg-emerald-500' : event.result === 'sent' ? 'bg-sky-500' : event.result === 'cancelled' ? 'bg-gray-400' : 'bg-red-500'
</script>

<template>
  <span v-if="error" class="text-xs text-gray-400">状态读取失败</span>
  <div v-else class="space-y-1" :class="validity ? 'w-64' : 'w-40'" :aria-busy="loading">
    <div v-for="lane in lanes" :key="lane.key">
      <div v-if="lane.key === 'collection' && validity" class="break-words text-[11px] leading-4 [overflow-wrap:anywhere]" :class="validity.status === 'valid' ? 'text-emerald-600' : 'text-amber-600'" :title="validity.reason">{{ validity.model }}：{{ validityText }}</div>
      <div class="flex h-4 items-center gap-1 whitespace-nowrap text-[11px] text-gray-500"><span>{{ lane.label }}</span><span>{{ lane.events[0] ? formatDateTime(lane.events[0].at).slice(5) : loading ? '读取中' : '-' }}</span></div>
      <button type="button" class="flex h-4 items-center gap-1" :aria-label="`查看最近${lane.label}记录`" @click="opened = true">
        <span v-for="(event, index) in slots(lane.events)" :key="index" class="h-3 w-1.5 shrink-0 rounded-sm" :class="color(event)" :data-state-result="event?.result || 'empty'" :title="event ? describe(event) : '暂无记录'" />
      </button>
    </div>
    <span v-if="recent?.paused" class="block text-[11px] text-amber-600" :title="recent.pause_reason">采集已暂停</span>
  </div>
  <BaseDialog :show="opened" :title="`最近采集 / 注入 #${accountId}`" width="wide" @close="opened = false">
    <p v-if="recent?.paused" class="mb-3 text-sm text-amber-600">{{ recent.pause_reason }}</p>
    <section v-for="lane in lanes" :key="lane.key" class="mb-4">
      <h3 class="mb-2 text-sm font-semibold">{{ lane.label }}</h3>
      <p v-if="!lane.events.length" class="py-3 text-sm text-gray-500">暂无记录</p>
      <ol v-else class="divide-y divide-gray-100 dark:divide-dark-700">
        <li v-for="(event, index) in lane.events" :key="index" class="py-2 text-sm">
          <p class="flex flex-wrap gap-x-3 gap-y-1"><span>{{ formatDateTime(event.at) }}</span><span>{{ resultName(event.result) }}</span><span>{{ sourceName(event.source) }}</span><span v-if="event.attempt">第 {{ event.attempt }} 次</span></p>
          <p class="break-all text-xs font-medium">{{ event.model }}</p>
          <p class="text-xs text-gray-500">返回长度 {{ event.turn_state_length || '-' }}<template v-if="event.injected_length"> · 注入长度 {{ event.injected_length }}</template><template v-if="event.http_status"> · HTTP {{ event.http_status }}</template></p>
          <p class="break-words text-xs text-gray-500">{{ event.message }}</p>
        </li>
      </ol>
    </section>
  </BaseDialog>
</template>
