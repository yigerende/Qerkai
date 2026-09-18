<script setup lang="ts">
import { computed, onUnmounted, ref, watch } from 'vue'
import type { QualityDetectorProgress } from '@/api/admin/accountQuality'
const props = defineProps<{ title: string; value: QualityDetectorProgress; serverNow: string }>()
const anchor = ref({ server: Date.parse(props.serverNow), local: performance.now() })
const now = ref(anchor.value.server)
watch(() => props.serverNow, value => { anchor.value = { server: Date.parse(value), local: performance.now() }; now.value = anchor.value.server })
const timer = setInterval(() => { now.value = anchor.value.server + performance.now() - anchor.value.local }, 1000)
onUnmounted(() => clearInterval(timer))
const batch = computed(() => props.value.batch)
const active = computed(() => props.value.enabled && batch.value?.status === 'running')
const status = computed(() => !props.value.enabled ? '已关闭' : active.value ? '检测中' : props.value.pending ? '等待调度' : '等待下次检测')
const elapsed = (value: string) => Math.max(0, Math.floor((now.value - Date.parse(value)) / 1000))
const next = computed(() => props.value.next_at ? Math.max(0, Math.ceil((Date.parse(props.value.next_at) - now.value) / 1000)) : null)
const date = (value?: string) => value ? new Date(value).toLocaleString() : '-'
</script>
<template>
 <section class="min-w-0 rounded-lg border border-gray-200 p-4 dark:border-dark-700" :aria-label="title + '进度'">
  <div class="flex items-center justify-between gap-2"><h2 class="font-medium">{{ title }}</h2><span role="status" class="text-sm text-primary-600">{{ status }}</span></div>
  <p class="my-2 text-sm">当前配置已检测 {{ value.checked }}/{{ value.total }} · 未检测 {{ value.unchecked }} · 待调度 {{ value.pending }}</p>
  <template v-if="batch">
   <p class="text-sm">{{ active ? '本批' : '最近一批' }}完成 {{ batch.done }}/{{ batch.total }} · 检测失败 {{ batch.failed }} · 跳过 {{ batch.skipped }}</p>
   <progress class="my-2 h-2 w-full" :value="batch.done" :max="Math.max(1,batch.total)" />
   <p v-if="batch.status === 'interrupted'" class="text-sm text-amber-600">上一批中断，未完成账号将重新排队。</p>
   <p v-if="batch.last_error" class="break-words text-sm text-red-600">最近错误：{{ batch.last_error }}</p>
   <div v-if="active" class="mt-2 flex flex-wrap gap-2 text-xs"><span v-for="item in batch.running" :key="item.account_id" class="rounded bg-gray-100 px-2 py-1 dark:bg-dark-700">账号 #{{ item.account_id }} · {{ elapsed(item.started_at) }}s</span></div>
   <p class="mt-2 text-xs text-gray-500">开始：{{ date(batch.started_at) }}<template v-if="batch.finished_at"> · 结束：{{ date(batch.finished_at) }}</template></p>
  </template>
  <p v-if="value.enabled && !active && !value.pending && next !== null" class="mt-2 text-xs text-gray-500">下次检测：{{ next > 0 ? next + 's' : '即将调度' }}</p>
 </section>
</template>
