<script setup lang="ts">
import { ref } from 'vue'
import { qualityAPI, qualityStatusLabel, qualityExecutionLabel, qualityOverallLabel, type QualityResult } from '@/api/admin/accountQuality'
const props = defineProps<{ accountId: number; result?: QualityResult; error?: boolean }>()
const opened = ref(false), loading = ref(false), failure = ref('')
const history = ref<QualityResult[]>([])
const date = (v?: string) => v ? new Date(v).toLocaleString() : '-'
async function show() {
 opened.value = true; loading.value = true; failure.value = ''
 try { history.value = (await qualityAPI.history(props.accountId)).items }
 catch { failure.value = '检测记录读取失败' } finally { loading.value = false }
}
</script>
<template>
 <button type="button" class="text-left text-xs leading-5" title="查看降智检测记录" @click="show">
  <span v-if="error" class="text-red-600">状态读取失败</span>
  <template v-else>
   <span class="block" :class="result?.question?.degraded ? 'text-red-600' : 'text-gray-600 dark:text-gray-300'">答题：{{ qualityExecutionLabel(result?.question_execution, result?.question) }}</span>
   <span class="block" :class="result?.model?.degraded ? 'text-red-600' : 'text-gray-500'">模型：{{ qualityExecutionLabel(result?.model_execution, result?.model) }}</span>
   <span class="block" :title="result?.overall?.reason" :class="result?.overall?.status === 'degraded' ? 'text-red-600' : result?.overall?.status === 'normal' ? 'text-emerald-600' : 'text-gray-500'">综合：{{ qualityOverallLabel(result?.overall?.status) }}</span>
   <span v-if="result?.scheduling?.paused" class="block text-amber-600" :title="result.scheduling.error || '等待后台复检恢复'">调度：{{ result.scheduling.state_required ? 'State / 复检暂停' : '降智暂停' }} · 连续正常 {{ result.scheduling.successes }}</span>
  </template>
 </button>
 <Teleport to="body">
  <div v-if="opened" class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" @click.self="opened = false" @keydown.esc="opened = false">
   <section role="dialog" aria-modal="true" aria-label="降智检测记录" class="max-h-[85vh] w-full max-w-3xl overflow-auto rounded-lg bg-white p-5 dark:bg-dark-900">
    <div class="flex items-center justify-between"><h2 class="text-lg font-semibold">降智检测记录 #{{ accountId }}</h2><button class="btn btn-secondary" @click="opened = false">关闭</button></div>
    <p class="mt-3 text-sm">综合：{{ qualityOverallLabel(result?.overall?.status) }} · {{ result?.overall?.reason || '等待检测证据' }}</p>
    <p v-if="result?.scheduling?.paused" class="mt-2 text-sm text-amber-600">{{ result.scheduling.state_required ? 'State / 复检暂停调度' : '降智暂停调度' }} · 连续正常 {{ result.scheduling.successes }}<template v-if="result.scheduling.next_at"> · 下次复检 {{ date(result.scheduling.next_at) }}</template><template v-if="result.scheduling.error"> · {{ result.scheduling.error }}</template></p>
    <p v-if="loading" class="py-6">读取中...</p><p v-else-if="failure" class="py-6 text-red-600">{{ failure }}</p>
    <p v-else-if="!history.length" class="py-6 text-gray-500">暂无检测记录</p>
    <article v-for="item in history" :key="item.version" class="border-b py-4 dark:border-dark-700">
     <p v-if="item.overall?.status" class="mb-1 text-sm">综合：{{ qualityOverallLabel(item.overall.status) }} · {{ item.overall.reason }}</p>
     <p class="mb-2 text-xs text-gray-500">{{ item.detection_kind === 'recovery' ? '调度恢复复检' : item.detection_kind === 'question' ? '答题检测' : item.detection_kind === 'model' ? '模型一致性检测' : item.detection_kind === 'state_refresh' ? 'State 已更新，等待模型验证' : '旧版状态快照（不代表两项都重新检测）' }}<template v-if="item.recorded_at"> · {{ date(item.recorded_at) }}</template></p>
     <template v-if="item.detection_kind !== 'model' && item.detection_kind !== 'state_refresh' && item.question.checked_at">
     <p>{{ item.question.question_name || '答题' }}：{{ qualityStatusLabel(item.question) }} · {{ date(item.question.checked_at) }} · {{ item.question.duration_ms }} ms</p>
     <p class="whitespace-pre-wrap break-words text-sm">{{ item.question.answer || item.question.error || '-' }}</p>
     <p class="text-xs text-gray-500">{{ item.question.reason }} · 连续异常 {{ item.question.failures }} / 连续正常 {{ item.question.successes }}</p>
     </template>
     <template v-if="item.detection_kind !== 'question' && item.model.checked_at">
     <p class="mt-2 text-sm">模型：{{ qualityStatusLabel(item.model) }} · {{ item.model.sent_model || '-' }} → {{ item.model.response_model || '-' }}</p>
     <p class="text-xs text-gray-500">样本 {{ date(item.model.evidence_at) }} · {{ item.model.error || '' }}</p>
     <p v-if="item.model.state_collected_at" class="text-xs text-gray-500">State 更新于 {{ date(item.model.state_collected_at) }}</p>
     </template>
    </article>
   </section>
  </div>
 </Teleport>
</template>
