<script setup lang="ts">
import { ref } from 'vue'
import { qualityAPI, qualityStatusLabel, type QualityResult } from '@/api/admin/accountQuality'
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
   <span class="block" :class="result?.question?.degraded ? 'text-red-600' : 'text-gray-600 dark:text-gray-300'">答题：{{ qualityStatusLabel(result?.question) }}</span>
   <span class="block" :class="result?.model?.degraded ? 'text-red-600' : 'text-gray-500'">模型：{{ qualityStatusLabel(result?.model) }}</span>
  </template>
 </button>
 <Teleport to="body">
  <div v-if="opened" class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" @click.self="opened = false" @keydown.esc="opened = false">
   <section role="dialog" aria-modal="true" aria-label="降智检测记录" class="max-h-[85vh] w-full max-w-3xl overflow-auto rounded-lg bg-white p-5 dark:bg-dark-900">
    <div class="flex items-center justify-between"><h2 class="text-lg font-semibold">降智检测记录 #{{ accountId }}</h2><button class="btn btn-secondary" @click="opened = false">关闭</button></div>
    <p v-if="loading" class="py-6">读取中...</p><p v-else-if="failure" class="py-6 text-red-600">{{ failure }}</p>
    <p v-else-if="!history.length" class="py-6 text-gray-500">暂无检测记录</p>
    <article v-for="item in history" :key="item.version" class="border-b py-4 dark:border-dark-700">
     <p>{{ item.question.question_name || '答题' }}：{{ qualityStatusLabel(item.question) }} · {{ date(item.question.checked_at) }} · {{ item.question.duration_ms }} ms</p>
     <p class="whitespace-pre-wrap break-words text-sm">{{ item.question.answer || item.question.error || '-' }}</p>
     <p class="text-xs text-gray-500">{{ item.question.reason }} · 连续异常 {{ item.question.failures }} / 连续正常 {{ item.question.successes }}</p>
     <p class="mt-2 text-sm">模型：{{ qualityStatusLabel(item.model) }} · {{ item.model.sent_model || '-' }} → {{ item.model.response_model || '-' }}</p>
     <p class="text-xs text-gray-500">样本 {{ date(item.model.evidence_at) }} · {{ item.model.error || '' }}</p>
    </article>
   </section>
  </div>
 </Teleport>
</template>
