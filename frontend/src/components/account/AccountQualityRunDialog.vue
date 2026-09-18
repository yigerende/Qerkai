<template>
  <BaseDialog :show="visible" title="批量降智检测" width="wide" @close="visible = false">
    <template v-if="stage === 'confirm'">
      <p class="text-sm text-gray-700 dark:text-gray-300">确认按已保存的降智检测配置，检测所选 {{ rows.length }} 个账号？</p>
    </template>
    <template v-else>
      <div class="flex flex-wrap justify-between gap-2 text-sm" aria-live="polite">
        <span>{{ submitting ? '正在提交' : active ? '检测中' : '本批已结束' }} · {{ completed }} / {{ rows.length }}</span>
        <span>异常 {{ counts.abnormal }} · 失败 {{ counts.failed }} · 跳过 {{ counts.skipped }} · 中断 {{ counts.interrupted }}</span>
      </div>
      <progress class="my-3 h-2 w-full accent-primary-600" :value="completed" :max="rows.length || 1" aria-label="检测进度" />
      <p v-if="error" role="alert" class="mb-3 break-words text-sm text-red-600">{{ error }}</p>
      <div class="max-h-[50vh] overflow-auto">
        <table class="w-full table-fixed text-left text-sm">
          <thead class="sticky top-0 bg-white dark:bg-dark-800"><tr><th class="w-2/5 py-2">账号</th><th class="w-[30%] py-2">答题</th><th class="w-[30%] py-2">模型一致性</th></tr></thead>
          <tbody>
            <tr v-for="row in pageRows" :key="row.id" class="border-t border-gray-100 align-top dark:border-dark-700">
              <td class="break-words py-2 pr-3">{{ row.name }}<div class="text-xs text-gray-500">#{{ row.id }}</div></td>
              <td v-if="row.outcome" colspan="2" class="break-words py-2 text-gray-500">{{ outcomeLabels[row.outcome] }}：{{ row.issue }}</td>
              <template v-else>
                <td v-for="kind in (['question', 'model'] as const)" :key="kind" class="break-words py-2 pr-2">
                  <span :class="row[kind].verdict?.status === 'error' ? 'text-red-600' : row[kind].verdict?.degraded || row[kind].verdict?.status === 'suspect' ? 'text-amber-600' : ''">{{ checkLabel(row[kind]) }}</span>
                  <div v-if="row[kind].verdict?.error" class="mt-1 text-xs text-red-600">{{ row[kind].verdict?.error }}</div>
                </td>
              </template>
            </tr>
          </tbody>
        </table>
      </div>
      <Pagination v-if="rows.length > 50" :page="page" :total="rows.length" :page-size="50" :page-size-options="[50]" @update:page="page = $event" />
    </template>
    <template #footer>
      <button class="btn btn-secondary" @click="visible = false">{{ stage === 'confirm' ? '取消' : '关闭' }}</button>
      <button v-if="stage === 'confirm'" class="btn btn-primary" @click="submit">开始检测</button>
    </template>
  </BaseDialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import Pagination from '@/components/common/Pagination.vue'
import { qualityStatusLabel, type QualityResult } from '@/api/admin/accountQuality'
import { useSelectedAccountQuality, type SelectedQualityCheck } from '@/composables/useSelectedAccountQuality'

const emit = defineEmits<{
  results: [results: QualityResult[]]
  visibility: [visible: boolean]
  progress: [value: { total: number; done: number; active: boolean }]
}>()
const { visible, stage, rows, submitting, error, completed, counts, active, prepare, submit } = useSelectedAccountQuality(results => emit('results', results))
const page = ref(1)
const pageRows = computed(() => rows.value.slice((page.value - 1) * 50, page.value * 50))
const outcomeLabels = { skipped: '跳过', error: '提交失败', interrupted: '已中断' }
function checkLabel(check: SelectedQualityCheck) {
  return check.state === 'done' ? qualityStatusLabel(check.verdict) : ({ queued: '排队中', running: '检测中', disabled: '未启用' })[check.state]
}
watch(visible, value => emit('visibility', value))
watch([completed, active, () => rows.value.length, stage], () => {
  if (stage.value === 'run') emit('progress', { total: rows.value.length, done: completed.value, active: active.value })
})
defineExpose({ open: (ids: number[]) => { page.value = 1; prepare(ids) }, reopen: () => { visible.value = true } })
</script>
