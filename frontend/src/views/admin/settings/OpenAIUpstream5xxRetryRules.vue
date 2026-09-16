<template>
  <section class="min-w-0 space-y-3 border-t border-gray-200 pt-4 dark:border-dark-700">
    <div class="flex flex-wrap items-center justify-between gap-2">
      <h3 class="text-sm font-medium">{{ t(`${base}.title`) }}</h3>
      <div class="flex items-center gap-2">
        <button type="button" class="btn btn-secondary h-8 w-8 p-0" :title="t(`${base}.restore`)" :aria-label="t(`${base}.restore`)" @click="emit('update:modelValue', defaultOpenAIUpstream5xxRetryRules())"><Icon name="refresh" size="sm" /></button>
        <button type="button" class="btn btn-secondary h-8 gap-1 px-2 text-xs" :disabled="modelValue.length >= 32" @click="addRule"><Icon name="plus" size="sm" />{{ t(`${base}.add`) }}</button>
      </div>
    </div>
    <p v-if="!modelValue.length" class="text-sm text-gray-500">{{ t(`${base}.empty`) }}</p>
    <div v-for="(rule, index) in modelValue" :key="rule.id" class="min-w-0 space-y-3 border-b border-gray-200 pb-4 dark:border-dark-700" :data-rule-id="rule.id">
      <div class="flex flex-wrap items-end gap-3">
        <label class="flex h-9 items-center gap-2 text-xs"><input type="checkbox" :checked="rule.enabled" @change="update(index, { enabled: ($event.target as HTMLInputElement).checked })" />{{ t('common.enabled') }}</label>
        <label class="min-w-0 flex-1 basis-48 text-xs">{{ t(`${base}.name`) }}<input class="input mt-1 block h-9 py-1 text-sm" :value="rule.name" maxlength="80" required @input="update(index, { name: ($event.target as HTMLInputElement).value })" /></label>
        <label class="text-xs">{{ t(`${base}.status`) }}<select class="input mt-1 block h-9 w-24 py-1 text-sm" :value="rule.status_code" @change="update(index, { status_code: Number(($event.target as HTMLSelectElement).value) as 502 | 503 })"><option :value="502">502</option><option :value="503">503</option></select></label>
        <label class="text-xs">{{ t(`${base}.mode`) }}<select class="input mt-1 block h-9 w-36 py-1 text-sm" :value="rule.match_mode" @change="update(index, { match_mode: ($event.target as HTMLSelectElement).value as 'all' | 'any' })"><option value="all">{{ t(`${base}.all`) }}</option><option value="any">{{ t(`${base}.any`) }}</option></select></label>
        <div class="flex h-9 shrink-0 items-center gap-1">
          <button type="button" class="btn btn-secondary h-8 w-8 p-0" :disabled="index === 0" :title="t(`${base}.up`)" :aria-label="t(`${base}.up`)" @click="move(index, -1)"><Icon name="arrowUp" size="sm" /></button>
          <button type="button" class="btn btn-secondary h-8 w-8 p-0" :disabled="index === modelValue.length - 1" :title="t(`${base}.down`)" :aria-label="t(`${base}.down`)" @click="move(index, 1)"><Icon name="arrowDown" size="sm" /></button>
          <button type="button" class="btn btn-secondary h-8 w-8 p-0 text-red-600" :title="t(`${base}.remove`)" :aria-label="t(`${base}.remove`)" @click="emit('update:modelValue', modelValue.filter((_, i) => i !== index))"><Icon name="trash" size="sm" /></button>
        </div>
      </div>
      <label class="block text-xs">{{ t(`${base}.keywords`) }}<textarea class="input mt-1 min-h-20 w-full font-mono text-xs" rows="2" :value="rule.keywords.join('\n')" @change="update(index, { keywords: ($event.target as HTMLTextAreaElement).value.split('\n').map(v => v.trim()).filter(Boolean) })" /></label>
    </div>
    <label class="block text-xs">{{ t(`${base}.payload`) }}<textarea v-model="payload" class="input mt-1 min-h-28 w-full font-mono text-xs" rows="4" spellcheck="false" /></label>
    <div class="flex flex-wrap items-center gap-3">
      <button type="button" class="btn btn-secondary h-9 gap-2 px-3 text-xs" :disabled="busy || !payload.trim()" @click="preview"><Icon name="beaker" size="sm" />{{ t(`${base}.preview`) }}</button>
      <span v-if="result" role="status" class="break-words text-sm" :class="result.matched ? 'text-emerald-600' : 'text-amber-600'">{{ result.matched ? `${result.rule_name} · ${result.status_code}` : t(`${base}.reasons.${result.reason}`) }}</span>
      <span v-if="error" role="alert" class="break-words text-sm text-red-600">{{ error }}</span>
    </div>
  </section>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import Icon from '@/components/icons/Icon.vue'
import { defaultOpenAIUpstream5xxRetryRules, previewOpenAIUpstream5xxRetryRule, type OpenAIUpstream5xxRetryRule, type OpenAIUpstream5xxRetryMatch } from '@/api/admin/openaiRetryRules'

const props = defineProps<{ modelValue: OpenAIUpstream5xxRetryRule[] }>()
const emit = defineEmits<{ 'update:modelValue': [OpenAIUpstream5xxRetryRule[]] }>()
const { t } = useI18n()
const base = 'admin.openaiUpstream5xxRetry.rules'
const payload = ref('')
const busy = ref(false)
const result = ref<OpenAIUpstream5xxRetryMatch | null>(null)
const error = ref('')
let revision = 0
watch([() => props.modelValue, payload], () => { revision++; result.value = null; error.value = '' }, { deep: true, flush: 'sync' })

function update(index: number, patch: Partial<OpenAIUpstream5xxRetryRule>) {
  emit('update:modelValue', props.modelValue.map((rule, i) => i === index ? { ...rule, ...patch } : rule))
}
function addRule() {
  const id = globalThis.crypto?.randomUUID?.() ?? `rule_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`
  emit('update:modelValue', [...props.modelValue, { id, name: t(`${base}.newName`), enabled: true, status_code: 503, match_mode: 'all', keywords: [] }])
}
function move(index: number, offset: number) {
  const rules = [...props.modelValue]
  const target = index + offset
  if (target < 0 || target >= rules.length) return
  ;[rules[index], rules[target]] = [rules[target]!, rules[index]!]
  emit('update:modelValue', rules)
}
async function preview() {
  const currentRevision = revision
  busy.value = true
  error.value = ''
  try {
    const response = await previewOpenAIUpstream5xxRetryRule(props.modelValue, payload.value)
    if (currentRevision === revision) result.value = response
  } catch (err: unknown) {
    if (currentRevision === revision) error.value = (err as { message?: string }).message || t(`${base}.failed`)
  } finally {
    busy.value = false
  }
}
</script>
