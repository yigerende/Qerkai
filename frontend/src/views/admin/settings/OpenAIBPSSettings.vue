<template>
  <div class="space-y-4" data-testid="openai-bps-settings">
    <div class="flex items-center justify-between gap-4">
      <div>
        <label class="text-sm font-medium text-gray-700 dark:text-gray-300">启用 OpenAI BPS 端点</label>
        <p class="mt-0.5 text-xs text-gray-500 dark:text-gray-400">仅对所选分组、模型的 OpenAI OAuth 账号生效，使用 BPS HTTP/SSE。与强制 WS、WS 优化调度互斥，开启时关闭另一套；关闭或未命中时沿用原流程。</p>
      </div>
      <Toggle :model-value="modelValue.enabled" @update:model-value="update({ enabled: $event })" />
    </div>
    <div v-if="modelValue.enabled" class="space-y-4 border-l-2 border-primary-200 pl-4 dark:border-primary-800">
      <ForceOpenAIWSGroups :model-value="modelValue.group_ids" scope-name="bps-group-scope" @update:model-value="update({ group_ids: $event })" />
      <label class="block">
        <span class="mb-1.5 block text-sm font-medium">启用模型（每行一个）</span>
        <textarea v-model="modelsText" rows="4" class="input w-full font-mono" placeholder="gpt-6-astra" />
        <span class="mt-1 block text-xs text-gray-500">普通模型默认请求同名上游；以 -bps 或 -basispoints 结尾的别名默认使用高级配置的上游模型，也可单独设置映射。实际可用性取决于账号权限。</span>
      </label>
      <details class="space-y-3 rounded-lg bg-gray-50 p-3 dark:bg-dark-700/50">
        <summary class="cursor-pointer text-sm">高级配置</summary>
        <label class="block text-sm">模型映射（每行 客户端模型=上游模型）<textarea v-model="mappingsText" rows="3" class="input mt-1 w-full font-mono" placeholder="astra-bps=gpt-6-astra" /></label>
        <label class="block text-sm">别名默认上游模型<input :value="modelValue.upstream_model" class="input mt-1 w-full" @input="update({ upstream_model: inputText($event) })" /></label>
        <label class="block text-sm">BPS Responses 地址<input :value="modelValue.responses_url" class="input mt-1 w-full" @input="update({ responses_url: inputText($event) })" /></label>
        <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <label class="text-sm">超时（秒）<input :value="modelValue.timeout_seconds" type="number" min="10" max="1800" class="input mt-1 w-full" @input="update({ timeout_seconds: Number(inputText($event)) })" /></label>
          <label class="text-sm">响应上限（MiB）<input :value="modelValue.max_response_bytes / 1048576" type="number" min="0.0625" max="128" step="0.0625" class="input mt-1 w-full" @input="update({ max_response_bytes: Math.round(Number(inputText($event)) * 1048576) })" /></label>
          <label class="text-sm">认证模式<input :value="modelValue.auth_mode" class="input mt-1 w-full" @input="update({ auth_mode: inputText($event) })" /></label>
          <label class="text-sm">工具目录版本（可选）<input :value="modelValue.tools_version_id" class="input mt-1 w-full" @input="update({ tools_version_id: inputText($event) })" /></label>
        </div>
      </details>
      <p class="text-xs text-gray-500">协议对齐 CPA BPS v0.1.18：支持流式、图片和客户端工具。不支持 Fast、独立 compact、仅 ID 续聊、多代理 v2 密文及 JSON Schema 输出。不会自动回退到原端点。</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import Toggle from '@/components/common/Toggle.vue'
import ForceOpenAIWSGroups from './ForceOpenAIWSGroups.vue'
import type { OpenAIBPSSettings } from '@/api/admin/settings'
const props = defineProps<{ modelValue: OpenAIBPSSettings }>()
const emit = defineEmits<{ 'update:modelValue': [value: OpenAIBPSSettings] }>()
const update = (value: Partial<OpenAIBPSSettings>) => emit('update:modelValue', { ...props.modelValue, ...value })
const inputText = (event: Event) => (event.target as HTMLInputElement).value
const modelsText = computed({
  get: () => props.modelValue.models.join('\n'),
  set: (text: string) => {
    const models = text.split(/\r?\n/)
    const allowed = new Set(models.map(m => m.trim()))
    update({ models, model_mappings: Object.fromEntries(Object.entries(props.modelValue.model_mappings ?? {}).filter(([key]) => allowed.has(key))) })
  }
})
const mappingsText = computed({
  get: () => Object.entries(props.modelValue.model_mappings ?? {}).filter(([key, value]) => key !== value).map(([key, value]) => `${key}=${value}`).join('\n'),
  set: (text: string) => update({ model_mappings: Object.fromEntries(text.split(/\r?\n/).filter(line => line.trim()).map(line => {
    const at = line.indexOf('='); return at < 0 ? [line.trim(), ''] : [line.slice(0, at).trim(), line.slice(at + 1).trim()]
  })) })
})
</script>
