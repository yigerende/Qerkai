<template>
  <div data-testid="downstream-model-alignment" class="space-y-4">
    <div class="flex items-center justify-between gap-4">
      <div>
        <label class="text-sm font-medium text-gray-700 dark:text-gray-300">{{ t('admin.settings.gatewayForwarding.downstreamModelAlignment') }}</label>
        <p class="mt-0.5 text-xs text-gray-500 dark:text-gray-400">{{ t('admin.settings.gatewayForwarding.downstreamModelAlignmentHint') }}</p>
      </div>
      <Toggle :model-value="modelValue.enabled" @update:model-value="update({ enabled: $event })" />
    </div>
    <div v-if="modelValue.enabled" class="space-y-4 border-l-2 border-primary-200 pl-4 dark:border-primary-800">
      <ForceOpenAIWSGroups :model-value="modelValue.group_ids" scope-name="downstream-model-group-scope" @update:model-value="update({ group_ids: $event })" />
      <label class="block">
        <span class="mb-1.5 block text-sm font-medium text-gray-700 dark:text-gray-300">{{ t('admin.settings.gatewayForwarding.downstreamModelAlignmentModels') }}</span>
        <textarea v-model="modelsText" rows="4" class="input w-full font-mono" placeholder="gpt-6-astra" />
        <span class="mt-1 block text-xs text-gray-500 dark:text-gray-400">{{ t('admin.settings.gatewayForwarding.downstreamModelAlignmentModelsHint') }}</span>
      </label>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import Toggle from '@/components/common/Toggle.vue'
import ForceOpenAIWSGroups from './ForceOpenAIWSGroups.vue'
import type { OpenAIDownstreamModelAlignmentSettings } from '@/api/admin/settings'

const props = defineProps<{ modelValue: OpenAIDownstreamModelAlignmentSettings }>()
const emit = defineEmits<{ 'update:modelValue': [value: OpenAIDownstreamModelAlignmentSettings] }>()
const { t } = useI18n()
const update = (value: Partial<OpenAIDownstreamModelAlignmentSettings>) => emit('update:modelValue', { ...props.modelValue, ...value })
const modelsText = computed({
  get: () => props.modelValue.models.join('\n'),
  set: (value: string) => update({ models: value.split(/\r?\n/) })
})
</script>
