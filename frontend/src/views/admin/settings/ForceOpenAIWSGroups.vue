<template>
  <fieldset class="min-w-0 space-y-3">
    <legend class="text-sm font-medium text-gray-700 dark:text-gray-300">
      {{ t('admin.settings.gatewayForwarding.forceWSGroups') }}
    </legend>
    <div class="flex flex-wrap gap-x-6 gap-y-2 text-sm text-gray-700 dark:text-gray-300">
      <label class="flex cursor-pointer items-center gap-2">
        <input type="radio" :name="scopeName" :checked="modelValue === null" @change="emit('update:modelValue', null)" />
        {{ t('admin.settings.gatewayForwarding.forceWSAllGroups') }}
      </label>
      <label class="flex cursor-pointer items-center gap-2">
        <input type="radio" :name="scopeName" :checked="modelValue !== null" @change="emit('update:modelValue', modelValue ?? [])" />
        {{ t('admin.settings.gatewayForwarding.forceWSSelectedGroups') }}
      </label>
    </div>
    <template v-if="modelValue !== null">
      <div v-if="loading" class="text-sm text-gray-500">{{ t('common.loading') }}</div>
      <div v-else-if="failed" class="flex items-center gap-3 text-sm text-red-600">
        <span>{{ t('admin.settings.gatewayForwarding.forceWSGroupsLoadFailed') }}</span>
        <button type="button" class="underline" @click="loadGroups">{{ t('common.retry') }}</button>
      </div>
      <GroupSelector v-else v-model="selectedGroups" class="force-ws-group-selector" :groups="groups" platform="openai" searchable />
    </template>
  </fieldset>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { adminAPI } from '@/api'
import GroupSelector from '@/components/common/GroupSelector.vue'
import type { AdminGroup } from '@/types'

const props = withDefaults(defineProps<{ modelValue: number[] | null; scopeName?: string }>(), { scopeName: 'force-ws-group-scope' })
const emit = defineEmits<{ 'update:modelValue': [value: number[] | null] }>()
const { t } = useI18n()
const groups = ref<AdminGroup[]>([])
const loading = ref(false)
const failed = ref(false)
const selectedGroups = computed({
  get: () => props.modelValue ?? [],
  set: (value: number[]) => emit('update:modelValue', value)
})

async function loadGroups() {
  loading.value = true
  failed.value = false
  try {
    groups.value = await adminAPI.groups.getAll()
  } catch {
    failed.value = true
  } finally {
    loading.value = false
  }
}

onMounted(loadGroups)
</script>

<style scoped>
.force-ws-group-selector :deep(input[type='text']) {
  min-width: 0;
}

@media (max-width: 640px) {
  .force-ws-group-selector :deep(.grid) {
    grid-template-columns: minmax(0, 1fr);
  }

  .force-ws-group-selector :deep(.col-span-2) {
    grid-column: 1 / -1;
  }
}
</style>
