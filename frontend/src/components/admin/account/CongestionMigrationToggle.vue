<template>
  <button
    type="button"
    role="switch"
    :aria-checked="enabled"
    :disabled="loading || saving || failed"
    :title="t('admin.accounts.congestionMigrationHint')"
    class="btn btn-secondary gap-2 whitespace-nowrap"
    @click="toggle"
  >
    <span>{{ t('admin.accounts.congestionMigration') }}</span>
    <span class="relative inline-flex h-5 w-9 rounded-full transition-colors" :class="enabled ? 'bg-primary-500' : 'bg-gray-300 dark:bg-dark-600'">
      <span class="absolute top-0.5 h-4 w-4 rounded-full bg-white transition-transform" :class="enabled ? 'translate-x-[18px]' : 'translate-x-0.5'" />
    </span>
  </button>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import apiClient from '@/api/client'
import { useAppStore } from '@/stores/app'

const { t } = useI18n()
const app = useAppStore()
const enabled = ref(false)
const loading = ref(true)
const saving = ref(false)
const failed = ref(false)
const endpoint = '/admin/settings/openai-congestion-migration'

onMounted(async () => {
  try {
    const { data } = await apiClient.get<{ enabled: boolean }>(endpoint)
    enabled.value = data.enabled
  } catch {
    failed.value = true
    app.showError(t('admin.accounts.congestionMigrationLoadFailed'))
  } finally {
    loading.value = false
  }
})

async function toggle() {
  if (loading.value || saving.value || failed.value) return
  saving.value = true
  try {
    const { data } = await apiClient.put<{ enabled: boolean }>(endpoint, { enabled: !enabled.value })
    enabled.value = data.enabled
    app.showSuccess(t('admin.accounts.congestionMigrationSaved'))
  } catch {
    app.showError(t('admin.accounts.congestionMigrationSaveFailed'))
  } finally {
    saving.value = false
  }
}
</script>
