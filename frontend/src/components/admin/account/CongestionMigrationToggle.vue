<template>
  <button type="button" aria-haspopup="dialog" :aria-expanded="show" :title="t('admin.accounts.congestionMigrationHint')" class="btn btn-secondary gap-2 whitespace-nowrap" @click="open">
    <span>{{ t('admin.accounts.congestionMigration') }}</span>
    <span aria-hidden="true" class="relative inline-flex h-5 w-9 rounded-full transition-colors" :class="saved.enabled ? 'bg-primary-500' : 'bg-gray-300 dark:bg-dark-600'">
      <span class="absolute top-0.5 h-4 w-4 rounded-full bg-white transition-transform" :class="saved.enabled ? 'translate-x-[18px]' : 'translate-x-0.5'" />
    </span>
  </button>
  <BaseDialog :show="show" :title="label('title')" width="wide" :close-on-escape="!saving" :show-close-button="!saving" @close="close">
    <div v-if="loading" class="py-10 text-center text-gray-500">{{ t('common.loading') }}</div>
    <div v-else-if="failed" class="space-y-3 py-8 text-center">
      <p class="text-red-600">{{ t('admin.accounts.congestionMigrationLoadFailed') }}</p>
      <button class="btn btn-secondary" @click="load">{{ label('retry') }}</button>
    </div>
    <form v-else id="congestion-settings-form" class="space-y-4" @submit.prevent="save">
      <label class="flex items-start gap-3 rounded-xl border border-primary-200 bg-primary-50 p-4 dark:border-primary-900 dark:bg-primary-950/30">
        <input v-model="draft.enabled" name="enabled" type="checkbox" role="switch" class="mt-1 h-4 w-4 accent-primary-500" :disabled="saving" />
        <span><span class="block font-semibold">{{ label('enabled') }}</span><span class="mt-1 block text-sm text-gray-600 dark:text-dark-300">{{ label('enabledHint') }}</span></span>
      </label>
      <div class="space-y-1 text-sm text-gray-500 dark:text-dark-400"><p>{{ label('intro') }}</p><p>{{ label('timing') }}</p></div>
      <section v-for="section in congestionSections" :key="section.key" class="rounded-xl border border-gray-200 p-4 dark:border-dark-600">
        <label v-if="section.toggle" class="flex items-center gap-2 font-semibold">
          <input v-model="draft[section.toggle]" :name="section.toggle" type="checkbox" class="h-4 w-4 accent-primary-500" :disabled="saving" />{{ label(section.key) }}
        </label>
        <h4 v-else class="font-semibold">{{ label(section.key) }}</h4>
        <p v-if="section.toggle && hint(section.toggle)" class="mt-1 text-xs text-gray-500 dark:text-dark-400">{{ hint(section.toggle) }}</p>
        <fieldset :disabled="saving || (!!section.toggle && !draft[section.toggle])" class="mt-3 grid grid-cols-1 gap-4 sm:grid-cols-2 disabled:opacity-50">
          <label v-for="key in section.fields" :key="key" class="block text-sm">
            <span>{{ label(key) }}</span>
            <input v-model.number="draft[key]" :name="key" type="number" step="1" required :min="congestionNumericFields[key].min" :max="congestionNumericFields[key].max" class="input mt-1 w-full" />
            <span v-if="hint(key)" class="mt-1 block text-xs leading-relaxed text-gray-500 dark:text-dark-400">{{ hint(key) }}</span>
          </label>
          <label v-for="key in section.checks || []" :key="key" class="flex items-start gap-2 text-sm sm:col-span-2">
            <input v-model="draft[key]" :name="key" type="checkbox" class="mt-0.5 h-4 w-4 shrink-0 accent-primary-500" />
            <span>{{ label(key) }}<span v-if="hint(key)" class="mt-1 block text-xs leading-relaxed text-gray-500 dark:text-dark-400">{{ hint(key) }}</span></span>
          </label>
        </fieldset>
      </section>
      <p v-if="!valid" class="text-sm text-red-600" role="alert">{{ label('invalid') }}</p>
    </form>
    <template #footer>
      <div class="flex w-full flex-wrap items-center justify-between gap-3">
        <button type="button" class="btn btn-secondary" :disabled="loading || failed || saving" @click="restoreDefaults">{{ label('defaults') }}</button>
        <div class="flex items-center gap-2">
          <button type="button" class="btn btn-secondary" :disabled="saving" @click="close">{{ label('cancel') }}</button>
          <button type="submit" form="congestion-settings-form" class="btn btn-primary" :disabled="loading || failed || saving || !valid">{{ saving ? t('common.saving') : label('save') }}</button>
        </div>
        <p class="w-full text-xs text-gray-500">{{ label('unsaved') }}</p>
      </div>
    </template>
  </BaseDialog>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import apiClient from '@/api/client'
import { useAppStore } from '@/stores/app'
import BaseDialog from '@/components/common/BaseDialog.vue'
import { congestionNumericFields, congestionSections, defaultCongestionSettings, validCongestionSettings, type CongestionSettings } from './congestionSettings'

const { t, te } = useI18n()
const app = useAppStore()
const show = ref(false)
const saved = ref<CongestionSettings>({ ...defaultCongestionSettings })
const draft = ref<CongestionSettings>({ ...defaultCongestionSettings })
const loading = ref(true)
const saving = ref(false)
const failed = ref(false)
const endpoint = '/admin/settings/openai-congestion-migration'
const label = (key: string) => t('admin.accounts.congestionConfig.' + key)
const hint = (key: string) => te('admin.accounts.congestionConfig.' + key + 'Hint') ? label(key + 'Hint') : ''
const valid = computed(() => validCongestionSettings(draft.value))
let loadGeneration = 0
async function load() {
  const generation = ++loadGeneration
  loading.value = true
  failed.value = false
  try {
    const { data } = await apiClient.get<CongestionSettings>(endpoint)
    if (generation !== loadGeneration) return
    saved.value = { ...defaultCongestionSettings, ...data }
    draft.value = { ...saved.value }
  } catch {
    if (generation !== loadGeneration) return
    failed.value = true
    app.showError(t('admin.accounts.congestionMigrationLoadFailed'))
  } finally {
    if (generation === loadGeneration) loading.value = false
  }
}
onMounted(load)
function open() { show.value = true; void load() }
function close() { if (!saving.value) show.value = false }
function restoreDefaults() { draft.value = { ...defaultCongestionSettings, enabled: draft.value.enabled } }
async function save() {
  if (loading.value || saving.value || failed.value || !valid.value) return
  saving.value = true
  try {
    const { data } = await apiClient.put<CongestionSettings>(endpoint, { ...draft.value })
    saved.value = { ...data }
    draft.value = { ...data }
    app.showSuccess(t('admin.accounts.congestionMigrationSaved'))
    show.value = false
  } catch {
    app.showError(t('admin.accounts.congestionMigrationSaveFailed'))
  } finally {
    saving.value = false
  }
}
</script>
