import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { defineComponent, ref } from 'vue'
import Rules from '../OpenAIUpstream5xxRetryRules.vue'
import messages from '@/i18n/locales/en/admin/openaiUpstream5xxRetry'
import { defaultOpenAIUpstream5xxRetryRules, previewOpenAIUpstream5xxRetryRule } from '@/api/admin/openaiRetryRules'

vi.mock('vue-i18n', async importOriginal => ({
  ...await importOriginal<typeof import('vue-i18n')>(),
  useI18n: () => ({
  t: (key: string) => key.split('.').reduce((value: any, part) => value?.[part], { admin: messages, common: { enabled: 'Enabled' } }) || key,
}) }))

vi.mock('@/api/admin/openaiRetryRules', async importOriginal => ({
  ...await importOriginal<typeof import('@/api/admin/openaiRetryRules')>(),
  previewOpenAIUpstream5xxRetryRule: vi.fn(),
}))

function editor() {
  return mount(defineComponent({
    components: { Rules },
    setup: () => ({ rules: ref(defaultOpenAIUpstream5xxRetryRules()) }),
    template: '<Rules v-model="rules" />',
  }))
}

describe('OpenAI retry rules editor', () => {
  beforeEach(() => vi.clearAllMocks())
  it('keeps both defaults selected, supports disabling, reordering, deletion and restoring', async () => {
    const wrapper = editor()
    expect(wrapper.findAll('input[type=checkbox]').every(input => (input.element as HTMLInputElement).checked)).toBe(true)
    await wrapper.find('input[type=checkbox]').setValue(false)
    await wrapper.find('[aria-label="Move down"]').trigger('click')
    expect(wrapper.findAll('[data-rule-id]')[0]?.attributes('data-rule-id')).toBe('server_overloaded')
    await wrapper.find('[aria-label="Delete rule"]').trigger('click')
    await wrapper.find('[aria-label="Delete rule"]').trigger('click')
    expect(wrapper.findAll('[data-rule-id]')).toHaveLength(0)
    await wrapper.find('[aria-label="Restore default rules"]').trigger('click')
    expect(wrapper.findAll('[data-rule-id]')).toHaveLength(2)
  })

  it('previews edited draft rules and clears stale results after edits', async () => {
    vi.mocked(previewOpenAIUpstream5xxRetryRule).mockResolvedValue({ matched: true, rule_name: 'Draft overload', rule_id: 'server_overloaded', status_code: 503, reason: 'matched' })
    const wrapper = editor()
    await wrapper.findAll('input[type=checkbox]')[0]!.setValue(false)
    await wrapper.findAll('input[maxlength]')[1]!.setValue('Draft overload')
    const payload = '{"type":"error","error":{"message":"overloaded"}}'
    await wrapper.findAll('textarea')[2]!.setValue(payload)
    const preview = wrapper.findAll('button').find(button => button.text().includes('Test match'))!
    await preview.trigger('click')
    await flushPromises()
    expect(previewOpenAIUpstream5xxRetryRule).toHaveBeenCalledWith(expect.arrayContaining([expect.objectContaining({ enabled: false }), expect.objectContaining({ name: 'Draft overload' })]), payload)
    expect(wrapper.get('[role=status]').text()).toContain('Draft overload')
    await wrapper.findAll('input[maxlength]')[1]!.setValue('Changed')
    expect(wrapper.find('[role=status]').exists()).toBe(false)
  })
})
