import { describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { defineComponent, ref } from 'vue'
import DownstreamModelAlignment from '../DownstreamModelAlignment.vue'
import ForceOpenAIWSGroups from '../ForceOpenAIWSGroups.vue'

vi.mock('@/api', () => ({ adminAPI: { groups: { getAll: vi.fn().mockResolvedValue([
  { id: 11, name: 'A', platform: 'openai' }, { id: 22, name: 'B', platform: 'openai' }
]) } } }))
vi.mock('vue-i18n', async (original) => ({
  ...await original<typeof import('vue-i18n')>(), useI18n: () => ({ t: (key: string) => key })
}))

describe('Downstream model alignment settings', () => {
  it('preserves independent group selections and multiline models through disabling', async () => {
    const wrapper = mount(defineComponent({
      components: { DownstreamModelAlignment, ForceOpenAIWSGroups },
      setup: () => ({ policy: ref({ enabled: false, group_ids: null as number[] | null, models: ['gpt-6-astra'] }), ws: ref<number[] | null>(null) }),
      template: '<DownstreamModelAlignment v-model="policy" /><ForceOpenAIWSGroups v-model="ws" />'
    }), { global: { stubs: { GroupBadge: true, Icon: true } } })
    const editor = wrapper.get('[data-testid="downstream-model-alignment"]')
    expect(editor.find('textarea').exists()).toBe(false)
    await editor.get('button').trigger('click')
    await flushPromises()
    await editor.get('textarea').setValue('gpt-6-astra\ngpt-5.6-terra\n')
    expect(wrapper.vm.policy.models).toEqual(['gpt-6-astra', 'gpt-5.6-terra', ''])
    await editor.findAll('input[type=radio]')[1]!.setValue()
    await editor.get('input[type=checkbox][value="11"]').setValue(true)
    expect(wrapper.vm.policy.group_ids).toEqual([11])
    expect(wrapper.vm.ws).toBeNull()
    expect(wrapper.findAll('input[name="force-ws-group-scope"]')).toHaveLength(2)
    expect(wrapper.findAll('input[name="downstream-model-group-scope"]')).toHaveLength(2)
    await editor.get('button').trigger('click')
    expect(wrapper.vm.policy.enabled).toBe(false)
    expect(wrapper.vm.policy.group_ids).toEqual([11])
    expect(wrapper.vm.policy.models[0]).toBe('gpt-6-astra')
  })
})
