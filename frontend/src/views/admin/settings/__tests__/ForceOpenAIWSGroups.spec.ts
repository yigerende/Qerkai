import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { defineComponent, ref } from 'vue'
import ForceOpenAIWSGroups from '../ForceOpenAIWSGroups.vue'

const { getAll } = vi.hoisted(() => ({ getAll: vi.fn() }))
vi.mock('@/api', () => ({ adminAPI: { groups: { getAll } } }))
vi.mock('vue-i18n', async (importOriginal) => ({
  ...await importOriginal<typeof import('vue-i18n')>(),
  useI18n: () => ({ t: (key: string) => key })
}))

function editor(initial: number[] | null = null) {
  return mount(defineComponent({
    components: { ForceOpenAIWSGroups },
    setup: () => ({ ids: ref(initial) }),
    template: '<ForceOpenAIWSGroups v-model="ids" />'
  }), { global: { stubs: { GroupBadge: true, Icon: true } } })
}

describe('Forced WS group selection', () => {
  beforeEach(() => {
    getAll.mockReset().mockResolvedValue([
      { id: 11, name: 'OpenAI A', platform: 'openai' },
      { id: 22, name: 'OpenAI B', platform: 'openai' },
      { id: 33, name: 'Composite', platform: 'composite' },
      { id: 44, name: 'Anthropic', platform: 'anthropic' }
    ])
  })

  it('keeps legacy all groups and distinguishes explicit empty selection', async () => {
    const wrapper = editor()
    await flushPromises()
    expect(wrapper.vm.ids).toBeNull()
    await wrapper.findAll('input[type=radio]')[1]!.setValue()
    expect(wrapper.vm.ids).toEqual([])
    expect(wrapper.findAll('input[type=checkbox]')).toHaveLength(3)
    await wrapper.get('input[type=checkbox][value="11"]').setValue(true)
    await wrapper.get('input[type=checkbox][value="22"]').setValue(true)
    expect(wrapper.vm.ids).toEqual([11, 22])
    await wrapper.get('input[type=checkbox][value="11"]').setValue(false)
    await wrapper.get('input[type=checkbox][value="22"]').setValue(false)
    expect(wrapper.vm.ids).toEqual([])
    await wrapper.findAll('input[type=radio]')[0]!.setValue()
    expect(wrapper.vm.ids).toBeNull()
  })

  it('preserves saved selection on a group loading failure and supports retry', async () => {
    getAll.mockRejectedValueOnce(new Error('offline'))
    const wrapper = editor([22])
    await flushPromises()
    expect(wrapper.text()).toContain('forceWSGroupsLoadFailed')
    expect(wrapper.vm.ids).toEqual([22])
    await wrapper.get('button').trigger('click')
    await flushPromises()
    expect((wrapper.get('input[type=checkbox][value="22"]').element as HTMLInputElement).checked).toBe(true)
    expect(wrapper.vm.ids).toEqual([22])
  })
})
