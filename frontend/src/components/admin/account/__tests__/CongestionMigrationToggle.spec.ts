import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import CongestionMigrationToggle from '../CongestionMigrationToggle.vue'
import { defaultCongestionSettings } from '../congestionSettings'

const mocks = vi.hoisted(() => ({ get: vi.fn(), put: vi.fn(), showError: vi.fn(), showSuccess: vi.fn() }))
vi.mock('@/api/client', () => ({ default: { get: mocks.get, put: mocks.put } }))
vi.mock('@/stores/app', () => ({ useAppStore: () => mocks }))
vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string) => key, te: () => false }) }))

const endpoint = '/admin/settings/openai-congestion-migration'
const create = () => mount(CongestionMigrationToggle, {
  global: { stubs: { BaseDialog: { props: ['show'], template: '<div v-if="show" role="dialog"><slot/><slot name="footer"/></div>' } } }
})
async function open(wrapper: ReturnType<typeof create>) {
  await flushPromises()
  await wrapper.get('[aria-haspopup="dialog"]').trigger('click')
  await flushPromises()
}

describe('CongestionMigrationToggle', () => {
  beforeEach(() => {
    vi.resetAllMocks()
    mocks.get.mockResolvedValue({ data: { ...defaultCongestionSettings, enabled: true, slow_seconds: 23 } })
    mocks.put.mockImplementation((_url, data) => Promise.resolve({ data }))
  })

  it('opens settings without toggling, persists edits only on save, and keeps custom values when disabled', async () => {
    const wrapper = create()
    await open(wrapper)
    expect(mocks.put).not.toHaveBeenCalled()
    expect(wrapper.find('[role="dialog"]').exists()).toBe(true)
    expect((wrapper.get('[name="slow_seconds"]').element as HTMLInputElement).value).toBe('23')
    await wrapper.get('[name="enabled"]').setValue(false)
    await wrapper.get('[name="slow_seconds"]').setValue(12)
    await wrapper.get('form').trigger('submit')
    await flushPromises()
    expect(mocks.put).toHaveBeenCalledWith(endpoint, { ...defaultCongestionSettings, enabled: false, slow_seconds: 12 })
    expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
  })

  it('cancels edits and reloads the saved values on the next open', async () => {
    const wrapper = create()
    await open(wrapper)
    await wrapper.get('[name="slow_seconds"]').setValue(30)
    await wrapper.findAll('button').find(b => b.text().endsWith('.cancel'))!.trigger('click')
    expect(mocks.put).not.toHaveBeenCalled()
    await open(wrapper)
    expect((wrapper.get('[name="slow_seconds"]').element as HTMLInputElement).value).toBe('23')
  })

  it('validates sample counts and bounds before saving', async () => {
    const wrapper = create()
    await open(wrapper)
    await wrapper.get('[name="slow_seconds"]').setValue(0)
    await wrapper.get('form').trigger('submit')
    expect(mocks.put).not.toHaveBeenCalled()
    expect(wrapper.find('[role="alert"]').exists()).toBe(true)
    await wrapper.get('[name="slow_seconds"]').setValue(15)
    await wrapper.get('[name="sample_limit"]').setValue(5)
    await wrapper.get('[name="slow_min_samples"]').setValue(6)
    await wrapper.get('form').trigger('submit')
    expect(mocks.put).not.toHaveBeenCalled()
  })

  it('restores recommendations without enabling a disabled master switch', async () => {
    mocks.get.mockResolvedValue({ data: { ...defaultCongestionSettings, slow_seconds: 90 } })
    const wrapper = create()
    await open(wrapper)
    await wrapper.findAll('button').find(b => b.text().endsWith('.defaults'))!.trigger('click')
    await wrapper.get('form').trigger('submit')
    await flushPromises()
    expect(mocks.put).toHaveBeenCalledWith(endpoint, defaultCongestionSettings)
  })

  it('retains editable values after save failure and prevents duplicate saves', async () => {
    let reject!: (error: Error) => void
    mocks.put.mockImplementation(() => new Promise((_, fail) => { reject = fail }))
    const wrapper = create()
    await open(wrapper)
    await wrapper.get('[name="slow_seconds"]').setValue(12)
    await wrapper.get('form').trigger('submit')
    await wrapper.get('form').trigger('submit')
    expect(mocks.put).toHaveBeenCalledTimes(1)
    reject(new Error('save failed'))
    await flushPromises()
    expect(wrapper.find('[role="dialog"]').exists()).toBe(true)
    expect((wrapper.get('[name="slow_seconds"]').element as HTMLInputElement).value).toBe('12')
    expect(mocks.showError).toHaveBeenCalledOnce()
  })

  it('blocks saving on a failed read but allows retrying the read', async () => {
    mocks.get.mockRejectedValue(new Error('read failed'))
    const wrapper = create()
    await open(wrapper)
    expect(wrapper.find('form').exists()).toBe(false)
    expect(wrapper.get('button[type="submit"]').attributes('disabled')).toBeDefined()
    expect(mocks.put).not.toHaveBeenCalled()
    mocks.get.mockResolvedValue({ data: defaultCongestionSettings })
    await wrapper.findAll('button').find(b => b.text().endsWith('.retry'))!.trigger('click')
    await flushPromises()
    expect(wrapper.find('form').exists()).toBe(true)
  })
})
