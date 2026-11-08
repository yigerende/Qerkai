import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import CongestionMigrationToggle from '../CongestionMigrationToggle.vue'

const mocks = vi.hoisted(() => ({ get: vi.fn(), put: vi.fn(), showError: vi.fn(), showSuccess: vi.fn() }))
vi.mock('@/api/client', () => ({ default: { get: mocks.get, put: mocks.put } }))
vi.mock('@/stores/app', () => ({ useAppStore: () => mocks }))
vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string) => key }) }))

describe('CongestionMigrationToggle', () => {
  beforeEach(() => { vi.resetAllMocks() })

  it('loads the saved value and explicitly saves false when turned off', async () => {
    mocks.get.mockResolvedValue({ data: { enabled: true } })
    mocks.put.mockResolvedValue({ data: { enabled: false } })
    const wrapper = mount(CongestionMigrationToggle)
    expect(wrapper.get('button').attributes('disabled')).toBeDefined()
    await flushPromises()
    expect(wrapper.get('button').attributes('aria-checked')).toBe('true')
    await wrapper.get('button').trigger('click')
    await flushPromises()
    expect(mocks.put).toHaveBeenCalledWith('/admin/settings/openai-congestion-migration', { enabled: false })
    expect(wrapper.get('button').attributes('aria-checked')).toBe('false')
  })

  it('keeps the saved value on failure and prevents duplicate saves', async () => {
    mocks.get.mockResolvedValue({ data: { enabled: false } })
    let reject!: (error: Error) => void
    mocks.put.mockImplementation(() => new Promise((_, fail) => { reject = fail }))
    const wrapper = mount(CongestionMigrationToggle)
    await flushPromises()
    await wrapper.get('button').trigger('click')
    await wrapper.get('button').trigger('click')
    expect(mocks.put).toHaveBeenCalledTimes(1)
    expect(wrapper.get('button').attributes('aria-checked')).toBe('false')
    reject(new Error('save failed'))
    await flushPromises()
    expect(wrapper.get('button').attributes('aria-checked')).toBe('false')
    expect(mocks.showError).toHaveBeenCalledOnce()
  })

  it('does not let a failed read overwrite the unknown server setting', async () => {
    mocks.get.mockRejectedValue(new Error('read failed'))
    const wrapper = mount(CongestionMigrationToggle)
    await flushPromises()
    expect(wrapper.get('button').attributes('disabled')).toBeDefined()
    await wrapper.get('button').trigger('click')
    expect(mocks.put).not.toHaveBeenCalled()
  })
})
