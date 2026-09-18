import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import View from '../OpenAIStateKeeperView.vue'
import { stateKeeperAPI, type StateKeeperSnapshot } from '@/api/admin/openaiStateKeeper'

vi.mock('@/components/layout/AppLayout.vue', () => ({ default: { template: '<main><slot /></main>' } }))

vi.mock('@/api/admin/openaiStateKeeper', () => ({ stateKeeperAPI: { get: vi.fn(), save: vi.fn(), collect: vi.fn() } }))
vi.mock('@/api/admin/accounts', () => ({ list: vi.fn().mockResolvedValue({ items: [{ id: 1, name: '测试账号' }], total: 1 }) }))
vi.mock('@/api/admin/groups', () => ({ getAll: vi.fn().mockResolvedValue([{ id: 11, name: '测试分组' }]) }))
vi.mock('@/api/admin/proxies', () => ({ getAll: vi.fn().mockResolvedValue([{ id: 1, name: '采集出口', host: '127.0.0.1', port: 8888 }]), create: vi.fn() }))

const initial = (): StateKeeperSnapshot => ({
  collection_path: '/v1/chat/completions',
  collection_endpoint: 'https://chatgpt.com/backend-api/codex/responses',
  settings: { enabled: true, injection_enabled: false, auto_refresh: false, auto_collect_interval_seconds: 0, concurrency: 50, account_ids: [1], group_ids: [11], all_groups: false, proxy_id: 1, model: 'test-model', ttl_seconds: 3600, refresh_before_seconds: 300, retry_seconds: 60, revision: 'one' },
  rows: [{ account_id: 1, model: 'test-model', status: 'not_observed', queued: false, collecting: false, http_status: 200, message: '仅发现普通 Codex 回合状态', has_codex_turn_state: true, attempts: 1, successes: 0, injections: 0 }],
  events: [], server_time: new Date().toISOString(),
})

function render() {
  return mount(View, { global: { stubs: { AppLayout: { template: '<main><slot /></main>' }, Icon: true } } })
}

describe('Upstream state management', () => {
  beforeEach(() => { vi.useFakeTimers(); vi.clearAllMocks(); vi.mocked(stateKeeperAPI.get).mockResolvedValue(initial()) })
  afterEach(() => vi.useRealTimers())

  it('saves injection separately and collects only after the draft is saved', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings: { ...JSON.parse(JSON.stringify(settings)), revision: 'two' } }))
    const wrapper = render(); await flushPromises()
    expect(wrapper.text()).toContain('/v1/chat/completions')
    expect(wrapper.text()).toContain('https://chatgpt.com/backend-api/codex/responses')
    const checkbox = wrapper.findAll('label').find(label => label.text() === '启用请求注入')!.get('input')
    await checkbox.setValue(true)
    expect(wrapper.findAll('button').find(b => b.text() === '立即采集')!.attributes('disabled')).toBeDefined()
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenCalledWith(expect.objectContaining({ injection_enabled: true, proxy_id: 1, account_ids: [1], group_ids: [11] }))
    expect(wrapper.findAll('button').find(b => b.text() === '立即采集')!.attributes('disabled')).toBeUndefined()
    await wrapper.findAll('button').find(b => b.text() === '立即采集')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.collect).toHaveBeenCalledWith([])
    wrapper.unmount()
  })

  it('does not overwrite unsaved edits while polling or label ordinary turn state as success', async () => {
    const wrapper = render(); await flushPromises()
    const model = wrapper.get('input[placeholder="填写实际使用的模型"]')
    await model.setValue('edited-model')
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect((model.element as HTMLInputElement).value).toBe('edited-model')
    await wrapper.findAll('button').find(b => b.text() === '账号状态')!.trigger('click')
    expect(wrapper.text()).toContain('未取得目标状态')
    expect(wrapper.text()).toContain('仅发现普通 Codex 回合状态')
    expect(stateKeeperAPI.collect).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('shows a seconds input only when automatic collection is enabled and saves it', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings: { ...JSON.parse(JSON.stringify(settings)), revision: 'two' } }))
    const wrapper = render(); await flushPromises()
    expect(wrapper.find('input[placeholder="输入秒数"]').exists()).toBe(false)
    const checkbox = wrapper.findAll('label').find(label => label.text() === '自动采集与刷新')!.get('input')
    await checkbox.setValue(true)
    await wrapper.get('input[placeholder="输入秒数"]').setValue(120)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect((wrapper.get('input[placeholder="输入秒数"]').element as HTMLInputElement).value).toBe('120')
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ auto_refresh: true, auto_collect_interval_seconds: 120 }))
    await checkbox.setValue(false)
    expect(wrapper.find('input[placeholder="输入秒数"]').exists()).toBe(false)
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ auto_refresh: false, auto_collect_interval_seconds: 120 }))
    wrapper.unmount()
  })

  it('saves collection concurrency independently of automatic collection', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings: { ...JSON.parse(JSON.stringify(settings)), revision: 'two' } }))
    const wrapper = render(); await flushPromises()
    const input = wrapper.get('input[placeholder="输入并发数"]')
    expect((input.element as HTMLInputElement).value).toBe('50')
    await input.setValue(8)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect((input.element as HTMLInputElement).value).toBe('8')
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ concurrency: 8, auto_refresh: false }))
    wrapper.unmount()
  })
})
