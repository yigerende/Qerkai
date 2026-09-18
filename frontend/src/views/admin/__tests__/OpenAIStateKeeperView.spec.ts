import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import View from '../OpenAIStateKeeperView.vue'
import { stateKeeperAPI, type StateKeeperDetail, type StateKeeperFileDetail, type StateKeeperSnapshot } from '@/api/admin/openaiStateKeeper'
import * as accountsAPI from '@/api/admin/accounts'

const { copyToClipboard } = vi.hoisted(() => ({ copyToClipboard: vi.fn() }))
vi.mock('@/composables/useClipboard', async () => {
  const { ref } = await import('vue')
  return { useClipboard: () => ({ copied: ref(false), copyToClipboard }) }
})

vi.mock('@/components/layout/AppLayout.vue', () => ({ default: { template: '<main><slot /></main>' } }))

vi.mock('@/api/admin/openaiStateKeeper', () => ({ stateKeeperAPI: { get: vi.fn(), detail: vi.fn(), fileDetail: vi.fn(), save: vi.fn(), collect: vi.fn(), collectPaused: vi.fn() } }))
vi.mock('@/api/admin/accounts', () => ({ list: vi.fn().mockResolvedValue({ items: [{ id: 1, name: '测试账号' }], total: 1 }) }))
vi.mock('@/api/admin/groups', () => ({ getAll: vi.fn().mockResolvedValue([{ id: 11, name: '测试分组' }]) }))
vi.mock('@/api/admin/proxies', () => ({ getAll: vi.fn().mockResolvedValue([{ id: 1, name: '采集出口', host: '127.0.0.1', port: 8888 }, { id: 2, name: '备用出口', host: '127.0.0.2', port: 8888 }, { id: 3, name: '第三出口', host: '127.0.0.3', port: 8888 }]), create: vi.fn() }))

const initial = (): StateKeeperSnapshot => ({
  collection_path: '/v1/chat/completions',
  collection_endpoint: 'https://chatgpt.com/backend-api/codex/responses',
  settings: { enabled: true, injection_enabled: false, response_refresh_enabled: false, auto_refresh: false, auto_collect_interval_seconds: 0, degradation_scan_enabled: false, degradation_scan_interval_seconds: 60, concurrency: 50, account_concurrency: 1, max_attempts: 3, retry_count: 0, retry_interval_seconds: 5, allowed_state_lengths: [], degraded_state_lengths: [], account_ids: [1], collection_group_ids: [], group_ids: [11], all_groups: false, proxy_id: 1, model: 'test-model', revision: 'one' },
  rows: [{ account_id: 1, model: 'test-model', status: 'ready', queued: false, collecting: false, http_status: 200, turn_state_length: 356, message: '已取得 x-codex-turn-state 响应头，已写入账号独立 State 文件', has_codex_turn_state: true, has_details: true, state_file_saved: true, fingerprint: 'stored-state', attempts: 1, successes: 1, injections: 0, paused: false, pause_reason: '', round_id: 'round1', round_attempts: 1, round_source: 'manual', quality_status: 'degraded', quality_reason: '答题异常' }],
  events: [{ at: new Date().toISOString(), account_id: 1, model: 'test-model', http_status: 200, turn_state_length: 356, result: 'collected', message: '已取得 x-codex-turn-state 响应头', kind: 'collection', source: 'manual', attempt: 1 }], server_time: new Date().toISOString(),
})

const capturedDetail = (accountId = 1): StateKeeperDetail => ({
  account_id: accountId, model: 'test-model', http_status: 200,
  header_name: 'x-codex-turn-state', header_value: 'abcd'.repeat(89), turn_state_length: 356,
  collected_at: '2026-09-18T06:00:00Z',
  save_allowed: true, state_file_saved: true,
})

const capturedFile = (accountId = 1): StateKeeperFileDetail => ({
  file_name: `account-${accountId}.state`,
  content: {
    account_id: accountId, model: 'test-model', proxy_id: 1,
    endpoint: 'https://chatgpt.com/backend-api/codex/responses', header_name: 'x-codex-turn-state',
    credential_stamp: 'credential-fingerprint', turn_state: 'saved-state',
    collected_at: '2026-09-18T06:00:00Z',
  },
})

function render() {
  return mount(View, { global: { stubs: {
    AppLayout: { template: '<main><slot /></main>' }, Icon: true,
    BaseDialog: { props: ['show', 'title'], template: '<section v-if="show" role="dialog"><h3>{{ title }}</h3><slot /><slot name="footer" /></section>' },
  } } })
}

async function showModelRows(wrapper: ReturnType<typeof render>) {
  await wrapper.findAll('button').find(b => b.text() === '账号状态')!.trigger('click')
  for (const button of wrapper.findAll('button[aria-expanded="false"]')) await button.trigger('click')
}

describe('Upstream state management', () => {
  beforeEach(() => { vi.useFakeTimers(); vi.clearAllMocks(); vi.mocked(stateKeeperAPI.get).mockResolvedValue(initial()) })
  afterEach(() => vi.useRealTimers())

  it('saves dynamic collection groups without turning members into explicit account selections', async () => {
    vi.mocked(accountsAPI.list).mockResolvedValueOnce({ items: [
      { id: 1, name: '手动账号', group_ids: [12] },
      { id: 2, name: '分组账号', group_ids: [11] },
    ], total: 2 } as Awaited<ReturnType<typeof accountsAPI.list>>)
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings }))
    const w = render(); await flushPromises()
    await w.get('input[aria-label="采集分组 测试分组"]').setValue(true)
    const member = w.get('input[aria-label="采集账号 分组账号"]')
    expect((member.element as HTMLInputElement).checked).toBe(true)
    expect(member.attributes('disabled')).toBeDefined()
    await w.get('input[aria-label="全选采集账号"]').setValue(false)
    await w.get('input[aria-label="全选采集账号"]').setValue(true)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ collection_group_ids: [11], account_ids: [1], group_ids: [11] }))
    await w.get('input[aria-label="采集分组 测试分组"]').setValue(false)
    expect((member.element as HTMLInputElement).checked).toBe(false)
    expect(member.attributes('disabled')).toBeUndefined()
    expect((w.get('input[aria-label="采集账号 手动账号"]').element as HTMLInputElement).checked).toBe(true)
    expect(stateKeeperAPI.collect).not.toHaveBeenCalled()
    w.unmount()
  })

  it('shows later group members from polling and preserves their expanded model rows', async () => {
    const data = initial()
    data.settings.account_ids = []
    data.settings.collection_group_ids = [11]
    data.rows[0].account_group_ids = [11]
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    const w = render(); await flushPromises()
    const joined = { ...data, rows: [...data.rows, { ...data.rows[0], account_id: 2, account_name: '后来加入的账号', account_group_ids: [11] }] }
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(joined)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    const member = w.get('input[aria-label="采集账号 后来加入的账号"]')
    expect((member.element as HTMLInputElement).checked).toBe(true)
    expect(member.attributes('disabled')).toBeDefined()
    await w.findAll('button').find(b => b.text() === '账号状态')!.trigger('click')
    await w.get('button[aria-controls="state-models-2"]').trigger('click')
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect(w.find('#state-models-2').exists()).toBe(true)
    expect(stateKeeperAPI.save).not.toHaveBeenCalled()
    expect(stateKeeperAPI.collect).not.toHaveBeenCalled()
    w.unmount()
  })

  it('saves response-triggered collection independently and preserves unsaved changes during polling', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings }))
    const w = render(); await flushPromises()
    const toggle = w.get('input[aria-label="请求响应触发采集"]')
    expect((toggle.element as HTMLInputElement).checked).toBe(false)
    await toggle.setValue(true)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect((toggle.element as HTMLInputElement).checked).toBe(true)
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ response_refresh_enabled: true, injection_enabled: false, auto_refresh: false, degradation_scan_enabled: false }))
    await toggle.setValue(false)
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ response_refresh_enabled: false }))
    expect(stateKeeperAPI.collect).not.toHaveBeenCalled()
    w.unmount()
  })

  it('selects all accounts and clears only matching accounts when searching', async () => {
    vi.mocked(accountsAPI.list).mockResolvedValueOnce({ items: [{ id: 1, name: '组A账号1' }, { id: 2, name: '组A账号2' }, { id: 3, name: '组B账号3' }], total: 3 } as Awaited<ReturnType<typeof accountsAPI.list>>)
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings }))
    const w = render(); await flushPromises()
    const selectAll = w.get('input[aria-label="全选采集账号"]')
    expect((selectAll.element as HTMLInputElement).indeterminate).toBe(true)
    await selectAll.setValue(true)
    expect((selectAll.element as HTMLInputElement).checked).toBe(true)
    expect((selectAll.element as HTMLInputElement).indeterminate).toBe(false)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect((selectAll.element as HTMLInputElement).checked).toBe(true)
    await w.get('input[aria-label="搜索采集账号"]').setValue('组A')
    expect(w.text()).toContain('全选搜索结果')
    await selectAll.setValue(false)
    await w.get('input[aria-label="搜索采集账号"]').setValue('')
    expect((selectAll.element as HTMLInputElement).indeterminate).toBe(true)
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ account_ids: [3] }))
    expect(stateKeeperAPI.collect).not.toHaveBeenCalled()
    await w.get('input[aria-label="搜索采集账号"]').setValue('no-matching-account')
    expect(selectAll.attributes('disabled')).toBeDefined()
    w.unmount()
  })

  it('starts collapsed and preserves each account expansion across polling', async () => {
    const data = initial()
    data.rows[0].models = [{ ...data.rows[0], model: 'gpt-5.5' }, { ...data.rows[0], model: 'gpt-6-astra', paused: true }]
    data.rows.push({ ...data.rows[0], account_id: 2 })
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    const w = render(); await flushPromises()
    await w.findAll('button').find(b => b.text() === '账号状态')!.trigger('click')
    expect(w.findAll('tbody tr')).toHaveLength(2)
    expect(w.find('.state-model-group').exists()).toBe(false)
    expect(w.findAll('.state-account-summary')[0].text()).toContain('待人工 1')
    const toggle = w.get('button[aria-controls="state-models-1"]')
    await toggle.trigger('click')
    expect(toggle.attributes('aria-expanded')).toBe('true')
    expect(w.get('#state-models-1').findAll('tr')).toHaveLength(2)
    expect(w.find('#state-models-2').exists()).toBe(false)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect(w.find('#state-models-1').exists()).toBe(true)
    await toggle.trigger('click')
    expect(w.findAll('tbody tr')).toHaveLength(2)
    expect(stateKeeperAPI.collect).not.toHaveBeenCalled()
    w.unmount()
  })

  it('selects a whole group without removing selections in other groups', async () => {
    vi.mocked(accountsAPI.list).mockResolvedValueOnce({ items: [
      { id: 1, name: '账号1', group_ids: [12] },
      { id: 2, name: '账号2', group_ids: [11] },
      { id: 3, name: '账号3', group_ids: [11, 12] },
    ], total: 3 } as Awaited<ReturnType<typeof accountsAPI.list>>)
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings }))
    const w = render(); await flushPromises()
    await w.get('select[aria-label="按分组筛选采集账号"]').setValue('11')
    expect(w.text()).toContain('全选当前分组')
    await w.get('input[aria-label="全选采集账号"]').setValue(true)
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ account_ids: [1, 2, 3], group_ids: [11] }))
    await w.get('input[aria-label="全选采集账号"]').setValue(false)
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ account_ids: [1] }))
    w.unmount()
  })

  it('marks unavailable accounts red and prevents collection while allowing configuration saves', async () => {
    const data = initial()
    data.rows[0].account_unavailable = true
    data.rows[0].account_unavailable_reason = '账号认证异常，停止采集'
    data.rows[0].paused = true
    data.rows[0].http_status = 401
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...data, settings }))
    const w = render(); await flushPromises()
    await showModelRows(w)
    expect(w.get('.state-account-summary').classes()).toContain('state-account-unavailable')
    expect(w.text()).toContain('账号认证异常，停止采集')
    expect(w.findAll('button').find(b => b.text() === '采集全部模型')!.attributes('disabled')).toBeDefined()
    expect(w.findAll('button').find(b => b.text() === '手动重试')!.attributes('disabled')).toBeDefined()
    const bulk = w.findAll('button').find(b => b.text().startsWith('一键重试待人工项'))!
    expect(bulk.text()).toContain('(0)')
    expect(bulk.attributes('disabled')).toBeDefined()
    await w.findAll('button').find(b => b.text() === '采集配置')!.trigger('click')
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ account_ids: [1] }))
    expect(stateKeeperAPI.collect).not.toHaveBeenCalled()
    w.unmount()
  })

  it('retries paused models in one request and excludes already active models from its count', async () => {
    const data = initial()
    data.rows[0].models = [
      { ...data.rows[0], model: 'ready' },
      { ...data.rows[0], model: 'paused', paused: true },
      { ...data.rows[0], model: 'queued', paused: true, queued: true },
      { ...data.rows[0], model: 'collecting', paused: true, collecting: true },
    ]
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    let resolve!: (value: { scheduled: boolean; scheduled_count: number }) => void
    vi.mocked(stateKeeperAPI.collectPaused).mockReturnValue(new Promise(done => { resolve = done }))
    const w = render(); await flushPromises()
    await w.findAll('button').find(b => b.text() === '账号状态')!.trigger('click')
    const button = w.findAll('button').find(b => b.text().startsWith('一键重试待人工项'))!
    expect(button.text()).toContain('(1)')
    await button.trigger('click')
    expect(button.attributes('disabled')).toBeDefined()
    await button.trigger('click')
    expect(stateKeeperAPI.collectPaused).toHaveBeenCalledTimes(1)
    expect(stateKeeperAPI.collect).not.toHaveBeenCalled()
    resolve({ scheduled: true, scheduled_count: 1 }); await flushPromises()
    expect(w.text()).toContain('已排队 1 个待人工账号模型')
    w.unmount()
  })

  it.each(['empty', 'disabled', 'dirty'])('disables bulk retry when %s', async reason => {
    const data = initial()
    data.rows[0].paused = reason !== 'empty'
    data.settings.enabled = reason !== 'disabled'
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    const w = render(); await flushPromises()
    if (reason === 'dirty') await w.get('textarea[aria-label="采集模型（每行一个）"]').setValue('unsaved-model')
    await w.findAll('button').find(b => b.text() === '账号状态')!.trigger('click')
    const button = w.findAll('button').find(b => b.text().startsWith('一键重试待人工项'))!
    expect(button.attributes('disabled')).toBeDefined()
    w.unmount()
  })

  it('selects multiple collection proxies and saves their explicit priority', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings }))
    const w = render(); await flushPromises()
    await w.findAll('label').find(label => label.text().startsWith('备用出口'))!.get('input').setValue(true)
    await w.findAll('label').find(label => label.text().startsWith('第三出口'))!.get('input').setValue(true)
    await w.get('button[aria-label="提高 备用出口 优先级"]').trigger('click')
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect(w.get('ol[aria-label="代理优先级"]').findAll('li').map(li => li.text())).toEqual(['1备用出口', '2采集出口', '3第三出口'])
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ proxy_id: 2, proxy_ids: [2, 1, 3], injection_enabled: false }))
    w.unmount()
  })

  it.each([false, true])('removes deleted proxy priorities while preserving unrelated drafts: %s', async draft => {
    const data = initial()
    data.settings.proxy_ids = [1, 2]
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    const w = render(); await flushPromises()
    if (draft) await w.get('textarea[aria-label="采集模型（每行一个）"]').setValue('unsaved-model')
    const updated = structuredClone(data)
    updated.settings.proxy_ids = [2]
    updated.settings.proxy_id = 2
    updated.settings.revision = 'proxy-removed'
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(updated)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect(w.get('ol[aria-label="代理优先级"]').findAll('li').map(li => li.text())).toEqual(['1备用出口'])
    expect((w.get('textarea[aria-label="采集模型（每行一个）"]').element as HTMLTextAreaElement).value).toBe(draft ? 'unsaved-model' : 'test-model')
    if (!draft) expect(w.findAll('button').find(b => b.text() === '立即采集')!.attributes('disabled')).toBeUndefined()
    w.unmount()
  })

  it('removes deleted accounts from expanded rows, retry counts and unsaved selections', async () => {
    const data = initial()
    data.settings.account_ids = [1, 2]
    data.rows[0].paused = true
    data.rows.push({ ...data.rows[0], account_id: 2 })
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...data, settings }))
    const w = render(); await flushPromises()
    await w.get('textarea[aria-label="采集模型（每行一个）"]').setValue('unsaved-model')
    await showModelRows(w)
    expect(w.findAll('.state-model-group')).toHaveLength(2)
    const updated = structuredClone(data)
    updated.settings.account_ids = [2]
    updated.settings.revision = 'account-removed'
    updated.rows = [updated.rows[1]]
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(updated)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect(w.findAll('.state-account-summary')).toHaveLength(1)
    expect(w.find('#state-models-1').exists()).toBe(false)
    expect(w.find('#state-models-2').exists()).toBe(true)
    expect(w.findAll('button').find(b => b.text().startsWith('一键重试待人工项'))!.text()).toContain('(1)')
    await w.findAll('button').find(b => b.text() === '采集配置')!.trigger('click')
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ account_ids: [2], models: ['unsaved-model'] }))
    w.unmount()
  })

  it('saves newline-separated models and retry policy without changing injection', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings }))
    const w = render(); await flushPromises()
    await w.get('textarea[aria-label="采集模型（每行一个）"]').setValue('gpt-5.5\n gpt-5.6-sol \ngpt-5.6-terra\ngpt-6-astra\ngpt-5.5\n')
    await w.get('input[aria-label="失败后重试轮数"]').setValue(3)
    await w.get('input[aria-label="重试间隔（s）"]').setValue(10)
    await w.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ models: ['gpt-5.5', 'gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-6-astra'], retry_count: 3, retry_interval_seconds: 10, injection_enabled: false }))
    w.unmount()
  })

  it('groups model states under one account and targets the clicked model', async () => {
    const data = initial()
    data.settings.models = ['gpt-5.5', 'gpt-6-astra']
    data.settings.retry_count = 9
    data.rows[0].models = [
      { ...data.rows[0], model: 'gpt-5.5', state_preview: 'gAAAAAA...first1', saved_state_length: 292 },
      { ...data.rows[0], model: 'gpt-6-astra', state_preview: 'gAAAAAA...second', saved_state_length: 332, next_retry_at: '2026-09-18T06:00:05Z', retry_attempt: 1, retry_limit: 2 },
    ]
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    vi.mocked(stateKeeperAPI.fileDetail).mockResolvedValue({ ...capturedFile(), content: { ...capturedFile().content, model: 'gpt-6-astra' } })
    const w = render(); await flushPromises()
    await showModelRows(w)
    expect(w.findAll('tbody')).toHaveLength(2)
    expect(w.findAll('tbody tr')).toHaveLength(3)
    expect(w.get('.state-account-summary').text()).toContain('测试账号')
    expect(w.text()).toContain('gAAAAAA...first1')
    expect(w.text()).toContain('已保存 · 332 字符')
    expect(w.text()).toContain('等待重试')
    expect(w.text()).toContain('重试 1 / 2')
    expect(w.text()).not.toContain('重试 1 / 9')
    await w.findAll('button').find(b => b.text() === 'gAAAAAA...second')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.fileDetail).toHaveBeenCalledWith(1, 'gpt-6-astra')
    expect(w.get('[role="dialog"]').text()).toContain('gpt-6-astra')
    w.unmount()
  })

  it('keeps automatic collection and degradation scanning intervals independent', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings }))
    const w = render(); await flushPromises()
    await w.findAll('label').find(label => label.text() === '定时自动采集')!.get('input').setValue(true)
    await w.get('input[placeholder="输入秒数"]').setValue(120)
    await w.findAll('label').find(label => label.text() === '定时降智扫描')!.get('input').setValue(true)
    await w.get('input[aria-label="降智扫描间隔（s）"]').setValue(45)
    await w.findAll('button').find(button => button.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ auto_refresh: true, auto_collect_interval_seconds: 120, degradation_scan_enabled: true, degradation_scan_interval_seconds: 45, injection_enabled: false }))
    w.unmount()
  })

  it('saves per-account concurrency, total budget and degraded lengths together', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings }))
    const w = render(); await flushPromises()
    await w.findAll('label').find(label => label.text() === '单账号采集并发数')!.get('input').setValue(4)
    await w.findAll('label').find(label => label.text() === '每轮最多请求次数（含首次）')!.get('input').setValue(9)
    await w.get('input[aria-label="允许保存的响应头长度"]').setValue('292,,332')
    await w.get('input[aria-label="降智响应头长度"]').setValue('356,356，376')
    await w.findAll('button').find(button => button.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ account_concurrency: 4, max_attempts: 9, allowed_state_lengths: [292, 332], degraded_state_lengths: [356, 376], injection_enabled: false }))
    await w.get('input[aria-label="降智响应头长度"]').setValue('332')
    await w.findAll('button').find(button => button.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenCalledTimes(1)
    expect(w.text()).toContain('允许保存与降智响应头长度不能重叠')
    w.unmount()
  })

  it('exposes persistent pauses and an explicit manual retry', async () => {
    const data = initial()
    data.rows[0] = { ...data.rows[0], paused: true, pause_reason: '已用完本轮次数，等待人工重试', round_attempts: 9 }
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    const w = render(); await flushPromises()
    await showModelRows(w)
    expect(w.text()).toContain('已暂停，待人工')
    expect(w.text()).toContain('本轮已请求 9 次')
    expect(w.text()).toContain('综合：降智')
    await w.findAll('button').find(button => button.text() === '手动重试')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.collect).toHaveBeenCalledWith([1], 'test-model')
    w.unmount()
  })

  it('retains injection controls and collects after the draft is saved', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings: { ...JSON.parse(JSON.stringify(settings)), revision: 'two' } }))
    const wrapper = render(); await flushPromises()
    expect(wrapper.text()).toContain('/v1/chat/completions')
    expect(wrapper.text()).toContain('https://chatgpt.com/backend-api/codex/responses')
    expect(wrapper.text()).toContain('启用请求注入')
    expect(wrapper.text()).toContain('注入范围')
    expect(wrapper.text()).not.toContain('缓存时长')
    expect(wrapper.text()).not.toContain('提前刷新')
    expect(wrapper.text()).not.toContain('HTTP 292')
    await wrapper.get('textarea[aria-label="采集模型（每行一个）"]').setValue('edited-model')
    expect(wrapper.findAll('button').find(b => b.text() === '立即采集')!.attributes('disabled')).toBeDefined()
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenCalledWith(expect.objectContaining({ injection_enabled: false, model: 'edited-model', proxy_id: 1, account_ids: [1] }))
    expect(wrapper.findAll('button').find(b => b.text() === '立即采集')!.attributes('disabled')).toBeUndefined()
    await wrapper.findAll('button').find(b => b.text() === '立即采集')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.collect).toHaveBeenCalledWith([], undefined)
    wrapper.unmount()
  })

  it('does not overwrite unsaved edits while polling', async () => {
    const wrapper = render(); await flushPromises()
    const model = wrapper.get('textarea[aria-label="采集模型（每行一个）"]')
    await model.setValue('edited-model')
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect((model.element as HTMLInputElement).value).toBe('edited-model')
    await showModelRows(wrapper)
    expect(wrapper.text()).toContain('已采集')
    expect(wrapper.text()).toContain('x-codex-turn-state')
    expect(stateKeeperAPI.collect).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('saves the injection switch and group scope without forcing injection off', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings: { ...JSON.parse(JSON.stringify(settings)), revision: 'two' } }))
    const wrapper = render(); await flushPromises()
    const injection = wrapper.findAll('label').find(label => label.text() === '启用请求注入')!.get('input')
    expect((injection.element as HTMLInputElement).checked).toBe(false)
    await injection.setValue(true)
    const allGroups = wrapper.findAll('label').find(label => label.text() === '适用于全部分组')!.get('input')
    await allGroups.setValue(true)
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ injection_enabled: true, all_groups: true }))
    await allGroups.setValue(false)
    expect(wrapper.findAll('label').find(label => label.text() === '测试分组')).toBeTruthy()
    await injection.setValue(false)
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ injection_enabled: false, all_groups: false, group_ids: [11] }))
    wrapper.unmount()
  })

  it('shows header lengths separately from HTTP status in account state and history', async () => {
    const wrapper = render(); await flushPromises()
    await wrapper.findAll('button').find(b => b.text() === '账号状态')!.trigger('click')
    expect(wrapper.findAll('th').map(th => th.text())).toContain('响应头值长度')
    expect(wrapper.text()).not.toContain('缓存截止')
    expect(wrapper.findAll('th').map(th => th.text())).toContain('采集 / 成功 / 注入')
    const stateCells = wrapper.get('tbody tr').findAll('td')
    expect(stateCells[3].text()).toBe('200')
    expect(stateCells[4].text()).toBe('356')
    await wrapper.findAll('button').find(b => b.text() === '采集记录')!.trigger('click')
    const historyCells = wrapper.get('tbody tr').findAll('td')
    expect(historyCells[3].text()).toBe('200')
    expect(historyCells[4].text()).toBe('356')
    wrapper.unmount()
  })

  it('shows no length when the latest response has no turn-state header', async () => {
    const data = initial()
    data.rows[0] = { ...data.rows[0], status: 'not_observed', turn_state_length: 0, has_codex_turn_state: false, message: '未取得有效 x-codex-turn-state 响应头' }
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    const wrapper = render(); await flushPromises()
    await wrapper.findAll('button').find(b => b.text() === '账号状态')!.trigger('click')
    expect(wrapper.get('tbody tr').findAll('td')[4].text()).toBe('—')
    wrapper.unmount()
  })

  it('loads only the clicked account detail and copies its complete header', async () => {
    const data = initial()
    data.rows.push({ ...data.rows[0], account_id: 2 })
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    vi.mocked(stateKeeperAPI.detail).mockResolvedValue(capturedDetail(2))
    const wrapper = render(); await flushPromises()
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect(stateKeeperAPI.detail).not.toHaveBeenCalled()
    await showModelRows(wrapper)
    await wrapper.get('#state-models-2').findAll('button').find(b => b.text() === '详情')!.trigger('click')
    await flushPromises()
    expect(stateKeeperAPI.detail).toHaveBeenCalledTimes(1)
    expect(stateKeeperAPI.detail).toHaveBeenCalledWith(2, 'test-model')
    const header = `x-codex-turn-state: ${capturedDetail(2).header_value}`
    expect(wrapper.get('[role="dialog"]').text()).toContain('账号 #2')
    expect(wrapper.get('pre[aria-label="完整响应头"]').text()).toBe(header)
    expect(wrapper.get('[role="dialog"]').text()).toContain('356 字符')
    await wrapper.findAll('button').find(b => b.text() === '复制完整响应头')!.trigger('click')
    expect(copyToClipboard).toHaveBeenCalledWith(header, '完整响应头已复制')
    await wrapper.findAll('button').find(b => b.text() === '关闭')!.trigger('click')
    expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
    expect(wrapper.text()).not.toContain(capturedDetail().header_value)
    wrapper.unmount()
  })

  it('disables details without a stored capture and handles load failures', async () => {
    const data = initial()
    data.rows.push({ ...data.rows[0], account_id: 2, fingerprint: undefined, turn_state_length: 0, has_details: false })
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    vi.mocked(stateKeeperAPI.detail).mockRejectedValueOnce(new Error('状态已清除')).mockResolvedValueOnce(capturedDetail())
    const wrapper = render(); await flushPromises()
    await showModelRows(wrapper)
    const buttons = wrapper.findAll('button').filter(b => b.text() === '详情')
    expect(buttons[1].attributes('disabled')).toBeDefined()
    await buttons[0].trigger('click'); await flushPromises()
    expect(wrapper.get('[role="alert"]').text()).toBe('状态已清除')
    expect(wrapper.findAll('button').find(b => b.text() === '复制完整响应头')!.attributes('disabled')).toBeDefined()
    await wrapper.findAll('button').find(b => b.text() === '重新加载')!.trigger('click'); await flushPromises()
    expect(wrapper.get('pre').text()).toContain(capturedDetail().header_value)
    wrapper.unmount()
  })

  it('discards a late detail response after closing and reopening another account', async () => {
    const data = initial()
    data.rows.push({ ...data.rows[0], account_id: 2 })
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    let resolveFirst!: (value: StateKeeperDetail) => void
    vi.mocked(stateKeeperAPI.detail)
      .mockReturnValueOnce(new Promise(resolve => { resolveFirst = resolve }))
      .mockResolvedValueOnce({ ...capturedDetail(2), header_value: 'second-state', turn_state_length: 12 })
    const wrapper = render(); await flushPromises()
    await showModelRows(wrapper)
    const buttons = wrapper.findAll('button').filter(b => b.text() === '详情')
    await buttons[0].trigger('click')
    expect(wrapper.get('[role="dialog"]').text()).toContain('加载中')
    await wrapper.findAll('button').find(b => b.text() === '关闭')!.trigger('click')
    await buttons[1].trigger('click'); await flushPromises()
    resolveFirst(capturedDetail()); await flushPromises()
    expect(wrapper.get('pre').text()).toBe('x-codex-turn-state: second-state')
    expect(wrapper.text()).not.toContain(capturedDetail().header_value)
    wrapper.unmount()
  })

  it('normalizes comma-separated allowed lengths, preserves drafts and rejects invalid lengths', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings: { ...JSON.parse(JSON.stringify(settings)), revision: 'two' } }))
    const wrapper = render(); await flushPromises()
    const input = wrapper.get('input[aria-label="允许保存的响应头长度"]')
    await input.setValue(' 292,,332，292, ')
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect((input.element as HTMLInputElement).value).toBe(' 292,,332，292, ')
    expect(wrapper.findAll('button').find(b => b.text() === '立即采集')!.attributes('disabled')).toBeDefined()
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ allowed_state_lengths: [292, 332] }))
    expect((input.element as HTMLInputElement).value).toBe('292,332')
    await input.setValue('292,abc')
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenCalledTimes(1)
    expect(wrapper.text()).toContain('响应头长度须为')
    await input.setValue('')
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ allowed_state_lengths: [] }))
    wrapper.unmount()
  })

  it('keeps filtered headers inspectable without reporting them as saved', async () => {
    const data = initial()
    data.rows[0] = { ...data.rows[0], status: 'filtered', fingerprint: undefined, state_file_saved: false }
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    vi.mocked(stateKeeperAPI.detail).mockResolvedValue({ ...capturedDetail(), save_allowed: false, state_file_saved: false })
    const wrapper = render(); await flushPromises()
    await showModelRows(wrapper)
    expect(wrapper.text()).toContain('长度不符，未保存')
    const button = wrapper.findAll('button').find(b => b.text() === '详情')!
    expect(button.attributes('disabled')).toBeUndefined()
    await button.trigger('click'); await flushPromises()
    expect(wrapper.get('[role="dialog"]').text()).toContain('未写入 State 文件')
    expect(wrapper.get('pre').text()).toContain(capturedDetail().header_value)
    wrapper.unmount()
  })

  it('enables State file viewing only for saved rows and shows complete file content', async () => {
    const data = initial()
    data.rows.push({ ...data.rows[0], account_id: 2, state_file_saved: false, has_details: false, fingerprint: undefined })
    vi.mocked(stateKeeperAPI.get).mockResolvedValue(data)
    vi.mocked(stateKeeperAPI.fileDetail).mockResolvedValue(capturedFile())
    const wrapper = render(); await flushPromises()
    await showModelRows(wrapper)
    const fileButtons = wrapper.findAll('button').filter(b => b.text() === '查看')
    expect(fileButtons).toHaveLength(2)
    expect(fileButtons[0].attributes('disabled')).toBeUndefined()
    expect(fileButtons[1].attributes('disabled')).toBeDefined()
    await fileButtons[0].trigger('click'); await flushPromises()
    expect(stateKeeperAPI.fileDetail).toHaveBeenCalledWith(1, 'test-model')
    const dialog = wrapper.get('[role="dialog"]')
    expect(dialog.text()).toContain('State 文件详情')
    expect(dialog.text()).toContain('account-1.state')
    const content = JSON.stringify(capturedFile().content, null, 2)
    expect(wrapper.get('pre[aria-label="完整 State 文件内容"]').text()).toBe(content)
    await wrapper.findAll('button').find(b => b.text() === '复制文件内容')!.trigger('click')
    expect(copyToClipboard).toHaveBeenCalledWith(content, '完整文件内容已复制')
    await wrapper.findAll('button').find(b => b.text() === '关闭')!.trigger('click')
    expect(wrapper.text()).not.toContain('saved-state')
    wrapper.unmount()
  })

  it('shows a seconds input only when automatic collection is enabled and saves it', async () => {
    vi.mocked(stateKeeperAPI.save).mockImplementation(async settings => ({ ...initial(), settings: { ...JSON.parse(JSON.stringify(settings)), revision: 'two' } }))
    const wrapper = render(); await flushPromises()
    expect(wrapper.find('input[placeholder="输入秒数"]').exists()).toBe(false)
    const checkbox = wrapper.findAll('label').find(label => label.text() === '定时自动采集')!.get('input')
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
    const input = wrapper.findAll('label').find(label => label.text() === '采集总并发数')!.get('input')
    expect((input.element as HTMLInputElement).value).toBe('50')
    await input.setValue(8)
    await vi.advanceTimersByTimeAsync(5000); await flushPromises()
    expect((input.element as HTMLInputElement).value).toBe('8')
    await wrapper.findAll('button').find(b => b.text() === '保存配置')!.trigger('click'); await flushPromises()
    expect(stateKeeperAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({ concurrency: 8, auto_refresh: false }))
    wrapper.unmount()
  })
})
