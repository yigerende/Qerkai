import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { defineComponent } from 'vue'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { useSelectedAccountQuality } from '../useSelectedAccountQuality'

const api = vi.hoisted(() => ({ runSelected: vi.fn(), results: vi.fn() }))
vi.mock('@/api/admin/accountQuality', () => ({ qualityAPI: api }))
const old = '2026-09-18T01:00:00Z', fresh = '2026-09-18T02:00:00Z'
const settings = { enabled: true, revision: 'r1' }
function item(id: number, question = old, model = old) {
  return { account_id: id, revision: 'r1', question: { checked_at: question, status: 'normal' }, model: { checked_at: model, status: 'no_samples' }, question_execution: 'queued', model_execution: 'queued' }
}
let state: ReturnType<typeof useSelectedAccountQuality>, wrapper: VueWrapper
function setup() { wrapper = mount(defineComponent({ setup() { state = useSelectedAccountQuality(); return () => null } })) }
beforeEach(() => {
  vi.useFakeTimers()
  vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
  api.runSelected.mockReset().mockImplementation(async (ids: number[]) => ({ revision: 'r1', question_enabled: true, model_audit_enabled: true, accounts: ids.map(account_id => ({ account_id, name: `account ${account_id}`, question_checked_at: old, model_checked_at: old })) }))
  api.results.mockReset().mockImplementation(async (ids: number[]) => ({ settings, accounts: ids.map(id => item(id)) }))
  setup()
})
afterEach(() => { wrapper.unmount(); vi.useRealTimers(); vi.restoreAllMocks() })

describe('selected quality runs', () => {
  it('requires selection and confirmation, snapshots IDs, ignores double submission', async () => {
    state.prepare([]); await state.submit(); expect(api.runSelected).not.toHaveBeenCalled()
    const ids = [9, 3]
    state.prepare(ids); ids.push(4)
    expect(api.runSelected).not.toHaveBeenCalled()
    await state.submit(); await state.submit()
    expect(api.runSelected).toHaveBeenCalledTimes(1)
    expect(api.runSelected.mock.calls[0][0]).toEqual([9, 3])
    state.visible.value = false
    state.prepare([99])
    expect(state.visible.value).toBe(true)
    expect(state.rows.value.map(row => row.id)).toEqual([9, 3])
  })
  it('does not count old results or model completion as question completion', async () => {
    state.prepare([8]); await state.submit(); expect(state.completed.value).toBe(0)
    api.results.mockResolvedValue({ settings, accounts: [item(8, old, fresh)] })
    await vi.advanceTimersByTimeAsync(3000)
    expect(state.completed.value).toBe(0)
    expect(state.rows.value[0].model.state).toBe('done')
    api.results.mockResolvedValue({ settings, accounts: [item(8, fresh, old)] })
    await vi.advanceTimersByTimeAsync(3000)
    expect(state.completed.value).toBe(1)
    expect(state.rows.value[0].model.verdict?.checked_at).toBe(fresh)
    expect(state.active.value).toBe(false)
  })
  it('submits and polls more than 100 selections in bounded serial chunks', async () => {
    state.prepare(Array.from({ length: 205 }, (_, i) => i + 1))
    let inFlight = 0, maximum = 0
    api.results.mockImplementation(async (ids: number[]) => {
      maximum = Math.max(maximum, ++inFlight); await Promise.resolve(); inFlight--
      return { settings, accounts: ids.map(id => item(id, fresh, fresh)) }
    })
    await state.submit()
    expect(api.runSelected.mock.calls.map(call => call[0].length)).toEqual([100, 100, 5])
    expect(api.runSelected.mock.calls[1][1]).toBe('r1')
    expect(api.results.mock.calls.map(call => call[0].length)).toEqual([100, 100, 5])
    expect(maximum).toBe(1); expect(state.completed.value).toBe(205)
  })
  it('shows skipped, disabled, failed and abnormal results as terminal states', async () => {
    api.runSelected.mockResolvedValue({ revision: 'r1', question_enabled: true, model_audit_enabled: false, accounts: [{ account_id: 1, name: 'claude', skip_reason: 'only OpenAI' }, { account_id: 2 }, { account_id: 3 }] })
    api.results.mockResolvedValue({ settings, accounts: [
      { ...item(2, fresh), question: { checked_at: fresh, status: 'error', error: 'timeout' } },
      { ...item(3, fresh), question: { checked_at: fresh, status: 'suspect' } }
    ] })
    state.prepare([1, 2, 3]); await state.submit()
    expect(api.results.mock.calls[0][0]).toEqual([2, 3])
    expect(state.completed.value).toBe(3)
    expect(state.counts.value).toMatchObject({ failed: 1, abnormal: 1, skipped: 1 })
    expect(state.rows.value[1].model.state).toBe('disabled')
  })
  it('retains accepted batches on partial submission failure and never automatically resubmits', async () => {
    api.runSelected.mockRejectedValueOnce(new Error('disconnected'))
    state.prepare(Array.from({ length: 205 }, (_, i) => i + 1)); await state.submit()
    expect(state.counts.value).toMatchObject({ failed: 100, interrupted: 105 })
    expect(api.results).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(10000)
    expect(api.runSelected).toHaveBeenCalledTimes(1)
  })
  it('finishes accepted batches even if later submission fails', async () => {
    const original = api.runSelected.getMockImplementation()!
    api.runSelected.mockImplementationOnce(original).mockRejectedValueOnce(new Error('offline'))
    api.results.mockImplementation(async (ids: number[]) => ({ settings, accounts: ids.map(id => item(id, fresh, fresh)) }))
    state.prepare(Array.from({ length: 205 }, (_, i) => i + 1)); await state.submit()
    expect(state.completed.value).toBe(205)
    expect(state.counts.value).toMatchObject({ failed: 100, interrupted: 5 })
    expect(api.results.mock.calls[0][0]).toHaveLength(100)
  })
  it('interrupts only unfinished rows on config change and skips deleted accounts', async () => {
    state.prepare([1, 2]); await state.submit()
    api.results.mockResolvedValueOnce({ settings, accounts: [{ ...item(1), question_execution: 'unavailable' }, item(2)] })
    await vi.advanceTimersByTimeAsync(3000)
    expect(state.rows.value[0].outcome).toBe('skipped')
    api.results.mockResolvedValueOnce({ settings: { ...settings, revision: 'r2' }, accounts: [item(2)] })
    await vi.advanceTimersByTimeAsync(3000)
    expect(state.rows.value[1].outcome).toBe('interrupted')
    expect(state.active.value).toBe(false)
  })
  it('does not overlap polls, retries read errors, and stops on unmount', async () => {
    let resolve!: (value: unknown) => void
    api.results.mockImplementationOnce(() => new Promise(r => { resolve = r }))
    state.prepare([7]); const pending = state.submit(); await flushPromises()
    await vi.advanceTimersByTimeAsync(15000)
    expect(api.results).toHaveBeenCalledTimes(1)
    resolve({ settings, accounts: [item(7)] }); await pending
    api.results.mockRejectedValueOnce(new Error('temporary'))
    await vi.advanceTimersByTimeAsync(3000); expect(state.error.value).toContain('temporary')
    await vi.advanceTimersByTimeAsync(3000); expect(state.error.value).toBe('')
    wrapper.unmount()
    await vi.advanceTimersByTimeAsync(30000)
    expect(api.results).toHaveBeenCalledTimes(3)
  })
})
