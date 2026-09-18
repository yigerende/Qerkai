import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import Cell from '../AccountStateKeeperCell.vue'
import type { StateKeeperEvent } from '@/api/admin/openaiStateKeeper'

const event = (kind: string, result: string): StateKeeperEvent => ({ kind, result, account_id: 1, at: '2026-09-18T08:00:00Z', model: 'test', http_status: 200, turn_state_length: 332, attempt: 3, source: 'manual', message: '已保存', injected_length: kind === 'injection' ? 332 : 0 })

describe('account collection and injection history', () => {
  it('keeps ten stable bars per lane and exposes result details', async () => {
    const w = mount(Cell, { props: { accountId: 1, recent: { account_id: 1, paused: true, pause_reason: '等待人工重试', collections: [event('collection', 'collected')], injections: [{ ...event('injection', 'sent'), message: '已携带 State 发送，恢复情况以降智检测为准' }] } }, global: { stubs: { BaseDialog: { props: ['show'], template: '<div v-if="show" role="dialog"><slot /></div>' } } } })
    expect(w.findAll('[data-state-result]')).toHaveLength(20)
    expect(w.get('[data-state-result="collected"]').attributes('title')).toContain('本轮第 3 次')
    expect(w.text()).toContain('采集已暂停')
    await w.get('button[aria-label="查看最近注入记录"]').trigger('click')
    expect(w.get('[role="dialog"]').text()).toContain('注入长度 332')
    expect(w.get('[role="dialog"]').text()).toContain('恢复情况以降智检测为准')
    expect(w.get('[role="dialog"]').text()).toContain('等待人工重试')
    w.unmount()
  })
})
