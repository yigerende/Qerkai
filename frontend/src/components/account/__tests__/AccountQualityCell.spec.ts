import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import AccountQualityCell from '../AccountQualityCell.vue'
import type { QualityResult } from '@/api/admin/accountQuality'

describe('account model verification after State refresh', () => {
  it('replaces the stale red model verdict with pending and then displays fresh recovery', async () => {
    const result: QualityResult = {
      account_id: 1, revision: 'policy', version: 'old',
      question: { status: 'normal', degraded: false, failures: 0, successes: 2, duration_ms: 100 },
      model: { status: 'degraded', degraded: true, failures: 2, successes: 0, no_new_samples: true },
      overall: { status: 'degraded', reason: 'model mismatch', conditions: [] }
    }
    const wrapper = mount(AccountQualityCell, { props: { accountId: 1, result } })
    expect(wrapper.text()).toContain('模型：已确认异常')
    expect(wrapper.findAll('span').find(node => node.text().startsWith('模型：'))?.classes()).toContain('text-red-600')
    const refreshed: QualityResult = {
      ...result,
      model: { status: 'state_pending', degraded: false, failures: 0, successes: 0, no_new_samples: true, state_validation_pending: true, state_collected_at: '2026-09-19T00:00:00Z' },
      overall: { status: 'pending', reason: 'waiting for new model evidence', conditions: [] }
    }
    await wrapper.setProps({ result: refreshed })
    expect(wrapper.text()).toContain('答题：正常')
    expect(wrapper.text()).toContain('模型：新 State 待验证')
    expect(wrapper.text()).toContain('综合：待检测')
    expect(wrapper.findAll('span').find(node => node.text().startsWith('模型：'))?.classes()).not.toContain('text-red-600')
    await wrapper.setProps({ result: { ...refreshed, model: { ...refreshed.model, status: 'normal', successes: 2, state_validation_pending: false }, overall: { status: 'normal', reason: 'verified', conditions: [] } } })
    expect(wrapper.text()).toContain('模型：正常')
    expect(wrapper.text()).toContain('综合：无降智')
    wrapper.unmount()
  })
})
