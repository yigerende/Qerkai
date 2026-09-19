import { beforeEach, describe, expect, it, vi } from 'vitest'

const { get } = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('@/api/client', () => ({ apiClient: { get } }))
import { exportData, getUpstreamBillingRatesWithEtag, list, listWithEtag } from '@/api/admin/accounts'

describe('account quality filter requests', () => {
  beforeEach(() => {
    get.mockReset().mockResolvedValue({ data: { items: [], total: 0 }, status: 200, headers: {} })
  })

  it.each(['degraded', 'normal', 'pending'])('preserves %s in list, refresh, billing snapshot and export', async (status) => {
    const filters = { quality_status: status, group: '7' }
    await list(2, 10, filters)
    await listWithEtag(2, 10, filters)
    await getUpstreamBillingRatesWithEtag(2, 10, filters)
    await exportData({ filters })
    expect(get).toHaveBeenCalledTimes(4)
    for (const [, config] of get.mock.calls) {
      expect(config.params).toMatchObject(filters)
    }
  })
})
