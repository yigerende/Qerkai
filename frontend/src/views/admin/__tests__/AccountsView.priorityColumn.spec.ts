import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'

import AccountsView from '../AccountsView.vue'

const { listAccounts, recentRequests, listWithEtag } = vi.hoisted(() => ({
  listAccounts: vi.fn(),
  recentRequests: vi.fn(),
  listWithEtag: vi.fn()
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    accounts: {
      list: listAccounts,
      listWithEtag,
      getRecentRequests: recentRequests,
      getBatchTodayStats: vi.fn().mockResolvedValue({ stats: {} }),
      getUpstreamBillingProbeSettings: vi.fn().mockResolvedValue({ enabled: true, interval_minutes: 30 }),
      delete: vi.fn(),
      batchClearError: vi.fn(),
      batchRefresh: vi.fn(),
      toggleSchedulable: vi.fn()
    },
    proxies: { getAll: vi.fn().mockResolvedValue([]) },
    groups: { getAll: vi.fn().mockResolvedValue([]) }
  }
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({ showError: vi.fn(), showSuccess: vi.fn(), showInfo: vi.fn() })
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => ({ token: 'test-token', isSimpleMode: false })
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({ t: (key: string) => key })
  }
})

const DataTableStub = {
  props: ['columns'],
  emits: ['sort'],
  template: `
    <div data-test="data-table">
      <span v-for="column in columns" :key="column.key" :data-column="column.key">
        {{ column.sortable ? 'sortable' : 'fixed' }}
      </span>
      <button data-test="sort-priority" @click="$emit('sort', 'priority', 'desc')" />
    </div>
  `
}

function mountView() {
  return mount(AccountsView, {
    global: {
      stubs: {
        AppLayout: { template: '<div><slot /></div>' },
        TablePageLayout: {
          template: '<div><slot name="filters" /><slot name="table" /><slot name="pagination" /></div>'
        },
        DataTable: DataTableStub,
        AccountTableActions: { name: 'AccountTableActions', template: '<div><slot name="after" /></div>' },
        AccountTableFilters: true,
        AccountBulkActionsBar: true,
        Pagination: true,
        ConfirmDialog: true,
        AccountActionMenu: true,
        ImportDataModal: true,
        ReAuthAccountModal: true,
        AccountTestModal: true,
        AccountStatsModal: true,
        ScheduledTestsPanel: true,
        SyncFromCrsModal: true,
        TempUnschedStatusModal: true,
        ErrorPassthroughRulesModal: true,
        TLSFingerprintProfilesModal: true,
        CreateAccountModal: true,
        EditAccountModal: true,
        BulkEditAccountModal: true,
        PlatformTypeBadge: true,
        AccountCapacityCell: true,
        AccountStatusIndicator: true,
        AccountTodayStatsCell: true,
        AccountGroupsCell: true,
        AccountUsageCell: true,
        HelpTooltip: true,
        Icon: true,
        Teleport: true
      }
    }
  })
}

describe('admin AccountsView priority column preferences', () => {
  beforeEach(() => {
    localStorage.clear()
    recentRequests.mockReset().mockResolvedValue({ accounts: [] })
    listWithEtag.mockReset().mockResolvedValue({ notModified: true, etag: 'same' })
    listAccounts.mockReset().mockResolvedValue({
      items: [],
      total: 0,
      page: 1,
      page_size: 20,
      pages: 0
    })
  })

  it('shows priority as a sortable column for fresh preferences', async () => {
    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.get('[data-column="priority"]').text()).toBe('sortable')

    await wrapper.get('[data-test="sort-priority"]').trigger('click')
    await flushPromises()

    expect(listAccounts).toHaveBeenLastCalledWith(
      1,
      20,
      expect.objectContaining({ sort_by: 'priority', sort_order: 'desc' }),
      expect.objectContaining({ signal: expect.any(AbortSignal) })
    )
  })

  it('refreshes recent requests manually and preserves optional column visibility', async () => {
    listAccounts.mockResolvedValue({ items: [{ id: 7, name: 'account', platform: 'openai', type: 'oauth' }], total: 1, pages: 1 })
    const wrapper = mountView()
    await flushPromises()
    expect(wrapper.find('[data-column="recent_requests"]').exists()).toBe(true)
    expect(wrapper.find('[data-column="recent_error"]').exists()).toBe(true)
    expect(recentRequests).toHaveBeenCalledWith([7], expect.any(AbortSignal))
    recentRequests.mockClear()
    wrapper.findComponent({ name: 'AccountTableActions' }).vm.$emit('refresh')
    await flushPromises()
    expect(recentRequests).toHaveBeenCalledTimes(1)
    await wrapper.get('[title="admin.accounts.moreActions"]').trigger('click')
    const columnButton = (label: string) => wrapper.findAll('button').find(button => button.text() === label)!
    await columnButton('admin.accounts.columns.recentRequests').trigger('click')
    await columnButton('admin.accounts.columns.recentError').trigger('click')
    expect(wrapper.find('[data-column="recent_requests"]').exists()).toBe(false)
    expect(wrapper.find('[data-column="recent_error"]').exists()).toBe(false)
    expect(JSON.parse(localStorage.getItem('account-hidden-columns') || '[]')).toEqual(expect.arrayContaining(['recent_requests', 'recent_error']))
    recentRequests.mockClear()
    wrapper.findComponent({ name: 'AccountTableActions' }).vm.$emit('refresh')
    await flushPromises()
    expect(recentRequests).not.toHaveBeenCalled()
    await columnButton('admin.accounts.columns.recentError').trigger('click')
    await flushPromises()
    expect(recentRequests).toHaveBeenCalledTimes(1)
    wrapper.unmount()
  })

  it('refreshes request history on the existing auto-refresh even when accounts return 304', async () => {
    vi.useFakeTimers()
    localStorage.setItem('account-auto-refresh', JSON.stringify({ enabled: true, interval_seconds: 5 }))
    listAccounts.mockResolvedValue({ items: [{ id: 7, name: 'account', platform: 'openai', type: 'oauth' }], total: 1, pages: 1 })
    const wrapper = mountView()
    try {
      await flushPromises()
      recentRequests.mockClear()
      await vi.advanceTimersByTimeAsync(31000)
      await flushPromises()
      expect(listWithEtag).toHaveBeenCalled()
      expect(recentRequests).toHaveBeenCalledWith([7], expect.any(AbortSignal))
    } finally {
      wrapper.unmount()
      vi.useRealTimers()
    }
  })

  it('preserves an existing preference that explicitly hides priority', async () => {
    localStorage.setItem('account-hidden-columns', JSON.stringify(['priority', 'today_stats']))
    localStorage.setItem('account-hidden-columns-version', 'scheduler-score-hidden-by-default')

    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.find('[data-column="priority"]').exists()).toBe(false)
    expect(JSON.parse(localStorage.getItem('account-hidden-columns') || '[]')).toEqual([
      'priority',
      'today_stats'
    ])
  })

  it('keeps priority visible while migrating older saved preferences', async () => {
    localStorage.setItem('account-hidden-columns', JSON.stringify(['today_stats']))

    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.get('[data-column="priority"]').text()).toBe('sortable')
    expect(JSON.parse(localStorage.getItem('account-hidden-columns') || '[]')).toEqual(
      expect.arrayContaining(['today_stats', 'scheduler_score'])
    )
    expect(JSON.parse(localStorage.getItem('account-hidden-columns') || '[]')).not.toContain('priority')
  })
})
