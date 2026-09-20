import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
vi.mock('@/api/admin/openaiStateKeeper', () => ({ stateKeeperAPI: { recent: vi.fn().mockResolvedValue({ accounts: [] }) } }))
import { defineComponent } from 'vue'

import AccountsView from '../AccountsView.vue'
import AccountActionMenu from '@/components/admin/account/AccountActionMenu.vue'
import AccountBulkActionsBar from '@/components/admin/account/AccountBulkActionsBar.vue'
import AccountTableFilters from '@/components/admin/account/AccountTableFilters.vue'
import BulkEditAccountModal from '@/components/account/BulkEditAccountModal.vue'
import { qualityAPI } from '@/api/admin/accountQuality'

vi.mock('@/api/admin/accountQuality', () => ({ qualityAPI: { runSelected: vi.fn(), results: vi.fn().mockResolvedValue({ accounts: [] }), history: vi.fn().mockResolvedValue({ items: [] }) }, qualityStatusLabel: () => '未检测', qualityExecutionLabel: () => '未检测' }))

const {
  listAccounts,
  listWithEtag,
  getById,
  getBatchTodayStats,
  getUpstreamBillingProbeSettings,
  getAllProxies,
  getAllGroups,
  showError
} = vi.hoisted(() => ({
  listAccounts: vi.fn(),
  listWithEtag: vi.fn(),
  getById: vi.fn(),
  getBatchTodayStats: vi.fn(),
  getUpstreamBillingProbeSettings: vi.fn(),
  getAllProxies: vi.fn(),
  getAllGroups: vi.fn(),
  showError: vi.fn()
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    accounts: {
      list: listAccounts,
      getById,
      listWithEtag,
      getBatchTodayStats,
      getUpstreamBillingProbeSettings,
      delete: vi.fn(),
      batchClearError: vi.fn(),
      batchRefresh: vi.fn(),
      toggleSchedulable: vi.fn()
    },
    proxies: { getAll: getAllProxies },
    groups: { getAll: getAllGroups }
  }
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({ showError, showSuccess: vi.fn(), showInfo: vi.fn() })
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => ({ token: 'test-token', isSimpleMode: false })
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return { ...actual, useI18n: () => ({ t: (key: string) => key }) }
})

const DataTableStub = defineComponent({
  props: { data: { type: Array, default: () => [] } },
  template: `
    <div>
      <div v-for="row in data" :key="row.id">
        <slot name="cell-select" :row="row" />
        <slot name="cell-groups" :row="row" />
        <slot name="cell-actions" :row="row" />
      </div>
    </div>
  `
})

const AccountGroupsCellStub = defineComponent({
  props: { groups: { type: Array, default: () => [] } },
  template: '<span data-test="account-groups">{{ groups.map(group => group.name).join(",") }}</span>'
})

const EditAccountModalStub = defineComponent({
  props: { show: Boolean, account: { type: Object, default: null } },
  template: '<div data-test="edit-account">{{ show ? account?.name : "" }}</div>'
})

const AccountTestModalStub = defineComponent({
  props: { show: Boolean, account: { type: Object, default: null } },
  template: '<div data-test="test-account">{{ show ? account?.name : "" }}</div>'
})

const AccountStatsModalStub = defineComponent({
  props: { show: Boolean, account: { type: Object, default: null } },
  template: '<div data-test="stats-account">{{ show ? account?.name : "" }}</div>'
})

function mountView() {
  return mount(AccountsView, {
    global: {
      stubs: {
        AppLayout: { template: '<div><slot /></div>' },
        TablePageLayout: { template: '<div><slot name="filters" /><slot name="table" /><slot name="pagination" /></div>' },
        DataTable: DataTableStub,
        AccountTableActions: { template: '<div><slot name="after" /></div>' },
        AccountTableFilters: true,
        AccountBulkActionsBar: true,
        Pagination: true,
        ConfirmDialog: true,
        AccountActionMenu: true,
        ImportDataModal: true,
        ReAuthAccountModal: true,
        AccountTestModal: AccountTestModalStub,
        AccountStatsModal: AccountStatsModalStub,
        ScheduledTestsPanel: true,
        SyncFromCrsModal: true,
        TempUnschedStatusModal: true,
        ErrorPassthroughRulesModal: true,
        TLSFingerprintProfilesModal: true,
        CreateAccountModal: true,
        EditAccountModal: EditAccountModalStub,
        BulkEditAccountModal: true,
        PlatformTypeBadge: true,
        AccountCapacityCell: true,
        AccountStatusIndicator: true,
        AccountTodayStatsCell: true,
        AccountGroupsCell: AccountGroupsCellStub,
        AccountUsageCell: true,
        UpstreamBillingRateCell: true,
        HelpTooltip: true,
        Icon: true,
        Teleport: true
      }
    }
  })
}

const listRow = {
  id: 42,
  name: 'compact row',
  platform: 'openai',
  type: 'oauth',
  status: 'active',
  schedulable: true,
  concurrency: 2,
  priority: 1,
  group_ids: [7],
  extra: {},
  credentials: {}
}

const fullAccount = {
  ...listRow,
  groups: [{ id: 7, name: 'codex', platform: 'openai' }],
  account_groups: [{ account_id: 42, group_id: 7 }],
  credentials: { api_key: 'redacted' },
  extra: { detail_only: true }
}

describe('admin AccountsView lite account list', () => {
  beforeEach(() => {
    vi.mocked(qualityAPI.runSelected).mockReset()
    localStorage.clear()
    listAccounts.mockReset().mockResolvedValue({ items: [listRow], total: 1, page: 1, page_size: 20, pages: 1 })
    listWithEtag.mockReset().mockResolvedValue({ notModified: true, etag: 'compact-etag', data: null })
    getById.mockReset().mockResolvedValue(fullAccount)
    getBatchTodayStats.mockReset().mockResolvedValue({ stats: {} })
    getUpstreamBillingProbeSettings.mockReset().mockResolvedValue({ enabled: true })
    getAllProxies.mockReset().mockResolvedValue([])
    getAllGroups.mockReset().mockResolvedValue([{ id: 7, name: 'codex', platform: 'openai' }])
    showError.mockReset()
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('keeps lite=1 on the initial list request', async () => {
    const wrapper = mountView()
    await flushPromises()

    expect(listAccounts).toHaveBeenCalledWith(
      1,
      20,
      expect.objectContaining({ lite: '1' }),
      expect.objectContaining({ signal: expect.any(AbortSignal) })
    )
    wrapper.unmount()
  })

  it.each(['degraded', 'normal', 'pending'])('preserves quality filter %s across reload, all-result selection and bulk edit', async (qualityStatus) => {
    vi.useFakeTimers()
    const wrapper = mountView()
    await flushPromises()
    const filters = wrapper.findComponent(AccountTableFilters)
    filters.vm.$emit('update:filters', { quality_status: qualityStatus })
    filters.vm.$emit('change')
    await vi.advanceTimersByTimeAsync(500)
    await flushPromises()
    expect(listAccounts).toHaveBeenLastCalledWith(1, 20, expect.objectContaining({ quality_status: qualityStatus }), expect.any(Object))
    wrapper.findComponent(AccountBulkActionsBar).vm.$emit('select-all-results')
    await flushPromises()
    expect(listAccounts).toHaveBeenLastCalledWith(1, 1000, expect.objectContaining({ quality_status: qualityStatus }))
    wrapper.findComponent(AccountBulkActionsBar).vm.$emit('edit-filtered')
    await flushPromises()
    expect(wrapper.findComponent(BulkEditAccountModal).props('target')).toMatchObject({ filters: { quality_status: qualityStatus } })
    expect(qualityAPI.runSelected).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('connects the bulk action to only the confirmed selection and exposes progress', async () => {
    listAccounts.mockResolvedValue({ items: [listRow, { ...listRow, id: 99 }], total: 2, page: 1, page_size: 20, pages: 1 })
    vi.mocked(qualityAPI.runSelected).mockResolvedValue({ revision: 'r1', question_enabled: true, model_audit_enabled: false, accounts: [{ account_id: 42, name: 'compact row', skip_reason: 'fixture skip' }] })
    const wrapper = mountView()
    await flushPromises()
    await wrapper.findAll('input[type="checkbox"]')[0].setValue(true)
    wrapper.findComponent(AccountBulkActionsBar).vm.$emit('quality-detect')
    await flushPromises()
    expect(wrapper.text()).toContain('所选 1 个账号')
    expect(qualityAPI.runSelected).not.toHaveBeenCalled()
    await wrapper.findAll('button').find(button => button.text() === '开始检测')!.trigger('click')
    await flushPromises()
    expect(qualityAPI.runSelected).toHaveBeenCalledWith([42], undefined, expect.any(AbortSignal))
    expect(wrapper.text()).toContain('1 / 1')
    expect(wrapper.text()).toContain('查看进度')
    wrapper.unmount()
  })

  it('maps group_ids through the group catalog for the table cell', async () => {
    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.get('[data-test="account-groups"]').text()).toBe('codex')
    wrapper.unmount()
  })

  it('keeps lite=1 on automatic ETag refreshes', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    localStorage.setItem('account-auto-refresh', JSON.stringify({ enabled: true, interval_seconds: 5 }))
    const wrapper = mountView()
    await flushPromises()

    wrapper.findComponent(AccountTableFilters).vm.$emit('update:filters', { quality_status: 'normal' })
    await vi.advanceTimersByTimeAsync(6000)
    await flushPromises()

    expect(listWithEtag).toHaveBeenCalledWith(
      1,
      20,
      expect.objectContaining({ lite: '1', quality_status: 'normal' }),
      expect.objectContaining({ etag: null })
    )
    wrapper.unmount()
  })

  it.each([0, 42, null])('refreshes RPM-only changes to %s without another polling request', async (requestRpm) => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    localStorage.setItem('account-auto-refresh', JSON.stringify({ enabled: true, interval_seconds: 5 }))
    listWithEtag.mockResolvedValue({
      notModified: false,
      etag: 'changed-rpm',
      data: { items: [{ ...listRow, request_rpm: requestRpm }], total: 1, page: 1, page_size: 20, pages: 1 }
    })
    const wrapper = mountView()
    await flushPromises()
    await vi.advanceTimersByTimeAsync(6000)
    await flushPromises()

    expect(listWithEtag).toHaveBeenCalledTimes(1)
    expect(wrapper.findComponent(DataTableStub).props('data')).toEqual([
      expect.objectContaining({ id: 42, request_rpm: requestRpm })
    ])
    wrapper.unmount()
  })

  it('loads the full account by id before opening edit, test, and stats actions', async () => {
    const wrapper = mountView()
    await flushPromises()

    const editButton = wrapper.findAll('button').find(button => button.text().includes('common.edit'))
    expect(editButton).toBeTruthy()
    await editButton!.trigger('click')
    await flushPromises()
    expect(getById).toHaveBeenCalledWith(42)
    expect(wrapper.get('[data-test="edit-account"]').text()).toBe('compact row')

    const menu = wrapper.findComponent(AccountActionMenu)
    menu.vm.$emit('test', listRow)
    await flushPromises()
    expect(getById).toHaveBeenCalledTimes(2)
    expect(wrapper.get('[data-test="test-account"]').text()).toBe('compact row')

    menu.vm.$emit('stats', listRow)
    await flushPromises()
    expect(getById).toHaveBeenCalledTimes(3)
    expect(wrapper.get('[data-test="stats-account"]').text()).toBe('compact row')
    wrapper.unmount()
  })

  it('shows an error and keeps the modal closed when detail loading fails', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    getById.mockRejectedValueOnce(new Error('detail failed'))
    const wrapper = mountView()
    await flushPromises()

    const editButton = wrapper.findAll('button').find(button => button.text().includes('common.edit'))
    await editButton!.trigger('click')
    await flushPromises()

    expect(showError).toHaveBeenCalledWith('detail failed')
    expect(wrapper.get('[data-test="edit-account"]').text()).toBe('')
    consoleError.mockRestore()
    wrapper.unmount()
  })
})
