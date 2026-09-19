import apiClient from '../client'

export interface StateKeeperSettings {
  enabled: boolean
  injection_enabled: boolean
  response_refresh_enabled: boolean
  auto_refresh: boolean
  auto_collect_interval_seconds: number
  degradation_scan_enabled: boolean
  degradation_scan_interval_seconds: number
  concurrency: number
  account_concurrency: number
  max_attempts: number
  retry_count: number
  retry_interval_seconds: number
  request_interval_seconds: number
  proxy_failure_threshold: number
  cooldown_seconds: number
  max_cooldown_seconds: number
  account_hourly_limit: number
  account_five_minute_limit: number
  account_ten_minute_limit: number
  allowed_state_lengths: number[]
  degraded_state_lengths: number[]
  account_ids: number[]
  collection_group_ids: number[]
  group_ids: number[]
  all_groups: boolean
  proxy_id: number
  proxy_ids?: number[]
  model: string
  models?: string[]
  revision: string
}

export interface StateKeeperRow {
  account_id: number
  account_name?: string
  account_group_ids?: number[]
  account_status?: string
  account_unavailable?: boolean
  account_unavailable_reason?: string
  model: string
  status: string
  queued: boolean
  collecting: boolean
  http_status: number
  turn_state_length: number
  message: string
  last_attempt_at?: string
  last_collection_at?: string
  collected_at?: string
  next_attempt_at?: string
  fingerprint?: string
  has_codex_turn_state: boolean
  has_details: boolean
  state_file_saved?: boolean
  attempts: number
  successes: number
  injections: number
  paused: boolean
  pause_reason: string
  round_id: string
  round_attempts: number
  round_source: string
  quality_status: string
  quality_reason: string
  state_preview?: string
  saved_state_length?: number
  retry_attempt?: number
  retry_limit?: number
  collection_proxy_id?: number
  saved_proxy_id?: number
  proxy_attempt?: number
  proxy_count?: number
  next_retry_at?: string
  auto_retry_pending?: boolean
  retry_reason?: string
  failure_cycles?: number
  cooldown_until?: string
  hourly_requests?: number
  five_minute_requests?: number
  ten_minute_requests?: number
  effective_concurrency?: number
  models?: StateKeeperRow[]
}

export interface StateKeeperEvent {
  at: string
  account_id: number
  model: string
  http_status: number
  turn_state_length: number
  result: string
  message: string
  id?: string
  kind: string
  source: string
  round_id?: string
  attempt: number
  injected_length?: number
  proxy_id?: number
}

export interface StateKeeperRecent {
  account_id: number
  paused: boolean
  pause_reason: string
  collections: StateKeeperEvent[]
  injections: StateKeeperEvent[]
}

export interface StateKeeperSnapshot {
  collection_path: string
  collection_endpoint: string
  settings: StateKeeperSettings
  rows: StateKeeperRow[]
  events: StateKeeperEvent[]
  server_time: string
  config_error?: string
  proxy_successes?: Record<string, number>
  proxy_stats_error?: string
}

export interface StateKeeperDetail {
  account_id: number
  model: string
  http_status: number
  header_name: string
  header_value: string
  turn_state_length: number
  collected_at: string
  save_allowed: boolean
  state_file_saved: boolean
}

export interface StateKeeperFileDetail {
  file_name: string
  content: {
    account_id: number
    model: string
    proxy_id: number
    endpoint: string
    header_name: string
    credential_stamp: string
    turn_state: string
    collected_at: string
  }
}

export const stateKeeperAPI = {
  async recent(accountIds: number[], signal?: AbortSignal) {
    return (await apiClient.post<{ accounts: StateKeeperRecent[] }>('/admin/openai-state-keeper/recent', { account_ids: accountIds }, { signal })).data
  },
  async get() {
    return (await apiClient.get<StateKeeperSnapshot>('/admin/openai-state-keeper')).data
  },
  async detail(accountId: number, model?: string) {
    return (await apiClient.get<StateKeeperDetail>(`/admin/openai-state-keeper/accounts/${accountId}/state`, { params: { model } })).data
  },
  async fileDetail(accountId: number, model?: string) {
    return (await apiClient.get<StateKeeperFileDetail>(`/admin/openai-state-keeper/accounts/${accountId}/file`, { params: { model } })).data
  },
  async save(settings: StateKeeperSettings) {
    return (await apiClient.put<StateKeeperSnapshot>('/admin/openai-state-keeper', settings)).data
  },
  async collect(accountIds: number[] = [], model?: string) {
    return (await apiClient.post('/admin/openai-state-keeper/collect', { account_ids: accountIds, model })).data
  },
  async collectPaused() {
    return (await apiClient.post<{ scheduled: boolean; scheduled_count: number }>('/admin/openai-state-keeper/collect', { paused_only: true })).data
  },
  async collectCooling() {
    return (await apiClient.post<{ scheduled: boolean; scheduled_count: number }>('/admin/openai-state-keeper/collect', { cooling_only: true })).data
  },
}
