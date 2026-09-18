import apiClient from '../client'

export interface StateKeeperSettings {
  enabled: boolean
  injection_enabled: boolean
  auto_refresh: boolean
  auto_collect_interval_seconds: number
  concurrency: number
  account_ids: number[]
  group_ids: number[]
  all_groups: boolean
  proxy_id: number
  model: string
  ttl_seconds: number
  refresh_before_seconds: number
  retry_seconds: number
  revision: string
}

export interface StateKeeperRow {
  account_id: number
  model: string
  status: string
  queued: boolean
  collecting: boolean
  http_status: number
  message: string
  last_attempt_at?: string
  collected_at?: string
  expires_at?: string
  next_attempt_at?: string
  fingerprint?: string
  has_codex_turn_state: boolean
  state_file_saved?: boolean
  attempts: number
  successes: number
  injections: number
}

export interface StateKeeperEvent {
  at: string
  account_id: number
  model: string
  http_status: number
  result: string
  message: string
}

export interface StateKeeperSnapshot {
  collection_path: string
  collection_endpoint: string
  settings: StateKeeperSettings
  rows: StateKeeperRow[]
  events: StateKeeperEvent[]
  server_time: string
  config_error?: string
}

export const stateKeeperAPI = {
  async get() {
    return (await apiClient.get<StateKeeperSnapshot>('/admin/openai-state-keeper')).data
  },
  async save(settings: StateKeeperSettings) {
    return (await apiClient.put<StateKeeperSnapshot>('/admin/openai-state-keeper', settings)).data
  },
  async collect(accountIds: number[] = []) {
    return (await apiClient.post('/admin/openai-state-keeper/collect', { account_ids: accountIds })).data
  },
}
