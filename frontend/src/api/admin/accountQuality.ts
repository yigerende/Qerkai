import { apiClient } from '../client'
export interface QualityQuestion { id: string; name: string; enabled: boolean; prompt: string; answer: string; match_mode: string; max_duration_ms: number }
export interface QualitySettings {
 all_groups: boolean; group_ids: number[];
 enabled: boolean; revision: string; question_enabled: boolean; model_audit_enabled: boolean;
 questions: QualityQuestion[]; model: string; model_audit_model: string; reasoning_effort: string; mode: string;
 interval_seconds: number; model_audit_interval_seconds: number; retry_seconds: number; failure_limit: number;
 recovery_limit: number; concurrency: number; timeout_seconds: number; history_limit: number
 degradation_mode: 'any' | 'all'; degradation_conditions: ('question' | 'model')[]
 pause_on_degradation?: boolean
}
export interface QualityVerdict { status?: string; degraded: boolean; failures: number; successes: number; checked_at?: string; evidence_at?: string; next_at?: string; error?: string }
export interface QualityResult {
 overall?: { status: 'degraded' | 'pending' | 'normal'; reason: string; conditions: { kind: string; status: string }[] };
 detection_kind?: 'question' | 'model' | 'state_refresh' | 'recovery'; recorded_at?: string;
 scheduling?: { paused: boolean; state_required?: boolean; quality_paused?: boolean; since?: string; next_at?: string; successes: number; error?: string }
 question_execution?: string; model_execution?: string;
 account_id: number; revision: string; version: string;
 question: QualityVerdict & { question_name?: string; answer?: string; reason?: string; duration_ms: number };
 model: QualityVerdict & { sent_model?: string; response_model?: string; no_new_samples: boolean; state_version?: string; state_collected_at?: string; state_validation_pending?: boolean }
}
export interface QualityBatch {
 id: string; status: string; total: number; done: number; failed: number; skipped: number;
 started_at: string; finished_at?: string; updated_at: string; last_error?: string;
 pending_ids: number[]; running: { account_id: number; started_at: string }[]
}
export interface QualityDetectorProgress {
 enabled: boolean; total: number; checked: number; unchecked: number; pending: number; next_at?: string; batch?: QualityBatch
}
export interface QualityProgress { server_now: string; question: QualityDetectorProgress; model: QualityDetectorProgress }
export type QualityPolicy = Pick<QualitySettings, 'enabled' | 'revision' | 'question_enabled' | 'model_audit_enabled' | 'failure_limit' | 'recovery_limit' | 'model_audit_model' | 'degradation_mode' | 'degradation_conditions'>
export interface QualitySelectedAccount { account_id: number; name: string; skip_reason?: string; question_checked_at?: string; model_checked_at?: string }
export interface QualitySelectedRun { revision: string; question_enabled: boolean; model_audit_enabled: boolean; accounts: QualitySelectedAccount[] }
export const qualityAPI = {
 settings: async () => (await apiClient.get<{ settings: QualitySettings; summary: Record<string, number>; progress: QualityProgress }>('/admin/account-quality/settings')).data,
 progress: async (signal?: AbortSignal) => (await apiClient.get<{ summary: Record<string, number>; progress: QualityProgress }>('/admin/account-quality/progress', { signal })).data,
 save: async (settings: QualitySettings) => (await apiClient.put<{ settings: QualitySettings }>('/admin/account-quality/settings', settings)).data,
 results: async (account_ids: number[], signal?: AbortSignal) => (await apiClient.post<{ accounts: QualityResult[]; settings: QualityPolicy; server_now: string }>('/admin/account-quality/results', { account_ids }, { signal })).data,
 runSelected: async (account_ids: number[], revision?: string, signal?: AbortSignal) => (await apiClient.post<QualitySelectedRun>('/admin/account-quality/run-selected', { account_ids, revision }, { signal })).data,
 run: async (account_ids: number[] = []) => (await apiClient.post('/admin/account-quality/run', { account_ids })).data,
 history: async (id: number) => (await apiClient.get<{ items: QualityResult[] }>(`/admin/account-quality/accounts/${id}/history`)).data
}
export function qualityExecutionLabel(execution: string | undefined, verdict?: QualityVerdict) {
 const previous = verdict?.status ? `（上次：${qualityStatusLabel(verdict)}）` : ''
 if (execution === 'running') return '检测中' + previous
 if (execution === 'queued') return '排队中' + previous
 if (execution === 'disabled') return '已关闭' + previous
 if (execution === 'excluded') return '不在定时分组' + previous
 return qualityStatusLabel(verdict)
}
export function qualityStatusLabel(v?: QualityVerdict) {
 const labels: Record<string, string> = { normal: '正常', variant: '版本别名一致', suspect: '疑似异常', degraded: '已确认异常', error: '检测失败', no_samples: '暂无样本', state_pending: '新 State 待验证' }
 return labels[v?.status || ''] || '未检测'
}
export const qualityOverallLabel = (status?: string) => ({ degraded: '降智', pending: '待检测', normal: '无降智' }[status || 'pending'] || '待检测')
