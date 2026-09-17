import { apiClient } from '../client'
export interface QualityQuestion { id: string; name: string; enabled: boolean; prompt: string; answer: string; match_mode: string; max_duration_ms: number }
export interface QualitySettings {
 enabled: boolean; revision: string; question_enabled: boolean; model_audit_enabled: boolean;
 questions: QualityQuestion[]; model: string; model_audit_model: string; reasoning_effort: string; mode: string;
 interval_seconds: number; model_audit_interval_seconds: number; retry_seconds: number; failure_limit: number;
 recovery_limit: number; concurrency: number; timeout_seconds: number; history_limit: number
}
export interface QualityVerdict { status?: string; degraded: boolean; failures: number; successes: number; checked_at?: string; evidence_at?: string; next_at?: string; error?: string }
export interface QualityResult {
 account_id: number; revision: string; version: string;
 question: QualityVerdict & { question_name?: string; answer?: string; reason?: string; duration_ms: number };
 model: QualityVerdict & { sent_model?: string; response_model?: string; no_new_samples: boolean }
}
export const qualityAPI = {
 settings: async () => (await apiClient.get<{ settings: QualitySettings; summary: Record<string, number> }>('/admin/account-quality/settings')).data,
 save: async (settings: QualitySettings) => (await apiClient.put<{ settings: QualitySettings }>('/admin/account-quality/settings', settings)).data,
 results: async (account_ids: number[], signal?: AbortSignal) => (await apiClient.post<{ accounts: QualityResult[]; settings: QualitySettings; server_now: string }>('/admin/account-quality/results', { account_ids }, { signal })).data,
 run: async (account_ids: number[] = []) => (await apiClient.post('/admin/account-quality/run', { account_ids })).data,
 history: async (id: number) => (await apiClient.get<{ items: QualityResult[] }>(`/admin/account-quality/accounts/${id}/history`)).data
}
export function qualityStatusLabel(v?: QualityVerdict) {
 const labels: Record<string, string> = { normal: '正常', variant: '版本别名一致', suspect: '疑似异常', degraded: '已确认异常', error: '检测失败', no_samples: '暂无样本' }
 return labels[v?.status || ''] || '未检测'
}
