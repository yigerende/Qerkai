import apiClient from '../client'

export interface OpenAIUpstream5xxRetryRule {
  id: string
  name: string
  enabled: boolean
  status_code: 502 | 503
  match_mode: 'all' | 'any'
  keywords: string[]
}

export interface OpenAIUpstream5xxRetryMatch {
  matched: boolean
  rule_id?: string
  rule_name?: string
  status_code?: number
  reason: string
}

export const defaultOpenAIUpstream5xxRetryRules = (): OpenAIUpstream5xxRetryRule[] => [
  { id: 'processing_error', name: '502 processing error', enabled: true, status_code: 502, match_mode: 'all', keywords: ['An error occurred while processing your request', 'You can retry your request'] },
  { id: 'server_overloaded', name: '503 server overloaded', enabled: true, status_code: 503, match_mode: 'all', keywords: ['Our servers are currently overloaded', 'Please try again later'] },
]

export async function previewOpenAIUpstream5xxRetryRule(rules: OpenAIUpstream5xxRetryRule[], payload: string): Promise<OpenAIUpstream5xxRetryMatch> {
  const { data } = await apiClient.post<OpenAIUpstream5xxRetryMatch>('/admin/ops/openai-upstream-5xx-retry/preview', { rules, payload })
  return data
}
