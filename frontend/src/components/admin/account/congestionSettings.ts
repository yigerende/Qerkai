export const defaultCongestionSettings = {
  window_seconds: 60,
  sample_limit: 256,
  slow_min_samples: 5,
  slow_seconds: 15,
  slow_percent: 50,
  slow_min_load_percent: 0,
  stalled_seconds: 20,
  stalled_min_requests: 5,
  stalled_percent: 20,
  error_min_samples: 5,
  error_min_count: 3,
  error_percent: 30,
  full_load_percent: 100,
  full_wait_seconds: 2,
  target_max_load_percent: 50,
  target_min_free_slots: 1,
  target_min_samples: 3,
  target_faster_percent: 30,
  protection_seconds: 120,
  pending_seconds: 600,
  enabled: false,
  slow_enabled: true,
  stalled_enabled: true,
  errors_enabled: true,
  error_429: true,
  error_5xx: true,
  error_network: true,
  full_enabled: true,
  target_require_faster: true,
  target_allow_unknown: false,
  avoid_unhealthy_new_sessions: true
}

export type CongestionSettings = typeof defaultCongestionSettings
type NumberKey = { [K in keyof CongestionSettings]: CongestionSettings[K] extends number ? K : never }[keyof CongestionSettings]
type BooleanKey = { [K in keyof CongestionSettings]: CongestionSettings[K] extends boolean ? K : never }[keyof CongestionSettings]
export const congestionNumericFields: Record<NumberKey, { key: NumberKey; min: number; max: number }> = {
  window_seconds: { key: 'window_seconds', min: 10, max: 3600 },
  sample_limit: { key: 'sample_limit', min: 5, max: 4096 },
  slow_min_samples: { key: 'slow_min_samples', min: 1, max: 4096 },
  slow_seconds: { key: 'slow_seconds', min: 1, max: 600 },
  slow_percent: { key: 'slow_percent', min: 1, max: 100 },
  slow_min_load_percent: { key: 'slow_min_load_percent', min: 0, max: 100 },
  stalled_seconds: { key: 'stalled_seconds', min: 1, max: 600 },
  stalled_min_requests: { key: 'stalled_min_requests', min: 1, max: 10000 },
  stalled_percent: { key: 'stalled_percent', min: 1, max: 100 },
  error_min_samples: { key: 'error_min_samples', min: 1, max: 4096 },
  error_min_count: { key: 'error_min_count', min: 1, max: 4096 },
  error_percent: { key: 'error_percent', min: 1, max: 100 },
  full_load_percent: { key: 'full_load_percent', min: 1, max: 100 },
  full_wait_seconds: { key: 'full_wait_seconds', min: 0, max: 30 },
  target_max_load_percent: { key: 'target_max_load_percent', min: 1, max: 100 },
  target_min_free_slots: { key: 'target_min_free_slots', min: 1, max: 10000 },
  target_min_samples: { key: 'target_min_samples', min: 1, max: 4096 },
  target_faster_percent: { key: 'target_faster_percent', min: 1, max: 99 },
  protection_seconds: { key: 'protection_seconds', min: 0, max: 3600 },
  pending_seconds: { key: 'pending_seconds', min: 30, max: 3600 },
}
export const congestionSections: Array<{ key: string; toggle?: BooleanKey; fields: NumberKey[]; checks?: BooleanKey[] }> = [
  {key: "observation", fields: ["window_seconds", "sample_limit"]},
  {key: "slow", fields: ["slow_min_samples", "slow_seconds", "slow_percent", "slow_min_load_percent"], toggle: "slow_enabled"},
  {key: "stalled", fields: ["stalled_seconds", "stalled_min_requests", "stalled_percent"], toggle: "stalled_enabled"},
  {key: "errors", fields: ["error_min_samples", "error_min_count", "error_percent"], toggle: "errors_enabled", checks: ["error_429", "error_5xx", "error_network"]},
  {key: "full", fields: ["full_load_percent", "full_wait_seconds"], toggle: "full_enabled"},
  {key: "target", fields: ["target_max_load_percent", "target_min_free_slots", "target_min_samples", "target_faster_percent"], checks: ["target_require_faster", "target_allow_unknown"]},
  {key: "session", fields: ["protection_seconds", "pending_seconds"], checks: ["avoid_unhealthy_new_sessions"]},
]
export function validCongestionSettings(c: CongestionSettings): boolean {
 return Object.values(congestionNumericFields).every(f => Number.isInteger(c[f.key]) && c[f.key] >= f.min && c[f.key] <= f.max)
  && [c.slow_min_samples, c.error_min_samples, c.error_min_count, c.target_min_samples].every(n => n <= c.sample_limit)
}
