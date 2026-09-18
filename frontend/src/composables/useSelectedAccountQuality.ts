import { computed, onScopeDispose, ref } from 'vue'
import { qualityAPI, type QualityResult, type QualityVerdict } from '@/api/admin/accountQuality'

type CheckState = 'queued' | 'running' | 'done' | 'disabled'
export interface SelectedQualityCheck { state: CheckState; baseline?: string; verdict?: QualityVerdict }
export interface SelectedQualityRow {
  id: number; name: string; submitted: boolean; issue?: string;
  outcome?: 'skipped' | 'error' | 'interrupted';
  question: SelectedQualityCheck; model: SelectedQualityCheck
}
export const selectedQualityDone = (row: SelectedQualityRow) => !!row.outcome ||
  (row.submitted && [row.question, row.model].every(check => check.state === 'done' || check.state === 'disabled'))

function errorText(error: unknown) {
  const e = error as { response?: { data?: { message?: string } }; message?: string }
  return e?.response?.data?.message || e?.message || '请求失败'
}

export function useSelectedAccountQuality(onResults?: (results: QualityResult[]) => void) {
  const visible = ref(false)
  const stage = ref<'confirm' | 'run'>('confirm')
  const rows = ref<SelectedQualityRow[]>([])
  const submitting = ref(false)
  const error = ref('')
  const revision = ref('')
  const completed = computed(() => rows.value.filter(selectedQualityDone).length)
  const active = computed(() => submitting.value || (stage.value === 'run' && completed.value < rows.value.length))
  const counts = computed(() => ({
    failed: rows.value.filter(row => row.outcome === 'error' || [row.question, row.model].some(c => c.verdict?.status === 'error')).length,
    abnormal: rows.value.filter(row => [row.question, row.model].some(c => c.verdict?.degraded || c.verdict?.status === 'suspect')).length,
    skipped: rows.value.filter(row => row.outcome === 'skipped').length,
    interrupted: rows.value.filter(row => row.outcome === 'interrupted').length
  }))
  let timer: ReturnType<typeof setTimeout> | undefined
  let controller = new AbortController()
  let disposed = false

  function prepare(ids: number[]) {
    if (active.value) { visible.value = true; return }
    const unique = [...new Set(ids.filter(id => Number.isSafeInteger(id) && id > 0))]
    if (!unique.length) return
    clearTimeout(timer)
    controller.abort()
    controller = new AbortController()
    revision.value = ''
    error.value = ''
    rows.value = unique.map(id => ({ id, name: `#${id}`, submitted: false, question: { state: 'queued' }, model: { state: 'queued' } }))
    stage.value = 'confirm'
    visible.value = true
  }

  function queuePoll() {
    if (!disposed && active.value) timer = setTimeout(poll, 3000)
  }

  async function submit() {
    if (stage.value !== 'confirm' || !rows.value.length || disposed) return
    stage.value = 'run'
    submitting.value = true
    for (let i = 0; i < rows.value.length && !disposed; i += 100) {
      const batch = rows.value.slice(i, i + 100)
      try {
        const result = await qualityAPI.runSelected(batch.map(row => row.id), revision.value || undefined, controller.signal)
        if (disposed) return
        revision.value = result.revision
        const byID = new Map(result.accounts.map(item => [item.account_id, item]))
        for (const row of batch) {
          const item = byID.get(row.id)
          if (!item) { row.outcome = 'error'; row.issue = '提交结果中缺少该账号'; continue }
          row.name = item.name || row.name
          if (item.skip_reason) { row.outcome = 'skipped'; row.issue = item.skip_reason; continue }
          row.submitted = true
          row.question = { state: result.question_enabled ? 'queued' : 'disabled', baseline: item.question_checked_at }
          row.model = { state: result.model_audit_enabled ? 'queued' : 'disabled', baseline: item.model_checked_at }
        }
      } catch (e) {
        if (disposed) return
        error.value = errorText(e)
        // A lost POST response may have been accepted. Never automatically resubmit it.
        for (const row of batch) { row.outcome = 'error'; row.issue = `提交未确认：${error.value}` }
        for (const row of rows.value.slice(i + 100)) { row.outcome = 'interrupted'; row.issue = '前一批提交失败，未提交' }
        break
      }
    }
    submitting.value = false
    await poll()
  }

  async function poll() {
    if (disposed || !active.value) return
    if (document.hidden) { queuePoll(); return }
    const pending = rows.value.filter(row => row.submitted && !selectedQualityDone(row))
    try {
      for (let i = 0; i < pending.length; i += 100) {
        const batch = pending.slice(i, i + 100)
        const result = await qualityAPI.results(batch.map(row => row.id), controller.signal)
        if (disposed) return
        if (result.settings.revision !== revision.value || !result.settings.enabled) {
          for (const row of pending.filter(row => !selectedQualityDone(row))) {
            row.outcome = 'interrupted'; row.issue = '检测配置已变更或关闭，请重新提交'
          }
          break
        }
        onResults?.(result.accounts)
        const byID = new Map(result.accounts.map(item => [item.account_id, item]))
        for (const row of batch) {
          const item = byID.get(row.id)
          if (!item || item.question_execution === 'unavailable') {
            row.outcome = 'skipped'; row.issue = '账号已删除或不再支持检测'; continue
          }
          for (const kind of ['question', 'model'] as const) {
            const check = row[kind]
            if (check.state === 'done' || check.state === 'disabled') continue
            const verdict = item[kind]
            if (item.revision === revision.value && verdict.checked_at && verdict.checked_at !== check.baseline) {
              check.state = 'done'; check.verdict = { ...verdict }
            } else {
              check.state = item[`${kind}_execution`] === 'running' ? 'running' : 'queued'
            }
          }
        }
      }
      if (!rows.value.some(row => row.outcome === 'error')) error.value = ''
    } catch (e) {
      if (!disposed) error.value = `进度读取失败，将重试：${errorText(e)}`
    } finally { queuePoll() }
  }

  onScopeDispose(() => { disposed = true; clearTimeout(timer); controller.abort() })
  return { visible, stage, rows, submitting, error, completed, counts, active, prepare, submit }
}
