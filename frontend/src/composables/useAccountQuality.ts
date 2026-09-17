import { onUnmounted, ref, watch, type Ref } from 'vue'
import { qualityAPI, type QualityResult } from '@/api/admin/accountQuality'
export function useAccountQuality(ids: Ref<number[]>, enabled: Ref<boolean>) {
 const results = ref<Record<number, QualityResult>>({})
 const error = ref(false)
 let controller: AbortController | null = null
 let pending: Promise<void> | null = null
 const invalidate = () => { controller?.abort(); controller = null; pending = null; results.value = {}; error.value = false }
 watch(() => ids.value.join(','), invalidate, { flush: 'sync' })
 watch(enabled, invalidate, { flush: 'sync' })
 onUnmounted(invalidate)
 const refresh = (): Promise<void> => {
  if (!enabled.value || !ids.value.length) { invalidate(); return Promise.resolve() }
  if (pending) return pending
  const current = new AbortController(); controller = current
  const selected = [...ids.value]; error.value = false
  pending = (async () => {
   try {
    const next: Record<number, QualityResult> = {}
    for (let i = 0; i < selected.length; i += 100) {
     const batch = selected.slice(i, i + 100)
     const data = await qualityAPI.results(batch, current.signal)
     if (controller !== current) return
     for (const item of data.accounts) if (batch.includes(item.account_id)) next[item.account_id] = item
    }
    if (controller === current) results.value = next
   } catch { if (controller === current) { results.value = {}; error.value = true } }
   finally { if (controller === current) { controller = null; pending = null } }
  })()
  return pending
 }
 return { results, error, refresh }
}
