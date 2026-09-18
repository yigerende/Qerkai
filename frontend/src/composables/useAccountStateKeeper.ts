import { onUnmounted, ref, watch, type Ref } from 'vue'
import { stateKeeperAPI, type StateKeeperRecent } from '@/api/admin/openaiStateKeeper'

export function useAccountStateKeeper(accountIDs: Ref<number[]>, enabled: Ref<boolean>) {
  const results = ref<Record<number, StateKeeperRecent>>({})
  const loading = ref(false)
  const error = ref(false)
  let controller: AbortController | null = null
  let pending: Promise<void> | null = null
  const invalidate = () => {
    controller?.abort()
    controller = null
    pending = null
    results.value = {}
    loading.value = false
    error.value = false
  }
  watch(() => accountIDs.value.join(','), invalidate, { flush: 'sync' })
  watch(enabled, invalidate, { flush: 'sync' })
  onUnmounted(invalidate)
  const refresh = (): Promise<void> => {
    if (!enabled.value || !accountIDs.value.length) { invalidate(); return Promise.resolve() }
    if (pending) return pending
    const current = new AbortController()
    controller = current
    const ids = [...accountIDs.value]
    loading.value = true
    error.value = false
    pending = (async () => {
      try {
        const next: Record<number, StateKeeperRecent> = {}
        for (let offset = 0; offset < ids.length; offset += 100) {
          const batch = ids.slice(offset, offset + 100)
          const data = await stateKeeperAPI.recent(batch, current.signal)
          if (controller !== current) return
          const allowed = new Set(batch)
          for (const item of data.accounts) if (allowed.has(item.account_id)) next[item.account_id] = item
        }
        if (controller === current) results.value = next
      } catch {
        if (controller === current) { results.value = {}; error.value = true }
      } finally {
        if (controller === current) { loading.value = false; pending = null; controller = null }
      }
    })()
    return pending
  }
  return { results, loading, error, refresh }
}
