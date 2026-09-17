import { afterEach, describe, expect, it, vi } from 'vitest'
import { defineComponent, nextTick, ref } from 'vue'
import { mount, flushPromises } from '@vue/test-utils'
import { useAccountQuality } from '../useAccountQuality'
const fetchResults = vi.hoisted(() => vi.fn())
vi.mock('@/api/admin/accountQuality', () => ({ qualityAPI: { results: fetchResults } }))
afterEach(() => vi.clearAllMocks())
describe('quality status reads', () => {
 it('uses bounded serial batches, no request when hidden, clears obsolete page results', async () => {
  const ids = ref(Array.from({length:205}, (_,i) => i+1)), enabled = ref(true)
  fetchResults.mockImplementation(async (batch: number[]) => ({accounts:batch.map(account_id=>({account_id,version:'v1'}))}))
  let state!: ReturnType<typeof useAccountQuality>
  const wrapper = mount(defineComponent({ setup() { state=useAccountQuality(ids,enabled);return () => null } }))
  await state.refresh();expect(fetchResults.mock.calls.map(c=>c[0].length)).toEqual([100,100,5]);expect(Object.keys(state.results.value)).toHaveLength(205)
  ids.value=[999];await nextTick();expect(state.results.value).toEqual({});await state.refresh();expect(Object.keys(state.results.value)).toEqual(['999'])
  enabled.value=false;await state.refresh();expect(fetchResults).toHaveBeenCalledTimes(4);expect(state.results.value).toEqual({});wrapper.unmount()
 })
 it('does not let stale in-flight response overwrite a newer page', async () => {
  const ids=ref([1]),enabled=ref(true);let finish!:(value:unknown)=>void
  fetchResults.mockImplementationOnce(()=>new Promise(resolve=>{finish=resolve})).mockResolvedValue({accounts:[{account_id:2,version:'new'}]})
  let state!:ReturnType<typeof useAccountQuality>
  const wrapper=mount(defineComponent({setup(){state=useAccountQuality(ids,enabled);return ()=>null}}))
  const old=state.refresh();ids.value=[2];await state.refresh();finish({accounts:[{account_id:1,version:'old'}]});await old;await flushPromises();expect(Object.keys(state.results.value)).toEqual(['2']);wrapper.unmount()
 })
 it('exposes failure instead of keeping a healthy-looking cached result',async()=>{
  const ids=ref([1]),enabled=ref(true);fetchResults.mockRejectedValue(new Error('offline'));let state!:ReturnType<typeof useAccountQuality>
  const wrapper=mount(defineComponent({setup(){state=useAccountQuality(ids,enabled);return ()=>null}}));await state.refresh();expect(state.error.value).toBe(true);expect(state.results.value).toEqual({});wrapper.unmount()
 })
})
