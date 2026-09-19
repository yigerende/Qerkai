import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import View from '../AccountQualityView.vue'
import Cell from '@/components/account/AccountQualityCell.vue'
import { qualityAPI, type QualitySettings, type QualityProgress, type QualityResult } from '@/api/admin/accountQuality'
import { getAllIncludingInactive } from '@/api/admin/groups'
import type { AdminGroup } from '@/types'

vi.mock('@/components/layout/AppLayout.vue', () => ({ default: { template: '<main><slot /></main>' } }))
vi.mock('@/api/admin/groups', () => ({ getAllIncludingInactive: vi.fn() }))
vi.mock('vue-i18n', async importOriginal => ({ ...await importOriginal<typeof import('vue-i18n')>(), useI18n: () => ({ t: (key: string) => key }) }))
vi.mock('@/stores/app', () => ({ useAppStore: () => ({ cachedPublicSettings: {} }) }))
vi.mock('@/api/admin/accountQuality', async importOriginal => ({
 ...await importOriginal<typeof import('@/api/admin/accountQuality')>(),
 qualityAPI: { settings: vi.fn(), progress: vi.fn(), save: vi.fn(), run: vi.fn(), history: vi.fn() }
}))

const progress = (): QualityProgress => ({ server_now: new Date().toISOString(),
 question: { enabled:true,total:62,checked:4,unchecked:58,pending:54,batch:{id:'b1',status:'running',total:32,done:4,failed:1,skipped:0,started_at:new Date().toISOString(),updated_at:new Date().toISOString(),pending_ids:[9,10],running:[{account_id:1550,started_at:new Date().toISOString()}]} },
 model: { enabled:true,total:62,checked:62,unchecked:0,pending:0 }
})
const settings = { enabled:true,all_groups:true,group_ids:[],question_enabled:true,model_audit_enabled:true,model:'gpt-6-astra',mode:'content_time',questions:[] } as unknown as QualitySettings
const render = () => mount(View, { global: { stubs: { Icon:true } } })

describe('quality detection progress', () => {
 beforeEach(() => {
  vi.useFakeTimers(); vi.clearAllMocks()
  vi.spyOn(window,'confirm').mockReturnValue(true)
  vi.mocked(qualityAPI.settings).mockResolvedValue({settings,summary:{total:62},progress:progress()})
  vi.mocked(qualityAPI.progress).mockResolvedValue({summary:{total:62},progress:progress()})
  vi.mocked(qualityAPI.run).mockResolvedValue({scheduled:true})
  vi.mocked(getAllIncludingInactive).mockResolvedValue([
   {id:10,name:'检测组A',platform:'openai'}, {id:20,name:'检测组B',platform:'composite'}, {id:30,name:'其他平台',platform:'claude'}
  ] as AdminGroup[])
  vi.mocked(qualityAPI.save).mockImplementation(async value => ({settings:value}))
 })
 afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers() })
 it('separates pending State validation from accounts without model samples', async () => {
  vi.mocked(qualityAPI.settings).mockResolvedValue({settings,summary:{total:62,model_state_pending:7,no_samples:2},progress:progress()})
  const w=render(); await flushPromises()
  const value=(label:string)=>w.findAll('.quality-stats article').find(item=>item.get('span').text()===label)!.get('strong').text()
  expect(value('新 State 待验证')).toBe('7')
  expect(value('模型无样本')).toBe('2')
  vi.mocked(qualityAPI.progress).mockResolvedValue({summary:{total:62,model_state_pending:0,no_samples:2},progress:progress()})
  await vi.advanceTimersByTimeAsync(3000); await flushPromises()
  expect(value('新 State 待验证')).toBe('0')
  expect(value('模型无样本')).toBe('2')
  expect(qualityAPI.run).not.toHaveBeenCalled()
  w.unmount()
 })
 it('saves selected overall conditions and concurrency above eight without an input maximum', async () => {
  const w=render(); await flushPromises()
  await w.findAll('button').find(b=>b.text()==='检测配置')!.trigger('click')
  expect((w.get('input[type="radio"][value="any"]').element as HTMLInputElement).checked).toBe(true)
  expect((w.get('input[name="pause_on_degradation"]').element as HTMLInputElement).checked).toBe(false)
  await w.get('input[name="pause_on_degradation"]').setValue(true)
  await w.get('input[type="radio"][value="all"]').setValue(true)
  await w.get('input[type="checkbox"][value="model"]').setValue(false)
  const concurrency = w.findAll('label').find(label => label.text() === '并发数')!.get('input')
  expect(concurrency.attributes('max')).toBeUndefined()
  await concurrency.setValue(40)
  await w.get('form').trigger('submit'); await flushPromises()
  expect(qualityAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({degradation_mode:'all', degradation_conditions:['question'],pause_on_degradation:true,concurrency:40}))
  w.unmount()
 })
 it('shows the independent scheduling pause and recovery progress', () => {
  const result={overall:{status:'normal',reason:'正常',conditions:[]},scheduling:{paused:true,successes:1,error:'等待第二次复检'}} as unknown as QualityResult
  const w=mount(Cell,{props:{accountId:1,result},global:{stubs:{Teleport:true}}})
  expect(w.text()).toContain('调度：降智暂停 · 连续正常 1')
  expect(w.text()).toContain('综合：无降智')
  w.unmount()
 })
 it('shows all three overall states and the explanation independently of collection', async () => {
  for (const [status, label] of [['degraded','降智'],['normal','无降智'],['pending','待检测']] as const) {
   const result={overall:{status,reason:'判定原因',conditions:[]}} as unknown as QualityResult
   vi.mocked(qualityAPI.history).mockResolvedValue({items:[]})
   const w=mount(Cell,{props:{accountId:1,result},global:{stubs:{Teleport:true}}})
   expect(w.text()).toContain(`综合：${label}`)
   await w.get('button').trigger('click'); await flushPromises()
   expect(w.text()).toContain('判定原因')
   w.unmount()
  }
 })
 it('shows pending, active IDs and batch counts; polls without loading the prompt bank or overwriting edits', async () => {
  const w=render(); await flushPromises()
  expect(w.text()).toContain('当前配置已检测 4/62')
  expect(w.text()).toContain('本批完成 4/32')
  expect(w.text()).toContain('待调度 54')
  expect(w.text()).toContain('账号 #1550')
  const input=w.get('input[maxlength="200"]'); await input.setValue('edited-model')
  const next=progress(); next.question.checked=32; next.question.batch!.done=32; next.question.batch!.status='done'; next.question.batch!.running=[]
  vi.mocked(qualityAPI.progress).mockResolvedValue({summary:{total:62},progress:next})
  await vi.advanceTimersByTimeAsync(3000); await flushPromises()
  expect(w.text()).toContain('最近一批完成 32/32')
  expect((input.element as HTMLInputElement).value).toBe('edited-model')
  expect(qualityAPI.settings).toHaveBeenCalledTimes(1)
  expect(qualityAPI.run).not.toHaveBeenCalled()
  await w.findAll('button').find(b=>b.text()==='立即检测')!.trigger('click'); await flushPromises()
  expect(qualityAPI.run).toHaveBeenCalledTimes(1)
  expect(w.text()).toContain('进度每 3 秒更新')
  w.unmount(); const count=vi.mocked(qualityAPI.progress).mock.calls.length
  await vi.advanceTimersByTimeAsync(9000); expect(qualityAPI.progress).toHaveBeenCalledTimes(count)
 })
 it('defaults to all groups, persists multiple selected groups, and keeps edits while polling', async () => {
  const w=render(); await flushPromises()
  await w.findAll('button').find(b=>b.text()==='检测配置')!.trigger('click')
  const all=w.findAll('label').find(label=>label.text()==='全部分组')!.get('input')
  expect((all.element as HTMLInputElement).checked).toBe(true)
  await all.setValue(false)
  await w.get('form').trigger('submit'); await flushPromises()
  expect(w.text()).toContain('请选择至少一个定时检测分组')
  expect(qualityAPI.save).not.toHaveBeenCalled()
  expect(w.text()).not.toContain('其他平台')
  await w.get('input[type="checkbox"][value="10"]').setValue(true)
  await w.get('input[type="checkbox"][value="20"]').setValue(true)
  await vi.advanceTimersByTimeAsync(3000)
  expect((w.get('input[value="10"]').element as HTMLInputElement).checked).toBe(true)
  expect(getAllIncludingInactive).toHaveBeenCalledTimes(1)
  await w.get('form').trigger('submit'); await flushPromises()
  expect(qualityAPI.save).toHaveBeenCalledWith(expect.objectContaining({all_groups:false,group_ids:[10,20]}))
  await all.setValue(true)
  await w.get('form').trigger('submit'); await flushPromises()
  expect(qualityAPI.save).toHaveBeenLastCalledWith(expect.objectContaining({all_groups:true}))
  w.unmount()
 })
 it('restores a saved subset and preserves it when group loading fails', async () => {
  vi.mocked(qualityAPI.settings).mockResolvedValue({settings:{...settings,all_groups:false,group_ids:[10,99]},summary:{total:2},progress:progress()})
  vi.mocked(getAllIncludingInactive).mockRejectedValueOnce(new Error('offline'))
  const w=render(); await flushPromises()
  await w.findAll('button').find(b=>b.text()==='检测配置')!.trigger('click')
  expect(w.text()).toContain('分组读取失败')
  await w.get('form').trigger('submit'); await flushPromises()
  expect(qualityAPI.save).toHaveBeenCalledWith(expect.objectContaining({all_groups:false,group_ids:[10,99]}))
  await w.get('button[title="重新读取分组"]').trigger('click'); await flushPromises()
  expect(w.text()).toContain('不可用分组 #99')
  expect((w.get('input[value="10"]').element as HTMLInputElement).checked).toBe(true)
  w.unmount()
 })
 it('does not overlap slow polling requests and cancels on unmount', async () => {
  let resolve!: (value: {summary:Record<string,number>;progress:QualityProgress}) => void
  vi.mocked(qualityAPI.progress).mockReturnValue(new Promise(r=>{resolve=r}))
  const w=render(); await flushPromises(); await vi.advanceTimersByTimeAsync(12000)
  expect(qualityAPI.progress).toHaveBeenCalledTimes(1)
  const signal=vi.mocked(qualityAPI.progress).mock.calls[0]![0]!
  w.unmount(); expect(signal.aborted).toBe(true)
  resolve({summary:{},progress:progress()}); await flushPromises()
 })
 it('distinguishes executing accounts and model-only history from a fresh question run', async () => {
  const record={account_id:1550,version:'v1',revision:'r1',detection_kind:'model',question_execution:'running',model_execution:'idle',
   question:{status:'normal',degraded:false,failures:0,successes:1,checked_at:new Date().toISOString(),question_name:'旧答题',answer:'21',duration_ms:100},
   model:{status:'degraded',degraded:true,failures:2,successes:0,checked_at:new Date().toISOString(),sent_model:'gpt-6-astra',response_model:'gpt-5.6-luna',no_new_samples:false}} as QualityResult
  vi.mocked(qualityAPI.history).mockResolvedValue({items:[record]})
  const w=mount(Cell,{props:{accountId:1550,result:record},global:{stubs:{Teleport:true}}})
  expect(w.text()).toContain('答题：检测中（上次：正常）')
  await w.get('button').trigger('click'); await flushPromises()
  expect(w.text()).toContain('模型一致性检测')
  expect(w.text()).not.toContain('旧答题')
  expect(w.text()).not.toContain('100 ms')
  expect(w.text()).toContain('gpt-6-astra → gpt-5.6-luna')
  w.unmount()
 })
})
