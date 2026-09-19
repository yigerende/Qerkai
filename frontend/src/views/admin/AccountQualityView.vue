<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import Icon from '@/components/icons/Icon.vue'
import AccountQualityProgress from '@/components/account/AccountQualityProgress.vue'
import GroupSelector from '@/components/common/GroupSelector.vue'
import { getAllIncludingInactive } from '@/api/admin/groups'
import type { AdminGroup } from '@/types'
import { qualityAPI, type QualitySettings, type QualityProgress } from '@/api/admin/accountQuality'
const form = ref<QualitySettings | null>(null)
const summary = ref<Record<string, number>>({})
const progress = ref<QualityProgress | null>(null)
const groups = ref<AdminGroup[]>([])
const groupsError = ref(''), groupsLoading = ref(false)
const missingGroups = computed(() => (form.value?.group_ids || []).filter(id => !groups.value.some(group => group.id === id)))
const normalizeSettings = (settings: QualitySettings): QualitySettings => ({ ...settings, pause_on_degradation: settings.pause_on_degradation ?? false, all_groups: settings.all_groups ?? true, group_ids: settings.group_ids || [], degradation_mode: settings.degradation_mode || 'any', degradation_conditions: settings.degradation_conditions ?? ([settings.question_enabled ? 'question' : '', settings.model_audit_enabled ? 'model' : ''].filter(Boolean) as ('question' | 'model')[]) })
async function loadGroups() {
 if (groupsLoading.value) return
 groupsLoading.value = true; groupsError.value = ''
 try { const items = await getAllIncludingInactive(); if (!disposed) groups.value = items.filter(group => group.platform === 'openai' || group.platform === 'composite') }
 catch (e) { if (!disposed) groupsError.value = '分组读取失败：' + errorText(e) }
 finally { groupsLoading.value = false }
}
let progressController: AbortController | null = null
let disposed = false
const busy = ref(false), message = ref(''), tab = ref('questions'), selected = ref('')
const stats = [['total','定时检测账号'],['normal','答题正常'],['suspect','答题疑似异常'],['degraded','已确认异常'],['errors','答题失败'],['model_normal','模型一致'],['model_degraded','模型异常'],['no_samples','模型无样本']]
const errorText = (e: unknown) => e instanceof Error ? e.message : '请求失败'
async function load(reset = false) { try { const data = await qualityAPI.settings(); if (disposed) return; summary.value = data.summary; progress.value = data.progress; if (reset || !form.value) form.value = normalizeSettings(data.settings) } catch (e) { if (!disposed) message.value = errorText(e) } }
async function refreshProgress() {
 if (progressController || disposed) return
 const controller = new AbortController(); progressController = controller
 try { const data = await qualityAPI.progress(controller.signal); if (!disposed) { summary.value = data.summary; progress.value = data.progress } }
 catch (e) { if (!controller.signal.aborted) message.value = '进度读取失败：' + errorText(e) }
 finally { if (progressController === controller) progressController = null }
}
async function save() { if (!form.value) return; if (!form.value.all_groups && !form.value.group_ids.length) { message.value = '请选择至少一个定时检测分组，或选择全部分组'; return } busy.value = true; try { form.value = normalizeSettings((await qualityAPI.save(form.value)).settings); message.value = '配置已保存'; await load() } catch (e) { message.value = errorText(e) } finally { busy.value = false } }
async function run() { if (!window.confirm('按已保存的分组范围检测 OpenAI 账号，答题会消耗实际额度。继续？')) return; busy.value = true; try { await qualityAPI.run(); message.value = '检测已排队，正在检测的账号继续执行，其余按顺序处理。进度每 3 秒更新。'; await refreshProgress() } catch (e) { message.value = errorText(e) } finally { busy.value = false } }
function add() { if (!form.value) return; const id = crypto.randomUUID ? crypto.randomUUID() : `q-${Date.now()}`; form.value.questions.push({ id, name:'新题目', enabled:true, prompt:'', answer:'', match_mode:'answer', max_duration_ms:20000 }); selected.value = id }
function remove(index: number) { if (window.confirm('删除该题目？')) form.value?.questions.splice(index, 1) }
function move(index: number, offset: number) { const list = form.value?.questions; if (!list || index + offset < 0 || index + offset >= list.length) return; const q = list.splice(index, 1)[0]; if (q) list.splice(index + offset, 0, q) }
async function importConfig(event: Event) {
 const file = (event.target as HTMLInputElement).files?.[0]; if (!file || !form.value) return
 try {
  if (file.size > 1 << 20) throw new Error('配置文件过大')
  const raw = JSON.parse(await file.text()), q = raw.settings || raw
  const questions = Array.isArray(q.questions) ? q.questions : q.prompt ? [{ id:'legacy',name:'原有题目',enabled:true,prompt:q.prompt,answer:q.answer || '',match_mode:q.match_mode || 'answer',max_duration_ms:q.max_duration_ms || 20000 }] : form.value.questions
  form.value = normalizeSettings({ ...form.value, ...q, questions, enabled:false })
  selected.value = ''; message.value = '配置已载入，尚未保存'
 } catch (e) { message.value = errorText(e) }
}
let timer: ReturnType<typeof setInterval> | undefined
onMounted(() => { void load(); void loadGroups(); timer = setInterval(() => { if (document.visibilityState !== 'hidden') void refreshProgress() }, 3000) })
onUnmounted(() => { disposed = true; clearInterval(timer); progressController?.abort() })
</script>
<template>
 <AppLayout>
  <div class="quality-page">
   <div class="quality-stats"><article v-for="s in stats" :key="s[0]"><span>{{ s[1] }}</span><strong>{{ summary[s[0]!] || 0 }}</strong></article></div>
   <header class="flex flex-wrap items-center justify-between gap-3 py-5"><h1 class="text-xl font-semibold">降智检测设置</h1><button class="btn btn-secondary" title="刷新统计" @click="load()"><Icon name="refresh" size="md" /></button></header>
   <p v-if="message" role="status" class="mb-4 break-words text-sm">{{ message }}</p>
   <div v-if="progress" class="mb-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
    <AccountQualityProgress title="答题检测" :value="progress.question" :server-now="progress.server_now" />
    <AccountQualityProgress title="模型一致性检测" :value="progress.model" :server-now="progress.server_now" />
   </div>
   <form v-if="form" @submit.prevent="save">
    <label class="flex items-center gap-2 py-3"><input v-model="form.enabled" type="checkbox">启用降智检测</label>
    <nav class="quality-tabs"><button v-for="item in [{id:'questions',name:'题目检测'},{id:'models',name:'模型一致性'},{id:'schedule',name:'检测配置'}]" :key="item.id" type="button" :class="{active:tab === item.id}" @click="tab = item.id">{{ item.name }}</button></nav>
    <div v-if="tab === 'questions'" class="quality-grid">
     <label class="check"><input v-model="form.question_enabled" type="checkbox">启用题目检测</label>
     <label>判断方式<select v-model="form.mode" class="input"><option value="content_time">内容＋耗时</option><option value="content">只看内容</option><option value="time">只看耗时</option></select></label>
     <label>检测模型<input v-model="form.model" class="input" maxlength="200" required></label>
     <label>推理强度<select v-model="form.reasoning_effort" class="input"><option v-for="v in ['low','medium','high','xhigh']" :key="v">{{ v }}</option></select></label>
     <div class="wide"><div class="flex justify-between"><h2 class="font-medium">检测题库</h2><button type="button" class="btn btn-secondary" :disabled="form.questions.length >= 50" @click="add"><Icon name="plus" size="sm" />新增题目</button></div>
      <div class="overflow-x-auto"><table class="quality-table"><thead><tr><th>启用</th><th>题目</th><th>答案</th><th>阈值</th><th>操作</th></tr></thead><tbody><tr v-for="(q,i) in form.questions" :key="q.id"><td><input v-model="q.enabled" type="checkbox" :aria-label="'启用 ' + q.name"></td><td><button type="button" class="text-primary-600" @click="selected = q.id">{{ q.name }}</button></td><td class="max-w-48 break-words">{{ q.answer }}</td><td>{{ q.max_duration_ms }} ms</td><td><div class="flex gap-2"><button type="button" title="上移" :disabled="i === 0" @click="move(i,-1)">↑</button><button type="button" title="下移" :disabled="i === form.questions.length - 1" @click="move(i,1)">↓</button><button type="button" title="删除题目" @click="remove(i)"><Icon name="trash" size="sm" /></button></div></td></tr></tbody></table></div>
     </div>
     <template v-for="q in form.questions.filter(q => q.id === selected)" :key="q.id">
      <label>题目名称<input v-model="q.name" class="input" maxlength="60" required></label><label>耗时阈值（ms）<input v-model.number="q.max_duration_ms" class="input" type="number" min="1" max="300000" required></label>
      <label class="wide">题目<textarea v-model="q.prompt" class="input" rows="7" required /></label>
      <label>匹配方式<select v-model="q.match_mode" class="input"><option value="answer">标准答案</option><option value="keyword">包含关键词</option><option value="regex">正则表达式</option></select></label><label>答案 / 关键词 / 正则<textarea v-model="q.answer" class="input" rows="2" :required="form.mode !== 'time'" /></label>
     </template>
    </div>
    <div v-if="tab === 'models'" class="quality-grid"><label class="check"><input v-model="form.model_audit_enabled" type="checkbox">启用模型一致性检测</label><label>实际发送模型<input v-model="form.model_audit_model" class="input" required></label><label>检查间隔（秒）<input v-model.number="form.model_audit_interval_seconds" class="input" type="number" min="10" max="86400" required></label><div>每账号样本上限：3</div></div>
    <div v-if="tab === 'schedule'" class="quality-grid">
     <div class="wide space-y-3">
      <h2 class="text-sm font-medium">定时检测分组</h2>
      <label class="check"><input v-model="form.all_groups" type="checkbox">全部分组</label>
      <template v-if="!form.all_groups">
       <p v-if="groupsLoading" class="text-sm text-gray-500">读取分组中...</p>
       <div v-else-if="groupsError" role="alert" class="flex items-center gap-2 text-sm text-red-600">{{ groupsError }}<button type="button" title="重新读取分组" @click="loadGroups"><Icon name="refresh" size="sm" /></button></div>
       <template v-else>
        <GroupSelector v-model="form.group_ids" class="quality-group-selector" :groups="groups" platform="openai" :searchable="true" />
        <label v-for="id in missingGroups" :key="id" class="check text-amber-600"><input v-model="form.group_ids" type="checkbox" :value="id">不可用分组 #{{ id }}</label>
       </template>
      </template>
     </div>
     <label>答题间隔（秒）<input v-model.number="form.interval_seconds" class="input" type="number" min="10" max="86400" required></label>
     <label>异常 / 失败复测间隔（秒）<input v-model.number="form.retry_seconds" class="input" type="number" min="10" max="86400" required></label>
     <label>连续异常次数<input v-model.number="form.failure_limit" class="input" type="number" min="1" max="20" required></label>
     <label>连续正常恢复次数<input v-model.number="form.recovery_limit" class="input" type="number" min="1" max="20" required></label>
     <fieldset class="space-y-2 md:col-span-2"><legend class="mb-2 font-medium">综合降智判断</legend>
      <div class="flex flex-wrap gap-5"><label class="check"><input v-model="form.degradation_conditions" type="checkbox" value="question">答题异常</label><label class="check"><input v-model="form.degradation_conditions" type="checkbox" value="model">模型不一致</label></div>
      <div class="flex flex-wrap gap-5"><label class="check"><input v-model="form.degradation_mode" type="radio" value="any">任一勾选条件满足</label><label class="check"><input v-model="form.degradation_mode" type="radio" value="all">全部勾选条件满足</label></div>
      <label class="check"><input v-model="form.pause_on_degradation" type="checkbox" name="pause_on_degradation">综合降智时暂停账号调度，复检恢复后自动解除</label>
     </fieldset>
     <label>并发数<input v-model.number="form.concurrency" class="input" type="number" min="1" required></label>
     <label>请求超时（秒）<input v-model.number="form.timeout_seconds" class="input" type="number" min="5" max="300" required></label>
     <label>每账号历史上限<input v-model.number="form.history_limit" class="input" type="number" min="1" max="1000" required></label>
     <label>导入原降智配置<input type="file" accept="application/json,.json" @change="importConfig"></label>
    </div>
    <footer class="flex flex-wrap gap-3 border-t py-5 dark:border-dark-700"><button type="submit" class="btn btn-primary" :disabled="busy">保存配置</button><button type="button" class="btn btn-secondary" :disabled="busy || !form.enabled" @click="run">立即检测</button></footer>
   </form>
  </div>
 </AppLayout>
</template>
<style scoped>
.quality-group-selector :deep(.truncate){white-space:normal;overflow-wrap:anywhere}
@media(max-width:640px){.quality-group-selector :deep(.grid){grid-template-columns:minmax(0,1fr)}}
.quality-page{max-width:1280px;margin:0 auto}.quality-stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));grid-template-rows:repeat(2,minmax(90px,auto));gap:12px}.quality-stats article{border:1px solid #94a3b840;border-radius:6px;padding:14px;min-width:0}.quality-stats span{font-size:12px;overflow-wrap:anywhere}.quality-stats strong{display:block;font-size:24px;line-height:1.5}.quality-stats article:nth-child(2) strong,.quality-stats article:nth-child(6) strong{color:#059669}.quality-stats article:nth-child(4) strong,.quality-stats article:nth-child(7) strong{color:#dc2626}.quality-tabs{display:flex;gap:24px;border-bottom:1px solid #94a3b840}.quality-tabs button{padding:12px 0;border-bottom:2px solid transparent}.quality-tabs .active{border-bottom-color:#059669}.quality-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:20px;padding:24px 0}.quality-grid label{display:flex;flex-direction:column;gap:8px;font-size:14px;min-width:0}.quality-grid .check{flex-direction:row;align-items:center}.quality-grid .wide{grid-column:1/-1}.quality-table{width:100%;min-width:600px;margin:16px 0}.quality-table th,.quality-table td{text-align:left;padding:10px 8px;border-bottom:1px solid #94a3b830}.quality-grid textarea{resize:vertical}@media(max-width:640px){.quality-grid{grid-template-columns:minmax(0,1fr)}.quality-stats{gap:6px}.quality-stats article{padding:8px}.quality-stats strong{font-size:20px}}
</style>
