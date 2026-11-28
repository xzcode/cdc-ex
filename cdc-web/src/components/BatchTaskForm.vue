<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import Icon from './Icon.vue'
import type { Api, BatchResult, Connection, ConnectorCaps } from '../types'
const props = defineProps<{ connections:Connection[]; connectors:Record<string,ConnectorCaps>; api:Api; t:(key:string,values?:Record<string,string|number>)=>string; connectorName:(type:string)=>string; modeLabel:(mode:string,type?:string)=>string }>()
const emit=defineEmits<{ complete:[results:BatchResult[],group:string]; busy:[value:boolean]; cancel:[] }>()
const draft=reactive({ group:'', prefix:'', source:'', sink:'', mode:'initial', query:'', firstId:'' })
const rows=ref<{id:number;checked:boolean;source:string;sink:string;client:string;publication:string}[]>([])
const sourceNames=ref<string[]>([]),sinkNames=ref<string[]>([]),partial=ref(false),error=ref(''),busy=ref(false),results=ref<BatchResult[]>([])
let sequence=0
const src=computed(()=>props.connections.find(c=>c.id===draft.source))
const dst=computed(()=>props.connections.find(c=>c.id===draft.sink))
const clientKey=computed(()=>({MYSQL:'replicationServerId',MARIADB:'replicationServerId',POSTGRESQL:'slot',KAFKA:'groupId'} as Record<string,string>)[src.value?.type||'']||'')
const modes=computed(()=>{const c=src.value;const options=(props.connectors[c?.type||'']?.sourceModes||['initial','no_data']).filter(x=>x!=='resume');return c?.type==='CLICKHOUSE'&&options.includes(c.options.mode||'')?[c.options.mode!]:options})
const checked=computed(()=>rows.value.filter(row=>row.checked))
const failed=computed(()=>results.value.filter(r=>r.outcome==='FAILED'))
function reset(){rows.value=[];sourceNames.value=[];sinkNames.value=[];partial.value=false;results.value=[];sequence=0}
watch(()=>draft.source,()=>{reset();draft.mode=modes.value.includes('initial')?'initial':modes.value[0]||'initial'})
watch(()=>draft.sink,reset)
function add(source='',sink='',selected=true){const id=++sequence,n=Number(draft.firstId)+id-1;rows.value.push({id,checked:selected,source,sink,client:clientKey.value==='replicationServerId'&&draft.firstId&&Number.isSafeInteger(n)&&n>0&&n<=4294967295?String(n):'',publication:''})}
function fillIds(){const n=Number(draft.firstId);if(!Number.isSafeInteger(n)||n<1||n+sequence-1>4294967295){error.value=props.t('batchRowInvalid');return}for(const row of rows.value)row.client=String(n+row.id-1);error.value=''}
async function run(fn:()=>Promise<void>){if(busy.value)return;busy.value=true;emit('busy',true);error.value='';try{await fn()}catch(e){error.value=(e as Error).message}finally{busy.value=false;emit('busy',false)}}
async function discover(){await run(async()=>{type Resources={resources:string[];truncated:boolean};const query='?query='+encodeURIComponent(draft.query)+'&limit=500'
  const [a,b]=await Promise.all([props.api<Resources>('/connections/'+encodeURIComponent(draft.source)+'/resources'+query),props.api<Resources>('/connections/'+encodeURIComponent(draft.sink)+'/resources'+query)])
  sourceNames.value=a.resources;sinkNames.value=b.resources;partial.value=a.truncated||b.truncated
  const existing=new Set(rows.value.map(row=>row.source));for(const name of a.resources)if(!existing.has(name))add(name,b.resources.includes(name)?name:'',false)
})}
function taskId(row:{id:number}){return draft.prefix.trim()+'-'+row.id}
async function create(){await run(async()=>{
  if(!src.value||!dst.value)throw new Error(props.t('connectionNeeded'))
  if(!/^[A-Za-z0-9_-]{1,48}$/.test(draft.prefix.trim()))throw new Error(props.t('batchPrefixInvalid'))
  if(!checked.value.length)throw new Error(props.t('batchNoSelection'))
  const tasks=checked.value.map(row=>{
    const source=row.source.trim(),sink=row.sink.trim(),client=row.client.trim(),publication=row.publication.trim()
    if(!source||!sink||(clientKey.value&&!client)||(clientKey.value==='slot'&&!publication))throw new Error(props.t('batchRowInvalid'))
    const sourceOverrides:Record<string,string>={[src.value!.type==='KAFKA'?'topic':'table']:source}
    if(clientKey.value)sourceOverrides[clientKey.value]=client
    if(clientKey.value==='slot')sourceOverrides.publication=publication
    return {id:taskId(row),name:(source+' → '+sink).slice(0,128),group:draft.group,sourceConnectionId:draft.source,sinkConnectionId:draft.sink,snapshotMode:draft.mode,sourceOverrides,sinkOverrides:{[dst.value!.type==='KAFKA'?'topic':'table']:sink},mapping:{rename:{},conversions:{},exclude:[]}}
  })
  results.value=await props.api<BatchResult[]>('/tasks/batch','POST',{tasks},120000)
  if(!failed.value.length)emit('complete',results.value,draft.group.trim())
})}
</script>
<template>
  <div class="modal-body batch-body">
    <p class="modal-description">{{ t('batchHint') }}</p>
    <div v-if="error" class="notice danger" role="alert"><Icon name="warning"/>{{ error }}</div>
    <fieldset :disabled="busy" class="batch-fieldset">
      <div class="form-grid">
        <label class="field"><span>{{ t('taskGroup') }}</span><input v-model="draft.group" maxlength="80"/></label>
        <label class="field"><span>{{ t('idPrefix') }}</span><input v-model="draft.prefix" maxlength="48" spellcheck="false"/></label>
        <label class="field"><span>{{ t('source') }}</span><select v-model="draft.source" :aria-label="t('source')"><option value="">{{ t('selectConnection') }}</option><option v-for="c in connections" :key="c.id" :value="c.id">{{ c.name }} · {{ connectorName(c.type) }}</option></select></label>
        <label class="field"><span>{{ t('destination') }}</span><select v-model="draft.sink" :aria-label="t('destination')"><option value="">{{ t('selectConnection') }}</option><option v-for="c in connections" :key="c.id" :value="c.id">{{ c.name }} · {{ connectorName(c.type) }}</option></select></label>
        <label class="field"><span>{{ t('snapshotMode') }}</span><select v-model="draft.mode" :aria-label="t('snapshotMode')"><option v-for="mode in modes" :key="mode" :value="mode">{{ modeLabel(mode,src?.type) }}</option></select></label>
        <label v-if="clientKey==='replicationServerId'" class="field"><span>{{ t('replicationServerId') }}</span><div class="inline-field"><input v-model="draft.firstId" inputmode="numeric" placeholder="55000" :aria-label="t('replicationServerId')"/><button class="button secondary small" @click="fillIds">{{ t('fillIds') }}</button></div></label>
      </div>
      <p v-if="clientKey" class="field-hint">{{ t('batchIdentityHint') }}</p>
      <div class="section-divider"><span>{{ t('sourceResource') }} → {{ t('targetResource') }}</span></div>
      <div class="batch-discovery"><label class="search-box"><Icon name="search" :size="16"/><input v-model="draft.query" :placeholder="t('resourceQuery')" :aria-label="t('resourceQuery')" @keydown.enter.prevent="discover"/></label><button class="button secondary" :disabled="!src||!dst" @click="discover"><Icon name="database" :size="16"/>{{ t('discoverResources') }}</button><button class="button secondary" :disabled="!src||!dst" @click="add()"><Icon name="plus" :size="16"/>{{ t('addPair') }}</button></div>
      <p class="field-hint">{{ t('discoveryHint') }}</p><div v-if="partial" class="notice warning"><Icon name="info"/>{{ t('discoveryTruncated') }}</div>
      <div v-if="!rows.length" class="mapping-empty">{{ t('noBatchRows') }}</div>
      <div v-else class="table-scroll batch-table"><table><thead><tr><th><input type="checkbox" :checked="checked.length===rows.length" :aria-label="t('selectAll')" @change="rows.forEach(row=>row.checked=($event.target as HTMLInputElement).checked)"/></th><th>{{ t('id') }}</th><th>{{ t('sourceResource') }}</th><th>{{ t('targetResource') }}</th><th v-if="clientKey">{{ t(clientKey) }}</th><th v-if="clientKey==='slot'">{{ t('publication') }}</th><th></th></tr></thead><tbody>
        <tr v-for="row in rows" :key="row.id"><td><input v-model="row.checked" type="checkbox" :aria-label="t('selectTask',{name:row.source||String(row.id)})"/></td><td class="mono batch-id">{{ taskId(row) }}</td><td><input v-model="row.source" :aria-label="t('sourceResource')+' '+row.id" list="batch-source-options" spellcheck="false"/></td><td><input v-model="row.sink" :aria-label="t('targetResource')+' '+row.id" list="batch-sink-options" spellcheck="false"/></td><td v-if="clientKey"><input v-model="row.client" :aria-label="t(clientKey)+' '+row.id" spellcheck="false"/></td><td v-if="clientKey==='slot'"><input v-model="row.publication" :aria-label="t('publication')+' '+row.id" spellcheck="false"/></td><td><button class="icon-button" :aria-label="t('remove')" @click="rows=rows.filter(item=>item.id!==row.id)"><Icon name="close" :size="16"/></button></td></tr>
      </tbody></table></div>
      <datalist id="batch-source-options"><option v-for="name in sourceNames" :key="name" :value="name"/></datalist><datalist id="batch-sink-options"><option v-for="name in sinkNames" :key="name" :value="name"/></datalist>
    </fieldset>
    <p class="field-hint">{{ t('batchModeHint') }}</p><div v-if="failed.length" class="notice warning"><Icon name="info"/><div>{{ t('batchSummary',{ok:results.length-failed.length,failed:failed.length}) }}<p v-for="row in failed" :key="row.id">{{ row.id }}: {{ row.code }}</p></div></div>
  </div>
  <footer class="modal-footer"><span class="subtle-label">{{ t('selectedTasks',{n:checked.length}) }}</span><button class="button secondary" :disabled="busy" @click="emit('cancel')">{{ t('cancel') }}</button><button class="button primary" :disabled="busy||!checked.length" @click="create">{{ t('batchCreate') }}</button></footer>
</template>
