<script setup lang="ts">
import { computed,ref,watch } from 'vue'
import Icon from './Icon.vue'
import type { Task } from '../types'
const props=defineProps<{tasks:Task[]; now:number;t:(key:string,values?:Record<string,string|number>)=>string;active:(task:Task)=>boolean;rate:(value?:number|null)=>string;duration:(value?:number|null)=>string}>()
const history=ref<{at:number;captured:number;delivered:number;complete:boolean}[]>([])
const metrics=computed(()=>{
  const active=props.tasks.filter(props.active),sampled=active.filter(task=>Number.isFinite(task.health?.capturedPerSecond)&&Number.isFinite(task.health?.deliveredPerSecond)),delayed=active.filter(task=>Number.isFinite(task.health?.lastDeliveryDelayMillis))
  return {active:active.length,sampled:sampled.length,delayed:delayed.length,captured:sampled.length?sampled.reduce((n,t)=>n+t.health!.capturedPerSecond!,0):null,delivered:sampled.length?sampled.reduce((n,t)=>n+t.health!.deliveredPerSecond!,0):null,delay:delayed.length?delayed.reduce((max,t)=>Math.max(max,t.health!.lastDeliveryDelayMillis!),0):null}
})
watch(()=>props.tasks,()=>{const m=metrics.value,at=props.now;if(m.captured!=null&&m.delivered!=null)history.value=[...history.value.filter(p=>at-p.at<=600000&&at-p.at>=1000),{at,captured:m.captured,delivered:m.delivered,complete:m.sampled===m.active}];else if(!m.active)history.value=[]},{immediate:true})
const max=computed(()=>Math.max(1,...history.value.flatMap(p=>[p.captured,p.delivered])))
function path(key:'captured'|'delivered'){const last=history.value.at(-1)?.at||0;return history.value.map((p,i)=>(i===0||!p.complete||!history.value[i-1]!.complete||p.at-history.value[i-1]!.at>25000?'M':'L')+(40+Math.max(0,1-(last-p.at)/600000)*550).toFixed(1)+' '+(110-p[key]/max.value*94).toFixed(1)).join(' ')}
</script>
<template><section class="panel live-monitor"><div class="panel-header"><h2><Icon name="bolt" :size="17"/>{{ t('monitor') }}</h2><span class="subtle-label">{{ t('rateCoverage',{n:metrics.sampled,total:metrics.active}) }}</span></div>
<div class="monitor-values"><div><span>{{ t('captureRate') }}</span><strong>{{ rate(metrics.captured) }}</strong></div><div><span>{{ t('deliveryRate') }}</span><strong>{{ rate(metrics.delivered) }}</strong></div><div :title="t('delayHint')"><span>{{ t('deliveryDelay') }}</span><strong>{{ duration(metrics.delay) }}</strong><small>{{ t('delayCoverage',{n:metrics.delayed}) }}</small></div></div>
<div v-if="history.length>1" class="monitor-graph"><svg viewBox="0 0 610 135" role="img" :aria-label="t('monitorHistory')"><g v-for="n in [0,1,2]" :key="n"><line x1="40" x2="590" :y1="16+n*47" :y2="16+n*47" stroke="#dce5eb" stroke-dasharray="3 4"/><text x="33" :y="20+n*47" text-anchor="end" fill="currentColor" font-size="10">{{ rate(max*(1-n/2)) }}</text></g><path :d="path('captured')" fill="none" stroke="#3576a9" stroke-width="2"/><path :d="path('delivered')" fill="none" stroke="#07865d" stroke-width="2"/><text x="40" y="128" font-size="10" fill="currentColor">−10 min</text><text x="590" y="128" text-anchor="end" font-size="10" fill="currentColor">0</text></svg><div class="monitor-legend"><span><i/>{{ t('captureRate') }}</span><span><i class="delivery-line"/>{{ t('deliveryRate') }}</span><small>{{ t('monitorHistory') }}</small></div></div>
<p class="monitor-note">{{ t('rateHint') }} {{ metrics.sampled?'':t('noSamples') }}</p></section></template>
