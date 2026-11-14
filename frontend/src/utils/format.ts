export const pct=(n:number|undefined)=>((n??0)*100).toFixed(1)+'%';export const dec=(n:number|undefined,d=3)=>(n??0).toFixed(d);export const date=(s:string)=>new Date(s).toLocaleString('zh-CN',{hour12:false});
export const bytes=(n:number|undefined)=>{if(!n)return '—';const m=n/1024/1024;return m>=1?m.toFixed(1)+' MB':(n/1024).toFixed(0)+' KB';};
