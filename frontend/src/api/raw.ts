import {request,post} from './client';import type {RawDataset,RawSample,RawBatch,Build,BuildPreview} from '../types/domain';
export const rawApi={
 all:()=>request<RawDataset[]>('/raw-datasets'),
 create:(data:any)=>post('/raw-datasets',data),
 samples:(id:number,params:{limit?:number;pipe_id?:string;start?:string;end?:string}={})=>{const q=new URLSearchParams();Object.entries(params).forEach(([k,v])=>v!==undefined&&v!==''&&q.set(k,String(v)));const s=q.toString();return request<RawSample[]>('/raw-datasets/'+id+'/samples'+(s?'?'+s:''))},
 batches:(id:number)=>request<RawBatch[]>('/raw-datasets/'+id+'/batches'),
 upload:(id:number,data:any)=>post('/raw-datasets/'+id+'/batches',data),
 build:(id:number,data:any)=>post('/raw-datasets/'+id+'/builds',data),
 builds:(rawId?:number)=>request<Build[]>('/builds'+(rawId?'?raw_dataset_id='+rawId:'')),
 buildDetail:(id:number)=>request<Build>('/builds/'+id),
 buildWindows:(id:number,offset=0,limit=50)=>request<BuildPreview>('/builds/'+id+'/windows?offset='+offset+'&limit='+limit),
};
