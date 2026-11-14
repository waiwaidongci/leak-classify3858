import {useState} from 'react';import type {RawDataset,User,Build,BuildPreview,RawSample,RawBatch} from '../types/domain';import {rawApi} from '../api/raw';import {Table} from './common/Table';import {Notice} from './common/Notice';import {Badge} from './common/Badge';import {Empty} from './common/Empty';import {useAsync} from '../hooks/useAsync';import {dec,date,bytes} from '../utils/format';

/** UI chunk size: the browser slice is only about request size; the backend
 * enforces its own per-batch cap and its own 128 MB build memory budget. */
const UI_BATCH=500;

type SampleRow={pipe_id:string;sampled_at:string;flow:number;pressure:number;acoustic_energy:number;temperature:number;pipe_age:number;leak:0|1};

function parseSamples(text:string):SampleRow[]{
 const value=JSON.parse(text);
 const list=Array.isArray(value)?value:(value.samples||value);
 if(!Array.isArray(list))throw new Error('批次内容必须是采样数组，或含 samples 数组的对象');
 return list;
}

export function RawIntake({rawDatasets,user,reload}:{rawDatasets:RawDataset[];user:User;reload:()=>Promise<any>}){
 const [selected,setSelected]=useState<number|0>(0),[newName,setNewName]=useState(''),[raw,setRaw]=useState(''),[uploadKey,setUploadKey]=useState(''),[batch,setBatch]=useState(1),[samples,setSamples]=useState<RawSample[]>([]),[samplePipe,setSamplePipe]=useState(''),[batches,setBatches]=useState<RawBatch[]>([]);
 const {busy,error,message,act}=useAsync();
 const current=rawDatasets.find(d=>d.id===selected)||rawDatasets[0];
 const id=current?.id||0;
 async function upload(){
  const rows=parseSamples(raw);
  const chunks:SampleRow[][]=[];
  for(let i=0;i<rows.length;i+=UI_BATCH)chunks.push(rows.slice(i,i+UI_BATCH));
  let received=0,inserted=0,deduplicated=0,replayed=false,landed=0;
  for(let i=0;i<chunks.length;i++){
   try{
    const result=await rawApi.upload(id,{samples:chunks[i],upload_key:uploadKey?uploadKey+':'+batch+':'+i:undefined});
    received+=result.received;inserted+=result.inserted;deduplicated+=result.deduplicated;replayed=replayed||!!result.replayed;landed++;
   }catch(e){
    // Each request is its own atomic batch.  If a later slice fails, say so
    // plainly instead of letting the earlier slices look like a full upload.
    const landedNote=landed?`；前 ${landed} 个分片（${inserted} 条）已成批入库，本分片未写入`:'；本批未写入任何数据';
    throw new Error((e as Error).message+landedNote);
   }
  }
  await reload();
  return {received,inserted,deduplicated,chunks:chunks.length,replayed};
 }
 async function loadBatches(){
  setBatches(await rawApi.batches(id));
 }
 return <section className="raw-intake">
  <div className="section-title"><h2>原始采样分批上传</h2><small>BATCHES ARE ORDER-FREE</small></div>
  <p className="fine">先建原始采样集，再按任意顺序、任意分批上传。管段 + UTC 时刻为自然键：同值重传并入去重计数，异值整批拒绝。上传顺序与分批边界不影响生成结果。</p>
  <div className="raw-toolbar">
   <label>原始采样集<select value={id||''} onChange={e=>setSelected(+e.target.value)} disabled={!rawDatasets.length}>{rawDatasets.map(d=><option key={d.id} value={d.id}>{d.name} / {d.samples}条 / {d.pipes}管段</option>)}</select></label>
   {user.role!=='viewer'&&<label>新建采样集<input value={newName} onChange={e=>setNewName(e.target.value)} maxLength={100} placeholder="如 现场乱序采样 · 第一批"/></label>}
   {user.role!=='viewer'&&<button type="button" disabled={busy||!newName} onClick={()=>act(async()=>{const d=await rawApi.create({name:newName,filename:newName+'.json',description:'现场原始采样，分批上传'});setNewName('');setSelected(d.id);await reload()},'采样集已建立')}>建立采样集</button>}
  </div>
  {user.role==='viewer'?<p className="fine">观察员只读：可核对采样与生成结果，上传与生成需分析师权限。</p>:<form className="editor" onSubmit={e=>{e.preventDefault();act(async()=>{const r=await upload();setRaw('');return r},'批次已入库；核对去重数与窗口生成结果')}}>
   <h2>上传一个批次</h2>
   <p className="fine">批次内自带乱序也可以：每一行只按管段与时刻归位；同一批次内的重复行同样适用同值去重 / 异值拒绝。</p>
   <label>批次标识（可选，用于失败重试与重放核对）<input value={uploadKey} onChange={e=>setUploadKey(e.target.value)} maxLength={60} placeholder="如 run-0712"/></label>
   <label>批次序号（同一标识下递增；决定幂等键，不决定窗口结果）<input type="number" min="1" value={batch} onChange={e=>setBatch(Math.max(1,+e.target.value))}/></label>
   <label>采样JSON数组<textarea rows={6} value={raw} onChange={e=>setRaw(e.target.value)} required placeholder='[{"pipe_id":"CW-P01","sampled_at":"2026-07-11T23:00:00Z","flow":12.4,"pressure":512.1,"acoustic_energy":38.2,"temperature":18.6,"pipe_age":17,"leak":0},…]'/></label>
   <p>时间戳必须带时区（UTC 或偏移）；无时区记录拒绝。单请求最多 20000 条，界面按 {UI_BATCH} 条分片顺序提交同一批次。</p>
   <button className="primary" disabled={busy||!id}>校验并入库本批</button>
  </form>}
  <Notice error={error} message={message}/>
  {id>0&&<div className="raw-sample-tools">
   <button type="button" disabled={busy} onClick={()=>act(loadBatches,'')}>核对批次台账 ↗</button>
   {batches.length>0&&<small>最近 {batches.length} 批 / 合计入库 {batches.reduce((n,b)=>n+b.inserted,0)} 条 / 去重 {batches.reduce((n,b)=>n+b.deduplicated,0)} 条</small>}
   <label>按管段核对采样<input value={samplePipe} onChange={e=>setSamplePipe(e.target.value)} placeholder="留空则按管段排序" maxLength={60}/></label>
   <button type="button" disabled={busy} onClick={()=>act(async()=>{setSamples(await rawApi.samples(id,{limit:100,pipe_id:samplePipe||undefined}))},'')}>看前100条采样 ↗</button>
  </div>}
  {batches.length>0&&<><div className="section-title"><h2>批次台账</h2><small>重放不新增采样</small></div>
   <Table headers={['批次','上传标识','收到','入库','去重','校验和','时间']}>{batches.map(b=><tr key={b.id}><td>#{b.id}</td><td>{b.upload_key||'—'}{b.replayed&&<small>重放，未新增</small>}</td><td>{b.received}</td><td>{b.inserted}</td><td>{b.deduplicated}</td><td className="mono detail">{b.checksum.slice(0,16)}…</td><td>{date(b.created_at)}</td></tr>)}</Table></>}
  {samples.length>0&&<><div className="section-title"><h2>原始采样核对</h2><small>最多 {samples.length} 条 · 只读</small></div>
   <Table headers={['管段','UTC 时刻','流量 L/s','压力','声学能量','温度 °C','管龄 年','漏损']}>{samples.map((s,i)=><tr key={s.id??i}><td className="mono">{s.pipe_id}</td><td>{date(s.sampled_at)}</td><td>{dec(s.flow,2)}</td><td>{dec(s.pressure,2)}</td><td>{dec(s.acoustic_energy,2)}</td><td>{dec(s.temperature,2)}</td><td>{dec(s.pipe_age,1)}</td><td><span className={'label-dot label-'+s.leak}/>{s.leak?'漏损':'对照'}</td></tr>)}</Table></>}
  <div className="section-title"><h2>原始采样集</h2><small>已有 {rawDatasets.length} 个</small></div>
  {!rawDatasets.length?<Empty title="还没有原始采样集" help="建立采样集后即可分批上传现场乱序原始采样。"/>:<Table headers={['采样集','采样条数','管段','批次','首末采样（UTC）','生成记录']}>{rawDatasets.map(d=><tr key={d.id}><td><strong>{d.name}</strong><small>{d.filename||'现场原始采样'}</small></td><td>{d.samples}</td><td>{d.pipes}</td><td>{d.batches}</td><td className="mono">{d.first_sample?date(d.first_sample):'—'}<small>{d.last_sample?date(d.last_sample):'—'}</small></td><td>{d.builds}</td></tr>)}</Table>}
 </section>
}

export function BuildDesk({rawDatasets,builds,user,reload}:{rawDatasets:RawDataset[];builds:Build[];user:User;reload:()=>Promise<any>}){
 const [rawId,setRawId]=useState<number|0>(0),[name,setName]=useState(''),[open,setOpen]=useState<number>(0),[preview,setPreview]=useState<BuildPreview|null>(null),[offset,setOffset]=useState(0);
 const {busy,error,message,act}=useAsync();
 const target=rawDatasets.find(d=>d.id===rawId)||rawDatasets[rawDatasets.length-1];
 async function loadWindows(id:number,nextOffset=0){setOpen(id);setOffset(nextOffset);setPreview(await rawApi.buildWindows(id,nextOffset,50))}
 return <section className="build-desk">
  <div className="section-title"><h2>生成五分钟滑动窗口</h2><small>UTC WHOLE-MINUTE START / LEFT-CLOSED RIGHT-OPEN</small></div>
  <p className="fine">起点为 UTC 整分钟，窗口为 [起点, 起点+5分钟)：第一个、最后一个采样之间每个整分钟都试一次；无采样的空窗口跳过。流量、声能、温度取均值；压力降=首压−尾压；流量变异=总体标准差/均值；管龄取最新值；任一采样漏损则窗口标漏损。流量均值为零的窗口整批拒绝。生成完成前窗口不进入训练；失败重试不留半批。</p>
  {user.role!=='viewer'&&<form className="editor" onSubmit={e=>{e.preventDefault();act(async()=>{await rawApi.build(target.id,{name:name||target.name+' · 五分钟窗口',memory_limit_bytes:128*1024*1024});setName('');await reload()},'窗口生成已入队')}}>
   <h2>从原始采样集生成</h2>
   <label>原始采样集<select value={target?.id||''} onChange={e=>setRawId(+e.target.value)} disabled={!rawDatasets.length}>{rawDatasets.map(d=><option key={d.id} value={d.id}>{d.name} / {d.samples}条</option>)}</select></label>
   <label>生成后的窗口数据集名称<input value={name} onChange={e=>setName(e.target.value)} maxLength={100} placeholder={target?target.name+' · 五分钟窗口':''}/></label>
   <p>额外内存上限固定 128 MB；后端按最坏峰值折算每批采样行数，按（管段,UTC）键序分块读取。仅有完整生成成功的窗口才会作为可训练数据集出现。</p>
   <button className="primary" disabled={busy||!target||!target.samples}>生成滑动窗口 →</button>
  </form>}
  <Notice error={error} message={message}/>
  <div className="section-title"><h2>生成记录</h2><small>每3秒刷新</small></div>
  {!builds.length?<Empty title="尚无生成记录" help="先上传原始采样，再生成五分钟窗口。"/>:<Table headers={['生成','原始采样集','状态','窗口 / 漏损','峰值内存估算','说明','']}>{builds.map(b=><tr key={b.id} className={open===b.id?'selected':''}><td>#{b.id}<small>{b.name}</small></td><td>#{b.raw_dataset_id}<small>{b.sample_count}条采样 / {b.pipe_count}管段</small></td><td><Badge status={b.status}/></td><td>{b.window_count}<small>{b.summary?.positive??0} 漏损 / {b.summary?.partial_windows??0} 凑不齐五分钟</small></td><td>{bytes(b.peak_estimated_bytes)}<small>上限 {bytes(b.memory_limit_bytes)} · 每批 {b.chunk_rows} 行</small></td><td className="description-cell">{b.error||(b.summary?.training_ready?'达到训练最小条件':(b.status==='succeeded'?'可作核对；样本量不足以训练':'—'))}</td><td><button className="text-button" disabled={busy} onClick={()=>act(async()=>{await loadWindows(b.id,0);},'')}>核对窗口 ↗</button></td></tr>)}</Table>}
  {preview&&<div className="build-preview"><div className="section-title"><h2>生成窗口核对 · #{preview.build.id}</h2><small>共 {preview.total} 窗口 · 第 {preview.offset+1}–{Math.min(preview.offset+preview.limit,preview.total)} 条</small></div>
   <p className="fine">{preview.build.summary?.window_rule||''}；跳过空窗 {preview.build.summary?.skipped_starts??0} 个起点。{preview.build.status==='succeeded'?'已封存为数据集 #'+(preview.build.window_dataset_id||'—'):'正在生成，暂存窗口仅供核对，不进入训练。'}</p>
   <Table headers={['窗口自然键','管段','UTC 起点','流量均值','压力降','声能均值','流量变异','管龄(最新)','温度均值','标签']}>{preview.windows.map(w=><tr key={w.reference}><td className="mono">{w.reference}</td><td>{w.pipe_id}</td><td>{w.window_start.slice(0,16).replace('T',' ')}</td><td>{dec(w.features.night_flow,2)}</td><td>{dec(w.features.pressure_drop,2)}</td><td>{dec(w.features.acoustic_energy,2)}</td><td>{dec(w.features.flow_variation,3)}</td><td>{dec(w.features.pipe_age,1)}</td><td>{dec(w.features.ambient_temp,2)}</td><td><span className={'label-dot label-'+w.label}/>{w.label?'漏损':'对照'}</td></tr>)}</Table>
   <div className="preview-pager"><button disabled={busy||offset<=0} onClick={()=>act(async()=>{await loadWindows(preview.build.id,Math.max(0,offset-50))},'')}>← 上一页</button><span className="mono">{preview.total?Math.min(preview.total,offset+1):0}–{Math.min(preview.total,offset+preview.limit)} / {preview.total}</span><button disabled={busy||offset+preview.limit>=preview.total} onClick={()=>act(async()=>{await loadWindows(preview.build.id,offset+preview.limit)},'')}>下一页 →</button></div>
  </div>}
 </section>
}
