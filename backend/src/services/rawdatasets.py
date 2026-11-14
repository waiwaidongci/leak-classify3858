"""Raw field samples: batch upload, deduplication and window generation.

Upload and generation deliberately keep three ideas apart:

* A *sample* is a single field reading, keyed by (pipe, UTC instant).
* A *batch* is one request.  It commits as a unit: a rejected batch leaves
  nothing behind, so retrying a failed batch cannot leave half a split.
* A *build* turns samples into windows.  Generated windows are staged and are
  invisible to training until the build succeeds as a whole; a failed build
  drops every staged row.

Generation reads the samples back in (pipe, instant) order from the database,
so upload order and batch boundaries cannot change the window set.  Re-sending
an identical reading for the same key is counted as a duplicate; sending
different values for the same key rejects the batch.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from fastapi import HTTPException
from sqlalchemy import select,func,tuple_,delete,insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.constants.enums import BuildStatus
from src.models import RawDataset,RawSample,RawUploadBatch,WindowBuild,GeneratedWindow,WindowDataset,SensorWindow
from src.utils.serialize import row
from .audit import record
from . import windowing
from .windowing import WindowError

MAX_EXTRA_MEMORY_BYTES=128*1024*1024
DEFAULT_MEMORY_LIMIT_BYTES=MAX_EXTRA_MEMORY_BYTES
MAX_BATCH_ROWS=20000


class NameClash(WindowError):
    """A completed build would collide with an existing dataset name."""
# Conservative per-sample footprint of the in-process row (dict, datetime, str
# and five floats).  Chunk size is derived from it so the estimated peak of
# chunk + window buffer stays inside the caller's limit.
ROW_BYTES=1024
# Worst-case simultaneous copies of one in-memory chunk (see run_build).
FACTOR=6
_SAMPLE_COLUMNS=(RawSample.id,RawSample.pipe_id,RawSample.sampled_at,RawSample.flow,RawSample.pressure,RawSample.acoustic_energy,RawSample.temperature,RawSample.pipe_age,RawSample.leak)


def _instant_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def _canonical(samples: list[dict]) -> str:
    ordered=sorted(samples,key=lambda s:(s['pipe_id'],s['sampled_at']))
    payload=[{'pipe_id':s['pipe_id'],'sampled_at':_instant_text(s['sampled_at']),**{k:float(s[k]) for k in ('flow','pressure','acoustic_energy','temperature','pipe_age')},'leak':int(s['leak'])} for s in ordered]
    return hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def _as_utc(value:datetime) -> datetime:
 # Some drivers (notably SQLite in tests) hand back naive timestamps even for
 # DateTime(timezone=True); every stored instant is UTC by construction.
 return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _sample_dict(values) -> dict:
    return {'id':values[0],'pipe_id':values[1],'sampled_at':values[2],'flow':values[3],'pressure':values[4],'acoustic_energy':values[5],'temperature':values[6],'pipe_age':values[7],'leak':values[8]}


def _existing_pairs(db:Session,raw_id:int,pairs:list[tuple]) -> dict:
    """Fetch stored samples for the batch's natural keys, in bounded IN chunks."""
    found={}
    unique=list(dict.fromkeys(pairs))
    for i in range(0,len(unique),500):
        chunk=unique[i:i+500]
        q=select(*_SAMPLE_COLUMNS).where(RawSample.raw_dataset_id==raw_id,tuple_(RawSample.pipe_id,RawSample.sampled_at).in_(chunk))
        for values in db.execute(q):
            sample=_sample_dict(values)
            found[(sample['pipe_id'],sample['sampled_at'])]=sample
    return found


def _pair_keys(found:dict) -> dict:
    return {(k[0],_as_utc(k[1])):v for k,v in found.items()}


def create_raw_dataset(db:Session,data,user):
    if db.scalar(select(RawDataset).where(RawDataset.name==data.name)):raise HTTPException(409,'原始采样集名称已存在')
    obj=RawDataset(name=data.name,filename=data.filename,description=data.description);db.add(obj);db.commit()
    record(db,user.email,'rawdataset.created',{'raw_dataset_id':obj.id});return row(obj)


def list_raw_datasets(db:Session):
    out=[]
    for d in db.scalars(select(RawDataset).order_by(RawDataset.id)):
        count=db.scalar(select(func.count()).select_from(RawSample).where(RawSample.raw_dataset_id==d.id)) or 0
        pipes=db.scalar(select(func.count(func.distinct(RawSample.pipe_id))).where(RawSample.raw_dataset_id==d.id)) or 0
        first=db.scalar(select(func.min(RawSample.sampled_at)).where(RawSample.raw_dataset_id==d.id))
        last=db.scalar(select(func.max(RawSample.sampled_at)).where(RawSample.raw_dataset_id==d.id))
        first=_as_utc(first) if first else None;last=_as_utc(last) if last else None
        batches=db.scalar(select(func.count()).select_from(RawUploadBatch).where(RawUploadBatch.raw_dataset_id==d.id)) or 0
        builds=db.scalar(select(func.count()).select_from(WindowBuild).where(WindowBuild.raw_dataset_id==d.id)) or 0
        out.append(dict(row(d),samples=count,pipes=pipes,batches=batches,builds=builds,first_sample=first.isoformat() if first else None,last_sample=last.isoformat() if last else None))
    return out


def get_raw_dataset(db:Session,id:int):
    obj=db.get(RawDataset,id)
    if not obj:raise HTTPException(404,'原始采样集不存在')
    return obj


def preview_samples(db:Session,id:int,limit:int=100,pipe_id:str|None=None,start:datetime|None=None,end:datetime|None=None):
    """Raw rows for inspection; `start` and `end` are inclusive bounds."""
    get_raw_dataset(db,id)
    q=select(*_SAMPLE_COLUMNS).where(RawSample.raw_dataset_id==id).order_by(RawSample.pipe_id,RawSample.sampled_at)
    if pipe_id:q=q.where(RawSample.pipe_id==pipe_id)
    if start is not None:q=q.where(RawSample.sampled_at>=windowing.check_instant(start))
    if end is not None:q=q.where(RawSample.sampled_at<=windowing.check_instant(end))
    return [_sample_dict(v) for v in db.execute(q.limit(max(1,min(limit,500))))]


def upload_batch(db:Session,raw_id:int,data,user):
    """Insert one batch atomically; duplicates collapse, conflicts reject.

    Nothing is written unless every sample passes, so a failed attempt followed
    by a retry never leaves half of a batch in the table.
    """
    get_raw_dataset(db,raw_id)
    received=len(data.samples)
    checksum=_canonical([s.model_dump() for s in data.samples])
    if data.upload_key:
        previous=db.scalar(select(RawUploadBatch).where(RawUploadBatch.raw_dataset_id==raw_id,RawUploadBatch.upload_key==data.upload_key))
        if previous:
            if previous.checksum!=checksum:raise HTTPException(409,'同一上传标识的内容不同；请换标识或核对重传数据')
            return dict(row(previous),replayed=True)
    # Collapse repeats inside the batch first.  Same key, same reading -> one row.
    unique={}
    for sample in data.samples:
        key=(sample.pipe_id,sample.sampled_at)
        if key in unique:
            if not windowing.same_sample(unique[key],sample.model_dump()):raise HTTPException(422,f'同管段同刻出现不同数值：{sample.pipe_id} {_instant_text(sample.sampled_at)}')
            continue
        unique[key]=sample.model_dump()
    # Check every key against what is already stored before writing anything:
    # a rejected batch leaves not one row behind, so a retry starts clean.
    pairs=list(unique)
    existing=_pair_keys(_existing_pairs(db,raw_id,pairs))
    for key,sample in unique.items():
        stored=existing.get(key)
        if stored and not windowing.same_sample(stored,sample):raise HTTPException(422,f'同管段同刻已存在不同数值：{key[0]} {_instant_text(key[1])}')
    to_insert=[s for key,s in unique.items() if key not in existing]
    if to_insert:
        db.commit()  # end the read transaction before retry-sensitive insert
        try:
            db.execute(insert(RawSample),[{'raw_dataset_id':raw_id,'pipe_id':s['pipe_id'],'sampled_at':s['sampled_at'],'flow':s['flow'],'pressure':s['pressure'],'acoustic_energy':s['acoustic_energy'],'temperature':s['temperature'],'pipe_age':s['pipe_age'],'leak':s['leak']} for s in to_insert])
            batch=RawUploadBatch(raw_dataset_id=raw_id,upload_key=data.upload_key,checksum=checksum,received=received,inserted=len(to_insert),deduplicated=received-len(to_insert))
            db.add(batch);db.commit()
        except IntegrityError:
            db.rollback();raise HTTPException(409,'同管段同刻已被并发写入不同记录，请重试该批')
    else:
        batch=RawUploadBatch(raw_dataset_id=raw_id,upload_key=data.upload_key,checksum=checksum,received=received,inserted=0,deduplicated=received)
        db.add(batch);db.commit()
    record(db,user.email,'raw.batch.uploaded',{'raw_dataset_id':raw_id,'received':received,'inserted':len(to_insert),'deduplicated':received-len(to_insert),'upload_key':data.upload_key})
    return dict(row(batch),replayed=False)


def list_batches(db:Session,raw_id:int):
    get_raw_dataset(db,raw_id)
    return [row(x) for x in db.scalars(select(RawUploadBatch).where(RawUploadBatch.raw_dataset_id==raw_id).order_by(RawUploadBatch.id))]


def _iter_chunks(db:Session,raw_id:int,chunk_rows:int):
    """Keyset-paginate samples in (pipe, instant) order.

    Using the natural-key tuple as the cursor means the generator never needs
    to hold a result set open, and it sees rows in exactly the order the
    window sweep assumes regardless of upload order or batching.  Every
    chunk is released as soon as the sweep has moved past it.
    """
    cursor=None
    while True:
        q=select(*_SAMPLE_COLUMNS).where(RawSample.raw_dataset_id==raw_id).order_by(RawSample.pipe_id,RawSample.sampled_at,RawSample.id)
        if cursor is not None:q=q.where(tuple_(RawSample.pipe_id,RawSample.sampled_at,RawSample.id)>cursor)
        values=db.execute(q.limit(chunk_rows)).all()
        if not values:return
        chunk=[_sample_dict(v) for v in values]
        cursor=(chunk[-1]['pipe_id'],chunk[-1]['sampled_at'],chunk[-1]['id'])
        del values
        for sample in chunk:yield sample
        del chunk


def _staged_rows(build_id:int,offset:int,windows:list[dict]):
    return [{'build_id':build_id,'seq':offset+i,'pipe_id':w['pipe_id'],'window_start':w['window_start'],'reference':w['reference'],'features':w['features'],'label':w['label'],'samples':w['samples'],'minute_slots':w['minute_slots']} for i,w in enumerate(windows)]


def count_samples(db:Session,raw_id:int) -> int:
    return db.scalar(select(func.count()).select_from(RawSample).where(RawSample.raw_dataset_id==raw_id)) or 0


def run_build(db:Session,build:WindowBuild):
    """Generate windows for `build` and materialise the dataset on success.

    Caller (the worker) owns the status transitions around this function; on
    any exception the staged rows are dropped here, and the caller marks the
    build failed.  Staged windows are never visible to training.
    """
    db.execute(delete(GeneratedWindow).where(GeneratedWindow.build_id==build.id))
    db.commit()
    limit=int(build.memory_limit_bytes or DEFAULT_MEMORY_LIMIT_BYTES)
    # A chunk is live in several places at the peak: the fetched sample rows,
    # the sweep's window buffer, the staging batch being written, and (at
    # copy-out) both the staged rows and the dataset rows, plus DBAPI buffers.
    # The factor below is deliberately conservative; measured RSS growth stays
    # under the limit (see tests/test_rawdatasets.py).
    chunk_rows=max(1,int(limit*0.9)//(FACTOR*ROW_BYTES))
    build.chunk_rows=chunk_rows
    build.peak_estimated_bytes=FACTOR*chunk_rows*ROW_BYTES
    build.status=BuildStatus.RUNNING
    db.commit()
    sample_count=count_samples(db,build.raw_dataset_id)
    build.sample_count=sample_count
    db.commit()
    if sample_count==0:
        raise WindowError('原始采样集为空，无法生成窗口')
    pipes=set();windows=0;positive=0;skipped=0;partial=0;batched=[];first_start=None;last_start=None
    streams=windowing.iter_windows_stream(_iter_chunks(db,build.raw_dataset_id,chunk_rows))
    offset=0
    for pipe_id,start,window,gap_starts,is_partial in streams:
        skipped+=gap_starts
        pipes.add(pipe_id)
        windows+=1
        positive+=window['label']
        partial+=1 if is_partial else 0
        if first_start is None or start<first_start:first_start=start
        if last_start is None or start>last_start:last_start=start
        batched.append({'pipe_id':pipe_id,'window_start':start,'reference':windowing.window_reference(pipe_id,start),'features':window['features'],'label':window['label'],'samples':window['samples'],'minute_slots':window['minute_slots']})
        if len(batched)>=chunk_rows:
            db.execute(insert(GeneratedWindow),_staged_rows(build.id,offset,batched));db.commit();offset+=len(batched);batched=[]
    if batched:
        db.execute(insert(GeneratedWindow),_staged_rows(build.id,offset,batched));db.commit()
    build.pipe_count=len(pipes);build.window_count=windows
    if windows==0:raise WindowError('未生成任何完整五分钟窗口；请检查采样是否覆盖连续的五个整分钟')
    # Check the name before writing anything durable: a clash must fail the
    # build without touching staging or the dataset table.
    if db.scalar(select(WindowDataset).where(WindowDataset.name==build.name)):
        raise NameClash('同名窗口数据集已存在，请换一个生成名称')
    # Locked summary: same totals no matter which batch or upload order arrived.
    build.summary={'samples':sample_count,'pipes':len(pipes),'windows':windows,'positive':positive,'partial_windows':partial,'negative':windows-positive,'prevalence':positive/windows if windows else 0.,'skipped_starts':skipped,'first_window_start':first_start.isoformat() if first_start else None,'last_window_start':last_start.isoformat() if last_start else None,'window_minutes':windowing.WINDOW_MINUTES,'window_rule':'UTC whole-minute start S between first and last sample, [S,S+5min), empty windows skipped','dedup':'same pipe+instant identical readings collapse; conflicts reject the batch','aggregation':{'night_flow':'mean','pressure_drop':'first pressure - last pressure','acoustic_energy':'mean','flow_variation':'population std / mean','pipe_age':'latest sample','ambient_temp':'mean','label':'1 if any sample leaks'},'chunk_rows':chunk_rows,'row_bytes_estimate':ROW_BYTES,'peak_estimated_bytes':FACTOR*chunk_rows*ROW_BYTES,'memory_limit_bytes':limit,'training_ready':windows>=100 and min(positive,windows-positive)>=20}
    # A completed build becomes one dataset; nothing partial ever reaches the
    # dataset listing or the training selector.  The dataset row, every copied
    # window and the success status are written in ONE transaction: a crash or
    # error mid-copy rolls all of it back, so there is never a dataset with
    # only some of its windows.
    dataset=WindowDataset(name=build.name,filename=f'raw:{build.raw_dataset_id}',description=f"由原始采样集 #{build.raw_dataset_id} 生成：{windowing.WINDOW_MINUTES} 分钟滑动窗口，UTC 整分钟起点。")
    db.add(dataset);db.flush()
    # Copy staging to the dataset in bounded chunks, ordering by staging seq
    # so each chunk corresponds to a contiguous seq range.  No commit inside
    # the loop: pending statements are flushed as they execute, so process
    # memory stays bounded while the transaction stays atomic.
    for first in range(0,windows,chunk_rows):
        staged=db.scalars(select(GeneratedWindow).where(GeneratedWindow.build_id==build.id,GeneratedWindow.seq>=first,GeneratedWindow.seq<first+chunk_rows).order_by(GeneratedWindow.seq)).all()
        if not staged:continue
        rows=[{'dataset_id':dataset.id,'reference':w.reference,'features':w.features,'label':w.label,'pipe_id':w.pipe_id,'window_start':w.window_start} for w in staged]
        db.execute(insert(SensorWindow),rows)
        db.execute(delete(GeneratedWindow).where(GeneratedWindow.build_id==build.id,GeneratedWindow.seq>=first,GeneratedWindow.seq<first+chunk_rows))
        del staged,rows
    db.execute(delete(GeneratedWindow).where(GeneratedWindow.build_id==build.id))
    build.window_dataset_id=dataset.id;build.status=BuildStatus.SUCCEEDED;build.error=None;build.completed_at=datetime.now(timezone.utc)
    db.commit()
    return build


def discard_build(db:Session,build:WindowBuild):
    db.execute(delete(GeneratedWindow).where(GeneratedWindow.build_id==build.id));db.commit()
