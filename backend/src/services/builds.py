"""Window-build orchestration: queue, status, preview, and failure cleanup."""
from __future__ import annotations

from datetime import datetime,timezone
from fastapi import HTTPException
from sqlalchemy import select,func,delete
from sqlalchemy.orm import Session

from src.constants.enums import BuildStatus
from src.models import WindowBuild,RawDataset,GeneratedWindow,SensorWindow,WindowDataset
from src.utils.serialize import row
from .audit import record
from . import rawdatasets


def queue_build(db:Session,raw_id:int,data,user):
 if not db.get(RawDataset,raw_id):raise HTTPException(404,'原始采样集不存在')
 # Reject a name clash now rather than after a full generation pass.
 if db.scalar(select(WindowDataset.id).where(WindowDataset.name==data.name)):
  raise HTTPException(409,'同名窗口数据集已存在，请换一个生成名称')
 samples=rawdatasets.count_samples(db,raw_id)
 if samples==0:raise HTTPException(422,'原始采样集还没有任何采样，先分批上传')
 obj=WindowBuild(raw_dataset_id=raw_id,name=data.name,status=BuildStatus.RUNNING,sample_count=samples,memory_limit_bytes=data.memory_limit_bytes)
 db.add(obj);db.commit()
 from src.worker import build_windows
 try:build_windows.delay(obj.id)
 except Exception:
  obj.status=BuildStatus.FAILED;obj.error='队列不可用，请重试';db.commit()
  raise HTTPException(503,obj.error)
 record(db,user.email,'raw.build.queued',{'build_id':obj.id,'raw_dataset_id':raw_id});return row(obj)


def list_builds(db:Session,raw_id:int|None=None):
 q=select(WindowBuild).order_by(WindowBuild.id.desc())
 if raw_id is not None:q=q.where(WindowBuild.raw_dataset_id==raw_id)
 return [build_row(db,b) for b in db.scalars(q)]


def get_build(db:Session,id:int):
 obj=db.get(WindowBuild,id)
 if not obj:raise HTTPException(404,'窗口生成记录不存在')
 return build_row(db,obj)


def build_row(db:Session,build:WindowBuild):
 staged=db.scalar(select(func.count()).select_from(GeneratedWindow).where(GeneratedWindow.build_id==build.id)) or 0
 return dict(row(build),staged_windows=staged)


def preview_windows(db:Session,id:int,limit:int=100,offset:int=0):
 """Show generated windows straight from staging, so a reviewer can inspect
 and reconcile the result even while (or after) the build runs."""
 build=db.get(WindowBuild,id)
 if not build:raise HTTPException(404,'窗口生成记录不存在')
 limit=max(1,min(limit,500));offset=max(0,offset)
 if build.status==BuildStatus.SUCCEEDED and build.window_dataset_id:
  q=select(SensorWindow).where(SensorWindow.dataset_id==build.window_dataset_id).order_by(SensorWindow.reference).offset(offset).limit(limit)
  windows=[row(w) for w in db.scalars(q)]
 else:
  q=select(GeneratedWindow).where(GeneratedWindow.build_id==id).order_by(GeneratedWindow.pipe_id,GeneratedWindow.window_start).offset(offset).limit(limit)
  windows=[{'reference':w.reference,'pipe_id':w.pipe_id,'window_start':w.window_start.isoformat(),'features':w.features,'label':w.label} for w in db.scalars(q)]
 if build.status==BuildStatus.SUCCEEDED and build.window_dataset_id:
  total=db.scalar(select(func.count()).select_from(SensorWindow).where(SensorWindow.dataset_id==build.window_dataset_id)) or 0
 elif build.status==BuildStatus.RUNNING:
  # Staging may already hold windows while the build is in flight; report what
  # a reviewer can actually page through, not the not-yet-final total.
  total=db.scalar(select(func.count()).select_from(GeneratedWindow).where(GeneratedWindow.build_id==id)) or 0
 else:
  total=build.window_count
 return {'build':build_row(db,build),'offset':offset,'limit':limit,'total':total,'windows':windows}


def fail_build(db:Session,id:int,error:str):
 """Mark a build failed and remove every staged window.

 The delete and the status change commit together: there is no state in which
 a failed build still carries a partial set of windows.
 """
 build=db.get(WindowBuild,id)
 if not build or build.status==BuildStatus.SUCCEEDED:return
 db.execute(delete(GeneratedWindow).where(GeneratedWindow.build_id==id))
 build.status=BuildStatus.FAILED;build.error=error[:1000];build.window_count=0;build.completed_at=datetime.now(timezone.utc)
 db.commit()
 record(db,'worker','raw.build.failed',{'build_id':id,'error':build.error})
