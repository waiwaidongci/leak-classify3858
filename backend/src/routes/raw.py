from datetime import datetime
from fastapi import APIRouter,Depends
from sqlalchemy.orm import Session
from src.config.database import get_db
from src.controllers.dependencies import current_user,writers
from src.schemas.raw import RawDatasetInput,RawBatchInput,BuildInput
from src.services import rawdatasets
from src.services import builds
router=APIRouter()

@router.get('/raw-datasets')
def index(db:Session=Depends(get_db),user=Depends(current_user)):return rawdatasets.list_raw_datasets(db)

@router.post('/raw-datasets',status_code=201)
def create(data:RawDatasetInput,db:Session=Depends(get_db),user=Depends(writers)):return rawdatasets.create_raw_dataset(db,data,user)

@router.get('/raw-datasets/{id}/samples')
def samples(id:int,limit:int=100,pipe_id:str|None=None,start:datetime|None=None,end:datetime|None=None,db:Session=Depends(get_db),user=Depends(current_user)):return rawdatasets.preview_samples(db,id,limit,pipe_id,start,end)

@router.get('/raw-datasets/{id}/batches')
def batches(id:int,db:Session=Depends(get_db),user=Depends(current_user)):return rawdatasets.list_batches(db,id)

@router.post('/raw-datasets/{id}/batches',status_code=201)
def upload(id:int,data:RawBatchInput,db:Session=Depends(get_db),user=Depends(writers)):return rawdatasets.upload_batch(db,id,data,user)

@router.post('/raw-datasets/{id}/builds',status_code=202)
def build(id:int,data:BuildInput,db:Session=Depends(get_db),user=Depends(writers)):return builds.queue_build(db,id,data,user)

@router.get('/builds')
def build_index(raw_dataset_id:int|None=None,db:Session=Depends(get_db),user=Depends(current_user)):return builds.list_builds(db,raw_dataset_id)

@router.get('/builds/{id}')
def build_detail(id:int,db:Session=Depends(get_db),user=Depends(current_user)):return builds.get_build(db,id)

@router.get('/builds/{id}/windows')
def build_windows(id:int,limit:int=100,offset:int=0,db:Session=Depends(get_db),user=Depends(current_user)):return builds.preview_windows(db,id,limit,offset)
