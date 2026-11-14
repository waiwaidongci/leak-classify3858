from datetime import datetime,timezone
from typing import Literal
from pydantic import BaseModel,Field,field_validator

PIPE_MAX=60

class RawSampleInput(BaseModel):
 pipe_id:str=Field(min_length=1,max_length=PIPE_MAX)
 sampled_at:datetime
 flow:float=Field(allow_inf_nan=False)
 pressure:float=Field(allow_inf_nan=False)
 acoustic_energy:float=Field(allow_inf_nan=False)
 temperature:float=Field(allow_inf_nan=False)
 pipe_age:float=Field(ge=0,allow_inf_nan=False)
 leak:Literal[0,1]
 @field_validator('pipe_id')
 @classmethod
 def normalise_pipe(cls,value):
  value=value.strip()
  if not value:raise ValueError('管段编号不能为空')
  if any(ch in value for ch in '\r\n\t'):raise ValueError('管段编号含控制字符')
  return value
 @field_validator('sampled_at')
 @classmethod
 def require_timezone(cls,value):
  # A naive datetime cannot be placed on the UTC minute grid: reject it here
  # rather than guess a zone.
  if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:raise ValueError('采样时间必须带时区；无时区记录拒绝')
  # Store one canonical instant: an offset such as +08:00 is converted to its
  # UTC equivalent, so the minute grid is always applied in UTC.
  return value.astimezone(timezone.utc)

class RawDatasetInput(BaseModel):
 name:str=Field(min_length=1,max_length=100)
 filename:str=Field(default='',max_length=160)
 description:str=Field(default='',max_length=500)

class RawBatchInput(BaseModel):
 samples:list[RawSampleInput]=Field(min_length=1,max_length=20000)
 upload_key:str|None=Field(default=None,max_length=80)

class BuildInput(BaseModel):
 name:str=Field(min_length=1,max_length=100)
 # The cap is the product limit for extra working memory.  Very small values
 # are refused because interpreter overhead alone is a few MB and would make
 # the per-chunk estimate meaningless.
 memory_limit_bytes:int=Field(default=128*1024*1024,ge=16*1024*1024,le=128*1024*1024)
