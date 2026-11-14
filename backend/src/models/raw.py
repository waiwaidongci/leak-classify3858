from datetime import datetime,timezone
from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import String,DateTime,Float,Integer,JSON,ForeignKey,UniqueConstraint,Index
from src.config.database import Base
class RawDataset(Base):
 __tablename__='raw_datasets'
 id:Mapped[int]=mapped_column(primary_key=True)
 name:Mapped[str]=mapped_column(String(100),unique=True)
 filename:Mapped[str]=mapped_column(String(160),default='')
 description:Mapped[str]=mapped_column(String(500),default='')
 created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc))
class RawSample(Base):
 """One field observation, natural key (raw dataset, pipe, UTC instant).

 Upload order is irrelevant: generation always streams this table in
 (pipe_id, sampled_at) order, so a retried or re-bounded batch lands on the
 same natural key rather than creating a second observation.
 """
 __tablename__='raw_samples'
 __table_args__=(UniqueConstraint('raw_dataset_id','pipe_id','sampled_at',name='uq_raw_samples_natural_key'),)
 id:Mapped[int]=mapped_column(primary_key=True)
 raw_dataset_id:Mapped[int]=mapped_column(ForeignKey('raw_datasets.id'),index=True)
 pipe_id:Mapped[str]=mapped_column(String(60))
 sampled_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),index=True)
 flow:Mapped[float]=mapped_column(Float)
 pressure:Mapped[float]=mapped_column(Float)
 acoustic_energy:Mapped[float]=mapped_column(Float)
 temperature:Mapped[float]=mapped_column(Float)
 pipe_age:Mapped[float]=mapped_column(Float)
 leak:Mapped[int]=mapped_column(Integer)
class RawUploadBatch(Base):
 __tablename__='raw_upload_batches'
 __table_args__=(UniqueConstraint('raw_dataset_id','upload_key',name='uq_raw_batches_upload_key'),)
 id:Mapped[int]=mapped_column(primary_key=True)
 raw_dataset_id:Mapped[int]=mapped_column(ForeignKey('raw_datasets.id'))
 upload_key:Mapped[str|None]=mapped_column(String(80),nullable=True)
 checksum:Mapped[str]=mapped_column(String(64))
 received:Mapped[int]=mapped_column(Integer)
 inserted:Mapped[int]=mapped_column(Integer)
 deduplicated:Mapped[int]=mapped_column(Integer)
 created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc))
class WindowBuild(Base):
 """Generation attempt. Generated windows are staged here until the whole
 build succeeds; only then is a WindowDataset materialised for training."""
 __tablename__='window_builds'
 id:Mapped[int]=mapped_column(primary_key=True)
 raw_dataset_id:Mapped[int]=mapped_column(ForeignKey('raw_datasets.id'),index=True)
 name:Mapped[str]=mapped_column(String(100))
 status:Mapped[str]=mapped_column(String(20),default='running')
 error:Mapped[str|None]=mapped_column(String(1000),nullable=True)
 window_dataset_id:Mapped[int|None]=mapped_column(ForeignKey('window_datasets.id'),nullable=True)
 sample_count:Mapped[int]=mapped_column(Integer,default=0)
 pipe_count:Mapped[int]=mapped_column(Integer,default=0)
 window_count:Mapped[int]=mapped_column(Integer,default=0)
 memory_limit_bytes:Mapped[int]=mapped_column(Integer,default=0)
 chunk_rows:Mapped[int]=mapped_column(Integer,default=0)
 peak_estimated_bytes:Mapped[int]=mapped_column(Integer,default=0)
 summary:Mapped[dict]=mapped_column(JSON,default=dict)
 created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc))
 completed_at:Mapped[datetime|None]=mapped_column(DateTime(timezone=True),nullable=True)
class GeneratedWindow(Base):
 """Staging rows for an in-flight build. Never read by training."""
 __tablename__='generated_windows'
 __table_args__=(UniqueConstraint('build_id','reference',name='uq_generated_windows_reference'),Index('ix_generated_windows_build','build_id','pipe_id','window_start'))
 id:Mapped[int]=mapped_column(primary_key=True)
 build_id:Mapped[int]=mapped_column(ForeignKey('window_builds.id'))
 pipe_id:Mapped[str]=mapped_column(String(60))
 window_start:Mapped[datetime]=mapped_column(DateTime(timezone=True))
 seq:Mapped[int]=mapped_column(Integer,default=0)
 reference:Mapped[str]=mapped_column(String(80))
 features:Mapped[dict]=mapped_column(JSON)
 label:Mapped[int]=mapped_column(Integer)
 samples:Mapped[int]=mapped_column(Integer,default=0)
 minute_slots:Mapped[int]=mapped_column(Integer,default=0)
