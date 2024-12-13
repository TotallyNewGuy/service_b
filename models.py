from sqlalchemy.orm import declarative_base, mapped_column
from sqlalchemy import BigInteger, Integer, PrimaryKeyConstraint, String


Base = declarative_base()



class CsvData(Base):
    __tablename__ = 'csv_data'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='csv_data_pkey'),
    )

    id = mapped_column(BigInteger)
    version = mapped_column(String, nullable=False)
    tripno = mapped_column(String, nullable=False)
    timepointid = mapped_column(String, nullable=False)
    runid = mapped_column(String, nullable=False)
    timepoint_time = mapped_column(String, nullable=False)
    ctadaytype = mapped_column(Integer, nullable=False)
    ctadaymap = mapped_column(Integer, nullable=False)
    daymap_id = mapped_column(Integer, nullable=False)
    schd_time = mapped_column(Integer, nullable=False)
    arrt_time = mapped_column(Integer, nullable=True)
    schd_headway = mapped_column(Integer, nullable=True)
    arrt_headway = mapped_column(Integer, nullable=True)
