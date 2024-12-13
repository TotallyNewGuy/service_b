import models
import pandas as pd
from db_util import get_db_engine
from sqlalchemy.ext.asyncio import async_sessionmaker


async def create_csv_record(df):
    async_session = async_sessionmaker(get_db_engine(), expire_on_commit=False)
    async with async_session() as session:
        async with session.begin():
            for _, row in df.iterrows():
                record = models.CsvData (
                        version = row["version"],
                        tripno = row["tripno"],
                        timepointid = row["timepointid"],
                        runid = row["runid"],
                        timepoint_time = row["timepoint_time"],
                        ctadaytype = row["ctadaytype"],
                        ctadaymap = row["ctadaymap"],
                        daymap_id = row["daymap_id"],
                        schd_time = row["schd_time"],
                    )
                session.add(record)


async def save_csv_to_db(all_entries):
    df = pd.DataFrame.from_records(all_entries)
    df.ctadaytype = pd.to_numeric(df.ctadaytype, errors='coerce').fillna(0).astype('int64')
    df.ctadaymap = pd.to_numeric(df.ctadaymap, errors='coerce').fillna(0).astype('int64')
    df.daymap_id = pd.to_numeric(df.daymap_id, errors='coerce').fillna(0).astype('int64')
    df.schd_time = pd.to_numeric(df.schd_time, errors='coerce').fillna(0).astype('int64')

    await create_csv_record(df)


async def update_schd_headway():
    return (True, "Successfully updating")
