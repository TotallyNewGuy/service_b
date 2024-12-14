import models
import constants
import pandas as pd
from sqlalchemy import text
from sqlalchemy import update
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import async_sessionmaker

from db_util import get_async_engine, get_db_engine


TZINFO = ZoneInfo('America/Chicago')

update_schd_headway_query = f"""
    WITH lagged_data AS (
        SELECT *, 
            LAG(schd_time) OVER (PARTITION BY ctadaytype, timepointid, runid ORDER BY schd_time) AS prev_arrival
        FROM csv_data
        WHERE timepointid IN ({','.join([f"'{str(station_id)}'" for station_id in constants.stop_map])})
    )
    UPDATE csv_data
    SET schd_headway = 
        CASE 
            WHEN lagged_data.prev_arrival IS NOT NULL THEN 
                (csv_data.schd_time - lagged_data.prev_arrival)
            ELSE -1 
        END
    FROM lagged_data
    WHERE csv_data.timepointid = lagged_data.timepointid
      AND csv_data.ctadaytype = lagged_data.ctadaytype
      AND csv_data.schd_time = lagged_data.schd_time;
"""

find_schd_headway_query = """
SELECT * 
FROM csv_data
WHERE runid = %(runid)s
    AND timepointid = %(timepointid)s
ORDER BY ABS(schd_time - %(arrt)s) ASC
LIMIT 1;
"""

find_prev_arrt_query = """
SELECT * 
FROM csv_data
WHERE runid = %(runid)s
    AND timepointid = %(timepointid)s
    AND arrt_time < %(arrt)s
ORDER BY arrt_time DESC
LIMIT 1;
"""

async def create_csv_record(df):
    async_session = async_sessionmaker(get_async_engine(), expire_on_commit=False)
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
    async_session = async_sessionmaker(get_async_engine(), expire_on_commit=False)
    async with async_session() as session:
        async with session.begin():
            await session.execute(
                text(update_schd_headway_query)
            )


def convert_arrT_to_sec_since_midnight(arrT: str):
    date_obj = datetime.fromisoformat(arrT).replace(tzinfo=TZINFO)
    arrT_in_sec = date_obj.second + date_obj.minute * 60 + date_obj.hour * 3600
    return arrT_in_sec


def update_arrt_headway(res_list):
    Session = sessionmaker(bind=get_db_engine())
    curr_session = Session()
    try:
        with curr_session.begin():
            print(f"find_schd_headway_query")
            for res in res_list:
                arrt = convert_arrT_to_sec_since_midnight(res["arrT"])
                runid = f"R{res['rn']}"
                timepointid = constants.stop_map_reverse[res['stpId']]
                df = pd.read_sql(find_schd_headway_query, get_db_engine(), params={'runid': runid, 'timepointid': timepointid, 'arrt': arrt})
                if len(df) != 0:
                    stmt = update(models.CsvData).where(models.CsvData.id == int(df["id"][0])).values(arrt_time=arrt)
                    curr_session.execute(stmt)
                    print(int(df["id"][0]))

        with curr_session.begin():
            print(f"find_prev_arrt_query")
            for res in res_list:
                arrt = convert_arrT_to_sec_since_midnight(res["arrT"])
                runid = f"R{res['rn']}"
                timepointid = constants.stop_map_reverse[res['stpId']]
                df = pd.read_sql(find_prev_arrt_query, get_db_engine(), params={'runid': runid, 'timepointid': timepointid, 'arrt': arrt})
                if len(df) != 0:
                    time_diff = arrt - int(df["arrt_time"][0])
                    print(type(time_diff))
                    stmt = update(models.CsvData).where(models.CsvData.id == int(df["id"][0])).values(arrt_headway=time_diff)
                    curr_session.execute(stmt)
                    print(int(df["id"][0]))
    finally:
        curr_session.close()
    
