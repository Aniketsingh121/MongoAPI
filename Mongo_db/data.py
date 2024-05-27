from concurrent.futures import ThreadPoolExecutor
from config.db_port import get_database
import os
import concurrent
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import mongo_query.datatransformation as datatransformation
import mongo_query.sensor_ids as sensor_ids



db = get_database()
sensor = db.load_profile_jdvvnl

def data_fetch(sensor_id, site_id):
    try:
        from_id = sensor_id + "-2024-01-01 00:00:00"
        to_id = sensor_id + "-2024-03-31 23:59:59"
        query = {"_id": {"$gte": from_id, "$lt": to_id}}

        results = list(sensor.find(query))
         

        df = datatransformation.init_transformation(results, site_id)
        if df is None:
           print(f"nothing transformed records for sensor_id: {sensor_id}")
        else:
            print(f"Fetched and transformed records {len(df)} for sensor_id: {sensor_id}")
        return df


    except Exception as e:
        print(f"Error fetching data for sensor_id {sensor_id}: {e}", exc_info=True)
        return None

def fetch_data_for_sensors(circle_id, output_dir="sensor_data"):
    os.makedirs(output_dir, exist_ok=True)

    sensors = sensor_ids.sensor_ids(circle_id)
    sensorids = [doc["id"] for doc in sensors]
    site_ids = [doc["site_id"] for doc in sensors]

    all_dicts = []  
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(data_fetch, sensor_id, site_id): (sensor_id, site_id) for sensor_id, site_id in zip(sensorids, site_ids)}

        for future in concurrent.futures.as_completed(futures):
            dicts = future.result()  
            if dicts is not None and len(dicts) > 0: 
                all_dicts.extend(dicts)  

    if all_dicts:
        combined_df = pd.DataFrame(all_dicts)
        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, os.path.join(output_dir, f"{circle_id}_data.parquet"))
        return "saved "


