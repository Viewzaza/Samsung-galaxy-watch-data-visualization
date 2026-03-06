import pandas as pd
import glob
import os
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Configuration
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "admin:adminpassword" # In influxdb 1.8 API, authentication is usually via username:password
INFLUXDB_ORG = "-" # Not needed for 1.8
INFLUXDB_BUCKET = "homeassistant/autogen" # database/retention_policy

def get_influx_client():
    return InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

def import_heart_rate(csv_path, write_api):
    print(f"Importing heart rate from {os.path.basename(csv_path)}...")
    df = pd.read_csv(csv_path, skiprows=1, index_col=False)
    
    # Check if 'com.samsung.health.heart_rate.start_time' and 'com.samsung.health.heart_rate.heart_rate' exist
    col_time = 'com.samsung.health.heart_rate.start_time'
    col_hr = 'com.samsung.health.heart_rate.heart_rate'
    
    if col_time not in df.columns or col_hr not in df.columns:
        print("Required heart rate columns not found.")
        return

    points = []
    # Drop rows with NaN heart rate
    df = df.dropna(subset=[col_time, col_hr])
    for index, row in df.iterrows():
        try:
            timestamp = pd.to_datetime(row[col_time]).to_pydatetime()
            hr_value = float(row[col_hr])
            point = Point("heart_rate") \
                .tag("source", "samsung_health") \
                .field("value", hr_value) \
                .time(timestamp, WritePrecision.S)
            points.append(point)
        except Exception as e:
            print(f"Error HR: {e}")
            continue
        if len(points) >= 1000:
            write_api.write(bucket=INFLUXDB_BUCKET, record=points)
            points = []
            
    if points:
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
    print("Heart rate import complete.")

def import_steps(csv_path, write_api):
    print(f"Importing steps from {os.path.basename(csv_path)}...")
    df = pd.read_csv(csv_path, skiprows=1, index_col=False)
    
    col_time = 'day_time'
    col_count = 'count'
    col_calorie = 'calorie'
    col_distance = 'distance'
    
    if col_time not in df.columns or col_count not in df.columns:
        print("Required steps columns not found.")
        return

    points = []
    df = df.dropna(subset=[col_time, col_count])
    for index, row in df.iterrows():
        try:
            # day_time is in milliseconds
            timestamp = datetime.fromtimestamp(float(row[col_time]) / 1000.0)
            count_value = float(row[col_count])
            
            point = Point("steps") \
                .tag("source", "samsung_health") \
                .field("count", count_value) \
                .time(timestamp, WritePrecision.S)
            
            if col_calorie in df.columns and not pd.isna(row[col_calorie]):
                point.field("calorie", float(row[col_calorie]))
            if col_distance in df.columns and not pd.isna(row[col_distance]):
                point.field("distance", float(row[col_distance]))
                
            points.append(point)
        except Exception as e:
            print(f"Error steps: {e}")
            continue
    if points:
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
    print("Steps import complete.")

def import_sleep(csv_path, write_api):
    print(f"Importing sleep from {os.path.basename(csv_path)}...")
    df = pd.read_csv(csv_path, skiprows=1, index_col=False)
    
    col_start = 'com.samsung.health.sleep.start_time'
    col_end = 'com.samsung.health.sleep.end_time'
    col_efficiency = 'efficiency'
    
    if col_start not in df.columns:
        print("Required sleep columns not found.")
        return

    points = []
    df = df.dropna(subset=[col_start])
    for index, row in df.iterrows():
        try:
            timestamp = pd.to_datetime(row[col_start]).to_pydatetime()
            point = Point("sleep") \
                .tag("source", "samsung_health") \
                .time(timestamp, WritePrecision.S)
            
            has_fields = False
            if col_efficiency in df.columns and not pd.isna(row[col_efficiency]):
                point.field("efficiency", float(row[col_efficiency]))
                has_fields = True
                
            if col_end in df.columns and not pd.isna(row[col_end]):
                end_time = pd.to_datetime(row[col_end]).to_pydatetime()
                duration = (end_time - timestamp).total_seconds() / 3600.0 # Duration in hours
                point.field("duration_hours", duration)
                has_fields = True
                
            if has_fields:
                points.append(point)
        except Exception as e:
            print(f"Error sleep: {e}")
            continue
    if points:
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
    print("Sleep import complete.")

def main(data_dir):
    client = get_influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    hr_files = glob.glob(os.path.join(data_dir, "com.samsung.shealth.tracker.heart_rate.*.csv"))
    if hr_files:
        import_heart_rate(hr_files[0], write_api)
        
    steps_files = glob.glob(os.path.join(data_dir, "com.samsung.shealth.step_daily_trend.*.csv"))
    if steps_files:
        import_steps(steps_files[0], write_api)
        
    sleep_files = glob.glob(os.path.join(data_dir, "com.samsung.shealth.sleep.*.csv"))
    if sleep_files:
        import_sleep(sleep_files[0], write_api)
        
    client.close()
    print("Data import finished successfully.")

if __name__ == "__main__":
    samsung_health_dir = r"C:\Users\user\Downloads\Phone Link\samsung health\Samsung Health\samsunghealth_stampmon4064_20260306183746"
    main(samsung_health_dir)
