import string
import requests
import json
import time

GRAFANA_URL = "http://localhost:3000"
AUTH = ("admin", "admin")

# Wait for Grafana to be ready
def wait_for_grafana():
    print("Waiting for Grafana...")
    for _ in range(30):
        try:
            resp = requests.get(f"{GRAFANA_URL}/api/health")
            if resp.status_code == 200:
                print("Grafana is up!")
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    print("Grafana did not start in time.")
    return False

def setup_datasource():
    url = f"{GRAFANA_URL}/api/datasources"
    payload = {
        "name": "InfluxDB",
        "type": "influxdb",
        "url": "http://influxdb:8086", # Docker internal network
        "access": "proxy",
        "database": "homeassistant",
        "user": "admin",
        "secureJsonData": {
            "password": "adminpassword"
        },
        "isDefault": True
    }
    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, auth=AUTH, json=payload, headers=headers)
    if resp.status_code == 200:
        print("Data source created successfully.")
        return resp.json()['datasource']['uid']
    elif resp.status_code == 409:
        print("Data source already exists.")
        # Fetch UID of existing
        get_resp = requests.get(url, auth=AUTH)
        for ds in get_resp.json():
            if ds["name"] == "InfluxDB":
                return ds["uid"]
    else:
        print(f"Failed to create data source: {resp.text}")
        return None

def create_dashboard(title, panels):
    url = f"{GRAFANA_URL}/api/dashboards/db"
    dashboard = {
        "dashboard": {
            "id": None,
            "uid": None,
            "title": title,
            "tags": ["samsung_health"],
            "timezone": "browser",
            "schemaVersion": 38,
            "version": 0,
            "refresh": "5s",
            "panels": panels
        },
        "overwrite": True
    }
    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, auth=AUTH, json=dashboard, headers=headers)
    if resp.status_code == 200:
        print(f"Dashboard '{title}' created successfully at {resp.json().get('url')}")
    else:
        print(f"Failed to create dashboard '{title}': {resp.text}")

def main():
    if not wait_for_grafana():
        return
        
    ds_uid = setup_datasource()
    if not ds_uid:
        return
        
    ds_ref = {"type": "influxdb", "uid": ds_uid}

    # Heart Rate Panel
    hr_panel = {
        "type": "timeseries",
        "title": "Heart Rate Over Time",
        "gridPos": {"x": 0, "y": 0, "w": 24, "h": 10},
        "datasource": ds_ref,
        "targets": [
            {
                "datasource": ds_ref,
                "query": "SELECT mean(\"value\") FROM \"heart_rate\" WHERE $timeFilter GROUP BY time($__interval) fill(null)",
                "rawQuery": True,
                "refId": "A",
                "format": "time_series"
            }
        ],
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "smooth",
                    "lineWidth": 2,
                    "fillOpacity": 10,
                    "gradientMode": "opacity"
                },
                "color": {
                    "mode": "fixed",
                    "fixedColor": "red"
                },
                "unit": "bpm",
                "min": 40,
                "max": 200
            },
            "overrides": []
        }
    }
    
    # Steps Panel
    steps_panel = {
        "type": "barchart",
        "title": "Daily Steps",
        "gridPos": {"x": 0, "y": 10, "w": 12, "h": 10},
        "datasource": ds_ref,
        "targets": [
            {
                "datasource": ds_ref,
                "query": "SELECT sum(\"count\") FROM \"steps\" WHERE $timeFilter GROUP BY time(1d) fill(null)",
                "rawQuery": True,
                "refId": "A",
                "format": "time_series"
            }
        ],
        "fieldConfig": {
            "defaults": {
                "color": {
                    "mode": "fixed",
                    "fixedColor": "blue"
                },
                "unit": "short",
                "custom": {
                    "lineWidth": 1,
                    "fillOpacity": 80
                }
            },
            "overrides": []
        }
    }
    
    # Calories & Distance Panel
    cal_panel = {
        "type": "timeseries",
        "title": "Calories & Distance",
        "gridPos": {"x": 12, "y": 10, "w": 12, "h": 10},
        "datasource": ds_ref,
        "targets": [
            {
                "datasource": ds_ref,
                "query": "SELECT sum(\"calorie\") as \"Calories (kcal)\", sum(\"distance\") as \"Distance (m)\" FROM \"steps\" WHERE $timeFilter GROUP BY time(1d) fill(null)",
                "rawQuery": True,
                "refId": "A",
                "format": "time_series"
            }
        ],
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "drawStyle": "bars",
                    "lineWidth": 1,
                    "fillOpacity": 60
                }
            },
            "overrides": [
                {
                    "matcher": {"id": "byName", "options": "Calories (kcal)"},
                    "properties": [{"id": "color", "value": {"fixedColor": "orange", "mode": "fixed"}}]
                },
                {
                    "matcher": {"id": "byName", "options": "Distance (m)"},
                    "properties": [{"id": "color", "value": {"fixedColor": "green", "mode": "fixed"}}]
                }
            ]
        }
    }
    
    # Sleep Panel
    sleep_panel = {
        "type": "barchart",
        "title": "Sleep Duration & Efficiency",
        "gridPos": {"x": 0, "y": 20, "w": 24, "h": 10},
        "datasource": ds_ref,
        "targets": [
            {
                "datasource": ds_ref,
                "query": "SELECT sum(\"duration_hours\") as \"Duration (hours)\", mean(\"efficiency\") as \"Efficiency (%)\" FROM \"sleep\" WHERE $timeFilter GROUP BY time(1d)",
                "rawQuery": True,
                "refId": "A",
                "format": "time_series"
            }
        ],
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "lineWidth": 1,
                    "fillOpacity": 80
                }
            },
            "overrides": [
                {
                    "matcher": {"id": "byName", "options": "Duration (hours)"},
                    "properties": [{"id": "color", "value": {"fixedColor": "purple", "mode": "fixed"}}]
                },
                {
                    "matcher": {"id": "byName", "options": "Efficiency (%)"},
                    "properties": [{"id": "color", "value": {"fixedColor": "teal", "mode": "fixed"}}]
                }
            ]
        }
    }

    create_dashboard("Samsung Health Overview", [hr_panel, steps_panel, cal_panel, sleep_panel])

if __name__ == "__main__":
    main()
