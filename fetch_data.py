# -*- coding: utf-8 -*-
import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import sys
sys.stdout.reconfigure(encoding='utf-8')

# Đọc danh sách tỉnh
cities_df = pd.read_csv("vietnam_provinces_latlon.csv")

API_KEY = "cb74e8b052a441f09ab114945250411"  

# Hàm lấy dữ liệu hourly từ WeatherAPI
def get_hourly_weather(city, lat, lon, date):

    url = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q={lat},{lon}&dt={date.strftime('%Y-%m-%d')}&lang=vi"
    res = requests.get(url)
    if res.status_code != 200:
        print(f"Lỗi {res.status_code} với {city} ngày {date.date()}")
        return []

    data = res.json()
    hourly_data = []
    for h in data.get("forecast", {}).get("forecastday", [])[0].get("hour", []):
        hourly_data.append({
            "date": pd.to_datetime(h['time']),
            "location": city,
            "latitude": lat,
            "longitude": lon,
            "temperature": h.get('temp_c', np.nan),
            "temp_min": np.nan,
            "temp_max": np.nan,
            "humidity": h.get('humidity', np.nan),
            "precipitation": h.get('precip_mm', 0.0),
            "rain_probability": h.get('chance_of_rain', np.nan),
            "rain_duration": 1 if h.get('precip_mm', 0.0) > 0 else 0,
            "wind_speed": h.get('wind_kph', np.nan)/3.6,  # km/h -> m/s
            "wind_direction": h.get('wind_degree', np.nan),
            "pressure": h.get('pressure_mb', np.nan),
            "cloud_cover": h.get('cloud', np.nan),
            "visibility": h.get('vis_km', np.nan),
            "uv_index": h.get('uv', np.nan),
            "sunrise": pd.to_datetime(data.get("forecast", {}).get("forecastday", [])[0].get("astro", {}).get("sunrise"), format="%I:%M %p") if data.get("forecast") else pd.NaT,
            "sunset": pd.to_datetime(data.get("forecast", {}).get("forecastday", [])[0].get("astro", {}).get("sunset"), format="%I:%M %p") if data.get("forecast") else pd.NaT,
            "dew_point": h.get('dewpoint_c', np.nan),
            "feels_like": h.get('feelslike_c', np.nan),
            "weather_condition": h.get('condition', {}).get('text', None),
            "weather_icon": h.get('condition', {}).get('icon', None)
        })
    return hourly_data

# Thu thập dữ liệu 15 ngày
records = []

start_date = datetime(2025, 10, 1)
end_date = datetime(2025, 10, 31)

for _, row in cities_df.iterrows():
    city = row["province"]
    lat = row["latitude"]
    lon = row["longitude"]

    date = start_date
    while date <= end_date:
        print(f"Lấy dữ liệu hourly cho {city} ngày {date.date()}")
        hourly_info = get_hourly_weather(city, lat, lon, date)
        records.extend(hourly_info)
        date += timedelta(days=1)
        time.sleep(1)  # tránh giới hạn API

# Tạo DataFrame
df = pd.DataFrame(records)

# Feature Engineering
df['month'] = df['date'].dt.month

def month_to_season(month):
    if month in [12,1,2]:
        return "Đông"
    elif month in [3,4,5]:
        return "Xuân"
    elif month in [6,7,8]:
        return "Hạ"
    else:
        return "Thu"

df['season'] = df['month'].apply(month_to_season)
df['temp_range'] = df['temp_max'] - df['temp_min']
df['is_rainy'] = df['precipitation'] > 0

def categorize_humidity(h):
    if pd.isna(h):
        return np.nan
    if h < 40:
        return "Thấp"
    elif h <= 70:
        return "Vừa"
    else:
        return "Cao"

df['humidity_category'] = df['humidity'].apply(categorize_humidity)
df['hour_of_day'] = df['date'].dt.hour
df['day_of_week'] = df['date'].dt.day_name()

# Lưu CSV
output_file = "weather_vietnam_hourly_weatherapi.csv"

# Nếu file chưa tồn tại, sẽ tạo mới với header
try:
    with open(output_file, 'x', encoding="utf-8-sig") as f:
        df.to_csv(f, index=False)
        print(f"✅ File mới đã tạo: {output_file}")
except FileExistsError:
    # Nếu file đã tồn tại, ghi thêm dữ liệu, bỏ header
    df.to_csv(output_file, index=False, mode='a', header=False, encoding="utf-8-sig")
    print(f"✅ Dữ liệu đã được thêm vào file hiện có: {output_file}")
print("✅ Thu thập dữ liệu hourly từ WeatherAPI, chuẩn hóa và tạo cột đặc trưng thành công!")