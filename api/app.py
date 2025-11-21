# api/app.py
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import unicodedata
import re
import os

# Import từ file gộp mới
from api.weather_services import (
    interpolate_to_24h,
    region_representative,
    detect_alerts,
    weighted_ensemble,
    bias_correct,
    RegionIndex,
)

# Các module chưa gộp vẫn giữ nguyên
from services.etl import WeatherSource
from services.bulletin import generate_bulletin

# Khởi tạo nguồn dữ liệu (OpenWeatherMap)
OWM = WeatherSource(
    name="openweather",
    base_url="https://api.openweathermap.org/data/2.5",
    api_key=os.getenv("OWM_API_KEY")
)

# Khởi tạo ứng dụng FastAPI
app = FastAPI(title="WeatherPro Vietnam")

# Bật CORS để frontend gọi API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load danh sách vùng (GeoJSON)
regions = RegionIndex("configs/vietnam_provinces.geojson")
wards = RegionIndex("configs/danang_wards.geojson")

# ------------------- UTILS -------------------
def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore").decode("utf-8")
    return str(text)

def find_region(region_norm: str, region_list: RegionIndex, source_name: str):
    for feat in region_list.features:
        name_norm = strip_accents(feat["properties"]["name"].lower())
        if region_norm == name_norm:
            return feat, source_name
    return None, None

# ------------------- ROUTES -------------------
@app.get("/v1/bulletin/onecall")
def bulletin_onecall(
    lat: float = Query(...),
    lon: float = Query(...),
    group_hours: bool = Query(False, description="Gom nhóm giờ có cùng mô tả")
):
    result = OWM.fetch_onecall(lat, lon, exclude="minutely,alerts")
    if not result:
        return {"error": "Không thể lấy dữ liệu từ One Call API."}

    current = result["current"]
    hourly = pd.DataFrame(result["hourly"])
    daily = pd.DataFrame(result["daily"])

    try:
        hourly_24h = interpolate_to_24h(hourly)
        text = generate_bulletin("Khu vực", hourly_24h, daily, current=current, group_hours=group_hours)
    except Exception as e:
        return {"error": f"Không thể sinh bản tin: {str(e)}"}

    return {
        "meta": {"lat": lat, "lon": lon},
        "bulletin": text,
        "current": current,
        "hourly": hourly_24h.to_dict(orient="records"),
        "daily": daily.to_dict(orient="records")
    }

@app.get("/v1/bulletin")
def bulletin(lat: float, lon: float, group_hours: bool = Query(False)):
    hourly = pd.DataFrame(OWM.fetch_hourly(lat, lon, hours=24))
    daily = pd.DataFrame(OWM.fetch_daily(lat, lon, days=10))
    current = OWM.fetch_current(lat, lon)

    hourly_24h = interpolate_to_24h(hourly)
    text = generate_bulletin("Khu vực", hourly_24h, daily, current=current, group_hours=group_hours)
    return {"bulletin": text}

@app.get("/v1/chat")
def chat(region: str, group_hours: bool = Query(False)):
    region_norm = region.strip().lower()
    region_norm = re.sub(r"^thoi tiet\s*", "", region_norm)
    region_norm = strip_accents(region_norm)

    match, source = find_region(region_norm, regions, "province")
    if not match:
        match, source = find_region(region_norm, wards, "ward")

    if not match:
        return {"error": f"❌ '{region}' không tìm thấy. Vui lòng nhập đúng tên tỉnh/thành hoặc phường/xã Đà Nẵng."}

    lon, lat = match["geometry"]["coordinates"]

    try:
        hourly = pd.DataFrame(OWM.fetch_hourly(lat, lon, hours=24))
        daily = pd.DataFrame(OWM.fetch_daily(lat, lon, days=10))
        current = OWM.fetch_current(lat, lon)

        hourly_24h = interpolate_to_24h(hourly)
        text = generate_bulletin(match["properties"]["name"], hourly_24h, daily, current=current, group_hours=group_hours)
    except Exception as e:
        return {"error": f"Không thể lấy dữ liệu thời tiết: {str(e)}"}

    return {
        "bulletin": text,
        "region": match["properties"]["name"],
        "source": source
    }