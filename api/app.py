# api/app.py
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import unicodedata
import pandas as pd
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
from services.utils_weather import (
    get_rain_value,
    format_rain_value,
    safe_str,
)

from services.current_summary import generate_current_summary, summarize_current

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
    """Bỏ dấu tiếng Việt để chuẩn hóa tên địa danh."""
    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore").decode("utf-8")
    return str(text)

def find_region(region_norm: str, region_list, source_name: str):
    """Tìm địa danh trong RegionIndex theo tên chuẩn hóa."""
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
    try:
        result = OWM.fetch_onecall(lat, lon, exclude="minutely,alerts")
        if not result:
            return {"error": safe_str("Không thể lấy dữ liệu từ One Call API.")}

        current = result.get("current", {}) or {}
        hourly = pd.DataFrame(result.get("hourly", []))
        daily = pd.DataFrame(result.get("daily", []))

        rain_value = get_rain_value(current, hourly)
        hourly_24h = interpolate_to_24h(hourly)
        text = generate_bulletin("Khu vực", hourly_24h, daily, current=current, group_hours=group_hours)
        current_summary_text = generate_current_summary(current, hourly)

        return {
            "meta": {"lat": safe_str(lat), "lon": safe_str(lon)},
            "bulletin": safe_str(text),
            "current": safe_str(current_summary_text),
            "rain_value": safe_str(format_rain_value(rain_value)),
            "hourly": safe_str(hourly_24h.to_dict(orient="records")),
            "daily": safe_str(daily.to_dict(orient="records"))
        }
    except Exception as e:
        return {"error": safe_str(f"Không thể sinh bản tin: {str(e)}")}


@app.get("/v1/bulletin")
def bulletin(lat: float, lon: float, group_hours: bool = Query(False)):
    try:
        hourly = pd.DataFrame(OWM.fetch_hourly(lat, lon, hours=24))
        daily = pd.DataFrame(OWM.fetch_daily(lat, lon, days=10))
        current = OWM.fetch_current(lat, lon)

        rain_value = get_rain_value(current, hourly)
        hourly_24h = interpolate_to_24h(hourly)
        text = generate_bulletin("Khu vực", hourly_24h, daily, current=current, group_hours=group_hours)
        current_summary_text = generate_current_summary(current, hourly)

        return {
            "bulletin": safe_str(text),
            "rain_value": safe_str(format_rain_value(rain_value)),
            "current": safe_str(current_summary_text)
        }
    except Exception as e:
        return {"error": safe_str(f"Không thể lấy dữ liệu thời tiết: {str(e)}")}


@app.get("/v1/chat")
def chat(region: str, group_hours: bool = Query(False)):
    region_norm = strip_accents(region.lower())

    match, source = find_region(region_norm, regions, "province")
    if not match:
        match, source = find_region(region_norm, wards, "ward")

    if not match:
        return {"error": safe_str(f"Không tìm thấy địa danh: {region}")}

    lon, lat = match["geometry"]["coordinates"]

    try:
        hourly = pd.DataFrame(OWM.fetch_hourly(lat, lon, hours=24))
        daily = pd.DataFrame(OWM.fetch_daily(lat, lon, days=10))
        current = OWM.fetch_current(lat, lon)

        rain_value = get_rain_value(current, hourly)
        current_summary_text = generate_current_summary(current, hourly)
        hourly_24h = interpolate_to_24h(hourly)
        text = generate_bulletin(
            match["properties"]["name"],
            hourly_24h,
            daily,
            current=current,
            group_hours=group_hours
        )

        return {
            "bulletin": safe_str(text),
            "region": safe_str(match["properties"]["name"]),
            "source": safe_str(source),
            "rain_value": safe_str(format_rain_value(rain_value)),
            "current": safe_str(current_summary_text)
        }
    except Exception as e:
        return {"error": safe_str(f"Không thể lấy dữ liệu thời tiết: {str(e)}")}