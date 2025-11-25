# services/openmeteo.py
import requests
import pandas as pd
from datetime import datetime, timezone

class OpenMeteoSource:
    def __init__(self, name: str = "openmeteo", base_url: str = "https://api.open-meteo.com/v1/forecast"):
        self.name = name
        self.base_url = base_url

    def _request(self, params: dict) -> dict:
        """Hàm gọi API chung"""
        try:
            resp = requests.get(self.base_url, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"⚠️ Lỗi khi gọi Open-Meteo: {e}")
            return {}

    def fetch_current(self, lat: float, lon: float) -> dict:
        """Lấy dữ liệu thời tiết hiện tại"""
        data = self._request({
            "latitude": lat,
            "longitude": lon,
            "current_weather": True,
            "timezone": "auto"
        }).get("current_weather", {})

        if not data:
            return {}

        try:
            return {
                "ts": pd.to_datetime(data.get("time"), utc=True) if data.get("time") else None,
                "temp": data.get("temperature"),
                "humidity": None,  # không có humidity current
                "pressure": None,
                "wind_speed": data.get("windspeed"),
                "wind_deg": data.get("winddirection"),
                "clouds": None,
                "rain": None,
                "weather_desc": None,
                "source": self.name
            }
        except Exception as e:
            print(f"⚠️ Lỗi parse dữ liệu Open-Meteo current: {e}")
            return {}

    def fetch_hourly(self, lat: float, lon: float, hours: int = 24) -> pd.DataFrame:
        """Lấy dữ liệu theo giờ"""
        data = self._request({
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,relativehumidity_2m,pressure_msl,windspeed_10m,cloudcover,precipitation",
            "forecast_days": 2,
            "timezone": "auto"
        }).get("hourly", {})

        if not data:
            return pd.DataFrame()

        try:
            df = pd.DataFrame({
                "ts": pd.to_datetime(data.get("time"), utc=True),
                "temp": data.get("temperature_2m"),
                "humidity": data.get("relativehumidity_2m"),
                "pressure": data.get("pressure_msl"),
                "wind_speed": data.get("windspeed_10m"),
                "clouds": data.get("cloudcover"),
                "rain": data.get("precipitation"),
                "weather_desc": None,
                "source": self.name
            })
            return df.head(hours)
        except Exception as e:
            print(f"⚠️ Lỗi parse dữ liệu Open-Meteo hourly: {e}")
            return pd.DataFrame()

    def fetch_daily(self, lat: float, lon: float, days: int = 7) -> pd.DataFrame:
        """Lấy dữ liệu theo ngày"""
        data = self._request({
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
            "forecast_days": days,
            "timezone": "auto"
        }).get("daily", {})

        if not data:
            return pd.DataFrame()

        try:
            df = pd.DataFrame({
                "ts": pd.to_datetime(data.get("time"), utc=True),
                "temp_min": data.get("temperature_2m_min"),
                "temp_max": data.get("temperature_2m_max"),
                "humidity": None,
                "pressure": None,
                "wind_speed": data.get("windspeed_10m_max"),
                "clouds": None,
                "rain": data.get("precipitation_sum"),
                "weather_desc": None,
                "source": self.name
            })
            return df.head(days)
        except Exception as e:
            print(f"⚠️ Lỗi parse dữ liệu Open-Meteo daily: {e}")
            return pd.DataFrame()


# Khởi tạo instance để dùng trong app
OpenMeteo = OpenMeteoSource()