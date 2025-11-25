# services/weatherapi.py
from dotenv import load_dotenv
load_dotenv()
import os
import requests
import pandas as pd
from datetime import datetime, timezone

class WeatherAPISource:
    def __init__(self, name: str, base_url: str, api_key: str | None, lang="vi", units="metric"):
        self.name = name
        self.base_url = base_url
        self.api_key = api_key
        self.lang = lang
        self.units = units
        if not self.api_key:
            raise ValueError("⚠️ Thiếu API key cho WeatherAPI. Hãy đặt biến môi trường WEATHERAPI_KEY.")

    def _request(self, endpoint: str, params: dict) -> dict:
        url = f"{self.base_url}/{endpoint}"
        params["key"] = self.api_key
        params["lang"] = self.lang
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"⚠️ Lỗi khi gọi WeatherAPI ({endpoint}): {e}")
            return {}

    def fetch_current(self, lat: float, lon: float) -> dict:
        data = self._request("current.json", {"q": f"{lat},{lon}"})
        if not data:
            return {}

        try:
            loc = data.get("location", {})
            c = data.get("current", {})
            return {
                "ts": datetime.utcfromtimestamp(c.get("last_updated_epoch", 0)).replace(tzinfo=timezone.utc),
                "temp": c.get("temp_c"),
                "humidity": c.get("humidity"),
                "pressure": c.get("pressure_mb"),
                "wind_speed": (c.get("wind_kph", 0) or 0) / 3.6,
                "wind_deg": c.get("wind_degree"),
                "clouds": c.get("cloud"),
                "rain": float(c.get("precip_mm", 0.0) or 0.0),
                "weather_desc": c.get("condition", {}).get("text", ""),
                "source": self.name,
                "location_name": loc.get("name"),
                "country": loc.get("country")
            }
        except Exception as e:
            print(f"⚠️ Lỗi parse dữ liệu WeatherAPI current: {e}")
            return {}

    def fetch_hourly(self, lat: float, lon: float, hours: int = 24) -> pd.DataFrame:
        data = self._request("forecast.json", {
            "q": f"{lat},{lon}",
            "days": 2,
            "aqi": "no",
            "alerts": "no"
        })
        if not data:
            return pd.DataFrame()

        try:
            forecastdays = data.get("forecast", {}).get("forecastday", [])
            records = []
            for day in forecastdays:
                for h in day.get("hour", []):
                    records.append({
                        "ts": datetime.utcfromtimestamp(h.get("time_epoch", 0)).replace(tzinfo=timezone.utc),
                        "temp": h.get("temp_c"),
                        "humidity": h.get("humidity"),
                        "pressure": h.get("pressure_mb"),
                        "wind_speed": (h.get("wind_kph", 0) or 0) / 3.6,
                        "wind_deg": h.get("wind_degree"),
                        "clouds": h.get("cloud"),
                        "rain": float(h.get("precip_mm", 0.0) or 0.0),
                        "weather_desc": h.get("condition", {}).get("text", ""),
                        "source": self.name
                    })
            return pd.DataFrame(records[:hours])
        except Exception as e:
            print(f"⚠️ Lỗi parse dữ liệu WeatherAPI hourly: {e}")
            return pd.DataFrame()

    def fetch_daily(self, lat: float, lon: float, days: int = 7) -> pd.DataFrame:
        data = self._request("forecast.json", {
            "q": f"{lat},{lon}",
            "days": days,
            "aqi": "no",
            "alerts": "no"
        })
        if not data:
            return pd.DataFrame()

        try:
            forecastdays = data.get("forecast", {}).get("forecastday", [])
            records = []
            for day in forecastdays:
                d = day.get("day", {})
                temp_min = d.get("mintemp_c")
                temp_max = d.get("maxtemp_c")
                temp_avg = d.get("avgtemp_c")

                # In log cảnh báo nếu min/max bị None
                if temp_min is None or temp_max is None:
                    print(f"⚠️ Cảnh báo: Thiếu dữ liệu min/max cho ngày {day.get('date')}")

                records.append({
                    "ts": datetime.utcfromtimestamp(day.get("date_epoch", 0)).replace(tzinfo=timezone.utc),
                    "temp_min": temp_min,
                    "temp_max": temp_max,
                    "temp_avg": temp_avg,
                    "humidity": d.get("avghumidity"),
                    "pressure": None,
                    "wind_speed": (d.get("maxwind_kph", 0) or 0) / 3.6,
                    "clouds": None,
                    "rain": float(d.get("totalprecip_mm", 0.0) or 0.0),
                    "weather_desc": d.get("condition", {}).get("text", ""),
                    "source": self.name
                })
            return pd.DataFrame(records)
        except Exception as e:
            print(f"⚠️ Lỗi parse dữ liệu WeatherAPI daily: {e}")
            return pd.DataFrame()

# Khởi tạo instance để dùng trong app
WeatherAPI = WeatherAPISource(
    name="weatherapi",
    base_url="https://api.weatherapi.com/v1",
    api_key=os.getenv("WEATHERAPI_KEY")
)