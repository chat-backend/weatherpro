# services/openweather.py
from dotenv import load_dotenv
load_dotenv()
import os
import requests
import pandas as pd
from datetime import datetime, timezone

class OpenWeatherSource:
    def __init__(self, name: str, api_key: str | None):
        self.name = name
        self.api_key = api_key
        if not self.api_key:
            raise ValueError("⚠️ Thiếu API key cho OpenWeatherMap. Hãy đặt biến môi trường OWM_API_KEY.")

        self.base_url_current = "https://api.openweathermap.org/data/2.5"
        self.base_url_onecall = "https://api.openweathermap.org/data/3.0"

    def _request(self, url: str, params: dict) -> dict:
        params["appid"] = self.api_key
        params["units"] = "metric"
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 401:
                print(f"⚠️ Unauthorized for {url}, sẽ fallback sang forecast.")
                return {"error": "unauthorized"}
            print(f"⚠️ Lỗi khi gọi OWM ({url}): {e}")
            return {}
        except Exception as e:
            print(f"⚠️ Lỗi khi gọi OWM ({url}): {e}")
            return {}

    def fetch_current(self, lat: float, lon: float) -> dict:
        data = self._request(f"{self.base_url_current}/weather", {"lat": lat, "lon": lon})
        if not data or "error" in data:
            return {}

        try:
            rain_val = 0.0
            if isinstance(data.get("rain"), dict):
                rain_val = float(data["rain"].get("1h", 0.0) or 0.0)

            return {
                "ts": datetime.utcfromtimestamp(data.get("dt", 0)).replace(tzinfo=timezone.utc),
                "temp": data.get("main", {}).get("temp"),
                "humidity": data.get("main", {}).get("humidity"),
                "pressure": data.get("main", {}).get("pressure"),
                "wind_speed": data.get("wind", {}).get("speed"),
                "wind_deg": data.get("wind", {}).get("deg"),
                "clouds": data.get("clouds", {}).get("all"),
                "rain": rain_val,
                "weather_desc": data.get("weather", [{}])[0].get("description", ""),
                "source": self.name
            }
        except Exception as e:
            print(f"⚠️ Lỗi parse dữ liệu OWM current: {e}")
            return {}

    def fetch_hourly(self, lat: float, lon: float, hours: int = 24) -> pd.DataFrame:
        data = self._request(f"{self.base_url_onecall}/onecall", {
            "lat": lat, "lon": lon, "exclude": "minutely,daily,alerts"
        })
        if not data or "error" in data:
            forecast = self._request(f"{self.base_url_current}/forecast", {"lat": lat, "lon": lon})
            if not forecast or "list" not in forecast:
                return pd.DataFrame()
            try:
                records = [{
                    "ts": datetime.utcfromtimestamp(item["dt"]).replace(tzinfo=timezone.utc),
                    "temp": item.get("main", {}).get("temp"),
                    "humidity": item.get("main", {}).get("humidity"),
                    "pressure": item.get("main", {}).get("pressure"),
                    "wind_speed": item.get("wind", {}).get("speed"),
                    "wind_deg": item.get("wind", {}).get("deg"),
                    "clouds": item.get("clouds", {}).get("all"),
                    "rain": float(item.get("rain", {}).get("3h", 0.0) or 0.0),
                    "weather_desc": item.get("weather", [{}])[0].get("description", ""),
                    "source": self.name
                } for item in forecast.get("list", [])[:hours]]
                return pd.DataFrame(records)
            except Exception as e:
                print(f"⚠️ Lỗi parse forecast hourly: {e}")
                return pd.DataFrame()

        try:
            hourly = data.get("hourly", [])[:hours]
            records = [{
                "ts": datetime.utcfromtimestamp(item["dt"]).replace(tzinfo=timezone.utc),
                "temp": item.get("temp"),
                "humidity": item.get("humidity"),
                "pressure": item.get("pressure"),
                "wind_speed": item.get("wind_speed"),
                "wind_deg": item.get("wind_deg"),
                "clouds": item.get("clouds"),
                "rain": float(item.get("rain", {}).get("1h", 0.0) or 0.0),
                "weather_desc": item.get("weather", [{}])[0].get("description", ""),
                "source": self.name
            } for item in hourly]
            return pd.DataFrame(records)
        except Exception as e:
            print(f"⚠️ Lỗi parse dữ liệu OWM hourly: {e}")
            return pd.DataFrame()

    def fetch_daily(self, lat: float, lon: float, days: int = 7) -> pd.DataFrame:
        data = self._request(f"{self.base_url_onecall}/onecall", {
            "lat": lat, "lon": lon, "exclude": "minutely,hourly,alerts"
        })
        if not data or "error" in data:
            forecast = self._request(f"{self.base_url_current}/forecast", {"lat": lat, "lon": lon})
            if not forecast or "list" not in forecast:
                return pd.DataFrame()
            try:
                df = pd.DataFrame([{
                    "ts": datetime.utcfromtimestamp(item["dt"]).replace(tzinfo=timezone.utc),
                    "temp": item.get("main", {}).get("temp"),
                    "humidity": item.get("main", {}).get("humidity"),
                    "pressure": item.get("main", {}).get("pressure"),
                    "wind_speed": item.get("wind", {}).get("speed"),
                    "wind_deg": item.get("wind", {}).get("deg"),
                    "clouds": item.get("clouds", {}).get("all"),
                    "rain": float(item.get("rain", {}).get("3h", 0.0) or 0.0),
                    "weather_desc": item.get("weather", [{}])[0].get("description", ""),
                    "source": self.name
                } for item in forecast.get("list", [])])
                df["date"] = df["ts"].dt.date
                daily = df.groupby("date").agg({
                    "temp": ["min", "max"],
                    "humidity": "mean",
                    "pressure": "mean",
                    "wind_speed": "mean",
                    "wind_deg": "mean",
                    "clouds": "mean",
                    "rain": "sum"
                }).reset_index()
                daily.columns = ["ts", "temp_min", "temp_max", "humidity", "pressure",
                                 "wind_speed", "wind_deg", "clouds", "rain"]
                daily["source"] = self.name
                return daily.head(days)
            except Exception as e:
                print(f"⚠️ Lỗi parse forecast daily: {e}")
                return pd.DataFrame()

        try:
            daily = data.get("daily", [])[:days]
            records = [{
                "ts": datetime.utcfromtimestamp(item["dt"]).replace(tzinfo=timezone.utc),
                "temp_min": item.get("temp", {}).get("min"),
                "temp_max": item.get("temp", {}).get("max"),
                "humidity": item.get("humidity"),
                "pressure": item.get("pressure"),
                "wind_speed": item.get("wind_speed"),
                "wind_deg": item.get("wind_deg"),
                "clouds": item.get("clouds"),
                "rain": float(item.get("rain", 0.0) or 0.0),
                "weather_desc": item.get("weather", [{}])[0].get("description", ""),
                "source": self.name
            } for item in daily]
            return pd.DataFrame(records)
        except Exception as e:
            print(f"⚠️ Lỗi parse dữ liệu OWM daily: {e}")
            return pd.DataFrame()
   
    def fetch_onecall(self, lat: float, lon: float, exclude: str = "minutely,alerts") -> dict:
        """Lấy dữ liệu tổng hợp từ OneCall API v3.0, fallback sang forecast nếu unauthorized"""
        data = self._request(f"{self.base_url_onecall}/onecall", {"lat": lat, "lon": lon, "exclude": exclude})
        if not data or "error" in data:
            # Fallback sang forecast 5-day/3-hour
            forecast = self._request(f"{self.base_url_current}/forecast", {"lat": lat, "lon": lon})
            return forecast if forecast else {}
        return data

# Khởi tạo instance OWM để dùng trực tiếp
OWM = OpenWeatherSource(
    name="openweather",
    api_key=os.getenv("OWM_API_KEY")
)