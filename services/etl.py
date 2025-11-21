# services/etl.py
import requests
import pandas as pd
from datetime import datetime, timezone

class WeatherSource:
    def __init__(self, name, base_url, api_key, lang="vi", units="metric"):
        self.name = name
        self.base_url = base_url
        self.api_key = api_key
        self.lang = lang
        self.units = units

    def fetch_hourly(self, lat, lon, hours=48):
        params = {
            "lat": lat, "lon": lon, "appid": self.api_key,
            "units": self.units, "lang": self.lang
        }
        url = f"{self.base_url}/forecast"
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json().get("list", [])
        except Exception as e:
            print(f"⚠️ Lỗi khi gọi API OWM: {e}")
            return pd.DataFrame()

        df = pd.DataFrame([{
            "ts": datetime.utcfromtimestamp(item["dt"]).replace(tzinfo=timezone.utc),
            "temp": item["main"]["temp"],
            "humidity": item["main"]["humidity"],
            "pressure": item["main"]["pressure"],
            "wind_speed": item["wind"]["speed"],
            "wind_deg": item["wind"].get("deg"),
            "clouds": item["clouds"]["all"],
            # Forecast API trả mưa theo 3h
            "rain": float(item.get("rain", {}).get("3h", 0.0) or 0.0),
            "weather_desc": item.get("weather", [{}])[0].get("description", ""),
            "source": self.name
        } for item in data])
        return df

    def fetch_daily(self, lat, lon, days=10):
        hourly = self.fetch_hourly(lat, lon, hours=24*days)
        if hourly.empty:
            return pd.DataFrame()

        daily = (hourly.set_index("ts")
                 .resample("24h").agg({
                     "temp": "mean",
                     "humidity": "mean",
                     "pressure": "mean",
                     "wind_speed": "mean",
                     "clouds": "mean",
                     "rain": "sum"
                 }).reset_index())
        daily["source"] = self.name
        return daily

    def fetch_current(self, lat, lon):
        params = {
            "lat": lat, "lon": lon, "appid": self.api_key,
            "units": self.units, "lang": self.lang
        }
        url = f"{self.base_url}/weather"
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            item = r.json()
        except Exception as e:
            print(f"⚠️ Lỗi khi gọi API OWM (current): {e}")
            return None

        # Lấy lượng mưa chính xác + fallback
        rain_val = 0.0
        if isinstance(item.get("rain"), dict):
            rain_val = float(item["rain"].get("1h", 0.0) or 0.0)
        else:
            rain_val = float(item.get("rain", 0.0) or 0.0)

        weather_arr = item.get("weather", [])
        desc = (weather_arr[0].get("description", "") if weather_arr else "").lower()
        main = (weather_arr[0].get("main", "") if weather_arr else "").strip()

        if rain_val == 0.0 and (main == "Rain" or "mưa" in desc):
            rain_val = 0.1

        current = {
            "ts": datetime.utcfromtimestamp(item["dt"]).replace(tzinfo=timezone.utc),
            "temp": item["main"]["temp"],
            "humidity": item["main"]["humidity"],
            "pressure": item["main"]["pressure"],
            "wind_speed": item["wind"]["speed"],
            "wind_deg": item["wind"].get("deg"),
            "clouds": item["clouds"]["all"],
            "rain": rain_val,
            "weather_desc": (weather_arr[0].get("description", "") if weather_arr else ""),
            "source": self.name
        }
        return current

    def fetch_onecall(self, lat, lon, exclude=None):
        """
        Gọi One Call API 3.0 để lấy current, hourly, daily trong một lần.
        exclude: chuỗi cách nhau bởi dấu phẩy, ví dụ "minutely,alerts"
        """
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": self.units,
            "lang": self.lang,
        }
        if exclude:
            params["exclude"] = exclude

        url = f"{self.base_url}/onecall"
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"⚠️ Lỗi khi gọi One Call API: {e}")
            return None

        # Parse current với fallback
        current = None
        if "current" in data:
            c = data["current"]

            rain_val = 0.0
            if isinstance(c.get("rain"), dict):
                rain_val = float(c["rain"].get("1h", 0.0) or 0.0)
            else:
                rain_val = float(c.get("rain", 0.0) or 0.0)

            weather_arr = c.get("weather", [])
            desc = (weather_arr[0].get("description", "") if weather_arr else "").lower()
            main = (weather_arr[0].get("main", "") if weather_arr else "").strip()

            if rain_val == 0.0 and (main == "Rain" or "mưa" in desc):
                rain_val = 0.1

            current = {
                "ts": datetime.utcfromtimestamp(c["dt"]).replace(tzinfo=timezone.utc),
                "temp": c["temp"],
                "humidity": c["humidity"],
                "pressure": c["pressure"],
                "wind_speed": c["wind_speed"],
                "wind_deg": c.get("wind_deg"),
                "clouds": c["clouds"],
                "rain": rain_val,
                "weather_desc": (weather_arr[0].get("description", "") if weather_arr else ""),
                "source": self.name,
            }

        # Parse hourly
        hourly = pd.DataFrame()
        if "hourly" in data:
            hourly = pd.DataFrame([{
                "ts": datetime.utcfromtimestamp(item["dt"]).replace(tzinfo=timezone.utc),
                "temp": item["temp"],
                "humidity": item["humidity"],
                "pressure": item["pressure"],
                "wind_speed": item["wind_speed"],
                "wind_deg": item.get("wind_deg"),
                "clouds": item["clouds"],
                "rain": float(item.get("rain", {}).get("1h", 0.0) or 0.0),
                "weather_desc": item.get("weather", [{}])[0].get("description", ""),
                "source": self.name
            } for item in data["hourly"]])

        # Parse daily
        daily = pd.DataFrame()
        if "daily" in data:
            daily = pd.DataFrame([{
                "ts": datetime.utcfromtimestamp(item["dt"]).replace(tzinfo=timezone.utc),
                "temp_min": item["temp"]["min"],
                "temp_max": item["temp"]["max"],
                "humidity": item["humidity"],
                "pressure": item["pressure"],
                "wind_speed": item["wind_speed"],
                "wind_deg": item.get("wind_deg"),
                "clouds": item["clouds"],
                "rain": float(item.get("rain", 0.0) or 0.0),
                "weather_desc": item.get("weather", [{}])[0].get("description", ""),
                "source": self.name
            } for item in data["daily"]])

        # Proxy bổ sung: nếu current vẫn 0.0 mà hourly[0] có mưa → cập nhật từ giờ gần nhất
        if current and not hourly.empty:
            h0 = data.get("hourly", [])
            h0_rain = 0.0
            if isinstance(h0, list) and len(h0) > 0 and isinstance(h0[0].get("rain"), dict):
                h0_rain = float(h0[0]["rain"].get("1h", 0.0) or 0.0)
            if current["rain"] == 0.0 and h0_rain > 0.0:
                current["rain"] = h0_rain

        return {
            "current": current,
            "hourly": hourly,
            "daily": daily
        }