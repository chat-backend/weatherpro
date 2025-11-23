# api/app_utils.py
import unicodedata
import pandas as pd
import numpy as np

from api.weather_services import interpolate_to_24h, detect_alerts, bias_correct
from services.bulletin import generate_bulletin
from services.current_summary import generate_current_summary, summarize_current
from services.openweather import OWM
from services.weatherapi import WeatherAPI
from services.openmeteo import OpenMeteo


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


# ------------------- TIME HELPERS -------------------
def _ensure_datetime_col(df: pd.DataFrame, candidates=("time", "timestamp", "dt", "date", "day")) -> str:
    """Chuẩn hóa một cột thời gian về pd.Timestamp UTC; trả về tên cột hoặc '' nếu không có."""
    if not isinstance(df, pd.DataFrame) or df.empty:
        return ""
    for col in candidates:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
                return col
            except Exception:
                pass
    # fallback: nếu cột là số (epoch)
    for col in df.columns:
        s = df[col]
        try:
            if pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
                df[col] = pd.to_datetime(s, unit="s", errors="coerce", utc=True)
                return col
        except Exception:
            continue
    return ""


def _pick_latest_row(df: pd.DataFrame, time_col: str):
    """Lấy bản ghi mới nhất theo time_col."""
    if not time_col:
        return None
    s = df.dropna(subset=[time_col]).sort_values(time_col)
    if s.empty:
        return None
    return s.iloc[-1]


def _slice_next_24h(df: pd.DataFrame, time_col: str, now: pd.Timestamp | None = None) -> pd.DataFrame:
    """Lọc dải giờ từ thời điểm hiện tại (UTC) đến +24h."""
    if not time_col or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    now = now or pd.Timestamp.utcnow().tz_localize("UTC")
    end = now + pd.Timedelta(hours=24)
    s = df.dropna(subset=[time_col])
    s = s[(s[time_col] >= now) & (s[time_col] <= end)].sort_values(time_col)
    return s


# ------------------- SOURCE COLLECTOR -------------------
def _collect_sources(lat: float, lon: float):
    """
    Gọi dữ liệu từ WeatherAPI (chính), OpenMeteo (phụ), OWM (fallback).
    Trả về dict gồm:
      - primary: dữ liệu chính để hiển thị
      - originals: dữ liệu gốc từ từng nguồn để debug/đối chiếu
    """

    def safe_fetch(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(f"⚠️ Lỗi khi gọi {fn.__name__}: {e}")
            return None

    # WeatherAPI (chính thức)
    wa_hourly = safe_fetch(WeatherAPI.fetch_hourly, lat, lon, hours=24)
    wa_daily = safe_fetch(WeatherAPI.fetch_daily, lat, lon, days=10)
    wa_current = safe_fetch(WeatherAPI.fetch_current, lat, lon)

    if wa_current or (isinstance(wa_hourly, pd.DataFrame) and not wa_hourly.empty):
        return {
            "primary": {"hourly": wa_hourly, "daily": wa_daily, "current": wa_current},
            "originals": {
                "wa": {"hourly": wa_hourly, "daily": wa_daily, "current": wa_current},
                "om": {"hourly": None, "daily": None, "current": None},
                "owm": {"hourly": None, "daily": None, "current": None},
            },
        }

    # Open-Meteo (phụ)
    om_hourly = safe_fetch(OpenMeteo.fetch_hourly, lat, lon, hours=24)
    om_daily = safe_fetch(OpenMeteo.fetch_daily, lat, lon, days=10)
    om_current = safe_fetch(OpenMeteo.fetch_current, lat, lon)

    if om_current or (isinstance(om_hourly, pd.DataFrame) and not om_hourly.empty):
        return {
            "primary": {"hourly": om_hourly, "daily": om_daily, "current": om_current},
            "originals": {
                "wa": {"hourly": wa_hourly, "daily": wa_daily, "current": wa_current},
                "om": {"hourly": om_hourly, "daily": om_daily, "current": om_current},
                "owm": {"hourly": None, "daily": None, "current": None},
            },
        }

    # OWM (fallback cuối cùng)
    owm_hourly = safe_fetch(OWM.fetch_hourly, lat, lon, hours=24)
    owm_daily = safe_fetch(OWM.fetch_daily, lat, lon, days=10)
    owm_current = safe_fetch(OWM.fetch_current, lat, lon)

    return {
        "primary": {"hourly": owm_hourly, "daily": owm_daily, "current": owm_current},
        "originals": {
            "wa": {"hourly": wa_hourly, "daily": wa_daily, "current": wa_current},
            "om": {"hourly": om_hourly, "daily": om_daily, "current": om_current},
            "owm": {"hourly": owm_hourly, "daily": owm_daily, "current": owm_current},
        },
    }


# ------------------- CORE RESPONSE -------------------
def _sanitize_for_json(obj):
    """Làm sạch NaN/NaT trong cấu trúc dữ liệu lồng nhau để JSON hợp lệ."""
    if isinstance(obj, pd.DataFrame):
        return obj.where(pd.notnull(obj), None).to_dict(orient="records")
    if isinstance(obj, pd.Series):
        return _sanitize_for_json(obj.to_dict())
    if isinstance(obj, np.ndarray):
        return _sanitize_for_json(obj.tolist())
    if isinstance(obj, list):
        return [_sanitize_for_json(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    try:
        if isinstance(obj, (np.floating, float)) and np.isnan(obj):
            return None
        if isinstance(obj, (np.integer, int)):
            return obj
        if isinstance(obj, (np.bool_, bool)):
            return obj
        if isinstance(obj, pd.Timestamp):
            return None if pd.isna(obj) else obj.isoformat()
        return None if pd.isna(obj) else obj
    except Exception:
        return obj


