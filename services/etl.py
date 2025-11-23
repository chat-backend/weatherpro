# services/etl.py
import pandas as pd
import numpy as np

from services.openweather import OWM
from services.weatherapi import WeatherAPI
from services.openmeteo import OpenMeteo

# ==============================
# Reliability tracking (ưu tiên: WeatherAPI > OpenMeteo > OpenWeather)
# ==============================
RELIABILITY = {
    "weatherapi": 1.0,     # nguồn chính thức
    "openmeteo": 0.9,      # nguồn phụ
    "openweather": 0.5     # fallback
}

DEVIATION_COUNT = {
    "weatherapi": 0,
    "openmeteo": 0,
    "openweather": 0
}


# ==============================
# Collect data from sources
# ==============================
def _safe_fetch(source, lat, lon):
    """Bọc gọi API từng nguồn, trả về dict current/hourly/daily; lỗi thì trả về {}."""
    try:
        return {
            "current": source.fetch_current(lat, lon),
            "hourly": source.fetch_hourly(lat, lon),
            "daily": source.fetch_daily(lat, lon)
        }
    except Exception as e:
        print(f"⚠️ Lỗi {source.name}: {e}")
        return {}


def collect_sources(lat: float, lon: float) -> dict:
    """Thu thập dữ liệu từ 3 nguồn (WeatherAPI, OpenMeteo, OpenWeather)."""
    return {
        "weatherapi": _safe_fetch(WeatherAPI, lat, lon),
        "openmeteo": _safe_fetch(OpenMeteo, lat, lon),
        "openweather": _safe_fetch(OWM, lat, lon)
    }


# ==============================
# Merge strategies
# ==============================
def merge_sources(results: dict, strategy: str = "best") -> dict:
    """
    Hợp nhất dữ liệu từ nhiều nguồn.
    strategy:
        - "best": chọn nguồn khả dụng nhất theo thứ tự ưu tiên WeatherAPI → OpenMeteo → OpenWeather
        - "avg": lấy trung bình từ các nguồn có dữ liệu
    """
    merged = {"current": {}, "hourly": None, "daily": None}

    # --- CURRENT ---
    if strategy == "best":
        for src in ["weatherapi", "openmeteo", "openweather"]:
            cur = results.get(src, {}).get("current", {})
            if cur and cur.get("temp") is not None:
                merged["current"] = cur
                break
    elif strategy == "avg":
        temps, hums, press, winds = [], [], [], []
        for src in results.values():
            cur = src.get("current", {})
            if cur:
                if cur.get("temp") is not None: temps.append(cur["temp"])
                if cur.get("humidity") is not None: hums.append(cur["humidity"])
                if cur.get("pressure") is not None: press.append(cur["pressure"])
                if cur.get("wind_speed") is not None: winds.append(cur["wind_speed"])
        merged["current"] = {
            "temp": float(np.mean(temps)) if temps else None,
            "humidity": float(np.mean(hums)) if hums else None,
            "pressure": float(np.mean(press)) if press else None,
            "wind_speed": float(np.mean(winds)) if winds else None,
            "source": "avg"
        }

    # --- HOURLY ---
    if strategy == "best":
        for src in ["weatherapi", "openmeteo", "openweather"]:
            df = results.get(src, {}).get("hourly")
            if isinstance(df, pd.DataFrame) and not df.empty:
                merged["hourly"] = df
                break
    elif strategy == "avg":
        dfs = [
            src.get("hourly")
            for src in results.values()
            if isinstance(src.get("hourly"), pd.DataFrame) and not src.get("hourly").empty
        ]
        if dfs:
            # Tránh FutureWarning: lọc rỗng trước khi concat
            dfs = [d for d in dfs if not d.dropna(how="all").empty]
            if dfs:
                merged["hourly"] = (
                    pd.concat(dfs, ignore_index=True)
                      .groupby("ts", sort=False)
                      .mean(numeric_only=True)
                      .reset_index()
                )

    # --- DAILY ---
    if strategy == "best":
        for src in ["weatherapi", "openmeteo", "openweather"]:
            df = results.get(src, {}).get("daily")
            if isinstance(df, pd.DataFrame) and not df.empty:
                merged["daily"] = df
                break
    elif strategy == "avg":
        dfs = [
            src.get("daily")
            for src in results.values()
            if isinstance(src.get("daily"), pd.DataFrame) and not src.get("daily").empty
        ]
        if dfs:
            dfs = [d for d in dfs if not d.dropna(how="all").empty]
            if dfs:
                merged["daily"] = (
                    pd.concat(dfs, ignore_index=True)
                      .groupby("ts", sort=False)
                      .mean(numeric_only=True)
                      .reset_index()
                )

    return merged


# ==============================
# Dynamic reliability merge
# ==============================
def merge_sources_dynamic(results: dict) -> dict:
    """Chọn nguồn có reliability cao nhất và chuẩn hóa dữ liệu daily."""
    merged = {"current": {}, "hourly": None, "daily": None}
    sorted_sources = sorted(RELIABILITY.items(), key=lambda x: x[1], reverse=True)

    # --- CURRENT ---
    for src, _ in sorted_sources:
        cur = results.get(src, {}).get("current", {})
        if cur and cur.get("temp") is not None:
            merged["current"] = cur
            break

    # --- HOURLY ---
    for src, _ in sorted_sources:
        df = results.get(src, {}).get("hourly")
        if isinstance(df, pd.DataFrame) and not df.empty:
            merged["hourly"] = df
            break

    # --- DAILY ---
    for src, _ in sorted_sources:
        df = results.get(src, {}).get("daily")
        if isinstance(df, pd.DataFrame) and not df.empty:
            merged["daily"] = _normalize_daily(df, src)
            break

    return merged


# ==============================
# Weighted average merge
# ==============================
def merge_sources_weighted(results: dict) -> dict:
    """Trung bình có trọng số theo reliability cho current, hourly, daily."""
    merged = {"current": {}, "hourly": None, "daily": None}

    # --- CURRENT ---
    temps, hums, press, winds, weights = [], [], [], [], []
    for src, score in RELIABILITY.items():
        cur = results.get(src, {}).get("current", {})
        if cur and cur.get("temp") is not None:
            temps.append(cur.get("temp"))
            hums.append(cur.get("humidity"))
            press.append(cur.get("pressure"))
            winds.append(cur.get("wind_speed"))
            weights.append(score)

    def weighted_avg(values, weights):
        if not values or not weights or sum(weights) == 0:
            return None
        vals, wts = [], []
        for v, w in zip(values, weights):
            if v is not None:
                vals.append(v)
                wts.append(w)
        return float(np.average(vals, weights=wts)) if vals else None

    merged["current"] = {
        "temp": weighted_avg(temps, weights),
        "humidity": weighted_avg(hums, weights),
        "pressure": weighted_avg(press, weights),
        "wind_speed": weighted_avg(winds, weights),
        "source": "weighted"
    }

    # --- HOURLY ---
    dfs_hourly = []
    for src, score in RELIABILITY.items():
        df = results.get(src, {}).get("hourly")
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.copy()
            for col in ["temp", "rain", "wind_speed", "humidity"]:
                if col in df.columns:
                    df[col] = df[col] * score
            df["weight"] = score
            dfs_hourly.append(df)

    if dfs_hourly:
        dfs_hourly = [d for d in dfs_hourly if not d.dropna(how="all").empty]
        if dfs_hourly:
            combined = pd.concat(dfs_hourly, ignore_index=True)
            grouped = combined.groupby("ts", sort=False).sum(numeric_only=True).reset_index()
            for col in ["temp", "rain", "wind_speed", "humidity"]:
                if col in grouped.columns and "weight" in grouped.columns and grouped["weight"].sum() != 0:
                    grouped[col] = grouped[col] / grouped["weight"]
            merged["hourly"] = grouped.drop(columns=["weight"]) if "weight" in grouped.columns else grouped

    # --- DAILY ---
    dfs_daily = []
    for src, score in RELIABILITY.items():
        df = results.get(src, {}).get("daily")
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = _normalize_daily(df, src).copy()
            for col in ["temp_min", "temp_max", "rain", "wind_speed"]:
                if col in df.columns:
                    df[col] = df[col] * score
            df["weight"] = score
            dfs_daily.append(df)

    if dfs_daily:
        dfs_daily = [d for d in dfs_daily if not d.dropna(how="all").empty]
        if dfs_daily:
            combined = pd.concat(dfs_daily, ignore_index=True)
            grouped = combined.groupby("ts", sort=False).sum(numeric_only=True).reset_index()
            for col in ["temp_min", "temp_max", "rain", "wind_speed"]:
                if col in grouped.columns and "weight" in grouped.columns and grouped["weight"].sum() != 0:
                    grouped[col] = grouped[col] / grouped["weight"]
            daily_out = grouped.drop(columns=["weight"]) if "weight" in grouped.columns else grouped
            merged["daily"] = _normalize_daily(daily_out, "avg")

    return merged


# ==============================
# Reliability update by deviation
# ==============================
def update_reliability_multi(results: dict,
                             metrics: list = ["temp", "humidity", "pressure", "wind_speed"],
                             thresholds: dict = None):
    """Cập nhật reliability dựa trên nhiều chỉ số."""
    if thresholds is None:
        thresholds = {
            "temp": 3.0,
            "humidity": 10.0,
            "pressure": 5.0,
            "wind_speed": 2.0
        }

    for metric in metrics:
        values, sources = [], []
        for src, data in results.items():
            cur = data.get("current", {})
            if cur and cur.get(metric) is not None:
                values.append(cur[metric])
                sources.append(src)

        if len(values) < 2:
            continue

        avg_val = float(np.mean(values))
        for src, val in zip(sources, values):
            deviation = abs(val - avg_val)
            if deviation > thresholds.get(metric, 1.0):
                RELIABILITY[src] = max(RELIABILITY[src] - 0.2, 0.0)
                DEVIATION_COUNT[src] += 1
            else:
                RELIABILITY[src] = min(RELIABILITY[src] + 0.1, 2.0)


# ==============================
# Reliability report
# ==============================
def reliability_report() -> pd.DataFrame:
    """Trả về báo cáo độ tin cậy."""
    return pd.DataFrame({
        "source": list(RELIABILITY.keys()),
        "reliability_score": list(RELIABILITY.values()),
        "deviation_count": [DEVIATION_COUNT[src] for src in RELIABILITY.keys()]
    })


# ==============================
# Forecast preparation
# ==============================
def prepare_forecast(lat: float, lon: float, strategy: str = "best") -> dict:
    """
    Gom dữ liệu từ 3 nguồn và hợp nhất theo chiến lược.
    strategy:
        - "best": ưu tiên WeatherAPI -> OpenMeteo -> OpenWeather
        - "avg": trung bình cộng
        - "dynamic": chọn theo reliability cao nhất
        - "weighted": trung bình có trọng số theo reliability
    """
    results = collect_sources(lat, lon)

    if strategy in ["best", "avg"]:
        merged = merge_sources(results, strategy=strategy)
    elif strategy == "dynamic":
        merged = merge_sources_dynamic(results)
    elif strategy == "weighted":
        merged = merge_sources_weighted(results)
    else:
        merged = merge_sources(results, strategy="best")

    # Cập nhật reliability sau mỗi lần merge
    update_reliability_multi(results)

    return merged

