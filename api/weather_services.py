# services/weather_services.py
import pandas as pd
import json
from shapely.geometry import shape, Point


# =========================
# 1. INTERPOLATE
# =========================
def interpolate_to_24h(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn hóa dữ liệu theo giờ thành đủ mốc (mỗi giờ 1 bản ghi).
    - Cần các cột: ts, temp, rain, wind_speed. weather_desc nếu có.
    - Nội suy tuyến tính cho cột số, ffill/bfill cho lấp khoảng trống.
    """
    if hourly_df is None or hourly_df.empty:
        raise ValueError("hourly_df rỗng hoặc không tồn tại")

    df = hourly_df.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts"])
    if df.empty:
        raise ValueError("hourly_df không có dữ liệu thời gian hợp lệ")

    df = df.set_index("ts").sort_index()
    start, end = df.index.min(), df.index.max()
    if pd.isna(start) or pd.isna(end):
        raise ValueError("Không tìm thấy thời gian hợp lệ trong dữ liệu")

    start_day = start.normalize()
    end_day = end.normalize() + pd.Timedelta(hours=23)
    full_range = pd.date_range(start=start_day, end=end_day, freq="h")

    numeric_cols = [c for c in ["temp", "rain", "wind_speed"] if c in df.columns]
    if not numeric_cols:
        raise ValueError("hourly_df thiếu các cột số bắt buộc: temp, rain, wind_speed")

    df_interp = df[numeric_cols].reindex(full_range)
    df_interp = df_interp.interpolate(method="linear").ffill().bfill()

    if "weather_desc" in df.columns:
        df_interp["weather_desc"] = df["weather_desc"].reindex(full_range, method="nearest")

    df_interp = df_interp.reset_index().rename(columns={"index": "ts"})
    return df_interp


# =========================
# 2. AGGREGATE
# =========================
def region_representative(hourly_df: pd.DataFrame, region_name: str) -> pd.DataFrame:
    """
    Tạo đại diện vùng: trung bình theo giờ, thêm chỉ số cảm nhận.
    """
    df = hourly_df.copy()
    if "temp" not in df.columns or "humidity" not in df.columns:
        raise ValueError("Thiếu cột bắt buộc: temp, humidity")

    # Chỉ số nhiệt độ cảm nhận đơn giản (heat index proxy)
    df["heat_index_proxy"] = df["temp"] + 0.33 * df["humidity"] / 100 * df["temp"] - 4
    df["region"] = region_name
    return df


# =========================
# 3. ALERTS
# =========================
def detect_alerts(hourly_df: pd.DataFrame):
    """
    Phát hiện cảnh báo thời tiết dựa trên dữ liệu theo giờ.
    """
    alerts = []
    for _, row in hourly_df.iterrows():
        ts = row["ts"]

        # Mưa lớn
        if row["rain"] >= 30:
            alerts.append({"ts": ts, "type": "heavy_rain", "severity": "severe"})
        elif row["rain"] >= 10:
            alerts.append({"ts": ts, "type": "heavy_rain", "severity": "moderate"})

        # Nắng nóng
        if row["temp"] >= 35:
            alerts.append({"ts": ts, "type": "heat", "severity": "moderate"})
        if row.get("heat_index_proxy", row["temp"]) >= 38:
            alerts.append({"ts": ts, "type": "heat", "severity": "severe"})

        # Gió mạnh
        if row["wind_speed"] >= 17:
            alerts.append({"ts": ts, "type": "wind", "severity": "severe"})
        elif row["wind_speed"] >= 10:
            alerts.append({"ts": ts, "type": "wind", "severity": "moderate"})

    return alerts


# =========================
# 4. ENSEMBLE
# =========================
def weighted_ensemble(dfs, weights=None):
    """
    Tạo ensemble có trọng số từ nhiều DataFrame.
    dfs: list[DataFrame] cùng schema/time-index
    weights: list[float] cùng chiều dài dfs (mặc định đều nhau)
    """
    if not dfs:
        raise ValueError("No dataframes provided")
    if weights is None:
        weights = [1.0 / len(dfs)] * len(dfs)

    merged = pd.concat([df.assign(weight=w) for df, w in zip(dfs, weights)])
    grouped = merged.groupby("ts")

    out = grouped.apply(lambda g: pd.Series({
        "temp": (g["temp"] * g["weight"]).sum(),
        "humidity": (g["humidity"] * g["weight"]).sum(),
        "pressure": (g["pressure"] * g["weight"]).sum(),
        "wind_speed": (g["wind_speed"] * g["weight"]).sum(),
        "clouds": (g["clouds"] * g["weight"]).sum(),
        "rain": (g["rain"] * g["weight"]).sum()
    })).reset_index()

    return out


def bias_correct(df: pd.DataFrame, bias_stats: dict) -> pd.DataFrame:
    """
    Hiệu chỉnh sai số đơn giản: x' = x - bias
    bias_stats: dict {var: bias_value}
    """
    for var, bias in bias_stats.items():
        if var in df.columns:
            df[var] = df[var] - bias
    return df


# =========================
# 5. REGIONS
# =========================
class RegionIndex:
    def __init__(self, geojson_path: str):
        with open(geojson_path, "r", encoding="utf-8") as f:
            gj = json.load(f)
        self.features = gj.get("features", [])

    def find_region(self, lat: float, lon: float):
        """
        Tìm vùng chứa tọa độ (lat, lon).
        """
        p = Point(lon, lat)
        for feat in self.features:
            polygon = shape(feat["geometry"])
            if polygon.contains(p):
                return {
                    "region_id": feat["properties"].get("id"),
                    "name": feat["properties"].get("name"),
                    "centroid": polygon.centroid.coords[0]
                }
        return None