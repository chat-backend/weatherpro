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
    if hourly_df is None or not isinstance(hourly_df, pd.DataFrame) or hourly_df.empty:
        raise ValueError("hourly_df rỗng hoặc không tồn tại")

    df = hourly_df.copy()
    if "ts" not in df.columns:
        raise ValueError("hourly_df thiếu cột 'ts'")

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts"])
    if df.empty:
        raise ValueError("hourly_df không có dữ liệu thời gian hợp lệ")

    df = df.set_index("ts").sort_index()

    # Xác định dải ngày đầy đủ theo giờ
    start, end = df.index.min(), df.index.max()
    if pd.isna(start) or pd.isna(end):
        raise ValueError("Không tìm thấy thời gian hợp lệ trong dữ liệu")

    start_day = start.normalize()
    end_day = end.normalize() + pd.Timedelta(hours=23)
    full_range = pd.date_range(start=start_day, end=end_day, freq="h")

    numeric_cols = [c for c in ["temp", "rain", "wind_speed"] if c in df.columns]
    if not numeric_cols:
        raise ValueError("hourly_df thiếu các cột số bắt buộc: temp, rain, wind_speed")

    # Nội suy và lấp đầy
    df_interp = df[numeric_cols].reindex(full_range)
    df_interp = df_interp.interpolate(method="linear").ffill().bfill()

    # Mô tả thời tiết: chọn bản ghi gần nhất
    if "weather_desc" in df.columns:
        df_interp["weather_desc"] = df["weather_desc"].reindex(full_range, method="nearest")

    df_interp = df_interp.reset_index().rename(columns={"index": "ts"})
    return df_interp


# =========================
# 2. AGGREGATE
# =========================
def region_representative(hourly_df: pd.DataFrame, region_name: str) -> pd.DataFrame:
    """
    Tạo đại diện vùng: nhân thêm chỉ số cảm nhận (heat index proxy).
    """
    if hourly_df is None or not isinstance(hourly_df, pd.DataFrame) or hourly_df.empty:
        raise ValueError("hourly_df rỗng hoặc không hợp lệ")

    df = hourly_df.copy()
    if "temp" not in df.columns or "humidity" not in df.columns:
        raise ValueError("Thiếu cột bắt buộc: temp, humidity")

    # Chỉ số nhiệt độ cảm nhận đơn giản (heat index proxy)
    # Công thức proxy: T + 0.33 * RH(%) / 100 * T - 4
    df["heat_index_proxy"] = df["temp"] + 0.33 * (df["humidity"] / 100.0) * df["temp"] - 4
    df["region"] = region_name
    return df


# =========================
# 3. ALERTS
# =========================
def detect_alerts(hourly_df: pd.DataFrame, daily_df: pd.DataFrame = None, current: dict = None):
    """
    Phát hiện cảnh báo dựa trên ngưỡng đơn giản:
    - Mưa >=10mm/h: mưa vừa, >=30mm/h: mưa to
    - Nhiệt độ >=35°C: nóng vừa; heat_index_proxy >=38°C: nóng nặng
    - Gió >=10 m/s: gió vừa; >=17 m/s: gió mạnh
    - Daily: nếu tổng mưa ngày > 50mm: mưa to
    - Current: nếu temp > 35°C: cảnh báo nóng vừa
    """
    alerts = []

    # Phân tích hourly
    if isinstance(hourly_df, pd.DataFrame) and not hourly_df.empty:
        for _, row in hourly_df.iterrows():
            ts = row.get("ts")
            rain = row.get("rain", 0) or 0
            temp = row.get("temp", 0) or 0
            wind = row.get("wind_speed", 0) or 0
            heat_index = row.get("heat_index_proxy", temp) or temp

            if rain >= 30:
                alerts.append({"ts": ts, "type": "heavy_rain", "severity": "severe"})
            elif rain >= 10:
                alerts.append({"ts": ts, "type": "heavy_rain", "severity": "moderate"})
            if temp >= 35:
                alerts.append({"ts": ts, "type": "heat", "severity": "moderate"})
            if heat_index >= 38:
                alerts.append({"ts": ts, "type": "heat", "severity": "severe"})
            if wind >= 17:
                alerts.append({"ts": ts, "type": "wind", "severity": "severe"})
            elif wind >= 10:
                alerts.append({"ts": ts, "type": "wind", "severity": "moderate"})

    # Phân tích daily
    if isinstance(daily_df, pd.DataFrame) and not daily_df.empty:
        if "rain" in daily_df.columns and daily_df["rain"].max() > 50:
            ts = str(daily_df["date"].max()) if "date" in daily_df.columns else "daily"
            alerts.append({"ts": ts, "type": "heavy_rain", "severity": "severe"})

    # Phân tích current
    if isinstance(current, dict) and current:
        if (current.get("temp") or 0) > 35:
            alerts.append({"ts": "now", "type": "heat", "severity": "moderate"})

    return alerts


# =========================
# 4. ENSEMBLE
# =========================
def weighted_ensemble(dfs, weights=None):
    """
    Tạo ensemble có trọng số từ nhiều DataFrame.
    dfs: list[DataFrame] cùng schema/time-index (cần có cột 'ts' hoặc 'date')
    weights: list[float] cùng chiều dài dfs (mặc định đều nhau)
    Kết quả là trung bình có trọng số cho tất cả cột numeric, bao gồm cả temp_min, temp_max, temp_avg.
    """
    if not dfs:
        raise ValueError("No dataframes provided")

    # Nếu không truyền weights → mặc định đều nhau
    if weights is None:
        weights = [1.0 / len(dfs)] * len(dfs)
    if len(weights) != len(dfs):
        raise ValueError("Length of weights must match length of dfs")

    normalized = []
    for df, w in zip(dfs, weights):
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            continue
        df = df.copy()

        # Chuẩn hóa cột thời gian
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
        elif "date" in df.columns:
            df["ts"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
        else:
            raise ValueError("DataFrame must contain 'ts' or 'date' column")

        # Gán trọng số
        df["weight"] = float(w)
        normalized.append(df)

    # Lọc bỏ DataFrame toàn NaN
    normalized = [df for df in normalized if not df.dropna(how="all").empty]
    if not normalized:
        raise ValueError("All dataframes are empty or all-NA")

    # Ghép lại
    merged = pd.concat(normalized, ignore_index=True)

    if "ts" not in merged.columns:
        raise ValueError("DataFrames must contain 'ts' column")

    grouped = merged.groupby("ts", sort=False)

    # Các cột numeric cần ensemble (bao gồm min/max/avg)
    numeric_cols = [
        "temp", "temp_min", "temp_max", "temp_avg",
        "humidity", "pressure", "wind_speed", "clouds", "rain"
    ]

    def aggregate_group(g: pd.DataFrame) -> pd.Series:
        result = {}
        total_weight = g["weight"].sum() if "weight" in g.columns else 0.0
        for col in numeric_cols:
            if col in g.columns and pd.api.types.is_numeric_dtype(g[col]) and total_weight > 0:
                result[col] = (g[col] * g["weight"]).sum() / total_weight
            else:
                result[col] = None
        # giữ lại cột mô tả nếu có
        if "weather_desc" in g.columns and not g["weather_desc"].isna().all():
            result["weather_desc"] = g["weather_desc"].dropna().iloc[0]
        return pd.Series(result)

    out = grouped.apply(aggregate_group, include_groups=False).reset_index()

    # Thay NaN bằng None để JSON hợp lệ
    out = out.where(pd.notnull(out), None)

    return out

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


# =========================
# 6. BIAS CORRECTION
# =========================
def bias_correct(data, bias_stats: dict):
    """
    Hiệu chỉnh sai số đơn giản: x' = x - bias
    bias_stats: dict {var: bias_value}
    Hỗ trợ cả DataFrame và dict. Trả về dữ liệu JSON-safe (NaN → None).
    """
    # DataFrame
    if isinstance(data, pd.DataFrame):
        if data.empty:
            return data
        df = data.copy()
        for var, bias in (bias_stats or {}).items():
            if var in df.columns and pd.api.types.is_numeric_dtype(df[var]):
                df[var] = df[var] - bias
        # Thay NaN bằng None để JSON hợp lệ
        df = df.where(pd.notnull(df), None)
        return df

    # Dict (current)
    elif isinstance(data, dict):
        corrected = data.copy()
        for var, bias in (bias_stats or {}).items():
            val = corrected.get(var)
            if isinstance(val, (int, float)):
                corrected[var] = val - bias
        # Thay NaN bằng None, an toàn với kiểu không hỗ trợ pd.isna
        safe_corrected = {}
        for k, v in corrected.items():
            try:
                safe_corrected[k] = None if pd.isna(v) else v
            except Exception:
                safe_corrected[k] = v
        return safe_corrected

    # List, Series... trả nguyên (để caller tự xử lý)
    else:
        return data