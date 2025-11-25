# services/storm_alert.py
import pandas as pd

# Ngưỡng cảnh báo bão (có thể điều chỉnh)
STORM_WIND_ALERT = 17       # m/s ~ cấp gió bão
STORM_PRESSURE_ALERT = 990  # hPa (áp suất thấp bất thường)
STORM_RAIN_ALERT = 100      # mm/ngày (mưa cực lớn)

def check_storm_alert(current: dict, daily_df: pd.DataFrame) -> str:
    """
    Kiểm tra và sinh cảnh báo bão dựa trên dữ liệu hiện tại và dự báo ngày.
    - current: dict dữ liệu thời tiết hiện tại (đã chuẩn hóa từ nguồn chính/phụ)
    - daily_df: DataFrame dự báo theo ngày (đã chuẩn hóa schema)
    """
    alerts = []

    # Kiểm tra gió hiện tại
    wind = float(current.get("wind_speed", 0.0) or 0.0)
    if wind >= STORM_WIND_ALERT:
        alerts.append(f"⚠️ Gió hiện tại đạt {wind:.1f} m/s, nguy cơ bão mạnh.")

    # Kiểm tra áp suất (nếu có)
    pressure = current.get("pressure")
    if pressure is not None and pressure <= STORM_PRESSURE_ALERT:
        alerts.append(f"⚠️ Áp suất hiện tại {pressure} hPa, dấu hiệu hình thành bão.")

    # Kiểm tra mưa theo ngày
    if isinstance(daily_df, pd.DataFrame) and not daily_df.empty and "rain" in daily_df.columns:
        heavy_rain_days = daily_df[daily_df["rain"] >= STORM_RAIN_ALERT]
        for _, row in heavy_rain_days.iterrows():
            ts_val = row.get("ts")
            try:
                date_txt = pd.to_datetime(ts_val, utc=True).strftime("%d/%m")
            except Exception:
                date_txt = str(ts_val)
            rain_val = row.get("rain", 0.0)
            alerts.append(f"⚠️ Ngày {date_txt}: dự báo mưa cực lớn {rain_val:.1f} mm, nguy cơ bão kèm theo.")

    # Nếu không có cảnh báo
    if not alerts:
        return "✅ Không có dấu hiệu bão trong dữ liệu hiện tại."

    return "\n".join(alerts)