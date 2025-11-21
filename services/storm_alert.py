# services/storm_alert.py
import pandas as pd

# Ngưỡng cảnh báo bão (có thể điều chỉnh)
STORM_WIND_ALERT = 17    # m/s ~ cấp gió bão
STORM_PRESSURE_ALERT = 990  # hPa (áp suất thấp bất thường)
STORM_RAIN_ALERT = 100   # mm/ngày (mưa cực lớn)

def check_storm_alert(current: dict, daily_df: pd.DataFrame) -> str:
    """
    Hàm kiểm tra và sinh cảnh báo bão dựa trên dữ liệu hiện tại và dự báo ngày.
    - current: dict dữ liệu thời tiết hiện tại
    - daily_df: DataFrame dự báo theo ngày
    """
    alerts = []

    # Kiểm tra gió hiện tại
    wind = current.get("wind_speed", 0.0)
    if wind >= STORM_WIND_ALERT:
        alerts.append(f"⚠️ Gió hiện tại đạt {wind:.1f} m/s, nguy cơ bão mạnh.")

    # Kiểm tra áp suất (nếu có)
    pressure = current.get("pressure", None)
    if pressure and pressure <= STORM_PRESSURE_ALERT:
        alerts.append(f"⚠️ Áp suất hiện tại {pressure} hPa, dấu hiệu hình thành bão.")

    # Kiểm tra mưa theo ngày
    if not daily_df.empty and "rain" in daily_df.columns:
        heavy_rain_days = daily_df[daily_df["rain"] >= STORM_RAIN_ALERT]
        for _, row in heavy_rain_days.iterrows():
            date_txt = pd.to_datetime(row["ts"], utc=True).strftime("%d/%m")
            rain_val = row["rain"]
            alerts.append(f"⚠️ Ngày {date_txt}: dự báo mưa cực lớn {rain_val:.1f} mm, nguy cơ bão kèm theo.")

    if not alerts:
        return "✅ Không có dấu hiệu bão trong dữ liệu hiện tại."
    return "\n".join(alerts)