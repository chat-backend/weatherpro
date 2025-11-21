# services/unusual_alert.py
import pandas as pd

# Danh sách hiện tượng bất thường cần cảnh báo (mở rộng, bao gồm cả giả tưởng)
UNUSUAL_EVENTS = [
    "sấm sét",
    "dông tố",
    "mưa đá",
    "lốc xoáy",
    "mưa axit",
    "sương mù dày đặc",
    "nhiệt độ bất thường",
    "áp suất bất thường",
    "hiện tượng kỳ lạ",
    "động đất",
    "sóng thần",
    "núi lửa",
    "bão cát",
    "khói bụi",
    "bầu trời xuất hiện vật lạ",
    "ánh sáng bất thường",
    "mưa thiên thạch",
    "sương muối",
    "hạn hán cực đoan",
    "cháy rừng",
]

def check_unusual_alert(current: dict, hourly_df: pd.DataFrame, daily_df: pd.DataFrame) -> str:
    """
    Hàm kiểm tra và sinh cảnh báo hiện tượng bất thường.
    - current: dict dữ liệu thời tiết hiện tại
    - hourly_df: DataFrame dự báo theo giờ
    - daily_df: DataFrame dự báo theo ngày
    """
    alerts = []

    # Kiểm tra mô tả thời tiết hiện tại
    desc = str(current.get("weather_desc", "")).lower()
    for event in UNUSUAL_EVENTS:
        if event in desc:
            alerts.append(f"⚠️ Hiện tượng bất thường phát hiện: {event.capitalize()} trong điều kiện hiện tại.")

    # Kiểm tra dữ liệu theo giờ
    if not hourly_df.empty and "weather_desc" in hourly_df.columns:
        for _, row in hourly_df.iterrows():
            desc_hour = str(row.get("weather_desc", "")).lower()
            for event in UNUSUAL_EVENTS:
                if event in desc_hour:
                    ts = pd.to_datetime(row["ts"], utc=True).strftime("%d/%m %H:%M")
                    alerts.append(f"⚠️ {ts}: dự báo xuất hiện {event}.")

    # Kiểm tra dữ liệu theo ngày
    if not daily_df.empty and "weather_desc" in daily_df.columns:
        for _, row in daily_df.iterrows():
            desc_day = str(row.get("weather_desc", "")).lower()
            for event in UNUSUAL_EVENTS:
                if event in desc_day:
                    ts = pd.to_datetime(row["ts"], utc=True).strftime("%d/%m")
                    alerts.append(f"⚠️ Ngày {ts}: dự báo có {event}.")

    if not alerts:
        return "✅ Không phát hiện hiện tượng bất thường trong dữ liệu hiện tại."
    return "\n".join(alerts)