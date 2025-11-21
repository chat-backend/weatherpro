# services/current_summary.py
import pandas as pd
from datetime import datetime, timezone, timedelta
from services.utils_weather import get_rain_value, format_rain_value

ICT = timezone(timedelta(hours=7))  # múi giờ Việt Nam

def generate_current_summary(current: dict, hourly_df: pd.DataFrame) -> str:
    """
    Sinh phần bản tin 'HIỆN TẠI' riêng biệt.
    """
    # lấy lượng mưa hợp lý
    rain_val = get_rain_value(current, hourly_df)
    summary = summarize_current(current, rain_val)

    # thời gian hiện tại
    now_local = datetime.now(ICT)
    ts_str = now_local.strftime("%H:%M ICT")

    # format text
    text = (
        f"🕒 HIỆN TẠI ({ts_str})\n"
        f"{summary['icon']} {summary['desc']}\n"
        f"🌡️ Nhiệt độ: {summary['temp']}\n"
        f"💨 Gió: {summary['wind']}\n"
        f"☁️ Mây: {summary['clouds']}\n"
        f"🌧️ Mưa: {summary['rain']}"
    )
    return text

def summarize_current(current: dict, rain_val: float | None) -> dict:
    """
    Tóm tắt điều kiện hiện tại với icon + mô tả.
    """
    wind_now = float(current.get("wind_speed", 0) or 0.0)
    clouds_now = float(current.get("clouds", 0) or 0.0)
    temp_now = float(current.get("temp", 0.0) or 0.0)

    rv = float(rain_val) if isinstance(rain_val, (int, float)) else 0.0

    if rv > 0 and wind_now > 6:
        icon_now, desc = "⛈️", "Mưa to kèm gió mạnh"
    elif rv > 0:
        if rv < 1:
            icon_now, desc = "🌦️", "Mưa rất nhẹ"
        elif rv < 5:
            icon_now, desc = "🌧️", "Mưa nhẹ"
        elif rv < 20:
            icon_now, desc = "🌧️", "Mưa vừa"
        else:
            icon_now, desc = "⛈️", "Mưa to"
    elif wind_now > 6:
        icon_now, desc = "💨", "Gió mạnh"
    elif clouds_now > 70:
        icon_now, desc = "☁️", "Nhiều mây"
    else:
        if temp_now >= 33:
            icon_now, desc = "🔥", "Nắng nóng gay gắt"
        elif temp_now >= 28:
            icon_now, desc = "☀️", "Nắng mạnh"
        elif temp_now >= 23:
            icon_now, desc = "🌤️", "Nắng nhẹ"
        else:
            icon_now, desc = "☀️", "Trời quang mát"

    return {
        "temp": f"{temp_now:.1f}°C",
        "wind": f"{wind_now:.1f} m/s",
        "clouds": f"{clouds_now:.0f}%",
        "rain": format_rain_value(rain_val),
        "icon": icon_now,
        "desc": desc
    }