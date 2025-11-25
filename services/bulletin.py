# services/bulletin.py
import pandas as pd
from datetime import datetime
from pytz import timezone
from services.storm_alert import check_storm_alert
from services.unusual_alert import check_unusual_alert
from services.current_summary import generate_current_summary

# Khai báo múi giờ
ICT = timezone("Asia/Bangkok")

# Ngưỡng cảnh báo
RAIN_DAILY_ALERT = 30.0   # mm
WIND_DAILY_ALERT = 10.0   # m/s
HEAT_ALERT = 35.0         # °C
COLD_ALERT = 15.0         # °C
HUMID_ALERT = 85          # %
VERY_HUMID = 90           # %
MUGGY_TEMP = 28.0         # °C (nóng nhẹ trở lên)

# ===== NHẬN ĐỊNH =====

def generate_comment(desc: str, temp: float | None = None, rain: float | None = None, wind: float | None = None) -> str:
    """
    Sinh nhận định tự động dựa trên mô tả, nhiệt độ, mưa, gió.
    Dùng chung cho tất cả phần bản tin.
    """
    d = desc.lower() if desc else ""

    if "mưa" in d or (rain and rain > 5):
        return "💡 Nhận định: Trời có mưa, nên mang theo áo mưa."
    if "nắng" in d or (temp and temp >= 33):
        return "💡 Nhận định: Nắng nóng, chú ý chống nắng khi ra ngoài."
    if "mây" in d and (rain is None or rain == 0):
        return "💡 Nhận định: Nhiều mây, thời tiết ôn hòa."
    if wind and wind >= 10:
        return "💡 Nhận định: Gió mạnh, hạn chế hoạt động ngoài trời."
    if temp and temp <= 15:
        return "💡 Nhận định: Trời lạnh, nên giữ ấm khi ra ngoài."
    return "💡 Nhận định: Thời tiết ôn hòa, thuận lợi cho sinh hoạt."

# ===== HÀM CHÍNH =====

def generate_bulletin(
    region_name: str,
    hourly_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    current: dict | None = None,
    source: str = "weatherapi",
    group_hours: bool = False
) -> dict:
    now_local = datetime.now(ICT)
    today = now_local.date()
    bulletin = []

    # ===== TIÊU ĐỀ =====
    src_name = source
    if isinstance(current, dict) and current.get("source"):
        src_name = current.get("source")

    # lấy mô tả và nhiệt độ hiện tại để chọn icon
    desc = current.get("weather_desc", "") if isinstance(current, dict) else ""
    temp_val = current.get("temp") if isinstance(current, dict) else None

    if desc and "mưa" in desc.lower():
        header_icon = "🌧️"
    elif desc and "nắng" in desc.lower():
        header_icon = "☀️"
    elif desc and "mây" in desc.lower():
        header_icon = "☁️"
    elif isinstance(temp_val, (int, float)) and temp_val <= 15:
        header_icon = "❄️"
    elif isinstance(temp_val, (int, float)) and temp_val >= 33:
        header_icon = "🔥"
    else:
        header_icon = "🌤️"

    bulletin.append(f"{header_icon}✨ BẢN TIN DỰ BÁO THỜI TIẾT — {region_name}")
    bulletin.append(f"📅 Ngày: {today.strftime('%d/%m/%Y')}")
    bulletin.append(f"🕒 Cập nhật lúc: {now_local.strftime('%H:%M %Z')}")
    bulletin.append(f"📡 Nguồn dữ liệu: {src_name}")
    bulletin.append("🌍 Phạm vi: Khu vực địa phương và lân cận")
    bulletin.append("🔔 Thông tin: Nhiệt độ, mưa, gió, độ ẩm, cảnh báo")
    bulletin.append("💡 Lưu ý: Dữ liệu có thể thay đổi theo thời gian\n")

    # ===== HIỆN TẠI =====
    if isinstance(current, dict):
        temp_raw = current.get("temp")
        hum_raw = current.get("humidity")
        wind_raw = current.get("wind_speed")
        desc = current.get("weather_desc", "Không rõ")
        uv = current.get("uv")
        vis = current.get("visibility")
        rain_val = current.get("rain", 0.0)
        wind_val = current.get("wind_speed", 0.0)

        temp_txt = f"{float(temp_raw):.1f}" if isinstance(temp_raw, (int, float)) else "-"
        hum_txt = f"{int(hum_raw)}" if isinstance(hum_raw, (int, float)) else "-"
        wind_txt = f"{float(wind_raw):.1f}" if isinstance(wind_raw, (int, float)) else "-"

        # Chọn icon tiêu đề theo điều kiện
        if "mưa" in desc.lower():
            header_icon = "🌦️"
        elif "nắng" in desc.lower():
            header_icon = "☀️"
        elif "mây" in desc.lower():
            header_icon = "☁️"
        elif isinstance(wind_raw, (int, float)) and wind_raw > 8:
            header_icon = "💨"
        else:
            header_icon = "🌤️"

        bulletin.append(f"{header_icon} HIỆN TẠI")

        # Dòng chi tiết với nhiều biểu tượng khác nhau
        line = (
            f"- 🌡️ {temp_txt}°C"
            f", {header_icon} {desc}"
            f", 💧 {hum_txt}%"
            f", 💨 {wind_txt} m/s"
        )
        if uv is not None:
            line += f", ☀️ UV {uv}"
        if vis is not None:
            line += f", 🌫️ Tầm nhìn {vis} km"

        # Nhận định tự động (dùng temp_raw thay cho temp_val)
        bulletin.append(generate_comment(desc, temp_raw, rain_val, wind_val))
        bulletin.append(line + "\n")
        
    # ===== TỔNG QUAN TRONG NGÀY =====
    total_rain = 0.0
    max_wind = 0.0
    avg_temp = "-"
    min_temp = "-"
    max_temp = "-"
    desc_day = ""   # <-- khai báo trước để tránh NameError

    if isinstance(hourly_df, pd.DataFrame) and not hourly_df.empty:
        dfh = hourly_df.copy()
        if "ts" in dfh.columns:
            dfh["ts"] = pd.to_datetime(dfh["ts"], errors="coerce", utc=True)
            dfh["ts_local"] = dfh["ts"].dt.tz_convert(ICT)
        today_df = dfh[dfh["ts_local"].dt.date == today] if "ts_local" in dfh else dfh

        avg_temp = round(today_df["temp"].mean(), 1) if "temp" in today_df else "-"
        min_temp = round(today_df["temp"].min(), 1) if "temp" in today_df else "-"
        max_temp = round(today_df["temp"].max(), 1) if "temp" in today_df else "-"
        total_rain = round(today_df["rain"].sum(), 1) if "rain" in today_df else 0.0
        max_wind = round(today_df["wind_speed"].max(), 1) if "wind_speed" in today_df else 0.0

        # mô tả tổng quan trong ngày (fallback nếu không có cột weather_desc)
        if "weather_desc" in today_df:
            desc_day = str(today_df["weather_desc"].mode()[0])  # lấy mô tả phổ biến nhất
        else:
            if total_rain >= 20:
                desc_day = "Mưa lớn"
            elif total_rain > 0:
                desc_day = "Có mưa"
            elif max_wind >= 10:
                desc_day = "Gió mạnh"
            elif avg_temp != "-" and avg_temp >= 33:
                desc_day = "Nắng nóng"
            elif avg_temp != "-" and avg_temp <= 15:
                desc_day = "Trời lạnh"
            else:
                desc_day = "Thời tiết ôn hòa"

        # Chọn icon tiêu đề theo điều kiện tổng quan
        if total_rain >= 20:
            header_icon = "⛈️"   # mưa lớn
        elif total_rain > 0:
            header_icon = "🌧️"   # có mưa
        elif max_wind >= 10:
            header_icon = "💨"   # gió mạnh
        elif avg_temp != "-" and avg_temp >= 33:
            header_icon = "🔥"   # nắng nóng
        elif avg_temp != "-" and avg_temp <= 15:
            header_icon = "❄️"   # trời lạnh
        else:
            header_icon = "🌤️"   # thời tiết ôn hòa

        bulletin.append(f"{header_icon} TỔNG QUAN TRONG NGÀY")
        bulletin.append(f"🌡️ Trung bình: {avg_temp}°C (dao động {min_temp}–{max_temp}°C)")
        bulletin.append(f"🌧️ Tổng mưa: {total_rain} mm")
        bulletin.append(f"💨 Gió mạnh nhất: {max_wind} m/s\n")

    # Nhận định tự động (luôn có desc_day)
    bulletin.append(generate_comment(desc_day, avg_temp, total_rain, max_wind))
    bulletin.append("")

    # ===== DỰ BÁO THEO GIỜ (24h) =====
    def choose_weather_icon(desc: str, temp: float | None = None, wind: float | None = None) -> str:
        """Chọn icon phù hợp dựa trên mô tả, nhiệt độ và gió."""
        d = desc.lower()
        if "mưa" in d:
            return "🌦️"
        if "nắng" in d:
            return "☀️"
        if "mây" in d:
            return "☁️"
        if isinstance(wind, (int, float)) and wind >= 8:
            return "💨"
        if isinstance(temp, (int, float)):
            if temp >= 33:
                return "🔥"
            elif temp <= 15:
                return "❄️"
        return "🌤️"

    if isinstance(hourly_df, pd.DataFrame) and not hourly_df.empty:
        bulletin.append("🕑 DỰ BÁO THEO GIỜ (24h)")
        for _, row in hourly_df.head(24).iterrows():
            ts = row["ts"].strftime("%H:%M") if pd.notnull(row["ts"]) else "-"
            temp_val = row.get("temp")
            temp_txt = f"{temp_val:.1f}" if isinstance(temp_val, (int, float)) else "-"
            desc = row.get("weather_desc", "Không rõ")
            rain_val = round(row.get("rain", 0.0) or 0.0, 1)
            wind_val = round(row.get("wind_speed", 0.0) or 0.0, 1)
            hum = row.get("humidity")
            hum_txt = f"{int(hum)}%" if isinstance(hum, (int, float)) else "-"

            # chọn icon tự động
            icon = choose_weather_icon(desc, temp_val, wind_val)

            # dòng chi tiết
            line = (
                f"{ts} → {icon} {temp_txt}°C | {desc} | "
                f"🌧️ {rain_val} mm | 💨 {wind_val} m/s | 💧 {hum_txt}"
            )
            bulletin.append(line)

            # nhận định tự động cho từng giờ
            bulletin.append(generate_comment(desc, temp_val, rain_val, wind_val))

        bulletin.append("")

    # ===== XU HƯỚNG 10 NGÀY =====
    bulletin.append("📅 XU HƯỚNG 10 NGÀY TỚI")
    if isinstance(daily_df, pd.DataFrame) and not daily_df.empty:
        dfd = daily_df.copy()
        dfd["ts"] = pd.to_datetime(dfd["ts"], errors="coerce", utc=True)
        dfd["ts_local"] = dfd["ts"].dt.tz_convert(ICT)
        dfd = dfd.dropna(subset=["ts_local"]).sort_values("ts_local").head(10)

        for _, row in dfd.iterrows():
            date_txt = row["ts_local"].strftime("%d/%m")

            # lấy nhiệt độ với fallback avg
            tmin_val = row.get("temp_min")
            tmax_val = row.get("temp_max")
            tavg_val = row.get("temp_avg")
            if tmin_val is None and isinstance(tavg_val, (int, float)):
                tmin_val = tavg_val
            if tmax_val is None and isinstance(tavg_val, (int, float)):
               tmax_val = tavg_val

            if isinstance(tmin_val, (int, float)) and isinstance(tmax_val, (int, float)) and tmin_val == tmax_val:
                temp_txt = f"{float(tmin_val):.1f}°C"
                avg_temp = tmin_val
            else:
                tmin_txt = f"{float(tmin_val):.1f}" if isinstance(tmin_val, (int, float)) else "-"
                tmax_txt = f"{float(tmax_val):.1f}" if isinstance(tmax_val, (int, float)) else "-"
                temp_txt = f"{tmin_txt}–{tmax_txt}°C"
                avg_temp = (tmin_val + tmax_val) / 2 if isinstance(tmin_val, (int, float)) and isinstance(tmax_val, (int, float)) else None

            rain_val = round(row.get("rain", 0.0) or 0.0, 1)
            wind_val = round(row.get("wind_speed", 0.0) or 0.0, 1)
            hum_val = row.get("humidity")
            hum_txt = f"{int(hum_val)}%" if isinstance(hum_val, (int, float)) else "-"
            desc_day = row.get("weather_desc", "Không rõ")

            # chọn icon tự động
            icon = choose_weather_icon(desc_day, avg_temp, wind_val)

            # dòng chi tiết
            bulletin.append(f"{icon} {date_txt} → 🌡️ {temp_txt} | 🌧️ {rain_val} mm | 💨 {wind_val} m/s | 💧 {hum_txt}")

            # nhận định tự động cho từng ngày
            bulletin.append(generate_comment(desc_day, avg_temp, rain_val, wind_val))
            bulletin.append("")

    # =========================
    # TỔNG CẢNH BÁO
    # =========================
    bulletin.append("🚨 CẢNH BÁO")

    if 'dfd' in locals() and isinstance(dfd, pd.DataFrame) and not dfd.empty:
        def detect_streak_with_decline(df, col, condition, label, icon):
            streak = 0
            start_date = None
            prev_date = None
            for i, row in enumerate(df.itertuples()):
                date = row.ts_local.date()
                val = getattr(row, col, None)
                if condition(val):
                    if streak == 0:
                        start_date = date
                    if prev_date and (date - prev_date).days == 1:
                        streak += 1
                    else:
                        streak = 1
                        start_date = date
                    prev_date = date
                    if streak >= 3:
                        msg = f"🚨 {icon} {label} liên tục {streak} ngày ({start_date.strftime('%d/%m')} → {date.strftime('%d/%m')})"
                        if i + 1 < len(df):
                            next_val = getattr(df.iloc[i+1], col, None)
                            if not condition(next_val):
                                msg += ", sau đó giảm"
                        bulletin.append(msg)
                else:
                    streak = 0
                    start_date = None
                    prev_date = None

        detect_streak_with_decline(dfd, "rain", lambda v: isinstance(v, (int, float)) and v >= 5, "Mưa", "🌧️")
        detect_streak_with_decline(dfd, "temp_max", lambda v: isinstance(v, (int, float)) and v >= HEAT_ALERT, "Nắng nóng", "🔥")
        detect_streak_with_decline(dfd, "wind_speed", lambda v: isinstance(v, (int, float)) and v >= WIND_DAILY_ALERT, "Gió mạnh", "💨")
        detect_streak_with_decline(dfd, "temp_min", lambda v: isinstance(v, (int, float)) and v <= COLD_ALERT, "Trời lạnh", "❄️")
    else:
        bulletin.append("⚠️ Không có dữ liệu dự báo 10 ngày.")

    # ===== CẢNH BÁO TỔNG =====
    alerts = []
    if total_rain > RAIN_DAILY_ALERT:
        alerts.append("🌧️ Mưa lớn trong ngày, nguy cơ ngập úng.")
    if max_wind > WIND_DAILY_ALERT:
        alerts.append("💨 Gió mạnh, cần chú ý an toàn.")
    if isinstance(current, dict):
        if isinstance(current.get("temp"), (int, float)):
            if current["temp"] >= HEAT_ALERT:
                alerts.append("🔥 Nắng nóng gay gắt.")
            if current["temp"] <= COLD_ALERT:
                alerts.append("❄️ Trời lạnh, cần giữ ấm.")
    if not alerts:
        alerts.append("✅ Không có cảnh báo đáng lo ngại.")
    bulletin.extend(alerts)

    # ===== CẢNH BÁO BÃO =====
    storm_msg = check_storm_alert(current or {}, daily_df)
    bulletin.append("\n⛈️ CẢNH BÁO BÃO")
    bulletin.append(storm_msg)

    # ===== CẢNH BÁO HIỆN TƯỢNG BẤT THƯỜNG =====
    unusual_msg = check_unusual_alert(current or {}, hourly_df, daily_df)
    bulletin.append("\n⚠️ CẢNH BÁO HIỆN TƯỢNG BẤT THƯỜNG")
    bulletin.append(unusual_msg)
  
    # ===== KẾT LUẬN =====
    bulletin.append("\n👉 Kết luận: Chủ động theo dõi và chuẩn bị để thích ứng với mọi biến động thời tiết.")

    return {
        "region": region_name,
        "bulletin": "\n".join(bulletin),
        "updated_at": now_local.isoformat()
    }



   





