# services/bulletin.py
import pandas as pd
from datetime import datetime, timezone, timedelta

# Import các hàm tiện ích
from services.current_summary import generate_current_summary
from services.storm_alert import check_storm_alert
from services.unusual_alert import check_unusual_alert

# ==========================
# Cấu hình ngưỡng cảnh báo
# ==========================
RAIN_HOURLY_ALERT = 5      # mm (mưa lớn theo giờ)
WIND_HOURLY_ALERT = 10     # m/s (gió giật mạnh theo giờ)
RAIN_DAILY_ALERT = 10      # mm (mưa đáng kể theo ngày)
WIND_DAILY_ALERT = 6       # m/s (gió mạnh theo ngày)
HEAT_ALERT = 33            # °C (nắng nóng)
COLD_ALERT = 15            # °C (lạnh)
HUMID_ALERT = 90           # % (độ ẩm cao)

ICT = timezone(timedelta(hours=7))  # Indochina Time (UTC+7)

# ==========================
# Tiện ích xử lý dữ liệu
# ==========================
def _safe_desc(val):
    """Chuẩn hóa mô tả thời tiết (chuỗi, lower, bỏ khoảng trắng thừa)."""
    if pd.isna(val) or val is None:
        return ""
    return str(val).strip().lower()

def classify_rain_hourly(rain_mm: float) -> str:
    if rain_mm == 0: return "Không mưa"
    elif rain_mm < 1: return "Mưa rất nhẹ"
    elif rain_mm < RAIN_HOURLY_ALERT: return "Mưa nhẹ"
    elif rain_mm < 10: return "Mưa vừa"
    elif rain_mm < 30: return "Mưa to"
    else: return "Mưa rất to"

def classify_rain(total_mm: float) -> str:
    if total_mm == 0: return "không mưa"
    elif total_mm < RAIN_DAILY_ALERT: return "mưa ít"
    elif total_mm < 30: return "mưa vừa"
    elif total_mm < 60: return "mưa to"
    else: return "mưa rất to"

def classify_wind(speed: float) -> str:
    if speed < 3: return "gió nhẹ"
    elif speed < WIND_DAILY_ALERT: return "gió vừa"
    elif speed < WIND_HOURLY_ALERT: return "gió mạnh"
    else: return "gió giật rất mạnh"

def classify_temp(temp_c: float) -> str:
    if temp_c < COLD_ALERT: return "lạnh"
    elif temp_c < 23: return "mát"
    elif temp_c < HEAT_ALERT: return "nóng"
    else: return "rất nóng"

def fmt_time_local(ts) -> str:
    ts = pd.to_datetime(ts, utc=True)
    return ts.astimezone(ICT).strftime("%H:%M ICT")

def fmt_date_local(ts) -> str:
    ts = pd.to_datetime(ts, utc=True)
    return ts.astimezone(ICT).strftime("%d/%m")

# ==========================
# Hàm chính sinh bản tin
# ==========================
def generate_bulletin(
    region_name: str,
    hourly_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    current: dict = None,
    group_hours: bool = False,
    rain_value: float = None
) -> str:
    now_local = datetime.now(ICT)
    today = now_local.date()
    bulletin = []

    # ===== TIÊU ĐỀ =====
    bulletin.append(f"🌤️✨ BẢN TIN DỰ BÁO THỜI TIẾT — {region_name}")
    bulletin.append(f"📅 Ngày: {today.strftime('%d/%m/%Y')}")
    bulletin.append(f"🕒 Cập nhật lúc: {now_local.strftime('%H:%M %Z')}\n")

    # ===== HIỆN TẠI =====
    if current is not None and hourly_df is not None:
        current_summary_text = generate_current_summary(current, hourly_df)
        bulletin.append(current_summary_text + "\n")

    # ===== TỔNG QUAN TRONG NGÀY =====
    hourly_df = hourly_df.copy()
    hourly_df["ts"] = pd.to_datetime(hourly_df["ts"], errors="coerce", utc=True)
    hourly_df["ts_local"] = hourly_df["ts"].dt.tz_convert(ICT)
    hourly_df = hourly_df.dropna(subset=["ts_local"])
    if "weather_desc" in hourly_df.columns:
        hourly_df["weather_desc"] = hourly_df["weather_desc"].apply(_safe_desc)
    else:
        hourly_df["weather_desc"] = ""

    hourly_today = hourly_df[hourly_df["ts_local"].dt.date == today]
    ref_df = hourly_today if not hourly_today.empty else hourly_df

    def _safe_mean(df, col): return round(float(df[col].dropna().mean()), 1) if col in df.columns and not df[col].dropna().empty else 0.0
    def _safe_min(df, col): return round(float(df[col].dropna().min()), 1) if col in df.columns and not df[col].dropna().empty else 0.0
    def _safe_max(df, col): return round(float(df[col].dropna().max()), 1) if col in df.columns and not df[col].dropna().empty else 0.0
    def _safe_sum(df, col): return round(float(df[col].dropna().sum()), 1) if col in df.columns and not df[col].dropna().empty else 0.0

    avg_temp = _safe_mean(ref_df, "temp")
    min_temp = _safe_min(ref_df, "temp")
    max_temp = _safe_max(ref_df, "temp")
    total_rain = _safe_sum(ref_df, "rain")
    max_wind = _safe_max(ref_df, "wind_speed")
    avg_humidity = round(float(ref_df["humidity"].dropna().mean()), 1) if "humidity" in ref_df.columns and not ref_df["humidity"].dropna().empty else None

    bulletin.append("🔎 TỔNG QUAN TRONG NGÀY")
    bulletin.append(f"🌡️ Trung bình: {avg_temp}°C (dao động {min_temp}–{max_temp}°C)")
    bulletin.append(f"🌧️ Tổng mưa: {total_rain} mm ({classify_rain(total_rain)})")
    bulletin.append(f"💨 Gió mạnh nhất: {max_wind} m/s ({classify_wind(max_wind)})")
    if avg_humidity is not None:
        bulletin.append(f"💧 Độ ẩm trung bình: {avg_humidity}%")
    if total_rain > RAIN_DAILY_ALERT:
        bulletin.append(f"⚠️ Cảnh báo: Lượng mưa hôm nay đã đạt {total_rain:.1f} mm, nguy cơ ngập úng cục bộ.")
    if max_wind > WIND_DAILY_ALERT:
        bulletin.append(f"⚠️ Cảnh báo: Gió mạnh nhất ghi nhận {max_wind:.1f} m/s, cần chú ý an toàn.")
    bulletin.append("")
    
    # ===== CẢNH BÁO BÃO =====
    storm_alert_text = check_storm_alert(current, daily_df)
    if "⚠️" in storm_alert_text:
        bulletin.append(storm_alert_text)
        bulletin.append("")

    # ===== CẢNH BÁO BẤT THƯỜNG =====
    unusual_alert_text = check_unusual_alert(current, hourly_df, daily_df)
    if "⚠️" in unusual_alert_text:
        bulletin.append(unusual_alert_text)
        bulletin.append("")

    # ===== DIỄN BIẾN THEO GIỜ (24h) =====
    bulletin.append("⏰ DIỄN BIẾN THEO GIỜ (24h)")
    start_ict = datetime.combine(today, datetime.min.time(), tzinfo=ICT)
    end_ict = start_ict + timedelta(hours=23)
    full_ict_range = pd.date_range(start=start_ict, end=end_ict, freq="1h", tz=ICT)
    full_utc_range = full_ict_range.tz_convert(timezone.utc)

    cols_keep = [c for c in ["temp","rain","wind_speed","humidity","weather_desc"] if c in hourly_df.columns]
    h24 = (hourly_df.set_index("ts").sort_index()[cols_keep].reindex(full_utc_range))
    num_cols = [c for c in ["temp","rain","wind_speed","humidity"] if c in h24.columns]
    if num_cols:
        h24[num_cols] = h24[num_cols].interpolate(method="time").ffill().bfill()
    if "weather_desc" in h24.columns:
        h24["weather_desc"] = h24["weather_desc"].fillna(method="ffill").fillna(method="bfill")
    h24 = h24.reset_index().rename(columns={"index":"ts"})
    h24["ts_local"] = h24["ts"].dt.tz_convert(ICT)

    rows = h24.to_dict(orient="records")
    if not rows:
        bulletin.append("⚠️ Không có dữ liệu theo giờ để hiển thị.")
    else:
        for row in rows:
            ts_date = row["ts_local"].strftime("%d/%m/%Y")
            ts_time = row["ts_local"].strftime("%H:%M ICT")

            temp = round(float(row.get("temp", 0.0) or 0.0), 1)
            wind = round(float(row.get("wind_speed", 0.0) or 0.0), 2)
            rain = round(float(row.get("rain", 0.0) or 0.0), 2)
            desc = _safe_desc(row.get("weather_desc", ""))

            # Phân loại mưa chi tiết
            if rain == 0:
                weather_desc = "Không mưa"
                weather_icon = "☀️"
            elif rain < 1:
                weather_desc = "Mưa rất nhẹ"
                weather_icon = "🌦️"
            elif rain < 5:
                weather_desc = "Mưa nhẹ"
                weather_icon = "🌧️"
            elif rain < 20:
                weather_desc = "Mưa vừa"
                weather_icon = "🌧️"
            else:
                weather_desc = "Mưa to"
                weather_icon = "⛈️"

            wind_desc = classify_wind(wind)

            # Logic icon tổng hợp
            if rain > 0 and wind > 6:
                icon = "⛈️"
            elif rain > 0:
                icon = weather_icon
            elif wind > 6:
                icon = "💨"
            elif "mây" in desc:
                icon = "☁️"
            else:
                if temp >= 33:
                    icon = "🔥"
                    weather_desc = "Nắng nóng gay gắt"
                elif temp >= 28:
                    icon = "☀️"
                    weather_desc = "Nắng mạnh"
                elif temp >= 23:
                    icon = "🌤️"
                    weather_desc = "Nắng nhẹ"
                else:
                    icon = "☀️"
                    weather_desc = "Trời quang"

            bulletin.append(
                f"   {icon} {ts_date} {ts_time} → 🌡️ {temp}°C | 💨 {wind} m/s ({wind_desc}) | 🌧️ {rain} mm → {weather_desc}"
        )

    # Xu hướng mưa và gió trong ngày
    if total_rain > 30:
        bulletin.append(f"👉 Xu hướng: Lượng mưa hôm nay đã đạt {total_rain:.1f} mm, có nguy cơ mưa kéo dài.")
    if max_wind > WIND_DAILY_ALERT:
        bulletin.append(f"👉 Xu hướng: Gió mạnh nhất ghi nhận {max_wind:.1f} m/s, cần chú ý an toàn.")

    bulletin.append("")
    
    # ===== CẢNH BÁO BÃO =====
    storm_alert_text = check_storm_alert(current, daily_df)
    if "⚠️" in storm_alert_text:
        bulletin.append(storm_alert_text)
        bulletin.append("")

    # ===== CẢNH BÁO BẤT THƯỜNG =====
    unusual_alert_text = check_unusual_alert(current, hourly_df, daily_df)
    if "⚠️" in unusual_alert_text:
        bulletin.append(unusual_alert_text)
        bulletin.append("")

    # ===== XU HƯỚNG 10 NGÀY =====
    rain_days = 0
    max_rain = 0.0
    max_rain_date = None
    bulletin.append("📅 XU HƯỚNG 10 NGÀY TỚI")

    if daily_df.empty or "ts" not in daily_df.columns:
        bulletin.append("⚠️ Không có dữ liệu dự báo 10 ngày.")
    else:
        daily_sorted = daily_df.copy()
        daily_sorted["ts"] = pd.to_datetime(daily_sorted["ts"], errors="coerce", utc=True)
        daily_sorted["ts_local"] = daily_sorted["ts"].dt.tz_convert(ICT)
        daily_sorted = daily_sorted.dropna(subset=["ts_local"]).sort_values("ts_local").head(10)

        if len(daily_sorted) < 10:
            bulletin.append(f"ℹ️ Xu hướng trên dựa vào {len(daily_sorted)} ngày dữ liệu thực.")

        if daily_sorted.empty:
            bulletin.append("⚠️ Không có dữ liệu dự báo 10 ngày.")
        else:
            for _, row in daily_sorted.iterrows():
                date_txt = row["ts_local"].strftime("%d/%m")
                temp_min_val = row.get("temp_min", None)
                temp_max_val = row.get("temp_max", None)
                temp_single = row.get("temp", None)

                if pd.notna(temp_min_val) and pd.notna(temp_max_val):
                    temp_min = round(float(temp_min_val), 1)
                    temp_max = round(float(temp_max_val), 1)
                elif pd.notna(temp_single):
                    t = round(float(temp_single), 1)
                    temp_min, temp_max = t, t
                else:
                    temp_min, temp_max = "?", "?"

                rain_d = round(float(row.get("rain", 0.0) or 0.0), 1)
                hum_d = round(float(row.get("humidity", 0.0) or 0.0), 1) if "humidity" in daily_sorted.columns and pd.notna(row.get("humidity")) else None
                hum_txt = f" | 💧 {hum_d}%" if hum_d is not None else ""

                wind_d = round(float(row.get("wind_speed", 0.0) or 0.0), 1)
                wind_txt = f" | 💨 {wind_d} m/s" if wind_d is not None else ""

                # Icon theo điều kiện
                if rain_d > 0 and wind_d > 6.0:
                    icon_day = "⛈️"
                elif rain_d > 0:
                    icon_day = "🌧️"
                elif hum_d is not None and hum_d > 85.0:
                    icon_day = "☁️"
                else:
                    icon_day = "☀️"

                bulletin.append(f"{icon_day} {date_txt} → 🌡️ {temp_min}–{temp_max}°C | 🌧️ {rain_d} mm{hum_txt}{wind_txt}")

                # Nhận định riêng cho từng ngày
                notes = []
                if rain_d >= 20.0:
                    notes.append(f"⚠️ {date_txt}: Mưa lớn {rain_d:.1f} mm")
                elif 0 < rain_d < 20.0:
                    notes.append(f"ℹ️ {date_txt}: Có mưa nhẹ {rain_d:.1f} mm")

                if wind_d >= 6.0:
                    notes.append(f"⚠️ {date_txt}: Gió mạnh {wind_d:.1f} m/s")

                if hum_d is not None and hum_d >= 85.0:
                    notes.append(f"ℹ️ {date_txt}: Độ ẩm cao {hum_d:.0f}%")

                if isinstance(temp_max, (int, float)) and temp_max >= 32.0:
                    notes.append(f"⚠️ {date_txt}: Nắng nóng (max {temp_max}°C)")
                elif isinstance(temp_min, (int, float)) and temp_min <= 20.0:
                    notes.append(f"ℹ️ {date_txt}: Trời mát/lạnh (min {temp_min}°C)")

                for n in notes:
                    bulletin.append("   ↪ " + n)

                if rain_d > RAIN_DAILY_ALERT:
                    rain_days += 1
                if rain_d > max_rain:
                    max_rain = rain_d
                    max_rain_date = date_txt

            # Xu hướng tổng thể
            if rain_days >= 3:
                bulletin.append(
                    f"⚠️ Mưa nhiều liên tiếp {rain_days} ngày"
                    + (f", cao điểm {max_rain_date} với {max_rain:.1f} mm." if max_rain_date else ".")
                )
                if max_rain_date:
                    bulletin.append(f"👉 Xu hướng: Mưa đạt đỉnh vào {max_rain_date}, sau đó có xu hướng giảm.")
                else:
                    bulletin.append("👉 Xu hướng: Mưa nhiều nhưng chưa rõ ngày cao điểm.")
            else:
                if max_rain > RAIN_DAILY_ALERT and max_rain_date:
                    bulletin.append(f"👉 Xu hướng: Có ngày mưa lớn ({max_rain:.1f} mm vào {max_rain_date}), cần chú ý.")
                else:
                    bulletin.append("👉 Xu hướng: Thời tiết ổn định, mưa không đáng kể.")
    
            # ===== CẢNH BÁO BÃO TRONG 10 NGÀY =====
            storm_alert_daily = check_storm_alert(current, daily_sorted)
            if "⚠️" in storm_alert_daily:
                  bulletin.append(storm_alert_daily)
                  bulletin.append("")

            # ===== CẢNH BÁO BẤT THƯỜNG TRONG 10 NGÀY =====
            unusual_alert_daily = check_unusual_alert(current, pd.DataFrame(), daily_sorted)
            if "⚠️" in unusual_alert_daily:
                 bulletin.append(unusual_alert_daily)
                 bulletin.append("")

    bulletin.append("")
    return "\n".join(bulletin)