# bulletin.py
import pandas as pd
from datetime import datetime, timezone, timedelta

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

def _to_ict(dt):
    """Đưa datetime về ICT, chấp nhận cả naive (giả định UTC) và aware."""
    if dt is None:
        return None
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ICT)

def _safe_desc(val):
    """Chuẩn hóa mô tả thời tiết (chuỗi, lower, bỏ khoảng trắng thừa)."""
    if pd.isna(val) or val is None:
        return ""
    return str(val).strip().lower()

# ==========================
# Tiện ích xử lý/định dạng
# ==========================
def summarize_period(df: pd.DataFrame):
    """Tóm tắt dữ liệu theo giờ: nhiệt độ TB, tổng mưa, gió mạnh nhất, độ ẩm TB nếu có."""
    temp_avg = round(df["temp"].mean(), 1)
    rain_sum = round(df["rain"].sum(), 1)
    wind_max = round(df["wind_speed"].max(), 1)
    humid_avg = round(df["humidity"].mean(), 1) if "humidity" in df.columns else None
    return temp_avg, rain_sum, wind_max, humid_avg

def fmt_time_local(ts) -> str:
    """Định dạng giờ theo ICT (UTC+7)."""
    ts = pd.to_datetime(ts, utc=True)
    return ts.astimezone(ICT).strftime("%H:%M ICT")

def fmt_date_local(ts) -> str:
    """Định dạng ngày dd/mm theo giờ địa phương để phát thanh viên đọc rõ."""
    ts = pd.to_datetime(ts, utc=True)
    return ts.astimezone(ICT).strftime("%d/%m")

def classify_rain_hourly(rain_mm: float) -> str:
    """Phân loại mưa theo lượng mưa giờ (mm)."""
    if rain_mm == 0:
        return "Không mưa"
    elif rain_mm < 1:
        return "Mưa rất nhẹ"
    elif rain_mm < RAIN_HOURLY_ALERT:
        return "Mưa nhẹ"
    elif rain_mm < 10:
        return "Mưa vừa"
    elif rain_mm < 30:
        return "Mưa to"
    else:
        return "Mưa rất to"

def classify_rain(total_mm: float) -> str:
    """Phân loại tổng lượng mưa theo ngày."""
    if total_mm == 0:
        return "không mưa"
    elif total_mm < RAIN_DAILY_ALERT:
        return "mưa ít"
    elif total_mm < 30:
        return "mưa vừa"
    elif total_mm < 60:
        return "mưa to"
    else:
        return "mưa rất to"

def classify_wind(speed: float) -> str:
    """Phân loại gió theo tốc độ m/s."""
    if speed < 3:
        return "gió nhẹ"
    elif speed < WIND_DAILY_ALERT:
        return "gió vừa"
    elif speed < WIND_HOURLY_ALERT:
        return "gió mạnh"
    else:
        return "gió giật rất mạnh"

def classify_temp(temp_c: float) -> str:
    """Phân loại nhiệt độ theo cảm nhận."""
    if temp_c < COLD_ALERT:
        return "lạnh"
    elif temp_c < 23:
        return "mát"
    elif temp_c < HEAT_ALERT:
        return "nóng"
    else:
        return "rất nóng"

# ==========================
# Nội suy dữ liệu 3h thành 24h
# ==========================
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
    start = df.index.min()
    end = df.index.max()
    if pd.isna(start) or pd.isna(end):
        raise ValueError("Không tìm thấy thời gian hợp lệ trong dữ liệu")

    start_day = start.normalize()
    end_day = end.normalize() + pd.Timedelta(hours=23)
    full_range = pd.date_range(start=start_day, end=end_day, freq="1h", tz="UTC")

    numeric_cols = [c for c in ["temp", "rain", "wind_speed", "humidity"] if c in df.columns]
    if not numeric_cols:
        raise ValueError("hourly_df thiếu các cột số bắt buộc: temp, rain, wind_speed")

    df_interp = df[numeric_cols].reindex(full_range)
    df_interp = df_interp.interpolate(method="linear").ffill().bfill()

    if "weather_desc" in df.columns:
        df_interp["weather_desc"] = df["weather_desc"].reindex(full_range, method="nearest")

    df_interp = df_interp.reset_index().rename(columns={"index": "ts"})
    return df_interp

# ==========================
# Sinh bản tin nâng cấp
# ==========================
def generate_bulletin(
    region_name: str,
    hourly_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    current: dict = None,
    group_hours: bool = False
) -> str:
    """
    Sinh bản tin thời tiết tự động theo dữ liệu thực tế.
    - Nếu hourly_df đã nội suy thì hiển thị đủ 24h.
    - group_hours=True: gom nhóm các giờ có cùng mô tả liên tiếp.
    - group_hours=False: hiển thị chi tiết từng giờ.
    - Có biểu tượng thời tiết trực quan, nổi bật và đồng bộ.
    """
    now_local = datetime.now(ICT)
    today = now_local.date()
    bulletin = []

    # ================== TIÊU ĐỀ ==================
    bulletin.append(f"🌤️✨ BẢN TIN DỰ BÁO THỜI TIẾT — {region_name}")
    bulletin.append(f"📅 Ngày: {today.strftime('%d/%m/%Y')}")
    bulletin.append(f"🕒 Cập nhật lúc: {now_local.strftime('%H:%M %Z')}\n")

    # ================== HIỆN TẠI ==================
    if isinstance(current, dict) and current:
        rain_val = None
        # Ưu tiên lấy từ current
        for key in ["rain_1h", "precipitation_1h", "rain", "precipitation", "rain_last_hour"]:
            val = current.get(key)
            if val is not None:
                try:
                    # Nếu val là dict (ví dụ {"1h":0.55}) thì lấy giá trị bên trong
                    if isinstance(val, dict) and "1h" in val:
                        rain_val = float(val["1h"])
                    else:
                        rain_val = float(val)
                except (TypeError, ValueError):
                    rain_val = None
                break

        # Fallback từ hourly gần nhất nếu current không có
        if rain_val is None and "ts" in hourly_df.columns:
            now_utc = datetime.now(timezone.utc)
            hourly_df2 = hourly_df.copy()
            hourly_df2["ts"] = pd.to_datetime(hourly_df2["ts"], errors="coerce", utc=True)
            hourly_df2["diff_min"] = (hourly_df2["ts"] - now_utc).abs().dt.total_seconds() / 60.0
            nearest = hourly_df2.sort_values("diff_min").head(1)
            if not nearest.empty and nearest.iloc[0]["diff_min"] <= 60:
                try:
                    val = nearest.iloc[0].get("rain", None)
                    rain_val = float(val) if val is not None else None
                except (TypeError, ValueError):
                    rain_val = None

        # Hiển thị lượng mưa
        rain_text = f"{rain_val:.2f} mm" if rain_val is not None else "Không rõ"

        # Lấy các thông số khác
        wind_now = float(current.get("wind_speed", 0) or 0.0)
        clouds_now = float(current.get("clouds", 0) or 0.0)
        temp_now = float(current.get("temp", 0.0) or 0.0)

        # Chọn icon theo logic mưa/gió/mây/nắng
        if rain_val > 0 and wind_now > 6:
            icon_now = "⛈️"
            weather_desc = "Mưa to kèm gió mạnh"
        elif rain_val > 0:
            if rain_val < 1:
                icon_now = "🌦️"; weather_desc = "Mưa rất nhẹ"
            elif rain_val < 5:
                icon_now = "🌧️"; weather_desc = "Mưa nhẹ"
            elif rain_val < 20:
                icon_now = "🌧️"; weather_desc = "Mưa vừa"
            else:
                icon_now = "⛈️"; weather_desc = "Mưa to"
        elif wind_now > 6:
             icon_now = "💨"; weather_desc = "Gió mạnh"
        elif clouds_now > 70:
             icon_now = "☁️"; weather_desc = "Nhiều mây"
        else:
            if temp_now >= 33:
                icon_now = "🔥"; weather_desc = "Nắng nóng gay gắt"
            elif temp_now >= 28:
                icon_now = "☀️"; weather_desc = "Nắng mạnh"
            elif temp_now >= 23:
                icon_now = "🌤️"; weather_desc = "Nắng nhẹ"
            else:
               icon_now = "☀️"; weather_desc = "Trời quang mát"

        # Thời gian hiển thị
        ts_local_dt = _to_ict(current.get("ts"))
        ts_date = (ts_local_dt.strftime("%d/%m/%Y") if ts_local_dt else today.strftime("%d/%m/%Y"))
        ts_time = (ts_local_dt.strftime("%H:%M ICT") if ts_local_dt else now_local.strftime("%H:%M ICT"))

        # Xuất bản tin HIỆN TẠI
        bulletin.append(f"{icon_now} HIỆN TẠI:")
        bulletin.append(f"   🗓 Ngày: {ts_date}")
        bulletin.append(f"   🕒 Giờ: {ts_time}")
        bulletin.append(f"   🌡️ Nhiệt độ: {current.get('temp','?')}°C")
        bulletin.append(f"   💧 Độ ẩm: {current.get('humidity','?')}%")
        bulletin.append(f"   📈 Áp suất: {current.get('pressure','?')} hPa")
        bulletin.append(f"   💨 Gió: {current.get('wind_speed','?')} m/s")
        bulletin.append(f"   ☁️ Mây: {current.get('clouds','?')}%")
        bulletin.append(f"   🌧️ Lượng mưa: {rain_text}")
        bulletin.append(f"   📝 Trạng thái: {weather_desc}")
        bulletin.append("")
    else:
        bulletin.append("ℹ️ Không có dữ liệu thời tiết hiện tại.")
        bulletin.append("")

    # ================== TỔNG QUAN TRONG NGÀY ==================
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

    def _safe_mean(df, col):
        return round(float(df[col].dropna().mean()), 1) if col in df.columns and not df[col].dropna().empty else 0.0
    def _safe_min(df, col):
        return round(float(df[col].dropna().min()), 1) if col in df.columns and not df[col].dropna().empty else 0.0
    def _safe_max(df, col):
        return round(float(df[col].dropna().max()), 1) if col in df.columns and not df[col].dropna().empty else 0.0
    def _safe_sum(df, col):
        return round(float(df[col].dropna().sum()), 1) if col in df.columns and not df[col].dropna().empty else 0.0

    avg_temp = _safe_mean(ref_df, "temp")
    min_temp = _safe_min(ref_df, "temp")
    max_temp = _safe_max(ref_df, "temp")
    total_rain = _safe_sum(ref_df, "rain")
    max_wind = _safe_max(ref_df, "wind_speed")
    avg_humidity = round(float(ref_df["humidity"].dropna().mean()), 1) if "humidity" in ref_df.columns and not ref_df["humidity"].dropna().empty else None

    bulletin.append("🔎 TỔNG QUAN TRONG NGÀY")
    bulletin.append(f"   🌡️ Trung bình: {avg_temp}°C (dao động {min_temp}–{max_temp}°C)")
    bulletin.append(f"   🌧️ Tổng mưa: {total_rain} mm ({classify_rain(total_rain)})")
    bulletin.append(f"   💨 Gió mạnh nhất: {max_wind} m/s ({classify_wind(max_wind)})")
    if avg_humidity is not None:
        bulletin.append(f"   💧 Độ ẩm trung bình: {avg_humidity}%")
    if total_rain > RAIN_DAILY_ALERT:
        bulletin.append(f"  ⚠️ Cảnh báo: Lượng mưa hôm nay đã đạt {total_rain:.1f} mm, nguy cơ ngập úng cục bộ.")
    if max_wind > WIND_DAILY_ALERT:
        bulletin.append(f"   ⚠️ Cảnh báo: Gió mạnh nhất ghi nhận {max_wind:.1f} m/s, cần chú ý an toàn.")
    bulletin.append("")

    # ================== DIỄN BIẾN THEO GIỜ (24H) ==================
    bulletin.append("⏰ DIỄN BIẾN THEO GIỜ (24h)")
    # Ép khung 24h hôm nay theo ICT
    start_ict = datetime.combine(today, datetime.min.time(), tzinfo=ICT)
    end_ict = start_ict + timedelta(hours=23)
    full_ict_range = pd.date_range(start=start_ict, end=end_ict, freq="1h", tz=ICT)
    full_utc_range = full_ict_range.tz_convert(timezone.utc)

    cols_keep = [c for c in ["temp","rain","wind_speed","humidity","weather_desc"] if c in hourly_df.columns]
    h24 = (hourly_df.set_index("ts")
           .sort_index()[cols_keep]
           .reindex(full_utc_range))
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
        if group_hours:
            # Gom nhóm giờ có cùng mô tả liên tiếp
            group = []
            prev_desc = None
            for row in rows:
                desc = _safe_desc(row.get("weather_desc", ""))
                if desc != prev_desc and group:
                    start = group[0]["ts_local"].strftime("%H:%M")
                    end = group[-1]["ts_local"].strftime("%H:%M")
                    avg_temp_g = round(pd.Series([r.get("temp", None) for r in group]).dropna().mean(), 1) if group else 0.0
                    avg_rain_g = round(pd.Series([r.get("rain", None) for r in group]).dropna().mean(), 1) if group else 0.0
                    bulletin.append(f"   🌡️ {start}–{end}: {avg_temp_g}°C, {prev_desc or 'không rõ'}, mưa TB {avg_rain_g} mm")
                    group = []
                group.append(row)
                prev_desc = desc
            if group:
                start = group[0]["ts_local"].strftime("%H:%M")
                end = group[-1]["ts_local"].strftime("%H:%M")
                avg_temp_g = round(pd.Series([r.get("temp", None) for r in group]).dropna().mean(), 1) if group else 0.0
                avg_rain_g = round(pd.Series([r.get("rain", None) for r in group]).dropna().mean(), 1) if group else 0.0
                bulletin.append(f"   🌡️ {start}–{end}: {avg_temp_g}°C, {prev_desc or 'không rõ'}, mưa TB {avg_rain_g} mm")
        else:
            # Hiển thị chi tiết từng giờ
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

                # Logic icon tổng hợp: mưa, gió, mây, nắng
                if rain > 0 and wind > 6:
                    icon = "⛈️"
                elif rain > 0:
                    icon = weather_icon
                elif wind > 6:
                    icon = "💨"
                elif "mây" in desc:
                    icon = "☁️"
                else:
                    # Thêm phân loại nắng theo nhiệt độ
                    if temp >= 33:
                        icon = "🔥"   # Nắng gắt
                        weather_desc = "Nắng nóng gay gắt"
                    elif temp >= 28:
                        icon = "☀️"   # Nắng mạnh
                        weather_desc = "Nắng mạnh"
                    elif temp >= 23:
                        icon = "🌤️"   # Nắng nhẹ, có thể kèm ít mây
                        weather_desc = "Nắng nhẹ"
                    else:
                        icon = "☀️"   # Trời quang mát
                        weather_desc = "Trời quang"

                bulletin.append(
                    f"   {icon} {ts_date} {ts_time} → 🌡️ {temp}°C | 💨 {wind} m/s ({wind_desc}) | 🌧️ {rain} mm → {weather_desc}"
                )
                
     # Xu hướng mưa: dựa trên tổng lượng mưa thực tế
    if total_rain > 30:
        bulletin.append(f"   👉 Xu hướng: Lượng mưa hôm nay đã đạt {total_rain:.1f} mm, có nguy cơ mưa kéo dài.")

    # Xu hướng gió: dựa trên tốc độ gió mạnh nhất
    if max_wind > WIND_DAILY_ALERT:
        bulletin.append(f"   👉 Xu hướng: Gió mạnh nhất ghi nhận {max_wind:.1f} m/s, cần chú ý an toàn.")

    bulletin.append("")

    # ================== XU HƯỚNG 10 NGÀY ==================
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
            if rain_d > 0 and wind_d > 6.0:   # gió mạnh > 6 m/s
                icon_day = "⛈️"
            elif rain_d > 0:                  # có mưa nhưng gió không mạnh
                icon_day = "🌧️"
            elif hum_d is not None and hum_d > 85.0:   # độ ẩm cao > 85%
                icon_day = "☁️"
            else:
                icon_day = "☀️"

            bulletin.append(f"{icon_day} {date_txt} → 🌡️ {temp_min}–{temp_max}°C | 🌧️ {rain_d} mm{hum_txt}{wind_txt}")

            # Nhận định riêng cho từng ngày
            notes = []

            # Mưa
            if rain_d >= 20.0:   # ngưỡng mưa lớn
                notes.append(f"⚠️ {date_txt}: Mưa lớn {rain_d:.1f} mm")
            elif 0 < rain_d < 20.0:
                notes.append(f"ℹ️ {date_txt}: Có mưa nhẹ {rain_d:.1f} mm")

            # Gió
            if wind_d >= 6.0:    # ngưỡng gió mạnh
                notes.append(f"⚠️ {date_txt}: Gió mạnh {wind_d:.1f} m/s")

            # Độ ẩm
            if hum_d is not None and hum_d >= 85.0:   # ngưỡng ẩm cao
                notes.append(f"ℹ️ {date_txt}: Độ ẩm cao {hum_d:.0f}%")

            # Nhiệt độ
            if isinstance(temp_max, (int, float)) and temp_max >= 32.0:   # ngưỡng nắng nóng
                notes.append(f"⚠️ {date_txt}: Nắng nóng (max {temp_max}°C)")
            elif isinstance(temp_min, (int, float)) and temp_min <= 20.0: # ngưỡng trời mát/lạnh
                notes.append(f"ℹ️ {date_txt}: Trời mát/lạnh (min {temp_min}°C)")

            # Đưa các nhận định vào bulletin
            for n in notes:
                bulletin.append("   ↪ " + n)

            # Thống kê xu hướng tổng thể
            if rain_d > RAIN_DAILY_ALERT:
                rain_days += 1
            if rain_d > max_rain:
                max_rain = rain_d
                max_rain_date = date_txt
        
        # Phân tích xu hướng tổng thể
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

        bulletin.append("")
        return "\n".join(bulletin)