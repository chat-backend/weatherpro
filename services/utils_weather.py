# services/utils_weather.py
import pandas as pd
from datetime import datetime, timezone

def extract_rain(obj) -> float | None:
    """
    Trả về lượng mưa (mm) từ một dict bất kỳ của OWM: current/hourly entry.
    Hỗ trợ cả dạng float/int và dict {"1h": ...}.
    """
    if obj is None:
        return None

    if isinstance(obj, dict):
        val = obj.get("rain")
        if isinstance(val, dict):
            if "1h" in val and isinstance(val["1h"], (int, float)):
                return float(val["1h"])
            if "3h" in val and isinstance(val["3h"], (int, float)):
                return float(val["3h"]) / 3.0
        elif isinstance(val, (int, float)):
            return float(val)

        for k in ["precipitation", "rain_1h", "precipitation_1h", "rain_last_hour"]:
            v = obj.get(k)
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, dict) and "1h" in v and isinstance(v["1h"], (int, float)):
                return float(v["1h"])

    if isinstance(obj, (int, float)):
        return float(obj)

    return None


def get_rain_value(current: dict, hourly_df: pd.DataFrame) -> float | None:
    """
    Lấy lượng mưa hợp lý nhất từ current và hourly.
    Ưu tiên số liệu thực tế (current), nếu không có thì dùng dự báo (hourly gần nhất).
    """
    rain_val = extract_rain(current)

    # Nếu trạng thái thời tiết ghi nhận mưa nhưng chưa có số liệu
    if (rain_val is None or rain_val == 0.0) and isinstance(current, dict) and "weather" in current and current["weather"]:
        desc_main = str(current["weather"][0].get("main", "")).lower()
        desc_text = str(current["weather"][0].get("description", "")).lower()
        if ("rain" in desc_main) or ("rain" in desc_text) or ("shower" in desc_text):
            rain_val = 0.3  # giả định mưa nhẹ tối thiểu

    # Fallback từ hourly gần nhất
    if (rain_val is None or rain_val == 0.0) and hourly_df is not None and not hourly_df.empty:
        df = hourly_df.copy()
        if "dt" in df.columns:
            df["ts_utc"] = pd.to_datetime(df["dt"], unit="s", utc=True)
        elif "ts" in df.columns:
            df["ts_utc"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
        else:
            return rain_val

        now_utc = datetime.now(timezone.utc)
        df["diff_min"] = (df["ts_utc"] - now_utc).abs().dt.total_seconds() / 60.0
        nearest = df.sort_values("diff_min").head(1)
        if not nearest.empty and nearest.iloc[0]["diff_min"] <= 60:
            rain_val = extract_rain(nearest.iloc[0].to_dict())

    return rain_val

def format_rain_value(rain_val: float | None) -> str:
    """
    Format lượng mưa để trả về cho frontend.
    - Nếu None → "Không rõ"
    - Nếu số → "{:.2f} mm"
    """
    if rain_val is None:
        return "Không rõ"
    try:
        return f"{float(rain_val):.2f} mm"
    except (TypeError, ValueError):
        return "Không rõ"

def safe_str(value) -> str:
    """
    Chuẩn hóa mọi giá trị thành chuỗi an toàn cho frontend.
    - Nếu None hoặc rỗng → trả về "" (chuỗi rỗng).
    - Nếu là dict/list → ép thành chuỗi JSON-like.
    - Nếu là số → ép thành chuỗi.
    - Nếu là chuỗi → giữ nguyên.
    """
    if value is None:
        return ""
    try:
        # Nếu là pandas DataFrame hoặc Series → ép sang dict trước
        if isinstance(value, (pd.DataFrame, pd.Series)):
            return str(value.to_dict())
        return str(value)
    except Exception:
        return ""