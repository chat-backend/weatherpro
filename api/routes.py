# api/routes.py
import logging
import pandas as pd
from fastapi import APIRouter, Query

from services.openweather import OWM
from services.weatherapi import WeatherAPI
from services.openmeteo import OpenMeteo
from api.weather_services import weighted_ensemble
from api.app_utils import _collect_sources, strip_accents, find_region
from api.app import regions, wards
from services.bulletin import generate_bulletin

router = APIRouter()
logger = logging.getLogger("WeatherPro")

@router.get("/v1/chat")
def chat(region: str, group_hours: bool = Query(False)):
    if regions is None:
        return {"error": "Không load được provinces.geojson"}
    if wards is None:
        return {"error": "Không load được danang_wards.geojson"}

    region_norm = strip_accents(region.lower())
    match, source = find_region(region_norm, regions, "province")
    if not match:
        match, source = find_region(region_norm, wards, "ward")
    if not match:
        return {"error": f"Không tìm thấy địa danh: {region}"}

    lon, lat = match["geometry"]["coordinates"]
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return {"error": "Tọa độ không hợp lệ trong GeoJSON."}

    try:
        sources = _collect_sources(lat, lon)
        originals = sources["originals"]

        wa_hourly   = originals["wa"]["hourly"]
        wa_daily    = originals["wa"]["daily"]
        wa_current  = originals["wa"]["current"]

        om_hourly   = originals["om"]["hourly"]
        om_daily    = originals["om"]["daily"]
        om_current  = originals["om"]["current"]

        owm_hourly  = originals["owm"]["hourly"]
        owm_daily   = originals["owm"]["daily"]
        owm_current = originals["owm"]["current"]

        hourly_raw = wa_hourly if isinstance(wa_hourly, pd.DataFrame) and not wa_hourly.empty else \
                     om_hourly if isinstance(om_hourly, pd.DataFrame) and not om_hourly.empty else owm_hourly

        daily_raw  = wa_daily if isinstance(wa_daily, pd.DataFrame) and not wa_daily.empty else \
                     om_daily if isinstance(om_daily, pd.DataFrame) and not om_daily.empty else owm_daily

        if wa_current and isinstance(wa_current, dict) and wa_current:
            current_raw = wa_current
        elif om_current and isinstance(om_current, dict) and om_current:
            current_raw = om_current
        elif owm_current and isinstance(owm_current, dict) and owm_current:
            current_raw = owm_current
        else:
            current_raw = {}

        sources_hourly = [df for df in [wa_hourly, om_hourly, owm_hourly] if isinstance(df, pd.DataFrame) and not df.empty]
        sources_daily  = [df for df in [wa_daily, om_daily, owm_daily] if isinstance(df, pd.DataFrame) and not df.empty]

        if sources_hourly:
            hourly_raw = weighted_ensemble(sources_hourly)
        if sources_daily:
            daily_raw = weighted_ensemble(sources_daily)

        hourly_df = hourly_raw if isinstance(hourly_raw, pd.DataFrame) else pd.DataFrame()
        daily_df  = daily_raw if isinstance(daily_raw, pd.DataFrame) else pd.DataFrame()
        current   = current_raw if isinstance(current_raw, dict) else {}

        # ✅ Trả về trực tiếp dict từ generate_bulletin
        bulletin_json = generate_bulletin(
            region_name=match["properties"]["name"],
            hourly_df=hourly_df,
            daily_df=daily_df,
            current=current,
            source=source,
            group_hours=group_hours
        )
        return bulletin_json

    except Exception:
        logger.exception("Error in chat, fallback to OWM")
        hourly = OWM.fetch_hourly(lat, lon, hours=24)
        daily = OWM.fetch_daily(lat, lon, days=10)
        current = OWM.fetch_current(lat, lon)

        hourly_df = hourly if isinstance(hourly, pd.DataFrame) else pd.DataFrame()
        daily_df  = daily if isinstance(daily, pd.DataFrame) else pd.DataFrame()
        current   = current if isinstance(current, dict) else {}

    bulletin_json = generate_bulletin(
        region_name=match["properties"]["name"],
        hourly_df=hourly_df,
        daily_df=daily_df,
        current=current,
        source=source,
        group_hours=group_hours
    )

    return bulletin_json

