# services/openweather.py

import os
from weatherpro.sources import WeatherSource  # hoặc đường dẫn đúng đến class WeatherSource

OWM = WeatherSource(
    name="openweather",
    base_url="https://api.openweathermap.org/data/2.5",
    api_key=os.getenv("OWM_API_KEY")
)