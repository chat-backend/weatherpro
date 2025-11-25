# api/app.py
import logging
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from api.weather_services import RegionIndex

# ==============================
# Logging setup
# ==============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WeatherPro")

# ==============================
# Load environment variables
# ==============================
load_dotenv()

# ==============================
# FastAPI app initialization
# ==============================
app = FastAPI(
    title="WeatherPro Vietnam",
    description="Weather data aggregation service for Vietnam regions",
    version="1.0.0"
)

# ==============================
# CORS setup
# ==============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ⚠️ Dev mode, nên giới hạn domain khi deploy
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================
# Load region indexes (absolute paths)
# ==============================
BASE_DIR = Path(__file__).resolve().parent.parent
regions_path = (BASE_DIR / "configs" / "vietnam_provinces.geojson").resolve()
wards_path   = (BASE_DIR / "configs" / "danang_wards.geojson").resolve()

def _load_index(path: Path, label: str):
    try:
        if not path.exists():
            raise FileNotFoundError(f"{label} not found at {path}")
        idx = RegionIndex(str(path))
        features = getattr(idx, "features", [])
        logger.info(f"✅ Loaded {label} from {path} with {len(features)} entries")
        return idx
    except Exception as e:
        logger.error(f"❌ Failed to load {label} from {path}: {e}")
        return None

regions = _load_index(regions_path, "Vietnam provinces")
wards   = _load_index(wards_path, "Danang wards")

# ==============================
# Health check route
# ==============================
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "provinces_loaded": None if regions is None else len(regions.features),
        "wards_loaded": None if wards is None else len(wards.features),
        "provinces_path": str(regions_path),
        "wards_path": str(wards_path),
        "primary_source": "WeatherAPI",
        "secondary_source": "OpenMeteo",
        "fallback_source": "OpenWeather"
    }

# ==============================
# Import routes
# ==============================
from api import routes
app.include_router(routes.router)