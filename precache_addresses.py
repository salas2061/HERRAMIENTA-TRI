import os
import json
import time
import requests
import pandas as pd

# -------------------------
# Configuración
# -------------------------
import os
BASE_DIR = os.path.dirname(__file__)
EXCEL_FILE = os.path.join(BASE_DIR, "data", "Mapa Geoespacial ATM (1) (1).xlsx")
OUTPUT_CACHE = os.path.join(BASE_DIR, "address_cache.json")

COL_LAT = "LATITUD"
COL_LON = "LONGITUD"
# -------------------------
# Cargar Excel
# -------------------------
df = pd.read_excel(EXCEL_FILE)

# -------------------------
# Cargar cache previo (si existe)
# -------------------------
if os.path.exists(OUTPUT_CACHE):
    with open(OUTPUT_CACHE, "r", encoding="utf-8") as f:
        address_cache = json.load(f)
else:
    address_cache = {}

# -------------------------
# Función para consultar Nominatim
# -------------------------
def fetch_address(lat, lon):
    url = f"https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "json", "zoom": 16}
    try:
        r = requests.get(url, params=params, headers={"User-Agent": "GeoApp/1.0"})
        if r.status_code == 200:
            data = r.json()
            return data.get("display_name", "Sin dirección")
    except Exception as e:
        print("❌ Error:", e)
    return "Sin dirección"

# -------------------------
# Recorrer registros
# -------------------------
for _, row in df.iterrows():
    lat, lon = row[COL_LAT], row[COL_LON]
    key = f"{lat},{lon}"

    if key not in address_cache:  # Solo si no está guardado
        print(f"🌍 Consultando {key} ...")
        address_cache[key] = fetch_address(lat, lon)
        time.sleep(1)  # ⚠️ Espera para no ser bloqueado

# -------------------------
# Guardar cache
# -------------------------
with open(OUTPUT_CACHE, "w", encoding="utf-8") as f:
    json.dump(address_cache, f, ensure_ascii=False, indent=2)

print(f"✅ Cache actualizado en {OUTPUT_CACHE} con {len(address_cache)} direcciones")