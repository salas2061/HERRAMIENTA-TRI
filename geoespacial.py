# ============================================================
#   BACKEND COMPLETO + LOGIN + SELECTOR
#   CAPAS: ISLAS (ATMs) + AGENTES + OFICINAS + INTEGRAL
#   ✅ + ZONAS RURAL / URBANA (ZONAS.xlsx) con BORDE NEÓN
#      - Rural: verde fosforescente
#      - Urbana: amarillo fosforescente
#      - Se actualiza con filtros y funciona en las 4 capas
#   ✅ + NODOS (NODOS1.xlsx) -> ÍCONO ROJO (pin) + CLICK muestra globo con NOMBRE
#      - Panel Comercial (derecha): ✅ MANTIENE CONTEO POR CATEGORÍAS (hospitales, clínicas, etc.)
#        usando PRINCIPALMENTE la columna TIPO (Excel) (fallback por NOMBRE si TIPO vacío)
#      - ✅ ELIMINADO: "Detalle por TIPO (Excel)" (lista dinámica)
#      - Se actualiza con filtros y funciona en las 4 capas
#   ✅ FIX NUEVO (POIs): si el NOMBRE NO indica el tipo, se muestra abajo: (TIPO)
#      Tipos esperados: SODIMAC, UNIVERSIDAD, PLAZA VEA, METRO, WONG, TOTTUS,
#                       CLINICA, HOSPITAL, CENTRO COMERCIAL, MERCADO
# ============================================================
import hashlib
import os
import re
import unicodedata
import json
import pandas as pd
import numpy as np
from flask import (
    Flask,
    render_template_string,
    request,
    jsonify,
    redirect,
    url_for,
    session,
)
from functools import wraps

# ============================================
# RECOMENDACIONES – CARGA BÁSICA
# ============================================
try:
    recomendaciones = pd.read_csv("data/recomendaciones.csv")
except Exception as e:
    print("⚠ No se pudo cargar recomendaciones.csv:", e)
    recomendaciones = pd.DataFrame()

# ============================================================
# NUEVO — Cargar base de clientes
# ============================================================
df_clientes = pd.read_csv("data/clientes_huanuco_v6.csv")
df_clientes = df_clientes[
    df_clientes["latitud"].notnull() &
    df_clientes["longitud"].notnull()
]
SEGMENTOS_CLIENTES = sorted(df_clientes["segmento"].dropna().astype(str).unique().tolist())

# ============================================================
# 1. CACHE DE DIRECCIONES
# ============================================================
CACHE_FILE = "address_cache.json"
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        address_cache = json.load(f)
else:
    address_cache = {}

def get_address(lat, lon):
    try:
        key = f"{float(lat):.6f},{float(lon):.6f}"
    except Exception:
        key = f"{lat},{lon}"
    return address_cache.get(key, "Dirección no encontrada")

# ============================================================
# HELPERS
# ============================================================
def normalize_col(s):
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("utf-8")
    s = s.upper().strip()
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def clean_str(x):
    return str(x).upper().strip() if pd.notnull(x) else ""

def parse_percent_series(s):
    """
    Convierte series tipo '84.24%' o 0.8424 a un número 0..100.
    """
    if s is None:
        return pd.Series([0.0])
    ss = s.astype(str).str.replace(",", ".", regex=False)
    ss = ss.str.replace("%", "", regex=False)
    ss = ss.str.replace(r"[^\d\.\-]", "", regex=True)
    v = pd.to_numeric(ss, errors="coerce").fillna(0.0)
    if len(v) and v.max() <= 1.0:
        v = v * 100.0
    return v


def parse_number_series(s):
    """
    Convierte strings tipo '3.406.481' o '-665.254' o '1,234.50' a número.
    - Detecta '.' como miles (incluye 1 solo punto si es formato miles).
    - Detecta ',' como decimal cuando corresponde.
    """
    if s is None:
        return pd.Series([0.0])

    ss = s.astype(str).str.strip()
    ss = ss.str.replace(r"[^\d\-\.,]", "", regex=True)

    has_dot = ss.str.contains(r"\.", regex=True)
    has_comma = ss.str.contains(",", regex=False)

    ss2 = ss.copy()

    # Caso 1: formato miles con puntos (1 o más):  -665.254  /  3.406.481
    mask_thousands_dot = (~has_comma) & ss2.str.match(r"^\-?\d{1,3}(\.\d{3})+$")
    ss2.loc[mask_thousands_dot] = ss2.loc[mask_thousands_dot].str.replace(".", "", regex=False)

    # Caso 2: ambos: '.' miles y ',' decimal
    mask_both = has_dot & has_comma
    ss2.loc[mask_both] = (
        ss2.loc[mask_both].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    )

    # Caso 3: solo ',' => decimal
    mask_only_comma = (~mask_both) & (~has_dot) & has_comma
    ss2.loc[mask_only_comma] = ss2.loc[mask_only_comma].str.replace(",", ".", regex=False)

    return pd.to_numeric(ss2, errors="coerce").fillna(0.0)

def norm_txt(s):
    """
    Normaliza texto para clasificación (quita tildes, uppercase, espacios).
    """
    s = unicodedata.normalize("NFKD", str(s))
    s = s.encode("ascii", "ignore").decode("utf-8")
    s = s.upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s

# --- (fallback) por nombre, si no hubiera TIPO ---
def nodo_categoria_por_nombre(nombre: str) -> str:
    n = norm_txt(nombre)

    # cadenas / marcas
    if "PLAZA VEA" in n or "PLAZAVEA" in n:
        return "PLAZA_VEA"
    if "SODIMAC" in n:
        return "SODIMAC"
    if re.search(r"\bMETRO\b", n):
        return "METRO"
    if "TOTTUS" in n:
        return "TOTTUS"
    if "WONG" in n:
        return "WONG"

    # generales
    if re.search(r"\bHOSPITAL(ES)?\b", n):
        return "HOSPITAL"
    if re.search(r"\bCLINIC(A|AS|O|OS)\b", n):
        return "CLINICA"
    if "UNIVERSIDAD" in n or re.search(r"\bUNIV\b", n):
        return "UNIVERSIDAD"
    if re.search(r"\bMERCAD(O|OS)\b", n) or "MARKET" in n or "FERIA" in n:
        return "MERCADO"

    # centros comerciales (muchas variantes)
    if (
        "CENTRO COMERCIAL" in n
        or "CENTROCOMERCIAL" in n
        or re.search(r"\bC\.?\s*C\.?\b", n)          # C.C.
        or re.search(r"\bC\.?\s*COMERCIAL\b", n)     # C. COMERCIAL
        or re.search(r"\bC\s*COMERCIAL\b", n)
        or "MALL" in n
        or "SHOPPING" in n
    ):
        return "CENTRO_COMERCIAL"

    return "OTRO"

def nodo_categoria_desde_tipo(tipo_excel: str, nombre_fallback: str = "") -> str:
    """
    ✅ Clasifica NODOS1.xlsx usando la COLUMNA TIPO (principal).
    Si TIPO está vacío, usa fallback por NOMBRE.
    Devuelve categorías usadas por el Panel Comercial.
    """
    t = norm_txt(tipo_excel)
    if not t:
        return nodo_categoria_por_nombre(nombre_fallback)

    t2 = t.replace("_", " ").replace("-", " ")
    t2 = re.sub(r"\s+", " ", t2).strip()

    # marcas/cadenas
    if "PLAZA VEA" in t2 or "PLAZAVEA" in t2:
        return "PLAZA_VEA"
    if "SODIMAC" in t2:
        return "SODIMAC"
    if re.search(r"\bMETRO\b", t2):
        return "METRO"
    if "TOTTUS" in t2:
        return "TOTTUS"
    if "WONG" in t2:
        return "WONG"

    # generales
    if re.search(r"\bHOSPITAL(ES)?\b", t2):
        return "HOSPITAL"
    if re.search(r"\bCLINIC(A|AS|O|OS)\b", t2):
        return "CLINICA"
    if "UNIVERSIDAD" in t2 or re.search(r"\bUNIV\b", t2):
        return "UNIVERSIDAD"
    if re.search(r"\bMERCAD(O|OS)\b", t2) or "MARKET" in t2 or "FERIA" in t2:
        return "MERCADO"

    # centros comerciales (variantes)
    if (
        "CENTRO COMERCIAL" in t2
        or "CENTROCOMERCIAL" in t2
        or re.search(r"\bC\.?\s*C\.?\b", t2)
        or re.search(r"\bC\.?\s*COMERCIAL\b", t2)
        or re.search(r"\bC\s*COMERCIAL\b", t2)
        or "MALL" in t2
        or "SHOPPING" in t2
    ):
        return "CENTRO_COMERCIAL"

    # si el TIPO trae algo raro/no estandar: fallback por nombre
    return nodo_categoria_por_nombre(nombre_fallback)

# --- para mostrar el TIPO EXACTO (10 tipos) en el popup cuando el nombre no lo indica ---
def tipo10_label_from_categoria(cat: str) -> str:
    cat = (cat or "").upper().strip()
    if cat == "PLAZA_VEA":
        return "PLAZA VEA"
    if cat == "CENTRO_COMERCIAL":
        return "CENTRO COMERCIAL"
    if cat == "HOSPITAL":
        return "HOSPITAL"
    if cat == "CLINICA":
        return "CLINICA"
    if cat == "SODIMAC":
        return "SODIMAC"
    if cat == "METRO":
        return "METRO"
    if cat == "TOTTUS":
        return "TOTTUS"
    if cat == "WONG":
        return "WONG"
    if cat == "UNIVERSIDAD":
        return "UNIVERSIDAD"
    if cat == "MERCADO":
        return "MERCADO"
    return "OTRO"

def nombre_indica_tipo(nombre: str) -> bool:
    n = norm_txt(nombre)
    if not n:
        return False
    if "PLAZA VEA" in n or "PLAZAVEA" in n:
        return True
    if "SODIMAC" in n:
        return True
    if re.search(r"\bMETRO\b", n):
        return True
    if "TOTTUS" in n:
        return True
    if "WONG" in n:
        return True
    if re.search(r"\bHOSPITAL(ES)?\b", n):
        return True
    if re.search(r"\bCLINIC(A|AS|O|OS)\b", n):
        return True
    if "UNIVERSIDAD" in n or re.search(r"\bUNIV\b", n):
        return True
    if re.search(r"\bMERCAD(O|OS)\b", n) or "MARKET" in n or "FERIA" in n:
        return True
    if (
        "CENTRO COMERCIAL" in n
        or "CENTROCOMERCIAL" in n
        or re.search(r"\bC\.?\s*C\.?\b", n)
        or re.search(r"\bC\.?\s*COMERCIAL\b", n)
        or "MALL" in n
        or "SHOPPING" in n
    ):
        return True
    return False

def nombre_popup_con_tipo(nombre: str, tipo10: str) -> str:
    nombre = (nombre or "").strip()
    tipo10 = (tipo10 or "").strip().upper()
    if not nombre:
        return f"({tipo10})" if tipo10 and tipo10 != "OTRO" else ""
    if tipo10 and tipo10 != "OTRO" and (not nombre_indica_tipo(nombre)):
        # ✅ 2 líneas: NOMBRE \n (TIPO)
        return f"{nombre}\n({tipo10})"
    return nombre

# ============================================================
# 2. CARGAR EXCEL PRINCIPAL (ISLAS / ATMs)
# ============================================================
BASE_DIR = os.path.dirname(__file__)
excel_main = os.path.join(BASE_DIR, "data", "Mapa Geoespacial ATM (1) (1).xlsx")
if not os.path.exists(excel_main):
    raise FileNotFoundError("No encontré archivo Excel de ATMs.")

raw = pd.read_excel(excel_main)
norm_map = {normalize_col(c): c for c in raw.columns}

def find_col(keys):
    for norm, orig in norm_map.items():
        for k in keys:
            if k in norm:
                return orig
    return None

COL_ATM  = find_col(["COD_ATM", "ATM"]) or "ATM"
COL_NAME = find_col(["NOMBRE", "CAJERO"]) or None
COL_DEPT = find_col(["DEPARTAMENTO"]) or "DEPARTAMENTO"
COL_PROV = find_col(["PROVINCIA"]) or "PROVINCIA"
COL_DIST = find_col(["DISTRITO"]) or "DISTRITO"
COL_LAT  = find_col(["LATITUD", "LAT"]) or "LATITUD"
COL_LON  = find_col(["LONGITUD", "LON"]) or "LONGITUD"
COL_DIV  = find_col(["DIVISION", "DIVISIÓN"]) or "DIVISIÓN"
COL_TIPO = find_col(["TIPO"]) or "TIPO"
COL_UBIC = find_col(["UBICACION", "UBICACIÓN", "UBICACION INTERNA"]) or "UBICACION_INTERNA"
PROM_COL = find_col(["PROMEDIO", "PROM"]) or None

if PROM_COL is None:
    raw["PROM_FAKE"] = 0.0
    PROM_COL = "PROM_FAKE"

for c in [COL_ATM, COL_DEPT, COL_PROV, COL_DIST, COL_LAT, COL_LON, COL_DIV, COL_TIPO, COL_UBIC, PROM_COL]:
    if c not in raw.columns:
        raw[c] = ""

df = raw.copy()

df[COL_LAT] = (
    df[COL_LAT].astype(str)
    .str.replace(",", ".", regex=False)
    .str.replace(r"[^\d\.\-]", "", regex=True)
    .replace("", np.nan)
    .astype(float)
)
df[COL_LON] = (
    df[COL_LON].astype(str)
    .str.replace(",", ".", regex=False)
    .str.replace(r"[^\d\.\-]", "", regex=True)
    .replace("", np.nan)
    .astype(float)
)
df = df.dropna(subset=[COL_LAT, COL_LON]).reset_index(drop=True)

df[PROM_COL] = pd.to_numeric(df[PROM_COL], errors="coerce").fillna(0.0)
df[COL_TIPO] = df[COL_TIPO].astype(str).fillna("")
df[COL_UBIC] = df[COL_UBIC].astype(str).fillna("")




# ============================================================
# NUEVO — CARGAR EXCEL COMERCIOS (cant_clientes) ✅
#   - Para "Heatmap Cantidad de clientes"
#   - Sin romper si el archivo no existe (queda desactivado)
# ============================================================
def _pick_first_existing(paths):
    for p in paths:
        if p and os.path.exists(p):
            return p
    return ""

excel_comercios = _pick_first_existing([
    os.path.join(BASE_DIR, "data", "dfcomercios_top_final1.xlsx"),
    os.path.join(BASE_DIR, "data", "dfcomercios_top_final1.xls"),
    os.path.join(BASE_DIR, "data", "dfcomercios_top_final1.csv"),
    os.path.join(BASE_DIR, "data", "dfcomercios_top_final.xlsx"),
    os.path.join(BASE_DIR, "data", "dfcomercios_top_final.csv"),
])

df_comercios = pd.DataFrame(columns=[
    "ID_COMERCIO",
    "DEPARTAMENTO","PROVINCIA","DISTRITO",
    "LATITUD","LONGITUD",
    "CANT_CLIENTES"
])
if excel_comercios:
    try:
        if excel_comercios.lower().endswith(".csv"):
            raw_c = pd.read_csv(excel_comercios)
        else:
            raw_c = pd.read_excel(excel_comercios)

        norm_map_c = {normalize_col(c): c for c in raw_c.columns}
        def find_col_c(keys):
            for norm, orig in norm_map_c.items():
                for k in keys:
                    if normalize_col(k) in norm:   # 👈 clave
                        return orig
            return None

        COLC_DEP  = find_col_c(["DEPARTAMENTO"]) or "DEPARTAMENTO"
        COLC_PROV = find_col_c(["PROVINCIA"]) or "PROVINCIA"
        COLC_DIST = find_col_c(["DISTRITO"]) or "DISTRITO"
        COLC_RUC  = find_col_c(["RUC"]) or "RUC"
        COLC_RS   = find_col_c(["RAZON", "RAZON SOCIAL", "RAZONSOCIAL", "RAZON_SOCIAL", "NOMBRE"]) or "RAZON_SOCIAL"
        COLC_DIR  = find_col_c(["DIRECCION", "DIRECCIÓN", "DIR", "ADDRESS"]) or "DIRECCION"


        COLC_ID   = (
            find_col_c(["COMMERCE_ID", "ID_COMERCIO", "ID COMERCIO", "COMERCIO_ID", "COMERCIO ID"])
            or "ID_COMERCIO"
        )
        COLC_LAT  = find_col_c(["LATITUD", "LAT"]) or "LATITUD"
        COLC_LON  = find_col_c(["LONGITUD", "LON"]) or "LONGITUD"
        COLC_CANT = (
            find_col_c(["CANT_CLIENTES", "CANT CLIENTES", "CANTCLIENTES", "CANT_CLIENTE", "CANT CLIENTE", "CANTCLIENTE"])
            or "CANT_CLIENTES"
        )

        for c in [COLC_ID, COLC_DEP, COLC_PROV, COLC_DIST, COLC_LAT, COLC_LON, COLC_CANT, COLC_RUC, COLC_RS, COLC_DIR]:
            if c not in raw_c.columns:
                raw_c[c] = ""

        raw_c[COLC_DEP]  = raw_c[COLC_DEP].apply(clean_str)
        raw_c[COLC_PROV] = raw_c[COLC_PROV].apply(clean_str)
        raw_c[COLC_DIST] = raw_c[COLC_DIST].apply(clean_str)

        raw_c[COLC_LAT] = (
            raw_c[COLC_LAT].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )
        raw_c[COLC_LON] = (
            raw_c[COLC_LON].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )

        raw_c[COLC_CANT] = pd.to_numeric(
            raw_c[COLC_CANT].astype(str).str.replace(",", ".", regex=False),
            errors="coerce"
        ).fillna(0.0)

        df_comercios = raw_c.dropna(subset=[COLC_LAT, COLC_LON]).copy()

        df_comercios = df_comercios.rename(columns={
            COLC_ID:   "ID_COMERCIO",
            COLC_DEP:  "DEPARTAMENTO",
            COLC_PROV: "PROVINCIA",
            COLC_DIST: "DISTRITO",
            COLC_LAT:  "LATITUD",
            COLC_LON:  "LONGITUD",
            COLC_CANT: "CANT_CLIENTES",
            COLC_RUC:  "RUC",
            COLC_RS:   "RAZON_SOCIAL",
            COLC_DIR:  "DIRECCION",
        })

        df_comercios = df_comercios[[
            "ID_COMERCIO",
            "DEPARTAMENTO","PROVINCIA","DISTRITO",
            "LATITUD","LONGITUD",
            "CANT_CLIENTES",
            "RUC","RAZON_SOCIAL","DIRECCION"
        ]].copy()


        print(f"✅ Comercios cargados (cant_clientes): {len(df_comercios)} filas ({excel_comercios})")
    except Exception as e:
        print("⚠ No se pudo cargar el Excel de comercios (cant_clientes):", e)
else:
    print("⚠ No existe Excel de comercios (cant_clientes) — heatmap cantidad clientes desactivado.")


# ============================================================
# 2B. CARGAR BASE DE EMPRESAS NÓMINA ✅
#   - Nueva capa comercial / empresarial
#   - No rompe si el archivo no existe
# ============================================================
excel_empresas = _pick_first_existing([
    os.path.join(BASE_DIR, "data", "BASE_EMPRESAS_NOMINAS.xlsx"),
    os.path.join(BASE_DIR, "data", "BASE_EMPRESAS_NOMINAS.xls"),
    "/mnt/data/BASE_EMPRESAS_NOMINAS.xlsx",
    "/mnt/data/BASE_EMPRESAS_NOMINAS.xls",
])

df_empresas = pd.DataFrame(columns=[
    "PERSONAL_ID","CUSTOMER_ID","NOMBRE_COMPLETO","OPERAREA_DESC","CIIU_AGRUPADO",
    "BBVA_BALANCE_AMOUNT","SYSTEM_BALANCE_AMOUNT","STOCK","TRABAJADORES",
    "DEPARTAMENTO","PROVINCIA","DISTRITO","LATITUD","LONGITUD",
    "PENETRACION_NOMINA","SHARE_WALLET"
])

if excel_empresas:
    try:
        raw_e = pd.read_excel(excel_empresas)
        norm_map_e = {normalize_col(c): c for c in raw_e.columns}

        def find_col_e(keys):
            for norm, orig in norm_map_e.items():
                for k in keys:
                    if normalize_col(k) in norm:
                        return orig
            return None

        COLE_PID   = find_col_e(["personal_id", "personal id", "ruc"]) or "personal_id"
        COLE_CID   = find_col_e(["customer_id", "customer id"]) or "customer_id"
        COLE_NAME  = find_col_e(["Nombre_completo", "Nombre completo", "razon social", "nombre"]) or "Nombre_completo"
        COLE_AREA  = find_col_e(["operarea_desc", "operarea desc", "area", "área"]) or "operarea_desc"
        COLE_CIIU  = find_col_e(["ciiu_agrupado", "ciiu agrupado", "ciiu"]) or "ciiu_agrupado"
        COLE_BBVA  = find_col_e(["bbva_balance_amount", "bbva balance amount"]) or "bbva_balance_amount"
        COLE_SYS   = find_col_e(["system_balance_amount", "system balance amount"]) or "system_balance_amount"
        COLE_STOCK = find_col_e(["stock"]) or "STOCK"
        COLE_TRAB  = find_col_e(["trabajadores", "empleados", "workers"]) or "trabajadores"
        COLE_DEP   = find_col_e(["departamento"]) or "Departamento"
        COLE_PROV  = find_col_e(["provincia"]) or "Provincia"
        COLE_DIST  = find_col_e(["distrito"]) or "Distrito"
        COLE_LAT   = find_col_e(["latitud", "lat"]) or "latitud"
        COLE_LON   = find_col_e(["longitud", "lon"]) or "longitud"

        for c in [COLE_PID, COLE_CID, COLE_NAME, COLE_AREA, COLE_CIIU, COLE_BBVA, COLE_SYS,
                  COLE_STOCK, COLE_TRAB, COLE_DEP, COLE_PROV, COLE_DIST, COLE_LAT, COLE_LON]:
            if c not in raw_e.columns:
                raw_e[c] = ""

        raw_e[COLE_DEP]  = raw_e[COLE_DEP].apply(clean_str)
        raw_e[COLE_PROV] = raw_e[COLE_PROV].apply(clean_str)
        raw_e[COLE_DIST] = raw_e[COLE_DIST].apply(clean_str)

        raw_e[COLE_LAT] = (
            raw_e[COLE_LAT].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )
        raw_e[COLE_LON] = (
            raw_e[COLE_LON].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )

        for c in [COLE_BBVA, COLE_SYS, COLE_STOCK, COLE_TRAB]:
            raw_e[c] = pd.to_numeric(
                raw_e[c].astype(str).str.replace(",", "", regex=False),
                errors="coerce"
            ).fillna(0.0)

        df_empresas = raw_e.dropna(subset=[COLE_LAT, COLE_LON]).copy()

        df_empresas = df_empresas.rename(columns={
            COLE_PID:   "PERSONAL_ID",
            COLE_CID:   "CUSTOMER_ID",
            COLE_NAME:  "NOMBRE_COMPLETO",
            COLE_AREA:  "OPERAREA_DESC",
            COLE_CIIU:  "CIIU_AGRUPADO",
            COLE_BBVA:  "BBVA_BALANCE_AMOUNT",
            COLE_SYS:   "SYSTEM_BALANCE_AMOUNT",
            COLE_STOCK: "STOCK",
            COLE_TRAB:  "TRABAJADORES",
            COLE_DEP:   "DEPARTAMENTO",
            COLE_PROV:  "PROVINCIA",
            COLE_DIST:  "DISTRITO",
            COLE_LAT:   "LATITUD",
            COLE_LON:   "LONGITUD",
        })

        df_empresas["NOMBRE_COMPLETO"] = df_empresas["NOMBRE_COMPLETO"].astype(str).fillna("").str.strip()
        df_empresas["OPERAREA_DESC"]   = df_empresas["OPERAREA_DESC"].astype(str).fillna("").str.strip()
        df_empresas["CIIU_AGRUPADO"]   = df_empresas["CIIU_AGRUPADO"].astype(str).fillna("").str.strip()

        df_empresas["PENETRACION_NOMINA"] = np.where(
            df_empresas["TRABAJADORES"] > 0,
            df_empresas["STOCK"] / df_empresas["TRABAJADORES"],
            0.0
        )
        df_empresas["SHARE_WALLET"] = np.where(
            df_empresas["SYSTEM_BALANCE_AMOUNT"] > 0,
            df_empresas["BBVA_BALANCE_AMOUNT"] / df_empresas["SYSTEM_BALANCE_AMOUNT"],
            0.0
        )

        df_empresas["PENETRACION_NOMINA"] = df_empresas["PENETRACION_NOMINA"].clip(lower=0, upper=1)
        df_empresas["SHARE_WALLET"] = df_empresas["SHARE_WALLET"].clip(lower=0, upper=1)

        print(f"✅ Empresas nómina cargadas: {len(df_empresas)} filas ({excel_empresas})")
    except Exception as e:
        print("⚠ No se pudo cargar BASE_EMPRESAS_NOMINAS:", e)
else:
    print("⚠ No existe BASE_EMPRESAS_NOMINAS — capa empresas desactivada.")

def _filter_empresas():
    if df_empresas is None or df_empresas.empty:
        return pd.DataFrame()

    dpto = request.args.get("departamento", "").upper().strip()
    prov = request.args.get("provincia", "").upper().strip()
    dist = request.args.get("distrito", "").upper().strip()

    dff = df_empresas.copy()

    if dpto:
        dff = dff[dff["DEPARTAMENTO"].astype(str).str.upper() == dpto]
    if prov:
        dff = dff[dff["PROVINCIA"].astype(str).str.upper() == prov]
    if dist:
        dff = dff[dff["DISTRITO"].astype(str).str.upper() == dist]

    return dff


# ============================================================
# 2B. CARGAR EXCEL DE AGENTES
# ============================================================
excel_agentes = os.path.join(BASE_DIR, "data", "AGENTES.xlsx")
if not os.path.exists(excel_agentes):
    raise FileNotFoundError("No encontré Excel de AGENTES.xlsx.")

raw_ag = pd.read_excel(excel_agentes)
norm_map_ag = {normalize_col(c): c for c in raw_ag.columns}

def find_col_ag(keys):
    for norm, orig in norm_map_ag.items():
        for k in keys:
            if k in norm:
                return orig
    return None

COLA_ID   = find_col_ag(["TERMINAL", "ID"]) or "TERMINAL"
COLA_COM  = find_col_ag(["COMERCIO"]) or "COMERCIO"
COLA_DEPT = find_col_ag(["DEPARTAMENTO"]) or "DEPARTAMENTO"
COLA_PROV = find_col_ag(["PROVINCIA"]) or "PROVINCIA"
COLA_DIST = find_col_ag(["DISTRITO"]) or "DISTRITO"
COLA_LAT  = find_col_ag(["LATITUD", "LAT"]) or "LATITUD"
COLA_LON  = find_col_ag(["LONGITUD", "LON"]) or "LONGITUD"
COLA_DIV  = find_col_ag(["DIVISION", "DIVISIÓN"]) or "DIVISION"
COLA_DIR  = find_col_ag(["DIRECCION", "DIRECCIÓN"]) or "DIRECCION"
COLA_CAPA = find_col_ag(["CAPA"]) or "CAPA"
# ✅ Nuevos meses según tu Excel: TRXS DIC / TRXS ENE
COLA_TRX_DIC = find_col_ag(["TRXS DIC", "TRXS DICIEMBRE", "TRX DIC", "TRX DICIEMBRE"]) or None
COLA_TRX_ENE = find_col_ag(["TRXS ENE", "TRXS ENERO", "TRX ENE", "TRX ENERO"]) or None

# ✅ Compatibilidad: NO rompes tu código actual (sigue usando trxs_oct / trxs_nov)
COLA_TRX_OCT = COLA_TRX_DIC
COLA_TRX_NOV = COLA_TRX_ENE
PROMA_COL = find_col_ag(["PROMEDIO", "PROM"]) or None

if PROMA_COL is None:
    raw_ag["PROM_FAKE"] = 0.0
    PROMA_COL = "PROM_FAKE"

raw_ag[COLA_LAT] = (
    raw_ag[COLA_LAT].astype(str)
    .str.replace(",", ".", regex=False)
    .str.replace(r"[^\d\.\-]", "", regex=True)
    .replace("", np.nan)
    .astype(float)
)
raw_ag[COLA_LON] = (
    raw_ag[COLA_LON].astype(str)
    .str.replace(",", ".", regex=False)
    .str.replace(r"[^\d\.\-]", "", regex=True)
    .replace("", np.nan)
    .astype(float)
)

df_agentes = raw_ag.dropna(subset=[COLA_LAT, COLA_LON]).reset_index(drop=True)
df_agentes[PROMA_COL] = pd.to_numeric(df_agentes[PROMA_COL], errors="coerce").fillna(0.0)
df_agentes[COLA_CAPA] = df_agentes[COLA_CAPA].astype(str).fillna("")

# ============================================================
# 2C. CARGAR EXCEL DE OFICINAS  ✅ (CON COLUMNAS NUEVAS)
# ============================================================
excel_oficinas = os.path.join(BASE_DIR, "data", "OFICINAS.xlsx")
if not os.path.exists(excel_oficinas):
    raise FileNotFoundError("No encontré Excel de OFICINAS.xlsx.")

raw_of = pd.read_excel(excel_oficinas)
norm_map_of = {normalize_col(c): c for c in raw_of.columns}

def find_col_of(keys):
    for norm, orig in norm_map_of.items():
        for k in keys:
            if k in norm:
                return orig
    return None

COLF_ID   = find_col_of(["COD OFIC", "COD. OFIC", "COD_OFIC"]) or "COD OFIC."
COLF_NAME = find_col_of(["OFICINA"]) or "OFICINA"
COLF_DIV  = find_col_of(["DIVISION", "DIVISIÓN"]) or "DIVISION"
COLF_DEPT = find_col_of(["DEPARTAMENTO"]) or "DEPARTAMENTO"
COLF_PROV = find_col_of(["PROVINCIA"]) or "PROVINCIA"
COLF_DIST = find_col_of(["DISTRITO"]) or "DISTRITO"
COLF_LAT  = find_col_of(["LATITUD", "LAT"]) or "LATITUD"
COLF_LON  = find_col_of(["LONGITUD", "LON"]) or "LONGITUD"
COLF_TRX  = find_col_of(["TRX", "TRXS"]) or "TRX"

COLF_EAS = find_col_of(["ESTRUCTURA AS", "ESTRUCTURA_AS"]) or "ESTRUCTURA AS"
COLF_EBP = find_col_of(["ESTRUCTURA EBP", "ESTRUCTURA_EBP"]) or "ESTRUCTURA EBP"
COLF_EAD = find_col_of(["ESTRUCTURA AD", "ESTRUCTURA_AD"]) or "ESTRUCTURA AD"
COLF_CLI = find_col_of(["CLIENTES UNICOS", "CLIENTES ÚNICOS", "CLIENTES_UNICOS"]) or "CLIENTES UNICOS"
COLF_TKT = find_col_of(["TOTAL_TICKETS", "TOTAL TICKETS"]) or "TOTAL_TICKETS"
COLF_RED = find_col_of(["RED LINES", "REDLINES", "RED_LINES"]) or "RED LINES"
COLF_DIR  = find_col_of(["DIRECCION", "DIRECCIÓN"]) or "DIRECCIÓN"
COLF_PERF = find_col_of(["PERFORMANCE_2025", "PERFORMANCE 2025", "PERFOMANCE_2025", "PERFOMANCE 2025"]) or "PERFORMANCE_2025"
COLF_BAI  = find_col_of(["BAI"]) or "BAI"
COLF_MARG = find_col_of(["MARGEN NETO", "MARGEN_NETO"]) or "MARGEN NETO"

for c in [COLF_EAS, COLF_EBP, COLF_EAD, COLF_CLI, COLF_TKT, COLF_RED, COLF_BAI, COLF_MARG]:
    if c not in raw_of.columns:
        raw_of[c] = 0

for c in [COLF_DIR, COLF_PERF]:
    if c not in raw_of.columns:
        raw_of[c] = ""

raw_of[COLF_LAT] = (
    raw_of[COLF_LAT].astype(str)
    .str.replace(",", ".", regex=False)
    .str.replace(r"[^\d\.\-]", "", regex=True)
    .replace("", np.nan)
    .astype(float)
)
raw_of[COLF_LON] = (
    raw_of[COLF_LON].astype(str)
    .str.replace(",", ".", regex=False)
    .str.replace(r"[^\d\.\-]", "", regex=True)
    .replace("", np.nan)
    .astype(float)
)

df_oficinas = raw_of.dropna(subset=[COLF_LAT, COLF_LON]).reset_index(drop=True)

df_oficinas[COLF_TRX] = pd.to_numeric(df_oficinas[COLF_TRX], errors="coerce").fillna(0.0)
df_oficinas[COLF_EAS] = pd.to_numeric(df_oficinas[COLF_EAS], errors="coerce").fillna(0.0)
df_oficinas[COLF_EBP] = pd.to_numeric(df_oficinas[COLF_EBP], errors="coerce").fillna(0.0)
df_oficinas[COLF_EAD] = pd.to_numeric(df_oficinas[COLF_EAD], errors="coerce").fillna(0.0)
df_oficinas[COLF_CLI] = pd.to_numeric(df_oficinas[COLF_CLI], errors="coerce").fillna(0.0)
df_oficinas[COLF_TKT] = pd.to_numeric(df_oficinas[COLF_TKT], errors="coerce").fillna(0.0)
df_oficinas[COLF_RED] = parse_percent_series(df_oficinas[COLF_RED])
df_oficinas[COLF_DIR]  = df_oficinas[COLF_DIR].astype(str).fillna("").str.strip()
df_oficinas[COLF_PERF] = df_oficinas[COLF_PERF].astype(str).fillna("").str.strip()
df_oficinas[COLF_BAI]  = parse_number_series(df_oficinas[COLF_BAI])
df_oficinas[COLF_MARG] = parse_number_series(df_oficinas[COLF_MARG])

# ============================================================
# LISTA DE OFICINAS PARA "UBICAR EN MAPA" (no filtra datos)
# ============================================================
OFICINAS_LOOKUP = []
for _, r in df_oficinas.iterrows():
    nombre = str(r.get(COLF_NAME, "")).strip()
    if not nombre:
        continue
    OFICINAS_LOOKUP.append({
        "nombre": nombre,
        "lat": float(r.get(COLF_LAT, 0.0)),
        "lon": float(r.get(COLF_LON, 0.0)),
        "departamento": str(r.get(COLF_DEPT, "")).strip(),
        "provincia": str(r.get(COLF_PROV, "")).strip(),
        "distrito": str(r.get(COLF_DIST, "")).strip(),
    })

# orden alfabético por nombre
OFICINAS_LOOKUP = sorted(OFICINAS_LOOKUP, key=lambda x: normalize_col(x["nombre"]))


# ============================================================
# 2D. CARGAR ZONAS (URBANA / RURAL) ✅ (ZONAS.xlsx)
# ============================================================
excel_zonas_local = os.path.join(BASE_DIR, "data", "ZONAS.xlsx")
excel_zonas_alt = "/mnt/data/ZONAS.xlsx"
excel_zonas = excel_zonas_local if os.path.exists(excel_zonas_local) else (excel_zonas_alt if os.path.exists(excel_zonas_alt) else "")

df_zonas = pd.DataFrame(columns=[
    "DEPARTAMENTO", "PROVINCIA", "DISTRITO",
    "UBIGEO_DIST", "CENTRO_POBLADO", "UBIGEO_CP",
    "TIPO_ZONA", "LATITUD", "LONGITUD"
])

if excel_zonas:
    try:
        raw_z = pd.read_excel(excel_zonas)

        for c in [
            "DEPARTAMENTO","PROVINCIA","DISTRITO","UBIGEO DEL DISTRITO",
            "NOMBRE DEL CENTRO POBLADO","UBIGEO DEL CENTRO POBLADO",
            "TIPO DE CENTRO POBLADO","LATITUD","LONGITUD"
        ]:
            if c not in raw_z.columns:
                raw_z[c] = ""

        raw_z["DEPARTAMENTO"] = raw_z["DEPARTAMENTO"].apply(clean_str)
        raw_z["PROVINCIA"] = raw_z["PROVINCIA"].apply(clean_str)
        raw_z["DISTRITO"] = raw_z["DISTRITO"].apply(clean_str)

        raw_z["UBIGEO_DIST"] = raw_z["UBIGEO DEL DISTRITO"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        raw_z["UBIGEO_CP"]   = raw_z["UBIGEO DEL CENTRO POBLADO"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

        raw_z["CENTRO_POBLADO"] = raw_z["NOMBRE DEL CENTRO POBLADO"].astype(str).str.upper().str.strip()
        raw_z["TIPO_ZONA"] = raw_z["TIPO DE CENTRO POBLADO"].astype(str).str.upper().str.strip()

        raw_z["LATITUD"] = (
            raw_z["LATITUD"].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )
        raw_z["LONGITUD"] = (
            raw_z["LONGITUD"].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )

        df_zonas = raw_z.dropna(subset=["LATITUD", "LONGITUD"]).reset_index(drop=True)
        df_zonas = df_zonas[[
            "DEPARTAMENTO","PROVINCIA","DISTRITO",
            "UBIGEO_DIST","CENTRO_POBLADO","UBIGEO_CP",
            "TIPO_ZONA","LATITUD","LONGITUD"
        ]].copy()

        print(f"✅ ZONAS.xlsx cargado: {len(df_zonas)} filas ({excel_zonas})")
    except Exception as e:
        print("⚠ No se pudo cargar ZONAS.xlsx:", e)
else:
    print("⚠ No existe ZONAS.xlsx (bordes rural/urbano desactivados).")

ZONAS_HULL_CACHE = {}

def _convex_hull_xy(points_xy):
    pts = sorted(set(points_xy))
    if len(pts) <= 1:
        return pts

    def cross(o, a, b):
        return (a[0]-o[0])*(b[1]-o[1]) - (a[1]-o[1])*(b[0]-o[0])

    lower = []
    for p in pts:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)

    upper = []
    for p in reversed(pts):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)

    return lower[:-1] + upper[:-1]

def _rect_from_points(points_xy, pad_min=0.01):
    xs = [p[0] for p in points_xy]
    ys = [p[1] for p in points_xy]
    minx, maxx = min(xs), max(xs)
    miny, maxy = min(ys), max(ys)

    if minx == maxx and miny == maxy:
        padx = pady = pad_min
    else:
        padx = max(pad_min, (maxx - minx) * 0.05)
        pady = max(pad_min, (maxy - miny) * 0.05)

    minx -= padx; maxx += padx
    miny -= pady; maxy += pady
    return [(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy)]

def _zona_polygon_latlon(dff):
    if dff is None or dff.empty:
        return []

    pts_xy = list(zip(dff["LONGITUD"].astype(float).tolist(), dff["LATITUD"].astype(float).tolist()))
    if len(pts_xy) < 3:
        rect = _rect_from_points(pts_xy) if len(pts_xy) > 0 else []
        return [[y, x] for (x, y) in rect]

    hull = _convex_hull_xy(pts_xy)
    if len(hull) < 3:
        rect = _rect_from_points(pts_xy)
        return [[y, x] for (x, y) in rect]

    return [[y, x] for (x, y) in hull]

# ============================================================
# 2E. CARGAR NODOS (NODOS1.xlsx) ✅ (CON TIPO + NOMBRE)
# ============================================================
excel_nodos_local = os.path.join(BASE_DIR, "data", "NODOS1.xlsx")
excel_nodos_alt = "/mnt/data/NODOS1.xlsx"
excel_nodos = excel_nodos_local if os.path.exists(excel_nodos_local) else (excel_nodos_alt if os.path.exists(excel_nodos_alt) else "")

df_nodos = pd.DataFrame(columns=["UBIGEO","DEPARTAMENTO","PROVINCIA","DISTRITO","TIPO","NOMBRE","LATITUD","LONGITUD"])

if excel_nodos:
    try:
        raw_n = pd.read_excel(excel_nodos)
        norm_map_n = {normalize_col(c): c for c in raw_n.columns}

        def find_col_n(keys):
            for norm, orig in norm_map_n.items():
                for k in keys:
                    if k in norm:
                        return orig
            return None

        COLN_UBI = find_col_n(["UBIGEO"]) or "UBIGEO"
        COLN_DEP = find_col_n(["DEPARTAMENTO"]) or "DEPARTAMENTO"
        COLN_PRO = find_col_n(["PROVINCIA"]) or "PROVINCIA"
        COLN_DIS = find_col_n(["DISTRITO"]) or "DISTRITO"
        COLN_TIP = find_col_n(["TIPO", "CATEGORIA", "CATEGOR"]) or "TIPO"
        COLN_NOM = find_col_n(["NOMBRE", "NAME"]) or "NOMBRE"
        COLN_LAT = find_col_n(["LATITUD", "LAT"]) or "LATITUD"
        COLN_LON = find_col_n(["LONGITUD", "LON"]) or "LONGITUD"

        for c in [COLN_UBI, COLN_DEP, COLN_PRO, COLN_DIS, COLN_TIP, COLN_NOM, COLN_LAT, COLN_LON]:
            if c not in raw_n.columns:
                raw_n[c] = ""

        raw_n[COLN_DEP] = raw_n[COLN_DEP].apply(clean_str)
        raw_n[COLN_PRO] = raw_n[COLN_PRO].apply(clean_str)
        raw_n[COLN_DIS] = raw_n[COLN_DIS].apply(clean_str)

        raw_n[COLN_UBI] = (
            raw_n[COLN_UBI].astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )

        raw_n[COLN_TIP] = raw_n[COLN_TIP].astype(str).str.strip()
        raw_n[COLN_NOM] = raw_n[COLN_NOM].astype(str).str.strip()

        raw_n[COLN_LAT] = (
            raw_n[COLN_LAT].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )
        raw_n[COLN_LON] = (
            raw_n[COLN_LON].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )

        df_nodos = raw_n.dropna(subset=[COLN_LAT, COLN_LON]).copy()

        df_nodos = df_nodos.rename(columns={
            COLN_UBI: "UBIGEO",
            COLN_DEP: "DEPARTAMENTO",
            COLN_PRO: "PROVINCIA",
            COLN_DIS: "DISTRITO",
            COLN_TIP: "TIPO",
            COLN_NOM: "NOMBRE",
            COLN_LAT: "LATITUD",
            COLN_LON: "LONGITUD",
        })

        df_nodos = df_nodos[["UBIGEO","DEPARTAMENTO","PROVINCIA","DISTRITO","TIPO","NOMBRE","LATITUD","LONGITUD"]].copy()
        print(f"✅ NODOS1.xlsx cargado: {len(df_nodos)} filas ({excel_nodos})")
    except Exception as e:
        print("⚠ No se pudo cargar NODOS1.xlsx:", e)
else:
    print("⚠ No existe NODOS1.xlsx (comercial/nodos desactivados).")

# ============================================================
# 3. JERARQUÍA TOTAL UNIFICADA (CLIENTES + TODOS LOS CANALES + NODOS)
# ============================================================
geo_frames = []
geo_frames.append(
    df[[COL_DEPT, COL_PROV, COL_DIST]].rename(
        columns={COL_DEPT: "departamento", COL_PROV: "provincia", COL_DIST: "distrito"}
    )
)
geo_frames.append(
    df_agentes[[COLA_DEPT, COLA_PROV, COLA_DIST]].rename(
        columns={COLA_DEPT: "departamento", COLA_PROV: "provincia", COLA_DIST: "distrito"}
    )
)
geo_frames.append(
    df_oficinas[[COLF_DEPT, COLF_PROV, COLF_DIST]].rename(
        columns={COLF_DEPT: "departamento", COLF_PROV: "provincia", COLF_DIST: "distrito"}
    )
)
geo_frames.append(df_clientes[["departamento", "provincia", "distrito"]])
if df_comercios is not None and not df_comercios.empty:
    geo_frames.append(
        df_comercios[["DEPARTAMENTO","PROVINCIA","DISTRITO"]].rename(
            columns={"DEPARTAMENTO":"departamento","PROVINCIA":"provincia","DISTRITO":"distrito"}
        )
    )

if df_nodos is not None and not df_nodos.empty:
    geo_frames.append(
        df_nodos[["DEPARTAMENTO","PROVINCIA","DISTRITO"]].rename(
            columns={"DEPARTAMENTO":"departamento","PROVINCIA":"provincia","DISTRITO":"distrito"}
        )
    )

geo_all = pd.concat(geo_frames, ignore_index=True)
geo_all["departamento"] = geo_all["departamento"].apply(clean_str)
geo_all["provincia"] = geo_all["provincia"].apply(clean_str)
geo_all["distrito"] = geo_all["distrito"].apply(clean_str)
geo_all = geo_all.dropna()

DEPARTAMENTOS = sorted(geo_all["departamento"].unique())

PROVINCIAS_BY_DEPT = {}
for dep in DEPARTAMENTOS:
    provs = geo_all.loc[geo_all["departamento"] == dep, "provincia"].unique().tolist()
    PROVINCIAS_BY_DEPT[dep] = sorted([p for p in provs if p])

DIST_BY_PROV = {}
provincias_unicas = sorted(geo_all["provincia"].unique())
for prov in provincias_unicas:
    dists = geo_all.loc[geo_all["provincia"] == prov, "distrito"].unique().tolist()
    DIST_BY_PROV[prov] = sorted([d for d in dists if d])

ALL_PROVINCIAS = sorted([p for p in geo_all["provincia"].dropna().unique().tolist() if p])
ALL_DISTRITOS  = sorted([d for d in geo_all["distrito"].dropna().unique().tolist() if d])

# ============================================================
# UNIFICACIÓN DE DIVISIONES (Islas + Oficinas + Agentes)
# ============================================================
div_frames = []
div_frames.append(
    df[[COL_DEPT, COL_PROV, COL_DIST, COL_DIV]].rename(
        columns={COL_DEPT: "departamento", COL_PROV: "provincia", COL_DIST: "distrito", COL_DIV: "division"}
    )
)
div_frames.append(
    df_agentes[[COLA_DEPT, COLA_PROV, COLA_DIST, COLA_DIV]].rename(
        columns={COLA_DEPT: "departamento", COLA_PROV: "provincia", COLA_DIST: "distrito", COLA_DIV: "division"}
    )
)
div_frames.append(
    df_oficinas[[COLF_DEPT, COLF_PROV, COLF_DIST, COLF_DIV]].rename(
        columns={COLF_DEPT: "departamento", COLF_PROV: "provincia", COLF_DIST: "distrito", COLF_DIV: "division"}
    )
)

div_all = pd.concat(div_frames, ignore_index=True)
div_all["departamento"] = div_all["departamento"].apply(clean_str)
div_all["provincia"] = div_all["provincia"].apply(clean_str)
div_all["distrito"] = div_all["distrito"].apply(clean_str)
div_all["division"] = div_all["division"].apply(clean_str)

DIVISIONES = sorted(div_all["division"].dropna().unique())

DIVISIONES_BY_DEPT = {}
for dep in DEPARTAMENTOS:
    divs = div_all.loc[div_all["departamento"] == dep, "division"].dropna().unique().tolist()
    DIVISIONES_BY_DEPT[dep] = sorted(set(divs))

DIVISIONES_BY_PROV = {}
for dep, prov_list in PROVINCIAS_BY_DEPT.items():
    for p in prov_list:
        divs = div_all.loc[div_all["provincia"] == p, "division"].dropna().unique().tolist()
        DIVISIONES_BY_PROV[p] = sorted(set(divs))

DIVISIONES_BY_DIST = {}
for prov, dists in DIST_BY_PROV.items():
    for d in dists:
        divs = div_all.loc[div_all["distrito"] == d, "division"].dropna().unique().tolist()
        DIVISIONES_BY_DIST[d] = sorted(set(divs))

# ============================================================
# 4. FLASK + LOGIN
# ============================================================
app = Flask(__name__)

@app.route("/healthz", methods=["GET", "HEAD"])
def healthz():
    return "ok", 200
app.secret_key = os.getenv("SECRET_KEY", "fallback_local")

APP_USER = os.getenv("APP_USERNAME", "adminbbva")
APP_PASS = os.getenv("APP_PASSWORD", "canales1020")

@app.after_request
def add_header(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Acceso Seguro — BBVA</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body{
      margin:0; padding:0; height:100vh; width:100%;
      display:flex; align-items:center; justify-content:center;
      background:url('{{ url_for('static', filename='bbva.png') }}') no-repeat center center fixed;
      background-size:cover;
      font-family:Arial,Helvetica,sans-serif;
    }
    .box{
      background:rgba(255,255,255,0.88);
      padding:30px 35px;
      border-radius:12px;
      box-shadow:0 8px 30px rgba(0,0,0,0.3);
      width:360px;
      text-align:center;
    }
    h2{color:#1464A5; margin:0 0 15px 0;}
    input{
      width:100%;
      padding:10px;
      margin:8px 0;
      border-radius:8px;
      border:1px solid #ddd;
    }
    button{
      width:100%;
      padding:10px;
      background:#1464A5;
      color:white;
      border:none;
      border-radius:8px;
      font-weight:600;
      cursor:pointer;
    }
    .error{color:#c0392b; font-size:14px; margin-bottom:8px;}
    .small{font-size:13px; color:#6b7a8a; margin-top:8px;}

  </style>
</head>
<body>
  <div class="box">
    <h2>Inicia sesión</h2>
    {% if error %}<div class="error">{{ error }}</div>{% endif %}
    <form method="post">
      <input name="username" placeholder="Usuario" required autofocus>
      <input name="password" type="password" placeholder="Contraseña" required>
      <button type="submit">Entrar</button>
    </form>
    <div class="small">Acceso restringido — Solo personal autorizado</div>
  </div>
</body>
</html>
"""

def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if session.get("user") != APP_USER:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapped

@app.route("/")
@app.route("/", methods=["GET", "HEAD"])
def root():
    # si Render hace health check con HEAD, devolvemos 200
    if request.method == "HEAD":
        return "", 200

    if session.get("user") == APP_USER:
        return redirect(url_for("mapa_integral"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = request.form.get("username")
        p = request.form.get("password")
        if u == APP_USER and p == APP_PASS:
            session.clear()
            session["user"] = u
            return redirect(url_for("mapa_integral"))
        return render_template_string(LOGIN_TEMPLATE, error="Credenciales incorrectas")
    return render_template_string(LOGIN_TEMPLATE)

@app.route("/logout")
def logout():
    session.clear()
    resp = redirect(url_for("login"))
    resp.set_cookie("session", "", expires=0)
    return resp

# ============================================================
# 5. SELECTOR DE CAPAS
# ============================================================
SELECTOR_TEMPLATE = None  # (removido: solo usamos /mapa/integral)

@app.route("/selector")
@login_required
def selector():
    return redirect(url_for("mapa_integral"))

@app.route("/api/recomendaciones")
@login_required
def api_recomendaciones():
    return jsonify(recomendaciones.to_dict(orient="records"))

# ============================================================
# ✅ API ZONAS — /api/zonas (RURAL / URBANA)
# ============================================================
@app.route("/api/zonas")
@login_required
def api_zonas():
    dpto = request.args.get("departamento", "").upper().strip()
    prov = request.args.get("provincia", "").upper().strip()
    dist = request.args.get("distrito", "").upper().strip()

    def build_for(tipo_key):
        cache_key = (dpto, prov, dist, tipo_key)
        if cache_key in ZONAS_HULL_CACHE:
            return ZONAS_HULL_CACHE[cache_key]

        dff = df_zonas
        if dpto: dff = dff[dff["DEPARTAMENTO"] == dpto]
        if prov: dff = dff[dff["PROVINCIA"] == prov]
        if dist: dff = dff[dff["DISTRITO"] == dist]

        dff_t = dff[dff["TIPO_ZONA"].astype(str).str.contains(tipo_key, na=False)]
        poly = _zona_polygon_latlon(dff_t)
        out = {"count": int(len(dff_t)), "poly": poly}
        ZONAS_HULL_CACHE[cache_key] = out
        return out

    rural = build_for("RURAL")
    urban = build_for("URBAN")

    return jsonify({"rural": rural, "urbano": urban})

# ============================================================
# ✅ API NODOS/COMERCIAL — /api/nodos
#   - Devuelve nodos filtrados por departamento/provincia/distrito
#   - ✅ Resumen por Panel Comercial basado en COLUMNA TIPO (Excel)
#   - ✅ NO devuelve detalle por tipo (Excel)
#   - ✅ Popup: si el NOMBRE no indica tipo, muestra (TIPO) abajo
# ============================================================
@app.route("/api/nodos")
@login_required
def api_nodos():
    if df_nodos is None or df_nodos.empty:
        return jsonify({"total": 0, "resumen": {}, "nodos": []})

    dpto = request.args.get("departamento", "").strip().upper()
    prov = request.args.get("provincia", "").strip().upper()
    dist = request.args.get("distrito", "").strip().upper()

    dff = df_nodos.copy()

    if dpto:
        dff = dff[dff["DEPARTAMENTO"].astype(str).str.strip().str.upper() == dpto]
    if prov:
        dff = dff[dff["PROVINCIA"].astype(str).str.strip().str.upper() == prov]
    if dist:
        dff = dff[dff["DISTRITO"].astype(str).str.strip().str.upper() == dist]

    if dff.empty:
        return jsonify({"total": 0, "resumen": {
            "total": 0,
            "hospitales": 0,
            "clinicas": 0,
            "centros_comerciales": 0,
            "plaza_vea": 0,
            "sodimac": 0,
            "metro": 0,
            "tottus": 0,
            "wong": 0,
            "universidades": 0,
            "mercados": 0,
            "otros": 0,
        }, "nodos": []})

    resumen = {
        "total": 0,
        "hospitales": 0,
        "clinicas": 0,
        "centros_comerciales": 0,
        "plaza_vea": 0,
        "sodimac": 0,
        "metro": 0,
        "tottus": 0,
        "wong": 0,
        "universidades": 0,
        "mercados": 0,
        "otros": 0,
    }

    nodos = []

    for _, r in dff.iterrows():
        nombre = str(r.get("NOMBRE","")).strip()
        tipo_excel = str(r.get("TIPO","")).strip()

        # ✅ categoría panel principal desde TIPO (y fallback por nombre)
        cat = nodo_categoria_desde_tipo(tipo_excel, nombre)
        tipo10 = tipo10_label_from_categoria(cat)

        # ✅ popup: si NOMBRE no indica el tipo, poner (TIPO) abajo
        nombre_popup = nombre_popup_con_tipo(nombre, tipo10)

        resumen["total"] += 1
        if cat == "HOSPITAL":
            resumen["hospitales"] += 1
        elif cat == "CLINICA":
            resumen["clinicas"] += 1
        elif cat == "CENTRO_COMERCIAL":
            resumen["centros_comerciales"] += 1
        elif cat == "PLAZA_VEA":
            resumen["plaza_vea"] += 1
        elif cat == "SODIMAC":
            resumen["sodimac"] += 1
        elif cat == "METRO":
            resumen["metro"] += 1
        elif cat == "TOTTUS":
            resumen["tottus"] += 1
        elif cat == "WONG":
            resumen["wong"] += 1
        elif cat == "UNIVERSIDAD":
            resumen["universidades"] += 1
        elif cat == "MERCADO":
            resumen["mercados"] += 1
        else:
            resumen["otros"] += 1

        nodos.append({
            "ubigeo": str(r.get("UBIGEO","")).strip(),
            "departamento": str(r.get("DEPARTAMENTO","")).strip(),
            "provincia": str(r.get("PROVINCIA","")).strip(),
            "distrito": str(r.get("DISTRITO","")).strip(),
            "tipo": tipo_excel,
            "nombre": nombre,                  # original
            "nombre_popup": nombre_popup,      # ✅ para el globo (2 líneas)
            "categoria": cat,
            "tipo10": tipo10,                  # ✅ 10 tipos estándar
            "lat": float(r.get("LATITUD", 0.0)),
            "lon": float(r.get("LONGITUD", 0.0)),
        })

    return jsonify({"total": len(nodos), "resumen": resumen, "nodos": nodos})


@app.route("/api/empresas_nominas_points")
@login_required
def api_empresas_nominas_points():
    if df_empresas is None or df_empresas.empty:
        return jsonify([])

    zoom_str = request.args.get("zoom", "10")
    try:
        zoom = int(float(zoom_str))
    except:
        zoom = 10

    dff = _filter_empresas()
    if dff.empty:
        return jsonify([])

    if zoom <= 5:
        sample_size = 1200
    elif zoom <= 9:
        sample_size = 4000
    elif zoom <= 13:
        sample_size = 9000
    else:
        sample_size = 15000

    sample_size = min(sample_size, len(dff))
    df_sample = dff if sample_size >= len(dff) else dff.sample(sample_size, replace=False, random_state=7)

    out = []
    for _, r in df_sample.iterrows():
        out.append({
            "personal_id": str(r.get("PERSONAL_ID", "")).strip(),
            "customer_id": str(r.get("CUSTOMER_ID", "")).strip(),
            "nombre_completo": str(r.get("NOMBRE_COMPLETO", "")).strip(),
            "operarea_desc": str(r.get("OPERAREA_DESC", "")).strip(),
            "ciiu_agrupado": str(r.get("CIIU_AGRUPADO", "")).strip(),
            "bbva_balance_amount": float(r.get("BBVA_BALANCE_AMOUNT", 0.0) or 0.0),
            "system_balance_amount": float(r.get("SYSTEM_BALANCE_AMOUNT", 0.0) or 0.0),
            "stock": float(r.get("STOCK", 0.0) or 0.0),
            "trabajadores": float(r.get("TRABAJADORES", 0.0) or 0.0),
            "penetracion_nomina": float(r.get("PENETRACION_NOMINA", 0.0) or 0.0),
            "share_wallet": float(r.get("SHARE_WALLET", 0.0) or 0.0),
            "departamento": str(r.get("DEPARTAMENTO", "")).strip(),
            "provincia": str(r.get("PROVINCIA", "")).strip(),
            "distrito": str(r.get("DISTRITO", "")).strip(),
            "lat": float(r.get("LATITUD", 0.0)),
            "lon": float(r.get("LONGITUD", 0.0)),
        })
    return jsonify(out)

@app.route("/api/empresas_nominas_heat")
@login_required
def api_empresas_nominas_heat():
    if df_empresas is None or df_empresas.empty:
        return jsonify([])

    dff = _filter_empresas().copy()
    if dff.empty:
        return jsonify([])

    # =========================================================
    # ✅ Filtrar por viewport visible del mapa (si viene)
    # south, west, north, east
    # =========================================================
    try:
        south = float(request.args.get("south", "nan"))
        west  = float(request.args.get("west", "nan"))
        north = float(request.args.get("north", "nan"))
        east  = float(request.args.get("east", "nan"))
    except Exception:
        south = west = north = east = float("nan")

    if all(pd.notna(v) for v in [south, west, north, east]):
        # caso normal
        if west <= east:
            dff = dff[
                (pd.to_numeric(dff["LATITUD"], errors="coerce").between(south, north)) &
                (pd.to_numeric(dff["LONGITUD"], errors="coerce").between(west, east))
            ].copy()
        else:
            # por si alguna vez el bbox cruza antimeridiano
            dff = dff[
                (pd.to_numeric(dff["LATITUD"], errors="coerce").between(south, north)) &
                (
                    (pd.to_numeric(dff["LONGITUD"], errors="coerce") >= west) |
                    (pd.to_numeric(dff["LONGITUD"], errors="coerce") <= east)
                )
            ].copy()

    if dff.empty:
        return jsonify([])

    out = []
    for _, r in dff.iterrows():
        try:
            lat = float(r.get("LATITUD", 0.0))
            lon = float(r.get("LONGITUD", 0.0))
        except Exception:
            continue

        if not (pd.notna(lat) and pd.notna(lon)):
            continue

        out.append({
            "lat": lat,
            "lon": lon,
            "trabajadores": float(r.get("TRABAJADORES", 0.0) or 0.0),
            "stock": float(r.get("STOCK", 0.0) or 0.0),
        })

    return jsonify(out)


@app.route("/api/resumen_empresas_nominas")
@login_required
def api_resumen_empresas_nominas():
    if df_empresas is None or df_empresas.empty:
        return jsonify({
            "total_empresas": 0, "total_trabajadores": 0, "total_stock": 0,
            "penetracion_prom": 0, "share_wallet_prom": 0,
            "saldo_bbva_total": 0, "saldo_system_total": 0,
            "top_area": "—", "top_sector": "—"
        })

    dff = _filter_empresas()
    if dff.empty:
        return jsonify({
            "total_empresas": 0, "total_trabajadores": 0, "total_stock": 0,
            "penetracion_prom": 0, "share_wallet_prom": 0,
            "saldo_bbva_total": 0, "saldo_system_total": 0,
            "top_area": "—", "top_sector": "—"
        })

    total_empresas = len(dff)
    total_trabajadores = float(dff["TRABAJADORES"].sum())
    total_stock = float(dff["STOCK"].sum())
    penetracion_prom = float((total_stock / total_trabajadores) if total_trabajadores > 0 else 0.0)
    saldo_bbva_total = float(dff["BBVA_BALANCE_AMOUNT"].sum())
    saldo_system_total = float(dff["SYSTEM_BALANCE_AMOUNT"].sum())
    share_wallet_prom = float((saldo_bbva_total / saldo_system_total) if saldo_system_total > 0 else 0.0)

    area_mode = dff["OPERAREA_DESC"].replace("", np.nan).dropna()
    sector_mode = dff["CIIU_AGRUPADO"].replace("", np.nan).dropna()

    return jsonify({
        "total_empresas": int(total_empresas),
        "total_trabajadores": round(total_trabajadores, 0),
        "total_stock": round(total_stock, 0),
        "penetracion_prom": round(100 * penetracion_prom, 1),
        "share_wallet_prom": round(100 * share_wallet_prom, 1),
        "saldo_bbva_total": round(saldo_bbva_total, 2),
        "saldo_system_total": round(saldo_system_total, 2),
        "top_area": str(area_mode.mode().iloc[0]) if not area_mode.empty else "—",
        "top_sector": str(sector_mode.mode().iloc[0]) if not sector_mode.empty else "—",
    })

# ============================================================
# 6. RUTAS MAPA
# ============================================================
@app.route("/mapa/integral")
@login_required
def mapa_integral():
    initial_center = df[[COL_LAT, COL_LON]].mean().tolist()
    return render_template_string(
        TEMPLATE_MAPA,
        tipo_mapa="integral",
        departamentos=DEPARTAMENTOS,
        provincias_by_dept=PROVINCIAS_BY_DEPT,
        dist_by_prov=DIST_BY_PROV,
        all_provincias=ALL_PROVINCIAS,
        all_distritos=ALL_DISTRITOS,
        div_by_dept=DIVISIONES_BY_DEPT,
        div_by_prov=DIVISIONES_BY_PROV,
        div_by_dist=DIVISIONES_BY_DIST,
        divisiones=DIVISIONES,
        oficinas_lookup=OFICINAS_LOOKUP,
        initial_center=initial_center,
        initial_zoom=6,
    )

@app.route("/mapa/<tipo>")
@login_required
def mapa_tipo(tipo):
    # Vista simplificada: todas las rutas /mapa/* van a INTEGRAL
    return redirect(url_for("mapa_integral"))

# ============================================================
# 7. API /api/points — ISLAS + AGENTES + OFICINAS
# ============================================================
@app.route("/api/points")
@login_required
def api_points():
    # Endpoint legacy (capas individuales). Se deja inhabilitado en la versión solo-INTEGRAL.
    return jsonify({"error": "Endpoint deshabilitado. Usa /api/points_integral"}), 410

@app.route("/api/clientes")
@login_required
def api_clientes():
    zoom_str = request.args.get("zoom", "10")
    try:
        zoom = int(float(zoom_str))
    except:
        zoom = 10

    dpto = request.args.get("departamento", "").upper().strip()
    prov = request.args.get("provincia", "").upper().strip()
    dist = request.args.get("distrito", "").upper().strip()
    #seg = request.args.get("segmento", "").upper().strip()

    dff = df_clientes.copy()
    if dpto: dff = dff[dff["departamento"].str.upper() == dpto]
    if prov: dff = dff[dff["provincia"].str.upper() == prov]
    if dist: dff = dff[dff["distrito"].str.upper() == dist]
    #if seg:  dff = dff[dff["segmento"].astype(str).str.upper() == seg]

    if dff.empty:
        return jsonify([])

    if zoom <= 5:
        sample_size = 1000
    elif zoom <= 9:
        sample_size = 3000
    elif zoom <= 13:
        sample_size = 7000
    else:
        sample_size = 12000

    sample_size = min(sample_size, len(dff))
    df_sample = dff.sample(sample_size, replace=False, random_state=None)

    puntos = [{"lat": float(r.latitud), "lon": float(r.longitud)} for _, r in df_sample.iterrows()]
    return jsonify(puntos)





# ============================================================
# HELPERS — COMERCIOS (para heatmap y puntos)
# ============================================================
def _stable_seed(key: str) -> int:
    # seed estable (misma salida para mismos filtros)
    h = hashlib.md5(key.encode("utf-8")).hexdigest()[:8]
    return int(h, 16)

def _grid_step_by_zoom(z: int) -> float:
    # mientras más zoom, grilla más fina
    if z <= 6:  return 0.25
    if z <= 8:  return 0.12
    if z <= 10: return 0.06
    if z <= 12: return 0.03
    return 0.0  # sin grilla (raw)

def _ensure_numeric_cols(dff: pd.DataFrame) -> pd.DataFrame:
    dff = dff.copy()
    dff["LATITUD"]  = pd.to_numeric(dff["LATITUD"], errors="coerce")
    dff["LONGITUD"] = pd.to_numeric(dff["LONGITUD"], errors="coerce")
    dff["CANT_CLIENTES"] = pd.to_numeric(dff["CANT_CLIENTES"], errors="coerce").fillna(0.0)
    dff = dff.dropna(subset=["LATITUD", "LONGITUD"])
    dff = dff[np.isfinite(dff["LATITUD"]) & np.isfinite(dff["LONGITUD"])]
    return dff

# ============================================================
# ✅ HEATMAP CANTIDAD DE CLIENTES (COMERCIOS) — ESTABLE + GRID
#   - Retorna puntos agregados por grilla según zoom
#   - w = suma de CANT_CLIENTES (peso real)
# ============================================================
@app.route("/api/heatmap_cant_clientes")
@login_required
def api_heat_cant_clientes():
    zoom_str = request.args.get("zoom", "10")
    try:
        zoom = int(float(zoom_str))
    except:
        zoom = 10

    # ✅ Ya NO se filtra por Departamento / Provincia / Distrito
    if df_comercios is None or df_comercios.empty:
        return jsonify([])

    dff = df_comercios.copy()

    if dff.empty:
        return jsonify([])

    dff = _ensure_numeric_cols(dff)

    step = _grid_step_by_zoom(zoom)

    # ✅ si step>0 agregamos por celdas para performance (estable)
    if step > 0:
        latg = (np.round(dff["LATITUD"].values / step) * step).astype(float)
        long = (np.round(dff["LONGITUD"].values / step) * step).astype(float)

        g = pd.DataFrame({
            "lat": latg,
            "lon": long,
            "w": dff["CANT_CLIENTES"].values
        })
        g = g.groupby(["lat", "lon"], as_index=False)["w"].sum()
        g["cant_clientes"] = g["w"]

        out = g.to_dict(orient="records")
        return jsonify(out)

    # ✅ zoom alto: raw (pero con cap estable por seguridad)
    max_raw = 60000
    if len(dff) > max_raw:
        seed = _stable_seed(f"{zoom}|RAW")
        dff = dff.sample(max_raw, replace=False, random_state=seed)

    out = []
    for _, r in dff.iterrows():
        w = float(r.get("CANT_CLIENTES", 0.0))
        out.append({
            "lat": float(r.get("LATITUD", 0.0)),
            "lon": float(r.get("LONGITUD", 0.0)),
            "cant_clientes": w,
            "w": w,  # ✅ peso real para heatmap
        })
    return jsonify(out)

# ============================================================
# ✅ PUNTOS COMERCIOS — CLICK POPUP (ID + cant_clientes) — ESTABLE
#   - Retorna puntos para checkbox df_comercios
#   - Muestreo SOLO si se pasa del límite, y SIEMPRE determinístico
# ============================================================
@app.route("/api/comercios_points")
@login_required
def api_comercios_points():
    zoom_str = request.args.get("zoom", "10")
    try:
        zoom = int(float(zoom_str))
    except:
        zoom = 10

    # ✅ Ya NO se filtra por Departamento / Provincia / Distrito
    if df_comercios is None or df_comercios.empty:
        return jsonify([])

    dff = df_comercios.copy()

    if dff.empty:
        return jsonify([])

    dff = _ensure_numeric_cols(dff)

    # límite por zoom (más zoom = más puntos permitidos)
    if zoom <= 6:
        cap = 4000
    elif zoom <= 10:
        cap = 10000
    elif zoom <= 13:
        cap = 20000
    else:
        cap = 30000

    if len(dff) > cap:
        seed = _stable_seed(f"{zoom}|PTS")
        dff = dff.sample(cap, replace=False, random_state=seed)

    out = []
    for _, r in dff.iterrows():
        out.append({
            "lat": float(r.get("LATITUD", 0.0)),
            "lon": float(r.get("LONGITUD", 0.0)),

            # ✅ manda ambos nombres (por compatibilidad)
            "id_comercio": str(r.get("ID_COMERCIO", "")).strip(),
            "commerce_id": str(r.get("ID_COMERCIO", "")).strip(),

            "cant_clientes": float(r.get("CANT_CLIENTES", 0.0)),
            "ruc": str(r.get("RUC", "")).strip(),
            "razon_social": str(r.get("RAZON_SOCIAL", "")).strip(),
            "direccion": str(r.get("DIRECCION", "")).strip(),
        })
    return jsonify(out)

# ============================================================
# API — RESUMEN DE CLIENTES VISIBLE SEGÚN FILTROS
# ============================================================
@app.route("/api/resumen_clientes")
@login_required
def api_resumen_clientes():
    dpto = request.args.get("departamento", "").upper().strip()
    prov = request.args.get("provincia", "").upper().strip()
    dist = request.args.get("distrito", "").upper().strip()
    segmento = request.args.get("segmento", "").upper().strip()

    dff = df_clientes.copy()
    if dpto: dff = dff[dff["departamento"].str.upper() == dpto]
    if prov: dff = dff[dff["provincia"].str.upper() == prov]
    if dist: dff = dff[dff["distrito"].str.upper() == dist]
    if segmento: dff = dff[dff["segmento"].astype(str).str.upper() == segmento]

    if dff.empty:
        return jsonify({
            "total": 0, "digital_pct": 0, "edad_prom": 0,
            "ingreso_prom": 0, "deuda_prom": 0, "top_segmento": "—"
        })

    total = len(dff)
    digital_pct = round(100 * dff["flag_digital"].mean(), 1) if "flag_digital" in dff.columns else 0
    edad_prom = round(dff["edad"].mean(), 1) if "edad" in dff.columns else 0
    ingreso_prom = round(dff["ingresos"].mean(), 2) if "ingresos" in dff.columns else 0
    deuda_prom = round(dff["deuda"].mean(), 2) if "deuda" in dff.columns else 0
    top_segmento = dff["segmento"].value_counts().idxmax() if "segmento" in dff.columns else "—"

    return jsonify({
        "total": total,
        "digital_pct": digital_pct,
        "edad_prom": edad_prom,
        "ingreso_prom": ingreso_prom,
        "deuda_prom": deuda_prom,
        "top_segmento": top_segmento
    })

# ============================================================
# API INTEGRAL /api/points_integral — 3 CAPAS
# ============================================================
@app.route("/api/points_integral")
@login_required
def api_points_integral():
    dpto = request.args.get("departamento", "").upper().strip()
    prov = request.args.get("provincia", "").upper().strip()
    dist = request.args.get("distrito", "").upper().strip()
    divi = request.args.get("division", "").upper().strip()

    # ------------ ATMs ------------
    dfA = df.copy()
    dfA[COL_DEPT] = dfA[COL_DEPT].astype(str).str.upper().str.strip()
    dfA[COL_PROV] = dfA[COL_PROV].astype(str).str.upper().str.strip()
    dfA[COL_DIST] = dfA[COL_DIST].astype(str).str.upper().str.strip()
    dfA[COL_DIV]  = dfA[COL_DIV].astype(str).str.upper().str.strip()
    dfA[COL_UBIC] = dfA[COL_UBIC].astype(str).str.upper().str.strip()
    dfA[COL_TIPO] = dfA[COL_TIPO].astype(str).str.upper().str.strip()

    if dpto: dfA = dfA[dfA[COL_DEPT] == dpto]
    if prov: dfA = dfA[dfA[COL_PROV] == prov]
    if dist: dfA = dfA[dfA[COL_DIST] == dist]
    if divi: dfA = dfA[dfA[COL_DIV] == divi]

    puntos_atm = []
    suma_atm = float(dfA[PROM_COL].sum())
    for _, r in dfA.iterrows():
        lat = float(r[COL_LAT]); lon = float(r[COL_LON])
        nombre = str(r.get(COL_NAME, r.get(COL_ATM, "")))
        puntos_atm.append({
            "tipo_canal": "ATM",
            "lat": lat, "lon": lon,
            "atm": str(r.get(COL_ATM, "")),
            "nombre": nombre,
            "promedio": float(r.get(PROM_COL, 0.0)),
            "division": str(r.get(COL_DIV, "")),
            "tipo": str(r.get(COL_TIPO, "")),
            "ubicacion": str(r.get(COL_UBIC, "")),
            "departamento": str(r.get(COL_DEPT, "")),
            "provincia": str(r.get(COL_PROV, "")),
            "distrito": str(r.get(COL_DIST, "")),
            "direccion": get_address(lat, lon),
        })

    # ------------ OFICINAS ------------
    dfO = df_oficinas.copy()
    dfO[COLF_DEPT] = dfO[COLF_DEPT].astype(str).str.upper().str.strip()
    dfO[COLF_PROV] = dfO[COLF_PROV].astype(str).str.upper().str.strip()
    dfO[COLF_DIST] = dfO[COLF_DIST].astype(str).str.upper().str.strip()
    dfO[COLF_DIV]  = dfO[COLF_DIV].astype(str).str.upper().str.strip()

    if dpto: dfO = dfO[dfO[COLF_DEPT] == dpto]
    if prov: dfO = dfO[dfO[COLF_PROV] == prov]
    if dist: dfO = dfO[dfO[COLF_DIST] == dist]
    if divi: dfO = dfO[dfO[COLF_DIV] == divi]

    puntos_of = []
    suma_of = float(dfO[COLF_TRX].sum())

    total_of = int(len(dfO))
    suma_of_eas = float(dfO[COLF_EAS].sum()) if len(dfO) else 0.0
    suma_of_ebp = float(dfO[COLF_EBP].sum()) if len(dfO) else 0.0
    suma_of_ead = float(dfO[COLF_EAD].sum()) if len(dfO) else 0.0
    suma_of_cli = float(dfO[COLF_CLI].sum()) if len(dfO) else 0.0
    suma_of_tkt = float(dfO[COLF_TKT].sum()) if len(dfO) else 0.0

    # redlines promedio se mantiene
    prom_of_red = float(dfO[COLF_RED].mean()) if len(dfO) else 0.0

    for _, r in dfO.iterrows():
        puntos_of.append({
            "tipo_canal": "OFICINA",
            "lat": float(r[COLF_LAT]),
            "lon": float(r[COLF_LON]),
            "atm": str(r.get(COLF_ID, "")),
            "nombre": str(r.get(COLF_NAME, "")),
            "promedio": float(r.get(COLF_TRX, 0.0)),
            "division": str(r.get(COLF_DIV, "")),
            "tipo": "OFICINA",
            "ubicacion": "OFICINA",
            "departamento": str(r.get(COLF_DEPT, "")),
            "provincia": str(r.get(COLF_PROV, "")),
            "distrito": str(r.get(COLF_DIST, "")),
            "direccion": str(r.get(COLF_DIR, "")).strip(),
            "performance_2025": str(r.get(COLF_PERF, "")).strip(),
            "bai": float(r.get(COLF_BAI, 0.0)),
            "margen_neto": float(r.get(COLF_MARG, 0.0)),
            "estructura_as": float(r.get(COLF_EAS, 0.0)),
            "estructura_ebp": float(r.get(COLF_EBP, 0.0)),
            "estructura_ad": float(r.get(COLF_EAD, 0.0)),
            "clientes_unicos": int(r.get(COLF_CLI, 0)),
            "total_tickets": int(r.get(COLF_TKT, 0)),
            "red_lines": float(r.get(COLF_RED, 0.0)),
        })

    # ------------ AGENTES ------------
    dfG = df_agentes.copy()
    dfG[COLA_DEPT] = dfG[COLA_DEPT].astype(str).str.upper().str.strip()
    dfG[COLA_PROV] = dfG[COLA_PROV].astype(str).str.upper().str.strip()
    dfG[COLA_DIST] = dfG[COLA_DIST].astype(str).str.upper().str.strip()
    dfG[COLA_DIV]  = dfG[COLA_DIV].astype(str).str.upper().str.strip()
    dfG[COLA_CAPA] = dfG[COLA_CAPA].astype(str).str.upper().str.strip()

    if dpto: dfG = dfG[dfG[COLA_DEPT] == dpto]
    if prov: dfG = dfG[dfG[COLA_PROV] == prov]
    if dist: dfG = dfG[dfG[COLA_DIST] == dist]
    if divi: dfG = dfG[dfG[COLA_DIV] == divi]

    puntos_ag = []
    suma_ag = float(dfG[PROMA_COL].sum())
    for _, r in dfG.iterrows():
        puntos_ag.append({
            "tipo_canal": "AGENTE",
            "lat": float(r[COLA_LAT]),
            "lon": float(r[COLA_LON]),
            "atm": str(r.get(COLA_ID, "")),
            "nombre": str(r.get(COLA_COM, "")),
            "promedio": float(r.get(PROMA_COL, 0.0)),
            "division": str(r.get(COLA_DIV, "")),
            "tipo": "AGENTE",
            "ubicacion": "AGENTE",
            "departamento": str(r.get(COLA_DEPT, "")),
            "provincia": str(r.get(COLA_PROV, "")),
            "distrito": str(r.get(COLA_DIST, "")),
            "direccion": str(r.get(COLA_DIR, "")),
            "capa": str(r.get(COLA_CAPA, "")),
            "trxs_oct": float(r.get(COLA_TRX_OCT, 0.0)) if COLA_TRX_OCT else 0.0,
            "trxs_nov": float(r.get(COLA_TRX_NOV, 0.0)) if COLA_TRX_NOV else 0.0,
        })

    return jsonify({
        "atms": puntos_atm,
        "oficinas": puntos_of,
        "agentes": puntos_ag,
        "suma_atms": suma_atm,
        "suma_oficinas": suma_of,
        "suma_agentes": suma_ag,
        "total_atms": len(puntos_atm),
        "total_oficinas": len(puntos_of),
        "total_agentes": len(puntos_ag),

        "suma_ofi_estructura_as": suma_of_eas,
        "suma_ofi_estructura_ebp": suma_of_ebp,
        "suma_ofi_estructura_ad": suma_of_ead,
        "suma_ofi_clientes_unicos": suma_of_cli,
        "suma_ofi_total_tickets": suma_of_tkt,

        # Red Lines se mantiene como promedio
        "prom_ofi_redlines": prom_of_red,
    })


# ============================================================
# 8. TEMPLATE MAPA — FRONTEND COMPLETO (ORDENADO + COMPLETO)
# ✅ + NODOS: pin rojo y popup globo (click) + Panel Comercial
# ✅ + Panel Comercial: SOLO categorías (sin lista por TIPO)
# ✅ + Popup POIs: si NOMBRE no indica tipo, se muestra (TIPO) abajo
# ✅ + df_comercios: puntos + globo azul (SIN PARPADEO al hacer zoom) + Panel df_comercios
# ✅ + Heatmaps robustos (prende/apaga siempre) + INDEPENDIENTES (no se apagan entre sí)
# ✅ + FIX CRÍTICO: 2do/3er/10mo click en heatmaps SIEMPRE vuelve a dibujar (sin quedarse vacío)
# ✅ + "Mostrando" se actualiza con filtros y con capas activas (base + POIs + df_comercios)
# ============================================================

TEMPLATE_MAPA = """\
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Mapa BBVA — {% if tipo_mapa == 'islas' %} ATMs {% else %} {{ tipo_mapa|upper }} {% endif %}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel='stylesheet' href='https://unpkg.com/leaflet@1.9.4/dist/leaflet.css'/>
  <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
  <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>
  <style>
    :root{
      --bbva-blue:#1464A5;
      --bbva-dark:#072146;
      --muted:#6b7a8a;
      --card:#ffffff;
      --neon-blue:#1E6CFF;
    }
   html,body{
  margin:0; padding:0; height:100%;
  font-family:Inter,Arial,Helvetica,sans-serif;
  background:#eef4fb;
}

body{
  height:100vh;              /* ✅ ocupa toda la pantalla */
  display:flex;              /* ✅ layout vertical */
  flex-direction:column;
  overflow:hidden;           /* ✅ evita “espacio + scroll” de página */
}

    header{
      background:var(--bbva-dark);
      color:#fff;
      height:74px;                /* altura mayor que el logo */
      padding:0 16px;

      display:grid;
      grid-template-columns:200px 1fr 160px;
      align-items:center;
    }
    header h1{
      margin:0;
      font-size:22px;
      font-weight:800;
      text-align:center;
    }
    .logout{
      justify-self:end;
      background:#1f6fb2;
      padding:6px 10px;
      border-radius:6px;
      color:#fff;
      text-decoration:none;
      font-weight:700;
    }
    /* Logo BBVA en el header (esquina izquierda) */
    .header-logo{
      display:flex;
      align-items:center;
      justify-content:flex-start;
    }

    .header-logo img{
      height:70px;
      width:auto;
      max-width:240px;
      object-fit:contain;
      border-radius:8px;
    }
    .topbar{
  padding:16px 20px 8px 20px;
  flex:0 0 auto;            /* ✅ no crece */
}
    .controls{
      background:white; padding:12px; border-radius:12px;
      box-shadow:0 4px 16px rgba(0,0,0,0.12);
      display:flex; gap:12px; align-items:center; flex-wrap:wrap;
    }
    .controls label{ font-size:13px; color:var(--muted); display:flex; align-items:center; gap:6px; }
    select{ padding:6px 10px; border-radius:8px; border:1px solid #d0d7e3; }
    input[type="checkbox"]{ transform:scale(1.05); }
    .main{
  flex:1 1 auto;            /* ✅ ocupa TODO el espacio restante */
  min-height:0;             /* ✅ CLAVE: evita el “bloque vacío” */
  display:flex;
  padding:0 20px 20px 20px;
  gap:18px;
  box-sizing:border-box;
}
.map-wrap{
  flex:1 1 auto;
  min-width:0;
  height:100%;
  position:relative;          /* ✅ necesario para superponer la leyenda */
}

#map{
  width:100%;
  height:100%;
  border-radius:12px;
  overflow:hidden;
  box-shadow:0 8px 24px rgba(0,0,0,0.18);
}

/* ✅ Leyenda flotante sobre el mapa */
.global-legend.map-overlay{
  position:absolute;
  left:14px;
  bottom:14px;
  z-index:700;                /* arriba del mapa */
  width:260px;
  padding:10px 10px 8px 10px;
  background:rgba(255,255,255,0.92);
  backdrop-filter: blur(6px);
  -webkit-backdrop-filter: blur(6px);
  box-shadow:0 10px 26px rgba(7,33,70,0.18);
}
/* ===== Resaltado temporal de oficina seleccionada ===== */
.office-focus-icon{
  background:transparent;
  border:none;
}

.office-focus-wrap{
  width:58px;
  height:58px;
  border-radius:50%;
  background:rgba(20,100,165,0.18);
  border:4px solid #1464A5;
  box-shadow:
    0 0 0 8px rgba(20,100,165,0.12),
    0 0 18px rgba(20,100,165,0.45);
  animation: officePulse 1.6s ease-in-out infinite;
}

@keyframes officePulse{
  0%{
    transform:scale(0.96);
    box-shadow:
      0 0 0 4px rgba(20,100,165,0.18),
      0 0 12px rgba(20,100,165,0.35);
  }
  50%{
    transform:scale(1.06);
    box-shadow:
      0 0 0 10px rgba(20,100,165,0.10),
      0 0 24px rgba(20,100,165,0.55);
  }
  100%{
    transform:scale(0.96);
    box-shadow:
      0 0 0 4px rgba(20,100,165,0.18),
      0 0 12px rgba(20,100,165,0.35);
  }
}
.side{
  width:360px;
  height:100%;              /* ✅ igual altura que el mapa */
  min-height:0;
  overflow-y:auto;          /* ✅ scroll SOLO del panel derecho */
  display:flex;
  flex-direction:column;
  gap:12px;
}
    .side-card{
      background:linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
      border-radius:18px;
      padding:16px;
      box-shadow:0 10px 28px rgba(7,33,70,0.10);
      border:1px solid rgba(20,100,165,0.10);
      font-size:13px;
    }

    /* ===== Leyenda global del sidebar ===== */
    .global-legend{
      background:#fff;
      border:1px solid rgba(20,100,165,0.10);
      border-radius:18px;
      padding:12px 12px 10px 12px;
      box-shadow:0 8px 22px rgba(7,33,70,0.08);
    }

    .global-legend .gl-title{
      font-size:12px;
      font-weight:800;
      color:var(--bbva-dark);
      margin-bottom:8px;
    }

    .global-legend .gl-items{
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:8px 10px;
    }

    .global-legend .gl-item{
      display:flex;
      align-items:center;
      gap:10px;
      padding:8px 10px;
      border-radius:14px;
      background:#f7fbff;
      border:1px solid rgba(20,100,165,0.08);
    }

    .global-legend .gl-item img{
      width:24px;
      height:24px;
      object-fit:contain;
      background:#fff;
      border:1px solid #e6eef8;
      border-radius:10px;
      padding:4px;
    }

    .global-legend .gl-item span{
      font-size:12px;
      color:var(--muted);
      font-weight:700;
      line-height:1.1;
    }

    .side-title{
      font-weight:800;
      margin-bottom:4px;
      display:flex;
      align-items:center;
      gap:8px;
      font-size:15px;
      color:var(--bbva-dark);
    }

    .muted{
      color:var(--muted);
      font-size:12px;
      line-height:1.35;
    }

    /* ===== Dashboard summary panels ===== */
    .dash-panel{
      display:flex;
      flex-direction:column;
      gap:12px;
    }

    .dash-top{
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:10px;
    }

    .dash-kpi{
      background:#ffffff;
      border:1px solid rgba(20,100,165,0.10);
      border-radius:14px;
      padding:12px 12px 10px 12px;
      box-shadow:0 4px 12px rgba(7,33,70,0.05);
    }

    .dash-kpi .k-label{
      font-size:11px;
      color:var(--muted);
      margin-bottom:4px;
    }

    .dash-kpi .k-value{
      font-size:22px;
      font-weight:800;
      color:var(--bbva-dark);
      line-height:1.05;
    }

    .dash-section{
      background:#ffffff;
      border:1px solid rgba(20,100,165,0.10);
      border-radius:14px;
      padding:12px;
    }

    .dash-section-title{
      font-size:12px;
      font-weight:800;
      color:var(--bbva-dark);
      margin-bottom:8px;
    }

    .dash-grid-2{
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:8px 12px;
    }

    .dash-grid-3{
      display:grid;
      grid-template-columns:1fr 1fr 1fr;
      gap:8px;
    }

    .metric-mini{
      display:flex;
      flex-direction:column;
      gap:2px;
      padding:8px 10px;
      background:#f7fbff;
      border-radius:12px;
      border:1px solid rgba(20,100,165,0.08);
    }

    .metric-mini .m-label{
      font-size:11px;
      color:var(--muted);
    }

    .metric-mini .m-value{
      font-size:15px;
      font-weight:800;
      color:var(--bbva-dark);
    }

    .legend.compact{
      margin-top:0;
    }

    .legend.compact .legend-title{
      font-size:12px;
      font-weight:800;
      color:var(--bbva-dark);
      margin-bottom:8px;
    }

    .legend.compact .legend-item{
      display:flex;
      align-items:center;
      gap:10px;
      margin-top:6px;
    }

    .legend.compact .legend-item img{
      width:22px;
      height:22px;
      object-fit:contain;
      background:#fff;
      border:1px solid #e6eef8;
      border-radius:10px;
      padding:4px;
      box-shadow:none;
    }

    .legend.compact .lbl{
      color:var(--muted);
      font-size:12px;
      line-height:1.2;
    }
    .brand-card{ padding:10px; }
    .brand-card img{ width:100%; height:120px; object-fit:cover; border-radius:10px; display:block; }
    .legend{ margin-top:10px; }
    .legend .legend-item{ display:flex; align-items:center; gap:12px; margin-top:8px; }
    .legend .legend-item img{
      width:35px; height:35px; object-fit:contain;
      background:#fff; border:1px solid #e6eef8; border-radius:14px;
      padding:6px; box-shadow:0 3px 10px rgba(0,0,0,0.10);
    }
    .legend .legend-item .lbl{ color:var(--muted); font-size:12px; }
    .icon-reco { font-size: 30px; color: #ffcc00; text-shadow: 0 0 4px black; }
    .side-card-atm{
      font-family: Inter, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
      white-space:normal;
      line-height:1.35;
      border-left:4px solid var(--bbva-blue);
      position:relative;
    }
    .side-card-atm h3{ margin:0 0 10px 0; font-size:14px; }
    /* ====== Panel detalle (punto seleccionado / recomendación) ====== */
    .detail-panel{ display:flex; flex-direction:column; gap:10px; }
    .dp-head{
      display:flex; align-items:flex-start; justify-content:space-between; gap:10px;
      padding:10px 12px;
      border:1px solid rgba(7,33,70,0.10);
      background:#fff;
      border-radius:12px;
    }
    .dp-title{ display:flex; flex-direction:column; gap:2px; }
    .dp-title .overline{ font-size:11px; color:var(--muted); letter-spacing:0.3px; }
    .dp-title .name{ font-size:14px; font-weight:700; color:var(--bbva-dark); line-height:1.2; }
    .dp-title .sub{ font-size:12px; color:var(--muted); }

    .badge{
      display:inline-flex; align-items:center; gap:6px;
      padding:6px 10px;
      border-radius:999px;
      font-size:11px; font-weight:700;
      border:1px solid rgba(20,100,165,0.25);
      background:rgba(20,100,165,0.07);
      color:var(--bbva-blue);
      white-space:nowrap;
    }

    .dp-kpis{
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:8px;
    }
    .kpi{
      padding:10px 12px;
      border-radius:12px;
      border:1px solid rgba(7,33,70,0.10);
      background:#fff;
    }
    .kpi .lbl{ font-size:11px; color:var(--muted); }
    .kpi .val{ font-size:16px; font-weight:800; color:var(--bbva-dark); margin-top:2px; }

    .dp-section{
      padding:10px 12px;
      border-radius:12px;
      border:1px solid rgba(7,33,70,0.10);
      background:#fff;
    }
    .dp-section .sec-title{
      font-size:12px; font-weight:800; color:var(--bbva-dark);
      margin:0 0 8px 0;
    }
    .dp-rows{ display:flex; flex-direction:column; gap:6px; }
    .dp-row{
      display:flex; justify-content:space-between; gap:10px;
      font-size:12px;
    }
    .dp-row .k{ color:var(--muted); }
    .dp-row .v{ color:var(--bbva-dark); font-weight:600; text-align:right; }

    .dp-note{
      padding:10px 12px;
      border-radius:12px;
      border:1px dashed rgba(20,100,165,0.35);
      background:rgba(20,100,165,0.05);
      font-size:12px;
      color:var(--bbva-dark);
      line-height:1.35;
    }

    .btn-small{
      width:100%;
      margin-top:8px;
    }
    .btn-small{
      display:inline-block; margin-top:8px;
      padding:4px 10px; border-radius:6px;
      border:none; background:var(--bbva-blue);
      color:white; font-size:12px; cursor:pointer;
    }
    @keyframes panelGlow{
      0%{box-shadow:0 0 0 rgba(20,100,165,0.0);}
      50%{box-shadow:0 0 18px rgba(20,100,165,0.55);}
      100%{box-shadow:0 0 0 rgba(20,100,165,0.0);}
    }
    .side-card-atm.glow{ animation:panelGlow 2.2s ease-in-out infinite; }
    .hidden{ display:none; }
    .leaflet-popup-content-wrapper{ border-radius:12px; box-shadow:0 6px 20px rgba(0,0,0,0.25); }

    .division-neon{
      filter: drop-shadow(0 0 10px rgba(30,108,255,0.95))
              drop-shadow(0 0 22px rgba(30,108,255,0.70))
              drop-shadow(0 0 38px rgba(30,108,255,0.40));
    }

    /* ======================================================
       ✅ ZONAS RURAL/URBANA (borde neón)
       ====================================================== */
    .zone-box{
      padding:6px 10px;
      border-radius:12px;
      border:1px solid #d0d7e3;
      background:#f7fbff;
      box-shadow:0 3px 10px rgba(0,0,0,0.06);
      display:flex; align-items:center; gap:10px;
    }
    .zone-swatch{
      width:18px; height:18px;
      border-radius:6px;
      border:1px solid rgba(0,0,0,0.18);
      box-shadow:0 0 10px rgba(255,255,255,0.45);
      flex:0 0 auto;
    }
    .zone-swatch.rural{ background:#00FF66; box-shadow:0 0 12px rgba(0,255,102,0.9); }
    .zone-swatch.urban{ background:#D6FF00; box-shadow:0 0 12px rgba(214,255,0,0.9); }

    .zone-neon-rural{
      filter: drop-shadow(0 0 10px rgba(0,255,102,0.95))
              drop-shadow(0 0 22px rgba(0,255,102,0.70))
              drop-shadow(0 0 38px rgba(0,255,102,0.40));
    }
    .zone-neon-urban{
      filter: drop-shadow(0 0 10px rgba(214,255,0,0.95))
              drop-shadow(0 0 22px rgba(214,255,0,0.70))
              drop-shadow(0 0 38px rgba(214,255,0,0.40));
    }

    /* ======================================================
       ✅ COMERCIAL / NODOS — icono rojo + popup globo
       ====================================================== */
    .leaflet-div-icon.nodo-pin-icon{ background:transparent; border:none; }

    .nodo-pin-wrap{
      width:34px; height:34px;
      filter: drop-shadow(0 0 10px rgba(255,0,0,0.85))
              drop-shadow(0 0 18px rgba(255,40,0,0.55));
    }
    .nodo-pin-wrap svg{ width:34px; height:34px; display:block; }

    .leaflet-popup.nodo-popup .leaflet-popup-content-wrapper{
      background:transparent !important;
      box-shadow:none !important;
      padding:0 !important;
      border-radius:0 !important;
    }
    .leaflet-popup.nodo-popup .leaflet-popup-content{ margin:0 !important; }
    .leaflet-popup.nodo-popup .leaflet-popup-tip{
      background:transparent !important;
      box-shadow:none !important;
    }

    .nodo-balloon{
      position:relative;
      display:inline-block;
      max-width:320px;
      padding:12px 16px;
      background: radial-gradient(circle at 30% 25%, #ffb0b0 0%, #ff2a2a 35%, #b80000 100%);
      color:#fff;
      font-weight:900;
      font-size:14px;
      line-height:1.15;
      border-radius:18px;
      border:2px solid rgba(255,255,255,0.92);
      text-shadow: 0 1px 2px rgba(0,0,0,0.55);
      box-shadow: 0 0 18px rgba(255,0,0,0.85), 0 0 44px rgba(255,70,0,0.60);
      white-space: pre-line;
    }
    .nodo-balloon:after{
      content:"";
      position:absolute;
      left:50%;
      bottom:-12px;
      transform:translateX(-50%);
      width:0;height:0;
      border-left:12px solid transparent;
      border-right:12px solid transparent;
      border-top:14px solid #ff2a2a;
      filter: drop-shadow(0 0 12px rgba(255,0,0,0.95));
    }

    .nodo-cluster{
      width:44px; height:44px;
      border-radius:22px;
      background: radial-gradient(circle at 30% 25%, #ffb0b0 0%, #ff2a2a 40%, #b80000 100%);
      border:2px solid rgba(255,255,255,0.9);
      color:#fff;
      display:flex;
      align-items:center;
      justify-content:center;
      font-weight:900;
      box-shadow: 0 0 18px rgba(255,0,0,0.85), 0 0 40px rgba(255,70,0,0.55);
      text-shadow: 0 1px 2px rgba(0,0,0,0.55);
    }

    /* ======================================================
       ✅ df_comercios — icono + globo azul (cool) (SIN PARPADEO)
       ====================================================== */
    .leaflet-div-icon.com-pin-icon{ background:transparent; border:none; }

    .com-pin-wrap{
      width:38px; height:38px;
      filter: drop-shadow(0 0 10px rgba(0,120,255,0.85))
              drop-shadow(0 0 18px rgba(0,160,255,0.55));
    }
    .com-pin-wrap svg{ width:38px; height:38px; display:block; }

    .leaflet-popup.com-popup .leaflet-popup-content-wrapper{
      background:transparent !important;
      box-shadow:none !important;
      padding:0 !important;
      border-radius:0 !important;
    }
    .leaflet-popup.com-popup .leaflet-popup-content{ margin:0 !important; }
    .leaflet-popup.com-popup .leaflet-popup-tip{
      background:transparent !important;
      box-shadow:none !important;
    }

    .com-balloon{
      position:relative;
      display:inline-block;
      max-width:340px;
      padding:12px 16px;
      background: radial-gradient(circle at 30% 25%, #b9e3ff 0%, #1e6cff 38%, #003a8c 100%);
      color:#fff;
      font-weight:900;
      font-size:13px;
      line-height:1.2;
      border-radius:18px;
      border:2px solid rgba(255,255,255,0.92);
      text-shadow: 0 1px 2px rgba(0,0,0,0.55);
      box-shadow: 0 0 18px rgba(0,120,255,0.85), 0 0 44px rgba(0,160,255,0.60);
    }
    .com-balloon:after{
      content:"";
      position:absolute;
      left:50%;
      bottom:-12px;
      transform:translateX(-50%);
      width:0;height:0;
      border-left:12px solid transparent;
      border-right:12px solid transparent;
      border-top:14px solid #1e6cff;
      filter: drop-shadow(0 0 12px rgba(0,120,255,0.95));
    }

    .com-cluster{
      width:46px; height:46px;
      border-radius:23px;
      background: radial-gradient(circle at 30% 25%, #b9e3ff 0%, #1e6cff 40%, #003a8c 100%);
      border:2px solid rgba(255,255,255,0.9);
      color:#fff;
      display:flex;
      align-items:center;
      justify-content:center;
      font-weight:900;
      box-shadow: 0 0 18px rgba(0,120,255,0.85), 0 0 40px rgba(0,160,255,0.55);
      text-shadow: 0 1px 2px rgba(0,0,0,0.55);
    }

    /* ======================================================
   ✅ HALO TRX en el ÍCONO (solo visible cuando Heatmap está ON)
   ====================================================== */
    .leaflet-div-icon.trx-div-icon{
      background: transparent !important;
      border: none !important;
    }

    .trx-wrap{
      width: 72px;
      height: 72px;
      border-radius: 999px;
      display:flex;
      align-items:center;
      justify-content:center;
      box-sizing:border-box;

      /* OFF por defecto */
      border: 7px solid transparent;
      box-shadow: none;
    }

    .trx-wrap img{
      width: 58px;
      height: 58px;
      object-fit: contain;
      display:block;
      filter: drop-shadow(0 0 6px rgba(0,0,0,0.35));
    }

    /* ✅ Solo cuando el heatmap está prendido */
    body.heat-on .trx-wrap.trx-y{
      border-color:#FFF200;
      box-shadow: 0 0 10px rgba(255,242,0,0.95),
                  0 0 22px rgba(255,242,0,0.55),
                  0 0 42px rgba(255,242,0,0.25);
    }
    body.heat-on .trx-wrap.trx-o{
      border-color:#FF8C00;
      box-shadow: 0 0 12px rgba(255,140,0,0.95),
                  0 0 26px rgba(255,140,0,0.55),
                  0 0 48px rgba(255,140,0,0.25);
    }
    body.heat-on .trx-wrap.trx-r{
      border-color:#FF2A00;
      box-shadow: 0 0 12px rgba(255,42,0,0.95),
                  0 0 28px rgba(255,42,0,0.55),
                  0 0 54px rgba(255,42,0,0.28);
    }
    /* ✅ OCULTAR SOLO UI DE ZONAS (checkboxes en la barra) */
.zone-box{
  display:none !important;
}

/* ✅ OCULTAR SOLO UI DE ZONAS (panel derecho) */
#panelZonasLegend{
  display:none !important;
}


/* ✅ OCULTAR SOLO UI: checkbox "Heatmap Clientes" */
#chkHeatClientes{
  display:none !important;
}

/* como el input está dentro de un label, ocultamos el label completo */
#chkHeatClientes{
  display:none !important;
}
#chkHeatClientes:where(*){ display:none !important; }
#chkHeatClientes{
  visibility:hidden !important;
}
#chkHeatClientes{
  position:absolute !important;
  left:-99999px !important;
}

/* ✅ esto es lo importante: oculta el label que contiene el checkbox */
#chkHeatClientes{
  display:none !important;
}
#chkHeatClientes{
  pointer-events:none !important;
}
#chkHeatClientes{
  opacity:0 !important;
}

/* ✅ ocultar el LABEL contenedor */
#chkHeatClientes{
  display:none !important;
}
#chkHeatClientes{
  -webkit-appearance:none !important;
}
#chkHeatClientes{
  appearance:none !important;
}

/* ✅ LA FORMA CORRECTA: ocultar el label padre */
#chkHeatClientes{
  display:none !important;
}
label:has(#chkHeatClientes){
  display:none !important;
}
/* ✅ OCULTAR SOLO UI: panel "Clientes visibles" */
#panelClientes{
  display:none !important;
}
#empNomSector,
#empNomArea{
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
  word-break: break-word;
}

#panelEmpresasNomina .metric-mini .m-value{
  font-size: 22px;
  line-height: 1.15;
}

#panelEmpresasNomina .metric-mini .m-label{
  min-height: 30px;
}
  </style>
</head>

<body>
  <header>
    <div class="header-logo">
      <img src="{{ url_for('static', filename='banco.png') }}" alt="BBVA">
    </div>
    <h1>TRI - TERRITORIAL INTELIGENCE</h1>
    <a href="/logout" class="logout">Cerrar sesión</a>
  </header>

  <div class="topbar">
    <div class="controls">
      <label>Departamento:
        <select id="selDepartamento">
          <option value="">-- Todos --</option>
          {% for d in departamentos %}
            <option value="{{d}}">{{d}}</option>
          {% endfor %}
        </select>
      </label>

      <label>Provincia:
        <select id="selProvincia">
          <option value="">-- Todas --</option>
          {% for p in all_provincias %}
            <option value="{{p}}">{{p}}</option>
          {% endfor %}
        </select>
      </label>

      <label>Distrito:
        <select id="selDistrito">
          <option value="">-- Todos --</option>
          {% for d in all_distritos %}
            <option value="{{d}}">{{d}}</option>
          {% endfor %}
        </select>
      </label>

      <label>División:
        <select id="selDivision">
          <option value="">-- Todas --</option>
          {% for dv in divisiones %}
            <option value="{{dv}}">{{dv}}</option>
          {% endfor %}
        </select>
      </label>

      <label>Oficina:
        <select id="selOficina">
          <option value="">-- Ubicar oficina --</option>
          {% for ofi in oficinas_lookup %}
            <option value="{{ loop.index0 }}">{{ ofi.nombre }}</option>
          {% endfor %}
        </select>
      </label>

      {% if tipo_mapa == 'islas' %}
      <label>Tipo ATM:
        <select id="selTipoATM">
          <option value="">-- Todos --</option>
          <option value="DISPENSADOR">Dispensador</option>
          <option value="MONEDERO">Monedero</option>
          <option value="RECICLADOR">Reciclador</option>
        </select>
      </label>

      <label>Ubicación:
        <select id="selUbicacionATM">
          <option value="">-- Todas --</option>
          <option value="OFICINA">Oficina</option>
          <option value="ISLA">Isla</option>
        </select>
      </label>
      {% endif %}

      {% if tipo_mapa == 'integral' %}
      <label style="margin-left:8px;">Canales:
        <span style="display:flex; gap:10px; margin-left:6px;">
          <label style="gap:4px;"><input type="checkbox" id="chkShowATMs" checked> ATMs</label>
          <label style="gap:4px;"><input type="checkbox" id="chkShowOficinas" checked> Oficinas</label>
          <label style="gap:4px;"><input type="checkbox" id="chkShowAgentes" checked> Agentes</label>
        </span>
      </label>
      {% endif %}

      <!-- Heatmaps + df_comercios + recomendaciones + POIs -->
      <label style="margin-left:16px;"><input type="checkbox" id="chkHeat" checked> Heatmap Trx Canales</label>
      <label style="margin-left:16px;"><input type="checkbox" id="chkHeatClientes"> Heatmap Clientes</label>
      <label style="margin-left:16px;"><input type="checkbox" id="chkHeatCantClientes"> Heatmap Cantidad de Clientes</label>
      <label style="margin-left:16px;"><input type="checkbox" id="chkComerciosPts"> Comercios Usados por Clientes</label>
      <label style="margin-left:16px;"><input type="checkbox" id="chkReco"> Recomendaciones</label>
      <label style="margin-left:16px;"><input type="checkbox" id="chkEmpresasNomina"> Empresas Nómina</label>
      <label style="margin-left:16px;"><input type="checkbox" id="chkHeatEmpresasNomina"> Heatmap Oportunidad Empresas</label>

      <!-- ✅ COMERCIAL / NODOS -->
      <label style="margin-left:16px;"><input type="checkbox" id="chkNodos"> Puntos Comerciales(Tráfico Personas)</label>

      <!-- ✅ ZONAS -->
      <div class="zone-box" style="margin-left:16px;">
        <span style="color:var(--muted); font-size:13px; font-weight:700;">Zonas:</span>
        <label style="gap:4px; margin:0;"><input type="checkbox" id="chkZonaRural"> Rural</label>
        <label style="gap:4px; margin:0;"><input type="checkbox" id="chkZonaUrbana"> Urbana</label>
      </div>

      <div style="flex:1"></div>
      <div style="font-size:13px; color:var(--muted);">
        Mostrando <span id="infoCount">--</span> {% if tipo_mapa == 'integral' %} puntos {% else %} registros {% endif %}
      </div>
    </div>
  </div>

  <div class="main">
    <div class="map-wrap">
      <div id="map"></div>

      <!-- ✅ Leyenda flotante sobre el mapa -->
      <div id="mapLegend" class="global-legend map-overlay">
        <div class="gl-title">Leyenda</div>
        <div class="gl-items">
          <div class="gl-item">
            <img src="{{ url_for('static', filename='atm_oficina1.png') }}" alt="ATM en Oficina">
            <span>ATM en Oficina</span>
          </div>
          <div class="gl-item">
            <img src="{{ url_for('static', filename='atm_isla1.png') }}" alt="ATM en Isla">
            <span>ATM en Isla</span>
          </div>
          <div class="gl-item">
            <img src="{{ url_for('static', filename='oficina1.png') }}" alt="Oficina">
            <span>Oficina</span>
          </div>
          <div class="gl-item">
            <img src="{{ url_for('static', filename='agente1.png') }}" alt="Agente">
            <span>Agente</span>
          </div>
        </div>
      </div>
    </div>

    <div class="side">


      <!-- ✅ PANEL ZONAS -->
      <div id="panelZonasLegend" class="side-card">
        <div class="side-title">🗺️ Zonas (Rural / Urbana)</div>
        <div class="muted">Bordes neón por filtros (Departamento / Provincia / Distrito). Funciona en las 4 capas.</div>

        <div class="legend">
          <div style="font-weight:700;">Leyenda</div>

          <div class="legend-item">
            <div class="zone-swatch rural"></div>
            <div class="lbl">Rural — verde fosforescente (<span id="zonaRuralCount">0</span>)</div>
          </div>

          <div class="legend-item">
            <div class="zone-swatch urban"></div>
            <div class="lbl">Urbana — amarillo fosforescente (<span id="zonaUrbanCount">0</span>)</div>
          </div>
        </div>
      </div>


      <!-- ✅ PANEL EMPRESAS NÓMINA -->
<div id="panelEmpresasNomina" class="side-card hidden">
  <div class="dash-panel">
    <div>
      <div class="side-title">🏢 Empresas Nómina</div>
      <div class="muted">Se actualiza con filtros y solo cuenta si “Empresas Nómina” o “Heatmap Oportunidad Empresas” está activado.</div>
    </div>

    <div class="dash-top">
      <div class="dash-kpi">
        <div class="k-label">Empresas</div>
        <div class="k-value" id="empNomTotal">0</div>
      </div>
      <div class="dash-kpi">
        <div class="k-label">Trabajadores</div>
        <div class="k-value" id="empNomTrab">0</div>
      </div>
    </div>

    <div class="dash-section">
      <div class="dash-section-title">Penetración BBVA</div>
      <div class="dash-grid-3">
        <div class="metric-mini">
          <div class="m-label">Stock nómina</div>
          <div class="m-value" id="empNomStock">0</div>
        </div>
        <div class="metric-mini">
          <div class="m-label">Penetración</div>
          <div class="m-value" id="empNomPen">0%</div>
        </div>
        <div class="metric-mini">
          <div class="m-label">Share wallet</div>
          <div class="m-value" id="empNomShare">0%</div>
        </div>
      </div>
    </div>

    <div class="dash-section">
      <div class="dash-section-title">Dominancia</div>
      <div class="dash-grid-2">
        <div class="metric-mini">
          <div class="m-label">Área dominante</div>
          <div class="m-value" id="empNomArea">—</div>
        </div>
        <div class="metric-mini">
          <div class="m-label">Sector dominante</div>
          <div class="m-value" id="empNomSector">—</div>
        </div>
      </div>
    </div>

    <div class="dash-section">
      <div class="dash-section-title">Saldos</div>
      <div class="dash-grid-2">
        <div class="metric-mini">
          <div class="m-label">Saldo BBVA</div>
          <div class="m-value" id="empNomSaldoBbva">0</div>
        </div>
        <div class="metric-mini">
          <div class="m-label">Saldo sistema</div>
          <div class="m-value" id="empNomSaldoSystem">0</div>
        </div>
      </div>
    </div>
  </div>
</div>

      <!-- ✅ PANEL COMERCIAL (POIs) -->
      <div id="panelComercial" class="side-card hidden">
        <div class="side-title">🏪 Panel Comercial</div>
        <div class="muted">Se actualiza con filtros y solo cuenta si “Comercial (POIs)” está activado.</div>

        <div style="margin-top:8px;"><b>Total POIs:</b> <span id="comTotal">0</span></div>

        <div style="margin-top:10px; font-weight:800;">Conteo por tipo (categorías)</div>
        <div class="muted">Hospitales: <span id="comHosp">0</span></div>
        <div class="muted">Clínicas: <span id="comClin">0</span></div>
        <div class="muted">Centros comerciales: <span id="comCC">0</span></div>
        <div class="muted">Plaza Vea: <span id="comPV">0</span></div>
        <div class="muted">Sodimac: <span id="comSod">0</span></div>
        <div class="muted">Metro: <span id="comMet">0</span></div>
        <div class="muted">Tottus: <span id="comTot">0</span></div>
        <div class="muted">Wong: <span id="comWon">0</span></div>
        <div class="muted">Universidades: <span id="comUni">0</span></div>
        <div class="muted">Mercados: <span id="comMer">0</span></div>

        <div class="legend">
          <div style="font-weight:700;">Leyenda</div>
          <div class="legend-item">
            <div class="nodo-pin-wrap" style="transform:scale(0.85);">
              <svg viewBox="0 0 24 24" fill="#ff2a2a" stroke="#ffffff" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                <path d="M12 22s7-4.5 7-12a7 7 0 0 0-14 0c0 7.5 7 12 7 12z"></path>
                <circle cx="12" cy="10" r="2.7" fill="#ffffff" stroke="none"></circle>
              </svg>
            </div>
            <div class="lbl">POI Comercial (click para ver NOMBRE / (TIPO) si falta)</div>
          </div>
        </div>
      </div>

      <!--
      <div id="panelDfComercios" class="card panel-df-comercios" style="display:none;">
        <div class="panel-title">
          📌 Panel df_comercios
        </div>

        <div class="panel-note">
          Se actualiza con filtros y solo cuenta si "df_comercios (puntos)" está activado.
        </div>

        <div class="panel-stats">
          <div class="stat-row">
            <span>Total puntos:</span>
            <span id="dfcTotal">0</span>
          </div>
          <div class="stat-row">
            <span>Suma cant_clientes:</span>
            <span id="dfcSumClientes">0</span>
          </div>
          <div class="stat-row">
            <span>Promedio cant_clientes:</span>
            <span id="dfcAvgClientes">0</span>
          </div>
        </div>
      </div>
      -->

      <div id="panelATMResumen" class="side-card hidden">
        <div class="dash-panel">
          <div>
            <div class="side-title">🌐 Panel ATMs</div>
            <div class="muted">Se actualiza con filtros y solo cuenta si ATMs está activado.</div>
          </div>

          <div class="dash-top">
            <div class="dash-kpi">
              <div class="k-label">Total ATMs</div>
              <div class="k-value" id="resAtmTotal">0</div>
            </div>
            <div class="dash-kpi">
              <div class="k-label">Suma TRX</div>
              <div class="k-value" id="resAtmSuma">0</div>
            </div>
          </div>

          <div class="dash-section">
            <div class="dash-section-title">Distribución</div>
            <div class="dash-grid-2">
              <div class="metric-mini">
                <div class="m-label">ATM en oficina</div>
                <div class="m-value" id="resAtmEnOfi">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">ATM en isla</div>
                <div class="m-value" id="resAtmEnIsla">0</div>
              </div>
            </div>
          </div>

          <div class="dash-section">
            <div class="dash-section-title">Tipos</div>
            <div class="dash-grid-3">
              <div class="metric-mini">
                <div class="m-label">Dispensador</div>
                <div class="m-value" id="resAtmDisp">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">Monedero</div>
                <div class="m-value" id="resAtmMon">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">Reciclador</div>
                <div class="m-value" id="resAtmRec">0</div>
              </div>
            </div>
          </div>

        </div>
      </div>

      <div id="panelOfiResumen" class="side-card hidden">
        <div class="dash-panel">
          <div>
            <div class="side-title">🏦 Panel Oficinas</div>
            <div class="muted">Se actualiza con filtros y solo cuenta si Oficinas está activado.</div>
          </div>

          <div class="dash-top">
            <div class="dash-kpi">
              <div class="k-label">Total Oficinas</div>
              <div class="k-value" id="resOfiTotal">0</div>
            </div>
            <div class="dash-kpi">
              <div class="k-label">Suma TRX</div>
              <div class="k-value" id="resOfiSuma">0</div>
            </div>
          </div>

          <div class="dash-section">
            <div class="dash-section-title">Estructura</div>
            <div class="dash-grid-3">
              <div class="metric-mini">
                <div class="m-label">Total AS</div>
                <div class="m-value" id="resOfiPromEAS">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">Total EBP</div>
                <div class="m-value" id="resOfiPromEBP">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">Total AD</div>
                <div class="m-value" id="resOfiPromEAD">0</div>
              </div>
            </div>
          </div>

          <div class="dash-section">
            <div class="dash-section-title">Indicadores</div>
            <div class="dash-grid-2">
              <div class="metric-mini">
                <div class="m-label">Clientes únicos</div>
                <div class="m-value" id="resOfiPromCLI">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">Total tickets</div>
                <div class="m-value" id="resOfiPromTKT">0</div>
              </div>
              <div class="metric-mini" style="grid-column:1 / -1;">
                <div class="m-label">Prom. Red Lines</div>
                <div class="m-value" id="resOfiPromRED">0%</div>
              </div>
            </div>
          </div>

        </div>
      </div>

      <div id="panelAgResumen" class="side-card hidden">
        <div class="dash-panel">
          <div>
            <div class="side-title">🧍 Panel Agentes</div>
            <div class="muted">Se actualiza con filtros y solo cuenta si Agentes está activado.</div>
          </div>

          <div class="dash-top">
            <div class="dash-kpi">
              <div class="k-label">Total Agentes</div>
              <div class="k-value" id="resAgTotal">0</div>
            </div>
            <div class="dash-kpi">
              <div class="k-label">Suma TRX</div>
              <div class="k-value" id="resAgSuma">0</div>
            </div>
          </div>

          <div class="dash-section">
            <div class="dash-section-title">Capas</div>
            <div class="dash-grid-3">
              <div class="metric-mini">
                <div class="m-label">A1</div>
                <div class="m-value" id="resAgA1">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">A2</div>
                <div class="m-value" id="resAgA2">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">A3</div>
                <div class="m-value" id="resAgA3">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">B</div>
                <div class="m-value" id="resAgB">0</div>
              </div>
              <div class="metric-mini">
                <div class="m-label">C</div>
                <div class="m-value" id="resAgC">0</div>
              </div>
            </div>
          </div>

        </div>
      </div>

      <div id="panelClientes" class="side-card hidden">
        <div class="side-title">Clientes visibles</div>
        <div class="muted">Total clientes: <span id="cliTotal">0</span></div>
        <div class="muted">% digitales: <span id="cliDigital">0%</span></div>
        <div class="muted">Edad promedio: <span id="cliEdad">0</span></div>
        <div class="muted">Ingreso promedio: <span id="cliIngreso">0</span></div>
        <div class="muted">Deuda promedio: <span id="cliDeuda">0</span></div>
        <div class="muted">Top segmento: <span id="cliTopSeg">—</span></div>
      </div>

      <div id="panelATM" class="side-card side-card-atm hidden">
        <h3 id="panelATMTitle">Panel del punto seleccionado</h3>
        <div id="atmDetalle" style="font-size:12px;"></div>
        <button id="btnVolver" class="btn-small">VOLVER</button>
      </div>

      <div id="panelReco" class="side-card side-card-atm hidden">
        <h3 id="recoTitle">Recomendación</h3>
        <div id="recoDetalle" style="font-size:12px;"></div>
        <button id="btnRecoVolver" class="btn-small">VOLVER</button>
      </div>
    </div>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
  <script src="https://unpkg.com/leaflet.heat/dist/leaflet-heat.js"></script>

  

  <script>
    // ======================================================
    // DATA INICIAL (Jinja)
    // ======================================================
    const PROV_BY_DEPT = {{ provincias_by_dept|tojson }};
    const DIST_BY_PROV = {{ dist_by_prov|tojson }};
    const DIV_BY_DEPT  = {{ div_by_dept|tojson }};
    const DIV_BY_PROV  = {{ div_by_prov|tojson }};
    const DIV_BY_DIST  = {{ div_by_dist|tojson }};

    const ALL_PROVINCIAS = {{ all_provincias|tojson }};
    const ALL_DISTRITOS  = {{ all_distritos|tojson }};

    const TIPO_MAPA = "{{ tipo_mapa }}";

    const OFICINAS_LOOKUP = {{ oficinas_lookup|tojson }};

    const INITIAL_CENTER = [{{ initial_center[0] }}, {{ initial_center[1] }}];
    const INITIAL_ZOOM = {{ initial_zoom }};

    const ICON_ATM_OFICINA_URL = "{{ url_for('static', filename='atm_oficina1.png') }}";
    const ICON_ATM_ISLA_URL    = "{{ url_for('static', filename='atm_isla1.png') }}";
    const ICON_OFICINA_URL     = "{{ url_for('static', filename='oficina1.png') }}";
    const ICON_AGENTE_URL      = "{{ url_for('static', filename='agente1.png') }}";

    const ICON_SIZE = 72;
    const ICON_ANCH = ICON_SIZE / 2;
    const POP_ANCH  = -ICON_ANCH;

    const ICON_ATM_OFICINA = L.icon({ iconUrl: ICON_ATM_OFICINA_URL, iconSize:[ICON_SIZE,ICON_SIZE], iconAnchor:[ICON_ANCH,ICON_ANCH], popupAnchor:[0,POP_ANCH] });
    const ICON_ATM_ISLA    = L.icon({ iconUrl: ICON_ATM_ISLA_URL,    iconSize:[ICON_SIZE,ICON_SIZE], iconAnchor:[ICON_ANCH,ICON_ANCH], popupAnchor:[0,POP_ANCH] });
    const ICON_OFICINA     = L.icon({ iconUrl: ICON_OFICINA_URL,     iconSize:[ICON_SIZE,ICON_SIZE], iconAnchor:[ICON_ANCH,ICON_ANCH], popupAnchor:[0,POP_ANCH] });
    const ICON_AGENTE      = L.icon({ iconUrl: ICON_AGENTE_URL,      iconSize:[ICON_SIZE,ICON_SIZE], iconAnchor:[ICON_ANCH,ICON_ANCH], popupAnchor:[0,POP_ANCH] });


    // ======================================================
// ✅ HALO TRX — ícono con borde por TRX (cacheado)
// Reglas:
//   < 900     => amarillo
//   900..1999 => naranja
//   >= 2000   => rojo
// ======================================================
function trxValue(pt){
  const v =
    pt?.promedio ??
    pt?.trx ??
    pt?.trxs_nov ??
    pt?.trxs_oct ??
    0;
  const n = Number(v);
  return isFinite(n) ? n : 0;
}

function trxClassFromVal(trx){
  if(trx < 900) return "trx-y";
  if(trx < 2000) return "trx-o";
  return "trx-r";
}

function baseIconUrlFromPoint(pt){
  const canal = String(pt?.tipo_canal || "").toUpperCase();

  // integral: usa tipo_canal si existe
  if(canal === "AGENTE" || TIPO_MAPA === "agentes") return ICON_AGENTE_URL;
  if(canal === "OFICINA" || TIPO_MAPA === "oficinas") return ICON_OFICINA_URL;

  // ATM (islas / integral)
  const ubic = String(pt?.ubicacion || "").toUpperCase();
  if(ubic.includes("OFICINA")) return ICON_ATM_OFICINA_URL;
  return ICON_ATM_ISLA_URL;
}

const TRX_ICON_CACHE = new Map();

function getTrxHaloIcon(pt){
  const imgUrl = baseIconUrlFromPoint(pt);
  const trxCls = trxClassFromVal(trxValue(pt));
  const key = `${imgUrl}|${trxCls}`;

  if(TRX_ICON_CACHE.has(key)) return TRX_ICON_CACHE.get(key);

  const html = `<div class="trx-wrap ${trxCls}"><img src="${imgUrl}" alt=""></div>`;

  const ic = L.divIcon({
    className: "trx-div-icon",
    html,
    iconSize: [ICON_SIZE, ICON_SIZE],
    iconAnchor: [ICON_ANCH, ICON_ANCH],
    popupAnchor: [0, POP_ANCH],
  });

  TRX_ICON_CACHE.set(key, ic);
  return ic;
}




    function getIcon(pt){
      const ubic = (pt.ubicacion || "").toUpperCase();
      if (TIPO_MAPA === "agentes") return ICON_AGENTE;
      if (TIPO_MAPA === "oficinas") return ICON_OFICINA;
      if (TIPO_MAPA === "islas"){
        if (ubic.includes("OFICINA")) return ICON_ATM_OFICINA;
        if (ubic.includes("ISLA")) return ICON_ATM_ISLA;
        return ICON_ATM_ISLA;
      }
      return ICON_ATM_ISLA;
    }



    const fmt2 = (v)=> {
      const n = Number(v || 0);
      return (isFinite(n) ? n : 0).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };

    const fmt0 = (v)=> {
      const n = Math.round(toNumberIntl(v));
      return (isFinite(n) ? n : 0).toLocaleString("en-US");
    };

    const fmtPct = (v)=> {
      const n = Number(v || 0);
      return `${(isFinite(n) ? n : 0).toFixed(2)}%`;
    };

    function toNumberIntl(v){
  if(v === null || v === undefined) return 0;
  if(typeof v === "number") return isFinite(v) ? v : 0;

  let s = String(v).trim();
  if(!s) return 0;
  s = s.replace(/\s/g, "");
  s = s.replace(/[^\d\-\.,]/g, "");

  // miles con puntos: -665.254 / 3.406.481
  if(!s.includes(",") && /^-?\d{1,3}(\.\d{3})+$/.test(s)){
    s = s.replace(/\./g, "");
  }
  // ambos: '.' miles y ',' decimal
  else if(s.includes(".") && s.includes(",")){
    s = s.replace(/\./g, "").replace(",", ".");
  }
  // solo coma => decimal
  else if(s.includes(",") && !s.includes(".")){
    s = s.replace(",", ".");
  }

  const n = Number(s);
  return isFinite(n) ? n : 0;
}

function fmtKM(v){
  const n = Math.abs(toNumberIntl(v)); // sin signo
  if(n >= 1e6) return `${Math.round(n/1e6)}MM`; // 👈 AQUÍ
  if(n >= 1e3) return `${Math.round(n/1e3)}K`;
  return `${Math.round(n)}`;
}

    const esc = (s) => String(s ?? "")
    .replace(/&/g,"&amp;")
    .replace(/</g,"&lt;")
    .replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;")
    .replace(/'/g,"&#39;");

    function escHtml(s){
      return String(s||"").replace(/[&<>"']/g, (c)=>({
        "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"
      }[c]));
    }

    // ======================================================
    // MAPA + PANES
    // ======================================================
    const map = L.map('map').setView(INITIAL_CENTER, INITIAL_ZOOM);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{ maxZoom:19 }).addTo(map);
    // ✅ Evita que al interactuar con la leyenda se mueva el mapa
    const mapLegend = document.getElementById("mapLegend");
    if(mapLegend){
      L.DomEvent.disableClickPropagation(mapLegend);
      L.DomEvent.disableScrollPropagation(mapLegend);
    }
    requestAnimationFrame(() => map.invalidateSize());
    window.addEventListener("resize", () => map.invalidateSize());


    map.createPane('zonesPane');
    map.getPane('zonesPane').style.zIndex = 380;
    // ✅ panes para que los heatmaps NO se pisen (ni cambien colores por overlay)
map.createPane('heatBasePane');
map.getPane('heatBasePane').style.zIndex = 385;

map.createPane('heatClientesPane');
map.getPane('heatClientesPane').style.zIndex = 386;

map.createPane('heatCantPane');
map.getPane('heatCantPane').style.zIndex = 387;

map.createPane('heatEmpresasPane');
map.getPane('heatEmpresasPane').style.zIndex = 388;

// ✅ Pane exclusivo para HALO (glow) por encima de los íconos
map.createPane('heatGlowPane');
map.getPane('heatGlowPane').style.zIndex = 620;          // > markerPane (600)
map.getPane('heatGlowPane').style.pointerEvents = "none";
map.getPane('heatGlowPane').style.mixBlendMode = "screen";


    // ======================================================
    // LAYERS BASE
    // ======================================================
    const markers = L.markerClusterGroup({ chunkedLoading:true });
    // ===== Estado para ubicar/resaltar oficina =====
    let currentOfficeFocusMarker = null;
    let currentOfficeFocusCircle = null;
    let latestOficinasData = [];

// ✅ Un SOLO gradiente (como dashboard de tu captura)
// ======================================================
// ✅ HEATMAP (INTEGRAL y demás): usa PROMEDIO/TRX como PESO
// ✅ Colores: AMARILLO → NARANJA → ROJO + brillo alrededor (glow)
//   - "heat"   : mancha controlada
//   - "heatGlow": halo fuerte y pequeño alrededor del punto
// ======================================================

// ✅ gradiente solo amarillo/naranja/rojo
const HEAT_GRADIENT_YOR = {
  0.00: "rgba(0,0,0,0)",
  0.35: "#FFF200", // amarillo
  0.70: "#FF9A00", // naranja
  1.00: "#FF2A00"  // rojo-naranja
};

// ✅ brillo tipo dashboard SIN pintar todo el mapa
const HEAT_PANE_FILTER =
  "saturate(1.15) brightness(1.03) " +
  "drop-shadow(0 0 10px rgba(255,140,0,0.55)) " +
  "drop-shadow(0 0 22px rgba(255,40,0,0.35))";

const HEAT_PANE_OPACITY = 0.92;

// ✅ capa “base” (no tan grande para no manchar)
const HEAT_BASE_OPTS = {
  pane: "heatBasePane",
  radius: 22,
  blur: 16,
  maxZoom: 18,
  minOpacity: 0.14,
  max: 1.0,
  gradient: HEAT_GRADIENT_YOR
};

// ✅ halo (glow) alrededor del punto (más pequeño + más opaco)
const HEAT_GLOW_OPTS = {
  pane: "heatGlowPane",   // ✅ CLAVE
  radius: 32,             // ✅ mayor para que se vea con ICON_SIZE=72
  blur: 18,
  maxZoom: 18,
  minOpacity: 0.30,
  max: 1.0,
  gradient: HEAT_GRADIENT_YOR
};


const heat = L.heatLayer([], HEAT_BASE_OPTS);
const heatGlow = L.heatLayer([], HEAT_GLOW_OPTS);

// ✅ evita que al reactivar vuelva a “default”
function applyHeatBaseStyle(){
  const o1 = { ...HEAT_BASE_OPTS };
  const o2 = { ...HEAT_GLOW_OPTS };

  if(typeof heat.setOptions === "function") heat.setOptions(o1);
  else Object.assign(heat.options, o1);

  if(typeof heatGlow.setOptions === "function") heatGlow.setOptions(o2);
  else Object.assign(heatGlow.options, o2);
}
applyHeatBaseStyle();

// ✅ estilo del pane (brillo/opacity) sin tocar el mapa base (tiles)
(function styleHeatPanes(){
  const p = map.getPane("heatBasePane");
  if(p){
    p.style.filter = HEAT_PANE_FILTER;
    p.style.opacity = String(HEAT_PANE_OPACITY);
  }

  const pCant = map.getPane("heatCantPane");
  if(pCant){
    pCant.style.filter = HEAT_PANE_FILTER;
    pCant.style.opacity = String(HEAT_PANE_OPACITY);
  }

  const pGlow = map.getPane("heatGlowPane");
  if(pGlow){
    pGlow.style.filter = HEAT_PANE_FILTER;
    pGlow.style.opacity = String(HEAT_PANE_OPACITY);
  }

  // ✅ NUEVO: estilo visual para heatmap de Empresas
  const pEmp = map.getPane("heatEmpresasPane");
  if(pEmp){
    pEmp.style.filter =
      "saturate(1.20) brightness(1.08) " +
      "drop-shadow(0 0 10px rgba(147,51,234,0.35)) " +
      "drop-shadow(0 0 18px rgba(107,33,168,0.25))";
    pEmp.style.opacity = "0.98";
  }
})();
    const markersReco = L.layerGroup();

    // ✅ Heatmap Clientes
    const heatClientes = L.heatLayer([], {
  pane: "heatClientesPane",
  radius: 7,
  blur: 6,
  maxZoom: 18,
  minOpacity: 0.04
});


    // ======================================================
// ✅ HEATMAP CLIENTES — ROBUSTO (2do/3er/10mo click siempre funciona)
//   - aborta requests anteriores
//   - resetea key al apagar
//   - si la key es igual pero la capa no está, la vuelve a poner
//   - si se apaga/enciende, SIEMPRE fuerza fetch nuevo
// ======================================================
let _cliHeatLastKey = "";
let _cliHeatAbort = null;
let _cliHeatReqSeq = 0;
let _cliHeatTimer = null;

function clearHeatClientes(){
  // cancela request pendiente
  if(_cliHeatAbort){ try{ _cliHeatAbort.abort(); }catch(e){} }
  _cliHeatAbort = null;

  // invalida cualquier respuesta vieja
  _cliHeatReqSeq++;

  // IMPORTANTÍSIMO: resetea key para que al reactivar vuelva a cargar
  _cliHeatLastKey = "";

  // limpia capa
  try{ if(map.hasLayer(heatClientes)) map.removeLayer(heatClientes); }catch(e){}
  try{ heatClientes.setLatLngs([]); }catch(e){}

  // panel
  try{ if(panelClientes) panelClientes.classList.add("hidden"); }catch(e){}
}

function _scheduleHeatClientes(force=false){
  if(_cliHeatTimer) clearTimeout(_cliHeatTimer);
  _cliHeatTimer = setTimeout(()=> fetchHeatClientes(force), 120);
}

async function fetchHeatClientes(force=false){
  try{
    // si checkbox no está activo, aseguramos apagado limpio
    if(!chkHeatClientes || !chkHeatClientes.checked){
      clearHeatClientes();
      return;
    }

    // muestra panel
    try{ if(panelClientes) panelClientes.classList.remove("hidden"); }catch(e){}

    const zoom = map.getZoom();
    const d  = (selDep?.value || "");
    const p  = (selProv?.value || "");
    const di = (selDist?.value || "");
    const seg = "";

    const key = `${zoom}|${d}|${p}|${di}|${seg}`;

    // si es la misma key y ya hay data, NO refetch:
    // solo aseguramos que la capa esté agregada al mapa (esto arregla el "2do click vacío")
    let hasData = false;
    try{
      const ll = heatClientes.getLatLngs();
      hasData = Array.isArray(ll) && ll.length > 0;
    }catch(e){}

    if(!force && _cliHeatLastKey === key && hasData){
      if(!map.hasLayer(heatClientes)) heatClientes.addTo(map);
      // refresca panel resumen si existe
      if(typeof fetchResumenClientes === "function"){
        fetchResumenClientes().catch(()=>{});
      }
      return;
    }

    _cliHeatLastKey = key;

    // abort anterior
    if(_cliHeatAbort){ try{ _cliHeatAbort.abort(); }catch(e){} }
    _cliHeatAbort = new AbortController();

    const mySeq = ++_cliHeatReqSeq;

    const qs = `zoom=${encodeURIComponent(zoom)}&departamento=${encodeURIComponent(d)}&provincia=${encodeURIComponent(p)}&distrito=${encodeURIComponent(di)}&segmento=${encodeURIComponent(seg)}`;

    const res = await fetch(`/api/clientes?${qs}`, {
      signal: _cliHeatAbort.signal,
      cache: "no-store"
    });
    if(!res.ok) throw new Error(`HTTP ${res.status}`);

    const arr = await res.json();

    // si ya hubo otra solicitud después, ignoramos esta respuesta
    if(mySeq !== _cliHeatReqSeq) return;

    const pts = Array.isArray(arr) ? arr : [];
    const latlngs = pts
      .filter(r => r && isFinite(r.lat) && isFinite(r.lon))
      // intensidad constante (puedes subir/bajar 0.6 si quieres)
      .map(r => [Number(r.lat), Number(r.lon), 0.6]);

    heatClientes.setLatLngs(latlngs);

    // SIEMPRE re-add si está activo
    if(chkHeatClientes.checked && !map.hasLayer(heatClientes)){
      heatClientes.addTo(map);
    }

    // fuerza redraw inmediato (evita que espere interacción)
if(typeof heatClientes.redraw === "function"){
  heatClientes.redraw();
}

    // panel resumen (si existe tu función)
    if(typeof fetchResumenClientes === "function"){
      try{ await fetchResumenClientes(); }catch(e){}
    }
  }catch(err){
    // AbortError = normal
    if(String(err||"").includes("AbortError")) return;
    console.error("Error heatmap clientes:", err);

    // evita quedarse "semi-encendido"
    try{ heatClientes.setLatLngs([]); }catch(e){}
    if(chkHeatClientes && chkHeatClientes.checked){
      if(!map.hasLayer(heatClientes)) heatClientes.addTo(map);
    }
  }
}

    // ✅ Heatmap Cantidad de Clientes (comercios)
  const HEAT_CANT_OPTS = {
  pane: "heatCantPane",
  radius: 34,        // antes 48 (muy grande = pinta todo)
  blur: 22,          // antes 28
  maxZoom: 18,
  minOpacity: 0.10,  // más suave para ver el mapa
  max: 1.0,
  gradient: HEAT_GRADIENT_YOR // ✅ mismo color que el heatmap base
};

const heatCantClientes = L.heatLayer([], HEAT_CANT_OPTS);

function applyHeatCantStyle(){
  const opts = { ...HEAT_CANT_OPTS };
  if(typeof heatCantClientes.setOptions === "function") heatCantClientes.setOptions(opts);
  else Object.assign(heatCantClientes.options, opts);
}
applyHeatCantStyle();

    // ✅ Cluster df_comercios
const comerciosCluster = L.markerClusterGroup({
  chunkedLoading: true,
  showCoverageOnHover: false,
  spiderfyOnMaxZoom: true,

  // ✅ CLAVE: no revienta al zoom medio/cerca
  disableClusteringAtZoom: 19,
  maxClusterRadius: (zoom) => (zoom >= 15 ? 130 : 95),

  iconCreateFunction: function(cluster){
    const n = cluster.getChildCount();
    return L.divIcon({
      className: "com-cluster-icon",
      html: `<div class="com-cluster">${n}</div>`,
      iconSize: [46,46],
      iconAnchor: [23,23]
    });
  }
});


    function comPinIcon(){
      const svg = `
        <div class="com-pin-wrap">
          <svg viewBox="0 0 24 24" fill="#1e6cff" stroke="#ffffff" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
            <path d="M12 22s7-4.5 7-12a7 7 0 0 0-14 0c0 7.5 7 12 7 12z"></path>
            <circle cx="12" cy="10" r="2.7" fill="#ffffff" stroke="none"></circle>
          </svg>
        </div>`;
      return L.divIcon({
        className: "com-pin-icon",
        html: svg,
        iconSize: [38,38],
        iconAnchor: [19,38],
        popupAnchor: [0,-32],
      });
    }

    function comBalloonHtml(r){
      const idc  = escHtml(r.commerce_id || r.id_comercio || "—");
      const cant = Number(r.cant_clientes || 0);
      const ruc  = escHtml(r.ruc || "—");
      const rs   = escHtml(r.razon_social || "—");
      const dir  = escHtml(r.direccion || "—");

      return `
        <div class="com-balloon">
          <div style="font-size:14px; margin-bottom:6px;">🏪 Comercio</div>
          <div><b>commerce_id:</b> ${idc}</div>
          <div><b>cant_clientes:</b> ${isFinite(cant) ? cant.toFixed(0) : "0"}</div>
          <div><b>RUC:</b> ${ruc}</div>
          <div><b>RazonSocial:</b> ${rs}</div>
          <div><b>Dirección:</b> ${dir}</div>
        </div>`;
    }

    // Añade capas base al mapa
    markers.addTo(map);
    heat.addTo(map);
    heatGlow.addTo(map); // ✅


    // ======================================================
    // UI ELEMENTS (DOM)
    // ======================================================
    const selDep = document.getElementById("selDepartamento");
    const selProv = document.getElementById("selProvincia");
    const selDist = document.getElementById("selDistrito");
    const selDiv = document.getElementById("selDivision");
    const selOficina = document.getElementById("selOficina");

    const chkHeat = document.getElementById("chkHeat");
    const chkHeatClientes = document.getElementById("chkHeatClientes");

    function setHeatOnClass(){
  document.body.classList.toggle("heat-on", !!(chkHeat && chkHeat.checked));
}
setHeatOnClass();
if(chkHeat) chkHeat.addEventListener("change", setHeatOnClass);

    const chkHeatCantClientes = document.getElementById("chkHeatCantClientes");
    const chkComerciosPts = document.getElementById("chkComerciosPts");
    const chkEmpresasNomina = document.getElementById("chkEmpresasNomina");
    const chkHeatEmpresasNomina = document.getElementById("chkHeatEmpresasNomina");

    const panelClientes = document.getElementById("panelClientes");
    const panelEmpresasNomina = document.getElementById("panelEmpresasNomina");
    const infoBox = document.getElementById("infoCount");

    const selTipoATM = document.getElementById("selTipoATM");
    const selUbicATM = document.getElementById("selUbicacionATM");
    const selSegmento = null;

    const chkReco = document.getElementById("chkReco");
    const chkZonaRural  = document.getElementById("chkZonaRural");
    const chkZonaUrbana = document.getElementById("chkZonaUrbana");

    const chkNodos = document.getElementById("chkNodos");
    const panelComercial = document.getElementById("panelComercial");

    const panelDfComercios = document.getElementById("panelDfComercios");
    // ======================================================
// ✅ HEATMAP y PUNTOS BASE: INDEPENDIENTES + DINÁMICOS
//   - Puntos siempre visibles (según checkboxes + filtros)
//   - Heatmap solo si chkHeat.checked
//   - Heatmap se construye con los canales seleccionados
//   - "Remarca" (glow) los checkbox de canales activos cuando heatmap está ON
// ======================================================

function _lblFromInputId(id){
  const el = document.getElementById(id);
  return el ? el.closest("label") : null;
}

function syncHeatCheckboxHighlight(){
  if(TIPO_MAPA !== "integral") return;

  const enabled = !!(chkHeat && chkHeat.checked);

  const lblATM = _lblFromInputId("chkShowATMs");
  const lblOFI = _lblFromInputId("chkShowOficinas");
  const lblAGE = _lblFromInputId("chkShowAgentes");

  [lblATM, lblOFI, lblAGE].forEach(l => l && l.classList.remove("heat-src-on"));
  if(!enabled) return;

  if(document.getElementById("chkShowATMs")?.checked)     lblATM?.classList.add("heat-src-on");
  if(document.getElementById("chkShowOficinas")?.checked) lblOFI?.classList.add("heat-src-on");
  if(document.getElementById("chkShowAgentes")?.checked)  lblAGE?.classList.add("heat-src-on");
}

function getCanalesSeleccionados(){
  if(TIPO_MAPA !== "integral"){
    return { ATM:true, OFICINA:true, AGENTE:true };
  }
  return {
    ATM: !!document.getElementById("chkShowATMs")?.checked,
    OFICINA: !!document.getElementById("chkShowOficinas")?.checked,
    AGENTE: !!document.getElementById("chkShowAgentes")?.checked,
  };
}

function getIconIntegral(pt){
  const canal = (pt.tipo_canal || "").toUpperCase();
  if(canal === "AGENTE") return ICON_AGENTE;
  if(canal === "OFICINA") return ICON_OFICINA;

  // ATM (usa ubicacion para isla/oficina)
  const ubic = (pt.ubicacion || "").toUpperCase();
  if(ubic.includes("OFICINA")) return ICON_ATM_OFICINA;
  if(ubic.includes("ISLA")) return ICON_ATM_ISLA;
  return ICON_ATM_ISLA;
}

function buildQSBase(){
  const qs = new URLSearchParams();
  qs.set("departamento", selDep?.value || "");
  qs.set("provincia", selProv?.value || "");
  qs.set("distrito", selDist?.value || "");
  qs.set("division", selDiv?.value || "");

  // filtros extra solo para islas
  if(TIPO_MAPA === "islas"){
    qs.set("tipo_atm", selTipoATM?.value || "");
    qs.set("ubic_atm", selUbicATM?.value || "");
  }
  return qs.toString();
}

function clearHeat(){
  try{ heat.setLatLngs([]); }catch(e){}
  try{ heatGlow.setLatLngs([]); }catch(e){}

  try{ if(map.hasLayer(heat)) map.removeLayer(heat); }catch(e){}
  try{ if(map.hasLayer(heatGlow)) map.removeLayer(heatGlow); }catch(e){}

  // ✅ deja estilo listo para el próximo “ON”
  applyHeatBaseStyle();
}

// --- pesos por PROMEDIO/TRX (robusto) ---
// - usa log1p para comprimir valores grandes
// - recorta por percentiles (5%..95%) para que outliers no “quemen” todo
// - devuelve peso final en [W_MIN..W_MAX]
const HEAT_W_MIN = 0.10;
const HEAT_W_MAX = 0.85;

function _buildWeightFn(pts){
  const vals = (Array.isArray(pts) ? pts : [])
    .map(p => Math.log1p(Math.max(0, Number(p?.promedio ?? 0))))
    .filter(v => isFinite(v));

  // poco data o todo igual → peso medio
  if(vals.length < 5){
    return () => 0.60;
  }

  vals.sort((a,b)=>a-b);
  const lo = vals[Math.floor(vals.length * 0.05)];
  const hi = vals[Math.floor(vals.length * 0.95)];
  const den = Math.max(1e-9, hi - lo);

  return (p)=>{
    const v = Math.log1p(Math.max(0, Number(p?.promedio ?? 0)));
    let t = (v - lo) / den;
    if(!isFinite(t)) t = 0.5;
    t = Math.max(0, Math.min(1, t));
    return HEAT_W_MIN + t * (HEAT_W_MAX - HEAT_W_MIN);
  };
}

function drawHeatFromPoints(pts){
  const enabled = !!(chkHeat && chkHeat.checked);
  syncHeatCheckboxHighlight();

  if(!enabled){
    clearHeat();
    return;
  }

  if(!map.hasLayer(heat)) heat.addTo(map);
  if(!map.hasLayer(heatGlow)) heatGlow.addTo(map);

  const wFn = _buildWeightFn(pts);

  const latlngs = (Array.isArray(pts) ? pts : [])
    .filter(p => p && isFinite(p.lat) && isFinite(p.lon))
    .map(p => [Number(p.lat), Number(p.lon), wFn(p)]); // ✅ peso por TRX/promedio

  heat.setLatLngs(latlngs);
  heatGlow.setLatLngs(latlngs);

  if(typeof heat.redraw === "function") heat.redraw();
  if(typeof heatGlow.redraw === "function") heatGlow.redraw();
}

function drawMarkersFromPoints(pts, isIntegral){
  markers.clearLayers();

  (Array.isArray(pts) ? pts : []).forEach(pt=>{
    if(!pt || !isFinite(pt.lat) || !isFinite(pt.lon)) return;

    const baseIcon = isIntegral ? getIconIntegral(pt) : getIcon(pt);
const icon = (chkHeat && chkHeat.checked) ? getTrxHaloIcon(pt) : baseIcon;
const m = L.marker([pt.lat, pt.lon], { icon });


    // Mantén tu lógica de click (panel)
    m.on("click", ()=> {
      if(typeof showATMPanel === "function") showATMPanel(pt);
    });

    markers.addLayer(m);
  });
}



async function refreshBasePointsAndHeat(){
  const qs = buildQSBase();

  // ✅ INTEGRAL: mezcla canales según checkboxes
  if(TIPO_MAPA === "integral"){
    const res = await fetch(`/api/points_integral?${qs}`, { cache: "no-store" });
    const data = await res.json();

    const sel = getCanalesSeleccionados();

    const pts = [];
    if(sel.ATM)     pts.push(...((data.atms||[]).map(p=>({ ...p, tipo_canal:"ATM" }))));
    if(sel.OFICINA) pts.push(...((data.oficinas||[]).map(p=>({ ...p, tipo_canal:"OFICINA" }))));
    if(sel.AGENTE)  pts.push(...((data.agentes||[]).map(p=>({ ...p, tipo_canal:"AGENTE" }))));

    // ✅ puntos SIEMPRE (independiente del heatmap)
    drawMarkersFromPoints(pts, true);

    // ✅ heatmap solo si chkHeat está ON y solo con pts visibles
    drawHeatFromPoints(pts);

    // contador base
    _counts.base = pts.length;
    refreshInfoCount();
    return;
  }

  // ✅ VISTAS: islas/agentes/oficinas
  const res = await fetch(`/api/points?tipo=${encodeURIComponent(TIPO_MAPA)}&${qs}`, { cache: "no-store" });
  const data = await res.json();
  const pts = data.puntos || [];

  drawMarkersFromPoints(pts, false);
  drawHeatFromPoints(pts);

  _counts.base = pts.length;
  refreshInfoCount();
}

// ======================================================
// ✅ EVENTOS: ya NO depende de heatmap para ver puntos
// ======================================================
function refreshAll(){
  // Si tú ya tienes un refreshAll más grande (zonas, nodos, etc.),
  // reemplaza SOLO su parte "base/heat" por refreshBasePointsAndHeat().
  refreshBasePointsAndHeat().catch(err=>console.error("refresh base/heat:", err));
}

if(chkHeat){
  chkHeat.addEventListener("change", ()=>{
    if(!chkHeat.checked){
      clearHeat();
    }else{
      applyHeatBaseStyle();
      if(!map.hasLayer(heat)) heat.addTo(map);
      if(!map.hasLayer(heatGlow)) heatGlow.addTo(map);
      if(typeof heat.redraw === "function") heat.redraw();
      if(typeof heatGlow.redraw === "function") heatGlow.redraw();
    }
    syncHeatCheckboxHighlight();
    refreshAll();
  });
}

if(TIPO_MAPA === "integral"){
  ["chkShowATMs","chkShowOficinas","chkShowAgentes"].forEach(id=>{
    const el = document.getElementById(id);
    if(el) el.addEventListener("change", ()=>{
      // al cambiar canales: se actualizan puntos y heat (si está ON)
      refreshAll();
    });
  });
}

// filtros → refresca base + heat
[selDep, selProv, selDist, selDiv, selTipoATM, selUbicATM].forEach(el=>{
  if(!el) return;
  el.addEventListener("change", refreshAll);
});

// inicial
map.whenReady(()=> refreshAll());


    // ======================================================
// ✅ FIX: al activar Heatmap Clientes debe dibujarse AL INSTANTE
// (sin necesidad de click en el mapa)
// ======================================================
function _toggleHeatClientes(){
  if(!chkHeatClientes) return;

  if(chkHeatClientes.checked){
    // fuerza carga inmediata cuando se activa
    map.whenReady(() => fetchHeatClientes(true));
  } else {
    // apagado limpio
    clearHeatClientes();
  }
}

// al cambiar el checkbox -> dibuja inmediatamente
chkHeatClientes.addEventListener("change", () => {
  _toggleHeatClientes();
  refreshInfoCount();
});

// si cambian filtros -> vuelve a calcular (con debounce)
[selDep, selProv, selDist].forEach(el => {
  if(!el) return;
  el.addEventListener("change", () => {
    if(chkHeatClientes && chkHeatClientes.checked){
      _scheduleHeatClientes(true);
    }
  });
});

// si haces zoom o mueves el mapa -> refresca (opcional pero recomendado)
map.on("zoomend moveend", () => {
  if(chkHeatClientes && chkHeatClientes.checked){
    _scheduleHeatClientes(false);
  }
});


if(chkEmpresasNomina){
  chkEmpresasNomina.addEventListener("change", async () => {
    syncEmpresasNominaVisibility();
    if(chkEmpresasNomina.checked){
      await fetchEmpresasNomina(true);
    }else{
      _counts.empresas = 0;
      refreshInfoCount();
      if(chkHeatEmpresasNomina?.checked) await fetchResumenEmpresasNomina();
    }
  });
}

if(chkHeatEmpresasNomina){
  chkHeatEmpresasNomina.addEventListener("change", async () => {
    syncEmpresasNominaVisibility();
    if(chkHeatEmpresasNomina.checked){
      await fetchHeatEmpresasNomina();
    }else{
      try{ if(map.hasLayer(heatEmpresasNomina)) map.removeLayer(heatEmpresasNomina); }catch(e){}
      try{ heatEmpresasNomina.setLatLngs([]); }catch(e){}
      if(!(chkEmpresasNomina?.checked)){
        setEmpresasNominaResumen(null);
      }else{
        await fetchResumenEmpresasNomina();
      }
    }
  });
}

[selDep, selProv, selDist].forEach(el => {
  if(!el) return;
  el.addEventListener("change", async () => {
    if(chkEmpresasNomina?.checked) await fetchEmpresasNomina(true);
    if(chkHeatEmpresasNomina?.checked) await fetchHeatEmpresasNomina();
    if(chkEmpresasNomina?.checked || chkHeatEmpresasNomina?.checked){
      await fetchResumenEmpresasNomina();
    }
  });
});

map.on("zoomend", () => {
  if(chkEmpresasNomina?.checked) fetchEmpresasNomina(false);
  if(chkHeatEmpresasNomina?.checked) fetchHeatEmpresasNomina();
});

map.on("moveend", () => {
  if(chkHeatEmpresasNomina?.checked) fetchHeatEmpresasNomina();
});

refreshGeoSelectors();
// por si el checkbox arranca marcado (cache del navegador)
map.whenReady(() => {
  if(chkHeatClientes && chkHeatClientes.checked){
    fetchHeatClientes(true);
  }
});


    // ======================================================
    // ✅ CONTADORES (para "Mostrando")
    // ======================================================
    const _counts = { base: 0, nodos: 0, comercios: 0, empresas: 0 };

    function refreshInfoCount(){
      let total = 0;
      total += Number(_counts.base || 0);
      if(chkNodos && chkNodos.checked) total += Number(_counts.nodos || 0);
      if(chkComerciosPts && chkComerciosPts.checked) total += Number(_counts.comercios || 0);
      if(chkEmpresasNomina && chkEmpresasNomina.checked) total += Number(_counts.empresas || 0);
      infoBox.textContent = String(total);
    }

    // ======================================================
    // PANEL DETALLE
    // ======================================================
    const panelATM = document.getElementById("panelATM");
    const atmDetalle = document.getElementById("atmDetalle");
    const btnVolver = document.getElementById("btnVolver");

    const panelReco = document.getElementById("panelReco");
    const recoDetalle = document.getElementById("recoDetalle");
    const btnRecoVolver = document.getElementById("btnRecoVolver");

    const panelATMResumen = document.getElementById("panelATMResumen");
    const panelOfiResumen = document.getElementById("panelOfiResumen");
    const panelAgResumen = document.getElementById("panelAgResumen");

    function hideResumenPanels(){
      if(panelATMResumen) panelATMResumen.classList.add("hidden");
      if(panelOfiResumen) panelOfiResumen.classList.add("hidden");
      if(panelAgResumen) panelAgResumen.classList.add("hidden");
      syncComercialVisibility();
      syncDfComerciosPanelVisibility();
    }

    function syncSinglePanelsVisibility(){
      if(TIPO_MAPA === "integral"){ syncIntegralPanelsVisibility(); return; }
      if(panelATMResumen) panelATMResumen.classList.toggle("hidden", TIPO_MAPA !== "islas");
      if(panelOfiResumen) panelOfiResumen.classList.toggle("hidden", TIPO_MAPA !== "oficinas");
      if(panelAgResumen) panelAgResumen.classList.toggle("hidden", TIPO_MAPA !== "agentes");
    }

    function showResumenPanels(){
      if(TIPO_MAPA === "integral"){ syncIntegralPanelsVisibility(); }
      else { syncSinglePanelsVisibility(); }
      syncComercialVisibility();
      syncDfComerciosPanelVisibility();
    }

    function showRecoPanel(r){
      if (!r) return;

      const canal = String(r.canal||"").toUpperCase();
      const ubic = `${r.departamento} / ${r.provincia} / ${r.distrito}`;

      const diagnostico = String(r.diagnostico||"").replace(/\\[|\\]|\'/g,"");

      const html = `
        <div class="detail-panel">
          <div class="dp-head">
            <div class="dp-title">
              <div class="overline">RECOMENDACIÓN</div>
              <div class="name">${esc(r.perfil_top || "Perfil dominante")}</div>
              <div class="sub">${esc(ubic)}</div>
            </div>
            <div class="badge">${esc(canal || "—")}</div>
          </div>

          <div class="dp-kpis">
            <div class="kpi"><div class="lbl">Clientes afectados</div><div class="val">${fmt0(r.clientes_afectados)}</div></div>
            <!--<div class="kpi"><div class="lbl">% Digitales</div><div class="val">${(Number(r.pct_digital||0)*100).toFixed(1)}%</div></div>-->
            <!--<div class="kpi"><div class="lbl">Edad promedio</div><div class="val">${Number(r.edad_prom||0).toFixed(1)}</div></div>-->
            <!--<div class="kpi"><div class="lbl">Ingreso promedio</div><div class="val">S/ ${Number(r.ingreso_prom||0).toFixed(2)}</div></div>-->
          </div>

          <div class="dp-section">
            <div class="sec-title">Información</div>
            <div class="dp-rows">
              <div class="dp-row"><span class="k">Canal sugerido</span><span class="v">${esc(canal)}</span></div>
              <div class="dp-row"><span class="k">Coordenadas</span><span class="v">lat ${esc(r.lat)} · lon ${esc(r.lon)}</span></div>
            </div>
          </div>

          <div class="dp-note">
            <b>Diagnóstico:</b> ${esc(diagnostico)}
          </div>
        </div>
      `;

      recoDetalle.innerHTML = html;
      hideResumenPanels();
      panelATM.classList.add("hidden");
      panelReco.classList.remove("hidden");
      panelReco.classList.add("glow");
    }

    function showATMPanel(pt){
      const lineaUbic = `${pt.departamento} / ${pt.provincia} / ${pt.distrito}`;
      let texto = "";

      if(TIPO_MAPA === "integral"){
        const canal = (pt.tipo_canal || "").toUpperCase();
        if(canal === "AGENTE"){
          texto =
`_____________________ AGENTE ${pt.atm} _____________________
• Comercio: ${pt.nombre}
• Dirección: ${pt.direccion}
• División: ${pt.division}
• Capa: ${pt.capa || ""}
• Tipo: ${pt.tipo}
• Ubicación: ${pt.ubicacion}
• Ubicación Geográfica: ${lineaUbic}
• Trxs Octubre: ${pt.trxs_oct ?? 0}
• Trxs Noviembre: ${pt.trxs_nov ?? 0}
_____________________ Promedio: ${pt.promedio} _____________________`;
        } else if(canal === "OFICINA"){
          texto =
`_____________________ OFICINA ${pt.atm} _____________________
• Nombre: ${pt.nombre}
• Dirección: ${pt.direccion}
• División: ${pt.division}
• Ubicación Geográfica: ${lineaUbic}

——— Métricas de la Oficina ———
• TRX: ${pt.promedio}
• Estructura AS: ${fmt2(pt.estructura_as)}
• Estructura EBP: ${fmt2(pt.estructura_ebp)}
• Estructura AD: ${fmt2(pt.estructura_ad)}
• Clientes únicos: ${fmt0(pt.clientes_unicos)}
• Total tickets: ${fmt0(pt.total_tickets)}
• Red Lines: ${fmtPct(pt.red_lines)}
_________________________________________`;
        } else {
          texto =
`_____________________ ATM ${pt.atm} _____________________
• Nombre: ${pt.nombre}
• Dirección: ${pt.direccion}
• División: ${pt.division}
• Tipo: ${pt.tipo}
• Ubicación: ${pt.ubicacion}
• Ubicación Geográfica: ${lineaUbic}
_____________________ Promedio: ${pt.promedio} _____________________`;
        }
      } else if(TIPO_MAPA === "agentes"){
        texto =
`_____________________ AGENTE ${pt.atm} _____________________
• Comercio: ${pt.nombre}
• Dirección: ${pt.direccion}
• División: ${pt.division}
• Capa: ${pt.capa}
• Tipo: ${pt.tipo}
• Ubicación: ${pt.ubicacion}
• Ubicación Geográfica: ${lineaUbic}
• Trxs Octubre: ${pt.trxs_oct ?? 0}
• Trxs Noviembre: ${pt.trxs_nov ?? 0}
_____________________ Promedio: ${pt.promedio} _____________________`;
      } else if(TIPO_MAPA === "oficinas"){
        texto =
`_____________________ OFICINA ${pt.atm} _____________________
• Nombre: ${pt.nombre}
• Dirección: ${pt.direccion}
• División: ${pt.division}
• Ubicación Geográfica: ${lineaUbic}

——— Métricas de la Oficina ———
• TRX: ${pt.promedio}
• Estructura AS: ${fmt2(pt.estructura_as)}
• Estructura EBP: ${fmt2(pt.estructura_ebp)}
• Estructura AD: ${fmt2(pt.estructura_ad)}
• Clientes únicos: ${fmt0(pt.clientes_unicos)}
• Total tickets: ${fmt0(pt.total_tickets)}
• Red Lines: ${fmtPct(pt.red_lines)}
_________________________________________`;
      } else {
        texto =
`_____________________ ATM ${pt.atm} _____________________
• Nombre: ${pt.nombre}
• Dirección: ${pt.direccion}
• División: ${pt.division}
• Tipo: ${pt.tipo}
• Ubicación: ${pt.ubicacion}
• Ubicación Geográfica: ${lineaUbic}
_____________________ Promedio: ${pt.promedio} _____________________`;
      }

      // Render HTML bonito según canal (manteniendo los mismos datos)
      const canal = (TIPO_MAPA === "integral") ? String(pt.tipo_canal||"").toUpperCase()
                  : (TIPO_MAPA === "agentes") ? "AGENTE"
                  : (TIPO_MAPA === "oficinas") ? "OFICINA"
                  : "ATM";

      const ubic = `${pt.departamento} / ${pt.provincia} / ${pt.distrito}`;

      let headOver = "PUNTO SELECCIONADO";
      let headName = pt.nombre || "";
      let headSub  = ubic;

      let kpis = "";
      let infoRows = "";

      if(canal === "OFICINA"){
        kpis = `
          <div class="dp-kpis">
            <div class="kpi"><div class="lbl">TRX</div><div class="val">${fmt0(pt.promedio)}</div></div>
            <div class="kpi"><div class="lbl">Clientes únicos</div><div class="val">${fmt0(pt.clientes_unicos)}</div></div>
            <div class="kpi"><div class="lbl">Total tickets</div><div class="val">${fmt0(pt.total_tickets)}</div></div>
            <div class="kpi"><div class="lbl">Red Lines</div><div class="val">${fmtPct(pt.red_lines)}</div></div>
            <div class="kpi"><div class="lbl">BAI</div><div class="val">${fmtKM(pt.bai ?? 0)}</div></div>
<div class="kpi"><div class="lbl">Margen Neto</div><div class="val">${fmtKM(pt.margen_neto ?? 0)}</div></div>
          </div>
        `;

        infoRows = `
          <div class="dp-row"><span class="k">Código</span><span class="v">${esc(pt.atm)}</span></div>
          <div class="dp-row"><span class="k">Performance 2025</span><span class="v">${esc(pt.performance_2025 || "—")}</span></div>
          <div class="dp-row"><span class="k">Dirección</span><span class="v">${esc(pt.direccion)}</span></div>
          <div class="dp-row"><span class="k">División</span><span class="v">${esc(pt.division)}</span></div>
          <div class="dp-row"><span class="k">Ubicación geográfica</span><span class="v">${esc(ubic)}</span></div>
        `;

        // Sección estructura (igual que hoy, solo ordenado)
        infoRows += `
          <div style="height:8px"></div>
          <div class="dp-row"><span class="k">Estructura AS</span><span class="v">${esc(fmt2(pt.estructura_as))}</span></div>
          <div class="dp-row"><span class="k">Estructura EBP</span><span class="v">${esc(fmt2(pt.estructura_ebp))}</span></div>
          <div class="dp-row"><span class="k">Estructura AD</span><span class="v">${esc(fmt2(pt.estructura_ad))}</span></div>
        `;
      }
      else if(canal === "AGENTE"){
        kpis = `
          <div class="dp-kpis">
            <div class="kpi"><div class="lbl">Promedio</div><div class="val">${fmt0(pt.promedio)}</div></div>
            <div class="kpi"><div class="lbl">Capa</div><div class="val">${esc(pt.capa || "—")}</div></div>
         <div class="kpi"><div class="lbl">Trxs Dic</div><div class="val">${fmt0(pt.trxs_oct ?? 0)}</div></div>
<div class="kpi"><div class="lbl">Trxs Ene</div><div class="val">${fmt0(pt.trxs_nov ?? 0)}</div></div>
          </div>
        `;

        infoRows = `
          <div class="dp-row"><span class="k">Código</span><span class="v">${esc(pt.atm)}</span></div>
          <div class="dp-row"><span class="k">Comercio</span><span class="v">${esc(pt.nombre)}</span></div>
          <div class="dp-row"><span class="k">Dirección</span><span class="v">${esc(pt.direccion)}</span></div>
          <div class="dp-row"><span class="k">División</span><span class="v">${esc(pt.division)}</span></div>
          <div class="dp-row"><span class="k">Tipo</span><span class="v">${esc(pt.tipo)}</span></div>
          <div class="dp-row"><span class="k">Ubicación</span><span class="v">${esc(pt.ubicacion)}</span></div>
          <div class="dp-row"><span class="k">Ubicación geográfica</span><span class="v">${esc(ubic)}</span></div>
        `;
      }
      else { // ATM
        kpis = `
          <div class="dp-kpis">
            <div class="kpi"><div class="lbl">Promedio</div><div class="val">${fmt0(pt.promedio)}</div></div>
            <div class="kpi"><div class="lbl">Tipo</div><div class="val">${esc(pt.tipo || "—")}</div></div>
            <div class="kpi"><div class="lbl">Ubicación</div><div class="val">${esc(pt.ubicacion || "—")}</div></div>
            <div class="kpi"><div class="lbl">División</div><div class="val">${esc(pt.division || "—")}</div></div>
          </div>
        `;

        infoRows = `
          <div class="dp-row"><span class="k">Código</span><span class="v">${esc(pt.atm)}</span></div>
          <div class="dp-row"><span class="k">Nombre</span><span class="v">${esc(pt.nombre)}</span></div>
          <div class="dp-row"><span class="k">Dirección</span><span class="v">${esc(pt.direccion)}</span></div>
          <div class="dp-row"><span class="k">Ubicación geográfica</span><span class="v">${esc(ubic)}</span></div>
        `;
      }

      const html = `
        <div class="detail-panel">
          <div class="dp-head">
            <div class="dp-title">
              <div class="overline">${esc(headOver)}</div>
              <div class="name">${esc(headName)}</div>
              <div class="sub">${esc(headSub)}</div>
            </div>
            <div class="badge">${esc(canal)}</div>
          </div>

          ${kpis}

          <div class="dp-section">
            <div class="sec-title">Detalle</div>
            <div class="dp-rows">
              ${infoRows}
            </div>
          </div>
        </div>
      `;

      atmDetalle.innerHTML = html;
           
      hideResumenPanels();
      panelATM.classList.remove("hidden");
      panelATM.classList.add("glow");
    }

    function clearOfficeFocus(){
      if(currentOfficeFocusMarker){
        try{ map.removeLayer(currentOfficeFocusMarker); }catch(e){}
        currentOfficeFocusMarker = null;
      }
      if(currentOfficeFocusCircle){
        try{ map.removeLayer(currentOfficeFocusCircle); }catch(e){}
        currentOfficeFocusCircle = null;
      }
    }

    function createOfficeFocusIcon(){
      return L.divIcon({
        className: "office-focus-icon",
        html: `<div class="office-focus-wrap"></div>`,
        iconSize: [58,58],
        iconAnchor: [29,29]
      });
    }

    function focusOfficeOnMap(ofi){
      if(!ofi) return;

      const lat = Number(ofi.lat);
      const lon = Number(ofi.lon);
      if(!isFinite(lat) || !isFinite(lon)) return;

      clearOfficeFocus();

      // zoom suficiente para distinguir la oficina
      map.setView([lat, lon], 18, { animate:true });

      // halo/anillo encima de todo
      currentOfficeFocusMarker = L.marker([lat, lon], {
        icon: createOfficeFocusIcon(),
        zIndexOffset: 10000
      }).addTo(map);

      currentOfficeFocusCircle = L.circle([lat, lon], {
        radius: 28,
        color: "#1464A5",
        weight: 2,
        fillColor: "#1464A5",
        fillOpacity: 0.10,
        opacity: 0.75
      }).addTo(map);

      // abre automáticamente el panel de detalle
      showATMPanel(ofi);
    }
    

    btnVolver.addEventListener("click", () => {
      panelATM.classList.add("hidden");
      panelATM.classList.remove("glow");
      showResumenPanels();
    });

    btnRecoVolver.onclick = () => {
      panelReco.classList.add("hidden");
      panelReco.classList.remove("glow");
      showResumenPanels();
    };

    // ======================================================
    // ✅ COMERCIAL/NODOS — pin rojo + popup globo + panel conteo
    // ======================================================
  
// ======================================================
// ✅ EMPRESAS NÓMINA — puntos + heatmap + panel resumen
// ======================================================
const HEAT_EMPRESAS_OPTS = {
  pane: "heatEmpresasPane",
  radius: 58,
  blur: 36,
  maxZoom: 18,
  minOpacity: 0.58,
  max: 1.0,
  gradient: {
    0.00: "rgba(0,0,0,0)",
    0.08: "#F3E8FF",
    0.22: "#E9D5FF",
    0.42: "#D8B4FE",
    0.65: "#C084FC",
    0.84: "#9333EA",
    1.00: "#6B21A8"
  }
};

const heatEmpresasNomina = L.heatLayer([], HEAT_EMPRESAS_OPTS);
const empresasCluster = L.markerClusterGroup({
  chunkedLoading: true,
  showCoverageOnHover: false,
  spiderfyOnMaxZoom: true,
  disableClusteringAtZoom: 18,
  maxClusterRadius: (zoom) => (zoom >= 15 ? 110 : 90),
  iconCreateFunction: function(cluster){
    const n = cluster.getChildCount();
    return L.divIcon({
      className: "emp-cluster-icon",
      html: `<div style="background:#6B46C1;color:#fff;border:3px solid rgba(255,255,255,.95);width:44px;height:44px;border-radius:999px;display:flex;align-items:center;justify-content:center;font-weight:800;box-shadow:0 8px 18px rgba(107,70,193,.35);">${n}</div>`,
      iconSize: [44,44],
      iconAnchor: [22,22]
    });
  }
});

let _empresasAbort = null;
let _empresasHeatAbort = null;

function empPinIcon(){
  const svg = `
    <div style="width:34px;height:34px;display:flex;align-items:center;justify-content:center;filter:drop-shadow(0 4px 10px rgba(107,70,193,.35));">
      <svg viewBox="0 0 24 24" width="34" height="34" fill="#6B46C1" stroke="#ffffff" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 22s7-4.5 7-12a7 7 0 0 0-14 0c0 7.5 7 12 7 12z"></path>
        <path d="M9 12h6M9 9h6M9 15h6"></path>
      </svg>
    </div>`;
  return L.divIcon({
    className: "emp-pin-icon",
    html: svg,
    iconSize: [34,34],
    iconAnchor: [17,34],
    popupAnchor: [0,-28]
  });
}

function fmtMoneyShort(v){
  const n = Number(v || 0);
  if(!isFinite(n)) return "0";
  return new Intl.NumberFormat("es-PE", { maximumFractionDigits: 0 }).format(Math.round(n));
}

function empBalloonHtml(r){
  const nombre = escHtml(r.nombre_completo || "—");
  const area = escHtml(r.operarea_desc || "—");
  const ciiu = escHtml(r.ciiu_agrupado || "—");
  const trab = fmtMoneyShort(r.trabajadores || 0);
  const stock = fmtMoneyShort(r.stock || 0);
  const pen = Number(r.penetracion_nomina || 0) * 100;
  const sw = Number(r.share_wallet || 0) * 100;
  return `
    <div class="com-balloon">
      <div style="font-size:14px; margin-bottom:6px;">🏢 Empresa Nómina</div>
      <div><b>Empresa:</b> ${nombre}</div>
      <div><b>Área:</b> ${area}</div>
      <div><b>Sector:</b> ${ciiu}</div>
      <div><b>Trabajadores:</b> ${trab}</div>
      <div><b>Stock BBVA:</b> ${stock}</div>
      <div><b>Penetración:</b> ${pen.toFixed(1)}%</div>
      <div><b>Share wallet:</b> ${sw.toFixed(1)}%</div>
    </div>`;
}

function syncEmpresasNominaVisibility(){
  const showPts = !!(chkEmpresasNomina && chkEmpresasNomina.checked);
  const showHeat = !!(chkHeatEmpresasNomina && chkHeatEmpresasNomina.checked);
  const showPanel = showPts || showHeat;

  if(panelEmpresasNomina) panelEmpresasNomina.classList.toggle("hidden", !showPanel);

  if(!showPts){
    try{ if(map.hasLayer(empresasCluster)) map.removeLayer(empresasCluster); }catch(e){}
    try{ empresasCluster.clearLayers(); }catch(e){}
    _counts.empresas = 0;
  }else{
    if(!map.hasLayer(empresasCluster)) empresasCluster.addTo(map);
  }

  if(!showHeat){
    try{ if(map.hasLayer(heatEmpresasNomina)) map.removeLayer(heatEmpresasNomina); }catch(e){}
    try{ heatEmpresasNomina.setLatLngs([]); }catch(e){}
  }else{
    if(!map.hasLayer(heatEmpresasNomina)) heatEmpresasNomina.addTo(map);
  }

  if(!showPanel){
    setEmpresasNominaResumen(null);
    refreshInfoCount();
  }
}

function setEmpresasNominaResumen(data){
  const d = data || {};
  const z = (id, val)=>{ const el = document.getElementById(id); if(el) el.textContent = val; };
  z("empNomTotal", fmtMoneyShort(d.total_empresas || 0));
  z("empNomTrab", fmtMoneyShort(d.total_trabajadores || 0));
  z("empNomStock", fmtMoneyShort(d.total_stock || 0));
  z("empNomPen", `${Number(d.penetracion_prom || 0).toFixed(1)}%`);
  z("empNomShare", `${Number(d.share_wallet_prom || 0).toFixed(1)}%`);
  z("empNomArea", d.top_area || "—");
  z("empNomSector", d.top_sector || "—");
  z("empNomSaldoBbva", fmtMoneyShort(d.saldo_bbva_total || 0));
  z("empNomSaldoSystem", fmtMoneyShort(d.saldo_system_total || 0));
}

async function fetchResumenEmpresasNomina(){
  if(!(chkEmpresasNomina?.checked || chkHeatEmpresasNomina?.checked)){
    setEmpresasNominaResumen(null);
    return;
  }

  const qs = new URLSearchParams();
  qs.set("departamento", selDep?.value || "");
  qs.set("provincia", selProv?.value || "");
  qs.set("distrito", selDist?.value || "");

  const res = await fetch(`/api/resumen_empresas_nominas?${qs.toString()}`, {
    cache: "no-store"
  });
  const data = await res.json();
  setEmpresasNominaResumen(data);
}

async function fetchEmpresasNomina(force=true){
  if(!chkEmpresasNomina || !chkEmpresasNomina.checked){
    syncEmpresasNominaVisibility();
    return;
  }

  syncEmpresasNominaVisibility();
  if(_empresasAbort){ try{ _empresasAbort.abort(); }catch(e){} }
  _empresasAbort = new AbortController();

  try{ empresasCluster.clearLayers(); }catch(e){}

  const qs = new URLSearchParams();
  qs.set("departamento", selDep?.value || "");
  qs.set("provincia", selProv?.value || "");
  qs.set("distrito", selDist?.value || "");
  qs.set("zoom", map.getZoom());

  const res = await fetch(`/api/empresas_nominas_points?${qs.toString()}`, {
    signal: _empresasAbort.signal,
    cache: "no-store"
  });
  const data = await res.json();

  (data || []).forEach(r=>{
    const lat = Number(r.lat), lon = Number(r.lon);
    if(!isFinite(lat) || !isFinite(lon)) return;
    const m = L.marker([lat, lon], { icon: empPinIcon(), zIndexOffset: 4600 });
    m.bindPopup(empBalloonHtml(r), {
      className: "com-popup",
      closeButton: false,
      autoPan: true,
      maxWidth: 390
    });
    empresasCluster.addLayer(m);
  });

  _counts.empresas = Array.isArray(data) ? data.length : 0;
  refreshInfoCount();
  if(!map.hasLayer(empresasCluster)) empresasCluster.addTo(map);
  await fetchResumenEmpresasNomina();
}

async function fetchHeatEmpresasNomina(){
  if(!chkHeatEmpresasNomina || !chkHeatEmpresasNomina.checked){
    syncEmpresasNominaVisibility();
    return;
  }

  syncEmpresasNominaVisibility();
  if(_empresasHeatAbort){ try{ _empresasHeatAbort.abort(); }catch(e){} }
  _empresasHeatAbort = new AbortController();

  const b = map.getBounds();
  const qs = new URLSearchParams();
  qs.set("departamento", selDep?.value || "");
  qs.set("provincia", selProv?.value || "");
  qs.set("distrito", selDist?.value || "");
  qs.set("south", b.getSouth());
  qs.set("west",  b.getWest());
  qs.set("north", b.getNorth());
  qs.set("east",  b.getEast());
  qs.set("zoom",  map.getZoom());

  const res = await fetch(`/api/empresas_nominas_heat?${qs.toString()}`, {
    signal: _empresasHeatAbort.signal,
    cache: "no-store"
  });
  const data = await res.json();

  const oportunidades = (data || []).map(r => {
    const trabajadores = Math.max(0, Number(r.trabajadores || 0));
    const stock = Math.max(0, Number(r.stock || 0));

    if(trabajadores <= 0) return 0;

    let penetracion = stock / trabajadores;
    if(!isFinite(penetracion)) penetracion = 0;
    penetracion = Math.max(0, Math.min(1, penetracion));

    return Math.max(0, trabajadores * (1 - penetracion));
  }).filter(v => isFinite(v));

  if(!oportunidades.length){
    heatEmpresasNomina.setLatLngs([]);
    if(!map.hasLayer(heatEmpresasNomina)) heatEmpresasNomina.addTo(map);
    if(typeof heatEmpresasNomina.redraw === "function") heatEmpresasNomina.redraw();
    await fetchResumenEmpresasNomina();
    return;
  }

  let maxOpp = 0;
  oportunidades.forEach(v => { maxOpp = Math.max(maxOpp, v); });
  maxOpp = Math.max(1, maxOpp);

  const totalPts = Array.isArray(data) ? data.length : 0;
  const EMP_HEAT_SCALE = totalPts <= 8 ? 1.55 : totalPts <= 15 ? 1.35 : 1.20;

  const latlngs = (data || []).map(r => {
    const lat = Number(r.lat);
    const lon = Number(r.lon);
    const trabajadores = Math.max(0, Number(r.trabajadores || 0));
    const stock = Math.max(0, Number(r.stock || 0));

    if(!isFinite(lat) || !isFinite(lon) || trabajadores <= 0){
      return null;
    }

    let penetracion = stock / trabajadores;
    if(!isFinite(penetracion)) penetracion = 0;
    penetracion = Math.max(0, Math.min(1, penetracion));

    const oportunidad = Math.max(0, trabajadores * (1 - penetracion));

    // ✅ Normalización robusta
    const norm = Math.log1p(oportunidad) / Math.log1p(maxOpp);

    // ✅ Abrimos mucho más la diferencia visual
    let w = Math.pow(norm, 0.30);

    // ✅ Piso más alto para que sí pinte
    const minFloor = totalPts <= 8 ? 0.34 : totalPts <= 15 ? 0.26 : 0.18;

    w = Math.max(minFloor, Math.min(1, w)) * EMP_HEAT_SCALE;
    w = Math.min(1, w);

    return [lat, lon, w];
  }).filter(Boolean);

  heatEmpresasNomina.setLatLngs(latlngs);

  if(!map.hasLayer(heatEmpresasNomina)) {
    heatEmpresasNomina.addTo(map);
  }

  if(typeof heatEmpresasNomina.redraw === "function") {
    heatEmpresasNomina.redraw();
  }

  await fetchResumenEmpresasNomina();
}

const nodosCluster = L.markerClusterGroup({
  chunkedLoading: true,
  showCoverageOnHover: false,
  spiderfyOnMaxZoom: true,

  // ✅ CLAVE: no revienta al zoom medio/cerca
  disableClusteringAtZoom: 19,
  maxClusterRadius: (zoom) => (zoom >= 15 ? 160 : 110),

  iconCreateFunction: function(cluster){
    const n = cluster.getChildCount();
    return L.divIcon({
      className: "nodo-cluster-icon",
      html: `<div class="nodo-cluster">${n}</div>`,
      iconSize: [44,44],
      iconAnchor: [22,22]
    });
  }
});


    function nodoPinIcon(){
      const svg = `
        <div class="nodo-pin-wrap">
          <svg viewBox="0 0 24 24" fill="#ff2a2a" stroke="#ffffff" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
            <path d="M12 22s7-4.5 7-12a7 7 0 0 0-14 0c0 7.5 7 12 7 12z"></path>
            <circle cx="12" cy="10" r="2.7" fill="#ffffff" stroke="none"></circle>
          </svg>
        </div>`;
      return L.divIcon({
        className: "nodo-pin-icon",
        html: svg,
        iconSize: [34,34],
        iconAnchor: [17,34],
        popupAnchor: [0,-30],
      });
    }

    function nodoBalloonHtml(nombre){
      return `<div class="nodo-balloon">${escHtml(nombre)}</div>`;
    }

    function syncComercialVisibility(){
      if(!chkNodos) return;
      const show = chkNodos.checked;
      if(panelComercial) panelComercial.classList.toggle("hidden", !show);

      if(!show){
        try{ if(map.hasLayer(nodosCluster)) map.removeLayer(nodosCluster); }catch(e){}
        nodosCluster.clearLayers();
        _counts.nodos = 0;
        setComercialCounts(null);
        refreshInfoCount();
      }else{
        if(!map.hasLayer(nodosCluster)) nodosCluster.addTo(map);
      }
    }

    function setComercialCounts(res){
      const z = (id, v)=>{ const el=document.getElementById(id); if(el) el.textContent = String(v ?? 0); };
      if(!res){
        z("comTotal", 0); z("comHosp", 0); z("comClin", 0); z("comCC", 0);
        z("comPV", 0); z("comSod", 0); z("comMet", 0); z("comTot", 0);
        z("comWon", 0); z("comUni", 0); z("comMer", 0);
        return;
      }
      z("comTotal", res.total);
      z("comHosp", res.hospitales);
      z("comClin", res.clinicas);
      z("comCC", res.centros_comerciales);
      z("comPV", res.plaza_vea);
      z("comSod", res.sodimac);
      z("comMet", res.metro);
      z("comTot", res.tottus);
      z("comWon", res.wong);
      z("comUni", res.universidades);
      z("comMer", res.mercados);
    }

    let _nodosLastKey = "";
    let _nodosAbort = null;

    async function fetchNodos(force=false){
      try{
        if(!chkNodos || !chkNodos.checked){
          syncComercialVisibility();
          return;
        }
        syncComercialVisibility();

        const zoom = map.getZoom();
        const d = selDep?.value || "";
        const p = selProv?.value || "";
        const di = selDist?.value || "";

        const qsp = new URLSearchParams();
        qsp.set("zoom", zoom);
        qsp.set("departamento", d);
        qsp.set("provincia", p);
        qsp.set("distrito", di);
        const qs = qsp.toString();

        if(!force && _nodosLastKey === qs){
          if(!map.hasLayer(nodosCluster)) nodosCluster.addTo(map);
          return;
        }
        _nodosLastKey = qs;

        if(_nodosAbort){ try{ _nodosAbort.abort(); }catch(e){} }
        _nodosAbort = new AbortController();

        nodosCluster.clearLayers();

        const res = await fetch(`/api/nodos?${qs}`, { signal: _nodosAbort.signal });
        const js = await res.json();
        const arr = js.nodos || [];
        const resumen = js.resumen || null;

        _counts.nodos = arr.length;
        refreshInfoCount();
        setComercialCounts(resumen);

        arr.forEach(n=>{
          const label = n.nombre_popup || n.nombre || "";
          const m = L.marker([n.lat, n.lon], { icon: nodoPinIcon(), zIndexOffset: 5000 });
          m.bindPopup(nodoBalloonHtml(label), {
            className: "nodo-popup",
            closeButton: false,
            autoPan: true,
            maxWidth: 360
          });
          nodosCluster.addLayer(m);
        });
      }catch(err){
        if(String(err||"").includes("AbortError")) return;
        console.error("Error cargando Comercial/NODOS:", err);
      }
    }

    // ======================================================
    // ✅ PANEL df_comercios visibilidad + stats
    // ======================================================
    function syncDfComerciosPanelVisibility(){
      if(!panelDfComercios) return;
      const show = (chkComerciosPts && chkComerciosPts.checked);
      panelDfComercios.classList.toggle("hidden", !show);
      if(!show){
        setDfComStats(0,0,0);
      }
    }

    function setDfComStats(total, sum, avg){
      const a = document.getElementById("dfcTotal");
      const b = document.getElementById("dfcSum");
      const c = document.getElementById("dfcAvg");
      if(a) a.textContent = String(total ?? 0);
      if(b) b.textContent = String(Math.round(Number(sum||0)));
      if(c) c.textContent = String((Number(avg||0)).toFixed(1));
    }

    // ======================================================
    // ✅ BORDE NEÓN POR DIVISIÓN
    // ======================================================
    let divisionBorderLayer = null;
    function clearDivisionBorder(){
      if(divisionBorderLayer){
        try { map.removeLayer(divisionBorderLayer); } catch(e){}
        divisionBorderLayer = null;
      }
    }
    function convexHullLatLng(latlngs){
      if(!latlngs || latlngs.length <= 2) return latlngs || [];
      const uniq = new Map();
      latlngs.forEach(ll=>{
        const k = ll.lat.toFixed(6) + "," + ll.lng.toFixed(6);
        uniq.set(k, ll);
      });
      const pts = Array.from(uniq.values()).map(ll => ({x: ll.lng, y: ll.lat}));
      if(pts.length <= 2) return pts.map(p => L.latLng(p.y, p.x));
      pts.sort((a,b) => (a.x === b.x) ? (a.y - b.y) : (a.x - b.x));
      const cross = (o,a,b) => (a.x - o.x)*(b.y - o.y) - (a.y - o.y)*(b.x - o.x);
      const lower = [];
      for(const p of pts){
        while(lower.length >= 2 && cross(lower[lower.length-2], lower[lower.length-1], p) <= 0) lower.pop();
        lower.push(p);
      }
      const upper = [];
      for(let i=pts.length-1; i>=0; i--){
        const p = pts[i];
        while(upper.length >= 2 && cross(upper[upper.length-2], upper[upper.length-1], p) <= 0) upper.pop();
        upper.push(p);
      }
      upper.pop(); lower.pop();
      const hull = lower.concat(upper);
      return hull.map(p => L.latLng(p.y, p.x));
    }
    function rectFromLatLngs(latlngs){
      const b = L.latLngBounds(latlngs);
      const sw = b.getSouthWest();
      const ne = b.getNorthEast();
      return [ sw, L.latLng(sw.lat, ne.lng), ne, L.latLng(ne.lat, sw.lng) ];
    }
    function drawDivisionBorder(latlngs){
      clearDivisionBorder();
      if(!latlngs || latlngs.length === 0) return;
      const glow = L.polygon(latlngs, {
        color: "#1E6CFF", weight: 18, opacity: 0.22, fill: false,
        lineCap: "round", lineJoin: "round", interactive: false, className: "division-neon"
      });
      const main = L.polygon(latlngs, {
        color: "#1E6CFF", weight: 9, opacity: 0.98, fill: false,
        lineCap: "round", lineJoin: "round", interactive: false, className: "division-neon"
      });
      divisionBorderLayer = L.layerGroup([glow, main]).addTo(map);
      try { glow.bringToFront(); main.bringToFront(); } catch(e){}
    }
    function updateDivisionBorderFromPoints(latlngs){
      const dv = (selDiv && selDiv.value) ? String(selDiv.value).trim() : "";
      if(!dv){ clearDivisionBorder(); return; }
      if(!latlngs || latlngs.length === 0){ clearDivisionBorder(); return; }
      let outline = [];
      if(latlngs.length < 3){
        outline = rectFromLatLngs(latlngs);
      }else{
        outline = convexHullLatLng(latlngs);
        if(!outline || outline.length < 3){
          outline = rectFromLatLngs(latlngs);
        }
      }
      drawDivisionBorder(outline);
    }

    // ======================================================
    // ✅ ZONAS RURAL / URBANA
    // ======================================================
    let zonaRuralLayer = null;
    let zonaUrbanLayer = null;

    function clearZonaRural(){
      if(zonaRuralLayer){ try{ map.removeLayer(zonaRuralLayer); }catch(e){} zonaRuralLayer=null; }
    }
    function clearZonaUrban(){
      if(zonaUrbanLayer){ try{ map.removeLayer(zonaUrbanLayer); }catch(e){} zonaUrbanLayer=null; }
    }

    function drawZona(polyLatLng, color, className){
      if(!polyLatLng || polyLatLng.length < 3) return null;

      const glow = L.polygon(polyLatLng, {
        pane: "zonesPane",
        color: color, weight: 18, opacity: 0.22, fill: false,
        lineCap: "round", lineJoin: "round",
        interactive: false,
        className: className
      });

      const main = L.polygon(polyLatLng, {
        pane: "zonesPane",
        color: color, weight: 9, opacity: 0.98, fill: false,
        lineCap: "round", lineJoin: "round",
        interactive: false,
        className: className
      });

      const grp = L.layerGroup([glow, main]).addTo(map);
      try { glow.bringToFront(); main.bringToFront(); } catch(e){}
      return grp;
    }

    async function fetchZonasBorders(){
      const showR = (chkZonaRural && chkZonaRural.checked);
      const showU = (chkZonaUrbana && chkZonaUrbana.checked);

      if(!showR) clearZonaRural();
      if(!showU) clearZonaUrban();

      const ruralCountEl = document.getElementById("zonaRuralCount");
      const urbanCountEl = document.getElementById("zonaUrbanCount");
      if(!showR && ruralCountEl) ruralCountEl.textContent = "0";
      if(!showU && urbanCountEl) urbanCountEl.textContent = "0";

      if(!showR && !showU) return;

      try{
        const d = selDep.value, p = selProv.value, di = selDist.value;
        const qs = `departamento=${encodeURIComponent(d)}&provincia=${encodeURIComponent(p)}&distrito=${encodeURIComponent(di)}`;
        const res = await fetch(`/api/zonas?${qs}`);
        const js = await res.json();

        const rural = js.rural || {};
        const urbano = js.urbano || {};

        if(ruralCountEl) ruralCountEl.textContent = String(rural.count ?? 0);
        if(urbanCountEl) urbanCountEl.textContent = String(urbano.count ?? 0);

        if(showR){
          clearZonaRural();
          zonaRuralLayer = drawZona(rural.poly || [], "#00FF66", "zone-neon-rural");
        }
        if(showU){
          clearZonaUrban();
          zonaUrbanLayer = drawZona(urbano.poly || [], "#D6FF00", "zone-neon-urban");
        }
      }catch(err){
        console.error("Error cargando zonas:", err);
      }
    }

    // ======================================================
    // COMBOS DEP/PROV/DIST/DIV
    // ======================================================
    function _setSelectOptions(selectEl, placeholder, options, currentValue){
      selectEl.innerHTML = `<option value="">${placeholder}</option>`;
      options.forEach(v => {
        selectEl.innerHTML += `<option value="${v}">${v}</option>`;
      });

      if(currentValue && options.includes(currentValue)){
        selectEl.value = currentValue;
      } else {
        selectEl.value = "";
      }
    }

    function _buildReverseMaps(){
      const deptsByProv = {};
      const provsByDist = {};
      const deptsByDist = {};

      Object.entries(PROV_BY_DEPT).forEach(([dep, provs]) => {
        (provs || []).forEach(prov => {
          if(!deptsByProv[prov]) deptsByProv[prov] = new Set();
          deptsByProv[prov].add(dep);
        });
      });

      Object.entries(DIST_BY_PROV).forEach(([prov, dists]) => {
        (dists || []).forEach(dist => {
          if(!provsByDist[dist]) provsByDist[dist] = new Set();
          provsByDist[dist].add(prov);

          if(!deptsByDist[dist]) deptsByDist[dist] = new Set();
          const deps = deptsByProv[prov] ? Array.from(deptsByProv[prov]) : [];
          deps.forEach(dep => deptsByDist[dist].add(dep));
        });
      });

      return { deptsByProv, provsByDist, deptsByDist };
    }

    const GEO_REVERSE = _buildReverseMaps();

    function _intersection(baseArr, allowedSet){
      if(!allowedSet) return [...baseArr];
      return baseArr.filter(v => allowedSet.has(v));
    }

    function refreshGeoSelectors(){
      const depVal  = selDep.value;
      const provVal = selProv.value;
      const distVal = selDist.value;

      let deps  = [...Object.keys(PROV_BY_DEPT)].sort();
      let provs = [...ALL_PROVINCIAS];
      let dists = [...ALL_DISTRITOS];

      // si hay departamento elegido, reduce provincias y distritos compatibles
      if(depVal){
        provs = _intersection(provs, new Set(PROV_BY_DEPT[depVal] || []));

        const distsForDep = new Set();
        (PROV_BY_DEPT[depVal] || []).forEach(p => {
          (DIST_BY_PROV[p] || []).forEach(d => distsForDep.add(d));
        });
        dists = _intersection(dists, distsForDep);
      }

      // si hay provincia elegida, reduce departamentos y distritos compatibles
      if(provVal){
        deps  = _intersection(deps, GEO_REVERSE.deptsByProv[provVal]);
        dists = _intersection(dists, new Set(DIST_BY_PROV[provVal] || []));
      }

      // si hay distrito elegido, reduce departamentos y provincias compatibles
      if(distVal){
        deps  = _intersection(deps, GEO_REVERSE.deptsByDist[distVal]);
        provs = _intersection(provs, GEO_REVERSE.provsByDist[distVal]);
      }

      deps.sort();
      provs.sort();
      dists.sort();

      _setSelectOptions(selDep,  "-- Todos --", deps,  depVal);
      _setSelectOptions(selProv, "-- Todas --", provs, provVal);
      _setSelectOptions(selDist, "-- Todos --", dists, distVal);

      updateDivisiones();
    }

    function updateProvincias(){
      refreshGeoSelectors();
    }

    function updateDistritos(){
      refreshGeoSelectors();
    }

    function updateDivisiones(){
      const d = selDep.value;
      const p = selProv.value;
      const di = selDist.value;
      selDiv.innerHTML = '<option value="">-- Todas --</option>';

      if(di && DIV_BY_DIST[di]){
        DIV_BY_DIST[di].forEach(v => selDiv.innerHTML += `<option value="${v}">${v}</option>`);
        return;
      }
      if(p && DIV_BY_PROV[p]){
        DIV_BY_PROV[p].forEach(v => selDiv.innerHTML += `<option value="${v}">${v}</option>`);
        return;
      }
      if(d && DIV_BY_DEPT[d]){
        DIV_BY_DEPT[d].forEach(v => selDiv.innerHTML += `<option value="${v}">${v}</option>`);
        return;
      }
      ({{ divisiones|tojson }}).forEach(v => selDiv.innerHTML += `<option value="${v}">${v}</option>`);
    }

    function ubicarOficinaSeleccionada(){
      if(!selOficina) return;

      const idx = selOficina.value;
      if(idx === "" || idx === null || idx === undefined){
        clearOfficeFocus();
        return;
      }

      const baseOfi = OFICINAS_LOOKUP[Number(idx)];
      if(!baseOfi) return;

      const nombreSel = String(baseOfi.nombre || "").trim().toUpperCase();

      // buscamos primero en la data real cargada en el mapa
      let ofiReal = latestOficinasData.find(o =>
        String(o.nombre || "").trim().toUpperCase() === nombreSel
      );

      // fallback por si aún no encuentra la oficina en latestOficinasData
      if(!ofiReal){
        ofiReal = {
          nombre: baseOfi.nombre,
          lat: baseOfi.lat,
          lon: baseOfi.lon,
          atm: "",
          promedio: 0,
          division: "",
          tipo: "OFICINA",
          ubicacion: "OFICINA",
          departamento: baseOfi.departamento || "",
          provincia: baseOfi.provincia || "",
          distrito: baseOfi.distrito || "",
          direccion: "",
          estructura_as: 0,
          estructura_ebp: 0,
          estructura_ad: 0,
          clientes_unicos: 0,
          total_tickets: 0,
          red_lines: 0,
          performance_2025: "",
          bai: 0,
          margen_neto: 0,
          tipo_canal: "OFICINA"
        };
      }

      focusOfficeOnMap(ofiReal);
    }


    // ======================================================
    // CLIENTES (RESUMEN)
    // ======================================================
    async function fetchResumenClientes(){
      const d = selDep.value, p = selProv.value, di = selDist.value, seg = "";
      const qs = `departamento=${encodeURIComponent(d)}&provincia=${encodeURIComponent(p)}&distrito=${encodeURIComponent(di)}&segmento=${encodeURIComponent(seg)}`;
      const res = await fetch(`/api/resumen_clientes?${qs}`);
      const js = await res.json();
      document.getElementById("cliTotal").textContent = js.total;
      document.getElementById("cliDigital").textContent = js.digital_pct + "%";
      document.getElementById("cliEdad").textContent = js.edad_prom;
      document.getElementById("cliIngreso").textContent = js.ingreso_prom;
      document.getElementById("cliDeuda").textContent = js.deuda_prom;
      document.getElementById("cliTopSeg").textContent = js.top_segmento;
    }

   

    // ======================================================
// ✅ HEATMAP CANTIDAD DE CLIENTES (COMERCIOS) — robusto
// FIX 2do click: re-add + redraw SIEMPRE, y si quedó vacío => vuelve a fetchear
// ======================================================
let _heatCantLastKey = "";
let _heatCantAbort = null;
let _heatCantLoading = false;

function clearHeatCantClientes(){
  if(_heatCantAbort){ try{ _heatCantAbort.abort(); }catch(e){} }
  _heatCantAbort = null;
  _heatCantLastKey = "";
  _heatCantLoading = false;

  try{ if(map.hasLayer(heatCantClientes)) map.removeLayer(heatCantClientes); }catch(e){}
  try{ heatCantClientes.setLatLngs([]); }catch(e){}
}

async function fetchHeatCantClientes(force=false){
  try{
    if(!chkHeatCantClientes || !chkHeatCantClientes.checked){
      clearHeatCantClientes();
      return;
    }

    // ✅ si está activo, aseguro SIEMPRE que el layer esté en el mapa
    try{ if(!map.hasLayer(heatCantClientes)) heatCantClientes.addTo(map); }catch(e){}

    const zoom = map.getZoom();
    // const d  = selDep.value, p = selProv.value, di = selDist.value;
    const key = `${zoom}`;

    // ✅ si mismo key y NO force:
    // - si está cargando => solo re-add + redraw (no duplicar fetch)
    // - si tiene data => re-add + redraw
    // - si NO tiene data => FORZAR fetch (esto mata el bug "se quedó vacío")
    const hasData = !!(heatCantClientes && heatCantClientes._latlngs && heatCantClientes._latlngs.length > 0);

    if(!force && _heatCantLastKey === key){
      if(_heatCantLoading){
        try{ if(!map.hasLayer(heatCantClientes)) heatCantClientes.addTo(map); }catch(e){}
        try{ heatCantClientes.redraw(); }catch(e){}
        return;
      }
      if(hasData){
        try{ if(!map.hasLayer(heatCantClientes)) heatCantClientes.addTo(map); }catch(e){}
        try{ heatCantClientes.redraw(); }catch(e){}
        return;
      }
      // si key igual pero está vacío => seguimos a fetch
    }

    _heatCantLastKey = key;

    if(_heatCantAbort){ try{ _heatCantAbort.abort(); }catch(e){} }
    _heatCantAbort = new AbortController();

    _heatCantLoading = true;

    const qs = `zoom=${zoom}`;
    const res = await fetch(`/api/heatmap_cant_clientes?${qs}`, { signal: _heatCantAbort.signal });
    const data = await res.json();

    const arr = (data || []);

    let maxC = 0;
    arr.forEach(r => { maxC = Math.max(maxC, Number(r.cant_clientes || r.w || 0)); });
    maxC = maxC || 1;

    // ✅ filtra coords inválidas (evita que el heat "muera")
    const pts = arr
      .map(r => {
        const lat = Number(r.lat), lon = Number(r.lon);
        if(!isFinite(lat) || !isFinite(lon)) return null;

        const c = Math.max(0, Number(r.cant_clientes || r.w || 0));
        const norm = Math.log1p(c) / Math.log1p(maxC);
        const HEAT_CANT_SCALE = 0.85; // ✅ menos fosforescente (0.75–0.90 recomendado)

const w = Math.min(1, Math.max(0.06, Math.pow(norm, 0.40))) * HEAT_CANT_SCALE;
return [lat, lon, w];
      })
      .filter(Boolean);

    heatCantClientes.setLatLngs(pts);

    // ✅ asegura mostrar SIEMPRE al activar
    if(!map.hasLayer(heatCantClientes)) heatCantClientes.addTo(map);
    heatCantClientes.redraw();

  }catch(err){
    if(String(err||"").includes("AbortError")) return;
    console.error("Error cargando heatmap cant_clientes:", err);
  }finally{
    _heatCantLoading = false;
  }
}

function syncHeatCantClientes(force=false){
  fetchHeatCantClientes(force);
}


// ======================================================
// ✅ PUNTOS df_comercios (independiente) + popup (SIN PARPADEO en zoom)
// FIX: si ya estaban cargados pero el layer fue removido, re-add inmediato
// ======================================================
let _comPtsLastKey = "";
let _comPtsAbort = null;
let _comPtsLoading = false;

function clearComerciosPts(){
  if(_comPtsAbort){ try{ _comPtsAbort.abort(); }catch(e){} }
  _comPtsAbort = null;
  _comPtsLastKey = "";
  _comPtsLoading = false;

  try{ if(map.hasLayer(comerciosCluster)) map.removeLayer(comerciosCluster); }catch(e){}
  try{ comerciosCluster.clearLayers(); }catch(e){}

  _counts.comercios = 0;
  refreshInfoCount();

  syncDfComerciosPanelVisibility();
}

async function fetchComerciosPts(force=false){
  try{
    if(!chkComerciosPts || !chkComerciosPts.checked){
      clearComerciosPts();
      return;
    }

    syncDfComerciosPanelVisibility();

    // clave SIN zoom -> no parpadea con zoom
    //const d = selDep.value, p = selProv.value, di = selDist.value;
    const key = `ALL`;

    // ✅ si mismo key y ya hay layers cargados:
    // - si el cluster NO está en el mapa => re-add y listo
    // - si está cargando => re-add y listo
    if(!force && _comPtsLastKey === key){
      const hasLayers = (comerciosCluster && comerciosCluster.getLayers && comerciosCluster.getLayers().length > 0);
      if(_comPtsLoading && hasLayers){
        if(!map.hasLayer(comerciosCluster)) comerciosCluster.addTo(map);
        return;
      }
      if(hasLayers){
        if(!map.hasLayer(comerciosCluster)) comerciosCluster.addTo(map);
        return;
      }
      // si key igual pero está vacío => seguimos a fetch
    }

    _comPtsLastKey = key;

    if(_comPtsAbort){ try{ _comPtsAbort.abort(); }catch(e){} }
    _comPtsAbort = new AbortController();

    _comPtsLoading = true;

    try{ comerciosCluster.clearLayers(); }catch(e){}

    const res = await fetch(`/api/comercios_points`, { signal: _comPtsAbort.signal });
    const data = await res.json();

    let sumCant = 0;
    let cnt = 0;

    (data || []).forEach(r=>{
      const lat = Number(r.lat), lon = Number(r.lon);
      if(!isFinite(lat) || !isFinite(lon)) return;

      const c = Number(r.cant_clientes || 0);
      if(isFinite(c)) sumCant += c;
      cnt++;

      const m = L.marker([lat, lon], { icon: comPinIcon(), zIndexOffset: 4500 });
      m.bindPopup(comBalloonHtml(r), {
        className: "com-popup",
        closeButton: false,
        autoPan: true,
        maxWidth: 380
      });
      comerciosCluster.addLayer(m);
    });

    _counts.comercios = cnt;
    refreshInfoCount();

    const avg = cnt ? (sumCant / cnt) : 0;
    setDfComStats(cnt, sumCant, avg);

    if(!map.hasLayer(comerciosCluster)) comerciosCluster.addTo(map);

  }catch(err){
    if(String(err||"").includes("AbortError")) return;
    console.error("Error cargando df_comercios (globos azules):", err);
  }finally{
    _comPtsLoading = false;
  }
}

function syncComerciosPts(force=false){
  fetchComerciosPts(force);
}


// ======================================================
// ✅ HOOKS: checkboxes independientes + zoom (solo heatmap) + filtros
// (evita listeners duplicados si este script se evalúa más de 1 vez)
// ======================================================
function syncComerciosLayersOnFilters(force=true){
  if(chkHeatCantClientes && chkHeatCantClientes.checked) syncHeatCantClientes(force);
  if(chkComerciosPts && chkComerciosPts.checked) syncComerciosPts(force);
}

if(!window.__BBVA_COMERCIOS_HOOKS__){
  window.__BBVA_COMERCIOS_HOOKS__ = true;

  // ✅ toggles independientes
  if(chkHeatCantClientes){
    chkHeatCantClientes.addEventListener("change", () => {
      if(chkHeatCantClientes.checked) syncHeatCantClientes(true);
      else clearHeatCantClientes();
    });
  }

  if(chkComerciosPts){
    chkComerciosPts.addEventListener("change", () => {
      if(chkComerciosPts.checked) syncComerciosPts(true);
      else clearComerciosPts();
    });
  }

  // ✅ el heatmap depende del zoom (tu backend muestrea por zoom)
  if(map){
    map.on("zoomend", () => {
      if(chkHeatCantClientes && chkHeatCantClientes.checked) syncHeatCantClientes(false);
    });
  }

  // ✅ al cambiar filtros, refresca SOLO si están activos
  [selDep, selProv, selDist].forEach(el=>{
    if(!el) return;
    el.addEventListener("change", () => {
      syncComerciosLayersOnFilters(true);
    });
  });
}

    // ======================================================
    // RECOMENDACIONES
    // ======================================================
    let recoLoaded = false;
    async function cargarRecomendaciones(){
      try {
        const res = await fetch("/api/recomendaciones");
        const data = await res.json();
        markersReco.clearLayers();
        data.forEach(r => {
          const m = L.marker([r.lat, r.lon], {
            icon: L.divIcon({ className: "icon-reco", html: "⚡", iconSize: [36, 36], iconAnchor: [18, 18] }),
            zIndexOffset: 2000
          });
          m.on("click", () => showRecoPanel(r));
          markersReco.addLayer(m);
        });
        recoLoaded = true;
        if (chkReco && chkReco.checked){
          if(!map.hasLayer(markersReco)) markersReco.addTo(map);
        }
      } catch(err){
        console.error("Error cargando recomendaciones:", err);
      }
    }

    // ======================================================
    // CAPAS NORMALES (NO integral)
    // ======================================================
    async function fetchPoints(){
      if(TIPO_MAPA === "integral") return;

      const d = selDep.value, p = selProv.value, di = selDist.value, dv = selDiv.value;
      const t_atm = selTipoATM ? selTipoATM.value : "";
      const u_atm = selUbicATM ? selUbicATM.value : "";

      const qs = `tipo=${TIPO_MAPA}&departamento=${encodeURIComponent(d)}&provincia=${encodeURIComponent(p)}&distrito=${encodeURIComponent(di)}&division=${encodeURIComponent(dv)}&tipo_atm=${encodeURIComponent(t_atm)}&ubic_atm=${encodeURIComponent(u_atm)}`;

      infoBox.textContent = "...";
      panelATM.classList.add("hidden");

      const res = await fetch(`/api/points_integral?${qs}`);
      const data = await res.json();
      const pts = data.puntos || [];

      _counts.base = (data.total_atms ?? pts.length) || 0;
      refreshInfoCount();

      markers.clearLayers();
heat.setLatLngs([]);
heatGlow.setLatLngs([]);   // ✅ NUEVO

let bounds = [];

// ✅ peso por TRX/promedio (normaliza a 0..1 robusto)
const wFn = _buildWeightFn(pts);

// ✅ construye heatPts con peso real (no 1.0)
const heatPts = (pts || [])
  .filter(p => p && isFinite(p.lat) && isFinite(p.lon))
  .map(p => [Number(p.lat), Number(p.lon), wFn(p)]);

pts.forEach(pt => {
  if(!pt || !isFinite(pt.lat) || !isFinite(pt.lon)) return;

const baseIcon = getIcon(pt);
const icon = (chkHeat && chkHeat.checked) ? getTrxHaloIcon(pt) : baseIcon;
const m = L.marker([pt.lat, pt.lon], { icon, zIndexOffset: 1200 });

  m.on("click", () => showATMPanel(pt));
  markers.addLayer(m);

  bounds.push([pt.lat, pt.lon]);
});

// ✅ set a ambas capas (base + halo)
heat.setLatLngs(heatPts);
heatGlow.setLatLngs(heatPts);


      if(bounds.length === 1) map.setView(bounds[0], 16);
      else if(bounds.length > 1) map.fitBounds(bounds, {padding:[20,20]});
      else map.setView(INITIAL_CENTER, INITIAL_ZOOM);

     if(chkHeat && chkHeat.checked){
  if(!map.hasLayer(heat)) heat.addTo(map);
  if(!map.hasLayer(heatGlow)) heatGlow.addTo(map);  // ✅ NUEVO

  if(typeof heat.redraw === "function") heat.redraw();
  if(typeof heatGlow.redraw === "function") heatGlow.redraw(); // ✅ NUEVO
}else{
  if(map.hasLayer(heat)) map.removeLayer(heat);
  if(map.hasLayer(heatGlow)) map.removeLayer(heatGlow); // ✅ NUEVO
}


      updateDivisionBorderFromPoints(bounds.map(b => L.latLng(b[0], b[1])));

      if(TIPO_MAPA === "islas"){
        document.getElementById("resAtmTotal").textContent = data.total_atms || 0;
        document.getElementById("resAtmSuma").textContent = Math.round(data.suma_total || 0);
        document.getElementById("resAtmEnOfi").textContent = data.total_oficinas || 0;
        document.getElementById("resAtmEnIsla").textContent = data.total_islas || 0;
        document.getElementById("resAtmDisp").textContent = data.total_disp || 0;
        document.getElementById("resAtmMon").textContent = data.total_mon || 0;
        document.getElementById("resAtmRec").textContent = data.total_rec || 0;
      }

      if(TIPO_MAPA === "oficinas"){
        document.getElementById("resOfiTotal").textContent = data.total_oficinas || 0;
        document.getElementById("resOfiSuma").textContent = fmt0(data.suma_trx);
        document.getElementById("resOfiPromEAS").textContent = fmt0(data.suma_estructura_as);
        document.getElementById("resOfiPromEBP").textContent = fmt0(data.suma_estructura_ebp);
        document.getElementById("resOfiPromEAD").textContent = fmt0(data.suma_estructura_ad);
        document.getElementById("resOfiPromCLI").textContent = fmt0(data.suma_clientes_unicos);
        document.getElementById("resOfiPromTKT").textContent = fmt0(data.suma_total_tickets);
        document.getElementById("resOfiPromRED").textContent = fmtPct(data.prom_redlines);
      }

      if(TIPO_MAPA === "agentes"){
        document.getElementById("resAgTotal").textContent = data.total_agentes || 0;
        document.getElementById("resAgSuma").textContent = Math.round(data.suma_total || 0);
        document.getElementById("resAgA1").textContent = data.total_capa_A1 || 0;
        document.getElementById("resAgA2").textContent = data.total_capa_A2 || 0;
        document.getElementById("resAgA3").textContent = data.total_capa_A3 || 0;
        document.getElementById("resAgB").textContent = data.total_capa_B || 0;
        document.getElementById("resAgC").textContent = data.total_capa_C || 0;
      }

      syncSinglePanelsVisibility();

      if (chkReco && chkReco.checked){
        if(!recoLoaded) await cargarRecomendaciones();
        if (!map.hasLayer(markersReco)) markersReco.addTo(map);
      } else {
        if (map.hasLayer(markersReco)) map.removeLayer(markersReco);
      }

      await fetchZonasBorders();
      await fetchNodos();
      await fetchHeatCantClientes(true);
      await fetchHeatClientes(true);

      // df_comercios: con filtros (sin parpadeo por zoom)
      await fetchComerciosPts(true);
    }

    // ======================================================
    // INTEGRAL
    // ======================================================
    const chkATMs = document.getElementById("chkShowATMs");
    const chkOficinas = document.getElementById("chkShowOficinas");
    const chkAgentes = document.getElementById("chkShowAgentes");

    function syncIntegralPanelsVisibility(){
      if(TIPO_MAPA !== "integral") return;
      if(panelATMResumen) panelATMResumen.classList.toggle("hidden", !(chkATMs && chkATMs.checked));
      if(panelOfiResumen) panelOfiResumen.classList.toggle("hidden", !(chkOficinas && chkOficinas.checked));
      if(panelAgResumen)  panelAgResumen.classList.toggle("hidden", !(chkAgentes && chkAgentes.checked));
    }

    async function fetchIntegral(){
      if(TIPO_MAPA !== "integral") return;

      const d = selDep.value, p = selProv.value, di = selDist.value, dv = selDiv.value;

      const showATMs = !chkATMs || chkATMs.checked;
      const showOfi  = !chkOficinas || chkOficinas.checked;
      const showAg   = !chkAgentes || chkAgentes.checked;

      if(!showATMs && !showOfi && !showAg){
        markers.clearLayers();
        heat.setLatLngs([]);

        _counts.base = 0;
        refreshInfoCount();

        if(chkHeat && chkHeat.checked){
          if(!map.hasLayer(heat)) heat.addTo(map);
          heat.redraw();
        }else{
          try{ if(map.hasLayer(heat)) map.removeLayer(heat); }catch(e){}
        }

        updateDivisionBorderFromPoints([]);
        syncIntegralPanelsVisibility();

        if (chkReco && chkReco.checked){
          if(!recoLoaded) await cargarRecomendaciones();
          if (!map.hasLayer(markersReco)) markersReco.addTo(map);
        } else {
          if (map.hasLayer(markersReco)) map.removeLayer(markersReco);
        }

        await fetchZonasBorders();
        await fetchNodos(true);
        await fetchHeatCantClientes(true);
        await fetchHeatClientes(true);
        await fetchComerciosPts(true);
        return;
      }

      const qs = `departamento=${encodeURIComponent(d)}&provincia=${encodeURIComponent(p)}&distrito=${encodeURIComponent(di)}&division=${encodeURIComponent(dv)}`;

      infoBox.textContent = "...";
      panelATM.classList.add("hidden");

      const res = await fetch(`/api/points_integral?${qs}`);
      const data = await res.json();

      latestOficinasData = Array.isArray(data.oficinas) ? data.oficinas : [];

      markers.clearLayers();
      heat.setLatLngs([]);
      heatGlow.setLatLngs([]);
      clearOfficeFocus();

      let bounds = [];
      let shownCount = 0;

      // ✅ guardamos puntos reales para calcular pesos por promedio
      const ptsHeat = [];


      if(showATMs){
        shownCount += (data.total_atms || 0);
        (data.atms || []).forEach(pt=>{
          const ubic = (pt.ubicacion || "").toUpperCase();
         const baseIcon = ubic.includes("OFICINA") ? ICON_ATM_OFICINA : ICON_ATM_ISLA;
const icon = (chkHeat && chkHeat.checked) ? getTrxHaloIcon({ ...pt, tipo_canal:"ATM" }) : baseIcon;
const m = L.marker([pt.lat, pt.lon], { icon, zIndexOffset: 1100 });

          m.on("click",()=>showATMPanel(pt));
          markers.addLayer(m);
          ptsHeat.push(pt); // ✅ luego se normaliza con _buildWeightFn

          bounds.push([pt.lat, pt.lon]);
        });
      }

if(showOfi){
  shownCount += (data.total_oficinas || 0);
    (data.oficinas || []).forEach(pt=>{
      const icon = (chkHeat && chkHeat.checked) ? getTrxHaloIcon({ ...pt, tipo_canal:"OFICINA" }) : ICON_OFICINA;
const m = L.marker([pt.lat, pt.lon], { icon, zIndexOffset: 1400 });

      m.on("click",()=>showATMPanel(pt));
      markers.addLayer(m);

      // ✅ AÑADIR AL HEATMAP (TRX oficinas)
    ptsHeat.push(pt);


      bounds.push([pt.lat, pt.lon]);
  });
}

    if(showAg){
  shownCount += (data.total_agentes || 0);
  (data.agentes || []).forEach(pt=>{
   const icon = (chkHeat && chkHeat.checked) ? getTrxHaloIcon({ ...pt, tipo_canal:"AGENTE" }) : ICON_AGENTE;
const m = L.marker([pt.lat, pt.lon], { icon, zIndexOffset: 1200 });

    m.on("click",()=>showATMPanel(pt));
    markers.addLayer(m);

    // ✅ AÑADIR AL HEATMAP (TRX agentes)
    ptsHeat.push(pt);


    bounds.push([pt.lat, pt.lon]);
  });

}

// ✅ convierte ptsHeat a intensidades 0..1 usando promedio (robusto)
const wFn = _buildWeightFn(ptsHeat);

const heatPts = (ptsHeat || [])
  .filter(p => p && isFinite(p.lat) && isFinite(p.lon))
  .map(p => [Number(p.lat), Number(p.lon), wFn(p)]);

heat.setLatLngs(heatPts);
heatGlow.setLatLngs(heatPts);


      _counts.base = shownCount;
      refreshInfoCount();

      heat.setLatLngs(heatPts);

      if(bounds.length === 1) map.setView(bounds[0], 16);
      else if(bounds.length > 1) map.fitBounds(bounds, {padding:[20,20]});
      else map.setView(INITIAL_CENTER, INITIAL_ZOOM);

     
      if(chkHeat && chkHeat.checked){
        if(!map.hasLayer(heat)) heat.addTo(map);
        if(!map.hasLayer(heatGlow)) heatGlow.addTo(map);
        if(typeof heat.redraw === "function") heat.redraw();
        if(typeof heatGlow.redraw === "function") heatGlow.redraw();
      }else{
        try{ if(map.hasLayer(heat)) map.removeLayer(heat); }catch(e){}
        try{ if(map.hasLayer(heatGlow)) map.removeLayer(heatGlow); }catch(e){}
      }


      updateDivisionBorderFromPoints(bounds.map(b => L.latLng(b[0], b[1])));

      // ATM resumen (integral)
      let atm_total = (data.total_atms || 0);
      let atm_suma  = (data.suma_atms || 0);
      let atm_ofi=0, atm_isla=0, atm_disp=0, atm_mon=0, atm_rec=0;
      (data.atms || []).forEach(pt=>{
        const u = (pt.ubicacion || "").toUpperCase();
        const t = (pt.tipo || "").toUpperCase();
        if(u.includes("OFICINA")) atm_ofi++; else atm_isla++;
        if(t.includes("DISPENSADOR")) atm_disp++;
        if(t.includes("MONEDERO")) atm_mon++;
        if(t.includes("RECICLADOR")) atm_rec++;
      });

      document.getElementById("resAtmTotal").textContent = showATMs ? atm_total : 0;
      document.getElementById("resAtmSuma").textContent  = showATMs ? fmt0(atm_suma) : "0";
      document.getElementById("resAtmEnOfi").textContent = showATMs ? atm_ofi : 0;
      document.getElementById("resAtmEnIsla").textContent= showATMs ? atm_isla : 0;
      document.getElementById("resAtmDisp").textContent  = showATMs ? atm_disp : 0;
      document.getElementById("resAtmMon").textContent   = showATMs ? atm_mon : 0;
      document.getElementById("resAtmRec").textContent   = showATMs ? atm_rec : 0;

      // Oficinas resumen (integral)
      const ofi_total = (data.total_oficinas || 0);
      const ofi_suma  = (data.suma_oficinas || 0);

      document.getElementById("resOfiTotal").textContent = showOfi ? ofi_total : 0;
      document.getElementById("resOfiSuma").textContent  = showOfi ? fmt0(ofi_suma) : "0";

      document.getElementById("resOfiPromEAS").textContent = showOfi ? fmt0(data.suma_ofi_estructura_as) : "0.00";
      document.getElementById("resOfiPromEBP").textContent = showOfi ? fmt0(data.suma_ofi_estructura_ebp) : "0.00";
      document.getElementById("resOfiPromEAD").textContent = showOfi ? fmt0(data.suma_ofi_estructura_ad) : "0.00";
      document.getElementById("resOfiPromCLI").textContent = showOfi ? fmt0(data.suma_ofi_clientes_unicos) : "0";
      document.getElementById("resOfiPromTKT").textContent = showOfi ? fmt0(data.suma_ofi_total_tickets) : "0";
      document.getElementById("resOfiPromRED").textContent = showOfi ? fmtPct(data.prom_ofi_redlines) : "0.00%";
      
      // Agentes resumen (integral)
      const ag_total = (data.total_agentes || 0);
      const ag_suma  = (data.suma_agentes || 0);

      let agA1=0, agA2=0, agA3=0, agB=0, agC=0;
      (data.agentes || []).forEach(pt=>{
        const c = String(pt.capa||"").toUpperCase().trim();
        if(c==="A1") agA1++;
        else if(c==="A2") agA2++;
        else if(c==="A3") agA3++;
        else if(c==="B") agB++;
        else if(c==="C") agC++;
      });

      document.getElementById("resAgTotal").textContent = showAg ? ag_total : 0;
      document.getElementById("resAgSuma").textContent   = showAg  ? fmt0(ag_suma)  : "0";
      document.getElementById("resAgA1").textContent    = showAg ? agA1 : 0;
      document.getElementById("resAgA2").textContent    = showAg ? agA2 : 0;
      document.getElementById("resAgA3").textContent    = showAg ? agA3 : 0;
      document.getElementById("resAgB").textContent     = showAg ? agB  : 0;
      document.getElementById("resAgC").textContent     = showAg ? agC  : 0;

      syncIntegralPanelsVisibility();

      if (chkReco && chkReco.checked){
        if(!recoLoaded) await cargarRecomendaciones();
        if (!map.hasLayer(markersReco)) markersReco.addTo(map);
      } else {
        if (map.hasLayer(markersReco)) map.removeLayer(markersReco);
      }

      await fetchZonasBorders();
      await fetchNodos(true);
      await fetchHeatCantClientes(true);
      await fetchHeatClientes(true);
      await fetchComerciosPts(true);
    }

    // ======================================================
    // EVENTOS / INIT
    // ======================================================
    let _reloadSeq = 0;

    async function scheduleReload(force=true){
      const mySeq = ++_reloadSeq;

      try{
        if(TIPO_MAPA === "integral") await fetchIntegral();
        else await fetchPoints();

        if(mySeq !== _reloadSeq) return;

        if(chkNodos && chkNodos.checked) await fetchNodos(true);
        if(chkComerciosPts && chkComerciosPts.checked) await fetchComerciosPts(true);

        if(chkHeatClientes && chkHeatClientes.checked) await fetchHeatClientes(true);
        if(chkHeatClientes && chkHeatClientes.checked) _scheduleHeatClientes(true);
        if(chkHeatCantClientes && chkHeatCantClientes.checked) await fetchHeatCantClientes(true);

        await fetchZonasBorders();
        refreshInfoCount();

      }catch(e){
        console.error("scheduleReload error:", e);

        try{
          if(chkNodos && chkNodos.checked) await fetchNodos(true);
          if(chkComerciosPts && chkComerciosPts.checked) await fetchComerciosPts(true);
          if(chkHeatClientes && chkHeatClientes.checked) await fetchHeatClientes(true);
          if(chkHeatCantClientes && chkHeatCantClientes.checked) await fetchHeatCantClientes(true);
          await fetchZonasBorders();
          refreshInfoCount();
        }catch(_){}
      }
    }

    selDep.addEventListener("change", () => {
      refreshGeoSelectors();
      scheduleReload(true);
    });

    selProv.addEventListener("change", () => {
      refreshGeoSelectors();
      scheduleReload(true);
    });

    selDist.addEventListener("change", () => {
      refreshGeoSelectors();
      scheduleReload(true);
    });

    selDiv.addEventListener("change", () => {
      scheduleReload(true);
    });

    if(selOficina){
      selOficina.addEventListener("change", ubicarOficinaSeleccionada);
    }

    if(selTipoATM) selTipoATM.addEventListener("change", ()=>scheduleReload(true));
    if(selUbicATM) selUbicATM.addEventListener("change", ()=>scheduleReload(true));

    if(selSegmento) selSegmento.addEventListener("change", () => {
      scheduleReload(true);
    });

    // ======================================================
    // ✅ HEATMAPS (INDEPENDIENTES) — FIX 2DO CLICK
    // ======================================================
    function turnOffHeatTRX(){
      try{ if(map.hasLayer(heat)) map.removeLayer(heat); }catch(e){}
    }


    // ======================================================
// ✅ LISTENERS ÚNICOS — Heatmap Clientes
// ======================================================
let _heatClientesListenersReady = false;

function initHeatClientesListeners(){
  if(_heatClientesListenersReady) return;
  _heatClientesListenersReady = true;

  if(chkHeatClientes){
    chkHeatClientes.addEventListener("change", () => {
      if(chkHeatClientes.checked){
        // al ACTIVAR: fuerza recarga SIEMPRE
        _scheduleHeatClientes(true);
      }else{
        clearHeatClientes();
      }
    });
  }

  // si mueves o cambias zoom: recarga (no force)
  map.on("moveend zoomend", () => {
    if(chkHeatClientes && chkHeatClientes.checked){
      _scheduleHeatClientes(false);
    }
  });
}

// LLAMAR UNA VEZ
initHeatClientesListeners();

    if(chkHeatCantClientes){
      chkHeatCantClientes.addEventListener("change", () => {
        if(chkHeatCantClientes.checked) fetchHeatCantClientes(true);
        else clearHeatCantClientes();
      });
    }

    // df_comercios puntos
    if(chkComerciosPts){
      chkComerciosPts.addEventListener("change", () => {
        syncDfComerciosPanelVisibility();
        if(chkComerciosPts.checked) fetchComerciosPts(true);
        else clearComerciosPts();
        refreshInfoCount();
      });
    }

    // zoom/move: refresca solo heatmaps (NO df_comercios)

    map.on("zoomend", () => {
  if(chkHeatClientes && chkHeatClientes.checked) fetchHeatClientes(false);
  if(chkHeatCantClientes && chkHeatCantClientes.checked) fetchHeatCantClientes(false);
});

map.on("moveend", () => {
  // tu endpoint usa zoom+filtros (no bbox), así que no hace falta refetch en pan.
  // esto solo asegura que si el layer se removió por cualquier motivo, se re-pinte.
  if(chkHeatCantClientes && chkHeatCantClientes.checked) fetchHeatCantClientes(false);
});

    // recomendaciones
    if(chkReco) chkReco.addEventListener("change", async () => {
      if(chkReco.checked){
        if(!recoLoaded) await cargarRecomendaciones();
        if(!map.hasLayer(markersReco)) markersReco.addTo(map);
      }else{
        if(map.hasLayer(markersReco)) map.removeLayer(markersReco);
      }
    });

    if(chkZonaRural) chkZonaRural.addEventListener("change", fetchZonasBorders);
    if(chkZonaUrbana) chkZonaUrbana.addEventListener("change", fetchZonasBorders);

    if(chkNodos) chkNodos.addEventListener("change", async () => {
      syncComercialVisibility();
      await fetchNodos(true);
      refreshInfoCount();
    });

    if(chkATMs) chkATMs.addEventListener("change", () => { syncIntegralPanelsVisibility(); scheduleReload(true); });
    if(chkOficinas) chkOficinas.addEventListener("change", () => { syncIntegralPanelsVisibility(); scheduleReload(true); });
    if(chkAgentes) chkAgentes.addEventListener("change", () => { syncIntegralPanelsVisibility(); scheduleReload(true); });

    // INIT
    updateProvincias();
    syncSinglePanelsVisibility();
    syncComercialVisibility();
    syncDfComerciosPanelVisibility();

    refreshInfoCount();
//
    // init capas dependientes de checkbox
    fetchHeatClientes(true);
    syncHeatCantClientes(true);
    fetchComerciosPts(true);

    if(TIPO_MAPA === "integral") fetchIntegral();
    else fetchPoints();
  </script>
</body>
</html>
"""
# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host=host, port=port, debug=debug)