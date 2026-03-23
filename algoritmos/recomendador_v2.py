# recomendador_grid.py
# Genera recomendaciones.csv (mismo contrato que el checkbox actual)
# CALIBRADO v2 + DEDUP JERÁRQUICO (Opción C)
#
# - Grid 500m
# - Score Real: 70% clientes + 30% compras
# - Potencial ponderado por TIPO (NODOS)
# - Percentiles más exigentes: Real P90, Potencial P80
# - Filtros absolutos: clientes >= 300 (cualquier recomendación), AGENTE requiere clientes >= 400
# - Deduplicación jerárquica por radio:
#     OFICINA (1500m) opaca ATM/AGENTE
#     ATM (1000m) opaca AGENTE
#     AGENTE (700m) entre sí
#
# Nota:
# - pct_digital/ingreso_prom/edad_prom se setean a 1 por defecto (evitar errores del front).

import math
import json
from pathlib import Path

import numpy as np
import pandas as pd


# =========================
# Config
# =========================
GRID_SIZE_M = 500  # 500 metros

WEIGHT_CLIENTES = 0.7
WEIGHT_COMPRAS = 0.3

# Percentiles (más exigentes)
PCTL_REAL_ALTO = 0.90   # antes 0.80
PCTL_REAL_MEDIO = 0.50  # medio es >=P50 y <=P90
PCTL_POT_ALTO = 0.80    # antes 0.70

# Umbrales absolutos
MIN_CLIENTES_ANY_RECO = 1500     # filtro global: si <300, no se recomienda nada en esa celda
MIN_CLIENTES_AGENTE = 1500       # AGENTE exige al menos 400 clientes
ATM_CLIENTES_UMBRAL = 7500      # ATM si clientes > 1000

# Nóminas
PCTL_OPORT_ALTA = 0.85
CLIENTES_OFICINA_UMBRAL = 15000
ATM_TO_OFICINA_UMBRAL = 15000
OPORTUNIDAD_MIN_OFICINA_NOMINAS = 2000
MIN_CLIENTES_NOMINAS = 300

# Deduplicación jerárquica (Opción C)
R_OFFICE_M = 1500
R_ATM_M = 1000
R_AGENT_M = 700

# Red actual
FILE_RED_OFICINAS = r"data/OFICINAS.xlsx"
FILE_RED_ATM = r"data/Mapa Geoespacial ATM (1) (1).xlsx"
FILE_RED_AGENTES = r"data/AGENTES.xlsx"

# Reglas contra red actual
R_OFFICE_EXIST_M = 500
R_ATM_EXIST_M = 300
R_AGENT_EXIST_M = 100

ATM_TRANS_REUBICAR_MAX = 3000
ATM_TRANS_ELIMINAR_MAX = 6000
AGENTE_TRANS_ELIMINAR = 700

FILE_EMPRESAS = r"data/BASE_EMPRESAS_NOMINAS.xlsx"
FILE_NODOS = r"data/NODOS1.xlsx"
FILE_COMERCIOS = r"data/dfcomercios_top_final1.xlsx"

FILE_MARKET_SHARE = r"data/resultado_unificado_limpio.csv"
OUT_MARKET_SHARE_DETALLE = r"data/market_share_detalle.csv"

# Output (mismo nombre esperado por el checkbox)
OUT_RECOMENDACIONES = "recomendaciones.csv"


# =========================
# Utilidades
# =========================
def _to_float_series(s: pd.Series) -> pd.Series:
    """Convierte una serie a float robustamente (maneja coma decimal)."""
    if s.dtype == "object":
        s = s.astype(str).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _earth_deg_steps(lat_ref: float, grid_m: float) -> tuple[float, float]:
    """
    Aproximación equirectangular para convertir metros a grados.
    - 1 deg lat ~ 111_320 m
    - 1 deg lon ~ 111_320 * cos(lat)
    """
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = 111_320.0 * math.cos(math.radians(lat_ref))
    meters_per_deg_lon = max(meters_per_deg_lon, 1e-6)  # robustez
    return grid_m / meters_per_deg_lat, grid_m / meters_per_deg_lon


def _assign_cells(
    df: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    lat_min: float,
    lon_min: float,
    lat_step: float,
    lon_step: float,
) -> pd.DataFrame:
    """Asigna (grid_x, grid_y, cell_id) a cada punto."""
    df = df.copy()
    gx = np.floor((df[lon_col] - lon_min) / lon_step).astype("Int64")
    gy = np.floor((df[lat_col] - lat_min) / lat_step).astype("Int64")
    df["grid_x"] = gx
    df["grid_y"] = gy
    df["cell_id"] = df["grid_x"].astype(str) + "_" + df["grid_y"].astype(str)
    return df


def _center_of_cell(
    grid_x: int,
    grid_y: int,
    lat_min: float,
    lon_min: float,
    lat_step: float,
    lon_step: float,
) -> tuple[float, float]:
    """Centro (lat, lon) de una celda."""
    lat_c = lat_min + (grid_y + 0.5) * lat_step
    lon_c = lon_min + (grid_x + 0.5) * lon_step
    return float(lat_c), float(lon_c)


def _percentile_rank(s: pd.Series) -> pd.Series:
    """Percentile rank robusto en [0,1]. Si todo es constante, retorna 0."""
    s = s.fillna(0.0)
    if s.nunique(dropna=False) <= 1:
        return pd.Series(np.zeros(len(s)), index=s.index, dtype=float)
    return s.rank(pct=True, method="average").astype(float)


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Distancia haversine en metros."""
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _select_spaced(df: pd.DataFrame, radius_m: float, sort_cols: list[str]) -> pd.DataFrame:
    """
    Selecciona puntos separados por radius_m, quedándose con los "mejores" según sort_cols (desc).
    sort_cols ejemplo: ["clientes_celda", "score_real"].
    """
    if df.empty:
        return df

    dfx = df.sort_values(by=sort_cols, ascending=[False] * len(sort_cols)).reset_index(drop=True)
    keep_rows = []
    kept_points = []  # lista de (lat, lon)

    for _, row in dfx.iterrows():
        lat, lon = float(row["lat"]), float(row["lon"])
        ok = True
        for (klat, klon) in kept_points:
            if _haversine_m(lat, lon, klat, klon) < radius_m:
                ok = False
                break
        if ok:
            keep_rows.append(row)
            kept_points.append((lat, lon))

    return pd.DataFrame(keep_rows)


def _remove_near(df: pd.DataFrame, blockers: pd.DataFrame, radius_m: float) -> pd.DataFrame:
    """Elimina filas de df que estén a <radius_m de cualquier punto en blockers."""
    if df.empty or blockers.empty:
        return df

    bl = blockers[["lat", "lon"]].to_numpy()
    out_rows = []

    for _, row in df.iterrows():
        lat, lon = float(row["lat"]), float(row["lon"])
        too_close = False
        for (blat, blon) in bl:
            if _haversine_m(lat, lon, float(blat), float(blon)) < radius_m:
                too_close = True
                break
        if not too_close:
            out_rows.append(row)

    return pd.DataFrame(out_rows)


# =========================
# Pesos de NODOS (TIPO)
# =========================
def build_tipo_weights(unique_tipos_norm: list[str]) -> dict[str, int]:
    """
    Pesos acordados:
    - 3: alto flujo + permanencia / atracción económica
    - 2: flujo alto moderado
    - 1: resto
    """
    w = {t: 1 for t in unique_tipos_norm}

    peso_3 = {
        "CENTRO COMERCIAL",
        "UNIVERSIDAD",
        "INSTITUTO",
        "MERCADO",
        "TERMINAL",
        "ESTADIO",
        "AEROPUERTO",
    }
    peso_2 = {
        "HOSPITAL",
        "CLINICA",
        "SUPERMERCADO",
        "MUNICIPALIDAD",
        "COLEGIO",
    }
    peso_1 = {
        "PLAZA",
        "PARQUE",
        "IGLESIA",
        "POSTA",
    }

    for t in unique_tipos_norm:
        if t in peso_3:
            w[t] = 3
        elif t in peso_2:
            w[t] = 2
        elif t in peso_1:
            w[t] = 1
        else:
            w[t] = 1

    return w

def _nearby_avg(df_red: pd.DataFrame, lat: float, lon: float, radius_m: float, value_col: str) -> tuple[int, float]:
    """
    Devuelve (cantidad_puntos_en_radio, promedio_columna) para todos los puntos de df_red
    dentro del radio. Si no hay puntos, retorna (0, 0.0).
    """
    vals = []
    for _, r in df_red.iterrows():
        d = _haversine_m(lat, lon, float(r["lat"]), float(r["lon"]))
        if d <= radius_m:
            vals.append(float(r[value_col]))
    if not vals:
        return 0, 0.0
    return len(vals), float(np.mean(vals))


def _exists_near(df_red: pd.DataFrame, lat: float, lon: float, radius_m: float) -> bool:
    """True si existe al menos un punto dentro del radio."""
    for _, r in df_red.iterrows():
        d = _haversine_m(lat, lon, float(r["lat"]), float(r["lon"]))
        if d <= radius_m:
            return True
    return False

def _build_market_share_tables(df_ms: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Construye:
    1) Tabla resumida por distrito para merge con recomendaciones
    2) Tabla detalle por departamento/provincia/distrito/banco para exportar a CSV

    Reglas:
    - Solo bancos: BBVA, BCP, IBK, SCO
    - Solo productos: PRODUCTO PN, PRODUCTO PYME
    - General = PN + PYME
    """

    df = df_ms.copy()

    # Normalizar nombres territoriales
    rename_map = {
        "adr_department_name": "departamento",
        "adr_province_name": "provincia",
        "adr_district_name": "distrito",
    }
    df = df.rename(columns=rename_map)

    df = df.dropna(subset=["departamento", "provincia", "distrito"]).copy()

    for c in ["departamento", "provincia", "distrito"]:
        df[c] = df[c].astype(str).str.strip().str.upper()

    # Convertir numéricos
    for c in df.columns:
        if c not in ["departamento", "provincia", "distrito", "index"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    bancos_validos = ["BBVA", "BCP", "IBK", "SCO"]

    detalle_rows = []

    for banco in bancos_validos:
        col_pn = f"PRODUCTO PN_{banco}"
        col_pyme = f"PRODUCTO PYME_{banco}"

        if col_pn not in df.columns:
            df[col_pn] = 0
        if col_pyme not in df.columns:
            df[col_pyme] = 0

        tmp = df[["departamento", "provincia", "distrito"]].copy()
        tmp["banco"] = banco
        tmp["monto_pn"] = df[col_pn]
        tmp["monto_pyme"] = df[col_pyme]
        tmp["monto_general"] = tmp["monto_pn"] + tmp["monto_pyme"]

        detalle_rows.append(tmp)

    ms_detalle = pd.concat(detalle_rows, ignore_index=True)

    # Consolidar por si hubiera duplicados
    ms_detalle = (
        ms_detalle.groupby(["departamento", "provincia", "distrito", "banco"], as_index=False)
        .agg(
            monto_pn=("monto_pn", "sum"),
            monto_pyme=("monto_pyme", "sum"),
            monto_general=("monto_general", "sum"),
        )
    )

    # Totales por territorio para calcular participación
    totales = (
        ms_detalle.groupby(["departamento", "provincia", "distrito"], as_index=False)
        .agg(
            total_pn=("monto_pn", "sum"),
            total_pyme=("monto_pyme", "sum"),
            total_general=("monto_general", "sum"),
        )
    )

    ms_detalle = ms_detalle.merge(
        totales,
        on=["departamento", "provincia", "distrito"],
        how="left"
    )

    ms_detalle["%Participacion_PN"] = np.where(
        ms_detalle["total_pn"] > 0,
        ms_detalle["monto_pn"] / ms_detalle["total_pn"],
        np.nan
    )

    ms_detalle["%Participacion_PYME"] = np.where(
        ms_detalle["total_pyme"] > 0,
        ms_detalle["monto_pyme"] / ms_detalle["total_pyme"],
        np.nan
    )

    ms_detalle["%Participacion_General"] = np.where(
        ms_detalle["total_general"] > 0,
        ms_detalle["monto_general"] / ms_detalle["total_general"],
        np.nan
    )

    # Tabla resumida solo para BBVA
    ms_bbva = ms_detalle[ms_detalle["banco"] == "BBVA"].copy()

    ms_resumen = ms_bbva[[
        "departamento",
        "provincia",
        "distrito",
        "%Participacion_General",
        "%Participacion_PN",
        "%Participacion_PYME",
    ]].copy()

    ms_resumen = ms_resumen.rename(columns={
        "%Participacion_General": "%Participacion_General_BBVA",
        "%Participacion_PN": "%Participacion_PN_BBVA",
        "%Participacion_PYME": "%Participacion_PYME_BBVA",
    })

    # Rankings nacionales: 1 = menor participación = mayor oportunidad
    ms_resumen["Ranking_General"] = ms_resumen["%Participacion_General_BBVA"].rank(
        method="dense", ascending=True
    ).astype("Int64")

    ms_resumen["Ranking_PN"] = ms_resumen["%Participacion_PN_BBVA"].rank(
        method="dense", ascending=True
    ).astype("Int64")

    ms_resumen["Ranking_PYME"] = ms_resumen["%Participacion_PYME_BBVA"].rank(
        method="dense", ascending=True
    ).astype("Int64")

    return ms_resumen, ms_detalle


# =========================
# Main
# =========================
def main():
    base_dir = Path(".")
    nodos_path = base_dir / FILE_NODOS
    comercios_path = base_dir / FILE_COMERCIOS
    out_path = base_dir / OUT_RECOMENDACIONES
    market_share_out_path = base_dir / OUT_MARKET_SHARE_DETALLE

    market_share_path = base_dir / FILE_MARKET_SHARE

    oficinas_red_path = base_dir / FILE_RED_OFICINAS
    atm_red_path = base_dir / FILE_RED_ATM
    agentes_red_path = base_dir / FILE_RED_AGENTES

    empresas = pd.read_excel(base_dir / FILE_EMPRESAS)

    market_share = pd.read_csv(market_share_path)

    # Coordenadas
    empresas["lat"] = _to_float_series(empresas["latitud"])
    empresas["lon"] = _to_float_series(empresas["longitud"])

    # Limpieza
    empresas = empresas.dropna(subset=["lat", "lon"]).copy()

    # Variable clave
    empresas["oportunidad"] = (
        pd.to_numeric(empresas["trabajadores"], errors="coerce").fillna(0)
        - pd.to_numeric(empresas["STOCK"], errors="coerce").fillna(0)
    )

    # Evitar negativos
    empresas["oportunidad"] = empresas["oportunidad"].clip(lower=0)

    # ---------
    # Cargar data
    # ---------
    nodos = pd.read_excel(nodos_path)
    comercios = pd.read_excel(comercios_path)

    oficinas_red = pd.read_excel(oficinas_red_path)
    atm_red = pd.read_excel(atm_red_path)
    agentes_red = pd.read_excel(agentes_red_path)

    # Normalizar columnas lat/lon
    nodos["lat"] = _to_float_series(nodos["LATITUD"])
    nodos["lon"] = _to_float_series(nodos["LONGITUD"])
    comercios["lat"] = _to_float_series(comercios["latitud"])
    comercios["lon"] = _to_float_series(comercios["longitud"])

    oficinas_red["lat"] = _to_float_series(oficinas_red["LATITUD"])
    oficinas_red["lon"] = _to_float_series(oficinas_red["LONGITUD"])

    atm_red["lat"] = _to_float_series(atm_red["LATITUD"])
    atm_red["lon"] = _to_float_series(atm_red["LONGITUD"])
    atm_red["Promedio 2025"] = pd.to_numeric(atm_red["Promedio 2025"], errors="coerce").fillna(0)
    atm_red["UBICACIÓN(INTERNA)"] = atm_red["UBICACIÓN(INTERNA)"].astype(str).str.strip().str.upper()

    agentes_red["lat"] = _to_float_series(agentes_red["LATITUD"])
    agentes_red["lon"] = _to_float_series(agentes_red["LONGITUD"])
    agentes_red["PROMEDIO"] = pd.to_numeric(agentes_red["PROMEDIO"], errors="coerce").fillna(0)

    # Limpiar
    nodos = nodos.dropna(subset=["lat", "lon"]).copy()
    comercios = comercios.dropna(subset=["lat", "lon"]).copy()

    oficinas_red = oficinas_red.dropna(subset=["lat", "lon"]).copy()
    atm_red = atm_red.dropna(subset=["lat", "lon"]).copy()
    agentes_red = agentes_red.dropna(subset=["lat", "lon"]).copy()

    # =========================
    # Market share por distrito
    # =========================
    market_share_tbl, market_share_detalle = _build_market_share_tables(market_share)

    # Métricas real
    comercios["cant_clientes"] = pd.to_numeric(comercios["cant_clientes"], errors="coerce").fillna(0).astype(float)
    comercios["cant_compras"] = pd.to_numeric(comercios["cant_compras"], errors="coerce").fillna(0).astype(float)

    # ---------
    # Definir grid (500m)
    # ---------
    all_lat = pd.concat([nodos["lat"], comercios["lat"], empresas["lat"]], ignore_index=True)
    all_lon = pd.concat([nodos["lon"], comercios["lon"], empresas["lon"]], ignore_index=True)

    lat_ref = float(all_lat.mean())
    lat_step, lon_step = _earth_deg_steps(lat_ref, GRID_SIZE_M)

    lat_min = float(all_lat.min())
    lon_min = float(all_lon.min())

    # Asignar celdas
    nodos = _assign_cells(nodos, "lat", "lon", lat_min, lon_min, lat_step, lon_step)
    comercios = _assign_cells(comercios, "lat", "lon", lat_min, lon_min, lat_step, lon_step)
    empresas = _assign_cells(empresas, "lat", "lon", lat_min, lon_min, lat_step, lon_step)

    # ---------
    # Potencial (NODOS ponderados)
    # ---------
    nodos["TIPO_NORM"] = nodos["TIPO"].astype(str).str.strip().str.upper()
    unique_tipos_norm = sorted(nodos["TIPO_NORM"].dropna().unique().tolist())
    tipo_weights = build_tipo_weights(unique_tipos_norm)
    nodos["peso_tipo"] = nodos["TIPO_NORM"].map(lambda x: tipo_weights.get(x, 1)).astype(int)

    nodos_agg = (
        nodos.groupby("cell_id", as_index=False)
        .agg(
            potencial_raw=("peso_tipo", "sum"),
            n_nodos=("peso_tipo", "size"),
            departamento=("DEPARTAMENTO", lambda x: x.mode().iloc[0] if len(x.mode()) else None),
            provincia=("PROVINCIA", lambda x: x.mode().iloc[0] if len(x.mode()) else None),
            distrito=("DISTRITO", lambda x: x.mode().iloc[0] if len(x.mode()) else None),
        )
    )

    # ---------
    # Real (Comercios Top)
    # ---------
    comercios_agg = (
        comercios.groupby("cell_id", as_index=False)
        .agg(
            clientes_celda=("cant_clientes", "sum"),
            compras_celda=("cant_compras", "sum"),
            n_comercios=("commerce_id", "nunique"),
            departamento=("departamento", lambda x: x.mode().iloc[0] if len(x.mode()) else None),
            provincia=("provincia", lambda x: x.mode().iloc[0] if len(x.mode()) else None),
            distrito=("distrito", lambda x: x.mode().iloc[0] if len(x.mode()) else None),
        )
    )

    empresas_agg = (
        empresas.groupby("cell_id", as_index=False)
        .agg(
            oportunidad_celda=("oportunidad", "sum"),
            empresas_celda=("oportunidad", "size"),
        )
    )

    # ---------
    # Base grid: unión de celdas con data
    # ---------
    base = pd.merge(comercios_agg, nodos_agg, on="cell_id", how="outer", suffixes=("", "_nodos"))

    base = pd.merge(base, empresas_agg, on="cell_id", how="left")

    base["oportunidad_celda"] = base["oportunidad_celda"].fillna(0)
    base["empresas_celda"] = base["empresas_celda"].fillna(0).astype(int)

    # Resolver ubicación admin: preferir comercios, si es NaN usar nodos
    for col in ["departamento", "provincia", "distrito"]:
        col_n = f"{col}_nodos"
        base[col] = base[col].where(base[col].notna(), base[col_n])
        if col_n in base.columns:
            base.drop(columns=[col_n], inplace=True)

    # Rellenar métricas faltantes
    base["clientes_celda"] = base["clientes_celda"].fillna(0.0)
    base["compras_celda"] = base["compras_celda"].fillna(0.0)
    base["n_comercios"] = base["n_comercios"].fillna(0).astype(int)
    base["potencial_raw"] = base["potencial_raw"].fillna(0).astype(int)
    base["n_nodos"] = base["n_nodos"].fillna(0).astype(int)

    # Extraer grid_x/grid_y desde cell_id para el centro de celda
    gxgy = base["cell_id"].str.split("_", expand=True)
    base["grid_x"] = pd.to_numeric(gxgy[0], errors="coerce").astype(int)
    base["grid_y"] = pd.to_numeric(gxgy[1], errors="coerce").astype(int)

    centers = base.apply(
        lambda r: _center_of_cell(int(r["grid_x"]), int(r["grid_y"]), lat_min, lon_min, lat_step, lon_step),
        axis=1,
        result_type="expand",
    )
    base["lat"] = centers[0].astype(float)
    base["lon"] = centers[1].astype(float)

    # ---------
    # Normalización (percentile rank)
    # ---------
    base["clientes_norm"] = _percentile_rank(base["clientes_celda"])
    base["compras_norm"] = _percentile_rank(base["compras_celda"])
    base["potencial_norm"] = _percentile_rank(base["potencial_raw"])
    base["oportunidad_norm"] = _percentile_rank(base["oportunidad_celda"])

    # Scores
    base["score_real"] = WEIGHT_CLIENTES * base["clientes_norm"] + WEIGHT_COMPRAS * base["compras_norm"]
    base["score_pot"] = base["potencial_norm"]

    # Umbrales por percentiles (más exigentes)
    thr_real_alto = float(base["score_real"].quantile(PCTL_REAL_ALTO))
    thr_real_medio = float(base["score_real"].quantile(PCTL_REAL_MEDIO))
    thr_pot_alto = float(base["score_pot"].quantile(PCTL_POT_ALTO))
    thr_oport_alta = float(base["oportunidad_norm"].quantile(PCTL_OPORT_ALTA))

    # Flags
    base["real_alto"] = base["score_real"] > thr_real_alto
    base["real_medio"] = (base["score_real"] >= thr_real_medio) & (base["score_real"] <= thr_real_alto)
    base["real_bajo"] = base["score_real"] < thr_real_medio
    base["pot_alto"] = base["score_pot"] > thr_pot_alto
    base["pot_bajo"] = ~base["pot_alto"]

    # ---------
    # Reglas de decisión (calibradas)
    # ---------
    
    def decidir_canal(row) -> str | None:
        clientes = float(row["clientes_celda"])
        score_real = float(row["score_real"])
        score_pot = float(row["score_pot"])
        oport_norm = float(row["oportunidad_norm"])

        if clientes < MIN_CLIENTES_ANY_RECO:
            if (
                oport_norm >= thr_oport_alta
                and row["oportunidad_celda"] > OPORTUNIDAD_MIN_OFICINA_NOMINAS
            ):
                return "oficina_nominas"
            return None

        real_alto = score_real > thr_real_alto
        real_medio = (score_real >= thr_real_medio) and (score_real <= thr_real_alto)
        pot_alto = score_pot > thr_pot_alto
        oport_alta = oport_norm >= thr_oport_alta

        # 1) OFICINA: mucho más exigente
        if real_alto and clientes >= CLIENTES_OFICINA_UMBRAL and pot_alto:
            return "oficina"

        if (
            oport_alta
            and not real_alto
            and row["oportunidad_celda"] > OPORTUNIDAD_MIN_OFICINA_NOMINAS
            and clientes >= MIN_CLIENTES_NOMINAS
        ):
            return "oficina_nominas"

        # 3) ATM: real alto, potencial bajo, volumen alto
        if real_alto and (not pot_alto) and clientes > ATM_CLIENTES_UMBRAL:
            return "atm"

        # 4) AGENTE: demanda media o alta con menor volumen
        if (real_medio and clientes >= MIN_CLIENTES_AGENTE) or (
            real_alto and (not pot_alto) and clientes <= ATM_CLIENTES_UMBRAL and clientes >= MIN_CLIENTES_AGENTE
        ):
            return "agente"

        return None


    base["canal"] = base.apply(decidir_canal, axis=1)


    # Override por volumen: si salió AGENTE pero hay muchos clientes, subir a ATM
    # Override 1: AGENTE -> ATM si el volumen es muy alto
    UMBRAL_ATM_OVERRIDE = 7500
    mask_upgrade_atm = (base["canal"] == "agente") & (base["clientes_celda"] > UMBRAL_ATM_OVERRIDE)
    base.loc[mask_upgrade_atm, "canal"] = "atm"

    # Override 2: ATM -> OFICINA solo si además cumple criterio de oficina fuerte
    mask_upgrade_oficina = (
        (base["canal"] == "atm")
        & (base["clientes_celda"] > ATM_TO_OFICINA_UMBRAL)
        & (base["score_real"] > thr_real_alto)
        & (base["score_pot"] > thr_pot_alto)
    )
    base.loc[mask_upgrade_oficina, "canal"] = "oficina"

    # Override 3: si hay oportunidad muy alta y no es oficina tradicional, marcar OFICINA-NOMINAS
    mask_nominas = (
        (base["oportunidad_norm"] >= thr_oport_alta)
        & (base["oportunidad_celda"] > OPORTUNIDAD_MIN_OFICINA_NOMINAS)
        & (base["clientes_celda"] >= MIN_CLIENTES_NOMINAS)
        & ((base["canal"].isin(["agente", "atm"])) | (base["canal"].isna()))
        & ~(
            (base["score_real"] > thr_real_alto)
            & (base["clientes_celda"] >= CLIENTES_OFICINA_UMBRAL)
            & (base["score_pot"] > thr_pot_alto)
        )
    )

    base.loc[mask_nominas, "canal"] = "oficina_nominas"

    # Filtrar solo recomendaciones (candidatos)
    rec = base[base["canal"].notna()].copy()

    # =========================
    # Deduplicación jerárquica (Opción C)
    # =========================
    rec_ofi = rec[rec["canal"] == "oficina"].copy()
    rec_ofi_nom = rec[rec["canal"] == "oficina_nominas"].copy()
    rec_atm = rec[rec["canal"] == "atm"].copy()
    rec_age = rec[rec["canal"] == "agente"].copy()

    # 1) OFICINAS tradicionales
    sel_ofi = _select_spaced(rec_ofi, R_OFFICE_M, sort_cols=["clientes_celda", "score_real"])

    # 2) OFICINAS-NOMINAS
    rec_ofi_nom2 = _remove_near(rec_ofi_nom, sel_ofi, R_OFFICE_M)
    sel_ofi_nom = _select_spaced(rec_ofi_nom2, R_OFFICE_M, sort_cols=["oportunidad_celda", "clientes_celda"])

    # 3) ATMs
    blockers_atm = pd.concat([sel_ofi, sel_ofi_nom], ignore_index=True)
    rec_atm2 = _remove_near(rec_atm, blockers_atm, R_ATM_M)
    sel_atm = _select_spaced(rec_atm2, R_ATM_M, sort_cols=["clientes_celda", "score_real"])

    # 4) AGENTES
    blockers_age = pd.concat([sel_ofi, sel_ofi_nom, sel_atm], ignore_index=True)
    rec_age2 = _remove_near(rec_age, blockers_age, R_AGENT_M)
    sel_age = _select_spaced(rec_age2, R_AGENT_M, sort_cols=["clientes_celda", "score_real"])

    # Resultado final
    rec = pd.concat([sel_ofi, sel_ofi_nom, sel_atm, sel_age], ignore_index=True)

    # =========================
    # Filtro contra red actual
    # =========================
    rec["accion_sugerida"] = "nuevo_punto"
    rec["red_actual_cercana"] = 0
    rec["promedio_transacciones_red"] = 0.0

    keep_rows = []

    for _, row in rec.iterrows():
        canal = str(row["canal"]).strip().lower()
        lat = float(row["lat"])
        lon = float(row["lon"])

        # 1) OFICINA-NOMINAS no se elimina nunca
        if canal == "oficina_nominas":
            keep_rows.append(row)
            continue

        # 2) OFICINA: si ya hay oficina a 500m, eliminar
        if canal == "oficina":
            existe_ofi = _exists_near(oficinas_red, lat, lon, R_OFFICE_EXIST_M)
            if not existe_ofi:
                keep_rows.append(row)
            continue

        # 3) ATM: mirar solo ATMs ISLA dentro de 300m
        if canal == "atm":
            atm_isla = atm_red[atm_red["UBICACIÓN(INTERNA)"] == "ISLA"].copy()
            n_atm, prom_atm = _nearby_avg(atm_isla, lat, lon, R_ATM_EXIST_M, "Promedio 2025")

            row["red_actual_cercana"] = n_atm
            row["promedio_transacciones_red"] = prom_atm

            if n_atm == 0:
                keep_rows.append(row)
                continue

            # > 6000: se mantiene recomendación
            if prom_atm > ATM_TRANS_ELIMINAR_MAX:
                keep_rows.append(row)
                continue

            # entre 3000 y 6000: se elimina
            if ATM_TRANS_REUBICAR_MAX <= prom_atm <= ATM_TRANS_ELIMINAR_MAX:
                continue

            # < 3000: se mantiene pero como reubicación
            if prom_atm < ATM_TRANS_REUBICAR_MAX:
                row["accion_sugerida"] = "reubicar"
                keep_rows.append(row)
                continue

        # 4) AGENTE: si hay agentes dentro de 100m con promedio > 700, eliminar
        # 4) AGENTE: evaluar promedio de agentes dentro de 100m
        if canal == "agente":
            n_age, prom_age = _nearby_avg(agentes_red, lat, lon, R_AGENT_EXIST_M, "PROMEDIO")

            row["red_actual_cercana"] = n_age
            row["promedio_transacciones_red"] = prom_age

            # Si no hay agentes cerca, se mantiene como nuevo punto
            if n_age == 0:
                row["accion_sugerida"] = "nuevo_punto"
                keep_rows.append(row)
                continue

            # Si el promedio es mayor a 700, se elimina
            if prom_age > 700:
                continue

            # Si el promedio es menor a 300, se mantiene como nuevo punto
            if prom_age < 300:
                row["accion_sugerida"] = "nuevo_punto"
                keep_rows.append(row)
                continue

            # Si está entre 300 y 700, se mantiene la recomendación
            row["accion_sugerida"] = "mantener_recomendacion"
            keep_rows.append(row)
            continue

        # Cualquier otro caso
        keep_rows.append(row)

    rec = pd.DataFrame(keep_rows).reset_index(drop=True)

    # ---------
    # Diagnóstico (explicabilidad)
    # ---------

    def _fmt_int(n) -> str:
        try:
            return f"{int(round(float(n))):,}".replace(",", ".")
        except Exception:
            return str(n)

    def _nodos_resumen(row) -> str:
        # Si no hay nodos
        n = int(row.get("n_nodos", 0) or 0)
        if n <= 0:
            return "No presenta polos estratégicos de alta afluencia."

        # Si hay nodos, mencionar los más relevantes por TIPO en esa celda.
        # Como en 'rec' ya estamos a nivel celda, solo tenemos conteo total (n_nodos).
        # Entonces lo dejamos genérico pero “negocio friendly”.
        # Si luego quieres TOP tipos por celda, lo podemos enriquecer agregando esa agregación.
        if n == 1:
            return "Presencia de 1 polo estratégico de afluencia (p. ej., universidad, centro comercial, hospital)."
        if 2 <= n <= 3:
            return "Presencia de polos estratégicos de afluencia (p. ej., universidades, centros comerciales, hospitales)."
        return "Alta presencia de polos estratégicos de afluencia (universidades, centros comerciales, mercados, etc.)."

    def _nivel_actividad(row) -> str:
        # Traducción simple de score_real a texto (sin percentiles)
        sr = float(row.get("score_real", 0) or 0)
        if sr >= 0.90:
            return "muy alta concentración financiera"
        if sr >= 0.75:
            return "alta actividad transaccional"
        if sr >= 0.55:
            return "actividad comercial moderada"
        return "baja actividad actual"

    def diagnostico(row) -> list[str]:
        canal = str(row.get("canal", "")).upper()

        oport = _fmt_int(row.get("oportunidad_celda", 0))
        linea_extra = f"Oportunidad estimada de captación: {oport} trabajadores no captados."

        clientes = _fmt_int(row.get("clientes_celda", 0))

        linea1 = f"Zona con {clientes} clientes activos y {_nivel_actividad(row)}."
        linea2 = _nodos_resumen(row)

        accion = str(row.get("accion_sugerida", "nuevo_punto")).lower()

        if canal == "OFICINA":
            linea3 = "Se recomienda OFICINA para brindar atención integral y mayor capacidad operativa."
        elif canal == "OFICINA_NOMINAS":
            linea3 = "Se recomienda OFICINA-NOMINAS por el alto potencial de captación de trabajadores en la zona."
        elif canal == "ATM":
            if accion == "reubicar":
                linea3 = "Se recomienda REUBICAR ATM, no abrir un punto nuevo, por bajo desempeño de la red actual cercana."
            else:
                linea3 = "Se recomienda ATM para absorber el volumen de operaciones y mejorar capacidad."
        elif canal == "AGENTE":
            linea3 = "Se recomienda AGENTE como solución de cobertura eficiente para la demanda local."
        else:
            linea3 = "Recomendación generada por criterios de demanda y potencial."

        return [linea1, linea_extra, linea3]


    rec["diagnostico"] = rec.apply(diagnostico, axis=1).apply(lambda x: json.dumps(x, ensure_ascii=False))

    # ---------
    # Output (mismo contrato del checkbox)
    # ---------
    rec = rec.sort_values(by=["canal", "score_real", "score_pot"], ascending=[True, False, False]).reset_index(drop=True)
    rec["cluster"] = np.arange(len(rec), dtype=int)

    def perfil_top(row) -> str:
        if row["canal"] == "oficina":
            return "Mixto"
        if row["canal"] == "oficina_nominas":
            return "Nominas"
        if row["canal"] == "atm" and row.get("accion_sugerida", "") == "reubicar":
            return "Reubicar"
        return "Real"

    out = pd.DataFrame(
        {
            "cluster": rec["cluster"].astype(int),
            "lat": rec["lat"].astype(float),
            "lon": rec["lon"].astype(float),
            "canal": rec["canal"].astype(str),
            "accion_sugerida": rec["accion_sugerida"].astype(str),
            "promedio_transacciones_red": rec["promedio_transacciones_red"].round(2),
            "clientes_afectados": rec["clientes_celda"].round(0).astype(int),
            "departamento": rec["departamento"].astype(str).str.upper(),
            "provincia": rec["provincia"].astype(str).str.upper(),
            "distrito": rec["distrito"].astype(str).str.upper(),
            "perfil_top": rec.apply(perfil_top, axis=1),
            "oportunidad_clientes_nominas": rec["oportunidad_celda"].fillna(0).round(0).astype(int),
            # Defaults para no romper el front
            "pct_digital": 1,
            "ingreso_prom": 1,
            "edad_prom": 1,
            "diagnostico": rec["diagnostico"],
        }
    )

    # =========================
    # Merge con market share
    # =========================
    for c in ["departamento", "provincia", "distrito"]:
        out[c] = out[c].astype(str).str.strip().str.upper()

    out = out.merge(
        market_share_tbl,
        on=["departamento", "provincia", "distrito"],
        how="left"
    )

    # Formato porcentual numérico (0-1)
    out["%Participacion_General_BBVA"] = out["%Participacion_General_BBVA"].round(2)
    out["%Participacion_PN_BBVA"] = out["%Participacion_PN_BBVA"].round(2)
    out["%Participacion_PYME_BBVA"] = out["%Participacion_PYME_BBVA"].round(2)

    out.to_csv(out_path, index=False, encoding="utf-8")
    market_share_detalle.to_csv(market_share_out_path, index=False, encoding="utf-8")

    print(f"✅ Market share detalle generado: {market_share_out_path.resolve()}")
    print(f"   Registros market share detalle: {len(market_share_detalle)}")
    print(f"✅ Recomendaciones generadas: {out_path.resolve()}")
    print(f"   Registros (post-dedup): {len(out)}")
    print(
        f"   Umbrales: thr_real(P90)={thr_real_alto:.3f}, thr_real(P50)={thr_real_medio:.3f}, thr_pot(P80)={thr_pot_alto:.3f}"
    )
    print(
        f"   Filtros: MIN_CLIENTES_ANY_RECO={MIN_CLIENTES_ANY_RECO}, MIN_CLIENTES_AGENTE={MIN_CLIENTES_AGENTE}, ATM_CLIENTES_UMBRAL={ATM_CLIENTES_UMBRAL}"
    )
    print(f"   Dedup radios: OFI={R_OFFICE_M}m, ATM={R_ATM_M}m, AGE={R_AGENT_M}m")
    print("   Canales:", out["canal"].value_counts().to_dict())

    # Detalle adicional por acción sugerida
    atm_nuevo = len(out[(out["canal"] == "atm") & (out["accion_sugerida"] == "nuevo_punto")])
    atm_reubicar = len(out[(out["canal"] == "atm") & (out["accion_sugerida"] == "reubicar")])

    agente_nuevo = len(out[(out["canal"] == "agente") & (out["accion_sugerida"] == "nuevo_punto")])
    agente_mantener = len(out[(out["canal"] == "agente") & (out["accion_sugerida"] == "mantener_recomendacion")])

    print("   Detalle ATM:", {"nuevo_punto": atm_nuevo, "reubicar": atm_reubicar})
    print("   Detalle AGENTE:", {"nuevo_punto": agente_nuevo, "mantener_recomendacion": agente_mantener})

    print("   Market Share merge OK:", out[["%Participacion_General_BBVA", "Ranking_General"]].notna().all(axis=1).sum(), "filas con cruce")


if __name__ == "__main__":
    main()