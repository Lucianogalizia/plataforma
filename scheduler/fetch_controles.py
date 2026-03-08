#!/usr/bin/env python3
# ==========================================================
# scheduler/fetch_controles.py
# ==========================================================

from __future__ import annotations

import io
import os
import sys
import time
from datetime import date
from typing import Optional

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================================
# Configuración
# ==========================================================

USUARIO  = os.environ.get("ZAFIRO_USUARIO",  "dgalizia")
PASSWORD = os.environ.get("ZAFIRO_PASSWORD", "")

URL_Q164 = "https://api.clear.infoil.com.ar/rest1/queries/164"
URL_Q160 = "https://patagonia.infoil.com.ar/rest1/queries/160"

Q164_DATASET = "Tests de Pozos Productores Aprobados"

FECHA_INICIO_HISTORICO = date(2024, 1, 1)

GCS_BUCKET = os.environ.get("DINAS_BUCKET",     "").strip()
GCS_PREFIX = os.environ.get("DINAS_GCS_PREFIX", "").strip().strip("/")

HISTORICO_BLOB = "controles/historico_CRUDO.csv"
MERMA_BLOB     = "controles/merma_por_pozo.csv"

LIMIT           = 1000
MAX_PAGES       = 500
REQUEST_TIMEOUT = (15, 90)

COLS_Q160 = [
    "Pozo.name",
    "Pozo>>Instalación>>Nombre",
    "Resumen de Producción Teórica de Pozo>>Cambio de estado>>Estado.name",
    "Resumen de Producción Teórica de Pozo>>Tipo de Producción>>Tipo.displayString",
    "Resumen de Producción Teórica de Pozo>>Sistema de Extracción>>Sistema de Extracción.name",
]

RENAME_Q160 = {
    "Pozo>>Instalación>>Nombre":                                                                "BATERIA",
    "Resumen de Producción Teórica de Pozo>>Cambio de estado>>Estado.name":                     "ESTADO_POZO",
    "Resumen de Producción Teórica de Pozo>>Tipo de Producción>>Tipo.displayString":            "TIPO_PRODUCCION",
    "Resumen de Producción Teórica de Pozo>>Sistema de Extracción>>Sistema de Extracción.name": "SIST_EXTRACCION",
}

# ==========================================================
# GCS
# ==========================================================

def _blob(name: str) -> str:
    return f"{GCS_PREFIX}/{name}" if GCS_PREFIX else name

def get_gcs_client():
    try:
        from google.cloud import storage
        return storage.Client()
    except Exception as e:
        print(f"  ❌ GCS client error: {e}")
        return None

def read_csv_gcs(blob_name: str) -> Optional[pd.DataFrame]:
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return None
    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(_blob(blob_name))
        if not blob.exists():
            print(f"  ℹ️  No existe {blob_name} en GCS — primera corrida")
            return None
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
        print(f"  📂 Leído de GCS: {blob_name} ({len(df)} filas)")
        return df
    except Exception as e:
        print(f"  ⚠️  Error leyendo {blob_name}: {e}")
        return None

def write_csv_gcs(df: pd.DataFrame, blob_name: str) -> bool:
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        print(f"  ❌ GCS no configurado")
        return False
    try:
        bucket  = client.bucket(GCS_BUCKET)
        blob    = bucket.blob(_blob(blob_name))
        content = df.to_csv(index=False, encoding="utf-8").encode("utf-8")
        blob.upload_from_string(content, content_type="text/csv")
        print(f"  ✅ Guardado: gs://{GCS_BUCKET}/{_blob(blob_name)} ({len(df)} filas)")
        return True
    except Exception as e:
        print(f"  ❌ Error guardando {blob_name}: {e}")
        return False

# ==========================================================
# HTTP
# ==========================================================

def build_session() -> requests.Session:
    s = requests.Session()
    s.auth = HTTPBasicAuth(USUARIO, PASSWORD)
    s.headers.update({"Accept": "application/json", "User-Agent": "PlatformaDINA/1.0"})
    retry = Retry(total=5, backoff_factor=1.0,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",), raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s

def get_json(session, url: str, params: dict | None = None):
    r = session.get(url, params=params or {}, timeout=REQUEST_TIMEOUT, allow_redirects=False)
    if r.status_code in (301, 302):
        raise RuntimeError(f"Redirección {r.status_code}")
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:600]}")
    try:
        return r.json()
    except Exception:
        raise RuntimeError(f"No se pudo parsear JSON. Body: {r.text[:600]}")

# ==========================================================
# Q164 — incremental por fecha
# ==========================================================

def _aplanar(val):
    if isinstance(val, dict):
        return val.get("displayString") or val.get("name") or str(val)
    return val

def _parse_q164(data: dict) -> tuple[list[str], list]:
    dataset = data.get("data", {}).get(Q164_DATASET)
    if not dataset:
        raise RuntimeError(f"No encontré '{Q164_DATASET}'. Claves: {list(data.get('data', {}).keys())}")
    columns = [m["column"] for m in dataset.get("metaData", [])]
    rows    = dataset.get("rows", dataset.get("data", []))
    return columns, rows

def fetch_q164_rango(session, fecha_inicio: date, fecha_fin: date) -> pd.DataFrame:
    print(f"  [Q164] Descargando {fecha_inicio} → {fecha_fin} ...")
    all_rows, columns = [], []
    offset, last_count = 0, -1

    for page in range(MAX_PAGES):
        params = {"rs": "fromTo", "start": str(fecha_inicio), "end": str(fecha_fin),
                  "offset": offset, "limit": LIMIT}
        data          = get_json(session, URL_Q164, params=params)
        columns, rows = _parse_q164(data)
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
        if len(all_rows) == last_count:
            break
        last_count = len(all_rows)
        if len(rows) < LIMIT:
            break
        time.sleep(0.1)

    print(f"  → {len(all_rows)} controles")
    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows, columns=columns if columns else None)
    return df.map(_aplanar)

# ==========================================================
# Q160 — metadata completa usando lógica robusta
# ==========================================================

def _extract_rows_q160(data) -> list:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    if "data" in data and isinstance(data["data"], dict):
        for _, block in data["data"].items():
            if isinstance(block, dict) and isinstance(block.get("data"), list):
                return block["data"]
    for k in ("data", "results", "items"):
        if isinstance(data.get(k), list):
            return data[k]
    return []

def fetch_q160(session) -> pd.DataFrame:
    """
    Descarga Q160 completo usando la misma lógica del script original:
    - Primero intenta traer todo de una sola llamada
    - Si no trae filas, intenta con paginado
    """
    print(f"  [Q160] Descargando metadata de pozos ...")

    # Intento 1: sin parámetros (como hace el script original)
    try:
        data  = get_json(session, URL_Q160)
        rows  = _extract_rows_q160(data)
        if rows:
            df = pd.json_normalize(rows)
            print(f"  → {len(df)} pozos (sin paginado)")
            return df
    except Exception as e:
        print(f"  ⚠️  Error primer intento Q160: {e}")

    # Intento 2: con paginado
    print(f"  → Intentando con paginado...")
    all_rows, offset, last_count = [], 0, -1
    try:
        for _ in range(MAX_PAGES):
            params = {"limit": LIMIT, "offset": offset}
            data   = get_json(session, URL_Q160, params=params)
            rows   = _extract_rows_q160(data)
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if len(all_rows) == last_count:
                break
            last_count = len(all_rows)
            if len(rows) < LIMIT:
                break
        if all_rows:
            df = pd.json_normalize(all_rows)
            print(f"  → {len(df)} pozos (con paginado)")
            return df
    except Exception as e:
        print(f"  ⚠️  Error paginado Q160: {e}")

    print(f"  ⚠️  Q160 sin datos — continuando sin metadata")
    return pd.DataFrame()

# ==========================================================
# Merge
# ==========================================================

def merge_con_q160(df: pd.DataFrame, df160: pd.DataFrame) -> pd.DataFrame:
    if df160.empty:
        for col in RENAME_Q160.values():
            df[col] = None
        return df
    cols_ok = [c for c in COLS_Q160 if c in df160.columns]
    merged  = df.merge(df160[cols_ok], left_on="Pozo", right_on="Pozo.name",
                       how="left").drop(columns=["Pozo.name"], errors="ignore")
    return merged.rename(columns=RENAME_Q160)

# ==========================================================
# Merma
# ==========================================================

def calcular_merma(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Fecha y Hora"] = pd.to_datetime(df["Fecha y Hora"], errors="coerce")
    hoy = pd.Timestamp.today().normalize()
    resumen = []

    for pozo, grp in df.groupby("Pozo"):
        grp       = grp.sort_values("Fecha y Hora")
        ultimo    = grp.iloc[-1]
        penultimo = grp.iloc[-2] if len(grp) >= 2 else None
        dias      = (hoy - ultimo["Fecha y Hora"]).days if pd.notna(ultimo["Fecha y Hora"]) else None

        if penultimo is not None:
            np_, pp_ = ultimo["Producción de Petróleo"], penultimo["Producción de Petróleo"]
            nb_, pb_ = ultimo["Producción de Líquido"],  penultimo["Producción de Líquido"]
            try:    pct_neta  = round((np_ - pp_) / pp_ * 100, 1) if pp_ != 0 else None
            except: pct_neta  = None
            try:    pct_bruta = round((nb_ - pb_) / pb_ * 100, 1) if pb_ != 0 else None
            except: pct_bruta = None
        else:
            np_ = pp_ = nb_ = pb_ = pct_neta = pct_bruta = None

        resumen.append({
            "POZO": pozo, "BATERIA": ultimo.get("BATERIA"), "ESTADO_POZO": ultimo.get("ESTADO_POZO"),
            "TIPO_PRODUCCION": ultimo.get("TIPO_PRODUCCION"), "SIST_EXTRACCION": ultimo.get("SIST_EXTRACCION"),
            "FECHA_ULTIMO_CONTROL": str(ultimo["Fecha y Hora"].date()) if pd.notna(ultimo["Fecha y Hora"]) else None,
            "DIAS_SIN_CONTROL": dias, "NETA_ULTIMO_M3": np_, "NETA_PENULTIMO_M3": pp_,
            "PCT_MERMA_NETA": pct_neta, "BRUTA_ULTIMO_M3": nb_, "BRUTA_PENULTIMO_M3": pb_,
            "PCT_MERMA_BRUTA": pct_bruta,
            "EN_MERMA_NETA":  (pct_neta  < 0) if pct_neta  is not None else None,
            "EN_MERMA_BRUTA": (pct_bruta < 0) if pct_bruta is not None else None,
        })

    return pd.DataFrame(resumen).sort_values("PCT_MERMA_NETA", ascending=True, na_position="last")

# ==========================================================
# Main
# ==========================================================

def main():
    print("=" * 60)
    print("  fetch_controles.py — Controles incrementales → GCS")
    print("=" * 60)

    if not PASSWORD:
        print("❌ ZAFIRO_PASSWORD no configurado.")
        sys.exit(1)
    if not GCS_BUCKET:
        print("❌ DINAS_BUCKET no configurado.")
        sys.exit(1)

    t0  = time.time()
    hoy = date.today()

    print("\n[1/5] Leyendo histórico desde GCS...")
    df_prev = read_csv_gcs(HISTORICO_BLOB)

    if df_prev is not None and not df_prev.empty and "Fecha y Hora" in df_prev.columns:
        df_prev["Fecha y Hora"] = pd.to_datetime(df_prev["Fecha y Hora"], errors="coerce")
        last_date    = df_prev["Fecha y Hora"].dropna().max().date()
        fecha_inicio = last_date
        print(f"  Último control: {last_date} → descargando desde {fecha_inicio} hasta {hoy}")
    else:
        fecha_inicio = FECHA_INICIO_HISTORICO
        print(f"  Primera corrida — desde {fecha_inicio} hasta {hoy}")

    if fecha_inicio > hoy:
        print("✅ Ya estás al día.")
        return

    session = build_session()

    print("\n[2/5] Descargando controles nuevos (Q164)...")
    df_nuevos = fetch_q164_rango(session, fecha_inicio, hoy)
    if df_nuevos.empty:
        print("  ℹ️  Sin controles nuevos.")
        return

    print("\n[3/5] Descargando metadata de pozos (Q160)...")
    df160 = fetch_q160(session)
    print(f"  → Q160 trajo {len(df160)} pozos")

    print("\n[4/5] Consolidando...")
    df_nuevos_merged = merge_con_q160(df_nuevos, df160)

    COLS_Q160_NAMES = list(RENAME_Q160.values())
    if df_prev is not None and not df_prev.empty:
        tiene_cols = all(c in df_prev.columns for c in COLS_Q160_NAMES)
        if not tiene_cols:
            print("  ℹ️  Mergeando histórico completo con Q160...")
            df_prev = merge_con_q160(df_prev, df160)
        df_combinado = pd.concat([df_prev, df_nuevos_merged], ignore_index=True)
    else:
        df_combinado = df_nuevos_merged.copy()

    df_combinado["Fecha y Hora"] = pd.to_datetime(df_combinado["Fecha y Hora"], errors="coerce")
    antes        = len(df_combinado)
    df_combinado = df_combinado.drop_duplicates(subset=["Pozo", "Fecha y Hora"])
    print(f"  Total: {len(df_combinado)} filas (dedup: -{antes - len(df_combinado)})")

    print("\n[5/5] Calculando merma y guardando...")
    df_merma = calcular_merma(df_combinado)
    en_merma = int(df_merma["EN_MERMA_NETA"].sum()) if "EN_MERMA_NETA" in df_merma.columns else 0
    print(f"  Pozos: {len(df_merma)} | En merma neta: {en_merma}")

    write_csv_gcs(df_combinado, HISTORICO_BLOB)
    write_csv_gcs(df_merma,     MERMA_BLOB)

    print(f"\n✅ Completado en {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
