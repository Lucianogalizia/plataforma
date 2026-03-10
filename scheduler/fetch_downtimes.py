#!/usr/bin/env python3
# ==========================================================
# scheduler/fetch_downtimes.py
#
# Proceso diario que descarga el histórico CRUDO de
# wellDowntimes desde la API de Zafiro/Infoil y lo guarda
# acumulativamente en GCS como CSV.
#
# Lee:    gs://BUCKET/merma/wellDowntimes_CRUDO.csv  (si existe)
# Escribe: gs://BUCKET/merma/wellDowntimes_CRUDO.csv
#
# Modo incremental: detecta el último día guardado y solo
# descarga los días que faltan hasta hoy.
#
# FIXES aplicados:
#   1. get_last_saved_date: usa FECHA HASTA como fallback si
#      FECHA DESDE tiene nulos, para no quedarse trabado.
#   2. Re-fetchea automáticamente los días que tengan filas
#      con FECHA DESDE nula en el histórico guardado.
#   3. Paradas activas (FECHA HASTA > hoy): se guarda null
#      en FECHA HASTA para no mostrar fechas futuras falsas.
#      Al día siguiente el scheduler las re-descarga con el
#      valor real.
#   4. sort_values con na_position="last" para que los nulos
#      no suban al tope y rompan el dedup.
#   5. Clave de dedup cambiada a [POZO, RUBRO, FECHA HASTA]
#      para no depender de FECHA DESDE que puede ser nula.
#
# Variables de entorno requeridas:
#   DINAS_BUCKET        → nombre del bucket GCS
#   DINAS_GCS_PREFIX    → prefijo dentro del bucket
#   ZAFIRO_USUARIO      → usuario API Zafiro
#   ZAFIRO_PASSWORD     → contraseña API Zafiro
# ==========================================================

from __future__ import annotations

import io
import os
import sys
import time
import random
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException

# ==========================================================
# Configuración
# ==========================================================

USUARIO  = os.environ.get("ZAFIRO_USUARIO", "dgalizia")
PASSWORD = os.environ.get("ZAFIRO_PASSWORD", "")

URL = "https://patagonia.infoil.com.ar/rest1/transactions/wellDowntimes/"

FECHA_INICIO_HISTORICO = date(2025, 12, 1)

# GCS
GCS_BUCKET = os.environ.get("DINAS_BUCKET", "").strip()
GCS_PREFIX = os.environ.get("DINAS_GCS_PREFIX", "").strip().strip("/")

DOWNTIMES_BLOB = "merma/wellDowntimes_CRUDO.csv"

# Paginación
LIMIT               = 1000
MAX_PAGES           = 20000
REQUEST_TIMEOUT     = (30, 180)
PAUSA_ENTRE_PAGINAS = 0.1
PAUSA_ENTRE_DIAS    = 0.2

# Columnas a conservar y renombres
COLUMNAS = {
    "dateAndTime"             : "FECHA DESDE",
    "finalDateAndTime"        : "FECHA HASTA",
    "oilShortfall"            : "oilShortfall",
    "waterShortfall"          : "waterShortfall",
    "liquidShortfall"         : "liquidShortfall",
    "gasShortfall"            : "gasShortfall",
    "waterInjection"          : "waterInjection",
    "potentialOil"            : "potentialOil",
    "sumpInjection"           : "sumpInjection",
    "potentialWater"          : "potentialWater",
    "potentialLiquid"         : "potentialLiquid",
    "potentialGas"            : "potentialGas",
    "potentialWaterInjection" : "potentialWaterInjection",
    "entity.name"             : "POZO",
    "shortfallCause.name"     : "RUBRO",
}


# ==========================================================
# GCS helpers
# ==========================================================

def get_gcs_client():
    try:
        from google.cloud import storage
        return storage.Client()
    except Exception as e:
        print(f"❌ No se pudo crear cliente GCS: {e}")
        return None


def blob_name() -> str:
    if GCS_PREFIX:
        return f"{GCS_PREFIX}/{DOWNTIMES_BLOB}"
    return DOWNTIMES_BLOB


def read_csv_from_gcs() -> Optional[pd.DataFrame]:
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return None
    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(blob_name())
        if not blob.exists():
            print("ℹ️  No existe CSV previo en GCS. Primera corrida.")
            return None
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
        print(f"📂 Histórico leído desde GCS: {len(df)} filas")
        return df
    except Exception as e:
        print(f"⚠️  Error leyendo CSV de GCS: {e}")
        return None


def write_csv_to_gcs(df: pd.DataFrame) -> bool:
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        print("❌ GCS no configurado (DINAS_BUCKET vacío).")
        return False
    try:
        bucket  = client.bucket(GCS_BUCKET)
        blob    = bucket.blob(blob_name())
        content = df.to_csv(index=False, encoding="utf-8").encode("utf-8")
        blob.upload_from_string(content, content_type="text/csv")
        print(f"✅ CSV subido a gs://{GCS_BUCKET}/{blob_name()} ({len(df)} filas)")
        return True
    except Exception as e:
        print(f"❌ Error subiendo CSV a GCS: {e}")
        return False


# ==========================================================
# HTTP helpers
# ==========================================================

def build_session():
    s = requests.Session()
    s.auth = HTTPBasicAuth(USUARIO, PASSWORD)
    s.headers.update({
        "Accept"    : "application/json",
        "User-Agent": "PythonZafiroClient/1.0",
    })
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def get_payload(session, params, max_intentos=12):
    last_status = None
    last_text   = ""

    for intento in range(1, max_intentos + 1):
        try:
            r = session.get(
                URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=False,
            )
            last_status = r.status_code
            last_text   = (r.text or "")[:1200]

            if 300 <= r.status_code < 400:
                raise RuntimeError(f"Redirección HTTP {r.status_code}")

            content_type    = (r.headers.get("Content-Type") or "").lower()
            looks_like_html = ("text/html" in content_type) or last_text.lstrip().startswith("<!DOCTYPE")

            if r.status_code in (502, 503, 504) or looks_like_html:
                sleep_s = min(60, 2 ** (intento - 1)) + random.uniform(0, 1.5)
                print(f"  ⚠️  Gateway/HTML {r.status_code} — reintento {intento}/{max_intentos} en {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue

            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {last_text}")

            return r.json()

        except (RequestException, RuntimeError) as e:
            sleep_s = min(60, 2 ** (intento - 1)) + random.uniform(0, 1.5)
            print(f"  ⚠️  Error intento {intento}/{max_intentos}: {e} — reintento en {sleep_s:.1f}s")
            time.sleep(sleep_s)

    raise RuntimeError(
        f"❌ Sin respuesta válida tras {max_intentos} intentos. "
        f"Último status={last_status}. Body: {last_text}"
    )


def extract_rows(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("data", "results", "items"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []


# ==========================================================
# Lógica de descarga
# ==========================================================

def get_last_saved_date(df: Optional[pd.DataFrame]) -> Optional[date]:
    """
    Detecta la fecha máxima en el histórico existente.
    FIX: usa FECHA DESDE ignorando nulos. Si todo es nulo,
    cae a FECHA HASTA como fallback restando 1 día.
    """
    if df is None or df.empty:
        return None

    # Intentar con FECHA DESDE (ignorando nulos)
    if "FECHA DESDE" in df.columns:
        s = pd.to_datetime(df["FECHA DESDE"], errors="coerce").dropna()
        if not s.empty:
            return s.max().date()

    # Fallback: usar FECHA HASTA si FECHA DESDE está todo vacío
    if "FECHA HASTA" in df.columns:
        s = pd.to_datetime(df["FECHA HASTA"], errors="coerce").dropna()
        if not s.empty:
            # FECHA HASTA es el día siguiente a las 07:00, restamos 1 día
            return (s.max().date() - timedelta(days=1))

    return None


def get_days_to_refetch(df: Optional[pd.DataFrame]) -> list[date]:
    """
    FIX: detecta qué días tienen filas con FECHA DESDE nula
    en el histórico guardado y los devuelve para re-fetchear.
    Esto ocurre cuando el scheduler corrió con paradas activas
    que Zafiro devolvió sin dateAndTime, y ahora ya están cerradas.
    """
    if df is None or df.empty or "FECHA DESDE" not in df.columns:
        return []

    nulos = df[df["FECHA DESDE"].isna()]
    if nulos.empty:
        return []

    # Los días a re-fetchear son los de FECHA HASTA menos 1 día
    # (porque FECHA HASTA = día siguiente 07:00)
    if "FECHA HASTA" not in df.columns:
        return []

    fechas_hasta = pd.to_datetime(nulos["FECHA HASTA"], errors="coerce").dropna()
    dias = sorted(set(
        (fh - timedelta(days=1)).date()
        for fh in fechas_hasta
        if pd.notna(fh)
    ))
    return dias


def fetch_rows_one_day(session, day_str: str):
    offset   = 0
    all_rows = []
    t0       = time.time()

    for _ in range(1, MAX_PAGES + 1):
        params = {
            "rs"    : "fromTo",
            "start" : day_str,
            "end"   : day_str,
            "offset": offset,
            "limit" : LIMIT,
        }
        payload = get_payload(session, params)
        rows    = extract_rows(payload)
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
        if len(rows) < LIMIT:
            break
        time.sleep(PAUSA_ENTRE_PAGINAS)

    return all_rows, len(all_rows), time.time() - t0


def select_and_rename(df: pd.DataFrame) -> pd.DataFrame:
    cols_presentes = {k: v for k, v in COLUMNAS.items() if k in df.columns}
    faltantes = [k for k in COLUMNAS if k not in df.columns]
    if faltantes:
        print(f"  ⚠️  Columnas ausentes en API (se omiten): {faltantes}")
    return df[list(cols_presentes.keys())].rename(columns=cols_presentes)


def daterange(d1: date, d2: date):
    d = d1
    while d <= d2:
        yield d
        d += timedelta(days=1)


def nullify_active_downtimes(df: pd.DataFrame, hoy: date) -> pd.DataFrame:
    """
    FIX: las paradas activas tienen FECHA HASTA > hoy (proyectada
    al próximo corte 07:00). Las marcamos con FECHA HASTA = null
    para no mostrar fechas futuras falsas en el frontend.
    Al día siguiente el scheduler las re-descarga con el valor real.
    """
    if "FECHA HASTA" not in df.columns:
        return df

    df = df.copy()
    df["FECHA HASTA"] = pd.to_datetime(df["FECHA HASTA"], errors="coerce")
    hoy_ts = pd.Timestamp(hoy)
    activas = df["FECHA HASTA"] > hoy_ts
    n_activas = activas.sum()
    if n_activas:
        df.loc[activas, "FECHA HASTA"] = pd.NaT
        print(f"  ⏳ Paradas activas (FECHA HASTA > hoy) marcadas como null: {n_activas}")
    return df


# ==========================================================
# Main
# ==========================================================

def main():
    print("=" * 60)
    print("  fetch_downtimes.py — Zafiro → GCS")
    print("=" * 60)

    if not PASSWORD:
        print("❌ ZAFIRO_PASSWORD no está configurado. Abortando.")
        sys.exit(1)

    if not GCS_BUCKET:
        print("❌ DINAS_BUCKET no está configurado. Abortando.")
        sys.exit(1)

    HOY = date.today()
    FIN = HOY

    # Leer histórico existente desde GCS
    df_prev    = read_csv_from_gcs()
    last_saved = get_last_saved_date(df_prev)

    # FIX: detectar días con FECHA DESDE nula para re-fetchear
    dias_refetch = get_days_to_refetch(df_prev)
    if dias_refetch:
        print(f"🔁 Días con FECHA DESDE nula detectados para re-fetchear: {[str(d) for d in dias_refetch]}")

    if last_saved is None:
        INICIO = FECHA_INICIO_HISTORICO
        print(f"🆕 Primera corrida. Descargando desde {INICIO} hasta {FIN}")
    else:
        INICIO = last_saved + timedelta(days=1)
        print(f"♻️  Incremental. Último guardado: {last_saved} → Descargando: {INICIO} → {FIN}")

    # Unir días nuevos + días a re-fetchear (sin duplicados, ordenados)
    dias_nuevos   = list(daterange(INICIO, FIN))
    todos_los_dias = sorted(set(dias_refetch + dias_nuevos))

    if not todos_los_dias:
        print(f"✅ Ya estás al día. Último guardado: {last_saved}")
        return

    session    = build_session()
    all_frames = []
    total_rows = 0
    t0         = time.time()

    print(f"\n🚀 Iniciando descarga | {todos_los_dias[0]} → {todos_los_dias[-1]}\n")

    for d in todos_los_dias:
        day_str = d.strftime("%Y-%m-%d")
        rows, n, dt_day = fetch_rows_one_day(session, day_str)

        if n == 0:
            print(f"  {day_str} | sin datos")
            time.sleep(PAUSA_ENTRE_DIAS)
            continue

        df_day = pd.json_normalize(rows)
        df_day = select_and_rename(df_day)
        all_frames.append(df_day)
        total_rows += n
        print(f"  {day_str} | filas={n} | {dt_day:.1f}s")
        time.sleep(PAUSA_ENTRE_DIAS)

    print()

    if not all_frames:
        print("⚠️  Sin datos nuevos en el rango consultado. No se actualiza GCS.")
        return

    df_new = pd.concat(all_frames, ignore_index=True)

    # FIX: marcar paradas activas con FECHA HASTA = null
    df_new = nullify_active_downtimes(df_new, HOY)

    # Merge con histórico
    if df_prev is not None and not df_prev.empty:
        # FIX: eliminar del histórico los días que re-fetcheamos
        # para que las filas nuevas (completas) los reemplacen
        if dias_refetch and "FECHA DESDE" in df_prev.columns:
            df_prev["FECHA DESDE"] = pd.to_datetime(df_prev["FECHA DESDE"], errors="coerce")
            dias_refetch_ts = pd.to_datetime(dias_refetch)
            mask_refetch = df_prev["FECHA DESDE"].dt.normalize().isin(dias_refetch_ts)
            # También eliminar filas con FECHA DESDE nula de esos días
            mask_nulos = df_prev["FECHA DESDE"].isna()
            n_eliminadas = (mask_refetch | mask_nulos).sum()
            df_prev = df_prev[~(mask_refetch | mask_nulos)]
            print(f"  🗑️  Filas reemplazadas del histórico: {n_eliminadas}")

        df_all = pd.concat([df_prev, df_new], ignore_index=True)
    else:
        df_all = df_new

    # FIX: clave de dedup usa FECHA HASTA en lugar de FECHA DESDE
    # para no depender de un campo que puede ser nulo
    key_cols = [c for c in ["POZO", "RUBRO", "FECHA HASTA"] if c in df_all.columns]
    if key_cols:
        antes  = len(df_all)
        df_all = df_all.drop_duplicates(subset=key_cols, keep="last")
        dupes  = antes - len(df_all)
        if dupes:
            print(f"  🔁 Duplicados eliminados: {dupes}")

    # FIX: ordenar con na_position="last" para que nulos no suban al tope
    if "FECHA DESDE" in df_all.columns:
        df_all["FECHA DESDE"] = pd.to_datetime(df_all["FECHA DESDE"], errors="coerce")
        df_all = df_all.sort_values("FECHA DESDE", ascending=False, na_position="last")

    # Subir a GCS
    write_csv_to_gcs(df_all)

    elapsed = time.time() - t0
    nulos_final = df_all["FECHA DESDE"].isna().sum() if "FECHA DESDE" in df_all.columns else "?"
    print(f"\n✅ Completado en {elapsed:.1f}s")
    print(f"   Nuevas filas descargadas : {total_rows}")
    print(f"   Total histórico en GCS   : {len(df_all)}")
    print(f"   FECHA DESDE nula (activas): {nulos_final}")
    print(f"   Destino: gs://{GCS_BUCKET}/{blob_name()}")


if __name__ == "__main__":
    main()
