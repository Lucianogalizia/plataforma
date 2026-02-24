#!/usr/bin/env python3
# ==========================================================
# scheduler/build_snapshot.py
#
# Proceso nocturno que construye el snapshot.parquet con
# la última medición por pozo + todos los extras (DIN).
#
# Lee:
#   - gs://BUCKET/din_index.parquet
#   - gs://BUCKET/niv_index.parquet
#   - Descarga .din de gs://BUCKET/data_store/din/...
#
# Escribe:
#   - gs://BUCKET/snapshot.parquet
#
# Uso:
#   python build_snapshot.py
#
# Variables de entorno requeridas:
#   DINAS_BUCKET       → nombre del bucket GCS
#   DINAS_GCS_PREFIX   → prefijo dentro del bucket (ej: interfaz_dinas)
#   GOOGLE_CLOUD_PROJECT → project ID (para Secret Manager)
# ==========================================================

from __future__ import annotations

import io
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

GCS_BUCKET = os.environ.get("DINAS_BUCKET", "").strip()
GCS_PREFIX = os.environ.get("DINAS_GCS_PREFIX", "").strip().strip("/")

EXTRA_FIELDS = {
    "Tipo AIB":                    ("AIB",        "MA"),
    "AIB Carrera":                 ("AIB",        "CS"),
    "Sentido giro":                ("AIB",        "SG"),
    "Tipo Contrapesos":            ("CONTRAPESO",  "TP"),
    "Distancia contrapesos (cm)":  ("CONTRAPESO",  "DE"),
    "Contrapeso actual":           ("RARE",        "CA"),
    "Contrapeso ideal":            ("RARE",        "CM"),
    "AIBEB_Torque max contrapeso": ("RAEB",        "TM"),
    "%Estructura":                 ("RARE",        "SE"),
    "%Balance":                    ("RARR",        "PC"),
    "Bba Diam Pistón":             ("BOMBA",       "DP"),
    "Bba Prof":                    ("BOMBA",       "PB"),
    "Bba Llenado":                 ("BOMBA",       "CA"),
    "GPM":                         ("AIB",         "GM"),
    "Caudal bruto efec":           ("RBO",         "CF"),
    "Polea Motor":                 ("MOTOR",       "DP"),
    "Potencia Motor":              ("MOTOR",       "PN"),
    "RPM Motor":                   ("MOTOR",       "RM"),
}

SECTION_RE = re.compile(r"^\s*\[(.+?)\]\s*$")
KV_RE      = re.compile(r"^\s*([^=]+?)\s*=\s*(.*?)\s*$")


# ---------------------------------------------------------------------------
# Helpers GCS
# ---------------------------------------------------------------------------

def _get_client():
    from google.cloud import storage
    return storage.Client()


def _gcs_join(*parts: str) -> str:
    parts_clean = [p.strip("/").replace("\\", "/") for p in parts if p and str(p).strip()]
    suffix = "/".join(parts_clean)
    if GCS_PREFIX:
        suffix = f"{GCS_PREFIX}/{suffix}"
    return f"gs://{GCS_BUCKET}/{suffix}"


def _parse_gs_url(gs_url: str):
    u = gs_url.strip()[5:]
    bucket, _, blob = u.partition("/")
    return bucket, blob


def _download_to_temp(gs_url: str) -> str:
    client = _get_client()
    bucket_name, blob_name = _parse_gs_url(gs_url)
    safe_name  = blob_name.replace("/", "__")
    local_path = os.path.join(tempfile.gettempdir(), safe_name)
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return local_path
    client.bucket(bucket_name).blob(blob_name).download_to_filename(local_path)
    return local_path


def _map_to_gcs(path_str: str | None) -> str | None:
    if not path_str or not GCS_BUCKET:
        return None
    p   = str(path_str).replace("\\", "/")
    idx = p.lower().find("/data_store/")
    if idx == -1:
        return None
    return _gcs_join(p[idx + 1:])


def _resolve(path_str: str | None) -> str | None:
    if not path_str:
        return None
    p = str(path_str).strip()
    if p.lower().startswith("gs://"):
        return p
    if Path(p).exists():
        return p
    gcs = _map_to_gcs(p)
    return gcs


# ---------------------------------------------------------------------------
# Parseo de .din
# ---------------------------------------------------------------------------

def _read_text(path: str) -> str:
    p = Path(path)
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return p.read_text(encoding=enc, errors="strict")
        except Exception:
            pass
    return p.read_text(encoding="latin-1", errors="ignore")


def _safe_float(v) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", ".")
    if "=" in s:
        s = s.split("=")[-1].strip()
    try:
        return float(s)
    except Exception:
        return None


def parse_din_extras(path_str: str) -> dict:
    if path_str.lower().startswith("gs://"):
        path_str = _download_to_temp(path_str)

    txt = _read_text(path_str)
    wanted = {
        (sec.upper(), key.upper()): col
        for col, (sec, key) in EXTRA_FIELDS.items()
    }
    out = {col: None for col in EXTRA_FIELDS}
    section = None

    for line in txt.splitlines():
        m = SECTION_RE.match(line)
        if m:
            section = m.group(1).strip().upper()
            continue
        m = KV_RE.match(line)
        if not m or not section:
            continue
        k = m.group(1).strip().upper()
        v = m.group(2).strip()
        if (section, k) in wanted:
            col = wanted[(section, k)]
            fv = _safe_float(v)
            out[col] = fv if fv is not None else (v if v else None)

    # Fallback %Balance
    if out.get("%Balance") is None:
        section = None
        for line in txt.splitlines():
            m = SECTION_RE.match(line)
            if m:
                section = m.group(1).strip().upper()
                continue
            m = KV_RE.match(line)
            if not m or not section:
                continue
            k = m.group(1).strip().upper()
            v = m.group(2).strip()
            if section == "RARR" and k == "PC":
                fv = _safe_float(v)
                out["%Balance"] = fv if fv is not None else (v if v else None)
                break

    return out


# ---------------------------------------------------------------------------
# Carga de índices
# ---------------------------------------------------------------------------

def load_index(kind: str = "din") -> pd.DataFrame:
    filename = f"{kind}_index.parquet"
    gs_url = _gcs_join(filename)
    try:
        lp = _download_to_temp(gs_url)
        df = pd.read_parquet(lp)
        print(f"  [{kind.upper()} index] {len(df)} filas desde GCS")
        return df
    except Exception as e:
        print(f"  ⚠️ No se pudo cargar {filename}: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Normalización
# ---------------------------------------------------------------------------

def normalize_no(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if s.upper() in ("<NA>", "NAN", "NONE"):
        return ""
    s = s.replace("–", "-").replace("—", "-").replace("−", "-")
    s = re.sub(r"\s+", "", s)
    return s.casefold().upper()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_snapshot():
    if not GCS_BUCKET:
        print("❌ DINAS_BUCKET no está configurado. Abortando.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  build_snapshot.py — {datetime.now(timezone.utc).isoformat()}")
    print(f"  Bucket: {GCS_BUCKET}")
    print(f"  Prefix: {GCS_PREFIX or '(vacío)'}")
    print(f"{'='*60}\n")

    # 1. Cargar índices
    df_din = load_index("din")
    df_niv = load_index("niv")

    if df_din.empty and df_niv.empty:
        print("❌ No hay índices disponibles. Abortando.")
        sys.exit(1)

    # 2. Resolver paths DIN
    if not df_din.empty and "path" in df_din.columns:
        df_din["path"] = df_din["path"].apply(lambda x: _resolve(x) if pd.notna(x) else None)

    # 3. Normalizar NO_key
    if not df_din.empty:
        no_col = next((c for c in ["pozo", "NO"] if c in df_din.columns), None)
        if no_col:
            df_din["NO_key"] = df_din[no_col].apply(normalize_no)

    if not df_niv.empty:
        no_col_n = next((c for c in ["pozo", "NO"] if c in df_niv.columns), None)
        if no_col_n:
            df_niv["NO_key"] = df_niv[no_col_n].apply(normalize_no)

    # 4. Obtener última medición DIN por pozo
    snap = pd.DataFrame()

    if not df_din.empty and "NO_key" in df_din.columns:
        sort_cols = [c for c in ["din_datetime", "mtime"] if c in df_din.columns]
        if sort_cols:
            df_din = df_din.sort_values(sort_cols, na_position="last")
        snap = (
            df_din.dropna(subset=["path"])
            .drop_duplicates(subset=["path"])
            .groupby("NO_key", as_index=False)
            .tail(1)
            .copy()
        )

    n_pozos = len(snap)
    print(f"  Pozos con DIN para procesar extras: {n_pozos}")

    # 5. Parsear extras de cada .din
    t_inicio = time.time()
    extras = []
    errores = 0

    for i, (_, row) in enumerate(snap.iterrows(), 1):
        path = row.get("path")
        if not path:
            extras.append({k: None for k in EXTRA_FIELDS})
            continue

        if i % 10 == 0 or i == n_pozos:
            elapsed  = time.time() - t_inicio
            vel      = elapsed / i
            restantes = n_pozos - i
            eta      = int(vel * restantes)
            print(
                f"  [{i}/{n_pozos}] {row.get('NO_key', '?')[:20]:<20}"
                f"  ETA: {eta // 60}m {eta % 60}s",
                end="\r",
                flush=True,
            )

        try:
            extras.append(parse_din_extras(str(path)))
        except Exception as e:
            extras.append({k: None for k in EXTRA_FIELDS})
            errores += 1

    print()
    print(f"  Extras extraídos. Errores: {errores}")

    df_extras = pd.DataFrame(extras, index=snap.index)
    snap = pd.concat([snap.reset_index(drop=True), df_extras.reset_index(drop=True)], axis=1)

    # 6. Guardar en GCS
    buf = io.BytesIO()
    snap.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    snap_blob = "snapshot.parquet"
    if GCS_PREFIX:
        snap_blob = f"{GCS_PREFIX}/{snap_blob}"

    client = _get_client()
    bucket = client.bucket(GCS_BUCKET)
    blob   = bucket.blob(snap_blob)
    blob.upload_from_file(buf, content_type="application/octet-stream")

    t_total = round(time.time() - t_inicio, 1)
    print(f"\n  ✅ snapshot.parquet guardado en gs://{GCS_BUCKET}/{snap_blob}")
    print(f"  Filas: {len(snap)} | Columnas: {len(snap.columns)} | Tiempo: {t_total}s")
    print(f"  Columnas: {list(snap.columns)[:15]}...")


if __name__ == "__main__":
    build_snapshot()
