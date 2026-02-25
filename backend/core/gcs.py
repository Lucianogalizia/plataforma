# ==========================================================
# backend/core/gcs.py
#
# Toda la lógica de Google Cloud Storage extraída de app.py
# Incluye:
#   - Cliente GCS (singleton)
#   - Construcción de rutas gs://
#   - Descarga de archivos a /tmp
#   - Resolución de paths (local o GCS)
#   - Mapeo de paths locales a GCS
# ==========================================================

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ==========================================================
# Config
# ==========================================================

GCS_BUCKET = os.environ.get("DINAS_BUCKET", "")
GCS_PREFIX = os.environ.get("DINAS_GCS_PREFIX", "")

INDEX_PARQUET_LOCAL = os.environ.get("DIN_INDEX_PARQUET_LOCAL", "din_index.parquet")
INDEX_CSV_LOCAL     = os.environ.get("DIN_INDEX_CSV_LOCAL", "din_index.csv")
NIV_INDEX_LOCAL     = os.environ.get("NIV_INDEX_LOCAL", "niv_index.parquet")

SNAPSHOT_LOCAL      = os.environ.get("SNAPSHOT_LOCAL", "snapshot.parquet")

# ==========================================================
# Cliente GCS (singleton)
# ==========================================================

_gcs_client = None

def get_gcs_client():
    global _gcs_client
    if _gcs_client is not None:
        return _gcs_client
    try:
        from google.cloud import storage
        _gcs_client = storage.Client()
        return _gcs_client
    except Exception:
        _gcs_client = None
        return None

# ==========================================================
# Helpers paths
# ==========================================================

def _join_prefix(name: str) -> str:
    if not GCS_PREFIX:
        return name
    return f"{GCS_PREFIX.strip('/')}/{name.lstrip('/')}"

def get_index_parquet_gcs() -> str:
    if not GCS_BUCKET:
        return ""
    return f"gs://{GCS_BUCKET}/{_join_prefix('din_index.parquet')}"

def get_niv_index_gcs() -> str:
    if not GCS_BUCKET:
        return ""
    return f"gs://{GCS_BUCKET}/{_join_prefix('niv_index.parquet')}"

def get_snapshot_gcs() -> str:
    if not GCS_BUCKET:
        return ""
    return f"gs://{GCS_BUCKET}/{_join_prefix('snapshot.parquet')}"

# ==========================================================
# Resolución / descarga
# ==========================================================

def resolve_existing_path(path: str) -> str | None:
    if not path:
        return None
    if path.startswith("gs://"):
        return path
    if os.path.exists(path):
        return path
    return None

def gcs_download_to_temp(gs_url: str) -> str:
    """
    Descarga un gs://bucket/obj a /tmp y devuelve el path local.
    """
    client = get_gcs_client()
    if client is None:
        raise RuntimeError("GCS no disponible")

    if not gs_url.startswith("gs://"):
        raise ValueError("gs_url inválida")

    # parse gs://bucket/blob
    no = gs_url[len("gs://"):]
    bucket_name, blob_name = no.split("/", 1)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    local_path = f"/tmp/{Path(blob_name).name}"
    blob.download_to_filename(local_path)
    return local_path

def read_parquet_any(local_path: str, gs_url: str):
    """
    Lee parquet desde path local si existe; si no, desde GCS.
    """
    import pandas as pd

    if local_path and os.path.exists(local_path):
        return pd.read_parquet(local_path)

    if gs_url:
        lp = gcs_download_to_temp(gs_url)
        return pd.read_parquet(lp)

    return pd.DataFrame()

# ==========================================================
# Carga de índices (din_index, niv_index, snapshot)
# ==========================================================

def load_din_index():
    """
    Carga el índice DIN desde local (parquet > csv) o GCS.
    Cache TTL para evitar IO repetido en cada request.
    """
    from core.cache import ttl_get
    import pandas as pd

    def _loader():
        # Local parquet
        if os.path.exists(INDEX_PARQUET_LOCAL):
            try:
                return pd.read_parquet(INDEX_PARQUET_LOCAL)
            except Exception:
                pass

        # Local CSV
        if os.path.exists(INDEX_CSV_LOCAL):
            return pd.read_csv(
                INDEX_CSV_LOCAL,
                parse_dates=["mtime", "din_datetime"],
                dayfirst=True,
                keep_default_na=True,
            )

        # GCS
        if GCS_BUCKET:
            try:
                return read_parquet_any("", get_index_parquet_gcs())
            except Exception:
                return pd.DataFrame()

        return pd.DataFrame()

    return ttl_get("gcs:din_index", _loader, ttl_s=300)  # 5 min


def load_niv_index():
    """
    Carga el índice NIV desde local o GCS.
    Cache TTL.
    """
    from core.cache import ttl_get
    import pandas as pd

    def _loader():
        if os.path.exists(NIV_INDEX_LOCAL):
            try:
                return pd.read_parquet(NIV_INDEX_LOCAL)
            except Exception:
                return pd.DataFrame()

        if GCS_BUCKET:
            try:
                return read_parquet_any("", get_niv_index_gcs())
            except Exception:
                return pd.DataFrame()

        return pd.DataFrame()

    return ttl_get("gcs:niv_index", _loader, ttl_s=300)  # 5 min


def load_snapshot():
    """
    Lee el snapshot.parquet pregenerado por build_snapshot.py.
    Cache TTL para no re-parsear parquet en cada request.
    """
    from core.cache import ttl_get
    import pandas as pd

    def _loader():
        gs_url = get_snapshot_gcs()

        # Si existe local explícito, úsalo
        if SNAPSHOT_LOCAL and os.path.exists(SNAPSHOT_LOCAL):
            try:
                return pd.read_parquet(SNAPSHOT_LOCAL)
            except Exception:
                pass

        if not gs_url:
            return pd.DataFrame()

        try:
            lp = gcs_download_to_temp(gs_url)
            return pd.read_parquet(lp)
        except Exception:
            return pd.DataFrame()

    return ttl_get("gcs:snapshot", _loader, ttl_s=60)  # 1 min


# ==========================================================
# Coordenadas (Excel)
# ==========================================================

def load_coords_repo(base_dir: Path | None = None) -> "pd.DataFrame":
    """
    Carga el Excel de coordenadas de pozos desde el repo.
    Cache TTL (súper importante) porque leer Excel es lento.
    """
    from core.cache import ttl_get
    import pandas as pd

    def _loader():
        nonlocal base_dir
        if base_dir is None:
            base_dir = Path(__file__).resolve().parent.parent  # backend/

        candidates = [
            base_dir / "assets" / "Nombres-Pozo_con_coordenadas.xlsx",
            Path.cwd() / "assets" / "Nombres-Pozo_con_coordenadas.xlsx",
            Path("/app/assets/Nombres-Pozo_con_coordenadas.xlsx"),
        ]

        for p in candidates:
            try:
                if p.exists():
                    return pd.read_excel(p)
            except Exception:
                pass

        hits = list(base_dir.rglob("Nombres-Pozo_con_coordenadas.xlsx"))
        if hits:
            try:
                return pd.read_excel(hits[0])
            except Exception:
                return pd.DataFrame()

        return pd.DataFrame()

    
    return ttl_get("repo:coords_excel", _loader, ttl_s=3600)  # 1 hora

def is_gs_path(path: str) -> bool:
    return isinstance(path, str) and path.startswith("gs://")
