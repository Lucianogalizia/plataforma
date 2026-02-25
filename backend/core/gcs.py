import os
import json
import tempfile
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()


def is_gs_path(path: str | None) -> bool:
    """True si parece una ruta de GCS tipo gs://bucket/obj."""
    return isinstance(path, str) and path.strip().lower().startswith("gs://")


def parse_gs_path(gs_path: str) -> tuple[str, str]:
    """Parsea 'gs://bucket/algo/archivo.ext' -> ('bucket', 'algo/archivo.ext')."""
    if not is_gs_path(gs_path):
        raise ValueError(f"No es una ruta gs:// válida: {gs_path!r}")
    no_scheme = gs_path.strip()[5:]  # len("gs://")
    parts = no_scheme.split("/", 1)
    bucket = parts[0]
    blob = parts[1] if len(parts) > 1 else ""
    if not bucket:
        raise ValueError(f"Ruta gs:// inválida (bucket vacío): {gs_path!r}")
    return bucket, blob


# ------------------------------
# Config
# ------------------------------
GCP_PROJECT = os.getenv("GCP_PROJECT")
GCS_BUCKET = os.getenv("GCS_BUCKET")

INDEX_PARQUET_BLOB = os.getenv("INDEX_PARQUET_BLOB", "index.parquet")
INDEX_PARQUET_LOCAL = os.getenv("INDEX_PARQUET_LOCAL", "/tmp/index.parquet")

NIV_PARQUET_BLOB = os.getenv("NIV_PARQUET_BLOB", "niv.parquet")
NIV_PARQUET_LOCAL = os.getenv("NIV_PARQUET_LOCAL", "/tmp/niv.parquet")

COORDS_REPO_BLOB = os.getenv("COORDS_REPO_BLOB", "coords_repo.json")
COORDS_REPO_LOCAL = os.getenv("COORDS_REPO_LOCAL", "/tmp/coords_repo.json")

SNAPSHOT_BLOB = os.getenv("SNAPSHOT_BLOB", "snapshot.json")
SNAPSHOT_LOCAL = os.getenv("SNAPSHOT_LOCAL", "/tmp/snapshot.json")


# ------------------------------
# Cliente / Bucket
# ------------------------------
def get_gcs_client():
    """Cliente de GCS (Cloud Run suele usar ADC / service account)."""
    if GCP_PROJECT:
        return storage.Client(project=GCP_PROJECT)
    return storage.Client()


def get_bucket():
    """Devuelve el bucket configurado por GCS_BUCKET."""
    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET no configurado")
    client = get_gcs_client()
    return client.bucket(GCS_BUCKET)


# ------------------------------
# Helpers de descarga
# ------------------------------
def gcs_download_to_temp(gs_path: str) -> str:
    """Descarga un gs://... a un archivo temporal y devuelve el path local."""
    bucket_name, blob_name = parse_gs_path(gs_path)
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    suffix = os.path.splitext(blob_name)[1] or ""
    fd, local_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    blob.download_to_filename(local_path)
    return local_path


def resolve_existing_path(path_or_gs: str) -> str:
    """
    Si es gs://... lo baja a /tmp y devuelve el path local.
    Si es path local, lo devuelve tal cual.
    """
    if is_gs_path(path_or_gs):
        return gcs_download_to_temp(path_or_gs)
    return path_or_gs


# ------------------------------
# Index parquet DIN
# ------------------------------
def load_din_index(force_download: bool = False) -> str:
    """
    Asegura que exista el index parquet local (cache en /tmp).
    Devuelve el path local del parquet.
    """
    if (not force_download) and os.path.exists(INDEX_PARQUET_LOCAL):
        return INDEX_PARQUET_LOCAL

    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET no configurado para descargar el index parquet")

    bucket = get_bucket()
    blob = bucket.blob(INDEX_PARQUET_BLOB)
    os.makedirs(os.path.dirname(INDEX_PARQUET_LOCAL), exist_ok=True)
    blob.download_to_filename(INDEX_PARQUET_LOCAL)
    return INDEX_PARQUET_LOCAL


# ------------------------------
# Index parquet NIV
# ------------------------------
def load_niv_index(force_download: bool = False) -> str:
    if (not force_download) and os.path.exists(NIV_PARQUET_LOCAL):
        return NIV_PARQUET_LOCAL

    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET no configurado para descargar el niv parquet")

    bucket = get_bucket()
    blob = bucket.blob(NIV_PARQUET_BLOB)
    os.makedirs(os.path.dirname(NIV_PARQUET_LOCAL), exist_ok=True)
    blob.download_to_filename(NIV_PARQUET_LOCAL)
    return NIV_PARQUET_LOCAL


# ------------------------------
# Coords repo (JSON)
# ------------------------------
def load_coords_repo(force_download: bool = False) -> dict:
    if (not force_download) and os.path.exists(COORDS_REPO_LOCAL):
        try:
            with open(COORDS_REPO_LOCAL, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            pass

    if not GCS_BUCKET:
        return {}

    try:
        bucket = get_bucket()
        blob = bucket.blob(COORDS_REPO_BLOB)
        if not blob.exists():
            return {}
        os.makedirs(os.path.dirname(COORDS_REPO_LOCAL), exist_ok=True)
        blob.download_to_filename(COORDS_REPO_LOCAL)
        with open(COORDS_REPO_LOCAL, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


# ------------------------------
# Snapshot (JSON)
# ------------------------------
def load_snapshot(force_download: bool = False) -> dict:
    if (not force_download) and os.path.exists(SNAPSHOT_LOCAL):
        try:
            with open(SNAPSHOT_LOCAL, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            pass

    if not GCS_BUCKET:
        return {}

    try:
        bucket = get_bucket()
        blob = bucket.blob(SNAPSHOT_BLOB)
        if not blob.exists():
            return {}
        os.makedirs(os.path.dirname(SNAPSHOT_LOCAL), exist_ok=True)
        blob.download_to_filename(SNAPSHOT_LOCAL)
        with open(SNAPSHOT_LOCAL, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


# ------------------------------
# Validaciones (JSON)
# ------------------------------
VALIDACIONES_BLOB = os.getenv("VALIDACIONES_BLOB", "validaciones.json")
VALIDACIONES_LOCAL = os.getenv("VALIDACIONES_LOCAL", "/tmp/validaciones.json")


def load_validaciones() -> dict:
    """Carga {padron: payload} desde /tmp o desde GCS."""
    if os.path.exists(VALIDACIONES_LOCAL):
        try:
            with open(VALIDACIONES_LOCAL, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            pass

    if not GCS_BUCKET:
        return {}

    try:
        bucket = get_bucket()
        blob = bucket.blob(VALIDACIONES_BLOB)
        if not blob.exists():
            return {}
        os.makedirs(os.path.dirname(VALIDACIONES_LOCAL), exist_ok=True)
        blob.download_to_filename(VALIDACIONES_LOCAL)
        with open(VALIDACIONES_LOCAL, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def save_validaciones(data: dict) -> None:
    os.makedirs(os.path.dirname(VALIDACIONES_LOCAL), exist_ok=True)
    with open(VALIDACIONES_LOCAL, "w", encoding="utf-8") as f:
        json.dump(data or {}, f, ensure_ascii=False, indent=2)

    if not GCS_BUCKET:
        return

    bucket = get_bucket()
    blob = bucket.blob(VALIDACIONES_BLOB)
    blob.upload_from_filename(VALIDACIONES_LOCAL, content_type="application/json")


def load_all_validaciones() -> dict:
    # compat con el import del router
    return load_validaciones()


# ------------------------------
# Diagnósticos (JSON por padrón)
# ------------------------------
DIAGS_PREFIX = os.getenv("DIAGS_PREFIX", "diags/")
DIAGS_LOCAL_DIR = os.getenv("DIAGS_LOCAL_DIR", "/tmp/diags")


def _diag_local_path(padron: str) -> str:
    return os.path.join(DIAGS_LOCAL_DIR, f"{padron}.json")


def load_diag_from_gcs(padron: str) -> dict | None:
    padron = str(padron)
    local = _diag_local_path(padron)

    if os.path.exists(local):
        try:
            with open(local, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    if not GCS_BUCKET:
        return None

    try:
        bucket = get_bucket()
        blob = bucket.blob(f"{DIAGS_PREFIX}{padron}.json")
        if not blob.exists():
            return None
        os.makedirs(DIAGS_LOCAL_DIR, exist_ok=True)
        blob.download_to_filename(local)
        with open(local, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_diag_to_gcs(padron: str, diag: dict) -> None:
    padron = str(padron)
    os.makedirs(DIAGS_LOCAL_DIR, exist_ok=True)
    local = _diag_local_path(padron)

    with open(local, "w", encoding="utf-8") as f:
        json.dump(diag or {}, f, ensure_ascii=False, indent=2)

    if not GCS_BUCKET:
        return

    bucket = get_bucket()
    blob = bucket.blob(f"{DIAGS_PREFIX}{padron}.json")
    blob.upload_from_filename(local, content_type="application/json")


def load_all_diags_from_gcs() -> dict:
    """Devuelve {padron: diag_json} listando blobs bajo DIAGS_PREFIX."""
    if not GCS_BUCKET:
        return {}

    out: dict = {}
    try:
        bucket = get_bucket()
        for blob in bucket.list_blobs(prefix=DIAGS_PREFIX):
            name = blob.name
            if not name.endswith(".json"):
                continue
            padron = os.path.basename(name)[:-5]
            try:
                out[padron] = json.loads(blob.download_as_text(encoding="utf-8"))
            except Exception:
                continue
        return out
    except Exception:
        return {}
