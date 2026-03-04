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
import tempfile
from pathlib import Path
from functools import lru_cache

from core.cache import cache as _cache

# TTL para datos que cambian ~1 vez por día (1 hora)
_DATA_TTL = 3600

# ---------- Variables de entorno ----------
GCS_BUCKET = os.environ.get("DINAS_BUCKET", "").strip()
GCS_PREFIX = os.environ.get("DINAS_GCS_PREFIX", "").strip().strip("/")

# ---------- Paths de índices ----------
PROYECTO_DIR = r"C:\Users\dgalizia\Desktop\Proyectos de IA\Interfaz Dinas"

INDEX_PARQUET_LOCAL = os.path.join(PROYECTO_DIR, "din_index.parquet")
INDEX_CSV_LOCAL     = os.path.join(PROYECTO_DIR, "din_index.csv")
NIV_INDEX_LOCAL     = os.path.join(PROYECTO_DIR, "niv_index.parquet")

# Roots para búsqueda local de archivos .din
DATA_ROOTS = [
    r"O:\Petroleum\Upstream\Desarrollo Operativo\Mediciones Fisicas",
    PROYECTO_DIR,
]


# ==========================================================
# Cliente GCS (singleton, se instancia una sola vez)
# ==========================================================

@lru_cache(maxsize=1)
def get_gcs_client():
    """
    Devuelve el cliente de Google Cloud Storage.
    Usa las credenciales por defecto del runtime (Cloud Run service account).
    Devuelve None si google-cloud-storage no está disponible.
    """
    try:
        from google.cloud import storage
        return storage.Client()
    except Exception:
        return None


# ==========================================================
# Construcción de rutas GCS
# ==========================================================

def gcs_join(*parts: str) -> str:
    """
    Construye una URL gs://bucket/prefix/part1/part2/...
    Respeta GCS_PREFIX si está definido.

    Ejemplo:
        gcs_join("data_store", "din", "Pozo", "archivo.din")
        → "gs://mi-bucket/interfaz_dinas/data_store/din/Pozo/archivo.din"
    """
    parts_clean = [
        p.strip("/").replace("\\", "/")
        for p in parts
        if p is not None and str(p).strip() != ""
    ]
    suffix = "/".join(parts_clean)
    if GCS_PREFIX:
        suffix = f"{GCS_PREFIX}/{suffix}"
    return f"gs://{GCS_BUCKET}/{suffix}"


def is_gs_path(p: str | None) -> bool:
    """Devuelve True si el string es una URL gs://"""
    return bool(p) and str(p).strip().lower().startswith("gs://")


def parse_gs_url(gs_url: str) -> tuple[str, str]:
    """
    Descompone gs://bucket/path/to/file en (bucket, blob_path).

    Raises:
        ValueError: si no es una URL gs://
    """
    u = gs_url.strip()
    if not u.lower().startswith("gs://"):
        raise ValueError(f"No es una URL gs://: {gs_url!r}")
    u = u[5:]
    bucket, _, blob = u.partition("/")
    return bucket, blob


# ==========================================================
# Índices en GCS
# ==========================================================

def get_index_parquet_gcs() -> str:
    """URL GCS del índice DIN principal."""
    return gcs_join("din_index.parquet") if GCS_BUCKET else ""


def get_niv_index_gcs() -> str:
    """URL GCS del índice NIV."""
    return gcs_join("niv_index.parquet") if GCS_BUCKET else ""


def get_snapshot_gcs() -> str:
    """URL GCS del snapshot pregenerado por build_snapshot.py."""
    blob = "snapshot.parquet"
    if GCS_PREFIX:
        blob = f"{GCS_PREFIX}/{blob}"
    return f"gs://{GCS_BUCKET}/{blob}" if GCS_BUCKET else ""


# ==========================================================
# Descarga de archivos GCS a /tmp
# ==========================================================

def gcs_download_to_temp(gs_url: str) -> str:
    """
    Descarga un archivo desde GCS a /tmp y devuelve el path local.
    Usa caché por nombre de archivo para no bajar el mismo archivo dos veces
    en la misma sesión (mientras el proceso esté vivo).

    Args:
        gs_url: URL del tipo gs://bucket/path/to/file

    Returns:
        Path local del archivo descargado en /tmp

    Raises:
        RuntimeError: si google-cloud-storage no está disponible
        Exception: cualquier error de descarga de GCS
    """
    client = get_gcs_client()
    if client is None:
        raise RuntimeError(
            "google-cloud-storage no está disponible. "
            "Verificá que esté en requirements.txt."
        )

    bucket_name, blob_name = parse_gs_url(gs_url)
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)

    # Nombre estable en /tmp basado en el path del blob
    safe_name  = blob_name.replace("/", "__")
    local_path = os.path.join(tempfile.gettempdir(), safe_name)

    # Si ya existe y tiene contenido, reusar (caché en disco)
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return local_path

    blob.download_to_filename(local_path)
    return local_path


# ==========================================================
# Carga de Parquet / CSV desde local o GCS
# ==========================================================

def read_parquet_any(local_path: str, gcs_path: str):
    """
    Lee un archivo Parquet desde local o GCS.
    Prioriza local. Si no existe, descarga de GCS.

    Returns:
        pd.DataFrame vacío si ninguna fuente está disponible.
    """
    import pandas as pd

    if local_path and os.path.exists(local_path):
        return pd.read_parquet(local_path)

    if gcs_path and is_gs_path(gcs_path):
        lp = gcs_download_to_temp(gcs_path)
        return pd.read_parquet(lp)

    return pd.DataFrame()


def read_csv_any(local_path: str, gcs_path: str):
    """
    Lee un archivo CSV desde local o GCS.
    Prioriza local. Si no existe, descarga de GCS.

    Returns:
        pd.DataFrame vacío si ninguna fuente está disponible.
    """
    import pandas as pd

    parse_kw = dict(
        parse_dates=["mtime", "din_datetime"],
        dayfirst=True,
        keep_default_na=True,
    )

    if local_path and os.path.exists(local_path):
        return pd.read_csv(local_path, **parse_kw)

    if gcs_path and is_gs_path(gcs_path):
        lp = gcs_download_to_temp(gcs_path)
        return pd.read_csv(lp, **parse_kw)

    return pd.DataFrame()


# ==========================================================
# Resolución de paths (local → GCS fallback)
# ==========================================================

def exists_local(p: str | None) -> bool:
    """Devuelve True si el path existe en el sistema de archivos local."""
    if not p:
        return False
    try:
        return Path(str(p)).exists()
    except Exception:
        return False


def map_local_datastore_to_gcs(path_str: str | None) -> str | None:
    """
    Convierte un path local del estilo:
        C:\\...\\Interfaz Dinas\\data_store\\din\\Pozo\\2025-12\\archivo.din
    en una URL GCS:
        gs://bucket/data_store/din/Pozo/2025-12/archivo.din

    Devuelve None si no se puede mapear o GCS_BUCKET no está configurado.
    """
    if not path_str or not GCS_BUCKET:
        return None

    p   = str(path_str).replace("\\", "/")
    idx = p.lower().find("/data_store/")
    if idx == -1:
        return None

    rel = p[idx + 1:]  # "data_store/..."
    return gcs_join(rel)


def resolve_existing_path(path_str: str | None) -> str | None:
    """
    Dado un path que puede ser local o gs://, devuelve el path
    que realmente existe y puede usarse para leer el archivo.

    Orden de resolución:
        1. Si ya es gs://, devolver tal cual.
        2. Si existe localmente, devolver el path local.
        3. Buscar por nombre de archivo en DATA_ROOTS (modo local).
        4. Mapear data_store → gs:// (modo Cloud Run).
        5. Devolver None si no se encontró.
    """
    if not path_str:
        return None

    p = str(path_str).strip()

    # 1. Ya es GCS
    if is_gs_path(p):
        return p

    # 2. Existe local
    if exists_local(p):
        return p

    # 3. Buscar por nombre en DATA_ROOTS (solo local)
    fname = Path(p).name
    for root in DATA_ROOTS:
        rootp = Path(root)
        if not rootp.exists():
            continue
        try:
            found = next(rootp.rglob(fname), None)
            if found and found.exists():
                return str(found)
        except Exception:
            pass

    # 4. Mapear a GCS (Cloud Run)
    gcs_guess = map_local_datastore_to_gcs(p)
    if gcs_guess:
        return gcs_guess

    return None


# ==========================================================
# Carga de índices (din_index, niv_index, snapshot)
# ==========================================================

def load_din_index():
    """
    Carga el índice DIN desde local (parquet > csv) o GCS.
    Resultado cacheado en memoria (_DATA_TTL).

    Returns:
        pd.DataFrame con el índice de archivos .din
    """
    cached = _cache.get("gcs_din_index")
    if cached is not None:
        return cached.copy()

    import pandas as pd

    result = pd.DataFrame()

    # Local parquet
    if os.path.exists(INDEX_PARQUET_LOCAL):
        try:
            result = pd.read_parquet(INDEX_PARQUET_LOCAL)
            _cache.set("gcs_din_index", result, ttl=_DATA_TTL)
            return result.copy()
        except Exception:
            pass

    # Local CSV
    if os.path.exists(INDEX_CSV_LOCAL):
        result = pd.read_csv(
            INDEX_CSV_LOCAL,
            parse_dates=["mtime", "din_datetime"],
            dayfirst=True,
            keep_default_na=True,
        )
        _cache.set("gcs_din_index", result, ttl=_DATA_TTL)
        return result.copy()

    # GCS
    if GCS_BUCKET:
        try:
            result = read_parquet_any("", get_index_parquet_gcs())
            _cache.set("gcs_din_index", result, ttl=_DATA_TTL)
            return result.copy()
        except Exception:
            return pd.DataFrame()

    return pd.DataFrame()


def load_niv_index():
    """
    Carga el índice NIV desde local o GCS.
    Resultado cacheado en memoria (_DATA_TTL).

    Returns:
        pd.DataFrame con el índice de archivos .niv
    """
    cached = _cache.get("gcs_niv_index")
    if cached is not None:
        return cached.copy()

    import pandas as pd

    if os.path.exists(NIV_INDEX_LOCAL):
        result = pd.read_parquet(NIV_INDEX_LOCAL)
        _cache.set("gcs_niv_index", result, ttl=_DATA_TTL)
        return result.copy()

    if GCS_BUCKET:
        try:
            result = read_parquet_any("", get_niv_index_gcs())
            _cache.set("gcs_niv_index", result, ttl=_DATA_TTL)
            return result.copy()
        except Exception:
            return pd.DataFrame()

    return pd.DataFrame()


def load_snapshot():
    """
    Lee el snapshot.parquet pregenerado por build_snapshot.py.
    Contiene una fila por pozo con la última medición + todos los extras.
    Carga en ~3 segundos en vez de 5 minutos.
    Resultado cacheado en memoria (_DATA_TTL).

    Returns:
        pd.DataFrame con el snapshot, vacío si no existe.
    """
    cached = _cache.get("gcs_snapshot")
    if cached is not None:
        return cached.copy()

    import pandas as pd

    gs_url = get_snapshot_gcs()
    if not gs_url:
        return pd.DataFrame()

    try:
        lp = gcs_download_to_temp(gs_url)
        result = pd.read_parquet(lp)
        _cache.set("gcs_snapshot", result, ttl=_DATA_TTL)
        return result.copy()
    except Exception:
        return pd.DataFrame()


# ==========================================================
# ==========================================================
# Diagnósticos en GCS (BLOQUE CORREGIDO)
# ==========================================================

def load_diag_from_gcs(no_key: str) -> dict | None:
    """
    Carga el JSON de diagnóstico de un pozo desde GCS.
    """
    import json
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return None

    # Mantenemos la estructura de subcarpeta: diagnosticos/POZO/diagnostico.json
    blob_name = f"diagnosticos/{no_key}/diagnostico.json"
    if GCS_PREFIX:
        blob_name = f"{GCS_PREFIX}/{blob_name}"

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(blob_name)
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text(encoding="utf-8"))
    except Exception:
        return None

def load_all_diags_from_gcs(pozos_interes: list[str]) -> dict[str, dict]:
    """
    OPTIMIZADO: Lista todos los archivos de la carpeta diagnosticos/ de una sola vez.
    Esto evita el Timeout (Error 500) y es mucho más rápido.
    Resultado cacheado en memoria (_DATA_TTL).
    """
    cached = _cache.get("gcs_all_diags")
    if cached is not None:
        return {k: v for k, v in cached.items() if k in pozos_interes}

    import json
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return {}

    results = {}
    try:
        bucket = client.bucket(GCS_BUCKET)
        prefix = "diagnosticos/"
        if GCS_PREFIX:
            prefix = f"{GCS_PREFIX}/{prefix}"

        # Listamos TODO lo que hay en la carpeta de diagnósticos de un solo golpe
        blobs = list(client.list_blobs(GCS_BUCKET, prefix=prefix))
        
        # Filtramos solo los que son diagnostico.json
        for blob in blobs:
            if not blob.name.endswith("diagnostico.json"):
                continue
            
            # Extraemos el nombre del pozo de la ruta: diagnosticos/POZO/diagnostico.json
            parts = blob.name.replace(prefix, "").split("/")
            if not parts: continue
            pozo_id = parts[0]

            try:
                data = json.loads(blob.download_as_text(encoding="utf-8"))
                if "error" not in data:
                    results[pozo_id] = data
            except:
                continue
    except Exception as e:
        print(f"Error masivo cargando diagnósticos: {e}")

    _cache.set("gcs_all_diags", results, ttl=_DATA_TTL)
    return {k: v for k, v in results.items() if k in pozos_interes}

def save_diag_to_gcs(no_key: str, diag: dict) -> bool:
    """
    Guarda el JSON de diagnóstico de un pozo en GCS.
    Invalida el caché de diagnósticos tras guardar.
    """
    import json
    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return False

    # Guardamos siempre en subcarpeta para mantener orden
    blob_name = f"diagnosticos/{no_key}/diagnostico.json"
    if GCS_PREFIX:
        blob_name = f"{GCS_PREFIX}/{blob_name}"

    try:
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(blob_name)
        blob.upload_from_string(
            json.dumps(diag, ensure_ascii=False, indent=2, default=str),
            content_type="application/json",
        )
        _cache.delete("gcs_all_diags")
        return True
    except Exception:
        return False


# ==========================================================
# Validaciones en GCS
# ==========================================================

def _val_blob_name(no_key: str) -> str:
    name = f"validaciones/{no_key}/validaciones.json"
    return f"{GCS_PREFIX}/{name}" if GCS_PREFIX else name


def load_validaciones(no_key: str) -> dict:
    """
    Carga el JSON de validaciones de un pozo desde GCS.
    Resultado cacheado en memoria (_DATA_TTL).
    Devuelve una COPIA para evitar que set_validacion (in-place) corrompa el caché.

    Returns:
        dict con validaciones, o {} si no existe.
    """
    import copy

    cache_key = f"gcs_val_{no_key}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)

    import json

    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return {}

    try:
        blob = client.bucket(GCS_BUCKET).blob(_val_blob_name(no_key))
        if not blob.exists():
            result = {}
        else:
            result = json.loads(blob.download_as_text(encoding="utf-8"))
        _cache.set(cache_key, result, ttl=_DATA_TTL)
        return result
    except Exception:
        return {}


def invalidate_validaciones_cache(no_key: str | None = None) -> None:
    """Invalida caché de validaciones (individual o todas)."""
    if no_key:
        _cache.delete(f"gcs_val_{no_key}")
    _cache.delete("gcs_all_val")


def save_validaciones(no_key: str, data: dict) -> bool:
    """
    Guarda el JSON de validaciones de un pozo en GCS.
    Invalida el caché tras guardar.

    Returns:
        True si se guardó correctamente, False si hubo error.
    """
    import json

    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return False

    try:
        blob = client.bucket(GCS_BUCKET).blob(_val_blob_name(no_key))
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2, default=str),
            content_type="application/json",
        )
        invalidate_validaciones_cache(no_key)
        return True
    except Exception:
        return False


def load_all_validaciones(pozos: list[str]) -> dict[str, dict]:
    """
    OPTIMIZADO: Lista todos los archivos de la carpeta validaciones/ de una sola vez.
    Mismo patrón batch que load_all_diags_from_gcs.
    Resultado cacheado en memoria (_DATA_TTL).

    Returns:
        dict { no_key: val_dict }
    """
    cached = _cache.get("gcs_all_val")
    if cached is not None:
        # Filtrar solo los pozos solicitados
        return {k: v for k, v in cached.items() if k in pozos}

    import json

    client = get_gcs_client()
    if not client or not GCS_BUCKET:
        return {}

    results = {}
    try:
        bucket = client.bucket(GCS_BUCKET)
        prefix = "validaciones/"
        if GCS_PREFIX:
            prefix = f"{GCS_PREFIX}/{prefix}"

        blobs = list(client.list_blobs(GCS_BUCKET, prefix=prefix))

        for blob in blobs:
            if not blob.name.endswith("validaciones.json"):
                continue

            parts = blob.name.replace(prefix, "").split("/")
            if not parts:
                continue
            no_key = parts[0]

            try:
                data = json.loads(blob.download_as_text(encoding="utf-8"))
                results[no_key] = data
            except Exception:
                continue
    except Exception as e:
        print(f"Error batch cargando validaciones: {e}")

    _cache.set("gcs_all_val", results, ttl=_DATA_TTL)
    return {k: v for k, v in results.items() if k in pozos}


# ==========================================================
# Coordenadas (Excel estático del repo)
# ==========================================================

def load_coords_repo(base_dir: Path | None = None) -> "pd.DataFrame":
    """
    Carga el Excel de coordenadas de pozos desde el repo.
    Busca en múltiples ubicaciones posibles.
    Resultado cacheado en memoria (_DATA_TTL).

    Returns:
        pd.DataFrame con columnas: nombre_corto, GEO_LATITUDE, GEO_LONGITUDE, nivel_5
    """
    cached = _cache.get("gcs_coords_repo")
    if cached is not None:
        return cached.copy()

    import pandas as pd

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
                result = pd.read_excel(p)
                _cache.set("gcs_coords_repo", result, ttl=_DATA_TTL)
                return result.copy()
        except Exception:
            pass

    # Búsqueda recursiva como último recurso
    hits = list(base_dir.rglob("Nombres-Pozo_con_coordenadas.xlsx"))
    if hits:
        result = pd.read_excel(hits[0])
        _cache.set("gcs_coords_repo", result, ttl=_DATA_TTL)
        return result.copy()

    return pd.DataFrame()
