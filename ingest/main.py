#!/usr/bin/env python3
# ==========================================================
# ingest/main.py
#
# Script de ingestión diaria para cuenta Gmail personal.
# Usa OAuth2 (refresh token) en lugar de Service Account.
#
# Variables de entorno requeridas:
#   GMAIL_CREDENTIALS_JSON   → contenido del credentials.json de Google Cloud
#   GMAIL_TOKEN_JSON         → contenido del token.json generado por autorizar_gmail.py
#   GMAIL_SENDER_ESPERADO    → email del remitente del parte (puede ser el mismo u otro)
#   GMAIL_ASUNTO_CONTIENE    → texto del asunto para filtrar (opcional)
#   GCS_SERVICE_ACCOUNT_JSON → service account con permisos en GCS
#   DINAS_BUCKET             → bucket GCS de la plataforma
#   DINAS_GCS_PREFIX         → prefijo dentro del bucket (ej: interfaz_dinas)
#   DINA_API_URL             → URL base del backend FastAPI
#   DINA_API_TOKEN           → token Bearer del backend (opcional)
# ==========================================================

from __future__ import annotations

import base64
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import storage

# ── Logging ───────────────────────────────────────────────

LOG_FILE = Path(__file__).parent / "ingest.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("ingest")


def _env(key: str, required: bool = True) -> str:
    v = os.environ.get(key, "").strip()
    if required and not v:
        log.error(f"Variable de entorno requerida no encontrada: {key}")
        sys.exit(1)
    return v


# ── Gmail OAuth2 ──────────────────────────────────────────

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]


def _get_gmail_service():
    """
    Conecta a Gmail con OAuth2 (refresh token).
    El access token se renueva automáticamente si venció.
    """
    token_str  = _env("GMAIL_TOKEN_JSON")
    token_data = json.loads(token_str)

    creds = Credentials(
        token         = token_data.get("token"),
        refresh_token = token_data.get("refresh_token"),
        token_uri     = token_data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id     = token_data.get("client_id"),
        client_secret = token_data.get("client_secret"),
        scopes        = token_data.get("scopes", GMAIL_SCOPES),
    )

    # Renovar si venció (el refresh_token no vence nunca)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            log.info("  Renovando access token...")
            creds.refresh(Request())
            _actualizar_secret_github(creds)
        else:
            log.error("  Token inválido. Correr autorizar_gmail.py de nuevo.")
            sys.exit(1)

    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _actualizar_secret_github(creds: Credentials) -> None:
    """
    Actualiza el secret GMAIL_TOKEN_JSON en GitHub con el access token renovado.
    Requiere que el workflow tenga permisos 'secrets: write' y pase GITHUB_TOKEN.
    Sin esto igual funciona — solo hay que tolerar que cada ~1h se renueve en memoria.
    """
    github_token = os.environ.get("GITHUB_TOKEN", "").strip()
    github_repo  = os.environ.get("GITHUB_REPOSITORY", "").strip()

    if not github_token or not github_repo:
        return

    try:
        nuevo_token = json.dumps({
            "token":         creds.token,
            "refresh_token": creds.refresh_token,
            "token_uri":     creds.token_uri,
            "client_id":     creds.client_id,
            "client_secret": creds.client_secret,
            "scopes":        list(creds.scopes or GMAIL_SCOPES),
        })

        headers = {
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        api = f"https://api.github.com/repos/{github_repo}"

        pk   = requests.get(f"{api}/actions/secrets/public-key", headers=headers, timeout=10).json()
        from base64 import b64encode
        from nacl import encoding, public  # type: ignore
        box  = public.SealedBox(public.PublicKey(pk["key"].encode(), encoding.Base64Encoder()))
        enc  = b64encode(box.encrypt(nuevo_token.encode())).decode()
        requests.put(f"{api}/actions/secrets/GMAIL_TOKEN_JSON",
                     headers=headers,
                     json={"encrypted_value": enc, "key_id": pk["key_id"]},
                     timeout=10).raise_for_status()
        log.info("  ✅ Secret GMAIL_TOKEN_JSON actualizado en GitHub")
    except ImportError:
        log.warning("  PyNaCl no disponible — token renovado en memoria solamente")
    except Exception as e:
        log.warning(f"  No se pudo actualizar secret en GitHub: {e}")


# ── Label ─────────────────────────────────────────────────

LABEL_PROCESADO = "PROCESADO_INGEST"


def _get_or_create_label(service) -> str:
    resp = service.users().labels().list(userId="me").execute()
    for lbl in resp.get("labels", []):
        if lbl["name"].lower() == LABEL_PROCESADO.lower():
            return lbl["id"]
    new = service.users().labels().create(
        userId="me",
        body={"name": LABEL_PROCESADO, "labelListVisibility": "labelShow", "messageListVisibility": "show"},
    ).execute()
    log.info(f"Label creado: {new['id']}")
    return new["id"]


# ── Buscar mails ──────────────────────────────────────────

def buscar_mails_nuevos(service) -> list[dict]:
    sender = _env("GMAIL_SENDER_ESPERADO")
    asunto = _env("GMAIL_ASUNTO_CONTIENE", required=False)

    query_parts = [f"from:{sender}", f"-label:{LABEL_PROCESADO}"]
    if asunto:
        query_parts.append(f'subject:"{asunto}"')

    query = " ".join(query_parts)
    log.info(f"Query Gmail: {query}")

    result   = service.users().messages().list(userId="me", q=query, maxResults=10).execute()
    mensajes = result.get("messages", [])
    log.info(f"Mails encontrados: {len(mensajes)}")
    return mensajes


# ── Descargar adjunto ─────────────────────────────────────

def descargar_adjunto_excel(service, msg_id: str) -> tuple[bytes, str] | None:
    msg   = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    parts = msg.get("payload", {}).get("parts", [])

    for part in parts:
        fn = part.get("filename", "")
        if not fn or Path(fn).suffix.lower() not in (".xlsx", ".xls", ".xlsm"):
            continue
        body = part.get("body", {})
        att_id = body.get("attachmentId")
        if att_id:
            att  = service.users().messages().attachments().get(
                userId="me", messageId=msg_id, id=att_id).execute()
            data = base64.urlsafe_b64decode(att["data"])
        elif body.get("data"):
            data = base64.urlsafe_b64decode(body["data"])
        else:
            continue
        log.info(f"  Adjunto: {fn} ({len(data):,} bytes)")
        return data, fn

    log.warning(f"  Sin adjunto Excel en {msg_id}")
    return None


# ── Procesar Excel ────────────────────────────────────────

def procesar_excel(data: bytes, filename: str) -> pd.DataFrame:
    from parser_parte_diario import parsear_parte_diario

    ext = Path(filename).suffix.lower()
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name

    try:
        df = parsear_parte_diario(tmp_path)
    finally:
        os.unlink(tmp_path)

    log.info(f"  Actividades: {len(df)}")
    if not df.empty:
        log.info(f"  Pozo={df['well_legal_name'].iloc[0]}  "
                 f"Evento={df['event_id'].iloc[0]}  "
                 f"Status={df['status_end'].iloc[0]}")
    return df


# ── GCS ───────────────────────────────────────────────────

def subir_a_gcs(df: pd.DataFrame, nombre_destino: str) -> str:
    sa_info     = json.loads(_env("GCS_SERVICE_ACCOUNT_JSON"))
    creds_gcs   = service_account.Credentials.from_service_account_info(sa_info)
    client      = storage.Client(credentials=creds_gcs, project=sa_info.get("project_id"))

    bucket_name = _env("DINAS_BUCKET")
    prefix      = _env("DINAS_GCS_PREFIX", required=False)
    blob_name   = f"{prefix}/{nombre_destino}" if prefix else nombre_destino

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    client.bucket(bucket_name).blob(blob_name).upload_from_string(csv_bytes, content_type="text/csv")

    url = f"gs://{bucket_name}/{blob_name}"
    log.info(f"  ✅ Subido: {url}")
    return url


# ── Invalidar caché ───────────────────────────────────────

def invalidar_cache_backend() -> None:
    api_url = _env("DINA_API_URL", required=False)
    if not api_url:
        return
    token   = _env("DINA_API_TOKEN", required=False)
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        r = requests.post(f"{api_url.rstrip('/')}/api/mi-modulo/reload", headers=headers, timeout=30)
        log.info(f"  Caché: {r.status_code}")
    except Exception as e:
        log.warning(f"  Caché error: {e}")


# ── Marcar procesado ──────────────────────────────────────

def marcar_como_procesado(service, msg_id: str, label_id: str) -> None:
    service.users().messages().modify(
        userId="me", id=msg_id, body={"addLabelIds": [label_id]}
    ).execute()
    log.info("  ✅ Marcado como procesado")


def nombre_destino_gcs(filename: str) -> str:
    hoy  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"parte_{Path(filename).stem}_{hoy}.csv"


# ── Main ──────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info(f"Ingestión — {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 60)

    service  = _get_gmail_service()
    label_id = _get_or_create_label(service)
    mensajes = buscar_mails_nuevos(service)

    if not mensajes:
        log.info("Sin mails nuevos. ✅")
        return

    procesados = errores = 0

    for meta in mensajes:
        msg_id = meta["id"]
        log.info(f"\n── Mensaje {msg_id} ──")
        try:
            resultado = descargar_adjunto_excel(service, msg_id)
            if resultado is None:
                marcar_como_procesado(service, msg_id, label_id)
                continue

            excel_bytes, filename = resultado
            df = procesar_excel(excel_bytes, filename)

            if df.empty:
                log.warning("  DataFrame vacío — saltando")
                errores += 1
                continue

            gs_url = subir_a_gcs(df, nombre_destino_gcs(filename))
            invalidar_cache_backend()
            marcar_como_procesado(service, msg_id, label_id)
            procesados += 1
            log.info(f"  ✅ OK → {gs_url}")

        except Exception as e:
            log.exception(f"  ❌ Error: {e}")
            errores += 1

    log.info(f"\nResultado: {procesados} OK, {errores} errores")
    if errores:
        sys.exit(1)


if __name__ == "__main__":
    main()
