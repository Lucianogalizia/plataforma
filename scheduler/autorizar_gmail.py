#!/usr/bin/env python3
# ==========================================================
# ingest/autorizar_gmail.py
#
# Script de autorización OAuth2 — se corre UNA SOLA VEZ
# en tu computadora para generar el token.json.
#
# Pasos:
#   1. Colocar el credentials.json descargado de Google Cloud
#      en la misma carpeta que este script.
#   2. Correr:  python autorizar_gmail.py
#   3. Se abre el navegador → loguearse con ia.clearpetroleum@gmail.com
#   4. Aceptar los permisos solicitados
#   5. Se genera token.json en la misma carpeta
#   6. Copiar el CONTENIDO de token.json como secret GMAIL_TOKEN_JSON en GitHub
#
# Después de esto, el token.json no se necesita más en la máquina.
# El refresh token dentro de él renueva el access token automáticamente.
# ==========================================================

import json
import os
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]

HERE = Path(__file__).parent


def main():
    creds_path = HERE / "credentials.json"
    token_path = HERE / "token.json"

    if not creds_path.exists():
        print()
        print("❌  No se encontró credentials.json")
        print()
        print("   Pasos para obtenerlo:")
        print("   1. Ir a https://console.cloud.google.com")
        print("   2. Crear o seleccionar un proyecto")
        print("   3. Activar la Gmail API:")
        print("      Menú → APIs y Servicios → Biblioteca → Gmail API → Habilitar")
        print("   4. Crear credenciales OAuth2:")
        print("      Menú → APIs y Servicios → Credenciales → Crear credenciales")
        print("      → ID de cliente OAuth 2.0")
        print("      → Tipo: Aplicación de escritorio")
        print("      → Nombre: ingest-dina (o el que quieras)")
        print("   5. Descargar el JSON y guardarlo como 'credentials.json'")
        print("      en la misma carpeta que este script")
        print()
        print("   ⚠️  IMPORTANTE: antes de poder autorizar, tenés que agregar")
        print("      ia.clearpetroleum@gmail.com como usuario de prueba en:")
        print("      APIs y Servicios → Pantalla de consentimiento OAuth")
        print("      → Usuarios de prueba → Agregar usuarios")
        print()
        return

    print()
    print("🔐  Iniciando autorización OAuth2 con Google...")
    print("    Se va a abrir el navegador.")
    print("    Logueate con ia.clearpetroleum@gmail.com y aceptá los permisos.")
    print()

    flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
    creds = flow.run_local_server(port=0, open_browser=True)

    # Serializar el token en el formato que espera main.py
    token_data = {
        "token":         creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri":     creds.token_uri,
        "client_id":     creds.client_id,
        "client_secret": creds.client_secret,
        "scopes":        list(creds.scopes),
    }

    token_path.write_text(json.dumps(token_data, indent=2), encoding="utf-8")

    print()
    print("✅  Autorización exitosa!")
    print(f"   Token guardado en: {token_path}")
    print()
    print("   Próximo paso: copiar el CONTENIDO de token.json como")
    print("   secret GMAIL_TOKEN_JSON en GitHub:")
    print()
    print("   GitHub → tu repo → Settings → Secrets and variables")
    print("   → Actions → New repository secret")
    print("   → Nombre: GMAIL_TOKEN_JSON")
    print("   → Valor: (pegá todo el contenido del archivo token.json)")
    print()
    print("   Contenido del token.json:")
    print("   " + "-" * 50)
    print(token_path.read_text(encoding="utf-8"))
    print("   " + "-" * 50)
    print()
    print("⚠️   Después de cargarlo en GitHub, podés borrar token.json")
    print("    de tu computadora. NO lo subas al repo.")
    print()


if __name__ == "__main__":
    main()
