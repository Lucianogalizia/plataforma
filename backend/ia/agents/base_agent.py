# =============================================================
# backend/ia/agents/base_agent.py
# Clase base para todos los agentes especializados de DINA.
# =============================================================
from __future__ import annotations
import json, io, math
from typing import Any
import pandas as pd


# ─── Helpers compartidos ──────────────────────────────────────────────────────

def buscar_pozo_fuzzy(pozo_raw: str) -> tuple[str | None, str | None]:
    """Busca pozo por nombre aproximado. Retorna (no_key, error_msg)."""
    try:
        from api.diagnosticos import _load_din_niv_ok
        from core.parsers import normalize_no_exact
        no_key_exact = normalize_no_exact(pozo_raw)
        din_ok, _ = _load_din_niv_ok()
        if din_ok is None or din_ok.empty:
            return no_key_exact, None
        todos = din_ok["NO_key"].dropna().unique().tolist()
        busqueda = pozo_raw.upper().replace(" ", "")
        candidatos = [p for p in todos if busqueda in p.upper().replace(" ", "")]
        if len(candidatos) == 1:
            return candidatos[0], None
        elif len(candidatos) > 1:
            return None, f"'{pozo_raw}' coincide con varios pozos: {candidatos[:8]}. Indicá el nombre completo."
        return no_key_exact, None
    except Exception:
        from core.parsers import normalize_no_exact
        return normalize_no_exact(pozo_raw), None


def read_csv_gcs(blob_path: str) -> pd.DataFrame | None:
    """Lee un CSV desde GCS. Devuelve None si no existe."""
    try:
        from core.gcs import get_gcs_client, GCS_BUCKET, GCS_PREFIX
        client = get_gcs_client()
        if not client or not GCS_BUCKET:
            return None
        full = f"{GCS_PREFIX}/{blob_path}" if GCS_PREFIX else blob_path
        blob = client.bucket(GCS_BUCKET).blob(full)
        if not blob.exists():
            return None
        return pd.read_csv(io.BytesIO(blob.download_as_bytes()), low_memory=False)
    except Exception:
        return None


def clean_records(df: pd.DataFrame, limit: int = 50) -> list[dict]:
    """Convierte DataFrame a lista de dicts limpia (sin NaN/Inf)."""
    df = df.where(df.notna(), other=None)
    return [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in r.items()}
        for r in df.head(limit).to_dict(orient="records")
    ]


# ─── Clase base ───────────────────────────────────────────────────────────────

class BaseAgent:
    """
    Subclases deben definir: TOOLS, SYSTEM_PROMPT, _ejecutar_tool().
    """
    TOOLS: list[dict] = []
    SYSTEM_PROMPT: str = ""
    NOMBRE: str = "agente"

    def consultar(self, mensaje: str, historial: list[dict] | None = None) -> dict:
        """
        Ejecuta el loop de tool-use y retorna
        {"respuesta": str, "tools_usadas": list[str]}.
        """
        from ia.diagnostico import get_openai_key
        from openai import OpenAI

        api_key = get_openai_key()
        if not api_key:
            return {"respuesta": "⚠️ No hay clave OpenAI configurada.", "tools_usadas": []}

        client = OpenAI(api_key=api_key)
        messages = [{"role": "system", "content": self.SYSTEM_PROMPT}]
        for msg in (historial or [])[-6:]:
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                messages.append({"role": msg["role"], "content": msg["content"]})
        messages.append({"role": "user", "content": mensaje})

        tools_usadas: list[str] = []

        resp = client.chat.completions.create(
            model="gpt-4o-mini", messages=messages, tools=self.TOOLS,
            tool_choice="auto", max_tokens=1200, temperature=0,
        )
        msg_resp = resp.choices[0].message

        # Loop de tool-use (máx 3 rondas)
        for _ in range(3):
            if not msg_resp.tool_calls:
                break
            messages.append(msg_resp)
            for tc in msg_resp.tool_calls:
                nombre_tool = tc.function.name
                tools_usadas.append(nombre_tool)
                try:
                    targs = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    targs = {}
                resultado = self._ejecutar_tool(nombre_tool, targs)
                messages.append({
                    "role": "tool", "tool_call_id": tc.id,
                    "content": json.dumps(resultado, ensure_ascii=False, default=str),
                })
            resp = client.chat.completions.create(
                model="gpt-4o-mini", messages=messages, tools=self.TOOLS,
                tool_choice="auto", max_tokens=1200, temperature=0,
            )
            msg_resp = resp.choices[0].message

        return {"respuesta": (msg_resp.content or "").strip(), "tools_usadas": tools_usadas}

    def _ejecutar_tool(self, nombre: str, args: dict) -> Any:
        return {"error": f"Tool desconocida en {self.NOMBRE}: {nombre}"}
