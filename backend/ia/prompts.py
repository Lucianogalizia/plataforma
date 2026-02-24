# ==========================================================
# backend/ia/prompts.py
#
# Construcción del prompt para diagnóstico IA extraído de
# diagnostico_tab.py
#
# Incluye:
#   - Catálogo de problemáticas
#   - Constantes de severidad y estado
#   - Builder del prompt completo (_build_prompt)
# ==========================================================

from __future__ import annotations


# ==========================================================
# Catálogo y constantes
# ==========================================================

CATALOGO_PROBLEMATICAS = [
    "Llenado bajo de bomba",
    "Golpeo de fondo",
    "Fuga en válvula viajera",
    "Fuga en válvula fija",
    "Interferencia de fluido",
    "Bomba asentada parcialmente",
    "Gas en bomba",
    "Desbalance de contrapesos",
    "Sobrecarga estructural",
    "Subcarrera / carrera insuficiente",
    "Desgaste de bomba",
    "Sumergencia crítica",
    "Tendencia de declinación de caudal",
    "Rotura / desgaste de varillas",
    "Exceso de fricción en varillas",
]

SEVERIDAD_ORDEN = {
    "CRÍTICA": 0,
    "ALTA":    1,
    "MEDIA":   2,
    "BAJA":    3,
}

SEVERIDAD_COLOR = {
    "BAJA":    "#28a745",
    "MEDIA":   "#ffc107",
    "ALTA":    "#fd7e14",
    "CRÍTICA": "#dc3545",
}

SEVERIDAD_EMOJI = {
    "BAJA":    "🟢",
    "MEDIA":   "🟡",
    "ALTA":    "🟠",
    "CRÍTICA": "🔴",
}

ESTADO_EMOJI = {
    "ACTIVA":   "⚠️",
    "RESUELTA": "✅",
}

ESTADO_COLOR = {
    "ACTIVA":   "#dc3545",
    "RESUELTA": "#28a745",
}

# Versión del schema de diagnóstico.
# Si el JSON cacheado en GCS tiene versión menor, se regenera automáticamente.
DIAG_SCHEMA_VERSION = 11


# ==========================================================
# Builder del prompt
# ==========================================================

def build_prompt(no_key: str, mediciones: list[dict]) -> str:
    """
    Construye el prompt completo para enviar a OpenAI.

    Cada elemento de mediciones tiene la estructura:
        {
            "fecha":    str,
            "path":     str,
            "vars":     dict,   # salida de extract_variables_from_parsed()
            "cs_shape": str,    # salida de describe_cs_shape()
        }

    El prompt instruye al modelo a:
        - Analizar la carta dinamométrica de superficie
        - Distinguir fill_ratio (geometría) de llenado de bomba (CA)
        - Detectar rulo real (cruce de ramas)
        - Clasificar problemáticas por severidad y estado
        - Devolver JSON estructurado con una entrada por medición

    Args:
        no_key:     identificador normalizado del pozo
        mediciones: lista de mediciones (máx. 3, de más antigua a más reciente)

    Returns:
        String con el prompt completo listo para enviar a la API.
    """
    from core.parsers import safe_to_float

    catalogo_str  = "\n".join(f"  - {p}" for p in CATALOGO_PROBLEMATICAS)
    lineas_med    = []
    vars_primera  = None
    fechas_labels = []

    for i, m in enumerate(mediciones):
        label = (
            "Única medición"
            if len(mediciones) == 1
            else ["Más antigua", "Intermedia", "Más reciente"][min(i, 2)]
        )
        fechas_labels.append({"label": label, "fecha": m["fecha"]})
        lineas_med.append(f"\n### [{label}] Fecha: {m['fecha']}")

        v = m["vars"]
        if vars_primera is None:
            vars_primera = v

        # --- Descripción de sumergencia con clasificación ---
        sumer    = v.get("Sumergencia_m")
        base_sum = v.get("Base_sumergencia") or "N/D"

        if sumer is None:
            sumer_str = "N/D (sin nivel — NO inferir problemas de sumergencia)"
        elif sumer < 0:
            sumer_str = f"{sumer} m ({base_sum}) — NEGATIVO: dato inconsistente"
        elif sumer < 50:
            sumer_str = f"{sumer} m ({base_sum}) — CRÍTICA (<50m)"
        elif sumer < 150:
            sumer_str = f"{sumer} m ({base_sum}) — BAJA (50-150m)"
        elif sumer < 400:
            sumer_str = f"{sumer} m ({base_sum}) — NORMAL (150-400m)"
        else:
            sumer_str = f"{sumer} m ({base_sum}) — ALTA (>400m)"

        # --- Líneas de datos de la medición ---
        lineas_med.append(
            f"  Tipo AIB: {v.get('Tipo_AIB') or 'N/D'} | "
            f"Carrera: {v.get('Carrera_pulg') or 'N/D'} pulg | "
            f"Golpes/min: {v.get('Golpes_min') or 'N/D'} | "
            f"Sentido giro: {v.get('Sentido_giro') or 'N/D'}"
        )
        lineas_med.append(
            f"  Motor: {v.get('Potencia_motor') or 'N/D'} HP | "
            f"RPM: {v.get('RPM_motor') or 'N/D'} | "
            f"Polea: {v.get('Polea_motor') or 'N/D'}"
        )
        lineas_med.append(
            f"  Bomba: Ø pistón {v.get('Diam_piston_pulg') or 'N/D'} pulg | "
            f"Prof bomba: {v.get('Prof_bomba_m') or 'N/D'} m | "
            f"Llenado de bomba (CA): {v.get('Llenado_pct') or 'N/D'}%"
        )
        lineas_med.append(
            f"  Niveles → PE: {v.get('PE_m') or 'N/D'} m | "
            f"PB: {v.get('PB_m') or 'N/D'} m | "
            f"NM: {v.get('NM_m') or 'N/D'} m | "
            f"NC: {v.get('NC_m') or 'N/D'} m | "
            f"ND: {v.get('ND_m') or 'N/D'} m"
        )
        lineas_med.append(f"  Sumergencia: {sumer_str}")
        lineas_med.append(
            f"  Contrapeso actual: {v.get('Contrapeso_actual') or 'N/D'} | "
            f"ideal: {v.get('Contrapeso_ideal') or 'N/D'} | "
            f"%Balance: {v.get('Pct_balance') or 'N/D'} | "
            f"%Estructura: {v.get('Pct_estructura') or 'N/D'} | "
            f"Torque máx: {v.get('Torque_max') or 'N/D'}"
        )
        lineas_med.append(
            f"  Caudal bruto efec: {v.get('Caudal_bruto') or 'N/D'} m³/día"
        )
        lineas_med.append(f"  Carta dinámica [CS]: {m['cs_shape']}")

        # --- Comparación con la medición más antigua ---
        if i > 0 and vars_primera:
            campos = [
                ("Carrera_pulg",     "Carrera"),
                ("Golpes_min",       "Golpes/min"),
                ("Diam_piston_pulg", "Ø pistón"),
                ("Prof_bomba_m",     "Prof bomba"),
                ("Llenado_pct",      "Llenado %"),
                ("Sumergencia_m",    "Sumergencia"),
                ("Pct_balance",      "%Balance"),
                ("Pct_estructura",   "%Estructura"),
                ("Caudal_bruto",     "Caudal bruto"),
                ("Torque_max",       "Torque máx"),
            ]
            diffs = []
            for key, lbl in campos:
                v0 = safe_to_float(vars_primera.get(key))
                v1 = safe_to_float(v.get(key))
                if v0 is not None and v1 is not None:
                    delta = round(v1 - v0, 2)
                    sign  = "+" if delta >= 0 else ""
                    diffs.append(f"{lbl}: {v0}→{v1} ({sign}{delta})")
                elif not (v0 is None and v1 is None):
                    diffs.append(f"{lbl}: {v0 or 'N/D'}→{v1 or 'N/D'}")
            if diffs:
                lineas_med.append(
                    f"  ↳ Cambios vs más antigua: {' | '.join(diffs)}"
                )

    # --- Variables sin cambio entre mediciones ---
    if len(mediciones) > 1:
        campos_config = [
            ("Carrera_pulg",    "Carrera"),
            ("Golpes_min",      "Golpes/min"),
            ("Diam_piston_pulg","Ø pistón"),
            ("Prof_bomba_m",    "Prof bomba"),
            ("Tipo_AIB",        "Tipo AIB"),
            ("Potencia_motor",  "Potencia motor"),
        ]
        sin_cambio = []
        for key, lbl in campos_config:
            vals    = [m["vars"].get(key) for m in mediciones]
            vals_ok = [x for x in vals if x is not None]
            if (
                len(vals_ok) == len(mediciones)
                and all(str(x) == str(vals_ok[0]) for x in vals_ok)
            ):
                sin_cambio.append(f"{lbl}={vals_ok[0]}")
        sin_cambio_str = (
            ", ".join(sin_cambio) if sin_cambio
            else "No determinado"
        )
    else:
        sin_cambio_str = (
            "Solo hay una medición, no aplica comparación temporal."
        )

    n_med = len(mediciones)

    # --- Schema de fechas para el JSON de respuesta ---
    fechas_schema = "\n".join(
        f'    {{"fecha": "{fl["fecha"]}", "label": "{fl["label"]}"}}'
        for fl in fechas_labels
    )

    # ==========================================================
    # PROMPT COMPLETO
    # ==========================================================
    prompt = f"""Eres un ingeniero senior experto en operaciones de pozos petroleros con bombeo mecánico (Rod Pump / Varillado).

Vas a analizar el historial dinamométrico del pozo **{no_key}** y producir un diagnóstico técnico estructurado en JSON.

---
## HISTORIAL DE MEDICIONES ({n_med} DINs, de más antiguo a más reciente)

{"".join(lineas_med)}

---
## VARIABLES SIN CAMBIO entre todas las mediciones
{sin_cambio_str}

---
## INSTRUCCIONES DE ANÁLISIS

### ⚠️ DISTINCIÓN CRÍTICA: fill_ratio vs llenado de bomba

**fill_ratio** y **llenado de bomba** son dos variables COMPLETAMENTE DISTINTAS:
- **Llenado de bomba (CA)**: porcentaje real de llenado calculado por DINA. >80% = bomba llena bien. <60% = llenado bajo problemático.
- **fill_ratio**: compacidad geométrica de la carta (área / rectángulo contenedor). NO mide llenado de bomba.
- **REGLA**: Para diagnosticar "Llenado bajo de bomba" usá ÚNICAMENTE el campo CA. Si CA >75%, NO reportes llenado bajo aunque fill_ratio sea bajo.

### Cómo interpretar la Carta Dinámica [CS]

**⚠️ CARTA DEGENERADA — prioridad máxima:**
- Si la carta dinámica contiene `CARTA_DEGENERADA=True`, la medición NO ES INTERPRETABLE. En este caso:
  1. El campo `resumen` debe explicar técnicamente por qué la carta es degenerada (señal ruidosa, rango de carga mínimo, etc.)
  2. El array `problemáticas` de esta medición debe contener UNA SOLA entrada: {{"nombre": "Carta no interpretable", "severidad": "ALTA", "estado": "ACTIVA", "descripcion": "<explicación del motivo técnico>"}}
  3. La `recomendacion` global debe indicar repetir la medición DIN en mejores condiciones operativas.
  4. NO inferir ninguna otra problemática de una carta degenerada.

**Golpe de fondo / golpe de bomba — RULO (CRUCE DE RAMAS):**
El rulo verdadero ocurre cuando las ramas ascendente y descendente de la carta SE CRUZAN, formando un lazo o bucle cerrado separado del cuerpo principal — visible como un "círculo" o "loop" en la zona izquierda (inicio/fin de carrera). NO es simplemente una variación en la bajada.

- **rulo_detectado=True**: las ramas ascendente y descendente se cruzan — la subida queda POR ENCIMA de la bajada en alguna zona de X solapado. Este es el único criterio válido para reportar "Golpeo de fondo". Si `rulo_detectado=False`, NO reportar golpeo de fondo aunque haya variaciones en la bajada.
- **rulo_amplitud**: magnitud del cruce (diferencia Y_subida - Y_bajada en el punto de máximo cruce). >15% del rango_carga = severidad ALTA. 8-15% = MEDIA. 5-8% = BAJA.
- **rulo_pos_en_carrera**: posición X donde ocurre el cruce máximo. Típicamente <20% de la carrera (zona izquierda = inicio/fin de carrera).

**Cuestionamiento del llenado declarado (CA):**
- **ratio_carga_min_max**: relación entre la carga mínima y la carga máxima de la carta.
  - >0.70 → coherente con llenado alto, la bomba sostiene bien la carga.
  - 0.55-0.70 → zona gris: posible gas, llenado sobreestimado o interferencia.
  - <0.55 → la carta geométricamente sugiere llenado real menor al declarado por CA. Si el CA dice >75% pero ratio_carga_min_max <0.55, reportar "Llenado sobreestimado — discrepancia carta vs CA" como problemática MEDIA, explicando que la carta no sostiene la carga esperada para ese nivel de llenado.
- **panza_extendida=True**: la rama inferior de la carta tiene un tramo horizontal prolongado donde la carga casi no varía. Es señal de gas en bomba, interferencia de fluido o llenado real bajo aunque el CA sea alto. Si `panza_extendida=True` con CA >75%, cuestionar el llenado declarado y agregar "Gas en bomba" o "Interferencia de fluido" como posible problemática.

**Otras métricas:**
- **subida_brusca=True**: carga sube muy abruptamente al inicio de la carrera ascendente → posible golpeo hidráulico o apertura violenta de válvula viajera.
- **bajada_lenta_posible_fuga_fija=True**: carga no cae suficiente al final de la bajada → sospecha fuga válvula fija.
- **forma muy_delgada** con buen llenado CA → puede indicar gas libre o interferencia de fluido.
- **area**: si cae entre mediciones con misma carrera y golpes/min → pérdida de eficiencia.
- **pos_carga_max**: pico muy temprano (<15%) con subida_brusca → confirma golpeo hidráulico.

### Cómo interpretar la Sumergencia
La sumergencia viene con clasificación en los datos:
- **N/D** → NO inferir problemas de sumergencia.
- **CRÍTICA (<50m)** → riesgo real de ingesta de gas y golpeo.
- **BAJA (50-150m)** → nivel bajo, monitorear.
- **NORMAL (150-400m)** → operación estándar.
- **ALTA (>400m)** → posible sobredimensionamiento.

### Estados de problemática
- **ACTIVA**: presente en la medición que se analiza.
- **RESUELTA**: estaba en mediciones anteriores pero ya no está en esta medición.

### Variables sin cambio como clave diagnóstica
Si Ø pistón, carrera y golpes/min no cambiaron pero el llenado bajó y la sumergencia subió → problema del yacimiento o bomba, no del ajuste operativo.

### Catálogo base (podés agregar nuevas si las detectás):
{catalogo_str}

---
## FORMATO DE RESPUESTA

**IMPORTANTE**: el JSON debe tener una entrada en `mediciones` por CADA DIN analizado, con sus problemáticas propias.
Respondé ÚNICAMENTE con un JSON válido, sin texto adicional ni markdown:

{{
  "pozo": "{no_key}",
  "fecha_analisis": "<fecha ISO de hoy>",
  "resumen": "<párrafo de 4-6 oraciones describiendo la evolución global del pozo a través de todas las mediciones: qué cambió, qué se mantuvo estable, conclusión técnica general>",
  "variables_sin_cambio": "<variables operativas que no cambiaron entre mediciones, o N/A si hay una sola>",
  "recomendacion": "<acción concreta recomendada para el próximo paso operativo>",
  "confianza": "<ALTA=3 DINs completos | MEDIA=2 DINs o datos parciales | BAJA=1 DIN o muchos N/D>",
  "mediciones": [
{fechas_schema}
    // REEMPLAZAR CADA ENTRADA CON:
    {{
      "fecha": "<fecha exacta del DIN>",
      "label": "<Más antigua|Intermedia|Más reciente|Única medición>",
      "llenado_pct": <número o null>,
      "sumergencia_m": <número o null>,
      "sumergencia_nivel": "<CRÍTICA|BAJA|NORMAL|ALTA|N/D>",
      "caudal_bruto": <número o null>,
      "pct_balance": <número o null>,
      "problemáticas": [
        {{
          "nombre": "<nombre>",
          "severidad": "<BAJA|MEDIA|ALTA|CRÍTICA>",
          "estado": "<ACTIVA|RESUELTA>",
          "descripcion": "<2-3 oraciones: evidencia concreta en ESTA medición>"
        }}
      ]
    }}
  ]
}}
"""
    return prompt
