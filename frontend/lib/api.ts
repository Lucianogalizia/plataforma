// ==========================================================
// lib/api.ts
// Cliente centralizado para consumir el backend FastAPI
// ==========================================================

const API_URL =
  process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// ---------- Helper base ----------

async function apiFetch<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options?.headers || {}),
    },
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => res.statusText);
    throw new Error(`API error ${res.status}: ${detail}`);
  }
  return res.json() as Promise<T>;
}

// ==========================================================
// CACHE GLOBAL (solo para GET)
// - Reutiliza respuestas entre pestañas (mientras no recargues el navegador)
// - TTL configurable
// - Deduplica requests simultáneos (misma URL -> 1 solo fetch)
// ==========================================================

type CacheEntry = { ts: number; data: unknown };
const GET_CACHE = new Map<string, CacheEntry>();
const INFLIGHT = new Map<string, Promise<unknown>>();

function cacheKey(path: string) {
  // path ya incluye querystring (si aplica)
  return path;
}

async function apiGetCached<T>(path: string, ttlMs = 3 * 60 * 1000): Promise<T> {
  const key = cacheKey(path);
  const now = Date.now();

  const hit = GET_CACHE.get(key);
  if (hit && now - hit.ts < ttlMs) return hit.data as T;

  const inflight = INFLIGHT.get(key);
  if (inflight) return inflight as Promise<T>;

  const p: Promise<T> = (async () => {
    const data = await apiFetch<T>(path);
    GET_CACHE.set(key, { ts: Date.now(), data });
    return data;
  })().finally(() => {
    INFLIGHT.delete(key);
  });

  INFLIGHT.set(key, p as Promise<unknown>);
  return p;
}

export function clearApiCache(prefix?: string) {
  if (!prefix) return GET_CACHE.clear();
  for (const k of GET_CACHE.keys()) if (k.startsWith(prefix)) GET_CACHE.delete(k);
}

// ==========================================================
// TIPOS
// ==========================================================

export interface Pozo {
  pozos: string[];
  total: number;
}

export interface OpcionDin {
  id: string;
  label: string;
}

export interface Medicion {
  ORIGEN: string;
  pozo: string;
  Batería?: string;
  fecha?: string;
  hora?: string;
  din_datetime?: string;
  niv_datetime?: string;
  DT_plot?: string;
  Sumergencia?: number | null;
  Sumergencia_base?: string;
  PB?: number | null;
  NM?: number | null;
  NC?: number | null;
  ND?: number | null;
  PE?: number | null;
  "AIB Carrera"?: number | null;
  "Sentido giro"?: string;
  "Tipo Contrapesos"?: string;
  "Distancia contrapesos (cm)"?: number | null;
  "Contrapeso actual"?: number | null;
  "Contrapeso ideal"?: number | null;
  "AIBEB_Torque max contrapeso"?: number | null;
  "%Estructura"?: number | null;
  "%Balance"?: number | null;
  "Bba Diam Pistón"?: number | null;
  "Bba Prof"?: number | null;
  "Bba Llenado"?: number | null;
  GPM?: number | null;
  "Caudal bruto efec"?: number | null;
  "Polea Motor"?: number | null;
  "Potencia Motor"?: number | null;
  "RPM Motor"?: number | null;
  path?: string;
  [key: string]: unknown;
}

export interface PuntoCS {
  i: number;
  X: number;
  Y: number;
}

export interface SerieSumergencia {
  dt: string;
  sumergencia: number;
  base: string | null;
  pb: number | null;
  nivel_usado: number | null;
  origen: string;
}

export interface SnapRow {
  NO_key: string;
  Bateria?: string;
  "Tipo AIB"?: string;
  ORIGEN?: string;
  SE?: string;
  DT_plot?: string;
  Dias_desde_ultima?: number;
  PE?: number | null;
  PB?: number | null;
  NM?: number | null;
  NC?: number | null;
  ND?: number | null;
  Sumergencia?: number | null;
  Sumergencia_base?: string;
  "AIB Carrera"?: number | null;
  "Sentido giro"?: string;
  "Tipo Contrapesos"?: string;
  "Distancia contrapesos (cm)"?: number | null;
  "Contrapeso actual"?: number | null;
  "Contrapeso ideal"?: number | null;
  "AIBEB_Torque max contrapeso"?: number | null;
  "Bba Diam Pistón"?: number | null;
  "Bba Llenado"?: number | null;
  "Caudal bruto efec"?: number | null;
  "Polea Motor"?: number | null;
  "Potencia Motor"?: number | null;
  "RPM Motor"?: number | null;
  GPM?: number | null;
  "%Estructura"?: number | null;
  "%Balance"?: number | null;
  [key: string]: unknown;
}

export interface SnapKpis {
  total_pozos: number;
  ultima_din: number;
  ultima_niv: number;
  con_sumergencia: number;
  con_pb: number;
}

export interface PuntoMapa {
  NO_key: string;
  nivel_5?: string;
  ORIGEN?: string;
  DT_plot_str?: string;
  Sumergencia?: number | null;
  Dias_desde_ultima?: number | null;
  lat: number;
  lon: number;
  PE?: number | null;
  PB?: number | null;
  NM?: number | null;
  NC?: number | null;
  ND?: number | null;
  Sumergencia_base?: string;
}

export interface TendenciaPozo {
  NO_key: string;
  n_puntos: number;
  pendiente_por_mes: number;
  valor_inicial: number;
  valor_final: number;
  delta_total: number;
  fecha_inicial: string;
  fecha_final: string;
}

export interface PozosMes {
  Mes: string;
  Pozos_medidos: number;
}

export interface Cobertura {
  total_pozos: number;
  pozos_con_din: number;
  pozos_sin_din: number;
  lista_sin_din: string[];
}

export interface ValidacionEstado {
  validada: boolean;
  comentario: string;
  historial: {
    timestamp: string;
    usuario: string;
    validada: boolean;
    comentario: string;
  }[];
}

export interface SemaforoRow {
  NO_key: string;
  ORIGEN?: string;
  DT_plot?: string;
  Dias_desde_ultima?: number;
  SE?: string;
  PB?: number | null;
  Sumergencia?: number | null;
  "Bba Llenado"?: number | null;
  Sumergencia_base?: string;
  "%Estructura"?: number | null;
  "%Balance"?: number | null;
  GPM?: number | null;
  "Caudal bruto efec"?: number | null;
  Semaforo_AIB: string;
}

export interface DiagnosticoMedicion {
  fecha: string;
  label: string;
  llenado_pct: number | null;
  sumergencia_m: number | null;
  sumergencia_nivel: string;
  caudal_bruto: number | null;
  pct_balance: number | null;
  "problemáticas": {
    nombre: string;
    severidad: "BAJA" | "MEDIA" | "ALTA" | "CRÍTICA";
    estado: "ACTIVA" | "RESUELTA";
    descripcion: string;
  }[];
}

export interface Diagnostico {
  pozo: string;
  fecha_analisis: string;
  resumen: string;
  variables_sin_cambio: string;
  recomendacion: string;
  confianza: string;
  mediciones: DiagnosticoMedicion[];
  _meta?: {
    generado_utc: string;
    paths_analizados: string[];
    fecha_din_mas_reciente: string;
    n_mediciones: number;
    schema_version: number;
  };
  error?: string;
}

export interface FilaDiagGlobal {
  Pozo: string;
  "Batería": string;
  "Fecha DIN": string;
  "Medición": string;
  "Llenado %": string;
  "Sumergencia": string;
  "Caudal m³/d": number | string;
  "%Balance": string;
  "Sev. máx": string;
  "Act.": number;
  "Res.": number;
  "Problemáticas": string;
  "Recomendación": string;
  "Confianza": string;
  "Generado": string;
}

export interface FilaValidacion {
  validada: boolean;
  pozo: string;
  bateria: string;
  fecha_medicion: string;
  sumergencia_m: number | null;
  base: string;
  comentario: string;
  usuario: string;
  _no_key: string;
  _fecha_key: string;
  lat?: number | null;
  lon?: number | null;
  Dias_desde_ultima?: number | null;
}
// ==========================================================
// DIN
// ==========================================================

export const api = {
  // ── Pozos ──────────────────────────────────────────────
  getPozos: (soloConDin = false) =>
    apiGetCached<Pozo>(`/api/din/pozos?solo_con_din=${soloConDin}`, 30 * 60 * 1000),

  // ── Mediciones de un pozo ──────────────────────────────
  getMediciones: (pozo: string) =>
    apiGetCached<{ pozo: string; total: number; mediciones: Medicion[]; opciones_din: OpcionDin[] }>(
      `/api/din/mediciones/${encodeURIComponent(pozo)}`
    ),

  // ── Carta Dinamométrica CS ─────────────────────────────
  getCartaSuperficie: (path: string) =>
    apiGetCached<{ n_puntos: number; puntos: PuntoCS[] }>(
      `/api/din/carta-superficie?path=${encodeURIComponent(path)}`
    ),

  // ── Histórico de Sumergencia ───────────────────────────
  getHistoricoSumergencia: (pozo: string) =>
    apiGetCached<{ pozo: string; serie: SerieSumergencia[] }>(
      `/api/din/historico-sumergencia/${encodeURIComponent(pozo)}`
    ),

  // ── Snapshot global ────────────────────────────────────
  getSnapshot: (params?: {
    origen?: string;
    sum_min?: number;
    sum_max?: number;
    est_min?: number;
    est_max?: number;
    bal_min?: number;
    bal_max?: number;
  }) => {
    const qs = new URLSearchParams();
    if (params?.origen)   qs.set("origen",  params.origen);
    if (params?.sum_min != null) qs.set("sum_min", String(params.sum_min));
    if (params?.sum_max != null) qs.set("sum_max", String(params.sum_max));
    if (params?.est_min != null) qs.set("est_min", String(params.est_min));
    if (params?.est_max != null) qs.set("est_max", String(params.est_max));
    if (params?.bal_min != null) qs.set("bal_min", String(params.bal_min));
    if (params?.bal_max != null) qs.set("bal_max", String(params.bal_max));
    return apiGetCached<{ total: number; snap: SnapRow[]; kpis: SnapKpis }>(
      `/api/din/snapshot?${qs}`
    );
  },

  // ── Tendencias ─────────────────────────────────────────
  getTendencias: (params?: {
    variable?: string;
    min_pts?: number;
    solo_positiva?: boolean;
    top?: number;
  }
) => {
    const qs = new URLSearchParams();
    if (params?.variable) qs.set("variable", params.variable);
    if (params?.min_pts != null) qs.set("min_pts", String(params.min_pts));
    if (params?.solo_positiva != null) qs.set("solo_positiva", String(params.solo_positiva));
    if (params?.top != null) qs.set("top", String(params.top));
    return apiGetCached<{ variable: string; pozos: TendenciaPozo[] }>(
      `/api/din/tendencias?${qs}`
    );
  },

  // ── Pozos por mes ──────────────────────────────────────
  getPozosPorMes: () =>
    apiGetCached<{ ultimo_mes: string; ultimo_valor: number; serie: PozosMes[] }>(
      "/api/din/pozos-por-mes"
    ),

  // ── Cobertura ──────────────────────────────────────────
  getCobertura: (params: {
    fecha_desde: string;
    fecha_hasta: string;
    modo?: string;
  }) => {
    const qs = new URLSearchParams({
      fecha_desde: params.fecha_desde,
      fecha_hasta: params.fecha_hasta,
      modo: params.modo || "historico",
    });
    return apiGetCached<Cobertura>(`/api/din/cobertura?${qs}`);
  },

  // ==========================================================
  // MAPA
  // ==========================================================
  getSnapshotMapa: (params?: {
    sum_min?: number;
    sum_max?: number;
    dias_min?: number;
    dias_max?: number;
    baterias?: string;
    solo_validadas?: boolean | null;
  }) => {
    const qs = new URLSearchParams();
    if (params?.sum_min != null)  qs.set("sum_min",  String(params.sum_min));
    if (params?.sum_max != null)  qs.set("sum_max",  String(params.sum_max));
    if (params?.dias_min != null) qs.set("dias_min", String(params.dias_min));
    if (params?.dias_max != null) qs.set("dias_max", String(params.dias_max));
    if (params?.baterias)         qs.set("baterias", params.baterias);
    if (params?.solo_validadas != null) qs.set("solo_validadas", String(params.solo_validadas));
    return apiGetCached<{ total: number; puntos: PuntoMapa[] }>(
      `/api/din/snapshot-mapa?${qs}`
    );
  },

  getBaterias: () =>
    apiGetCached<{ baterias: string[] }>("/api/mapa/baterias", 30 * 60 * 1000),

  getSemaforoAib: async (params?: {
    sum_media?: number;
    sum_alta?: number;
    llen_ok?: number;
    llen_bajo?: number;
    solo_se_aib?: boolean;
  }) => {
    const qs = new URLSearchParams();
    if (params?.sum_media != null)  qs.set("sum_media",  String(params.sum_media));
    if (params?.sum_alta != null)   qs.set("sum_alta",   String(params.sum_alta));
    if (params?.llen_ok != null)    qs.set("llen_ok",    String(params.llen_ok));
    if (params?.llen_bajo != null)  qs.set("llen_bajo",  String(params.llen_bajo));
    if (params?.solo_se_aib != null) qs.set("solo_se_aib", String(params.solo_se_aib));
    const raw = await apiGetCached<{
      total: number;
      counts: { total_aib?: number; normal?: number; alerta?: number; critico?: number; sin_datos?: number };
      puntos: SemaforoRow[];
    }>(`/api/mapa/semaforo-aib?${qs}`);
    return {
      total:     raw.total,
      normales:  raw.counts?.normal   ?? 0,
      alertas:   raw.counts?.alerta   ?? 0,
      criticos:  raw.counts?.critico  ?? 0,
      sin_datos: raw.counts?.sin_datos ?? 0,
      rows:      raw.puntos ?? [],
    };
  },

  // ==========================================================
  // ==========================================================
  // VALIDACIONES
  // ==========================================================
  getValidaciones: (pozo: string) =>
    apiGetCached<{ pozo: string; mediciones: Record<string, ValidacionEstado> }>(
      `/api/validaciones/${encodeURIComponent(pozo)}`
    ),

  getHistorialValidaciones: (pozos: string) =>
    apiGetCached<{ total: number; historial: Record<string, unknown>[] }>(
      `/api/validaciones/historial?pozos=${encodeURIComponent(pozos)}`
    ),

  saveValidacion: (
    pozo: string,
    body: {
      fecha_key: string;
      validada: boolean;
      comentario?: string;
      usuario?: string;
    }
  ) =>
    apiFetch<{ ok: boolean }>(`/api/validaciones/${encodeURIComponent(pozo)}`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getTablaValidaciones: (params?: {
    sum_min?: number;
    sum_max?: number;
    dias_max?: number;
    baterias?: string;
    solo_validadas?: boolean;
    solo_no_validadas?: boolean;
  }) => {
    const qs = new URLSearchParams();
    if (params?.sum_min != null) qs.set("sum_min", String(params.sum_min));
    if (params?.sum_max != null) qs.set("sum_max", String(params.sum_max));
    if (params?.dias_max != null) qs.set("dias_max", String(params.dias_max));
    if (params?.baterias) qs.set("baterias", params.baterias);
    if (params?.solo_validadas)    qs.set("solo_validadas",    "true");
    if (params?.solo_no_validadas) qs.set("solo_no_validadas", "true");
    return apiGetCached<{
      total: number;
      filas: FilaValidacion[];
    }>(`/api/validaciones/tabla?${qs}`);
  },

  // ==========================================================
  // DIAGNÓSTICOS
  // ==========================================================
  getDiagnostico: (pozo: string) =>
    apiGetCached<Diagnostico>(`/api/diagnosticos/${encodeURIComponent(pozo)}`),

  generarDiagnostico: (pozo: string) =>
    apiFetch<Diagnostico>(`/api/diagnosticos/${encodeURIComponent(pozo)}/generar`, {
      method: "POST",
    }),

  getTablaGlobalDiag: () =>
    apiGetCached<{ total: number; rows: FilaDiagGlobal[] }>("/api/diagnosticos/tabla-global"),

  getEstadoCache: () =>
    apiGetCached<{
      total_pozos_con_din: number;
      con_diagnostico: number;
      pendientes: number;
    }>("/api/diagnosticos/estado-cache"),

  generarTodos: (body: { solo_pendientes?: boolean }) =>
    apiFetch<{ ok: string[]; error: unknown[]; salteados: string[] }>(
      "/api/diagnosticos/generar-todos",
      { method: "POST", body: JSON.stringify(body) }
    ),

  // ==========================================================
  // SISTEMA
  // ==========================================================
  
  // ==========================================================
  // MERMA
  // ==========================================================
  getMermaInfo: () =>
    apiGetCached<{
      exists: boolean;
      updated_at: string | null;
      file?: string;
      size_kb?: number;
      error?: string;
    }>("/api/merma/info", 30 * 1000),

  // ==========================================================
  // SISTEMA
  // ==========================================================
  getHealth: () =>
    apiFetch<{ status: string; uptime_seg: number; timestamp: string }>("/api/health"),

  getInfo: () =>
    apiGetCached<{
      version: string;
      gcs_bucket: string;
      gcs_prefix: string;
      gcs_ok: boolean;
      openai_ok: boolean;
      din_count: number;
      niv_count: number;
      uptime_seg: number;
    }>("/api/info", 30 * 1000),
};

export default api;
