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
// TIPOS — HISTÓRICO DE PÉRDIDAS (NUEVO)
// ==========================================================

export interface DowntimeRow {
  "FECHA DESDE"?:           string | null;
  "FECHA HASTA"?:           string | null;
  oilShortfall?:            number | null;
  waterShortfall?:          number | null;
  liquidShortfall?:         number | null;
  gasShortfall?:            number | null;
  waterInjection?:          number | null;
  potentialOil?:            number | null;
  sumpInjection?:           number | null;
  potentialWater?:          number | null;
  potentialLiquid?:         number | null;
  potentialGas?:            number | null;
  potentialWaterInjection?: number | null;
  POZO?:                    string | null;
  RUBRO?:                   string | null;
}

export interface DowntimeInfo {
  exists:      boolean;
  updated_at?: string | null;
  file?:       string;
  size_kb?:    number;
  rows?:       number;
  columns?:    string[];
  fecha_min?:  string | null;
  fecha_max?:  string | null;
  error?:      string;
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
  // VALIDACIONES
  // ==========================================================
  getValidaciones: (pozo: string) =>
    apiGetCached<{ pozo: string; mediciones: Record<string, ValidacionEstado> }>(
      `/api/validaciones/${encodeURIComponent(pozo)}`
    ),

  getValidacionesBatch: (pozos: string[]) =>
    apiGetCached<{
      validaciones: Record<string, { mediciones: Record<string, ValidacionEstado> }>;
    }>(
      `/api/validaciones/batch?pozos=${encodeURIComponent(pozos.join(","))}`
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
  // HISTÓRICO DE PÉRDIDAS (NUEVO)
  // ==========================================================
  getDowntimesInfo: () =>
    apiGetCached<DowntimeInfo>("/api/merma/downtimes/info", 30 * 1000),

  getDowntimes: (params?: {
    pozo?:        string;
    fecha_desde?: string;
    fecha_hasta?: string;
    limit?:       number;
  }) => {
    const qs = new URLSearchParams();
    if (params?.pozo)        qs.set("pozo",        params.pozo);
    if (params?.fecha_desde) qs.set("fecha_desde", params.fecha_desde);
    if (params?.fecha_hasta) qs.set("fecha_hasta", params.fecha_hasta);
    if (params?.limit)       qs.set("limit",       String(params.limit));
    return apiGetCached<{ total: number; data: DowntimeRow[] }>(
      `/api/merma/downtimes?${qs}`,
      5 * 60 * 1000
    );
  },

  // ==========================================================
  // ALERTAS LLENADO DE BOMBA BM
  // ==========================================================
  getAlertasLlenadoInfo: () =>
    apiGetCached<{
      exists: boolean;
      updated_at: string | null;
      file?: string;
      size_kb?: number;
      error?: string;
    }>("/api/alertas-llenado/info", 30 * 1000),

  // ==========================================================
  // PREDICCIÓN ALTA PRESIÓN
  // ==========================================================
  getAlertasPresionInfo: () =>
    apiGetCached<{
      exists: boolean;
      updated_at: string | null;
      file?: string;
      size_kb?: number;
      error?: string;
    }>("/api/alertas-presion/info", 30 * 1000),

  // ==========================================================
  // INSTALACIÓN DE FONDO
  // ==========================================================
  getInstalacionFondoInfo: () =>
    apiGetCached<{
      exists: boolean;
      updated_at: string | null;
      file?: string;
      size_kb?: number;
      error?: string;
    }>("/api/instalacion-fondo/info", 30 * 1000),


  // ==========================================================
  // CONTROLES HISTÓRICOS
  // ==========================================================
  getControlesInfo: () =>
    apiGetCached<ControlesInfo>("/api/controles/info", 30 * 1000),

  getControlesHistorico: (params?: {
    pozo?:        string;
    bateria?:     string;
    estado_pozo?: string;
    fecha_desde?: string;
    fecha_hasta?: string;
    limit?:       number;
  }) => {
    const qs = new URLSearchParams();
    if (params?.pozo)        qs.set("pozo",        params.pozo);
    if (params?.bateria)     qs.set("bateria",     params.bateria);
    if (params?.estado_pozo) qs.set("estado_pozo", params.estado_pozo);
    if (params?.fecha_desde) qs.set("fecha_desde", params.fecha_desde);
    if (params?.fecha_hasta) qs.set("fecha_hasta", params.fecha_hasta);
    if (params?.limit)       qs.set("limit",       String(params.limit));
    return apiGetCached<{ total: number; data: ControlRow[] }>(
      `/api/controles/historico?${qs}`,
      5 * 60 * 1000
    );
  },

  getControlesMerma: (params?: {
    solo_merma?:  boolean;
    bateria?:     string;
    estado_pozo?: string;
    limit?:       number;
  }) => {
    const qs = new URLSearchParams();
    if (params?.solo_merma)  qs.set("solo_merma",  String(params.solo_merma));
    if (params?.bateria)     qs.set("bateria",     params.bateria);
    if (params?.estado_pozo) qs.set("estado_pozo", params.estado_pozo);
    if (params?.limit)       qs.set("limit",       String(params.limit));
    return apiGetCached<{ total: number; data: MermaRow[] }>(
      `/api/controles/merma?${qs}`,
      5 * 60 * 1000
    );
  },

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

  // ==========================================================
  // RRHH — Guardias
  // ==========================================================

  rrhhLogin: (legajo: string, cuil: string) =>
    apiFetch<{ ok: boolean; user: RRHHUser }>("/api/rrhh/login", {
      method: "POST",
      body: JSON.stringify({ legajo, cuil }),
    }),

  rrhhPeriodos: (n = 8) =>
    apiGetCached<{ actual: string; periodos: RRHHPeriodo[] }>(
      `/api/rrhh/periodos?n=${n}`, 60 * 60 * 1000
    ),

  rrhhPersonal: () =>
    apiGetCached<{ personal: RRHHPersona[] }>("/api/rrhh/personal", 30 * 60 * 1000),

  rrhhImportPersonal: (rows: Partial<RRHHPersona>[]) =>
    apiFetch<{ ok: boolean; insertados: number; actualizados: number }>(
      "/api/rrhh/personal/import",
      { method: "POST", body: JSON.stringify({ rows }) }
    ).then(r => { clearApiCache("/api/rrhh/personal"); return r; }),

  rrhhGetParte: (legajo: string, periodo: string) =>
    apiGetCached<RRHHParte>(
      `/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}`, 5 * 60 * 1000
    ),

  rrhhGuardarParte: (legajo: string, periodo: string, items: RRHHItem[]) =>
    apiFetch<{ ok: boolean; estado: string; parte: RRHHParte }>(
      `/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}/guardar`,
      { method: "POST", body: JSON.stringify({ items }) }
    ).then(r => {
      clearApiCache(`/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}`);
      clearApiCache(`/api/rrhh/bitacora/${encodeURIComponent(legajo)}`);
      return r;
    }),

  rrhhEnviarParte: (legajo: string, periodo: string, items: RRHHItem[]) =>
    apiFetch<{ ok: boolean; estado: string; parte: RRHHParte }>(
      `/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}/enviar`,
      { method: "POST", body: JSON.stringify({ items }) }
    ).then(r => {
      clearApiCache(`/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}`);
      clearApiCache(`/api/rrhh/bitacora/${encodeURIComponent(legajo)}`);
      clearApiCache("/api/rrhh/equipo");
      return r;
    }),

  rrhhGuardarParteLider: (legajo: string, periodo: string, items: RRHHItem[]) =>
    apiFetch<{ ok: boolean; estado: string; parte: RRHHParte }>(
      `/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}/guardar-lider`,
      { method: "POST", body: JSON.stringify({ items }) }
    ).then(r => {
      clearApiCache(`/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}`);
      clearApiCache(`/api/rrhh/bitacora/${encodeURIComponent(legajo)}`);
      clearApiCache(`/api/rrhh/consolidado`);
      return r;
    }),

  rrhhAprobar: (legajo: string, periodo: string, aprobadorLegajo: string) =>
    apiFetch<{ ok: boolean; estado: string }>(
      `/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}/aprobar`,
      { method: "POST", body: JSON.stringify({ aprobador_legajo: aprobadorLegajo }) }
    ).then(r => {
      clearApiCache(`/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}`);
      clearApiCache(`/api/rrhh/bitacora/${encodeURIComponent(legajo)}`);
      clearApiCache("/api/rrhh/equipo");
      clearApiCache("/api/rrhh/consolidado");
      return r;
    }),

  rrhhRechazar: (legajo: string, periodo: string, aprobadorLegajo: string, comentario: string) =>
    apiFetch<{ ok: boolean; estado: string }>(
      `/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}/rechazar`,
      { method: "POST", body: JSON.stringify({ aprobador_legajo: aprobadorLegajo, comentario }) }
    ).then(r => {
      clearApiCache(`/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}`);
      clearApiCache(`/api/rrhh/bitacora/${encodeURIComponent(legajo)}`);
      clearApiCache("/api/rrhh/equipo");
      return r;
    }),

  rrhhReabrir: (legajo: string, periodo: string, aprobadorLegajo: string) =>
    apiFetch<{ ok: boolean; estado: string }>(
      `/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}/reabrir`,
      { method: "POST", body: JSON.stringify({ aprobador_legajo: aprobadorLegajo }) }
    ).then(r => {
      clearApiCache(`/api/rrhh/parte/${encodeURIComponent(legajo)}/${periodo}`);
      clearApiCache(`/api/rrhh/bitacora/${encodeURIComponent(legajo)}`);
      clearApiCache("/api/rrhh/equipo");
      clearApiCache("/api/rrhh/consolidado");
      return r;
    }),

  rrhhBitacora: (legajo: string) =>
    apiGetCached<{ legajo: string; partes: RRHHBitacoraItem[] }>(
      `/api/rrhh/bitacora/${encodeURIComponent(legajo)}`, 5 * 60 * 1000
    ),

  rrhhPendientes: (leaderLegajo: string) =>
    apiGetCached<{ leader_legajo: string; pendientes: RRHHPendiente[] }>(
      `/api/rrhh/equipo/${encodeURIComponent(leaderLegajo)}/pendientes`, 2 * 60 * 1000
    ),

  rrhhConsolidado: (leaderLegajo: string, periodo: string) =>
    apiGetCached<RRHHConsolidado>(
      `/api/rrhh/consolidado/${encodeURIComponent(leaderLegajo)}/${periodo}`, 5 * 60 * 1000
    ),
};

export default api;

// ==========================================================
// TIPOS RRHH
// ==========================================================

export interface RRHHUser {
  legajo:        string;
  nombre:        string;
  leader_legajo: string;
  funcion?:      string;
  role:          "empleado" | "lider";
}

export interface RRHHPeriodo {
  id:      string;
  display: string;
  start:   string;
  end:     string;
}


// ==========================================================
// CONTROLES HISTÓRICOS
// ==========================================================

export interface ControlesInfo {
  exists:     boolean;
  updated_at: string | null;
  rows?:      number;
  pozos?:     number;
  en_merma?:  number;
  fecha_min?: string | null;
  fecha_max?: string | null;
  error?:     string;
}

export interface ControlRow {
  Pozo?:                     string;
  "Día Operativo"?:          string;
  "Fecha y Hora"?:           string;
  Estado?:                   string;
  "Producción de Gas"?:      number | null;
  "Producción de Líquido"?:  number | null;
  "Producción de Petróleo"?: number | null;
  BATERIA?:                  string | null;
  ESTADO_POZO?:              string | null;
  TIPO_PRODUCCION?:          string | null;
  SIST_EXTRACCION?:          string | null;
}

export interface MermaRow {
  POZO?:                string;
  BATERIA?:             string | null;
  ESTADO_POZO?:         string | null;
  TIPO_PRODUCCION?:     string | null;
  SIST_EXTRACCION?:     string | null;
  FECHA_ULTIMO_CONTROL?: string | null;
  DIAS_SIN_CONTROL?:    number | null;
  NETA_ULTIMO_M3?:      number | null;
  NETA_PENULTIMO_M3?:   number | null;
  PCT_MERMA_NETA?:      number | null;
  BRUTA_ULTIMO_M3?:     number | null;
  BRUTA_PENULTIMO_M3?:  number | null;
  PCT_MERMA_BRUTA?:     number | null;
  EN_MERMA_NETA?:       boolean | null;
  EN_MERMA_BRUTA?:      boolean | null;
}

export interface RRHHPersona {
  legajo:        string;
  cuil:          string;
  nombre:        string;
  leader_legajo: string;
  funcion?:      string;
  origen?:       string;
  lugar_trabajo?: string;
}

export interface RRHHItem {
  fecha:      string;
  tipo:       "G" | "F" | "D" | "HO" | "HV" | "HE";
  valor_num?: number;
  comentario?: string;
}

export interface RRHHGrillaRow {
  fecha:      string;
  G:          boolean;
  F:          boolean;
  D:          boolean;
  HO:         boolean;
  HV:         number;
  HE:         number;
  comentario: string;
}

export interface RRHHTotales {
  G: number; F: number; D: number; HO: number; HV: number; HE: number;
}

export interface RRHHParte {
  legajo:             string;
  periodo:            string;
  periodo_display:    string;
  periodo_inicio:     string;
  periodo_fin:        string;
  estado:             "BORRADOR" | "ENVIADO" | "APROBADO" | "RECHAZADO";
  submitted_at?:      string;
  approved_at?:       string;
  approved_by?:       string;
  rejection_comment?: string;
  grilla:             RRHHGrillaRow[];
  totales:            RRHHTotales;
}

export interface RRHHBitacoraItem {
  periodo:              string;
  periodo_display:      string;
  periodo_inicio:       string;
  periodo_fin:          string;
  estado:               string;
  submitted_at?:        string;
  approved_at?:         string;
  approved_by_nombre?:  string;
  rejection_comment?:   string;
}

export interface RRHHPendiente {
  legajo:          string;
  nombre:          string;
  periodo:         string;
  periodo_display: string;
  periodo_inicio:  string;
  periodo_fin:     string;
  estado:          string;
  submitted_at?:   string;
}

export interface RRHHConsolidadoEmpleado {
  legajo:      string;
  nombre:      string;
  funcion:     string;
  estado:      string;
  approved_at?: string;
  G: number; F: number; D: number; HO: number; HV: number; HE: number;
  dias: {
    fecha:      string;
    tipos:      string[];
    HV:         number;
    HE:         number;
    comentario: string;
  }[];
}

export interface RRHHConsolidado {
  leader_legajo:   string;
  periodo:         string;
  periodo_display: string;
  periodo_inicio:  string;
  periodo_fin:     string;
  empleados:       RRHHConsolidadoEmpleado[];
}
