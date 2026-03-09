// ==========================================================
// lib/datastore.ts
//
// Cache global a nivel módulo — sobrevive desmontajes de React.
// Los datos se cargan UNA SOLA VEZ y quedan en memoria.
// Los filtros se aplican 100% en el cliente (sin llamadas al backend).
// ==========================================================

import api, { clearApiCache } from "./api";
import type { DowntimeRow, DowntimeInfo, ControlRow, MermaRow, ControlesInfo } from "./api";

// ── Tipos internos ──────────────────────────────────────────

type StoreStatus = "idle" | "loading" | "ready" | "error";

interface DowntimesData {
  info: DowntimeInfo;
  rows: DowntimeRow[];
}

interface ControlesData {
  info:      ControlesInfo;
  controles: ControlRow[];
  merma:     MermaRow[];
}

// ── Stores globales (fuera de React) ───────────────────────
// Al estar a nivel módulo, NO se pierden al navegar entre pestañas.

let dtStatus:  StoreStatus   = "idle";
let dtData:    DowntimesData | null = null;
let dtError:   string | null = null;
let dtPromise: Promise<void> | null = null;
const dtListeners: Set<() => void> = new Set();

let ctStatus:  StoreStatus   = "idle";
let ctData:    ControlesData | null = null;
let ctError:   string | null = null;
let ctPromise: Promise<void> | null = null;
const ctListeners: Set<() => void> = new Set();

// ── Notificadores ──────────────────────────────────────────

function notifyDT() { dtListeners.forEach(fn => fn()); }
function notifyCT() { ctListeners.forEach(fn => fn()); }

// ── Carga de Downtimes ─────────────────────────────────────

export function loadDowntimes(force = false): Promise<void> {
  if (!force && (dtStatus === "ready" || dtStatus === "loading")) {
    return dtPromise ?? Promise.resolve();
  }

  dtStatus  = "loading";
  dtError   = null;
  notifyDT();

  dtPromise = Promise.all([
    api.getDowntimesInfo(),
    api.getDowntimes({ limit: 50000 }),  // carga TODO sin filtros
  ]).then(([info, rowsResp]) => {
    dtData   = { info, rows: rowsResp.data };
    dtStatus = "ready";
    dtError  = null;
  }).catch(e => {
    dtStatus = "error";
    dtError  = e instanceof Error ? e.message : "Error cargando datos";
    dtData   = null;
  }).finally(() => {
    dtPromise = null;
    notifyDT();
  });

  return dtPromise;
}

export function getDowntimesSnapshot() {
  return { status: dtStatus, data: dtData, error: dtError };
}

export function subscribeDowntimes(fn: () => void) {
  dtListeners.add(fn);
  return () => dtListeners.delete(fn);
}

export function invalidateDowntimes() {
  dtStatus = "idle";
  dtData   = null;
  clearApiCache("/api/merma/downtimes");
}

// ── Carga de Controles ─────────────────────────────────────

export function loadControles(force = false): Promise<void> {
  if (!force && (ctStatus === "ready" || ctStatus === "loading")) {
    return ctPromise ?? Promise.resolve();
  }

  ctStatus  = "loading";
  ctError   = null;
  notifyCT();

  ctPromise = Promise.all([
    api.getControlesInfo(),
    api.getControlesHistorico({ limit: 50000 }),  // carga TODO sin filtros
    api.getControlesMerma({ limit: 10000 }),
  ]).then(([info, controlesResp, mermaResp]) => {
    ctData   = { info, controles: controlesResp.data, merma: mermaResp.data };
    ctStatus = "ready";
    ctError  = null;
  }).catch(e => {
    ctStatus = "error";
    ctError  = e instanceof Error ? e.message : "Error cargando datos";
    ctData   = null;
  }).finally(() => {
    ctPromise = null;
    notifyCT();
  });

  return ctPromise;
}

export function getControlesSnapshot() {
  return { status: ctStatus, data: ctData, error: ctError };
}

export function subscribeControles(fn: () => void) {
  ctListeners.add(fn);
  return () => ctListeners.delete(fn);
}

export function invalidateControles() {
  ctStatus = "idle";
  ctData   = null;
  clearApiCache("/api/controles");
}
