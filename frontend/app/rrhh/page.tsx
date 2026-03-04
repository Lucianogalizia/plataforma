"use client";

// ==========================================================
// app/rrhh/page.tsx
// Módulo RRHH — Guardias y partes mensuales
// Usa api.ts centralizado para cache automático en frontend.
// ==========================================================

import React, { useState, useEffect, useCallback, useMemo } from "react";
import {
  LogIn, LogOut, User, Calendar, ClipboardList,
  Users, FileSpreadsheet, ChevronDown, ChevronUp,
  Check, X, AlertCircle, Clock, Send, Save,
  Download, RefreshCw, Shield,
} from "lucide-react";
import api, { clearApiCache } from "@/lib/api";
import type {
  RRHHUser, RRHHPeriodo, RRHHParte, RRHHGrillaRow,
  RRHHTotales, RRHHBitacoraItem, RRHHPendiente, RRHHItem,
} from "@/lib/api";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// ==========================================================
// Utilities
// ==========================================================

const ESTADO_CONFIG: Record<string, { label: string; color: string; icon: React.ReactNode }> = {
  BORRADOR:  { label: "Borrador",          color: "text-slate-400 bg-slate-800",   icon: <Clock size={12} /> },
  ENVIADO:   { label: "Enviado",           color: "text-yellow-400 bg-yellow-900/30", icon: <Send size={12} /> },
  APROBADO:  { label: "Aprobado",          color: "text-emerald-400 bg-emerald-900/30", icon: <Check size={12} /> },
  RECHAZADO: { label: "Rechazado",         color: "text-red-400 bg-red-900/30",    icon: <X size={12} /> },
  "SIN PARTE": { label: "Sin parte",       color: "text-slate-500 bg-slate-800",   icon: <AlertCircle size={12} /> },
};

function EstadoBadge({ estado }: { estado: string }) {
  const cfg = ESTADO_CONFIG[estado] || ESTADO_CONFIG["SIN PARTE"];
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cfg.color}`}>
      {cfg.icon} {cfg.label}
    </span>
  );
}

function formatFecha(iso?: string | null) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString("es-AR", { day: "2-digit", month: "2-digit", year: "numeric" });
}

function formatDatetime(iso?: string | null) {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("es-AR", {
      day: "2-digit", month: "2-digit", year: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  } catch { return iso; }
}

function grillaToItems(grilla: RRHHGrillaRow[]): RRHHItem[] {
  const items: RRHHItem[] = [];
  for (const row of grilla) {
    const comentario = row.comentario || undefined;
    for (const t of ["G","F","D","HO"] as const) {
      if (row[t]) items.push({ fecha: row.fecha, tipo: t, comentario });
    }
    if (row.HV > 0) items.push({ fecha: row.fecha, tipo: "HV", valor_num: row.HV, comentario });
    if (row.HE > 0) items.push({ fecha: row.fecha, tipo: "HE", valor_num: row.HE, comentario });
  }
  return items;
}

// ==========================================================
// Login
// ==========================================================

function LoginForm({ onLogin }: { onLogin: (user: RRHHUser) => void }) {
  const [legajo, setLegajo] = useState("");
  const [cuil, setCuil]     = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError]   = useState("");

  const handleSubmit = async () => {
    setError("");
    if (!legajo.trim() || !cuil.trim()) {
      setError("Completá legajo y CUIL.");
      return;
    }
    setLoading(true);
    try {
      const res = await api.rrhhLogin(legajo.trim(), cuil.trim());
      onLogin(res.user);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al iniciar sesión.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex items-center justify-center min-h-screen bg-[#0f172a]">
      <div className="w-full max-w-sm">
        <div className="bg-[#1e293b] border border-[#334155] rounded-2xl p-8 shadow-2xl">
          <div className="text-center mb-8">
            <div className="w-14 h-14 bg-sky-500/20 rounded-2xl flex items-center justify-center mx-auto mb-4">
              <Shield size={28} className="text-sky-400" />
            </div>
            <h1 className="text-2xl font-bold text-slate-100">RRHH — Guardias</h1>
            <p className="text-slate-400 text-sm mt-1">Ingresá con tu Legajo y CUIL</p>
          </div>

          <div className="space-y-4">
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1.5">Legajo</label>
              <input
                type="text"
                value={legajo}
                onChange={e => setLegajo(e.target.value)}
                onKeyDown={e => e.key === "Enter" && handleSubmit()}
                placeholder="Ej: 5478"
                className="w-full bg-[#0f172a] border border-[#334155] rounded-lg px-3 py-2.5 text-slate-200 text-sm focus:outline-none focus:border-sky-500 transition-colors"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1.5">CUIL (completo o últimos 4 dígitos)</label>
              <input
                type="text"
                value={cuil}
                onChange={e => setCuil(e.target.value)}
                onKeyDown={e => e.key === "Enter" && handleSubmit()}
                placeholder="Ej: 20359612835 o 2835"
                className="w-full bg-[#0f172a] border border-[#334155] rounded-lg px-3 py-2.5 text-slate-200 text-sm focus:outline-none focus:border-sky-500 transition-colors"
              />
            </div>

            {error && (
              <div className="flex items-center gap-2 text-red-400 text-sm bg-red-900/20 border border-red-900/30 rounded-lg px-3 py-2">
                <AlertCircle size={14} /> {error}
              </div>
            )}

            <button
              onClick={handleSubmit}
              disabled={loading}
              className="w-full bg-sky-600 hover:bg-sky-500 disabled:opacity-50 text-white font-medium py-2.5 rounded-lg transition-colors flex items-center justify-center gap-2"
            >
              {loading ? <RefreshCw size={16} className="animate-spin" /> : <LogIn size={16} />}
              {loading ? "Verificando..." : "Ingresar"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ==========================================================
// Grilla de carga de parte
// ==========================================================

function GrillaParteMes({
  parte,
  esLider,
  onSave,
  onEnviar,
}: {
  parte: RRHHParte;
  esLider: boolean;
  onSave: (grilla: RRHHGrillaRow[]) => Promise<void>;
  onEnviar: (grilla: RRHHGrillaRow[]) => Promise<void>;
}) {
  const [grilla, setGrilla] = useState<RRHHGrillaRow[]>(parte.grilla);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState<{ tipo: "ok" | "err"; texto: string } | null>(null);

  const editable = esLider || parte.estado === "BORRADOR" || parte.estado === "RECHAZADO";

  const totales = useMemo<RRHHTotales>(() => {
    const t: RRHHTotales = { G: 0, F: 0, D: 0, HO: 0, HV: 0, HE: 0 };
    for (const r of grilla) {
      if (r.G) t.G++; if (r.F) t.F++;
      if (r.D) t.D++; if (r.HO) t.HO++;
      t.HV = Math.round((t.HV + (r.HV || 0)) * 100) / 100;
      t.HE = Math.round((t.HE + (r.HE || 0)) * 100) / 100;
    }
    return t;
  }, [grilla]);

  const update = (idx: number, field: keyof RRHHGrillaRow, value: unknown) => {
    setGrilla(prev => {
      const next = [...prev];
      next[idx] = { ...next[idx], [field]: value };
      return next;
    });
  };

  const handleSave = async () => {
    setSaving(true); setMsg(null);
    try {
      await onSave(grilla);
      setMsg({ tipo: "ok", texto: esLider ? "Guardado y aprobado automáticamente." : "Borrador guardado." });
    } catch (e: unknown) {
      setMsg({ tipo: "err", texto: e instanceof Error ? e.message : "Error al guardar." });
    } finally { setSaving(false); }
  };

  const handleEnviar = async () => {
    setSaving(true); setMsg(null);
    try {
      await onEnviar(grilla);
      setMsg({ tipo: "ok", texto: "Parte enviado a aprobación. Queda bloqueado hasta revisión del líder." });
    } catch (e: unknown) {
      setMsg({ tipo: "err", texto: e instanceof Error ? e.message : "Error al enviar." });
    } finally { setSaving(false); }
  };

  const formatFechaRow = (iso: string) => {
    const [y, m, d] = iso.split("-");
    const days = ["Dom","Lun","Mar","Mié","Jue","Vie","Sáb"];
    const day = new Date(Number(y), Number(m) - 1, Number(d)).getDay();
    return `${days[day]} ${d}/${m}`;
  };

  return (
    <div className="space-y-4">
      {/* Info del período */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h3 className="text-lg font-semibold text-slate-100">{parte.periodo_display}</h3>
          <p className="text-xs text-slate-400">
            {formatFecha(parte.periodo_inicio)} → {formatFecha(parte.periodo_fin)}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <EstadoBadge estado={parte.estado} />
          {parte.estado === "RECHAZADO" && parte.rejection_comment && (
            <span className="text-xs text-red-400 bg-red-900/20 px-2 py-1 rounded border border-red-900/30">
              ❌ {parte.rejection_comment}
            </span>
          )}
        </div>
      </div>

      {/* Estado informativo */}
      {parte.estado === "ENVIADO" && !esLider && (
        <div className="bg-yellow-900/20 border border-yellow-700/30 rounded-lg px-4 py-2.5 text-sm text-yellow-300 flex items-center gap-2">
          <Clock size={14} /> Parte enviado y en revisión. Esperá la aprobación de tu líder.
        </div>
      )}
      {parte.estado === "APROBADO" && (
        <div className="bg-emerald-900/20 border border-emerald-700/30 rounded-lg px-4 py-2.5 text-sm text-emerald-300 flex items-center gap-2">
          <Check size={14} /> Parte aprobado
          {parte.approved_at ? ` el ${formatDatetime(parte.approved_at)}` : ""}
          {esLider ? " (auto-aprobado)" : ""}.
        </div>
      )}

      {/* Grilla */}
      <div className="overflow-x-auto rounded-xl border border-[#334155]">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-[#1e293b] border-b border-[#334155]">
              <th className="px-4 py-2.5 text-left text-slate-400 font-medium w-28">Fecha</th>
              {(["G","F","D","HO"] as const).map(t => (
                <th key={t} className="px-3 py-2.5 text-center text-slate-400 font-medium w-14">{t}</th>
              ))}
              <th className="px-3 py-2.5 text-center text-slate-400 font-medium w-20">HV</th>
              <th className="px-3 py-2.5 text-center text-slate-400 font-medium w-20">HE</th>
              <th className="px-4 py-2.5 text-left text-slate-400 font-medium">Comentario</th>
            </tr>
          </thead>
          <tbody>
            {grilla.map((row, i) => {
              const isWeekend = (() => {
                const d = new Date(row.fecha + "T00:00:00");
                const day = d.getDay();
                return day === 0 || day === 6;
              })();
              return (
                <tr
                  key={row.fecha}
                  className={`border-b border-[#1e293b] transition-colors ${
                    isWeekend ? "bg-[#1a2540]" : i % 2 === 0 ? "bg-[#0f172a]" : "bg-[#111827]"
                  } hover:bg-[#1e2d45]`}
                >
                  <td className={`px-4 py-2 text-xs font-mono ${isWeekend ? "text-sky-400" : "text-slate-300"}`}>
                    {formatFechaRow(row.fecha)}
                  </td>
                  {(["G","F","D","HO"] as const).map(t => (
                    <td key={t} className="px-3 py-2 text-center">
                      <input
                        type="checkbox"
                        checked={!!row[t]}
                        disabled={!editable}
                        onChange={e => update(i, t, e.target.checked)}
                        className="w-4 h-4 rounded accent-sky-500 cursor-pointer disabled:cursor-not-allowed"
                      />
                    </td>
                  ))}
                  <td className="px-2 py-1.5">
                    <input
                      type="number"
                      min={0} step={0.5}
                      value={row.HV || ""}
                      disabled={!editable}
                      onChange={e => update(i, "HV", parseFloat(e.target.value) || 0)}
                      placeholder="0"
                      className="w-16 bg-[#1e293b] border border-[#334155] rounded px-2 py-1 text-xs text-slate-200 text-center focus:outline-none focus:border-sky-500 disabled:opacity-50"
                    />
                  </td>
                  <td className="px-2 py-1.5">
                    <input
                      type="number"
                      min={0} step={0.5}
                      value={row.HE || ""}
                      disabled={!editable}
                      onChange={e => update(i, "HE", parseFloat(e.target.value) || 0)}
                      placeholder="0"
                      className="w-16 bg-[#1e293b] border border-[#334155] rounded px-2 py-1 text-xs text-slate-200 text-center focus:outline-none focus:border-sky-500 disabled:opacity-50"
                    />
                  </td>
                  <td className="px-2 py-1.5">
                    <input
                      type="text"
                      value={row.comentario || ""}
                      disabled={!editable}
                      onChange={e => update(i, "comentario", e.target.value)}
                      placeholder="Opcional"
                      className="w-full bg-[#1e293b] border border-[#334155] rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-sky-500 disabled:opacity-50"
                    />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* RRHHTotales */}
      <div className="grid grid-cols-6 gap-3">
        {(["G","F","D","HO","HV","HE"] as const).map(t => (
          <div key={t} className="bg-[#1e293b] border border-[#334155] rounded-xl p-3 text-center">
            <p className="text-xs text-slate-500 mb-1">{t === "HV" ? "Hs Viaje" : t === "HE" ? "Hs Extra" : `Días ${t}`}</p>
            <p className="text-xl font-bold text-sky-400">
              {t === "HV" || t === "HE" ? totales[t].toFixed(1) : totales[t]}
            </p>
          </div>
        ))}
      </div>

      {/* Acciones */}
      {editable && (
        <div className="flex items-center gap-3 flex-wrap">
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex items-center gap-2 px-4 py-2 bg-slate-700 hover:bg-slate-600 text-slate-200 rounded-lg text-sm font-medium transition-colors disabled:opacity-50"
          >
            {saving ? <RefreshCw size={14} className="animate-spin" /> : <Save size={14} />}
            {esLider ? "Guardar (auto-aprobado)" : "Guardar borrador"}
          </button>
          {!esLider && (
            <button
              onClick={handleEnviar}
              disabled={saving}
              className="flex items-center gap-2 px-4 py-2 bg-sky-600 hover:bg-sky-500 text-white rounded-lg text-sm font-medium transition-colors disabled:opacity-50"
            >
              {saving ? <RefreshCw size={14} className="animate-spin" /> : <Send size={14} />}
              Enviar a aprobación
            </button>
          )}
        </div>
      )}

      {msg && (
        <div className={`flex items-center gap-2 text-sm px-4 py-2.5 rounded-lg border ${
          msg.tipo === "ok"
            ? "text-emerald-300 bg-emerald-900/20 border-emerald-700/30"
            : "text-red-400 bg-red-900/20 border-red-900/30"
        }`}>
          {msg.tipo === "ok" ? <Check size={14} /> : <AlertCircle size={14} />}
          {msg.texto}
        </div>
      )}

      {!editable && parte.estado === "ENVIADO" && !esLider && (
        <p className="text-xs text-slate-500 italic">Edición deshabilitada — parte en revisión.</p>
      )}
    </div>
  );
}

// ==========================================================
// Tab: Mi Parte
// ==========================================================

function TabMiParte({ user, periodos }: { user: RRHHUser; periodos: RRHHPeriodo[] }) {
  const [periodoId, setPeriodoId] = useState(periodos[0]?.id || "");
  const [parte, setParte] = useState<RRHHParte | null>(null);
  const [loading, setLoading] = useState(false);
  const esLider = user.role === "lider";

  const loadParte = useCallback(async (pid: string) => {
    setLoading(true);
    try {
      const data = await api.rrhhGetParte(user.legajo, pid);
      setParte(data);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, [user.legajo]);

  useEffect(() => {
    if (periodoId) loadParte(periodoId);
  }, [periodoId, loadParte]);

  const handleSave = async (grilla: RRHHGrillaRow[]) => {
    const items = grillaToItems(grilla);
    const res = esLider
      ? await api.rrhhGuardarParteLider(user.legajo, periodoId, items)
      : await api.rrhhGuardarParte(user.legajo, periodoId, items);
    // El backend devuelve el parte actualizado — usarlo directamente
    setParte(res.parte);
  };

  const handleEnviar = async (grilla: RRHHGrillaRow[]) => {
    const res = await api.rrhhEnviarParte(user.legajo, periodoId, grillaToItems(grilla));
    setParte(res.parte);
  };

  return (
    <div className="space-y-5">
      <div className="flex items-center gap-3">
        <label className="text-sm text-slate-400 whitespace-nowrap">Período:</label>
        <select
          value={periodoId}
          onChange={e => setPeriodoId(e.target.value)}
          className="bg-[#0f172a] border border-[#334155] rounded-lg px-3 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-sky-500"
        >
          {periodos.map(p => (
            <option key={p.id} value={p.id}>
              {p.display} ({p.start.slice(5,10).split("-").reverse().join("/")} → {p.end.slice(5,10).split("-").reverse().join("/")})
            </option>
          ))}
        </select>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-20">
          <RefreshCw size={20} className="animate-spin text-sky-400 mr-2" />
          <span className="text-slate-400">Cargando parte...</span>
        </div>
      ) : parte ? (
        <GrillaParteMes
          key={parte.periodo}
          parte={parte}
          esLider={esLider}
          onSave={handleSave}
          onEnviar={handleEnviar}
        />
      ) : null}
    </div>
  );
}

// ==========================================================
// Tab: Bitácora
// ==========================================================

function TabBitacora({ user }: { user: RRHHUser }) {
  const [partes, setPartes] = useState<RRHHBitacoraItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    api.rrhhBitacora(user.legajo)
      .then(r => setPartes(r.partes))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [user.legajo]);

  if (loading) return (
    <div className="flex items-center justify-center py-20">
      <RefreshCw size={20} className="animate-spin text-sky-400 mr-2" />
      <span className="text-slate-400">Cargando bitácora...</span>
    </div>
  );

  if (!partes.length) return (
    <div className="text-center py-16 text-slate-500">
      <ClipboardList size={36} className="mx-auto mb-3 opacity-40" />
      <p>No hay partes registrados aún.</p>
    </div>
  );

  return (
    <div className="space-y-2">
      <p className="text-sm text-slate-400 mb-4">Historial de todos tus partes enviados.</p>
      {partes.map(p => (
        <div key={p.periodo} className="bg-[#1e293b] border border-[#334155] rounded-xl overflow-hidden">
          <button
            onClick={() => setExpanded(e => e === p.periodo ? null : p.periodo)}
            className="w-full flex items-center justify-between px-5 py-4 hover:bg-[#243347] transition-colors"
          >
            <div className="flex items-center gap-4">
              <Calendar size={16} className="text-sky-400" />
              <div className="text-left">
                <p className="text-sm font-semibold text-slate-200">{p.periodo_display}</p>
                <p className="text-xs text-slate-500">
                  {formatFecha(p.periodo_inicio)} → {formatFecha(p.periodo_fin)}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-3">
              <EstadoBadge estado={p.estado} />
              {expanded === p.periodo ? <ChevronUp size={16} className="text-slate-400" /> : <ChevronDown size={16} className="text-slate-400" />}
            </div>
          </button>

          {expanded === p.periodo && (
            <div className="px-5 pb-4 border-t border-[#334155] pt-4 grid grid-cols-2 gap-4 text-sm">
              <div>
                <p className="text-slate-500 text-xs mb-1">Enviado</p>
                <p className="text-slate-200">{formatDatetime(p.submitted_at)}</p>
              </div>
              <div>
                <p className="text-slate-500 text-xs mb-1">Revisado por</p>
                <p className="text-slate-200">{p.approved_by_nombre || "—"}</p>
              </div>
              {p.estado === "APROBADO" && (
                <div>
                  <p className="text-slate-500 text-xs mb-1">Aprobado</p>
                  <p className="text-emerald-400">{formatDatetime(p.approved_at)}</p>
                </div>
              )}
              {p.estado === "RECHAZADO" && p.rejection_comment && (
                <div className="col-span-2">
                  <p className="text-slate-500 text-xs mb-1">Motivo del rechazo</p>
                  <p className="text-red-400 bg-red-900/20 rounded px-3 py-2 text-sm">
                    {p.rejection_comment}
                  </p>
                </div>
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

// ==========================================================
// Tab: Equipo (Líder)
// ==========================================================

function TabEquipo({ user }: { user: RRHHUser }) {
  const [pendientes, setPendientes] = useState<RRHHPendiente[]>([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<RRHHPendiente | null>(null);
  const [parteSel, setParteSel] = useState<RRHHParte | null>(null);
  const [loadingParte, setLoadingParte] = useState(false);
  const [accion, setAccion] = useState<"aprobar" | "rechazar" | null>(null);
  const [comentario, setComentario] = useState("");
  const [procesando, setProcesando] = useState(false);
  const [msg, setMsg] = useState<{ tipo: "ok" | "err"; texto: string } | null>(null);

  const loadPendientes = useCallback(() => {
    setLoading(true);
    api.rrhhPendientes(user.legajo)
      .then(r => setPendientes(r.pendientes))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [user.legajo]);

  useEffect(() => { loadPendientes(); }, [loadPendientes]);

  const openParte = async (item: RRHHPendiente) => {
    setSelected(item); setParteSel(null); setAccion(null); setMsg(null); setComentario("");
    setLoadingParte(true);
    try {
      const data = await api.rrhhGetParte(item.legajo, item.periodo);
      setParteSel(data);
    } catch (e) { console.error(e); }
    finally { setLoadingParte(false); }
  };

  const handleAprobar = async () => {
    if (!selected) return;
    setProcesando(true); setMsg(null);
    try {
      await api.rrhhAprobar(selected.legajo, selected.periodo, user.legajo);
      setMsg({ tipo: "ok", texto: `Parte de ${selected.nombre} aprobado.` });
      setSelected(null);
      loadPendientes();
    } catch (e: unknown) {
      setMsg({ tipo: "err", texto: e instanceof Error ? e.message : "Error." });
    } finally { setProcesando(false); }
  };

  const handleRechazar = async () => {
    if (!selected || !comentario.trim()) return;
    setProcesando(true); setMsg(null);
    try {
      await api.rrhhRechazar(selected.legajo, selected.periodo, user.legajo, comentario.trim());
      setMsg({ tipo: "ok", texto: `Parte de ${selected.nombre} rechazado.` });
      setSelected(null);
      loadPendientes();
    } catch (e: unknown) {
      setMsg({ tipo: "err", texto: e instanceof Error ? e.message : "Error." });
    } finally { setProcesando(false); }
  };

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h3 className="text-base font-semibold text-slate-200">
          Partes pendientes de aprobación
        </h3>
        <button onClick={loadPendientes} className="text-slate-400 hover:text-sky-400 transition-colors">
          <RefreshCw size={16} className={loading ? "animate-spin" : ""} />
        </button>
      </div>

      {msg && (
        <div className={`flex items-center gap-2 text-sm px-4 py-2.5 rounded-lg border ${
          msg.tipo === "ok"
            ? "text-emerald-300 bg-emerald-900/20 border-emerald-700/30"
            : "text-red-400 bg-red-900/20 border-red-900/30"
        }`}>
          {msg.tipo === "ok" ? <Check size={14} /> : <AlertCircle size={14} />}
          {msg.texto}
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center py-12">
          <RefreshCw size={20} className="animate-spin text-sky-400 mr-2" />
          <span className="text-slate-400">Cargando...</span>
        </div>
      ) : pendientes.length === 0 ? (
        <div className="text-center py-12 text-slate-500">
          <Check size={36} className="mx-auto mb-3 opacity-40 text-emerald-500" />
          <p>No hay partes pendientes. ¡Todo al día!</p>
        </div>
      ) : (
        <div className="overflow-x-auto rounded-xl border border-[#334155]">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-[#1e293b] border-b border-[#334155]">
                <th className="px-4 py-2.5 text-left text-slate-400 font-medium">Empleado</th>
                <th className="px-4 py-2.5 text-left text-slate-400 font-medium">Período</th>
                <th className="px-4 py-2.5 text-left text-slate-400 font-medium">Enviado</th>
                <th className="px-4 py-2.5 text-center text-slate-400 font-medium">Acción</th>
              </tr>
            </thead>
            <tbody>
              {pendientes.map((p, i) => (
                <tr key={`${p.legajo}-${p.periodo}`}
                    className={`border-b border-[#1e293b] ${i % 2 === 0 ? "bg-[#0f172a]" : "bg-[#111827]"}`}>
                  <td className="px-4 py-2.5">
                    <p className="text-slate-200 font-medium">{p.nombre}</p>
                    <p className="text-xs text-slate-500">Legajo {p.legajo}</p>
                  </td>
                  <td className="px-4 py-2.5 text-slate-300 text-sm">{p.periodo_display}</td>
                  <td className="px-4 py-2.5 text-slate-400 text-xs">{formatDatetime(p.submitted_at)}</td>
                  <td className="px-4 py-2.5 text-center">
                    <button
                      onClick={() => openParte(p)}
                      className="px-3 py-1.5 bg-sky-600 hover:bg-sky-500 text-white text-xs rounded-lg transition-colors"
                    >
                      Ver parte
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Modal de revisión */}
      {selected && (
        <div className="fixed inset-0 bg-black/70 z-50 flex items-start justify-center p-6 overflow-y-auto">
          <div className="bg-[#1e293b] border border-[#334155] rounded-2xl w-full max-w-4xl shadow-2xl">
            <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155]">
              <div>
                <h2 className="text-lg font-bold text-slate-100">Parte de {selected.nombre}</h2>
                <p className="text-sm text-slate-400">
                  {selected.periodo_display} · Legajo {selected.legajo}
                </p>
              </div>
              <button onClick={() => setSelected(null)} className="text-slate-400 hover:text-slate-200">
                <X size={20} />
              </button>
            </div>

            <div className="p-6 space-y-5">
              {loadingParte ? (
                <div className="flex items-center justify-center py-12">
                  <RefreshCw size={20} className="animate-spin text-sky-400 mr-2" />
                  <span className="text-slate-400">Cargando parte...</span>
                </div>
              ) : parteSel ? (
                <>
                  {/* Grilla en modo lectura */}
                  <div className="overflow-x-auto rounded-xl border border-[#334155] max-h-80 overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="sticky top-0">
                        <tr className="bg-[#1e293b] border-b border-[#334155]">
                          <th className="px-4 py-2 text-left text-slate-400 font-medium">Fecha</th>
                          {["G","F","D","HO","HV","HE"].map(t => (
                            <th key={t} className="px-3 py-2 text-center text-slate-400 font-medium">{t}</th>
                          ))}
                          <th className="px-4 py-2 text-left text-slate-400 font-medium">Comentario</th>
                        </tr>
                      </thead>
                      <tbody>
                        {parteSel.grilla.map((row, i) => (
                          <tr key={row.fecha}
                              className={`border-b border-[#1e293b] ${i % 2 === 0 ? "bg-[#0f172a]" : "bg-[#111827]"}`}>
                            <td className="px-4 py-1.5 text-xs font-mono text-slate-300">
                              {row.fecha.slice(8)}/{row.fecha.slice(5,7)}
                            </td>
                            {(["G","F","D","HO"] as const).map(t => (
                              <td key={t} className="px-3 py-1.5 text-center text-sm">
                                {row[t] ? <span className="text-emerald-400">✓</span> : <span className="text-slate-700">—</span>}
                              </td>
                            ))}
                            <td className="px-3 py-1.5 text-center text-slate-300 text-xs">{row.HV || "—"}</td>
                            <td className="px-3 py-1.5 text-center text-slate-300 text-xs">{row.HE || "—"}</td>
                            <td className="px-4 py-1.5 text-xs text-slate-400">{row.comentario || ""}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  {/* RRHHTotales */}
                  <div className="grid grid-cols-6 gap-2">
                    {(["G","F","D","HO","HV","HE"] as const).map(t => (
                      <div key={t} className="bg-[#0f172a] border border-[#334155] rounded-lg p-2 text-center">
                        <p className="text-xs text-slate-500">{t}</p>
                        <p className="text-lg font-bold text-sky-400">
                          {t === "HV" || t === "HE" ? parteSel.totales[t].toFixed(1) : parteSel.totales[t]}
                        </p>
                      </div>
                    ))}
                  </div>

                  {/* Acciones */}
                  {accion === null && (
                    <div className="flex gap-3">
                      <button
                        onClick={() => setAccion("aprobar")}
                        className="flex items-center gap-2 px-5 py-2.5 bg-emerald-700 hover:bg-emerald-600 text-white rounded-lg text-sm font-medium transition-colors"
                      >
                        <Check size={16} /> Aprobar
                      </button>
                      <button
                        onClick={() => setAccion("rechazar")}
                        className="flex items-center gap-2 px-5 py-2.5 bg-red-800 hover:bg-red-700 text-white rounded-lg text-sm font-medium transition-colors"
                      >
                        <X size={16} /> Rechazar
                      </button>
                    </div>
                  )}

                  {accion === "aprobar" && (
                    <div className="bg-emerald-900/20 border border-emerald-700/30 rounded-xl p-4 space-y-3">
                      <p className="text-sm text-emerald-300">¿Confirmás la aprobación del parte?</p>
                      <div className="flex gap-3">
                        <button
                          onClick={handleAprobar}
                          disabled={procesando}
                          className="px-4 py-2 bg-emerald-700 hover:bg-emerald-600 text-white rounded-lg text-sm font-medium transition-colors disabled:opacity-50"
                        >
                          {procesando ? "Procesando..." : "✅ Confirmar aprobación"}
                        </button>
                        <button onClick={() => setAccion(null)} className="px-4 py-2 text-slate-400 hover:text-slate-200 text-sm">
                          Cancelar
                        </button>
                      </div>
                    </div>
                  )}

                  {accion === "rechazar" && (
                    <div className="bg-red-900/20 border border-red-800/30 rounded-xl p-4 space-y-3">
                      <label className="block text-sm text-red-300 font-medium">Motivo del rechazo (obligatorio)</label>
                      <textarea
                        value={comentario}
                        onChange={e => setComentario(e.target.value)}
                        placeholder="Describí el motivo..."
                        rows={3}
                        className="w-full bg-[#0f172a] border border-[#334155] rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-red-500 resize-none"
                      />
                      <div className="flex gap-3">
                        <button
                          onClick={handleRechazar}
                          disabled={procesando || !comentario.trim()}
                          className="px-4 py-2 bg-red-700 hover:bg-red-600 text-white rounded-lg text-sm font-medium transition-colors disabled:opacity-50"
                        >
                          {procesando ? "Procesando..." : "❌ Confirmar rechazo"}
                        </button>
                        <button onClick={() => setAccion(null)} className="px-4 py-2 text-slate-400 hover:text-slate-200 text-sm">
                          Cancelar
                        </button>
                      </div>
                    </div>
                  )}
                </>
              ) : null}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ==========================================================
// Tab: Consolidado (Líder)
// ==========================================================

function TabConsolidado({ user, periodos }: { user: RRHHUser; periodos: RRHHPeriodo[] }) {
  const [periodoId, setPeriodoId] = useState(periodos[0]?.id || "");
  const [data, setData] = useState<{
    periodo_display: string;
    periodo_inicio: string;
    periodo_fin: string;
    empleados: {
      legajo: string; nombre: string; funcion: string; estado: string;
      G: number; F: number; D: number; HO: number; HV: number; HE: number;
      dias: { fecha: string; tipos: string[]; HV: number; HE: number; comentario: string }[];
    }[];
  } | null>(null);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [downloading, setDownloading] = useState(false);

  const loadConsolidado = useCallback(async (pid: string) => {
    setLoading(true);
    try {
      const d = await api.rrhhConsolidado(user.legajo, pid);
      setData(d);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  }, [user.legajo]);

  useEffect(() => {
    if (periodoId) loadConsolidado(periodoId);
  }, [periodoId, loadConsolidado]);

  const handleDownload = async () => {
    setDownloading(true);
    try {
      const res = await fetch(`${API_URL}/api/rrhh/consolidado/${user.legajo}/${periodoId}/excel`);
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `consolidado_${user.legajo}_${periodoId}.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) { console.error(e); }
    finally { setDownloading(false); }
  };

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <label className="text-sm text-slate-400">Período:</label>
          <select
            value={periodoId}
            onChange={e => { setPeriodoId(e.target.value); setExpanded(null); }}
            className="bg-[#0f172a] border border-[#334155] rounded-lg px-3 py-1.5 text-sm text-slate-200 focus:outline-none focus:border-sky-500"
          >
            {periodos.map(p => (
              <option key={p.id} value={p.id}>
                {p.display} ({p.start.slice(5,10).split("-").reverse().join("/")} → {p.end.slice(5,10).split("-").reverse().join("/")})
              </option>
            ))}
          </select>
        </div>
        <button
          onClick={handleDownload}
          disabled={downloading || loading || !data}
          className="flex items-center gap-2 px-4 py-2 bg-emerald-700 hover:bg-emerald-600 disabled:opacity-50 text-white rounded-lg text-sm font-medium transition-colors"
        >
          {downloading ? <RefreshCw size={14} className="animate-spin" /> : <Download size={14} />}
          {downloading ? "Generando Excel..." : "Descargar Excel"}
        </button>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-16">
          <RefreshCw size={20} className="animate-spin text-sky-400 mr-2" />
          <span className="text-slate-400">Cargando consolidado...</span>
        </div>
      ) : data ? (
        <div className="space-y-3">
          <p className="text-sm text-slate-400">
            {data.periodo_display} — {formatFecha(data.periodo_inicio)} → {formatFecha(data.periodo_fin)}
            {" · "}<span className="text-sky-400 font-medium">{data.empleados.length} empleados</span>
          </p>

          {/* Tabla resumen */}
          <div className="overflow-x-auto rounded-xl border border-[#334155]">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-[#1e293b] border-b border-[#334155]">
                  <th className="px-4 py-2.5 text-left text-slate-400 font-medium">Empleado</th>
                  <th className="px-3 py-2.5 text-left text-slate-400 font-medium">Estado</th>
                  {["G","F","D","HO","HV","HE"].map(t => (
                    <th key={t} className="px-3 py-2.5 text-center text-slate-400 font-medium w-14">{t}</th>
                  ))}
                  <th className="px-3 py-2.5 text-center text-slate-400 font-medium">Detalle</th>
                </tr>
              </thead>
              <tbody>
                {data.empleados.map((emp, i) => (
                  <React.Fragment key={emp.legajo}>
                    <tr
                      key={emp.legajo}
                      className={`border-b border-[#1e293b] ${i % 2 === 0 ? "bg-[#0f172a]" : "bg-[#111827]"}`}
                    >
                      <td className="px-4 py-2.5">
                        <p className="text-slate-200 font-medium">{emp.nombre}</p>
                        <p className="text-xs text-slate-500">{emp.funcion || "—"}</p>
                      </td>
                      <td className="px-3 py-2.5"><EstadoBadge estado={emp.estado} /></td>
                      {(["G","F","D","HO","HV","HE"] as const).map(t => (
                        <td key={t} className="px-3 py-2.5 text-center text-slate-300 tabular-nums">
                          {t === "HV" || t === "HE" ? emp[t].toFixed(1) : emp[t]}
                        </td>
                      ))}
                      <td className="px-3 py-2.5 text-center">
                        <button
                          onClick={() => setExpanded(e => e === emp.legajo ? null : emp.legajo)}
                          className="text-sky-400 hover:text-sky-300 transition-colors"
                        >
                          {expanded === emp.legajo
                            ? <ChevronUp size={16} />
                            : <ChevronDown size={16} />}
                        </button>
                      </td>
                    </tr>

                    {/* Fila expandible con detalle día a día */}
                    {expanded === emp.legajo && (
                      <tr key={`${emp.legajo}-det`} className="bg-[#0a1628]">
                        <td colSpan={10} className="px-6 py-4">
                          <div className="overflow-x-auto rounded-lg border border-[#334155]">
                            <table className="w-full text-xs">
                              <thead>
                                <tr className="bg-[#1e293b]">
                                  <th className="px-3 py-2 text-left text-slate-400">Fecha</th>
                                  {["G","F","D","HO","HV","HE"].map(t => (
                                    <th key={t} className="px-3 py-2 text-center text-slate-400">{t}</th>
                                  ))}
                                  <th className="px-3 py-2 text-left text-slate-400">Comentario</th>
                                </tr>
                              </thead>
                              <tbody>
                                {emp.dias.map((dia, j) => (
                                  <tr
                                    key={dia.fecha}
                                    className={`border-t border-[#1e293b] ${j % 2 === 0 ? "bg-[#0f172a]" : "bg-[#111827]"}`}
                                  >
                                    <td className="px-3 py-1.5 font-mono text-slate-300">
                                      {dia.fecha.slice(8)}/{dia.fecha.slice(5,7)}
                                    </td>
                                    {["G","F","D","HO"].map(t => (
                                      <td key={t} className="px-3 py-1.5 text-center">
                                        {dia.tipos.includes(t)
                                          ? <span className="text-emerald-400">✓</span>
                                          : <span className="text-slate-700">—</span>}
                                      </td>
                                    ))}
                                    <td className="px-3 py-1.5 text-center text-slate-300">{dia.HV || "—"}</td>
                                    <td className="px-3 py-1.5 text-center text-slate-300">{dia.HE || "—"}</td>
                                    <td className="px-3 py-1.5 text-slate-400">{dia.comentario || ""}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}

                {/* Fila de totales */}
                <tr className="bg-[#1e2d45] border-t-2 border-sky-900/50">
                  <td className="px-4 py-3 font-bold text-slate-200" colSpan={2}>TOTALES</td>
                  {(["G","F","D","HO","HV","HE"] as const).map(t => (
                    <td key={t} className="px-3 py-3 text-center font-bold text-sky-400 tabular-nums">
                      {t === "HV" || t === "HE"
                        ? data.empleados.reduce((s, e) => s + e[t], 0).toFixed(1)
                        : data.empleados.reduce((s, e) => s + e[t], 0)}
                    </td>
                  ))}
                  <td />
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      ) : null}
    </div>
  );
}

// ==========================================================
// Tab: Personal (Líder — gestión)
// ==========================================================

function TabPersonal({ user }: { user: RRHHUser }) {
  const [personal, setPersonal] = useState<{legajo:string;cuil:string;nombre:string;leader_legajo:string;funcion?:string;origen?:string;lugar_trabajo?:string}[]>([]);
  const [loading, setLoading] = useState(false);
  const [importJson, setImportJson] = useState("");
  const [importMsg, setImportMsg] = useState<{ tipo: "ok" | "err"; texto: string } | null>(null);
  const [importing, setImporting] = useState(false);

  useEffect(() => {
    setLoading(true);
    api.rrhhPersonal()
      .then(r => setPersonal(r.personal))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const handleImport = async () => {
    setImporting(true); setImportMsg(null);
    try {
      const rows = JSON.parse(importJson);
      const res = await api.rrhhImportPersonal(rows);
      setImportMsg({ tipo: "ok", texto: `OK. Insertados: ${res.insertados} | Actualizados: ${res.actualizados}` });
      setImportJson("");
      clearApiCache("/api/rrhh/personal");
      const d = await api.rrhhPersonal();
      setPersonal(d.personal);
    } catch (e: unknown) {
      setImportMsg({ tipo: "err", texto: e instanceof Error ? e.message : "Error al importar." });
    } finally { setImporting(false); }
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-base font-semibold text-slate-200 mb-3">Personal registrado</h3>
        {loading ? (
          <div className="flex items-center gap-2 text-slate-400 text-sm py-4">
            <RefreshCw size={14} className="animate-spin" /> Cargando...
          </div>
        ) : (
          <div className="overflow-x-auto rounded-xl border border-[#334155]">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-[#1e293b] border-b border-[#334155]">
                  {["Legajo","Nombre","CUIL","Líder","Función","Origen","Lugar trabajo"].map(h => (
                    <th key={h} className="px-4 py-2.5 text-left text-slate-400 font-medium whitespace-nowrap">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {personal.map((p, i) => (
                  <tr key={p.legajo} className={`border-b border-[#1e293b] ${i % 2 === 0 ? "bg-[#0f172a]" : "bg-[#111827]"}`}>
                    <td className="px-4 py-2 font-mono text-slate-300 text-xs">{p.legajo}</td>
                    <td className="px-4 py-2 text-slate-200">{p.nombre}</td>
                    <td className="px-4 py-2 font-mono text-slate-400 text-xs">{p.cuil}</td>
                    <td className="px-4 py-2 text-slate-400 text-xs">{p.leader_legajo}</td>
                    <td className="px-4 py-2 text-slate-400 text-xs">{p.funcion || "—"}</td>
                    <td className="px-4 py-2 text-slate-400 text-xs">{p.origen || "—"}</td>
                    <td className="px-4 py-2 text-slate-400 text-xs">{p.lugar_trabajo || "—"}</td>
                  </tr>
                ))}
                {!personal.length && (
                  <tr><td colSpan={7} className="px-4 py-6 text-center text-slate-500">Sin personal registrado.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div>
        <h3 className="text-base font-semibold text-slate-200 mb-2">Importar personal (JSON)</h3>
        <p className="text-xs text-slate-500 mb-3">
          Pegá un JSON con el siguiente formato: <code className="bg-slate-800 px-1 rounded">{"[{legajo, cuil, nombre, leader_legajo, funcion?, origen?, lugar_trabajo?}]"}</code>
        </p>
        <textarea
          value={importJson}
          onChange={e => setImportJson(e.target.value)}
          rows={5}
          placeholder={`[\n  {"legajo":"5478","cuil":"20359612835","nombre":"Juan Pérez","leader_legajo":"9001","funcion":"Operador"}\n]`}
          className="w-full bg-[#0f172a] border border-[#334155] rounded-xl px-4 py-3 text-sm text-slate-200 font-mono focus:outline-none focus:border-sky-500 resize-none"
        />
        {importMsg && (
          <div className={`mt-2 text-sm px-3 py-2 rounded-lg ${
            importMsg.tipo === "ok" ? "text-emerald-300 bg-emerald-900/20" : "text-red-400 bg-red-900/20"
          }`}>
            {importMsg.texto}
          </div>
        )}
        <button
          onClick={handleImport}
          disabled={importing || !importJson.trim()}
          className="mt-3 flex items-center gap-2 px-4 py-2 bg-sky-600 hover:bg-sky-500 disabled:opacity-50 text-white rounded-lg text-sm font-medium transition-colors"
        >
          {importing ? <RefreshCw size={14} className="animate-spin" /> : null}
          {importing ? "Importando..." : "Importar"}
        </button>
      </div>
    </div>
  );
}

// ==========================================================
// App principal
// ==========================================================

type Tab = "mi-parte" | "bitacora" | "equipo" | "consolidado" | "personal";

export default function RRHHPage() {
  const [user, setUser] = useState<RRHHUser | null>(null);
  const [periodos, setPeriodos] = useState<RRHHPeriodo[]>([]);
  const [activeTab, setActiveTab] = useState<Tab>("mi-parte");

  useEffect(() => {
    // Intentar recuperar sesión desde sessionStorage
    try {
      const stored = sessionStorage.getItem("rrhh_user");
      if (stored) setUser(JSON.parse(stored));
    } catch { /* noop */ }
  }, []);

  useEffect(() => {
    if (!user) return;
    api.rrhhPeriodos(8)
      .then(r => setPeriodos(r.periodos))
      .catch(console.error);
  }, [user]);

  const handleLogin = (u: RRHHUser) => {
    setUser(u);
    try { sessionStorage.setItem("rrhh_user", JSON.stringify(u)); } catch { /* noop */ }
  };

  const handleLogout = () => {
    setUser(null);
    try { sessionStorage.removeItem("rrhh_user"); } catch { /* noop */ }
  };

  if (!user) return <LoginForm onLogin={handleLogin} />;

  const esLider = user.role === "lider";

  const TABS: { id: Tab; label: string; icon: React.ReactNode; onlyLider?: boolean }[] = [
    { id: "mi-parte",    label: "Mi Parte",    icon: <Calendar size={15} /> },
    { id: "bitacora",    label: "Bitácora",    icon: <ClipboardList size={15} /> },
    { id: "equipo",      label: "Mi Equipo",   icon: <Users size={15} />,         onlyLider: true },
    { id: "consolidado", label: "Consolidado", icon: <FileSpreadsheet size={15} />, onlyLider: true },
    { id: "personal",    label: "Personal",    icon: <User size={15} />,          onlyLider: true },
  ];

  const visibleTabs = TABS.filter(t => !t.onlyLider || esLider);

  return (
    <div className="flex flex-col min-h-screen bg-[#0f172a]">
      {/* Header */}
      <div className="bg-[#1e293b] border-b border-[#334155] px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 bg-sky-500/20 rounded-xl flex items-center justify-center">
            <Shield size={18} className="text-sky-400" />
          </div>
          <div>
            <h1 className="text-lg font-bold text-slate-100">RRHH — Guardias</h1>
            <p className="text-xs text-slate-400">
              {user.nombre} · Legajo {user.legajo}
              {" · "}
              <span className={`font-medium ${esLider ? "text-sky-400" : "text-slate-300"}`}>
                {esLider ? "👑 Líder" : "👤 Empleado"}
              </span>
            </p>
          </div>
        </div>
        <button
          onClick={handleLogout}
          className="flex items-center gap-2 text-sm text-slate-400 hover:text-slate-200 transition-colors"
        >
          <LogOut size={15} /> Cerrar sesión
        </button>
      </div>

      {/* Tabs */}
      <div className="border-b border-[#334155] bg-[#1e293b]/50 px-6">
        <div className="flex gap-1">
          {visibleTabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex items-center gap-2 px-4 py-3 text-sm transition-colors border-b-2 -mb-px ${
                activeTab === tab.id
                  ? "border-sky-500 text-sky-400 font-medium"
                  : "border-transparent text-slate-400 hover:text-slate-200"
              }`}
            >
              {tab.icon} {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Contenido */}
      <div className="flex-1 p-6 overflow-auto">
        {activeTab === "mi-parte" && periodos.length > 0 && (
          <TabMiParte user={user} periodos={periodos} />
        )}
        {activeTab === "bitacora" && <TabBitacora user={user} />}
        {activeTab === "equipo" && esLider && periodos.length > 0 && (
          <TabEquipo user={user} />
        )}
        {activeTab === "consolidado" && esLider && periodos.length > 0 && (
          <TabConsolidado user={user} periodos={periodos} />
        )}
        {activeTab === "personal" && esLider && (
          <TabPersonal user={user} />
        )}
      </div>
    </div>
  );
}
