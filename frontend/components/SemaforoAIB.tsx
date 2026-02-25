"use client";

import { useState } from "react";
import KPICard from "./KPICard";
import { SemaforoRow } from "@/lib/api";

const SEV_COLOR: Record<string, string> = {
  "🟢 NORMAL":   "bg-green-500/10  text-green-400  border-green-500/30",
  "🟡 ALERTA":   "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  "🔴 CRÍTICO":  "bg-red-500/10    text-red-400    border-red-500/30",
  "SIN DATOS":   "bg-slate-500/10  text-slate-400  border-slate-500/30",
  "NO APLICA":   "bg-slate-700/10  text-slate-500  border-slate-700/30",
};

const TABLE_HEADERS = [
  "Estado","NO_key","ORIGEN","DT_plot","Días","SE",
  "PB","Sumergencia","Bba Llenado","Sumergencia_base",
  "%Estructura","%Balance","GPM","Caudal bruto efec"
];

interface SemaforoAIBProps {
  rows: SemaforoRow[];
  kpis: { total: number; criticos: number; alertas: number; normales: number; sin_datos: number; };
  onRefresh?: () => void;
  sumMedia: number; sumAlta: number; llenOk: number; llenBajo: number;
  setSumMedia: (v: number) => void; setSumAlta: (v: number) => void;
  setLlenOk: (v: number) => void;  setLlenBajo: (v: number) => void;
}

function TableRow({ r }: { r: SemaforoRow }) {
  return (
    <tr className="border-b border-[#334155] hover:bg-slate-800/40">
      <td className="px-3 py-2">
        <span className={`text-xs px-2 py-0.5 rounded border ${SEV_COLOR[r.Semaforo_AIB] || SEV_COLOR["SIN DATOS"]}`}>
          {r.Semaforo_AIB}
        </span>
      </td>
      <td className="px-3 py-2 font-mono text-xs text-slate-300 whitespace-nowrap">{r.NO_key}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r.ORIGEN || "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400 whitespace-nowrap">{r.DT_plot?.slice(0,16) || "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r.Dias_desde_ultima != null ? r.Dias_desde_ultima.toFixed(1) : "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r.SE || "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r.PB ?? "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r.Sumergencia != null ? r.Sumergencia.toFixed(1) : "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r["Bba Llenado"] ?? "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r.Sumergencia_base || "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r["%Estructura"] ?? "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r["%Balance"] ?? "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r.GPM ?? "—"}</td>
      <td className="px-3 py-2 text-xs text-slate-400">{r["Caudal bruto efec"] ?? "—"}</td>
    </tr>
  );
}

export default function SemaforoAIB({
  rows, kpis, onRefresh,
  sumMedia, sumAlta, llenOk, llenBajo,
  setSumMedia, setSumAlta, setLlenOk, setLlenBajo,
}: SemaforoAIBProps) {
  const [filtro, setFiltro]           = useState("Todos");
  const [origenSel, setOrigenSel]     = useState<string[]>([]);
  const [soloSeAib, setSoloSeAib]     = useState(false);
  const [soloConLlen, setSoloConLlen] = useState(false);
  const [fechaDesde, setFechaDesde]   = useState("");
  const [fechaHasta, setFechaHasta]   = useState("");
  const [filtrosOpen, setFiltrosOpen] = useState(true);

  const origenOpts = [...new Set(rows.map((r) => r.ORIGEN).filter(Boolean) as string[])].sort();

  let rowsFiltradas = [...rows];
  if (origenSel.length > 0) rowsFiltradas = rowsFiltradas.filter((r) => origenSel.includes(r.ORIGEN || ""));
  if (soloSeAib)    rowsFiltradas = rowsFiltradas.filter((r) => r.SE?.trim().toUpperCase() === "AIB");
  if (soloConLlen)  rowsFiltradas = rowsFiltradas.filter((r) => r["Bba Llenado"] != null);
  if (fechaDesde)   rowsFiltradas = rowsFiltradas.filter((r) => !r.DT_plot || r.DT_plot.slice(0,10) >= fechaDesde);
  if (fechaHasta)   rowsFiltradas = rowsFiltradas.filter((r) => !r.DT_plot || r.DT_plot.slice(0,10) <= fechaHasta);

  const totalAib = rowsFiltradas.filter((r) => r.SE?.trim().toUpperCase() === "AIB").length;
  const normales = rowsFiltradas.filter((r) => r.Semaforo_AIB === "🟢 NORMAL").length;
  const alertas  = rowsFiltradas.filter((r) => r.Semaforo_AIB === "🟡 ALERTA").length;
  const criticos = rowsFiltradas.filter((r) => r.Semaforo_AIB === "🔴 CRÍTICO").length;
  const sinDatos = rowsFiltradas.filter((r) => r.Semaforo_AIB === "SIN DATOS").length;

  const criticosRows = rowsFiltradas
    .filter((r) => r.Semaforo_AIB === "🔴 CRÍTICO")
    .sort((a, b) => (b.Sumergencia ?? 0) - (a.Sumergencia ?? 0));

  const opciones = ["Todos", "🔴 CRÍTICO", "🟡 ALERTA", "🟢 NORMAL", "SIN DATOS"];
  const filtered = filtro === "Todos" ? rowsFiltradas : rowsFiltradas.filter((r) => r.Semaforo_AIB === filtro);

  return (
    <div className="space-y-4">

      {/* Filtros independientes */}
      <div className="card">
        <button
          onClick={() => setFiltrosOpen((o) => !o)}
          className="flex items-center gap-2 text-sm font-semibold text-slate-300 w-full text-left"
        >
          <span>{filtrosOpen ? "▼" : "▶"}</span>
          Filtros Semáforo AIB (independientes)
        </button>
        {filtrosOpen && (
          <div className="mt-3 flex flex-wrap gap-6 items-start">
            <div>
              <label className="text-xs text-slate-400 block mb-1">Origen (AIB)</label>
              <div className="flex gap-2 flex-wrap">
                {origenOpts.map((o) => (
                  <label key={o} className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer">
                    <input type="checkbox" checked={origenSel.includes(o)}
                      onChange={(e) => setOrigenSel((prev) => e.target.checked ? [...prev, o] : prev.filter((x) => x !== o))}
                      className="accent-sky-400" />
                    {o}
                  </label>
                ))}
                {origenSel.length > 0 && (
                  <button onClick={() => setOrigenSel([])} className="text-xs text-slate-500 hover:text-slate-300">Limpiar</button>
                )}
              </div>
            </div>
            <div>
              <label className="text-xs text-slate-400 block mb-1">Rango fechas (DT_plot) — AIB</label>
              <div className="flex gap-2 items-center">
                <input type="text" value={fechaDesde} onChange={(e) => setFechaDesde(e.target.value)}
                  placeholder="2025-01-01"
                  className="bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-xs text-slate-200 w-28" />
                <span className="text-slate-500 text-xs">–</span>
                <input type="text" value={fechaHasta} onChange={(e) => setFechaHasta(e.target.value)}
                  placeholder="2026-12-31"
                  className="bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-xs text-slate-200 w-28" />
                {(fechaDesde || fechaHasta) && (
                  <button onClick={() => { setFechaDesde(""); setFechaHasta(""); }}
                    className="text-xs text-slate-500 hover:text-red-400 ml-1">✕</button>
                )}
              </div>
            </div>
            <div className="flex flex-col gap-2">
              <label className="flex items-center gap-2 text-xs text-slate-300 cursor-pointer">
                <input type="checkbox" checked={soloSeAib} onChange={(e) => setSoloSeAib(e.target.checked)} className="accent-sky-400" />
                Solo SE = AIB
              </label>
              <label className="flex items-center gap-2 text-xs text-slate-300 cursor-pointer">
                <input type="checkbox" checked={soloConLlen} onChange={(e) => setSoloConLlen(e.target.checked)} className="accent-sky-400" />
                Solo con Bba Llenado
              </label>
            </div>
          </div>
        )}
      </div>

      {/* Umbrales */}
      <div className="card">
        <h3 className="text-sm font-semibold text-slate-300 mb-3">⚙️ Umbrales Semáforo AIB (independientes)</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[
            { label: "Umbral Sumergencia media (m)", val: sumMedia, set: setSumMedia },
            { label: "Umbral Sumergencia alta (m)",  val: sumAlta,  set: setSumAlta  },
            { label: "Llenado OK (≥ %)",             val: llenOk,   set: setLlenOk   },
            { label: "Llenado bajo (< %)",           val: llenBajo, set: setLlenBajo  },
          ].map(({ label, val, set }) => (
            <div key={label}>
              <label className="text-xs text-slate-400 block mb-1">{label}</label>
              <input type="number" value={val} onChange={(e) => set(+e.target.value)}
                className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 w-full" />
            </div>
          ))}
        </div>
        <div className="mt-3">
          <button
            onClick={() => onRefresh && onRefresh()}
            className="text-xs px-4 py-1.5 rounded border border-sky-500 text-sky-400 hover:bg-sky-500/10 transition-colors"
          >
            ↻ Aplicar umbrales
          </button>
          <span className="text-xs text-slate-500 ml-2">Los umbrales se calculan en el servidor</span>
        </div>
      </div>

      {/* KPIs recalculados */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <KPICard title="Pozos AIB (independiente)" value={totalAib} />
        <KPICard title="🟢 Normal"  value={normales} color="green" />
        <KPICard title="🟡 Alerta"  value={alertas}  color="yellow" />
        <KPICard title="🔴 Crítico" value={criticos} color="red" />
        <KPICard title="Sin datos"  value={sinDatos} />
      </div>

      {/* Tabla críticos */}
      {criticosRows.length > 0 ? (
        <div className="card p-0 overflow-hidden">
          <div className="px-4 py-3 border-b border-[#334155]">
            <h3 className="text-sm font-medium text-red-400">🔴 AIB Crítico — prioridad (independiente)</h3>
          </div>
          <div className="overflow-x-auto overflow-y-auto max-h-72">
            <table className="w-full text-xs">
              <thead className="sticky top-0 z-10">
                <tr>{TABLE_HEADERS.map((h) => (
                  <th key={h} className="text-left text-xs text-slate-500 px-3 py-2 bg-[#1e293b] border-b border-[#334155] whitespace-nowrap">{h}</th>
                ))}</tr>
              </thead>
              <tbody>{criticosRows.map((r, i) => <TableRow key={i} r={r} />)}</tbody>
            </table>
          </div>
        </div>
      ) : (
        <p className="text-xs text-slate-500 px-1">No hay pozos en 🔴 CRÍTICO con los umbrales y filtros actuales.</p>
      )}

      {/* Tabla completa */}
      <div className="card p-0 overflow-hidden">
        <div className="px-4 py-3 border-b border-[#334155]">
          <h3 className="text-sm font-medium text-slate-300">📋 Semáforo AIB — tabla (independiente)</h3>
        </div>
        <div className="px-4 py-2 border-b border-[#334155] flex gap-2 flex-wrap items-center">
          {opciones.map((op) => (
            <button key={op} onClick={() => setFiltro(op)}
              className={`text-xs px-3 py-1 rounded-full border transition-colors ${
                filtro === op ? "bg-sky-400/10 border-sky-400 text-sky-300" : "border-slate-600 text-slate-400 hover:border-slate-400"
              }`}>
              {op}
            </button>
          ))}
          {onRefresh && (
            <button onClick={onRefresh}
              className="ml-auto text-xs px-3 py-1 rounded-full border border-slate-600 text-slate-400 hover:border-sky-400 hover:text-sky-400 transition-colors">
              ↻ Actualizar
            </button>
          )}
        </div>
        <div className="overflow-x-auto overflow-y-auto max-h-[480px]">
          <table className="w-full text-xs">
            <thead className="sticky top-0 z-10">
              <tr>{TABLE_HEADERS.map((h) => (
                <th key={h} className="text-left text-xs text-slate-500 px-3 py-2.5 bg-[#1e293b] border-b border-[#334155] whitespace-nowrap">{h}</th>
              ))}</tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => <TableRow key={i} r={r} />)}
              {filtered.length === 0 && (
                <tr><td colSpan={TABLE_HEADERS.length} className="px-3 py-8 text-center text-slate-500">Sin pozos en este estado.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
