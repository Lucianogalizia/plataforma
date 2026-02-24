"use client";

import { useState } from "react";
import api, { SemaforoRow } from "@/lib/api";
import KPICard from "./KPICard";

const SEV_COLOR: Record<string, string> = {
  "🟢 NORMAL":   "bg-green-500/10  text-green-400  border-green-500/30",
  "🟡 ALERTA":   "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  "🔴 CRÍTICO":  "bg-red-500/10    text-red-400    border-red-500/30",
  "SIN DATOS":   "bg-slate-500/10  text-slate-400  border-slate-500/30",
  "NO APLICA":   "bg-slate-700/10  text-slate-500  border-slate-700/30",
};

interface SemaforoAIBProps {
  rows: SemaforoRow[];
  kpis: {
    total: number;
    criticos: number;
    alertas: number;
    normales: number;
    sin_datos: number;
  };
  onRefresh?: () => void;
}

export default function SemaforoAIB({ rows, kpis, onRefresh }: SemaforoAIBProps) {
  const [filtro, setFiltro] = useState<string>("Todos");

  const opciones = ["Todos", "🔴 CRÍTICO", "🟡 ALERTA", "🟢 NORMAL", "SIN DATOS"];
  const filtered =
    filtro === "Todos" ? rows : rows.filter((r) => r.Semaforo_AIB === filtro);

  return (
    <div className="space-y-4">
      {/* KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <KPICard title="AIB Totales" value={kpis.total} />
        <KPICard title="🔴 Críticos"  value={kpis.criticos}  color="red" />
        <KPICard title="🟡 Alertas"   value={kpis.alertas}   color="yellow" />
        <KPICard title="🟢 Normales"  value={kpis.normales}  color="green" />
        <KPICard title="Sin datos"    value={kpis.sin_datos} />
      </div>

      {/* Filtro rápido */}
      <div className="flex gap-2 flex-wrap">
        {opciones.map((op) => (
          <button
            key={op}
            onClick={() => setFiltro(op)}
            className={`text-xs px-3 py-1.5 rounded-full border transition-colors ${
              filtro === op
                ? "bg-sky-400/10 border-sky-400 text-sky-300"
                : "border-slate-600 text-slate-400 hover:border-slate-400"
            }`}
          >
            {op}
          </button>
        ))}
        {onRefresh && (
          <button
            onClick={onRefresh}
            className="ml-auto text-xs px-3 py-1.5 rounded-full border border-slate-600 text-slate-400 hover:border-sky-400 hover:text-sky-400 transition-colors"
          >
            ↻ Actualizar
          </button>
        )}
      </div>

      {/* Tabla */}
      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto max-h-[520px] overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 z-10">
              <tr>
                {["Estado", "Pozo", "Origen", "Días", "SE", "PB", "Sumergencia", "Llenado %", "%Bal", "%Est"].map((h) => (
                  <th key={h} className="text-left text-xs text-slate-500 px-3 py-2.5 bg-[#1e293b] border-b border-[#334155] whitespace-nowrap">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => (
                <tr key={i} className="border-b border-[#334155] hover:bg-slate-800/40">
                  <td className="px-3 py-2">
                    <span className={`text-xs px-2 py-0.5 rounded border ${SEV_COLOR[r.Semaforo_AIB] || SEV_COLOR["SIN DATOS"]}`}>
                      {r.Semaforo_AIB}
                    </span>
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-slate-300">{r.NO_key}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.ORIGEN || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">
                    {r.Dias_desde_ultima != null ? r.Dias_desde_ultima.toFixed(0) : "—"}
                  </td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.SE || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.PB ?? "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">
                    {r.Sumergencia != null ? r.Sumergencia.toFixed(1) : "—"}
                  </td>
                  <td className="px-3 py-2 text-xs text-slate-400">
                    {r["Bba Llenado"] != null ? `${r["Bba Llenado"]}%` : "—"}
                  </td>
                  <td className="px-3 py-2 text-xs text-slate-400">
                    {r["%Balance"] != null ? `${r["%Balance"]}%` : "—"}
                  </td>
                  <td className="px-3 py-2 text-xs text-slate-400">
                    {r["%Estructura"] != null ? `${r["%Estructura"]}%` : "—"}
                  </td>
                </tr>
              ))}
              {filtered.length === 0 && (
                <tr>
                  <td colSpan={10} className="px-3 py-8 text-center text-slate-500 text-sm">
                    Sin pozos en este estado.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
