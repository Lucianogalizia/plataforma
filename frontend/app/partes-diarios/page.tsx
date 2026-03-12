"use client";

import { useEffect, useState, useCallback } from "react";
import api, { clearApiCache } from "@/lib/api";

interface ParteDiarioInfo {
  exists: boolean;
  updated_at: string | null;
  file?: string;
  size_kb?: number;
  rows?: number;
  columns?: string[];
  error?: string;
}

interface ParteDiarioRow {
  well_legal_name?: string;
  date_report?: string;
  time_from?: string;
  time_to?: string;
  activity_duration?: number | null;
  activity_class?: string;
  activity_code?: string;
  expr1?: string;
  status_end?: string;
  rig_name?: string;
  contractor_name?: string;
  event_objective_1?: string;
  [key: string]: unknown;
}

const COLS_OCULTAR = ["_INGESTED_AT", "_SOURCE_FILE", "well_id", "event_id", "entity_type", "event_code", "event_type", "well_legal_name"];

const COL_LABELS: Record<string, string> = {
  date_report: "Fecha",
  time_from: "Inicio",
  time_to: "Fin",
  activity_duration: "Duración (h)",
  activity_class: "Clase",
  activity_code: "Código",
  activity_phase: "Sub-código",
  activity_subcode: "Tarifa",
  expr1: "Descripción",
  status_end: "Estado",
  rig_name: "Equipo",
  contractor_name: "Contratista",
  event_objective_1: "Tipo Intervención",
  event_objective_2: "Objetivo",
  step_no: "Paso",
};

export default function PartesDiariosPage() {
  const [info, setInfo] = useState<ParteDiarioInfo | null>(null);
  const [rows, setRows] = useState<ParteDiarioRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingRows, setLoadingRows] = useState(false);
  const [pozoFiltro, setPozoFiltro] = useState("");
  const [pozos, setPozos] = useState<{ pozo: string; status: string }[]>([]);

  const cargarInfo = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.getPartesDiariosInfo();
      setInfo(data);
    } catch {
      setInfo({ exists: false, updated_at: null, error: "Error conectando con el backend" });
    }
    setLoading(false);
  }, []);

  const cargarPozos = useCallback(async () => {
    try {
      const data = await api.getPartesDiariosPozos();
      setPozos(data.pozos);
    } catch {
      setPozos([]);
    }
  }, []);

  const cargarDatos = useCallback(async () => {
    setLoadingRows(true);
    try {
      const data = await api.getPartesDiariosDatos({
        pozo: pozoFiltro || undefined,
        limit: 2000,
      });
      setRows(data.data);
    } catch {
      setRows([]);
    }
    setLoadingRows(false);
  }, [pozoFiltro]);

  useEffect(() => {
    cargarInfo();
    cargarPozos();
  }, [cargarInfo, cargarPozos]);

  useEffect(() => {
    if (info?.exists) cargarDatos();
  }, [info, cargarDatos]);

  const handleRefresh = () => {
    clearApiCache("/api/partes-diarios");
    cargarInfo();
    cargarPozos();
    cargarDatos();
  };

  const formatDate = (iso: string | null) => {
    if (!iso) return "—";
    try {
      return new Date(iso).toLocaleString("es-AR", {
        day: "2-digit", month: "2-digit", year: "numeric",
        hour: "2-digit", minute: "2-digit",
      });
    } catch { return iso; }
  };

  const columnas = rows.length > 0
    ? Object.keys(rows[0]).filter(c => !COLS_OCULTAR.includes(c))
    : [];

  const statusColor = (status?: string) => {
    if (!status) return "text-slate-400";
    if (status === "EN_CURSO") return "text-yellow-400";
    if (status === "COMPLETADO") return "text-green-400";
    return "text-slate-400";
  };

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155]">
        <div>
          <h1 className="text-xl font-bold text-slate-100">Partes Diarios de Torre</h1>
          {info?.exists && (
            <p className="text-xs text-slate-400 mt-1">
              Última actualización: {formatDate(info.updated_at)}
              {info.rows ? ` · ${info.rows} actividades` : ""}
              {info.size_kb ? ` · ${info.size_kb} KB` : ""}
            </p>
          )}
        </div>
        <button
          onClick={handleRefresh}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-sky-600 hover:bg-sky-500 text-white disabled:opacity-50 transition-colors"
        >
          <svg className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Actualizar
        </button>
      </div>

      {info?.exists && pozos.length > 0 && (
        <div className="flex items-center gap-4 px-6 py-3 border-b border-[#334155] bg-[#1e293b]">
          <label className="text-xs text-slate-400">Pozo:</label>
          <select
            value={pozoFiltro}
            onChange={e => setPozoFiltro(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-sm rounded-lg px-3 py-1.5 focus:outline-none focus:border-sky-500"
          >
            <option value="">Todos</option>
            {pozos.map(p => (
              <option key={p.pozo} value={p.pozo}>
                {p.pozo} — {p.status}
              </option>
            ))}
          </select>
          <span className="text-xs text-slate-500">{rows.length} actividades</span>
        </div>
      )}

      <div className="flex-1 overflow-auto">
        {loading ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <div className="w-8 h-8 border-2 border-sky-400 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
              <p className="text-slate-400 text-sm">Cargando...</p>
            </div>
          </div>
        ) : !info?.exists ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center max-w-md px-6">
              <h2 className="text-lg font-semibold text-slate-200 mb-2">Sin datos todavía</h2>
              <p className="text-sm text-slate-400 mb-1">
                {info?.error || "No hay partes diarios procesados aún."}
              </p>
            </div>
          </div>
        ) : (
          <div className="overflow-x-auto">
            {loadingRows ? (
              <div className="flex items-center justify-center py-20">
                <div className="w-6 h-6 border-2 border-sky-400 border-t-transparent rounded-full animate-spin" />
              </div>
            ) : (
              <table className="w-full text-sm text-left">
                <thead className="sticky top-0 bg-[#1e293b] border-b border-[#334155]">
                  <tr>
                    {columnas.map(col => (
                      <th key={col} className="px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider whitespace-nowrap">
                        {COL_LABELS[col] || col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-[#1e293b]">
                  {rows.map((row, i) => (
                    <tr key={i} className="hover:bg-[#1e293b] transition-colors">
                      {columnas.map(col => (
                        <td key={col} className={`px-4 py-2.5 whitespace-nowrap text-xs ${col === "status_end" ? statusColor(row[col] as string) : "text-slate-300"}`}>
                          {col === "activity_duration" && row[col] != null
                            ? Number(row[col]).toFixed(2)
                            : row[col] != null ? String(row[col]) : "—"}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>
    </div>
  );
}