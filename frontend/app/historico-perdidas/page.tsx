"use client";

import { useEffect, useState, useCallback } from "react";
import api, { clearApiCache } from "@/lib/api";
import type { DowntimeRow, DowntimeInfo } from "@/lib/api";

export default function HistoricoPérdidasPage() {
  const [info, setInfo]       = useState<DowntimeInfo | null>(null);
  const [rows, setRows]       = useState<DowntimeRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);

  // Filtros
  const [filtroPOZO,  setFiltroPOZO]  = useState("");
  const [filtroRUBRO, setFiltroRUBRO] = useState("");
  const [fechaDesde,  setFechaDesde]  = useState("");
  const [fechaHasta,  setFechaHasta]  = useState("");

  const cargarDatos = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [infoData, rowsData] = await Promise.all([
        api.getDowntimesInfo(),
        api.getDowntimes({
          pozo:        filtroPOZO   || undefined,
          fecha_desde: fechaDesde   || undefined,
          fecha_hasta: fechaHasta   || undefined,
          limit:       5000,
        }),
      ]);
      setInfo(infoData);
      setRows(rowsData.data);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error cargando datos");
    }
    setLoading(false);
  }, [filtroPOZO, fechaDesde, fechaHasta]);

  useEffect(() => {
    cargarDatos();
  }, [cargarDatos]);

  const handleRefresh = () => {
    clearApiCache("/api/merma/downtimes");
    cargarDatos();
  };

  // Filtro local por RUBRO (sin ir al backend)
  const rowsFiltrados = filtroRUBRO
    ? rows.filter((r) =>
        r.RUBRO?.toLowerCase().includes(filtroRUBRO.toLowerCase())
      )
    : rows;

  // Listas únicas para los selectores
  const pozosUnicos  = Array.from(new Set(rows.map((r) => r.POZO).filter((v): v is string => Boolean(v)))).sort();
  const rubrosUnicos = Array.from(new Set(rows.map((r) => r.RUBRO).filter((v): v is string => Boolean(v)))).sort();

  const formatDate = (val: string | null | undefined) => {
    if (!val) return "—";
    return val.slice(0, 16).replace("T", " ");
  };

  const formatNum = (v: number | null | undefined) =>
    v == null ? "—" : v.toLocaleString("es-AR", { minimumFractionDigits: 3, maximumFractionDigits: 3 });

  return (
    <div className="flex flex-col h-full bg-[#0f172a]">

      {/* ── Header ── */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155]">
        <div>
          <h1 className="text-xl font-bold text-slate-100">
            Histórico de Pérdidas
          </h1>
          {info?.exists && (
            <p className="text-xs text-slate-400 mt-1">
              Actualizado: {info.updated_at ? new Date(info.updated_at).toLocaleString("es-AR") : "—"}
              {info.rows != null ? ` · ${info.rows.toLocaleString()} registros` : ""}
              {info.fecha_min && info.fecha_max
                ? ` · ${info.fecha_min.slice(0, 10)} → ${info.fecha_max.slice(0, 10)}`
                : ""}
            </p>
          )}
        </div>
        <button
          onClick={handleRefresh}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-sky-600 hover:bg-sky-500 text-white disabled:opacity-50 transition-colors"
        >
          <svg
            className={`w-4 h-4 ${loading ? "animate-spin" : ""}`}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
            />
          </svg>
          Actualizar
        </button>
      </div>

      {/* ── Filtros ── */}
      <div className="flex flex-wrap gap-3 px-6 py-3 border-b border-[#334155] bg-[#1e293b]">
        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Pozo</label>
          <select
            value={filtroPOZO}
            onChange={(e) => setFiltroPOZO(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-sm rounded-lg px-3 py-1.5 min-w-[200px]"
          >
            <option value="">Todos</option>
            {pozosUnicos.map((p) => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Rubro</label>
          <select
            value={filtroRUBRO}
            onChange={(e) => setFiltroRUBRO(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-sm rounded-lg px-3 py-1.5 min-w-[200px]"
          >
            <option value="">Todos</option>
            {rubrosUnicos.map((r) => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Fecha desde</label>
          <input
            type="date"
            value={fechaDesde}
            onChange={(e) => setFechaDesde(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-sm rounded-lg px-3 py-1.5"
          />
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Fecha hasta</label>
          <input
            type="date"
            value={fechaHasta}
            onChange={(e) => setFechaHasta(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-sm rounded-lg px-3 py-1.5"
          />
        </div>

        {(filtroPOZO || filtroRUBRO || fechaDesde || fechaHasta) && (
          <div className="flex flex-col justify-end">
            <button
              onClick={() => {
                setFiltroPOZO("");
                setFiltroRUBRO("");
                setFechaDesde("");
                setFechaHasta("");
              }}
              className="text-xs text-slate-400 hover:text-slate-200 px-3 py-1.5 border border-[#334155] rounded-lg transition-colors"
            >
              Limpiar filtros
            </button>
          </div>
        )}
      </div>

      {/* ── KPIs ── */}
      {!loading && rowsFiltrados.length > 0 && (
        <div className="flex gap-4 px-6 py-3 border-b border-[#334155]">
          <KpiCard label="Eventos" value={rowsFiltrados.length.toLocaleString()} />
          <KpiCard
            label="Pozos únicos"
            value={new Set(rowsFiltrados.map((r) => r.POZO)).size.toLocaleString()}
          />
          <KpiCard
            label="Pérd. Petróleo total (m³)"
            value={rowsFiltrados
              .reduce((s, r) => s + (r.oilShortfall ?? 0), 0)
              .toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            highlight
          />
          <KpiCard
            label="Pérd. Líquido total (m³)"
            value={rowsFiltrados
              .reduce((s, r) => s + (r.liquidShortfall ?? 0), 0)
              .toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          />
        </div>
      )}

      {/* ── Tabla ── */}
      <div className="flex-1 overflow-auto px-6 py-4">
        {loading ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <div className="w-8 h-8 border-2 border-sky-400 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
              <p className="text-slate-400 text-sm">Cargando histórico...</p>
            </div>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-center max-w-md">
              <p className="text-slate-300 font-medium mb-1">Error cargando datos</p>
              <p className="text-slate-500 text-sm">{error}</p>
              <p className="text-xs text-slate-600 mt-2">
                El histórico se genera automáticamente todos los días a las 8 AM.
                Si es la primera vez, ejecutá el Job manualmente desde Cloud Run.
              </p>
            </div>
          </div>
        ) : rowsFiltrados.length === 0 ? (
          <div className="flex items-center justify-center h-64">
            <p className="text-slate-500 text-sm">
              No hay datos para los filtros seleccionados.
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto rounded-lg border border-[#334155]">
            <table className="w-full text-sm text-left">
              <thead className="bg-[#1e293b] text-slate-400 text-xs uppercase tracking-wider sticky top-0">
                <tr>
                  <Th>Pozo</Th>
                  <Th>Rubro</Th>
                  <Th>Fecha Desde</Th>
                  <Th>Fecha Hasta</Th>
                  <Th right>Petróleo (m³)</Th>
                  <Th right>Agua (m³)</Th>
                  <Th right>Líquido (m³)</Th>
                  <Th right>Gas (m³)</Th>
                  <Th right>Pot. Petróleo</Th>
                  <Th right>Pot. Líquido</Th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[#1e293b]">
                {rowsFiltrados.map((row, i) => (
                  <tr
                    key={i}
                    className="bg-[#0f172a] hover:bg-[#1e293b] transition-colors"
                  >
                    <td className="px-4 py-2 font-mono text-xs text-slate-300 whitespace-nowrap">
                      {row.POZO ?? "—"}
                    </td>
                    <td className="px-4 py-2 text-slate-400 whitespace-nowrap">
                      {row.RUBRO ?? "—"}
                    </td>
                    <td className="px-4 py-2 text-slate-400 whitespace-nowrap text-xs">
                      {formatDate(row["FECHA DESDE"])}
                    </td>
                    <td className="px-4 py-2 text-slate-400 whitespace-nowrap text-xs">
                      {formatDate(row["FECHA HASTA"])}
                    </td>
                    <Td highlight>{formatNum(row.oilShortfall)}</Td>
                    <Td>{formatNum(row.waterShortfall)}</Td>
                    <Td>{formatNum(row.liquidShortfall)}</Td>
                    <Td>{formatNum(row.gasShortfall)}</Td>
                    <Td>{formatNum(row.potentialOil)}</Td>
                    <Td>{formatNum(row.potentialLiquid)}</Td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Sub-componentes ──────────────────────────────────────

function KpiCard({
  label,
  value,
  highlight,
}: {
  label: string;
  value: string;
  highlight?: boolean;
}) {
  return (
    <div className="bg-[#1e293b] rounded-lg px-4 py-2 border border-[#334155]">
      <p className="text-xs text-slate-500">{label}</p>
      <p
        className={`text-base font-bold mt-0.5 ${
          highlight ? "text-sky-400" : "text-slate-200"
        }`}
      >
        {value}
      </p>
    </div>
  );
}

function Th({
  children,
  right,
}: {
  children: React.ReactNode;
  right?: boolean;
}) {
  return (
    <th className={`px-4 py-3 whitespace-nowrap ${right ? "text-right" : ""}`}>
      {children}
    </th>
  );
}

function Td({
  children,
  highlight,
}: {
  children: React.ReactNode;
  highlight?: boolean;
}) {
  return (
    <td
      className={`px-4 py-2 text-right font-mono text-xs ${
        highlight ? "text-sky-300" : "text-slate-400"
      }`}
    >
      {children}
    </td>
  );
}
