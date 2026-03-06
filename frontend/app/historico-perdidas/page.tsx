"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import api, { clearApiCache } from "@/lib/api";
import type { DowntimeRow, DowntimeInfo } from "@/lib/api";

type SortKey = keyof DowntimeRow;
type SortDir = "asc" | "desc";

const DEFAULT_WIDTHS = [130, 210, 110, 110, 100, 95, 95, 85, 100, 100];

const COLS: { key: SortKey; label: string; num?: boolean; highlight?: boolean }[] = [
  { key: "POZO",           label: "Pozo" },
  { key: "RUBRO",          label: "Rubro" },
  { key: "FECHA DESDE",    label: "Desde" },
  { key: "FECHA HASTA",    label: "Hasta" },
  { key: "oilShortfall",   label: "Petróleo (m³)", num: true, highlight: true },
  { key: "waterShortfall", label: "Agua (m³)",     num: true },
  { key: "liquidShortfall",label: "Líquido (m³)",  num: true },
  { key: "gasShortfall",   label: "Gas (m³)",      num: true },
  { key: "potentialOil",   label: "Pot. Petróleo", num: true },
  { key: "potentialLiquid",label: "Pot. Líquido",  num: true },
];

export default function HistoricoPérdidasPage() {
  const [info, setInfo]       = useState<DowntimeInfo | null>(null);
  const [rows, setRows]       = useState<DowntimeRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);

  const [filtroPOZO,  setFiltroPOZO]  = useState("");
  const [filtroRUBRO, setFiltroRUBRO] = useState("");
  const [fechaDesde,  setFechaDesde]  = useState("");
  const [fechaHasta,  setFechaHasta]  = useState("");

  const [sortKey, setSortKey] = useState<SortKey>("FECHA DESDE");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  // Ancho de columnas redimensionables
  const [colWidths, setColWidths] = useState<number[]>(DEFAULT_WIDTHS);
  const resizingRef = useRef<{ colIdx: number; startX: number; startW: number } | null>(null);

  // ── Carga de datos ──────────────────────────────────────
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

  useEffect(() => { cargarDatos(); }, [cargarDatos]);

  const handleRefresh = () => {
    clearApiCache("/api/merma/downtimes");
    cargarDatos();
  };

  // ── Ordenamiento ────────────────────────────────────────
  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortKey(key); setSortDir("asc"); }
  };

  // ── Resize de columnas ──────────────────────────────────
  const startResize = (e: React.MouseEvent, colIdx: number) => {
    e.preventDefault();
    resizingRef.current = { colIdx, startX: e.clientX, startW: colWidths[colIdx] };

    const onMove = (ev: MouseEvent) => {
      if (!resizingRef.current) return;
      const { colIdx: ci, startX, startW } = resizingRef.current;
      const newW = Math.max(50, startW + (ev.clientX - startX));
      setColWidths(prev => { const next = [...prev]; next[ci] = newW; return next; });
    };
    const onUp = () => {
      resizingRef.current = null;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  };

  // ── Exportar a CSV ────────────────────────────────────
  const exportarCSV = () => {
    const headers = ["Pozo","Rubro","Fecha Desde","Fecha Hasta","Petróleo (m³)","Agua (m³)","Líquido (m³)","Gas (m³)","Pot. Petróleo","Pot. Líquido"];
    const filas = rowsOrdenados.map(r => [
      r.POZO ?? "",
      r.RUBRO ?? "",
      r["FECHA DESDE"] ?? "",
      r["FECHA HASTA"] ?? "",
      r.oilShortfall ?? "",
      r.waterShortfall ?? "",
      r.liquidShortfall ?? "",
      r.gasShortfall ?? "",
      r.potentialOil ?? "",
      r.potentialLiquid ?? "",
    ].map(v => `"${String(v).replace(/"/g, '""')}"`).join(","));

    const csv = [headers.join(","), ...filas].join("\n");
    const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href     = url;
    a.download = `historico_perdidas_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // ── Filtros y ordenamiento ──────────────────────────────
  const rowsFiltrados = filtroRUBRO
    ? rows.filter(r => r.RUBRO?.toLowerCase().includes(filtroRUBRO.toLowerCase()))
    : rows;

  const rowsOrdenados = [...rowsFiltrados].sort((a, b) => {
    const va = a[sortKey] ?? "";
    const vb = b[sortKey] ?? "";
    if (va < vb) return sortDir === "asc" ? -1 : 1;
    if (va > vb) return sortDir === "asc" ? 1 : -1;
    return 0;
  });

  const pozosUnicos  = Array.from(new Set(rows.map(r => r.POZO).filter((v): v is string => Boolean(v)))).sort();
  const rubrosUnicos = Array.from(new Set(rows.map(r => r.RUBRO).filter((v): v is string => Boolean(v)))).sort();

  const fmtDate = (v?: string | null) => v ? v.slice(0, 16).replace("T", " ") : "—";
  const fmtNum  = (v?: number | null) =>
    v == null ? "—" : v.toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  return (
    <div className="flex flex-col h-full bg-[#0f172a]">

      {/* ── Header ── */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155]">
        <div>
          <h1 className="text-xl font-bold text-slate-100">Histórico de Pérdidas</h1>
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
        <div className="flex gap-2">
          {/* Exportar Excel */}
          {rows.length > 0 && (
            <button onClick={exportarCSV}
              className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-emerald-700 hover:bg-emerald-600 text-white transition-colors">
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
              </svg>
              Exportar CSV
            </button>
          )}
          {/* Actualizar */}
          <button onClick={handleRefresh} disabled={loading}
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-sky-600 hover:bg-sky-500 text-white disabled:opacity-50 transition-colors">
            <svg className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Actualizar
          </button>
        </div>
      </div>

      {/* ── Filtros ── */}
      <div className="flex flex-wrap gap-3 px-6 py-3 border-b border-[#334155] bg-[#1e293b]">
        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Pozo</label>
          <select value={filtroPOZO} onChange={e => setFiltroPOZO(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-xs rounded-lg px-3 py-1.5 min-w-[180px]">
            <option value="">Todos</option>
            {pozosUnicos.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Rubro</label>
          <select value={filtroRUBRO} onChange={e => setFiltroRUBRO(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-xs rounded-lg px-3 py-1.5 min-w-[180px]">
            <option value="">Todos</option>
            {rubrosUnicos.map(r => <option key={r} value={r}>{r}</option>)}
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Fecha desde</label>
          <input type="date" value={fechaDesde} onChange={e => setFechaDesde(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-xs rounded-lg px-3 py-1.5" />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Fecha hasta</label>
          <input type="date" value={fechaHasta} onChange={e => setFechaHasta(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-xs rounded-lg px-3 py-1.5" />
        </div>
        {(filtroPOZO || filtroRUBRO || fechaDesde || fechaHasta) && (
          <div className="flex flex-col justify-end">
            <button onClick={() => { setFiltroPOZO(""); setFiltroRUBRO(""); setFechaDesde(""); setFechaHasta(""); }}
              className="text-xs text-slate-400 hover:text-slate-200 px-3 py-1.5 border border-[#334155] rounded-lg transition-colors">
              Limpiar
            </button>
          </div>
        )}
      </div>

      {/* ── KPIs ── */}
      {!loading && rowsOrdenados.length > 0 && (
        <div className="flex gap-3 px-6 py-3 border-b border-[#334155]">
          <KpiCard label="Eventos" value={rowsOrdenados.length.toLocaleString()} />
          <KpiCard label="Pozos únicos" value={new Set(rowsOrdenados.map(r => r.POZO)).size.toLocaleString()} />
          <KpiCard label="Pérd. Petróleo (m³)" highlight
            value={rowsOrdenados.reduce((s, r) => s + (r.oilShortfall ?? 0), 0)
              .toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} />
          <KpiCard label="Pérd. Líquido (m³)"
            value={rowsOrdenados.reduce((s, r) => s + (r.liquidShortfall ?? 0), 0)
              .toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} />
        </div>
      )}

      {/* ── Tabla ── */}
      <div className="flex-1 overflow-auto px-4 py-3">
        {loading ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <div className="w-8 h-8 border-2 border-sky-400 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
              <p className="text-slate-400 text-sm">Cargando histórico...</p>
            </div>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-64">
            <p className="text-red-400 text-sm">{error}</p>
          </div>
        ) : rowsOrdenados.length === 0 ? (
          <div className="flex items-center justify-center h-64">
            <p className="text-slate-500 text-sm">No hay datos para los filtros seleccionados.</p>
          </div>
        ) : (
          <div className="rounded-lg border border-[#334155] overflow-hidden">
            <table className="text-left" style={{ borderCollapse: "collapse", width: colWidths.reduce((a, b) => a + b, 0) }}>
              <thead className="bg-[#1e293b] sticky top-0 z-10">
                <tr>
                  {COLS.map((col, i) => (
                    <th key={String(col.key)}
                      style={{ width: colWidths[i], minWidth: colWidths[i], position: "relative" }}
                      className="border-b border-[#334155] px-0 py-0 select-none">
                      {/* Contenido del header */}
                      <div
                        onClick={() => handleSort(col.key)}
                        className={`flex items-center gap-1 px-2 py-2 cursor-pointer hover:bg-[#263348] text-xs font-semibold text-slate-400 uppercase tracking-wide ${col.num ? "justify-end" : ""}`}
                      >
                        <span className="truncate">{col.label}</span>
                        {sortKey === col.key && (
                          <span className="text-sky-400 flex-shrink-0">{sortDir === "asc" ? "↑" : "↓"}</span>
                        )}
                      </div>
                      {/* Handle de resize */}
                      <div
                        onMouseDown={e => startResize(e, i)}
                        style={{
                          position: "absolute", right: 0, top: 0, bottom: 0,
                          width: 5, cursor: "col-resize", zIndex: 20,
                        }}
                        className="hover:bg-sky-500 opacity-0 hover:opacity-100 transition-opacity"
                      />
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rowsOrdenados.map((row, i) => (
                  <tr key={i} className={`border-b border-[#1a2535] hover:bg-[#1e293b] transition-colors ${i % 2 === 0 ? "bg-[#0f172a]" : "bg-[#0d1526]"}`}>
                    <td style={{ width: colWidths[0] }} className="px-2 py-1.5 font-mono text-xs text-slate-300 truncate" title={row.POZO ?? ""}>{row.POZO ?? "—"}</td>
                    <td style={{ width: colWidths[1] }} className="px-2 py-1.5 text-xs text-slate-400 truncate" title={row.RUBRO ?? ""}>{row.RUBRO ?? "—"}</td>
                    <td style={{ width: colWidths[2] }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{fmtDate(row["FECHA DESDE"])}</td>
                    <td style={{ width: colWidths[3] }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{fmtDate(row["FECHA HASTA"])}</td>
                    <td style={{ width: colWidths[4] }} className="px-2 py-1.5 text-xs text-sky-300 text-right font-mono">{fmtNum(row.oilShortfall)}</td>
                    <td style={{ width: colWidths[5] }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row.waterShortfall)}</td>
                    <td style={{ width: colWidths[6] }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row.liquidShortfall)}</td>
                    <td style={{ width: colWidths[7] }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row.gasShortfall)}</td>
                    <td style={{ width: colWidths[8] }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row.potentialOil)}</td>
                    <td style={{ width: colWidths[9] }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row.potentialLiquid)}</td>
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

function KpiCard({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div className="bg-[#1e293b] rounded-lg px-4 py-2 border border-[#334155]">
      <p className="text-xs text-slate-500">{label}</p>
      <p className={`text-base font-bold mt-0.5 ${highlight ? "text-sky-400" : "text-slate-200"}`}>{value}</p>
    </div>
  );
}
