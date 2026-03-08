"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import api from "@/lib/api";
import type { ControlesInfo, ControlRow, MermaRow } from "@/lib/api";

type Vista = "controles" | "merma";

// ── Helpers ───────────────────────────────────────────────
const fmtDate  = (v?: string | null) => v ? v.slice(0, 16).replace("T", " ") : "—";
const fmtNum   = (v?: number | null) =>
  v == null ? "—" : v.toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtPct   = (v?: number | null) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(1)}%`;

function PctBadge({ v }: { v?: number | null }) {
  if (v == null) return <span className="text-slate-500">—</span>;
  const color = v < -20 ? "text-red-400" : v < 0 ? "text-orange-400" : "text-emerald-400";
  return <span className={`font-mono font-semibold ${color}`}>{fmtPct(v)}</span>;
}

function Diasbadge({ v }: { v?: number | null }) {
  if (v == null) return <span className="text-slate-500">—</span>;
  const color = v > 90 ? "text-red-400" : v > 45 ? "text-orange-400" : "text-slate-300";
  return <span className={`font-mono ${color}`}>{v}d</span>;
}

function KpiCard({ label, value, highlight, sub }: { label: string; value: string; highlight?: boolean; sub?: string }) {
  return (
    <div className="bg-[#1e293b] rounded-lg px-4 py-2.5 border border-[#334155] min-w-[140px]">
      <p className="text-xs text-slate-500">{label}</p>
      <p className={`text-lg font-bold mt-0.5 ${highlight ? "text-sky-400" : "text-slate-200"}`}>{value}</p>
      {sub && <p className="text-[10px] text-slate-500 mt-0.5">{sub}</p>}
    </div>
  );
}

// ============================================================
// Página principal
// ============================================================
export default function ControlesHistoricosPage() {
  const [vista, setVista]     = useState<Vista>("controles");
  const [info,  setInfo]      = useState<ControlesInfo | null>(null);
  const [controles, setControles] = useState<ControlRow[]>([]);
  const [merma,     setMerma]     = useState<MermaRow[]>([]);
  const [loading,   setLoading]   = useState(true);
  const [error,     setError]     = useState<string | null>(null);

  // Filtros
  const [filtroPOZO,    setFiltroPOZO]    = useState("");
  const [filtroBATERIA, setFiltroBATERIA] = useState("");
  const [filtroESTADO,  setFiltroESTADO]  = useState("");
  const [fechaDesde,    setFechaDesde]    = useState("");
  const [fechaHasta,    setFechaHasta]    = useState("");
  const [soloMerma,     setSoloMerma]     = useState(false);

  // Sort
  const [sortKeyC, setSortKeyC] = useState<keyof ControlRow>("Fecha y Hora");
  const [sortDirC, setSortDirC] = useState<"asc" | "desc">("desc");
  const [sortKeyM, setSortKeyM] = useState<keyof MermaRow>("PCT_MERMA_NETA");
  const [sortDirM, setSortDirM] = useState<"asc" | "desc">("asc");

  // Resize columnas
  const [colWidthsC, setColWidthsC] = useState([160, 110, 150, 110, 90, 90, 90, 110, 140, 150, 150]);
  const [colWidthsM, setColWidthsM] = useState([160, 110, 130, 150, 150, 110, 90, 90, 90, 90, 90, 90, 90]);
  const resizingRef = useRef<{ colIdx: number; startX: number; startW: number; table: Vista } | null>(null);

  const startResize = (e: React.MouseEvent, colIdx: number, table: Vista) => {
    e.preventDefault();
    const widths = table === "controles" ? colWidthsC : colWidthsM;
    resizingRef.current = { colIdx, startX: e.clientX, startW: widths[colIdx], table };
    const onMove = (ev: MouseEvent) => {
      if (!resizingRef.current) return;
      const { colIdx: ci, startX, startW, table: t } = resizingRef.current;
      const newW = Math.max(60, startW + (ev.clientX - startX));
      if (t === "controles") setColWidthsC(p => { const n = [...p]; n[ci] = newW; return n; });
      else                   setColWidthsM(p => { const n = [...p]; n[ci] = newW; return n; });
    };
    const onUp = () => { resizingRef.current = null; window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  };

  // ── Carga de datos ─────────────────────────────────────
  const cargarDatos = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const [infoData, controlesData, mermaData] = await Promise.all([
        api.getControlesInfo(),
        api.getControlesHistorico({
          pozo:        filtroPOZO    || undefined,
          bateria:     filtroBATERIA || undefined,
          estado_pozo: filtroESTADO  || undefined,
          fecha_desde: fechaDesde    || undefined,
          fecha_hasta: fechaHasta    || undefined,
          limit: 10000,
        }),
        api.getControlesMerma({
          solo_merma:  soloMerma,
          bateria:     filtroBATERIA || undefined,
          estado_pozo: filtroESTADO  || undefined,
          limit: 5000,
        }),
      ]);
      setInfo(infoData);
      setControles(controlesData.data);
      setMerma(mermaData.data);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error cargando datos");
    }
    setLoading(false);
  }, [filtroPOZO, filtroBATERIA, filtroESTADO, fechaDesde, fechaHasta, soloMerma]);

  useEffect(() => { cargarDatos(); }, [cargarDatos]);

  // ── Sort controles ─────────────────────────────────────
  const handleSortC = (key: keyof ControlRow) => {
    if (sortKeyC === key) setSortDirC(d => d === "asc" ? "desc" : "asc");
    else { setSortKeyC(key); setSortDirC("asc"); }
  };
  const handleSortM = (key: keyof MermaRow) => {
    if (sortKeyM === key) setSortDirM(d => d === "asc" ? "desc" : "asc");
    else { setSortKeyM(key); setSortDirM("asc"); }
  };

  const sortedControles = [...controles].sort((a, b) => {
    const va = a[sortKeyC] ?? ""; const vb = b[sortKeyC] ?? "";
    if (va < vb) return sortDirC === "asc" ? -1 : 1;
    if (va > vb) return sortDirC === "asc" ? 1 : -1;
    return 0;
  });

  const sortedMerma = [...merma].sort((a, b) => {
    const va = a[sortKeyM] ?? ""; const vb = b[sortKeyM] ?? "";
    if (va < vb) return sortDirM === "asc" ? -1 : 1;
    if (va > vb) return sortDirM === "asc" ? 1 : -1;
    return 0;
  });

  // ── Valores únicos para filtros ───────────────────────
  const baterias = Array.from(new Set(controles.map(r => r.BATERIA).filter(Boolean))).sort() as string[];
  const estados  = Array.from(new Set(controles.map(r => r.ESTADO_POZO).filter(Boolean))).sort() as string[];

  // ── Exportar CSV ──────────────────────────────────────
  const exportarCSV = () => {
    const data = vista === "controles" ? sortedControles : sortedMerma;
    if (!data.length) return;
    const headers = Object.keys(data[0]);
    const rows    = data.map(r => headers.map(h => `"${String((r as Record<string,unknown>)[h] ?? "").replace(/"/g, '""')}"`).join(","));
    const csv     = [headers.join(","), ...rows].join("\n");
    const blob    = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
    const a       = document.createElement("a");
    a.href        = URL.createObjectURL(blob);
    a.download    = `controles_${vista}_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  // ── KPIs ─────────────────────────────────────────────
  const kpiControles = controles.length.toLocaleString();
  const kpiPozos     = new Set(controles.map(r => r.Pozo)).size.toLocaleString();
  const kpiMerma     = merma.filter(r => r.EN_MERMA_NETA).length.toLocaleString();
  const kpiDias      = merma.length > 0
    ? Math.round(merma.reduce((s, r) => s + (r.DIAS_SIN_CONTROL ?? 0), 0) / merma.length)
    : 0;

  const ThC = ({ label, k, i }: { label: string; k: keyof ControlRow; i: number }) => (
    <th style={{ width: colWidthsC[i], minWidth: colWidthsC[i], position: "relative" }}
      className="border-b border-[#334155] px-0 py-0 select-none">
      <div onClick={() => handleSortC(k)}
        className="flex items-center gap-1 px-2 py-2 cursor-pointer hover:bg-[#263348] text-xs font-semibold text-slate-400 uppercase tracking-wide">
        <span className="truncate">{label}</span>
        {sortKeyC === k && <span className="text-sky-400 flex-shrink-0">{sortDirC === "asc" ? "↑" : "↓"}</span>}
      </div>
      <div onMouseDown={e => startResize(e, i, "controles")}
        style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: 5, cursor: "col-resize", zIndex: 20 }}
        className="hover:bg-sky-500 opacity-0 hover:opacity-100 transition-opacity" />
    </th>
  );

  const ThM = ({ label, k, i, right }: { label: string; k: keyof MermaRow; i: number; right?: boolean }) => (
    <th style={{ width: colWidthsM[i], minWidth: colWidthsM[i], position: "relative" }}
      className="border-b border-[#334155] px-0 py-0 select-none">
      <div onClick={() => handleSortM(k)}
        className={`flex items-center gap-1 px-2 py-2 cursor-pointer hover:bg-[#263348] text-xs font-semibold text-slate-400 uppercase tracking-wide ${right ? "justify-end" : ""}`}>
        <span className="truncate">{label}</span>
        {sortKeyM === k && <span className="text-sky-400 flex-shrink-0">{sortDirM === "asc" ? "↑" : "↓"}</span>}
      </div>
      <div onMouseDown={e => startResize(e, i, "merma")}
        style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: 5, cursor: "col-resize", zIndex: 20 }}
        className="hover:bg-sky-500 opacity-0 hover:opacity-100 transition-opacity" />
    </th>
  );

  return (
    <div className="flex flex-col h-full bg-[#0f172a]">

      {/* ── Header ── */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155]">
        <div>
          <h1 className="text-xl font-bold text-slate-100">Controles Históricos de Producción</h1>
          {info?.exists && (
            <p className="text-xs text-slate-400 mt-1">
              Actualizado: {info.updated_at ? new Date(info.updated_at).toLocaleString("es-AR") : "—"}
              {info.rows != null ? ` · ${info.rows.toLocaleString()} controles` : ""}
              {info.fecha_min && info.fecha_max ? ` · ${info.fecha_min} → ${info.fecha_max}` : ""}
            </p>
          )}
        </div>
        <div className="flex gap-2">
          <button onClick={exportarCSV}
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-emerald-700 hover:bg-emerald-600 text-white transition-colors">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
            </svg>
            Exportar CSV
          </button>
          <button onClick={cargarDatos} disabled={loading}
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-sky-600 hover:bg-sky-500 text-white disabled:opacity-50 transition-colors">
            <svg className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Actualizar
          </button>
        </div>
      </div>

      {/* ── Filtros ── */}
      <div className="flex flex-wrap gap-3 px-6 py-3 border-b border-[#334155] bg-[#1e293b]">
        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Pozo</label>
          <input value={filtroPOZO} onChange={e => setFiltroPOZO(e.target.value)} placeholder="Buscar pozo..."
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-xs rounded-lg px-3 py-1.5 w-44" />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Batería</label>
          <select value={filtroBATERIA} onChange={e => setFiltroBATERIA(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-xs rounded-lg px-3 py-1.5 w-44">
            <option value="">Todas</option>
            {baterias.map(b => <option key={b} value={b}>{b}</option>)}
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-400">Estado pozo</label>
          <select value={filtroESTADO} onChange={e => setFiltroESTADO(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] text-slate-200 text-xs rounded-lg px-3 py-1.5 w-44">
            <option value="">Todos</option>
            {estados.map(e => <option key={e} value={e}>{e}</option>)}
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
        {(filtroPOZO || filtroBATERIA || filtroESTADO || fechaDesde || fechaHasta) && (
          <div className="flex flex-col justify-end">
            <button onClick={() => { setFiltroPOZO(""); setFiltroBATERIA(""); setFiltroESTADO(""); setFechaDesde(""); setFechaHasta(""); }}
              className="text-xs text-slate-400 hover:text-slate-200 px-3 py-1.5 border border-[#334155] rounded-lg transition-colors">
              Limpiar
            </button>
          </div>
        )}
      </div>

      {/* ── KPIs ── */}
      {!loading && (
        <div className="flex flex-wrap gap-3 px-6 py-3 border-b border-[#334155]">
          <KpiCard label="Controles"   value={kpiControles} />
          <KpiCard label="Pozos únicos" value={kpiPozos} />
          <KpiCard label="En merma neta" value={kpiMerma} highlight sub="último vs penúltimo control" />
          <KpiCard label="Días prom. sin control" value={`${kpiDias}d`} sub="sobre pozos analizados" />
        </div>
      )}

      {/* ── Tabs ── */}
      <div className="flex gap-0 px-6 pt-3 border-b border-[#334155]">
        {(["controles", "merma"] as Vista[]).map(v => (
          <button key={v} onClick={() => setVista(v)}
            className={`px-5 py-2 text-sm font-medium border-b-2 transition-colors ${
              vista === v
                ? "border-sky-400 text-sky-400"
                : "border-transparent text-slate-400 hover:text-slate-200"
            }`}>
            {v === "controles" ? `Todos los controles (${controles.length.toLocaleString()})` : `Merma por pozo (${merma.length.toLocaleString()})`}
          </button>
        ))}
        {vista === "merma" && (
          <label className="ml-auto flex items-center gap-2 text-xs text-slate-400 cursor-pointer mb-1">
            <input type="checkbox" checked={soloMerma} onChange={e => setSoloMerma(e.target.checked)}
              className="accent-sky-400" />
            Solo pozos en merma
          </label>
        )}
      </div>

      {/* ── Contenido ── */}
      <div className="flex-1 overflow-auto px-4 py-3">
        {loading ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <div className="w-8 h-8 border-2 border-sky-400 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
              <p className="text-slate-400 text-sm">Cargando controles...</p>
            </div>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-64">
            <p className="text-red-400 text-sm">{error}</p>
          </div>
        ) : vista === "controles" ? (

          /* ── Tabla Controles ── */
          sortedControles.length === 0 ? (
            <div className="flex items-center justify-center h-64">
              <p className="text-slate-500 text-sm">No hay datos para los filtros seleccionados.</p>
            </div>
          ) : (
            <div className="rounded-lg border border-[#334155] overflow-hidden">
              <table className="text-left" style={{ borderCollapse: "collapse", width: colWidthsC.reduce((a, b) => a + b, 0) }}>
                <thead className="bg-[#1e293b] sticky top-0 z-10">
                  <tr>
                    <ThC i={0}  k="Pozo"                  label="Pozo" />
                    <ThC i={1}  k="Día Operativo"          label="Día Op." />
                    <ThC i={2}  k="Fecha y Hora"           label="Fecha y Hora" />
                    <ThC i={3}  k="Estado"                 label="Ctrl. Estado" />
                    <ThC i={4}  k="Producción de Gas"      label="Gas (m³)" />
                    <ThC i={5}  k="Producción de Líquido"  label="Líquido (m³)" />
                    <ThC i={6}  k="Producción de Petróleo" label="Petróleo (m³)" />
                    <ThC i={7}  k="BATERIA"                label="Batería" />
                    <ThC i={8}  k="ESTADO_POZO"            label="Estado Pozo" />
                    <ThC i={9}  k="TIPO_PRODUCCION"        label="Tipo Producción" />
                    <ThC i={10} k="SIST_EXTRACCION"        label="Sist. Extracción" />
                  </tr>
                </thead>
                <tbody>
                  {sortedControles.map((row, i) => (
                    <tr key={i} className={`border-b border-[#1a2535] hover:bg-[#1e293b] transition-colors ${i % 2 === 0 ? "bg-[#0f172a]" : "bg-[#0d1526]"}`}>
                      <td style={{ width: colWidthsC[0]  }} className="px-2 py-1.5 font-mono text-xs text-slate-300 truncate">{row.Pozo ?? "—"}</td>
                      <td style={{ width: colWidthsC[1]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row["Día Operativo"] ? String(row["Día Operativo"]).slice(0, 10) : "—"}</td>
                      <td style={{ width: colWidthsC[2]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{fmtDate(row["Fecha y Hora"])}</td>
                      <td style={{ width: colWidthsC[3]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.Estado ?? "—"}</td>
                      <td style={{ width: colWidthsC[4]  }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row["Producción de Gas"])}</td>
                      <td style={{ width: colWidthsC[5]  }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row["Producción de Líquido"])}</td>
                      <td style={{ width: colWidthsC[6]  }} className="px-2 py-1.5 text-xs text-sky-300 text-right font-mono">{fmtNum(row["Producción de Petróleo"])}</td>
                      <td style={{ width: colWidthsC[7]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.BATERIA ?? "—"}</td>
                      <td style={{ width: colWidthsC[8]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.ESTADO_POZO ?? "—"}</td>
                      <td style={{ width: colWidthsC[9]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.TIPO_PRODUCCION ?? "—"}</td>
                      <td style={{ width: colWidthsC[10] }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.SIST_EXTRACCION ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )

        ) : (

          /* ── Tabla Merma ── */
          sortedMerma.length === 0 ? (
            <div className="flex items-center justify-center h-64">
              <p className="text-slate-500 text-sm">No hay datos de merma disponibles.</p>
            </div>
          ) : (
            <div className="rounded-lg border border-[#334155] overflow-hidden">
              <table className="text-left" style={{ borderCollapse: "collapse", width: colWidthsM.reduce((a, b) => a + b, 0) }}>
                <thead className="bg-[#1e293b] sticky top-0 z-10">
                  <tr>
                    <ThM i={0}  k="POZO"               label="Pozo" />
                    <ThM i={1}  k="BATERIA"             label="Batería" />
                    <ThM i={2}  k="DIAS_SIN_CONTROL"    label="Días sin ctrl." right />
                    <ThM i={3}  k="ESTADO_POZO"         label="Estado Pozo" />
                    <ThM i={4}  k="SIST_EXTRACCION"     label="Sist. Extracción" />
                    <ThM i={5}  k="FECHA_ULTIMO_CONTROL" label="Último ctrl." />
                    <ThM i={6}  k="NETA_PENULTIMO_M3"   label="Neta Ant. (m³)" right />
                    <ThM i={7}  k="NETA_ULTIMO_M3"      label="Neta Últ. (m³)" right />
                    <ThM i={8}  k="PCT_MERMA_NETA"      label="% Merma Neta" right />
                    <ThM i={9}  k="BRUTA_PENULTIMO_M3"  label="Bruta Ant. (m³)" right />
                    <ThM i={10} k="BRUTA_ULTIMO_M3"     label="Bruta Últ. (m³)" right />
                    <ThM i={11} k="PCT_MERMA_BRUTA"     label="% Merma Bruta" right />
                    <ThM i={12} k="TIPO_PRODUCCION"     label="Tipo Prod." />
                  </tr>
                </thead>
                <tbody>
                  {sortedMerma.map((row, i) => (
                    <tr key={i} className={`border-b border-[#1a2535] hover:bg-[#1e293b] transition-colors ${i % 2 === 0 ? "bg-[#0f172a]" : "bg-[#0d1526]"}`}>
                      <td style={{ width: colWidthsM[0]  }} className="px-2 py-1.5 font-mono text-xs text-slate-300 truncate">{row.POZO ?? "—"}</td>
                      <td style={{ width: colWidthsM[1]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.BATERIA ?? "—"}</td>
                      <td style={{ width: colWidthsM[2]  }} className="px-2 py-1.5 text-xs text-right"><Diasbadge v={row.DIAS_SIN_CONTROL} /></td>
                      <td style={{ width: colWidthsM[3]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.ESTADO_POZO ?? "—"}</td>
                      <td style={{ width: colWidthsM[4]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.SIST_EXTRACCION ?? "—"}</td>
                      <td style={{ width: colWidthsM[5]  }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.FECHA_ULTIMO_CONTROL ?? "—"}</td>
                      <td style={{ width: colWidthsM[6]  }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row.NETA_PENULTIMO_M3)}</td>
                      <td style={{ width: colWidthsM[7]  }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row.NETA_ULTIMO_M3)}</td>
                      <td style={{ width: colWidthsM[8]  }} className="px-2 py-1.5 text-xs text-right"><PctBadge v={row.PCT_MERMA_NETA} /></td>
                      <td style={{ width: colWidthsM[9]  }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row.BRUTA_PENULTIMO_M3)}</td>
                      <td style={{ width: colWidthsM[10] }} className="px-2 py-1.5 text-xs text-slate-400 text-right font-mono">{fmtNum(row.BRUTA_ULTIMO_M3)}</td>
                      <td style={{ width: colWidthsM[11] }} className="px-2 py-1.5 text-xs text-right"><PctBadge v={row.PCT_MERMA_BRUTA} /></td>
                      <td style={{ width: colWidthsM[12] }} className="px-2 py-1.5 text-xs text-slate-400 truncate">{row.TIPO_PRODUCCION ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )
        )}
      </div>
    </div>
  );
}
