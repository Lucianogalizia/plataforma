"use client";

import { useEffect, useState, useCallback } from "react";
import api, { SnapRow, SnapKpis, TendenciaPozo, PozosMes, Cobertura } from "@/lib/api";
import KPICard from "@/components/KPICard";
import SemaforoAIB from "@/components/SemaforoAIB";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  HistogramLike, ScatterChart, Scatter, Legend,
} from "recharts";

const TREND_VARS = ["Sumergencia","PB","NM","NC","ND","%Estructura","%Balance","GPM","Caudal bruto efec"];

const SEV_COLORS: Record<string,string> = {
  "🟢 NORMAL":  "#22c55e",
  "🟡 ALERTA":  "#eab308",
  "🔴 CRÍTICO": "#ef4444",
  "SIN DATOS":  "#64748b",
};

function histo(data: (number|null|undefined)[], bins = 20) {
  const vals = data.filter((v) => v != null) as number[];
  if (!vals.length) return [];
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const step = (max - min) / bins || 1;
  const counts: Record<string,number> = {};
  vals.forEach((v) => {
    const b = Math.floor((v - min) / step);
    const key = (min + b * step).toFixed(1);
    counts[key] = (counts[key] || 0) + 1;
  });
  return Object.entries(counts).map(([x, y]) => ({ x: parseFloat(x), y }));
}

export default function EstadisticasPage() {
  const [snap, setSnap] = useState<SnapRow[]>([]);
  const [kpis, setKpis] = useState<SnapKpis | null>(null);
  const [loading, setLoading] = useState(true);

  const [sumRange, setSumRange] = useState<[number,number]>([0,10000]);
  const [origenSel, setOrigenSel] = useState<string[]>([]);
  const [origenOpts, setOrigenOpts] = useState<string[]>([]);

  // Tendencias
  const [trendVar, setTrendVar] = useState("Sumergencia");
  const [minPts, setMinPts] = useState(4);
  const [soloPos, setSoloPos] = useState(true);
  const [tendencias, setTendencias] = useState<TendenciaPozo[]>([]);
  const [loadingTend, setLoadingTend] = useState(false);

  // Pozos por mes
  const [ppm, setPpm] = useState<PozosMes[]>([]);
  const [ultimoMes, setUltimoMes] = useState("");
  const [ultimoVal, setUltimoVal] = useState(0);

  // Cobertura
  const [cobFrom, setCobFrom] = useState("");
  const [cobTo, setCobTo] = useState("");
  const [cobModo, setCobModo] = useState("historico");
  const [cobertura, setCobertura] = useState<Cobertura|null>(null);

  // Semáforo AIB params
  const [semaforoData, setSemaforoData] = useState<any>(null);
  const [sumMedia, setSumMedia] = useState(200);
  const [sumAlta, setSumAlta]  = useState(250);
  const [llenOk, setLlenOk]   = useState(70);
  const [llenBajo, setLlenBajo] = useState(50);

  const loadSnap = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.getSnapshot();
      setSnap(res.snap);
      setKpis(res.kpis);
      const origs = [...new Set(res.snap.map((r) => r.ORIGEN).filter(Boolean) as string[])].sort();
      setOrigenOpts(origs);
      setOrigenSel(origs);
      const sums = res.snap.map((r) => r.Sumergencia).filter((v) => v != null) as number[];
      if (sums.length) setSumRange([Math.min(...sums), Math.max(...sums)]);
    } catch {}
    setLoading(false);
  }, []);

  const loadTend = useCallback(async () => {
    setLoadingTend(true);
    try {
      const res = await api.getTendencias({ variable: trendVar, min_pts: minPts, solo_positiva: soloPos, top: 100 });
      setTendencias(res.pozos);
    } catch {}
    setLoadingTend(false);
  }, [trendVar, minPts, soloPos]);

  const loadPpm = useCallback(async () => {
    try {
      const res = await api.getPozosPorMes();
      setPpm(res.serie);
      setUltimoMes(res.ultimo_mes || "");
      setUltimoVal(res.ultimo_valor || 0);
    } catch {}
  }, []);

  const loadCobertura = useCallback(async () => {
    if (!cobFrom || !cobTo) return;
    try {
      const res = await api.getCobertura({ fecha_desde: cobFrom, fecha_hasta: cobTo, modo: cobModo });
      setCobertura(res);
    } catch {}
  }, [cobFrom, cobTo, cobModo]);

  const loadSemaforo = useCallback(async () => {
    try {
      const res = await api.getSemaforoAib({
        sum_media: sumMedia, sum_alta: sumAlta,
        llen_ok: llenOk, llen_bajo: llenBajo, solo_se_aib: true
      });
      setSemaforoData(res);
    } catch {}
  }, [sumMedia, sumAlta, llenOk, llenBajo]);

  useEffect(() => { loadSnap(); loadPpm(); }, [loadSnap, loadPpm]);
  useEffect(() => { loadTend(); }, [loadTend]);
  useEffect(() => { loadSemaforo(); }, [loadSemaforo]);

  // Filtrar snap
  const snapF = snap.filter((r) => {
    if (origenSel.length && !origenSel.includes(r.ORIGEN || "")) return false;
    if (r.Sumergencia != null && (r.Sumergencia < sumRange[0] || r.Sumergencia > sumRange[1])) return false;
    return true;
  });

  const histSum  = histo(snapF.map((r) => r.Sumergencia));
  const histPB   = histo(snapF.map((r) => r.PB));
  const histDias = histo(snapF.map((r) => r.Dias_desde_ultima));

  const origenCount = origenSel.length
    ? Object.entries(
        snapF.reduce((acc, r) => {
          const o = r.ORIGEN || "?";
          acc[o] = (acc[o] || 0) + 1;
          return acc;
        }, {} as Record<string,number>)
      ).map(([o, c]) => ({ ORIGEN: o, Pozos: c }))
    : [];

  const ebData = snapF
    .filter((r) => r["%Estructura"] != null && r["%Balance"] != null)
    .map((r) => ({ x: r["%Estructura"]!, y: r["%Balance"]!, name: r.NO_key }));

  // Calidad del dato
  const badSum  = snapF.filter((r) => r.Sumergencia != null && r.Sumergencia < 0);
  const pbVals  = snapF.map((r) => r.PB).filter((v) => v != null) as number[];
  const q1pb    = pbVals.sort((a,b)=>a-b)[Math.floor(pbVals.length*0.25)] ?? 0;
  const q3pb    = pbVals[Math.floor(pbVals.length*0.75)] ?? 0;
  const iqr     = q3pb - q1pb;
  const badPB   = snapF.filter((r) => r.PB != null && (r.PB! < q1pb - 1.5*iqr || r.PB! > q3pb + 1.5*iqr));

  const topTend = tendencias.slice(0, 30).sort((a,b) => a.pendiente_por_mes - b.pendiente_por_mes);

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-xl font-bold text-slate-100">📊 Estadísticas (última medición por pozo)</h2>
      </div>

      {loading && <p className="text-slate-500 text-sm animate-pulse">Cargando snapshot…</p>}

      {!loading && kpis && (
        <>
          {/* KPIs */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            <KPICard title="Pozos (snapshot)"  value={kpis.total_pozos}     color="sky" />
            <KPICard title="Última = DIN"       value={kpis.ultima_din} />
            <KPICard title="Última = NIV"       value={kpis.ultima_niv} />
            <KPICard title="Con Sumergencia"    value={kpis.con_sumergencia} color="green" />
            <KPICard title="Con PB"             value={kpis.con_pb} />
          </div>

          {/* Filtros */}
          <div className="card space-y-3">
            <h3 className="text-sm font-semibold text-slate-300">Filtros snapshot</h3>
            <div className="flex flex-wrap gap-4">
              <div>
                <label className="text-xs text-slate-400 block mb-1">Origen</label>
                <div className="flex gap-2">
                  {origenOpts.map((o) => (
                    <label key={o} className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={origenSel.includes(o)}
                        onChange={(e) =>
                          setOrigenSel((prev) =>
                            e.target.checked ? [...prev, o] : prev.filter((x) => x !== o)
                          )
                        }
                        className="accent-sky-400"
                      />
                      {o}
                    </label>
                  ))}
                </div>
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">
                  Rango Sumergencia: {sumRange[0].toFixed(0)} – {sumRange[1].toFixed(0)}
                </label>
                <input
                  type="range"
                  min={0} max={sumRange[1]} step={1}
                  value={sumRange[0]}
                  onChange={(e) => setSumRange([+e.target.value, sumRange[1]])}
                  className="accent-sky-400 w-32"
                />
                <input
                  type="range"
                  min={sumRange[0]} max={5000} step={1}
                  value={sumRange[1]}
                  onChange={(e) => setSumRange([sumRange[0], +e.target.value])}
                  className="accent-sky-400 w-32 ml-2"
                />
              </div>
            </div>
          </div>

          {/* KPIs filtrados */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <KPICard title="Pozos filtrados" value={snapF.length} color="sky" />
            <KPICard title="Sumer. < 0"      value={badSum.length}  color={badSum.length > 0 ? "red" : "default"} />
            <KPICard title="PB anómalo"       value={badPB.length}  color={badPB.length > 0 ? "yellow" : "default"} />
            <KPICard title="PB faltante"      value={snapF.filter(r => r.PB == null).length} />
          </div>

          {/* Tabla snapshot */}
          <div className="card p-0 overflow-hidden">
            <div className="px-4 py-3 border-b border-[#334155]">
              <h3 className="text-sm font-medium text-slate-300">📋 Pozos — última medición (filtrados)</h3>
            </div>
            <div className="overflow-x-auto max-h-80">
              <table className="text-xs">
                <thead className="sticky top-0 z-10">
                  <tr>
                    {["NO_key","Bateria","Tipo AIB","ORIGEN","SE","DT_plot","Días","PB","Sumergencia","AIB Carrera","%Estructura","%Balance","Bba Llenado","GPM","Caudal bruto efec"].map((h) => (
                      <th key={h} className="bg-[#1e293b] border-b border-[#334155] px-3 py-2 whitespace-nowrap">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {[...snapF]
                    .sort((a, b) => (a.Dias_desde_ultima || 0) - (b.Dias_desde_ultima || 0))
                    .map((r, i) => (
                    <tr key={i}>
                      <td className="px-3 py-1.5 font-mono text-slate-300">{r.NO_key}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r.Bateria || "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r["Tipo AIB"] || "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r.ORIGEN || "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r.SE || "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400 whitespace-nowrap">{r.DT_plot?.slice(0,10) || "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r.Dias_desde_ultima?.toFixed(0) || "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r.PB ?? "—"}</td>
                      <td className="px-3 py-1.5 text-sky-300 font-semibold">
                        {r.Sumergencia != null ? r.Sumergencia.toFixed(1) : "—"}
                      </td>
                      <td className="px-3 py-1.5 text-slate-400">{r["AIB Carrera"] ?? "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r["%Estructura"] ?? "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r["%Balance"] ?? "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r["Bba Llenado"] ?? "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r.GPM ?? "—"}</td>
                      <td className="px-3 py-1.5 text-slate-400">{r["Caudal bruto efec"] ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Gráficos snapshot */}
          <div>
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3">
              📈 Gráficos (snapshot, DIN+NIV mezclados)
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Por origen */}
              <div className="card p-0 overflow-hidden">
                <div className="px-4 py-2 border-b border-[#334155] text-xs text-slate-400">Pozos por ORIGEN</div>
                <div className="p-3">
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={origenCount}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="ORIGEN" tick={{ fill: "#94a3b8", fontSize: 11 }} stroke="#64748b" />
                      <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} stroke="#64748b" />
                      <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155" }} />
                      <Bar dataKey="Pozos" fill="#38bdf8" radius={[4,4,0,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Histograma días */}
              <div className="card p-0 overflow-hidden">
                <div className="px-4 py-2 border-b border-[#334155] text-xs text-slate-400">Antigüedad de última medición (días)</div>
                <div className="p-3">
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={histDias}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="x" tick={{ fill: "#94a3b8", fontSize: 10 }} stroke="#64748b" />
                      <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} stroke="#64748b" />
                      <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155" }} />
                      <Bar dataKey="y" fill="#22c55e" radius={[2,2,0,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Histograma Sumergencia */}
              <div className="card p-0 overflow-hidden">
                <div className="px-4 py-2 border-b border-[#334155] text-xs text-slate-400">Distribución de Sumergencia</div>
                <div className="p-3">
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={histSum}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="x" tick={{ fill: "#94a3b8", fontSize: 10 }} stroke="#64748b" />
                      <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} stroke="#64748b" />
                      <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155" }} />
                      <Bar dataKey="y" fill="#38bdf8" radius={[2,2,0,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Histograma PB */}
              <div className="card p-0 overflow-hidden">
                <div className="px-4 py-2 border-b border-[#334155] text-xs text-slate-400">Distribución de PB</div>
                <div className="p-3">
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={histPB}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="x" tick={{ fill: "#94a3b8", fontSize: 10 }} stroke="#64748b" />
                      <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} stroke="#64748b" />
                      <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155" }} />
                      <Bar dataKey="y" fill="#a78bfa" radius={[2,2,0,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          </div>

          {/* %Estructura vs %Balance */}
          {ebData.length > 0 && (
            <div className="card p-0 overflow-hidden">
              <div className="px-4 py-2 border-b border-[#334155] text-xs text-slate-400">
                🧰 %Estructura vs %Balance (DIN-only)
              </div>
              <div className="p-3">
                <ResponsiveContainer width="100%" height={280}>
                  <ScatterChart margin={{ top: 8, right: 24, left: 8, bottom: 8 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey="x" name="%Estructura" tick={{ fill: "#94a3b8", fontSize: 11 }} stroke="#64748b" />
                    <YAxis dataKey="y" name="%Balance" tick={{ fill: "#94a3b8", fontSize: 11 }} stroke="#64748b" />
                    <Tooltip
                      cursor={{ strokeDasharray: "3 3" }}
                      contentStyle={{ background: "#1e293b", border: "1px solid #334155" }}
                      content={({ payload }) => {
                        const d = payload?.[0]?.payload;
                        if (!d) return null;
                        return (
                          <div className="text-xs p-2">
                            <p className="text-slate-200 font-mono">{d.name}</p>
                            <p className="text-slate-400">%Estructura: {d.x}</p>
                            <p className="text-slate-400">%Balance: {d.y}</p>
                          </div>
                        );
                      }}
                    />
                    <Scatter data={ebData} fill="#f97316" opacity={0.7} />
                  </ScatterChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {/* Pozos por mes */}
          <div className="space-y-2">
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              🛢️ Pozos medidos por mes
            </h3>
            {ultimoMes && (
              <p className="text-sm text-slate-300">
                📌 Último mes ({ultimoMes}): <strong className="text-sky-400">{ultimoVal}</strong> pozos medidos
              </p>
            )}
            {ppm.length > 0 && (
              <div className="card p-0 overflow-hidden">
                <div className="p-3">
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={ppm}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                      <XAxis dataKey="Mes" tick={{ fill: "#94a3b8", fontSize: 10 }} stroke="#64748b" />
                      <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} stroke="#64748b" />
                      <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155" }} />
                      <Bar dataKey="Pozos_medidos" fill="#38bdf8" radius={[3,3,0,0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}
          </div>

          {/* Cobertura DIN vs NIV */}
          <div className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              ✅ Cobertura DIN vs NIV
            </h3>
            <div className="flex flex-wrap gap-3 items-end">
              <div>
                <label className="text-xs text-slate-400 block mb-1">Desde</label>
                <input type="date" value={cobFrom} onChange={(e) => setCobFrom(e.target.value)}
                  className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200" />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Hasta</label>
                <input type="date" value={cobTo} onChange={(e) => setCobTo(e.target.value)}
                  className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200" />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Modo</label>
                <select value={cobModo} onChange={(e) => setCobModo(e.target.value)}
                  className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200">
                  <option value="historico">Todas las mediciones</option>
                  <option value="snapshot">Última por pozo</option>
                </select>
              </div>
              <button onClick={loadCobertura}
                className="px-4 py-1.5 bg-sky-600 hover:bg-sky-500 rounded text-sm font-medium text-white transition-colors">
                Calcular
              </button>
            </div>

            {cobertura && (
              <>
                <div className="grid grid-cols-3 gap-3">
                  <KPICard title="Pozos en ventana"     value={cobertura.total_pozos} />
                  <KPICard title="Con DIN"              value={cobertura.pozos_con_din} color="green" />
                  <KPICard title="Sin DIN"              value={cobertura.pozos_sin_din} color={cobertura.pozos_sin_din > 0 ? "red" : "default"} />
                </div>
                {cobertura.lista_sin_din.length > 0 && (
                  <details className="card text-xs">
                    <summary className="cursor-pointer text-slate-400 hover:text-slate-200">
                      Ver {cobertura.lista_sin_din.length} pozos sin DIN en la ventana
                    </summary>
                    <p className="mt-2 text-slate-400 break-words">
                      {cobertura.lista_sin_din.join(", ")}
                    </p>
                  </details>
                )}
              </>
            )}
          </div>

          {/* Calidad del dato */}
          <div className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              🧪 Calidad del dato
            </h3>
            <div className="grid grid-cols-3 gap-3">
              <KPICard title="Sumergencia < 0" value={badSum.length}  color={badSum.length > 0 ? "red" : "green"} />
              <KPICard title="PB anómalo (IQR)" value={badPB.length} color={badPB.length > 0 ? "yellow" : "green"} />
              <KPICard title="PB faltante"      value={snapF.filter(r=>r.PB==null).length} />
            </div>
            {badSum.length > 0 && (
              <details className="card text-xs">
                <summary className="cursor-pointer text-slate-400">Pozos con Sumergencia &lt; 0</summary>
                <div className="mt-2 overflow-x-auto">
                  <table className="text-xs w-full">
                    <thead><tr>
                      {["NO_key","ORIGEN","DT_plot","PB","Sumergencia"].map(h=>(
                        <th key={h} className="text-left px-2 py-1 text-slate-500">{h}</th>
                      ))}
                    </tr></thead>
                    <tbody>
                      {badSum.map((r,i)=>(
                        <tr key={i} className="border-t border-[#334155]">
                          <td className="px-2 py-1 font-mono">{r.NO_key}</td>
                          <td className="px-2 py-1">{r.ORIGEN}</td>
                          <td className="px-2 py-1">{r.DT_plot?.slice(0,10)}</td>
                          <td className="px-2 py-1">{r.PB}</td>
                          <td className="px-2 py-1 text-red-400">{r.Sumergencia?.toFixed(1)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </details>
            )}
          </div>

          {/* Tendencias */}
          <div className="space-y-4">
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">
              📈 Pozos con tendencia en aumento
            </h3>
            <div className="flex flex-wrap gap-4 items-end">
              <div>
                <label className="text-xs text-slate-400 block mb-1">Variable</label>
                <select value={trendVar} onChange={(e) => setTrendVar(e.target.value)}
                  className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200">
                  {TREND_VARS.map(v => <option key={v} value={v}>{v}</option>)}
                </select>
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Mín. puntos</label>
                <input type="number" min={2} max={20} value={minPts}
                  onChange={(e) => setMinPts(+e.target.value)}
                  className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 w-20" />
              </div>
              <label className="flex items-center gap-2 text-sm text-slate-300">
                <input type="checkbox" checked={soloPos} onChange={(e) => setSoloPos(e.target.checked)}
                  className="accent-sky-400" />
                Solo pendiente positiva
              </label>
            </div>

            {loadingTend && <p className="text-slate-500 text-sm animate-pulse">Calculando tendencias…</p>}

            {!loadingTend && tendencias.length > 0 && (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {/* Tabla */}
                <div className="card p-0 overflow-hidden">
                  <div className="overflow-x-auto max-h-80">
                    <table className="text-xs">
                      <thead className="sticky top-0">
                        <tr>
                          {["Pozo","Puntos","Pend/mes","Delta","V.ini","V.fin","Desde","Hasta"].map(h=>(
                            <th key={h} className="bg-[#1e293b] border-b border-[#334155] px-3 py-2 whitespace-nowrap">{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {tendencias.slice(0,100).map((r,i) => (
                          <tr key={i} className="border-b border-[#334155]">
                            <td className="px-3 py-1.5 font-mono text-slate-300">{r.NO_key}</td>
                            <td className="px-3 py-1.5 text-slate-400">{r.n_puntos}</td>
                            <td className="px-3 py-1.5 text-sky-400 font-semibold">{r.pendiente_por_mes.toFixed(3)}</td>
                            <td className="px-3 py-1.5 text-slate-400">{r.delta_total.toFixed(1)}</td>
                            <td className="px-3 py-1.5 text-slate-400">{r.valor_inicial.toFixed(1)}</td>
                            <td className="px-3 py-1.5 text-slate-400">{r.valor_final.toFixed(1)}</td>
                            <td className="px-3 py-1.5 text-slate-500 whitespace-nowrap">{r.fecha_inicial.slice(0,10)}</td>
                            <td className="px-3 py-1.5 text-slate-500 whitespace-nowrap">{r.fecha_final.slice(0,10)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Gráfico top 30 */}
                <div className="card p-0 overflow-hidden">
                  <div className="px-4 py-2 border-b border-[#334155] text-xs text-slate-400">
                    Top 30 — Pendiente por mes ({trendVar})
                  </div>
                  <div className="p-3">
                    <ResponsiveContainer width="100%" height={380}>
                      <BarChart data={topTend} layout="vertical" margin={{ left: 60 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                        <XAxis type="number" tick={{ fill: "#94a3b8", fontSize: 10 }} stroke="#64748b" />
                        <YAxis type="category" dataKey="NO_key" tick={{ fill: "#94a3b8", fontSize: 9 }} stroke="#64748b" width={55} />
                        <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155" }} />
                        <Bar dataKey="pendiente_por_mes" fill="#f97316" radius={[0,3,3,0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            )}
          </div>
        </>
      )}

      {/* Semáforo AIB — sección independiente */}
      <div className="border-t border-[#334155] pt-8">
        <h2 className="text-lg font-bold text-slate-100 mb-4">🚦 Semáforo AIB (SE = AIB)</h2>

        {/* Umbrales */}
        <div className="card mb-4">
          <h3 className="text-sm font-semibold text-slate-300 mb-3">⚙️ Umbrales Semáforo AIB</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {[
              { label: "Sumergencia media (m)", val: sumMedia, set: setSumMedia },
              { label: "Sumergencia alta (m)",  val: sumAlta,  set: setSumAlta  },
              { label: "Llenado OK (≥ %)",      val: llenOk,   set: setLlenOk   },
              { label: "Llenado bajo (< %)",    val: llenBajo, set: setLlenBajo  },
            ].map(({ label, val, set }) => (
              <div key={label}>
                <label className="text-xs text-slate-400 block mb-1">{label}</label>
                <input
                  type="number"
                  value={val}
                  onChange={(e) => set(+e.target.value)}
                  className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 w-full"
                />
              </div>
            ))}
          </div>
        </div>

        {semaforoData && (
          <SemaforoAIB
            rows={semaforoData.rows}
            kpis={{
              total:    semaforoData.total,
              criticos: semaforoData.criticos,
              alertas:  semaforoData.alertas,
              normales: semaforoData.normales,
              sin_datos:semaforoData.sin_datos,
            }}
            onRefresh={loadSemaforo}
          />
        )}
      </div>
    </div>
  );
}
