"use client";

import { useEffect, useState, useCallback } from "react";
import api, { SnapRow, SnapKpis, TendenciaPozo, PozosMes, Cobertura } from "@/lib/api";
import KPICard from "@/components/KPICard";
import SemaforoAIB from "@/components/SemaforoAIB";
import SortableTable from "@/components/SortableTable";
import PlotlyChart from "@/components/PlotlyChart";

const TREND_VARS = ["Sumergencia","PB","NM","NC","ND","%Estructura","%Balance","GPM","Caudal bruto efec"];

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
  const [estRange, setEstRange] = useState<[number,number]>([0,200]);
  const [balRange, setBalRange] = useState<[number,number]>([0,200]);
  const [origenSel, setOrigenSel] = useState<string[]>([]);
  const [origenOpts, setOrigenOpts] = useState<string[]>([]);
  const [pozoSearch, setPozoSearch] = useState("");

  const [trendVar, setTrendVar] = useState("Sumergencia");
  const [minPts, setMinPts] = useState(4);
  const [soloPos, setSoloPos] = useState(true);
  const [tendencias, setTendencias] = useState<TendenciaPozo[]>([]);
  const [loadingTend, setLoadingTend] = useState(false);

  const [ppm, setPpm] = useState<PozosMes[]>([]);
  const [ultimoMes, setUltimoMes] = useState("");
  const [ultimoVal, setUltimoVal] = useState(0);

  const [cobFrom, setCobFrom] = useState("");
  const [cobTo, setCobTo] = useState("");
  const [cobModo, setCobModo] = useState("historico");
  const [cobertura, setCobertura] = useState<Cobertura|null>(null);

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
      const ests = res.snap.map((r) => r["%Estructura"]).filter((v) => v != null) as number[];
      if (ests.length) setEstRange([Math.min(...ests), Math.max(...ests)]);
      const bals = res.snap.map((r) => r["%Balance"]).filter((v) => v != null) as number[];
      if (bals.length) setBalRange([Math.min(...bals), Math.max(...bals)]);
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

  const loadSemaforo = useCallback(async (sm = sumMedia, sa = sumAlta, lo = llenOk, lb = llenBajo) => {
    try {
      const res = await api.getSemaforoAib({ sum_media: sm, sum_alta: sa, llen_ok: lo, llen_bajo: lb, solo_se_aib: true });
      setSemaforoData(res);
    } catch {}
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => { loadSnap(); loadPpm(); }, [loadSnap, loadPpm]);
  useEffect(() => { loadTend(); }, [loadTend]);
  useEffect(() => { loadSemaforo(); }, [loadSemaforo]);

  const snapF = snap.filter((r) => {
    if (origenSel.length && !origenSel.includes(r.ORIGEN || "")) return false;
    if (r.Sumergencia != null && (r.Sumergencia < sumRange[0] || r.Sumergencia > sumRange[1])) return false;
    if (r["%Estructura"] != null && (r["%Estructura"]! < estRange[0] || r["%Estructura"]! > estRange[1])) return false;
    if (r["%Balance"] != null && (r["%Balance"]! < balRange[0] || r["%Balance"]! > balRange[1])) return false;
    if (pozoSearch && !r.NO_key?.toLowerCase().includes(pozoSearch.toLowerCase())) return false;
    return true;
  });

  const histSum  = histo(snapF.map((r) => r.Sumergencia));
  const histPB   = histo(snapF.map((r) => r.PB));
  const histDias = histo(snapF.map((r) => r.Dias_desde_ultima));

  const origenCount = Object.entries(
    snapF.reduce((acc, r) => { const o = r.ORIGEN || "?"; acc[o] = (acc[o]||0)+1; return acc; }, {} as Record<string,number>)
  ).map(([o,c]) => ({ ORIGEN: o, Pozos: c }));

  const ebData = snapF
    .filter((r) => r["%Estructura"] != null && r["%Balance"] != null)
    .map((r) => ({ x: r["%Estructura"]!, y: r["%Balance"]!, name: r.NO_key }));

  const badSum = snapF.filter((r) => r.Sumergencia != null && r.Sumergencia < 0);
  const pbVals = snapF.map((r) => r.PB).filter((v) => v != null) as number[];
  const q1pb   = pbVals.sort((a,b)=>a-b)[Math.floor(pbVals.length*0.25)] ?? 0;
  const q3pb   = pbVals[Math.floor(pbVals.length*0.75)] ?? 0;
  const iqr    = q3pb - q1pb;
  const badPB  = snapF.filter((r) => r.PB != null && (r.PB! < q1pb - 1.5*iqr || r.PB! > q3pb + 1.5*iqr));

  const snapCols = [
    { key: "NO_key",                      label: "NO_key" },
    { key: "Bateria",                     label: "Bateria" },
    { key: "Tipo AIB",                    label: "Tipo AIB" },
    { key: "ORIGEN",                      label: "ORIGEN" },
    { key: "SE",                          label: "SE" },
    { key: "DT_plot",                     label: "DT_plot", render: (v: string) => v?.slice(0,10) || "—" },
    { key: "Dias_desde_ultima",           label: "Días",    render: (v: number) => v?.toFixed(0) || "—" },
    { key: "PE",                          label: "PE" },
    { key: "PB",                          label: "PB" },
    { key: "NM",                          label: "NM" },
    { key: "NC",                          label: "NC" },
    { key: "ND",                          label: "ND" },
    { key: "Sumergencia",                 label: "Sumergencia", render: (v: number) => v != null ? <span className="text-sky-300 font-semibold">{v.toFixed(1)}</span> : "—" },
    { key: "Sumergencia_base",            label: "Sumer. base" },
    { key: "AIB Carrera",                 label: "AIB Carrera" },
    { key: "Sentido giro",                label: "Sentido giro" },
    { key: "Tipo Contrapesos",            label: "Tipo Contrapesos" },
    { key: "Distancia contrapesos (cm)",  label: "Dist. contrapesos" },
    { key: "Contrapeso actual",           label: "Contrapeso actual" },
    { key: "Contrapeso ideal",            label: "Contrapeso ideal" },
    { key: "AIBEB_Torque max contrapeso", label: "Torque max" },
    { key: "Bba Diam Pistón",             label: "Bba Diam Pistón" },
    { key: "Bba Llenado",                 label: "Bba Llenado" },
    { key: "GPM",                         label: "GPM" },
    { key: "Caudal bruto efec",           label: "Caudal bruto efec" },
    { key: "Polea Motor",                 label: "Polea Motor" },
    { key: "Potencia Motor",              label: "Potencia Motor" },
    { key: "RPM Motor",                   label: "RPM Motor" },
    { key: "%Estructura",                 label: "%Estructura" },
    { key: "%Balance",                    label: "%Balance" },
  ];

  const tendCols = [
    { key: "NO_key",             label: "Pozo" },
    { key: "n_puntos",           label: "Puntos" },
    { key: "pendiente_por_mes",  label: "Pend/mes", render: (v: number) => <span className="text-sky-400 font-semibold">{v?.toFixed(3)}</span> },
    { key: "delta_total",        label: "Delta",    render: (v: number) => v?.toFixed(1) },
    { key: "valor_inicial",      label: "V.ini",    render: (v: number) => v?.toFixed(1) },
    { key: "valor_final",        label: "V.fin",    render: (v: number) => v?.toFixed(1) },
    { key: "fecha_inicial",      label: "Desde",    render: (v: string) => v?.slice(0,10) },
    { key: "fecha_final",        label: "Hasta",    render: (v: string) => v?.slice(0,10) },
  ];

  const topTend = [...tendencias].sort((a,b) => a.pendiente_por_mes - b.pendiente_por_mes).slice(0,30);

  return (
    <div className="space-y-8">
      <h2 className="text-xl font-bold text-slate-100">📊 Estadísticas (última medición por pozo)</h2>

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
                      <input type="checkbox" checked={origenSel.includes(o)}
                        onChange={(e) => setOrigenSel((prev) => e.target.checked ? [...prev, o] : prev.filter((x) => x !== o))}
                        className="accent-sky-400" />
                      {o}
                    </label>
                  ))}
                </div>
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Rango Sumergencia: {sumRange[0].toFixed(0)} – {sumRange[1].toFixed(0)}</label>
                <input type="range" min={0} max={sumRange[1]} step={1} value={sumRange[0]} onChange={(e) => setSumRange([+e.target.value, sumRange[1]])} className="accent-sky-400 w-32" />
                <input type="range" min={sumRange[0]} max={5000} step={1} value={sumRange[1]} onChange={(e) => setSumRange([sumRange[0], +e.target.value])} className="accent-sky-400 w-32 ml-2" />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Rango %Estructura: {estRange[0].toFixed(1)} – {estRange[1].toFixed(1)}</label>
                <input type="range" min={0} max={estRange[1]} step={0.1} value={estRange[0]} onChange={(e) => setEstRange([+e.target.value, estRange[1]])} className="accent-sky-400 w-32" />
                <input type="range" min={estRange[0]} max={200} step={0.1} value={estRange[1]} onChange={(e) => setEstRange([estRange[0], +e.target.value])} className="accent-sky-400 w-32 ml-2" />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Rango %Balance: {balRange[0].toFixed(1)} – {balRange[1].toFixed(1)}</label>
                <input type="range" min={0} max={balRange[1]} step={0.1} value={balRange[0]} onChange={(e) => setBalRange([+e.target.value, balRange[1]])} className="accent-sky-400 w-32" />
                <input type="range" min={balRange[0]} max={200} step={0.1} value={balRange[1]} onChange={(e) => setBalRange([balRange[0], +e.target.value])} className="accent-sky-400 w-32 ml-2" />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Buscar pozo (NO_KEY)</label>
                <div className="flex items-center gap-2">
                  <input
                    type="text"
                    value={pozoSearch}
                    onChange={(e) => setPozoSearch(e.target.value)}
                    placeholder="Escribí el nombre del pozo..."
                    list="pozo-options"
                    className="bg-slate-800 border border-slate-600 rounded px-3 py-1.5 text-sm text-slate-100 placeholder:text-slate-500 focus:outline-none focus:border-sky-400 w-56"
                  />
                  {pozoSearch && (
                    <button
                      onClick={() => setPozoSearch("")}
                      className="text-xs text-slate-400 hover:text-slate-200 underline whitespace-nowrap"
                    >
                      limpiar
                    </button>
                  )}
                </div>
                <datalist id="pozo-options">
                  {[...new Set(snap.map(r => r.NO_key).filter(Boolean))].sort().map(p => (
                    <option key={p} value={p} />
                  ))}
                </datalist>
              </div>
            </div>
          </div>

          {/* KPIs filtrados */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <KPICard title="Pozos filtrados" value={snapF.length} color="sky" />
            <KPICard title="Sumer. < 0"      value={badSum.length} color={badSum.length > 0 ? "red" : "default"} />
            <KPICard title="PB anómalo"       value={badPB.length} color={badPB.length > 0 ? "yellow" : "default"} />
            <KPICard title="PB faltante"      value={snapF.filter(r => r.PB == null).length} />
          </div>

          {/* Tabla snapshot */}
          <div>
            <h3 className="text-sm font-medium text-slate-300 mb-2">📋 Pozos — última medición (filtrados)</h3>
            <SortableTable
              cols={snapCols}
              rows={[...snapF].sort((a,b) => (a.Dias_desde_ultima||0) - (b.Dias_desde_ultima||0))}
              title="pozos_ultima_medicion"
              maxHeight="320px"
            />
          </div>

          {/* Gráficos snapshot */}
          <div>
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3">📈 Gráficos (snapshot, DIN+NIV mezclados)</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="card p-3">
                <PlotlyChart
                  title="Pozos por ORIGEN"
                  data={[{ type: "bar", x: origenCount.map(d=>d.ORIGEN), y: origenCount.map(d=>d.Pozos), marker: { color: "#38bdf8" } }]}
                  height={220}
                />
              </div>
              <div className="card p-3">
                <PlotlyChart
                  title="Antigüedad de última medición (días)"
                  data={[{ type: "bar", x: histDias.map(d=>d.x), y: histDias.map(d=>d.y), marker: { color: "#22c55e" } }]}
                  height={220}
                />
              </div>
              <div className="card p-3">
                <PlotlyChart
                  title="Distribución de Sumergencia"
                  data={[{ type: "bar", x: histSum.map(d=>d.x), y: histSum.map(d=>d.y), marker: { color: "#38bdf8" } }]}
                  height={220}
                />
              </div>
              <div className="card p-3">
                <PlotlyChart
                  title="Distribución de PB"
                  data={[{ type: "bar", x: histPB.map(d=>d.x), y: histPB.map(d=>d.y), marker: { color: "#a78bfa" } }]}
                  height={220}
                />
              </div>
            </div>
          </div>

          {/* %Estructura vs %Balance */}
          {ebData.length > 0 && (
            <div className="card p-0 overflow-hidden">
              <div className="px-4 py-2 border-b border-[#334155] text-xs text-slate-400">
                🧰 DIN-only (porque %Estructura/%Balance y Llenado suelen venir de DIN)
              </div>
              <div className="p-3 grid grid-cols-1 md:grid-cols-2 gap-4">
                <PlotlyChart
                  title="%Estructura vs %Balance (snapshot, DIN-only)"
                  data={[{
                    type: "scatter", mode: "markers",
                    x: ebData.map(d=>d.x), y: ebData.map(d=>d.y),
                    text: ebData.map(d=>d.name),
                    hovertemplate: "<b>%{text}</b><br>%Estructura: %{x}<br>%Balance: %{y}<extra></extra>",
                    marker: { color: "#f97316", opacity: 0.7, size: 7 },
                  }]}
                  layout={{ xaxis: { title: { text: "%Estructura" } }, yaxis: { title: { text: "%Balance" } } }}
                  height={280}
                />
                <SortableTable
                  cols={[
                    { key: "name",    label: "NO_key" },
                    { key: "ORIGEN",  label: "ORIGEN" },
                    { key: "x",       label: "%Estructura", render: (v:number) => v?.toFixed(2) },
                    { key: "y",       label: "%Balance",    render: (v:number) => v?.toFixed(2) },
                  ]}
                  rows={[...ebData].sort((a,b)=>(a.x??0)-(b.x??0)).map(d=>({...d, ORIGEN:"DIN"}))}
                  title="estructura_balance"
                  maxHeight="280px"
                />
              </div>
            </div>
          )}

          {/* Pozos por mes */}
          <div className="space-y-2">
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">🛢️ Pozos medidos por mes</h3>
            {ultimoMes && (
              <p className="text-sm text-slate-300">📌 Último mes ({ultimoMes}): <strong className="text-sky-400">{ultimoVal}</strong> pozos medidos</p>
            )}
            {ppm.length > 0 && (
              <div className="card p-3">
                <PlotlyChart
                  data={[{ type: "bar", x: ppm.map(d=>d.Mes), y: ppm.map(d=>d.Pozos_medidos), marker: { color: "#38bdf8" } }]}
                  height={220}
                />
              </div>
            )}
          </div>

          {/* Cobertura DIN vs NIV */}
          <div className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">✅ Cobertura DIN vs NIV</h3>
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
                  <KPICard title="Pozos en ventana" value={cobertura.total_pozos} />
                  <KPICard title="Con DIN"           value={cobertura.pozos_con_din} color="green" />
                  <KPICard title="Sin DIN"           value={cobertura.pozos_sin_din} color={cobertura.pozos_sin_din > 0 ? "red" : "default"} />
                </div>
                {cobertura.lista_sin_din.length > 0 && (
                  <details className="card text-xs">
                    <summary className="cursor-pointer text-slate-400 hover:text-slate-200">
                      Ver {cobertura.lista_sin_din.length} pozos sin DIN en la ventana
                    </summary>
                    <p className="mt-2 text-slate-400 break-words">{cobertura.lista_sin_din.join(", ")}</p>
                  </details>
                )}
              </>
            )}
          </div>

          {/* Calidad del dato */}
          <div className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">🧪 Calidad del dato</h3>
            <div className="grid grid-cols-3 gap-3">
              <KPICard title="Sumergencia < 0"  value={badSum.length} color={badSum.length > 0 ? "red" : "green"} />
              <KPICard title="PB anómalo (IQR)" value={badPB.length}  color={badPB.length > 0 ? "yellow" : "green"} />
              <KPICard title="PB faltante"       value={snapF.filter(r=>r.PB==null).length} />
            </div>
            {badSum.length > 0 && (
              <details className="card text-xs">
                <summary className="cursor-pointer text-slate-400">Pozos con Sumergencia &lt; 0</summary>
                <div className="mt-2">
                  <SortableTable
                    cols={[
                      { key: "NO_key",      label: "NO_key" },
                      { key: "ORIGEN",      label: "ORIGEN" },
                      { key: "DT_plot",     label: "DT_plot", render: (v:string) => v?.slice(0,10) },
                      { key: "PB",          label: "PB" },
                      { key: "Sumergencia", label: "Sumergencia", render: (v:number) => <span className="text-red-400">{v?.toFixed(1)}</span> },
                    ]}
                    rows={badSum}
                    title="sumergencia_negativa"
                    maxHeight="240px"
                  />
                </div>
              </details>
            )}
          </div>

          {/* Tendencias */}
          <div className="space-y-4">
            <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">📈 Pozos con tendencia en aumento</h3>
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
                <input type="number" min={2} max={20} value={minPts} onChange={(e) => setMinPts(+e.target.value)}
                  className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 w-20" />
              </div>
              <label className="flex items-center gap-2 text-sm text-slate-300">
                <input type="checkbox" checked={soloPos} onChange={(e) => setSoloPos(e.target.checked)} className="accent-sky-400" />
                Solo pendiente positiva
              </label>
            </div>

            {loadingTend && <p className="text-slate-500 text-sm animate-pulse">Calculando tendencias…</p>}

            {!loadingTend && tendencias.length > 0 && (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <SortableTable
                  cols={tendCols}
                  rows={tendencias.slice(0,100)}
                  title="tendencias"
                  maxHeight="380px"
                />
                <div className="card p-3">
                  <PlotlyChart
                    title={`Top 30 — Pendiente por mes (${trendVar})`}
                    data={[{
                      type: "bar", orientation: "h",
                      x: topTend.map(d=>d.pendiente_por_mes),
                      y: topTend.map(d=>d.NO_key),
                      marker: { color: "#f97316" },
                    }]}
                    layout={{ margin: { l: 80 }, yaxis: { autorange: "reversed" } }}
                    height={380}
                  />
                </div>
              </div>
            )}
          </div>
        </>
      )}

      {/* Semáforo AIB */}
      <div className="border-t border-[#334155] pt-8">
        <h2 className="text-lg font-bold text-slate-100 mb-4">🚦 Semáforo AIB (SE = AIB)</h2>
        <SemaforoAIB
          rows={semaforoData?.rows ?? []}
          kpis={{
            total:     semaforoData?.total    ?? 0,
            criticos:  semaforoData?.criticos ?? 0,
            alertas:   semaforoData?.alertas  ?? 0,
            normales:  semaforoData?.normales ?? 0,
            sin_datos: semaforoData?.sin_datos ?? 0,
          }}
          onRefresh={() => loadSemaforo(sumMedia, sumAlta, llenOk, llenBajo)}
          sumMedia={sumMedia} sumAlta={sumAlta} llenOk={llenOk} llenBajo={llenBajo}
          setSumMedia={setSumMedia} setSumAlta={setSumAlta} setLlenOk={setLlenOk} setLlenBajo={setLlenBajo}
        />
      </div>
    </div>
  );
}
