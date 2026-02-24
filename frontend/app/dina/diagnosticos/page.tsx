"use client";

import { useEffect, useState, useCallback } from "react";
import api, { Diagnostico, FilaDiagGlobal } from "@/lib/api";
import KPICard from "@/components/KPICard";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

const SEV_EMOJI: Record<string, string> = {
  BAJA: "🟢", MEDIA: "🟡", ALTA: "🟠", "CRÍTICA": "🔴",
};
const SEV_COLOR: Record<string, string> = {
  BAJA: "text-green-400", MEDIA: "text-yellow-400", ALTA: "text-orange-400", "CRÍTICA": "text-red-400",
};
const ESTADO_EMOJI: Record<string, string> = { ACTIVA: "⚠️", RESUELTA: "✅" };

function DiagCard({ diag, pozo }: { diag: Diagnostico; pozo: string }) {
  if (diag.error) {
    return (
      <div className="card border-red-500/30 text-red-400 text-sm">
        Error: {diag.error}
      </div>
    );
  }

  const meds = diag.mediciones || [];
  const totalActivas = meds.reduce(
    (a, m) => a + m["problemáticas"].filter((p) => p.estado === "ACTIVA").length, 0
  );
  const totalResueltas = meds.reduce(
    (a, m) => a + m["problemáticas"].filter((p) => p.estado === "RESUELTA").length, 0
  );

  return (
    <div className="space-y-4">
      {/* KPIs */}
      <div className="grid grid-cols-3 md:grid-cols-5 gap-3">
        <KPICard title="Batería"          value={diag._meta ? "—" : "—"} />
        <KPICard title="DINs analizados"  value={diag._meta?.n_mediciones || meds.length} />
        <KPICard title="Confianza"        value={diag.confianza || "—"} />
        <KPICard title="⚠️ Activas"       value={totalActivas}   color={totalActivas > 0 ? "red" : "green"} />
        <KPICard title="✅ Resueltas"     value={totalResueltas} color="green" />
      </div>

      {/* Resumen */}
      <div className="card border-sky-500/20">
        <h4 className="text-xs text-slate-400 uppercase tracking-wide mb-2">📝 Resumen ejecutivo</h4>
        <p className="text-sm text-slate-300 leading-relaxed">{diag.resumen}</p>
      </div>

      <div className="card">
        <h4 className="text-xs text-slate-400 uppercase tracking-wide mb-1">
          🔒 Variables operativas sin cambio
        </h4>
        <p className="text-xs text-slate-500">{diag.variables_sin_cambio || "N/D"}</p>
      </div>

      {/* Mediciones */}
      <div>
        <h4 className="text-sm font-semibold text-slate-300 mb-3">📋 Detalle por medición</h4>
        {meds.map((med, i) => {
          const isRecent = med.label === "Más reciente" || med.label === "Única medición";
          return (
            <details
              key={i}
              open={isRecent}
              className="card mb-3 group"
            >
              <summary className="cursor-pointer flex items-center justify-between text-sm font-medium text-slate-200 hover:text-white list-none">
                <span>📅 {med.fecha} — {med.label}</span>
                <span className="text-xs text-slate-500 group-open:rotate-90 transition-transform">▶</span>
              </summary>

              <div className="mt-4 space-y-4">
                {/* Variables clave */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                  <KPICard
                    title="Llenado bomba"
                    value={med.llenado_pct != null ? `${med.llenado_pct}%` : "N/D"}
                  />
                  <KPICard
                    title="Sumergencia"
                    value={med.sumergencia_m != null ? `${med.sumergencia_m} m` : "N/D"}
                    subtitle={med.sumergencia_nivel}
                    color={
                      med.sumergencia_nivel === "CRÍTICA" ? "red" :
                      med.sumergencia_nivel === "BAJA"    ? "yellow" : "default"
                    }
                  />
                  <KPICard
                    title="Caudal bruto"
                    value={med.caudal_bruto != null ? `${med.caudal_bruto} m³/d` : "N/D"}
                  />
                  <KPICard
                    title="%Balance"
                    value={med.pct_balance != null ? `${med.pct_balance}%` : "N/D"}
                  />
                </div>

                {/* Problemáticas */}
                {med["problemáticas"].length === 0 ? (
                  <div className="text-sm text-green-400">✅ Sin problemáticas en esta medición.</div>
                ) : (
                  <div className="space-y-2">
                    {[...med["problemáticas"]]
                      .sort((a, b) => {
                        const ea = a.estado === "ACTIVA" ? 0 : 1;
                        const eb = b.estado === "ACTIVA" ? 0 : 1;
                        const sa = ["CRÍTICA","ALTA","MEDIA","BAJA"].indexOf(a.severidad);
                        const sb = ["CRÍTICA","ALTA","MEDIA","BAJA"].indexOf(b.severidad);
                        return ea - eb || sa - sb;
                      })
                      .map((p, j) => (
                        <div key={j} className="bg-slate-800/50 rounded-lg p-3 border border-slate-700/50">
                          <div className="flex items-center gap-2 mb-1">
                            <span>{ESTADO_EMOJI[p.estado]}</span>
                            <span>{SEV_EMOJI[p.severidad]}</span>
                            <span className="text-sm font-semibold text-slate-200">{p.nombre}</span>
                            <span className={`ml-auto text-xs font-bold ${SEV_COLOR[p.severidad]}`}>
                              {p.severidad}
                            </span>
                            <span className={`text-xs ${p.estado === "ACTIVA" ? "text-red-400" : "text-green-400"}`}>
                              {p.estado}
                            </span>
                          </div>
                          <p className="text-xs text-slate-400 leading-relaxed">{p.descripcion}</p>
                        </div>
                      ))}
                  </div>
                )}
              </div>
            </details>
          );
        })}
      </div>

      {/* Recomendación */}
      <div className="card border-green-500/20">
        <h4 className="text-xs text-slate-400 uppercase tracking-wide mb-2">💡 Recomendación</h4>
        <p className="text-sm text-green-300">{diag.recomendacion}</p>
      </div>

      {/* Meta */}
      {diag._meta && (
        <p className="text-xs text-slate-600">
          Generado: {diag._meta.generado_utc?.slice(0,19).replace("T"," ")} UTC |
          DIN más reciente: {diag._meta.fecha_din_mas_reciente}
        </p>
      )}
    </div>
  );
}

export default function DiagnosticosPage() {
  const [pozo, setPozo] = useState("");
  const [diag, setDiag] = useState<Diagnostico | null>(null);
  const [loadingDiag, setLoadingDiag] = useState(false);
  const [generando, setGenerando] = useState(false);

  // Tabla global
  const [tablaGlobal, setTablaGlobal] = useState<FilaDiagGlobal[]>([]);
  const [loadingTabla, setLoadingTabla] = useState(false);
  const [estadoCache, setEstadoCache] = useState<{ total_pozos_con_din: number; con_diagnostico: number; pendientes: number } | null>(null);

  // Generación en lote
  const [soloPend, setSoloPend] = useState(true);
  const [generandoLote, setGenerandoLote] = useState(false);
  const [resLote, setResLote] = useState<{ ok: string[]; error: unknown[]; salteados: string[] } | null>(null);

  // Filtros tabla global
  const [filtroSev, setFiltroSev] = useState<string[]>(["CRÍTICA","ALTA","MEDIA","BAJA","RESUELTA","NINGUNA"]);
  const [filtroProb, setFiltroProb] = useState("");

  const cargarDiag = useCallback(async (p: string) => {
    if (!p) return;
    setLoadingDiag(true);
    try {
      const res = await api.getDiagnostico(p);
      setDiag(res);
    } catch {
      setDiag(null);
    }
    setLoadingDiag(false);
  }, []);

  const cargarTabla = useCallback(async () => {
    setLoadingTabla(true);
    try {
      const [tab, cache] = await Promise.all([
        api.getTablaGlobalDiag(),
        api.getEstadoCache(),
      ]);
      setTablaGlobal(tab.rows);
      setEstadoCache(cache);
    } catch {}
    setLoadingTabla(false);
  }, []);

  useEffect(() => {
    const p = sessionStorage.getItem("dina_pozo_sel") || "";
    setPozo(p);
    cargarDiag(p);
    cargarTabla();

    const handler = (e: Event) => {
      const p2 = (e as CustomEvent<string>).detail;
      setPozo(p2);
      cargarDiag(p2);
    };
    window.addEventListener("dina:pozo", handler);
    return () => window.removeEventListener("dina:pozo", handler);
  }, [cargarDiag, cargarTabla]);

  const handleGenerar = async () => {
    setGenerando(true);
    try {
      const res = await api.generarDiagnostico(pozo);
      setDiag(res);
    } catch {}
    setGenerando(false);
  };

  const handleGenerarLote = async () => {
    setGenerandoLote(true);
    try {
      const res = await api.generarTodos({ solo_pendientes: soloPend });
      setResLote(res);
      await cargarTabla();
    } catch {}
    setGenerandoLote(false);
  };

  // KPIs tabla
  const pozosUnicos = new Set(tablaGlobal.map((r) => r.Pozo)).size;
  const criticos    = new Set(tablaGlobal.filter((r) => r["Sev. máx"] === "CRÍTICA").map((r) => r.Pozo)).size;
  const altos       = new Set(tablaGlobal.filter((r) => r["Sev. máx"] === "ALTA").map((r) => r.Pozo)).size;
  const sinProb     = new Set(tablaGlobal.filter((r) => r["Sev. máx"] === "NINGUNA").map((r) => r.Pozo)).size;

  // Frecuencia problemáticas
  const probFreq: Record<string,number> = {};
  tablaGlobal.forEach((r) => {
    r.Problemáticas.split("\n").forEach((line) => {
      const match = line.match(/\S.+? \[/);
      if (match) {
        const nombre = line.replace(/^[^\w]+/, "").split("[")[0].trim();
        if (nombre) probFreq[nombre] = (probFreq[nombre] || 0) + 1;
      }
    });
  });
  const freqData = Object.entries(probFreq)
    .sort((a,b)=>a[1]-b[1])
    .map(([p, c]) => ({ p, c }));

  // Filtro tabla
  const sevOpts = ["CRÍTICA","ALTA","MEDIA","BAJA","RESUELTA","NINGUNA"];
  const tablaF = tablaGlobal.filter((r) => {
    if (!filtroSev.includes(r["Sev. máx"])) return false;
    if (filtroProb && !r.Problemáticas.toLowerCase().includes(filtroProb.toLowerCase())) return false;
    return true;
  });

  // Export CSV
  const exportCSV = () => {
    const keys = Object.keys(tablaF[0] || {}).filter((k) => k !== "Problemáticas");
    const header = [...keys, "Problemáticas"].join(",");
    const body = tablaF.map((r) =>
      [...keys, "Problemáticas"].map((k) => JSON.stringify((r as unknown as Record<string,unknown>)[k] ?? "")).join(","))
    );
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([[header, ...body].join("\n")], { type: "text/csv" }));
    a.download = "diagnosticos_mediciones.csv";
    a.click();
  };

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-xl font-bold text-slate-100">🤖 Diagnósticos IA</h2>
        <p className="text-slate-400 text-sm">Análisis de cartas dinamométricas con inteligencia artificial</p>
      </div>

      {/* Generación en lote */}
      <details className="card">
        <summary className="cursor-pointer text-sm font-semibold text-slate-300 list-none flex items-center justify-between">
          ⚙️ Generación en lote — todos los pozos
          <span className="text-xs text-slate-500">▼</span>
        </summary>
        <div className="mt-4 space-y-3">
          {estadoCache && (
            <div className="grid grid-cols-3 gap-3">
              <KPICard title="Total pozos DIN"    value={estadoCache.total_pozos_con_din} />
              <KPICard title="✅ Con diagnóstico" value={estadoCache.con_diagnostico} color="green" />
              <KPICard title="⏳ Pendientes"       value={estadoCache.pendientes} color="yellow" />
            </div>
          )}
          <div className="flex gap-4 items-center flex-wrap">
            <label className="flex items-center gap-2 text-sm text-slate-300 cursor-pointer">
              <input
                type="checkbox"
                checked={soloPend}
                onChange={(e) => setSoloPend(e.target.checked)}
                className="accent-sky-400"
              />
              Saltear pozos ya actualizados
            </label>
            <button
              onClick={handleGenerarLote}
              disabled={generandoLote}
              className="px-4 py-2 bg-sky-600 hover:bg-sky-500 disabled:opacity-40 rounded text-sm font-medium text-white transition-colors"
            >
              {generandoLote ? "⏳ Generando…" : "🚀 Generar todos los diagnósticos"}
            </button>
          </div>
          {resLote && (
            <div className="text-xs text-slate-400 space-y-1">
              <p>✅ Generados: {resLote.ok.length}</p>
              <p>❌ Con error: {resLote.error.length}</p>
              <p>⏭️ Salteados: {resLote.salteados.length}</p>
            </div>
          )}
        </div>
      </details>

      {/* Diagnóstico individual */}
      <div>
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-base font-semibold text-slate-200">
            🔍 Diagnóstico individual — Pozo: <span className="text-sky-400">{pozo || "…"}</span>
          </h3>
          <button
            onClick={handleGenerar}
            disabled={!pozo || generando}
            className="px-3 py-1.5 bg-sky-600 hover:bg-sky-500 disabled:opacity-40 rounded text-xs font-medium text-white transition-colors"
          >
            {generando ? "⏳ Generando…" : "🔄 Regenerar"}
          </button>
        </div>

        {loadingDiag && (
          <p className="text-slate-500 text-sm animate-pulse">Verificando caché y generando diagnóstico…</p>
        )}

        {!loadingDiag && diag && (
          <DiagCard diag={diag} pozo={pozo} />
        )}

        {!loadingDiag && !diag && pozo && (
          <div className="card text-slate-500 text-sm text-center py-8">
            No hay diagnóstico en caché para <strong>{pozo}</strong>.
            Presioná "Regenerar" para generar uno nuevo.
          </div>
        )}
      </div>

      {/* Tabla global */}
      <div className="border-t border-[#334155] pt-6 space-y-4">
        <h3 className="text-base font-semibold text-slate-200">
          📋 Tabla global — una fila por medición
        </h3>

        {/* KPIs */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <KPICard title="Pozos diagnos."   value={pozosUnicos} color="sky" />
          <KPICard title="Mediciones total" value={tablaGlobal.length} />
          <KPICard title="🔴 CRÍTICOS"      value={criticos}    color="red" />
          <KPICard title="🟠 ALTA sev."     value={altos}       color="orange" />
          <KPICard title="🟢 Sin problemas" value={sinProb}     color="green" />
        </div>

        {/* Filtros */}
        <div className="flex flex-wrap gap-4 items-end">
          <div>
            <label className="text-xs text-slate-400 block mb-1">Severidad máxima</label>
            <div className="flex gap-1 flex-wrap">
              {sevOpts.map((s) => (
                <button
                  key={s}
                  onClick={() =>
                    setFiltroSev((prev) =>
                      prev.includes(s) ? prev.filter((x) => x !== s) : [...prev, s]
                    )
                  }
                  className={`text-xs px-2 py-1 rounded border transition-colors ${
                    filtroSev.includes(s)
                      ? "bg-sky-500/10 border-sky-500/30 text-sky-300"
                      : "border-slate-600 text-slate-500"
                  }`}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label className="text-xs text-slate-400 block mb-1">Buscar problemática</label>
            <input
              type="text"
              placeholder="ej: Llenado bajo"
              value={filtroProb}
              onChange={(e) => setFiltroProb(e.target.value)}
              className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 w-48"
            />
          </div>
        </div>

        {loadingTabla && (
          <p className="text-slate-500 text-sm animate-pulse">Cargando tabla global…</p>
        )}

        {/* Tabla */}
        {!loadingTabla && tablaF.length > 0 && (
          <>
            <p className="text-xs text-slate-500">
              Mostrando {tablaF.length} mediciones ({new Set(tablaF.map((r) => r.Pozo)).size} pozos)
            </p>
            <div className="card p-0 overflow-hidden">
              <div className="overflow-x-auto max-h-[520px] overflow-y-auto">
                <table className="text-xs w-full">
                  <thead className="sticky top-0 z-10">
                    <tr>
                      {["Pozo","Batería","Fecha DIN","Medición","Llenado %","Sumergencia","Caudal m³/d","%Balance","Sev. máx","Act.","Res.","Problemáticas","Recomendación"].map((h)=>(
                        <th key={h} className="bg-[#1e293b] border-b border-[#334155] px-3 py-2 whitespace-nowrap text-left">
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {tablaF.map((r, i) => (
                      <tr key={i} className="border-b border-[#334155] hover:bg-slate-800/40">
                        <td className="px-3 py-2 font-mono text-slate-300 whitespace-nowrap">{r.Pozo}</td>
                        <td className="px-3 py-2 text-slate-400">{r["Batería"]}</td>
                        <td className="px-3 py-2 text-slate-400 whitespace-nowrap">{r["Fecha DIN"]}</td>
                        <td className="px-3 py-2 text-slate-400 text-xs whitespace-nowrap">{r.Medición}</td>
                        <td className="px-3 py-2 text-slate-400">{r["Llenado %"]}</td>
                        <td className="px-3 py-2 text-sky-300">{r.Sumergencia}</td>
                        <td className="px-3 py-2 text-slate-400">{r["Caudal m³/d"]}</td>
                        <td className="px-3 py-2 text-slate-400">{r["%Balance"]}</td>
                        <td className="px-3 py-2">
                          <span className={`text-xs font-bold ${SEV_COLOR[r["Sev. máx"]] || "text-slate-400"}`}>
                            {SEV_EMOJI[r["Sev. máx"]] || ""} {r["Sev. máx"]}
                          </span>
                        </td>
                        <td className="px-3 py-2 text-center text-red-400">{r["Act."]}</td>
                        <td className="px-3 py-2 text-center text-green-400">{r["Res."]}</td>
                        <td className="px-3 py-2 text-slate-400 max-w-xs">
                          <pre className="whitespace-pre-wrap text-xs font-sans leading-relaxed">
                            {r.Problemáticas}
                          </pre>
                        </td>
                        <td className="px-3 py-2 text-slate-400 max-w-xs text-xs">{r.Recomendación}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Gráficos */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Severidad */}
              {(() => {
                const ultimos = Object.values(
                  tablaF.reduce((acc, r) => {
                    if (!acc[r.Pozo] || acc[r.Pozo]["Fecha DIN"] < r["Fecha DIN"]) {
                      acc[r.Pozo] = r;
                    }
                    return acc;
                  }, {} as Record<string, FilaDiagGlobal>)
                );
                const sevCount: Record<string,number> = {};
                ultimos.forEach((r) => {
                  sevCount[r["Sev. máx"]] = (sevCount[r["Sev. máx"]] || 0) + 1;
                });
                const data = ["CRÍTICA","ALTA","MEDIA","BAJA","RESUELTA","NINGUNA"]
                  .map((s) => ({ Severidad: s, Pozos: sevCount[s] || 0 }))
                  .filter((d) => d.Pozos > 0);
                const colorMap: Record<string,string> = {
                  CRÍTICA: "#ef4444", ALTA: "#f97316", MEDIA: "#eab308",
                  BAJA: "#22c55e", RESUELTA: "#64748b", NINGUNA: "#94a3b8"
                };
                return (
                  <div className="card p-0 overflow-hidden">
                    <div className="px-4 py-2 border-b border-[#334155] text-xs text-slate-400">
                      Pozos por severidad (última medición)
                    </div>
                    <div className="p-3">
                      <ResponsiveContainer width="100%" height={200}>
                        <BarChart data={data}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                          <XAxis dataKey="Severidad" tick={{ fill: "#94a3b8", fontSize: 10 }} stroke="#64748b" />
                          <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} stroke="#64748b" />
                          <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155" }} />
                          <Bar dataKey="Pozos" fill="#38bdf8" radius={[4,4,0,0]}
                            label={false}
                          />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                );
              })()}

              {/* Frecuencia problemáticas */}
              {freqData.length > 0 && (
                <div className="card p-0 overflow-hidden">
                  <div className="px-4 py-2 border-b border-[#334155] text-xs text-slate-400">
                    Frecuencia de problemáticas
                  </div>
                  <div className="p-3">
                    <ResponsiveContainer width="100%" height={Math.max(200, freqData.length * 22)}>
                      <BarChart data={freqData} layout="vertical" margin={{ left: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                        <XAxis type="number" tick={{ fill: "#94a3b8", fontSize: 10 }} stroke="#64748b" />
                        <YAxis type="category" dataKey="p" tick={{ fill: "#94a3b8", fontSize: 9 }} stroke="#64748b" width={130} />
                        <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155" }} />
                        <Bar dataKey="c" fill="#f97316" radius={[0,3,3,0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )}
            </div>

            {/* Export */}
            <div className="flex gap-3">
              <button
                onClick={exportCSV}
                className="text-xs px-3 py-1.5 border border-slate-600 rounded text-slate-400 hover:border-sky-400 hover:text-sky-400 transition-colors"
              >
                ⬇️ Descargar CSV
              </button>
            </div>
          </>
        )}

        {!loadingTabla && tablaGlobal.length === 0 && (
          <div className="card text-center text-slate-500 text-sm py-8">
            Todavía no hay diagnósticos en GCS. Usá el panel ⚙️ de arriba para generarlos.
          </div>
        )}
      </div>
    </div>
  );
}
