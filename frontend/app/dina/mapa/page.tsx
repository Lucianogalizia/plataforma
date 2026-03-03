"use client";

import { useEffect, useState, useCallback } from "react";
import api, { PuntoMapa } from "@/lib/api";
import MapaSumergencia from "@/components/MapaSumergencia";
import TablaValidaciones from "@/components/TablaValidaciones";
import KPICard from "@/components/KPICard";

export default function MapaPage() {
  const [puntos, setPuntos] = useState<PuntoMapa[]>([]);
  const [loading, setLoading] = useState(true);
  const [baterias, setBaterias] = useState<string[]>([]);
  const [batSel, setBatSel] = useState<string[]>([]);
  const [batSearch, setBatSearch] = useState("");
  const [batOpen, setBatOpen] = useState(false);

  const [sumMin, setSumMin] = useState(0);
  const [sumMax, setSumMax] = useState(5000);
  const [diasMin, setDiasMin] = useState(0);
  const [diasMax, setDiasMax] = useState(9999);

  const [filtroVal, setFiltroVal] = useState<"Todos" | "Solo validadas" | "Solo no validadas">("Todos");

  const cargar = useCallback(async () => {
    setLoading(true);
    try {
      // solo_validadas: null=todos, true=solo validadas, false=solo no validadas
      const soloVal =
        filtroVal === "Solo validadas"    ? true  :
        filtroVal === "Solo no validadas" ? false : null;

      const [mapa, bats] = await Promise.all([
        api.getSnapshotMapa({
          sum_min: sumMin, sum_max: sumMax,
          dias_min: diasMin, dias_max: diasMax,
          baterias: batSel.join(","),
          solo_validadas: soloVal,
        }),
        api.getBaterias(),
      ]);
      let pts = mapa.puntos;

      setPuntos(pts);
      if (bats.baterias && batSel.length === 0) {
        const nombres = bats.baterias.map((b: { nombre: string } | string) =>
          typeof b === "string" ? b : b.nombre
        );
        setBaterias(nombres);
        setBatSel(nombres);
      }
    } catch {
      setPuntos([]);
    }
    setLoading(false);
  }, [sumMin, sumMax, diasMin, diasMax, batSel, filtroVal]);

  useEffect(() => {
    api.getBaterias()
      .then((r) => {
        const nombres = (r.baterias || []).map((b: { nombre: string } | string) =>
          typeof b === "string" ? b : b.nombre
        );
        setBaterias(nombres);
        setBatSel(nombres);
      })
      .catch(() => {})
      .finally(() => cargar());
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const puntosConCoords = puntos.filter((p) => p.lat != null && p.lon != null);
  const sumValues = puntosConCoords.map((p) => p.Sumergencia).filter((v) => v != null) as number[];

  const batFiltradas = baterias.filter((b) =>
    b.toLowerCase().includes(batSearch.toLowerCase())
  );

  const toggleBat = (b: string) =>
    setBatSel((prev) => prev.includes(b) ? prev.filter((x) => x !== b) : [...prev, b]);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-bold text-slate-100">
          🗺️ Mapa de Sumergencia — heatmap densidad
        </h2>
        <p className="text-slate-400 text-sm mt-1">Última medición por pozo</p>
      </div>

      {/* Filtros */}
      <div className="card space-y-4">
        <h3 className="text-sm font-semibold text-slate-300">Filtros</h3>
        <div className="flex flex-wrap gap-6 items-start">

          {/* Batería — picklist */}
          <div className="relative">
            <label className="text-xs text-slate-400 block mb-1">Batería (nivel_5)</label>
            <button
              onClick={() => setBatOpen((o) => !o)}
              className="flex items-center gap-2 bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 min-w-[200px] hover:border-sky-500 transition-colors"
            >
              <span className="flex-1 text-left truncate">
                {batSel.length === baterias.length
                  ? "Todas las baterías"
                  : batSel.length === 0
                  ? "Ninguna"
                  : `${batSel.length} seleccionadas`}
              </span>
              <span className="text-slate-500 text-xs">{batOpen ? "▲" : "▼"}</span>
            </button>

            {batOpen && (
              <div className="absolute z-50 mt-1 w-64 bg-[#1e293b] border border-[#334155] rounded shadow-xl">
                {/* Buscador */}
                <div className="p-2 border-b border-[#334155]">
                  <input
                    autoFocus
                    type="text"
                    placeholder="Buscar batería…"
                    value={batSearch}
                    onChange={(e) => setBatSearch(e.target.value)}
                    className="w-full bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-xs text-slate-200 placeholder-slate-600"
                  />
                </div>
                {/* Acciones rápidas */}
                <div className="flex gap-1 p-2 border-b border-[#334155]">
                  <button
                    onClick={() => setBatSel(baterias)}
                    className="text-xs px-2 py-0.5 bg-slate-700 text-slate-300 rounded hover:bg-slate-600 flex-1"
                  >
                    Todas
                  </button>
                  <button
                    onClick={() => setBatSel([])}
                    className="text-xs px-2 py-0.5 bg-slate-700 text-slate-300 rounded hover:bg-slate-600 flex-1"
                  >
                    Ninguna
                  </button>
                </div>
                {/* Lista */}
                <div className="overflow-y-auto max-h-52">
                  {batFiltradas.map((b) => (
                    <label
                      key={b}
                      className="flex items-center gap-2 px-3 py-1.5 hover:bg-slate-700/50 cursor-pointer text-xs text-slate-300"
                    >
                      <input
                        type="checkbox"
                        checked={batSel.includes(b)}
                        onChange={() => toggleBat(b)}
                        className="accent-sky-400 w-3 h-3"
                      />
                      {b}
                    </label>
                  ))}
                </div>
                <div className="p-2 border-t border-[#334155]">
                  <button
                    onClick={() => setBatOpen(false)}
                    className="w-full text-xs px-2 py-1 bg-sky-600 hover:bg-sky-500 text-white rounded"
                  >
                    Cerrar
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Rango Sumergencia */}
          <div>
            <label className="text-xs text-slate-400 block mb-1">
              Rango Sumergencia: {sumMin} – {sumMax}
            </label>
            <div className="flex gap-2">
              <input
                type="number" value={sumMin}
                onChange={(e) => setSumMin(+e.target.value)}
                className="bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200 w-24"
                placeholder="Min"
              />
              <input
                type="number" value={sumMax}
                onChange={(e) => setSumMax(+e.target.value)}
                className="bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200 w-24"
                placeholder="Max"
              />
            </div>
          </div>

          {/* Rango Días */}
          <div>
            <label className="text-xs text-slate-400 block mb-1">
              Días desde última: {diasMin} – {diasMax}
            </label>
            <div className="flex gap-2">
              <input
                type="number" value={diasMin}
                onChange={(e) => setDiasMin(+e.target.value)}
                className="bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200 w-24"
              />
              <input
                type="number" value={diasMax}
                onChange={(e) => setDiasMax(+e.target.value)}
                className="bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200 w-24"
              />
            </div>
          </div>

          {/* Filtro validación */}
          <div>
            <label className="text-xs text-slate-400 block mb-1">Filtrar por validación</label>
            <div className="flex gap-2">
              {(["Todos", "Solo validadas", "Solo no validadas"] as const).map((op) => (
                <button
                  key={op}
                  onClick={() => { setFiltroVal(op); }}
                  className={`text-xs px-2 py-1 rounded border transition-colors ${
                    filtroVal === op
                      ? "bg-sky-500/10 border-sky-400 text-sky-300"
                      : "border-slate-600 text-slate-400 hover:border-slate-400"
                  }`}
                >
                  {op}
                </button>
              ))}
            </div>
          </div>
        </div>

        <button
          onClick={cargar}
          disabled={loading}
          className="px-4 py-2 bg-sky-600 hover:bg-sky-500 disabled:opacity-40 rounded text-sm font-medium text-white transition-colors"
        >
          {loading ? "Cargando…" : "🔄 Aplicar filtros"}
        </button>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-3 gap-3">
        <KPICard title="Pozos con coords" value={puntosConCoords.length} color="sky" />
        <KPICard
          title="Sumer. máx"
          value={sumValues.length ? Math.max(...sumValues).toFixed(0) + " m" : "—"}
          color="red"
        />
        <KPICard
          title="Sumer. media"
          value={
            sumValues.length
              ? (sumValues.reduce((a, b) => a + b, 0) / sumValues.length).toFixed(0) + " m"
              : "—"
          }
          color="sky"
        />
      </div>

      {loading && (
        <p className="text-slate-500 text-sm animate-pulse">Cargando mapa…</p>
      )}

      {!loading && puntosConCoords.length > 0 && (
        <div className="card p-0 overflow-hidden">
          <div className="px-4 py-3 border-b border-[#334155]">
            <h3 className="text-sm font-medium text-slate-300">
              Heatmap de Sumergencia — {puntosConCoords.length} pozos
            </h3>
          </div>
          <div className="p-2">
            <MapaSumergencia puntos={puntosConCoords} height={520} />
          </div>
        </div>
      )}

      {!loading && puntosConCoords.length === 0 && (
        <div className="card text-center text-slate-500 text-sm py-8">
          No hay pozos con coordenadas para los filtros seleccionados.
        </div>
      )}

      {/* Tabla — recibe puntos ya filtrados */}
      {puntos.length > 0 && (
        <div className="space-y-4">
          <div className="border-t border-[#334155] pt-6">
            <h3 className="text-base font-semibold text-slate-200 mb-4">
              📋 Pozos filtrados — selección, validación y exportación
            </h3>
            <TablaValidaciones pozos={puntos} />
          </div>
        </div>
      )}
    </div>
  );
}
