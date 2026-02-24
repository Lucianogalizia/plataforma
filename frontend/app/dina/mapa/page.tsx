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

  const [sumMin, setSumMin] = useState(0);
  const [sumMax, setSumMax] = useState(5000);
  const [diasMin, setDiasMin] = useState(0);
  const [diasMax, setDiasMax] = useState(9999);

  const [filtroVal, setFiltroVal] = useState<"Todos" | "Solo validadas" | "Solo no validadas">("Todos");

  const cargar = useCallback(async () => {
    setLoading(true);
    try {
      const [mapa, bats] = await Promise.all([
        api.getSnapshotMapa({
          sum_min: sumMin, sum_max: sumMax,
          dias_min: diasMin, dias_max: diasMax,
          baterias: batSel.join(","),
        }),
        api.getBaterias(),
      ]);
      let pts = mapa.puntos;

      if (filtroVal === "Solo no validadas") {
        pts = pts.filter((p) => p.Sumergencia != null && p.Sumergencia < 0);
      }

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
        <div className="flex flex-wrap gap-4">
          {/* Batería */}
          <div>
            <label className="text-xs text-slate-400 block mb-1">Batería (nivel_5)</label>
            <div className="flex flex-wrap gap-1 max-w-md">
              <button
                onClick={() => setBatSel(baterias)}
                className="text-xs px-2 py-0.5 bg-slate-700 text-slate-300 rounded hover:bg-slate-600"
              >
                Todas
              </button>
              <button
                onClick={() => setBatSel([])}
                className="text-xs px-2 py-0.5 bg-slate-700 text-slate-300 rounded hover:bg-slate-600"
              >
                Ninguna
              </button>
              {baterias.map((b) => (
                <button
                  key={b}
                  onClick={() =>
                    setBatSel((prev) =>
                      prev.includes(b) ? prev.filter((x) => x !== b) : [...prev, b]
                    )
                  }
                  className={`text-xs px-2 py-0.5 rounded border transition-colors ${
                    batSel.includes(b)
                      ? "bg-sky-500/10 border-sky-500/30 text-sky-300"
                      : "bg-slate-800 border-slate-600 text-slate-500"
                  }`}
                >
                  {b}
                </button>
              ))}
            </div>
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
                  onClick={() => setFiltroVal(op)}
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
