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
      const mapa = await api.getSnapshotMapa({
        sum_min: sumMin, sum_max: sumMax,
        dias_min: diasMin, dias_max: diasMax,
        baterias: batSel.join(","),
      });

      let pts = mapa.puntos;

      if (filtroVal === "Solo no validadas") {
        pts = pts.filter((p) => p.Sumergencia != null && p.Sumergencia < 0);
      }

      setPuntos(pts);
    } catch {
      setPuntos([]);
    }
    setLoading(false);
  }, [sumMin, sumMax, diasMin, diasMax, batSel, filtroVal]);

  useEffect(() => {
    api.getBaterias()
      .then(async (r) => {
        const nombres = (r.baterias || []).map((b: { nombre: string } | string) =>
          typeof b === "string" ? b : b.nombre
        );
        setBaterias(nombres);
        setBatSel(nombres);

        // primer carga usando la selección "todas" sin esperar al re-render
        setLoading(true);
        try {
          const mapa = await api.getSnapshotMapa({
            sum_min: sumMin, sum_max: sumMax,
            dias_min: diasMin, dias_max: diasMax,
            baterias: nombres.join(","),
          });

          let pts = mapa.puntos;
          setPuntos(pts);
        } catch {
          setPuntos([]);
        }
        setLoading(false);
      })
      .catch(() => {});
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
              <div className="absolute z-50 mt-2 w-[260px] bg-[#0b1220] border border-[#334155] rounded shadow-lg p-2">
                <input
                  value={batSearch}
                  onChange={(e) => setBatSearch(e.target.value)}
                  placeholder="Buscar..."
                  className="w-full bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200 mb-2"
                />
                <div className="max-h-64 overflow-auto space-y-1">
                  <button
                    onClick={() => setBatSel(baterias)}
                    className="w-full text-left text-xs text-slate-300 hover:text-sky-400"
                  >
                    Seleccionar todas
                  </button>
                  <button
                    onClick={() => setBatSel([])}
                    className="w-full text-left text-xs text-slate-300 hover:text-sky-400"
                  >
                    Limpiar selección
                  </button>

                  <div className="h-px bg-[#334155] my-2" />

                  {batFiltradas.map((b) => (
                    <label key={b} className="flex items-center gap-2 text-sm text-slate-200">
                      <input
                        type="checkbox"
                        checked={batSel.includes(b)}
                        onChange={() => toggleBat(b)}
                      />
                      <span className="truncate">{b}</span>
                    </label>
                  ))}
                </div>

                <div className="flex justify-end mt-2">
                  <button
                    onClick={() => setBatOpen(false)}
                    className="text-xs text-slate-300 hover:text-sky-400"
                  >
                    Cerrar
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Sumergencia */}
          <div>
            <label className="text-xs text-slate-400 block mb-1">Sumergencia (m)</label>
            <div className="flex gap-2">
              <input
                type="number"
                value={sumMin}
                onChange={(e) => setSumMin(Number(e.target.value))}
                className="w-28 bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200"
              />
              <input
                type="number"
                value={sumMax}
                onChange={(e) => setSumMax(Number(e.target.value))}
                className="w-28 bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200"
              />
            </div>
          </div>

          {/* Días */}
          <div>
            <label className="text-xs text-slate-400 block mb-1">Días desde última</label>
            <div className="flex gap-2">
              <input
                type="number"
                value={diasMin}
                onChange={(e) => setDiasMin(Number(e.target.value))}
                className="w-28 bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200"
              />
              <input
                type="number"
                value={diasMax}
                onChange={(e) => setDiasMax(Number(e.target.value))}
                className="w-28 bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200"
              />
            </div>
          </div>

          {/* Validaciones */}
          <div>
            <label className="text-xs text-slate-400 block mb-1">Validaciones</label>
            <select
              value={filtroVal}
              onChange={(e) => setFiltroVal(e.target.value as any)}
              className="bg-[#0f172a] border border-[#334155] rounded px-2 py-1 text-sm text-slate-200"
            >
              <option>Todos</option>
              <option>Solo validadas</option>
              <option>Solo no validadas</option>
            </select>
          </div>

          <div className="flex items-end gap-2">
            <button
              onClick={cargar}
              className="bg-sky-600 hover:bg-sky-500 text-white text-sm font-semibold px-4 py-2 rounded"
            >
              Aplicar
            </button>
          </div>
        </div>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KPICard title="Puntos con coords" value={String(puntosConCoords.length)} />
        <KPICard title="Min Sumergencia" value={sumValues.length ? String(Math.min(...sumValues)) : "-"} />
        <KPICard title="Max Sumergencia" value={sumValues.length ? String(Math.max(...sumValues)) : "-"} />
        <KPICard title="Selección baterías" value={`${batSel.length}/${baterias.length}`} />
      </div>

      {/* Mapa */}
      <div className="card">
        <MapaSumergencia puntos={puntosConCoords} loading={loading} />
      </div>

      {/* Tabla */}
      <div className="card">
        <TablaValidaciones />
      </div>
    </div>
  );
}
