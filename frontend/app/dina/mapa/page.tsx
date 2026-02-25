"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import api, { PuntoMapa } from "@/lib/api";
import MapaSumergencia from "@/components/MapaSumergencia";
import TablaValidaciones from "@/components/TablaValidaciones";
import KPICard from "@/components/KPICard";
import { useQuery } from "@tanstack/react-query";

type FiltroVal = "Todos" | "Solo validadas" | "Solo no validadas";

export default function MapaPage() {
  const [batSel, setBatSel] = useState<string[]>([]);
  const [batSearch, setBatSearch] = useState("");
  const [batOpen, setBatOpen] = useState(false);

  const [sumMin, setSumMin] = useState(0);
  const [sumMax, setSumMax] = useState(5000);
  const [diasMin, setDiasMin] = useState(0);
  const [diasMax, setDiasMax] = useState(9999);

  const [filtroVal, setFiltroVal] = useState<FiltroVal>("Todos");

  // 1) Baterías (cacheado)
  const bateriasQ = useQuery({
    queryKey: ["mapa", "baterias"],
    queryFn: () => api.getBaterias(),
    staleTime: 10 * 60_000, // baterías rara vez cambian
  });

  const baterias = useMemo(() => {
    const raw = bateriasQ.data?.baterias ?? [];
    return raw.map((b: any) => (typeof b === "string" ? b : b.nombre)).filter(Boolean);
  }, [bateriasQ.data]);

  // Seleccionar todas por defecto solo 1 vez
  const initedRef = useRef(false);
  useEffect(() => {
    if (initedRef.current) return;
    if (!baterias.length) return;
    setBatSel(baterias);
    initedRef.current = true;
  }, [baterias]);

  // 2) Puntos del mapa (cacheado por filtros)
  const puntosQ = useQuery({
    queryKey: [
      "mapa",
      "puntos",
      { sumMin, sumMax, diasMin, diasMax, baterias: batSel.join(","), filtroVal }
    ],
    queryFn: async () => {
      const solo_validadas =
        filtroVal === "Todos" ? undefined : filtroVal === "Solo validadas";

      const res = await api.getSnapshotMapa({
        sum_min: sumMin,
        sum_max: sumMax,
        dias_min: diasMin,
        dias_max: diasMax,
        baterias: batSel.join(","),
        solo_validadas,
      });

      return (res.puntos ?? []) as PuntoMapa[];
    },
    enabled: batSel.length > 0, // no pedir hasta que haya selección
    keepPreviousData: true,     // evita parpadeo al cambiar filtros
  });

  const puntos = puntosQ.data ?? [];
  const loading = bateriasQ.isLoading || puntosQ.isLoading;

  // Filtro de búsqueda de baterías para el dropdown
  const batsFiltradas = useMemo(() => {
    const q = batSearch.trim().toLowerCase();
    if (!q) return baterias;
    return baterias.filter((b) => b.toLowerCase().includes(q));
  }, [baterias, batSearch]);

  // KPIs simples
  const kpiTotal = puntos.length;
  const kpiValidadas = useMemo(() => puntos.filter((p) => p.validada !== false).length, [puntos]);
  const kpiNoVal = kpiTotal - kpiValidadas;

  const toggleBat = (b: string) => {
    setBatSel((prev) => (prev.includes(b) ? prev.filter((x) => x !== b) : [...prev, b]));
  };

  const selectAll = () => setBatSel(baterias);
  const clearAll = () => setBatSel([]);

  return (
    <div className="p-6 space-y-4">
      <div className="flex flex-wrap gap-3 items-center">
        <div className="relative">
          <button
            onClick={() => setBatOpen((s) => !s)}
            className="px-3 py-2 rounded bg-slate-200/10 border border-[#334155] text-slate-200 text-sm"
          >
            Baterías ({batSel.length}/{baterias.length})
          </button>

          {batOpen && (
            <div className="absolute z-20 mt-2 w-80 rounded border border-[#334155] bg-[#0f172a] p-3 shadow">
              <input
                value={batSearch}
                onChange={(e) => setBatSearch(e.target.value)}
                placeholder="Buscar batería…"
                className="w-full mb-2 px-2 py-1 rounded bg-[#0b1220] border border-[#334155] text-slate-200 text-sm"
              />

              <div className="flex gap-2 mb-2">
                <button
                  onClick={selectAll}
                  className="text-xs px-2 py-1 rounded bg-slate-200/10 border border-[#334155] text-slate-200"
                >
                  Seleccionar todas
                </button>
                <button
                  onClick={clearAll}
                  className="text-xs px-2 py-1 rounded bg-slate-200/10 border border-[#334155] text-slate-200"
                >
                  Limpiar
                </button>
              </div>

              <div className="max-h-64 overflow-auto space-y-1 pr-1">
                {batsFiltradas.map((b) => (
                  <label key={b} className="flex items-center gap-2 text-sm text-slate-200">
                    <input
                      type="checkbox"
                      checked={batSel.includes(b)}
                      onChange={() => toggleBat(b)}
                    />
                    <span>{b}</span>
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>

        <div className="flex flex-wrap gap-2 items-center">
          <label className="text-xs text-slate-400">Sum min</label>
          <input
            type="number"
            value={sumMin}
            onChange={(e) => setSumMin(Number(e.target.value))}
            className="w-24 px-2 py-1 rounded bg-[#0f172a] border border-[#334155] text-slate-200 text-sm"
          />

          <label className="text-xs text-slate-400">Sum max</label>
          <input
            type="number"
            value={sumMax}
            onChange={(e) => setSumMax(Number(e.target.value))}
            className="w-24 px-2 py-1 rounded bg-[#0f172a] border border-[#334155] text-slate-200 text-sm"
          />

          <label className="text-xs text-slate-400">Días min</label>
          <input
            type="number"
            value={diasMin}
            onChange={(e) => setDiasMin(Number(e.target.value))}
            className="w-24 px-2 py-1 rounded bg-[#0f172a] border border-[#334155] text-slate-200 text-sm"
          />

          <label className="text-xs text-slate-400">Días max</label>
          <input
            type="number"
            value={diasMax}
            onChange={(e) => setDiasMax(Number(e.target.value))}
            className="w-24 px-2 py-1 rounded bg-[#0f172a] border border-[#334155] text-slate-200 text-sm"
          />

          <select
            value={filtroVal}
            onChange={(e) => setFiltroVal(e.target.value as FiltroVal)}
            className="px-2 py-1 rounded bg-[#0f172a] border border-[#334155] text-slate-200 text-sm"
          >
            <option>Todos</option>
            <option>Solo validadas</option>
            <option>Solo no validadas</option>
          </select>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        <KPICard title="Puntos" value={kpiTotal} />
        <KPICard title="Validadas" value={kpiValidadas} />
        <KPICard title="No validadas" value={kpiNoVal} />
      </div>

      {bateriasQ.isError && (
        <div className="text-sm text-red-300">Error cargando baterías.</div>
      )}
      {puntosQ.isError && (
        <div className="text-sm text-red-300">Error cargando puntos del mapa.</div>
      )}

      <MapaSumergencia puntos={puntos} loading={loading} />

      <TablaValidaciones puntos={puntos} />
    </div>
  );
}
