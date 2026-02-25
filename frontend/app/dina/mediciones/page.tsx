"use client";

import { useEffect, useState, useCallback } from "react";
import api, { Medicion, SerieSumergencia, OpcionDin } from "@/lib/api";
import CartaDinamica from "@/components/CartaDinamica";
import KPICard from "@/components/KPICard";
import SortableTable from "@/components/SortableTable";
import PlotlyChart from "@/components/PlotlyChart";

const COLS_BASE = [
  "ORIGEN","pozo","Batería","fecha","hora","din_datetime","niv_datetime",
  "CO","empresa","SE","NM","NC","ND","PE","PB","CM","Sumergencia","Sumergencia_base",
  "AIB Carrera","Sentido giro","Tipo Contrapesos","Distancia contrapesos (cm)",
  "Contrapeso actual","Contrapeso ideal","AIBEB_Torque max contrapeso",
  "%Estructura","%Balance","Bba Diam Pistón","Bba Prof","Bba Llenado",
  "GPM","Caudal bruto efec","Polea Motor","Potencia Motor","RPM Motor",
];

export default function MedicionesPage() {
  const [pozo, setPozo] = useState<string>("");
  const [mediciones, setMediciones] = useState<Medicion[]>([]);
  const [opciones, setOpciones] = useState<OpcionDin[]>([]);
  const [serie, setSerie] = useState<SerieSumergencia[]>([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);

  const cargar = useCallback(async (p: string) => {
    if (!p) return;
    setLoading(true);
    try {
      const [med, hist] = await Promise.all([
        api.getMediciones(p),
        api.getHistoricoSumergencia(p),
      ]);
      setMediciones(med.mediciones);
      setOpciones(med.opciones_din);
      setTotal(med.total);
      setSerie(hist.serie);
    } catch {
      setMediciones([]);
      setSerie([]);
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    const p = sessionStorage.getItem("dina_pozo_sel") || "";
    setPozo(p);
    cargar(p);
    const handler = (e: Event) => {
      const p2 = (e as CustomEvent<string>).detail;
      setPozo(p2);
      cargar(p2);
    };
    window.addEventListener("dina:pozo", handler);
    return () => window.removeEventListener("dina:pozo", handler);
  }, [cargar]);

  const cols = COLS_BASE.filter((c) => mediciones.some((m) => m[c] != null));

  const medCols = cols.map((c) => ({
    key: c,
    label: c,
    render: (v: any) =>
      v == null ? "—" : typeof v === "number" ? v.toFixed(2) : String(v),
  }));

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-bold text-slate-100">
        📈 Mediciones — Pozo: {pozo || "…"}
      </h2>

      {loading && <p className="text-slate-500 text-sm animate-pulse">Cargando mediciones…</p>}

      {!loading && (
        <>
          {/* KPIs */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <KPICard title="Mediciones totales" value={total} color="sky" />
            <KPICard title="Datos DIN" value={mediciones.filter((m) => m.ORIGEN === "DIN").length} />
            <KPICard title="Datos NIV" value={mediciones.filter((m) => m.ORIGEN === "NIV").length} />
            <KPICard title="Con Sumergencia" value={mediciones.filter((m) => m.Sumergencia != null).length} color="green" />
          </div>

          {/* Tabla */}
          <div>
            <h3 className="text-sm font-medium text-slate-300 mb-2">Tabla de mediciones (DIN + NIV)</h3>
            <SortableTable
              cols={medCols}
              rows={mediciones}
              title={`mediciones_${pozo}`}
              maxHeight="320px"
              emptyMsg="Sin mediciones para este pozo."
            />
          </div>

          {/* Histórico Sumergencia */}
          {serie.length > 0 && (
            <div className="card p-3">
              <PlotlyChart
                title="Histórico — Sumergencia vs Tiempo"
                data={[{
                  type: "scatter",
                  mode: "lines+markers",
                  x: serie.map((s) => s.dt),
                  y: serie.map((s) => s.sumergencia),
                  name: "Sumergencia",
                  line: { color: "#38bdf8", width: 2 },
                  marker: { size: 4 },
                }]}
                layout={{ yaxis: { title: { text: "Sumergencia (m)" } } }}
                height={380}
              />
            </div>
          )}

          {serie.length === 0 && (
            <div className="card text-center text-slate-500 text-sm py-6">
              No hay datos de Sumergencia disponibles para este pozo.
            </div>
          )}

          {/* Carta Dinamométrica */}
          {opciones.length > 0 ? (
            <div className="card p-0 overflow-hidden">
              <div className="px-4 py-3 border-b border-[#334155]">
                <h3 className="text-sm font-medium text-slate-300">Carta Dinamométrica — Superficie (CS)</h3>
              </div>
              <div className="p-4">
                <CartaDinamica opcionesDin={opciones} />
              </div>
            </div>
          ) : (
            <div className="card text-center text-slate-500 text-sm py-6">
              Este pozo no tiene archivos DIN para graficar (solo NIV o no se resolvió el path).
            </div>
          )}
        </>
      )}
    </div>
  );
}
