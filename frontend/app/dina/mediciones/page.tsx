"use client";

import { useEffect, useState, useCallback } from "react";
import api, { Medicion, SerieSumergencia, OpcionDin } from "@/lib/api";
import CartaDinamica from "@/components/CartaDinamica";
import KPICard from "@/components/KPICard";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from "recharts";

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

  const cols = COLS_BASE.filter((c) =>
    mediciones.some((m) => m[c] != null)
  );

  const serieData = serie.map((s) => ({
    dt: new Date(s.dt).toLocaleDateString("es-AR"),
    Sumergencia: s.sumergencia,
    PB: s.pb,
    Nivel: s.nivel_usado,
    origen: s.origen,
  }));

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-bold text-slate-100">
          📈 Mediciones — Pozo: {pozo || "…"}
        </h2>
      </div>

      {loading && (
        <p className="text-slate-500 text-sm animate-pulse">Cargando mediciones…</p>
      )}

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
          <div className="card p-0 overflow-hidden">
            <div className="px-4 py-3 border-b border-[#334155] flex items-center justify-between">
              <h3 className="text-sm font-medium text-slate-300">
                Tabla de mediciones (DIN + NIV)
              </h3>
              <span className="text-xs text-slate-500">{total} registros</span>
            </div>
            <div className="overflow-x-auto max-h-80 overflow-y-auto">
              <table className="w-full text-xs">
                <thead className="sticky top-0 z-10">
                  <tr>
                    {cols.map((c) => (
                      <th key={c} className="bg-[#1e293b] border-b border-[#334155] px-3 py-2">
                        {c}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {mediciones.map((m, i) => (
                    <tr key={i}>
                      {cols.map((c) => {
                        const v = m[c];
                        return (
                          <td key={c} className="px-3 py-1.5 text-slate-300">
                            {v == null ? "—" : typeof v === "number" ? v.toFixed(2) : String(v)}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                  {mediciones.length === 0 && (
                    <tr>
                      <td colSpan={cols.length || 1} className="px-3 py-6 text-center text-slate-500">
                        Sin mediciones para este pozo.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* Histórico Sumergencia */}
          {serieData.length > 0 && (
            <div className="card p-0 overflow-hidden">
              <div className="px-4 py-3 border-b border-[#334155]">
                <h3 className="text-sm font-medium text-slate-300">
                  📉 Histórico — Sumergencia vs Tiempo
                </h3>
              </div>
              <div className="p-4">
                <ResponsiveContainer width="100%" height={380}>
                  <LineChart data={serieData} margin={{ top: 8, right: 24, left: 8, bottom: 8 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey="dt" stroke="#64748b" tick={{ fill: "#94a3b8", fontSize: 11 }} />
                    <YAxis
                      stroke="#64748b"
                      tick={{ fill: "#94a3b8", fontSize: 11 }}
                      label={{ value: "Sumergencia (m)", angle: -90, position: "insideLeft", fill: "#64748b", fontSize: 12 }}
                    />
                    <Tooltip
                      contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                      labelStyle={{ color: "#94a3b8", fontSize: 11 }}
                    />
                    <Legend wrapperStyle={{ fontSize: 12, color: "#94a3b8" }} />
                    <Line
                      type="monotone"
                      dataKey="Sumergencia"
                      stroke="#38bdf8"
                      strokeWidth={2}
                      dot={{ r: 3, fill: "#38bdf8" }}
                      activeDot={{ r: 5 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {serieData.length === 0 && !loading && (
            <div className="card text-center text-slate-500 text-sm py-6">
              No hay datos de Sumergencia disponibles para este pozo.
            </div>
          )}

          {/* Carta Dinamométrica */}
          {opciones.length > 0 ? (
            <div className="card p-0 overflow-hidden">
              <div className="px-4 py-3 border-b border-[#334155]">
                <h3 className="text-sm font-medium text-slate-300">
                  Carta Dinamométrica — Superficie (CS)
                </h3>
              </div>
              <div className="p-4">
                <CartaDinamica opcionesDin={opciones} />
              </div>
            </div>
          ) : (
            !loading && (
              <div className="card text-center text-slate-500 text-sm py-6">
                Este pozo no tiene archivos DIN para graficar (solo NIV o no se resolvió el path).
              </div>
            )
          )}
        </>
      )}
    </div>
  );
}
