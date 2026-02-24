"use client";

import { useEffect, useState, useCallback } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import api, { OpcionDin, PuntoCS } from "@/lib/api";

interface CartaDinamicaProps {
  opcionesDin: OpcionDin[];
}

const COLORES = [
  "#38bdf8", "#22c55e", "#f97316", "#a78bfa", "#fb7185",
  "#facc15", "#34d399", "#60a5fa", "#f472b6",
];

export default function CartaDinamica({ opcionesDin }: CartaDinamicaProps) {
  const [seleccionados, setSeleccionados] = useState<string[]>(
    opcionesDin.length > 0 ? [opcionesDin[0].id] : []
  );
  const [curvas, setCurvas] = useState<Record<string, PuntoCS[]>>({});
  const [loading, setLoading] = useState(false);
  const [errores, setErrores] = useState<string[]>([]);

  const cargarCurvas = useCallback(async () => {
    if (!seleccionados.length) return;
    setLoading(true);
    setErrores([]);
    const nuevas: Record<string, PuntoCS[]> = {};
    const errs: string[] = [];

    await Promise.all(
      seleccionados.map(async (id) => {
        if (curvas[id]) {
          nuevas[id] = curvas[id];
          return;
        }
        try {
          const res = await api.getCartaSuperficie(id);
          if (res.n_puntos > 0) nuevas[id] = res.puntos;
          else errs.push(`Sin puntos CS: ${id.split("/").pop()}`);
        } catch {
          errs.push(`Error: ${id.split("/").pop()}`);
        }
      })
    );

    setCurvas((prev) => ({ ...prev, ...nuevas }));
    setErrores(errs);
    setLoading(false);
  }, [seleccionados, curvas]);

  useEffect(() => {
    cargarCurvas();
  }, [cargarCurvas]);

  // Combinar todos los puntos para el eje X
  const allXs = new Set<number>();
  seleccionados.forEach((id) => {
    curvas[id]?.forEach((p) => allXs.add(p.X));
  });
  const xs = Array.from(allXs).sort((a, b) => a - b);

  // Construir data para recharts
  const chartData = xs.map((x) => {
    const row: Record<string, unknown> = { X: x };
    seleccionados.forEach((id, idx) => {
      const pts = curvas[id];
      if (!pts) return;
      const pt = pts.find((p) => p.X === x);
      const label =
        opcionesDin.find((o) => o.id === id)?.label || `Medición ${idx + 1}`;
      row[label] = pt?.Y ?? null;
    });
    return row;
  });

  const labels = seleccionados.map(
    (id, idx) =>
      opcionesDin.find((o) => o.id === id)?.label || `Medición ${idx + 1}`
  );

  const toggleSeleccion = (id: string) => {
    setSeleccionados((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]
    );
  };

  return (
    <div className="space-y-4">
      {/* Selector de mediciones */}
      <div>
        <p className="text-xs text-slate-400 mb-2">
          Seleccioná una o varias mediciones DIN para superponer:
        </p>
        <div className="flex flex-wrap gap-2">
          {opcionesDin.map((op, i) => {
            const sel = seleccionados.includes(op.id);
            return (
              <button
                key={op.id}
                onClick={() => toggleSeleccion(op.id)}
                className={`text-xs px-3 py-1.5 rounded-full border transition-colors ${
                  sel
                    ? "border-sky-400 bg-sky-400/10 text-sky-300"
                    : "border-slate-600 text-slate-400 hover:border-slate-400"
                }`}
                style={sel ? { borderColor: COLORES[i % COLORES.length] } : undefined}
              >
                <span
                  className="inline-block w-2 h-2 rounded-full mr-1.5"
                  style={{ background: sel ? COLORES[i % COLORES.length] : "#475569" }}
                />
                {op.label}
              </button>
            );
          })}
        </div>
      </div>

      {errores.length > 0 && (
        <div className="text-xs text-yellow-400 bg-yellow-400/10 px-3 py-2 rounded">
          ⚠️ {errores.join(" | ")}
        </div>
      )}

      {loading && (
        <div className="text-xs text-slate-400 text-center py-4">
          Cargando carta dinamométrica...
        </div>
      )}

      {!loading && chartData.length > 0 && (
        <div className="card p-0 overflow-hidden">
          <div className="px-4 py-3 border-b border-[#334155]">
            <h3 className="text-sm font-medium text-slate-300">
              Carta Dinamométrica — Superficie (CS)
            </h3>
          </div>
          <ResponsiveContainer width="100%" height={520}>
            <LineChart
              data={chartData}
              margin={{ top: 16, right: 32, left: 16, bottom: 16 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis
                dataKey="X"
                stroke="#64748b"
                tick={{ fill: "#94a3b8", fontSize: 11 }}
                label={{ value: "X (posición / carrera)", position: "insideBottom", offset: -8, fill: "#64748b", fontSize: 12 }}
              />
              <YAxis
                stroke="#64748b"
                tick={{ fill: "#94a3b8", fontSize: 11 }}
                label={{ value: "Y (carga)", angle: -90, position: "insideLeft", offset: 8, fill: "#64748b", fontSize: 12 }}
              />
              <Tooltip
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                labelStyle={{ color: "#94a3b8", fontSize: 11 }}
                itemStyle={{ fontSize: 12 }}
              />
              <Legend wrapperStyle={{ fontSize: 12, color: "#94a3b8" }} />
              {labels.map((label, i) => (
                <Line
                  key={label}
                  type="monotone"
                  dataKey={label}
                  stroke={COLORES[i % COLORES.length]}
                  strokeWidth={2}
                  dot={false}
                  connectNulls={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {!loading && chartData.length === 0 && seleccionados.length > 0 && (
        <p className="text-sm text-slate-500 text-center py-8">
          No se encontraron puntos CS para las mediciones seleccionadas.
        </p>
      )}
    </div>
  );
}
