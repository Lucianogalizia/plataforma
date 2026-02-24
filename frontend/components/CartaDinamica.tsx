"use client";

import { useEffect, useState, useRef } from "react";
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
  const chartRef = useRef<HTMLDivElement>(null);
  const curvasRef = useRef(curvas);
  curvasRef.current = curvas;

  const cargarCurvas = async (ids: string[]) => {
    if (!ids.length) return;
    setLoading(true);
    setErrores([]);
    const nuevas: Record<string, PuntoCS[]> = {};
    const errs: string[] = [];

    await Promise.all(
      ids.map(async (id) => {
        if (curvasRef.current[id]) {
          nuevas[id] = curvasRef.current[id];
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
  };

  useEffect(() => {
    cargarCurvas(seleccionados);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seleccionados]);

  // Resize al cambiar tamaño de ventana
  useEffect(() => {
    const onResize = () => {
      import("plotly.js-dist-min").then((mod) => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const Plotly = (mod as any).default ?? mod;
        if (chartRef.current) Plotly.Plots.resize(chartRef.current);
      });
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  // Renderizar con Plotly
  useEffect(() => {
    if (!chartRef.current || loading) return;

    const traces = seleccionados
      .filter((id) => curvas[id]?.length)
      .map((id, i) => {
        const pts = curvas[id];
        const label = opcionesDin.find((o) => o.id === id)?.label || `Medición ${i + 1}`;
        return {
          x: pts.map((p) => p.X),
          y: pts.map((p) => p.Y),
          mode: "lines",
          name: label,
          line: { color: COLORES[i % COLORES.length], width: 2 },
          type: "scatter",
        };
      });

    if (!traces.length) return;

    const layout = {
      xaxis: {
        title: { text: "X (posición / carrera)", font: { color: "#64748b" } },
        gridcolor: "#334155",
        color: "#64748b",
        tickfont: { color: "#94a3b8", size: 11 },
        zeroline: false,
      },
      yaxis: {
        title: { text: "Y (carga)", font: { color: "#64748b" } },
        gridcolor: "#334155",
        color: "#64748b",
        tickfont: { color: "#94a3b8", size: 11 },
        zeroline: false,
      },
      paper_bgcolor: "#1e293b",
      plot_bgcolor: "#0f172a",
      font: { color: "#94a3b8" },
      legend: { font: { color: "#94a3b8", size: 12 }, bgcolor: "rgba(0,0,0,0)" },
      margin: { t: 20, r: 30, b: 60, l: 70 },
      height: 520,
      autosize: true,
    };

    const config = {
      responsive: true,
      displayModeBar: true,
      displaylogo: false,
      modeBarButtonsToRemove: ["sendDataToCloud", "lasso2d"],
    };

    import("plotly.js-dist-min").then((mod) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const Plotly = (mod as any).default ?? mod;
      if (chartRef.current) {
        Plotly.react(chartRef.current, traces, layout, config).then(() => {
          setTimeout(() => {
            if (chartRef.current) Plotly.Plots.resize(chartRef.current);
          }, 50);
        });
      }
    });
  }, [curvas, seleccionados, loading, opcionesDin]);

  const toggleSeleccion = (id: string) => {
    setSeleccionados((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]
    );
  };

  const hayCurvas = seleccionados.some((id) => curvas[id]?.length);

  return (
    <div className="space-y-4">
      {/* Selector de mediciones — scroll horizontal si hay muchas */}
      <div>
        <p className="text-xs text-slate-400 mb-2">
          Seleccioná una o varias mediciones DIN para superponer:
        </p>
        <div className="flex gap-2 overflow-x-auto pb-1">
          {opcionesDin.map((op, i) => {
            const sel = seleccionados.includes(op.id);
            return (
              <button
                key={op.id}
                onClick={() => toggleSeleccion(op.id)}
                className={`shrink-0 text-xs px-3 py-1.5 rounded-full border transition-colors ${
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
        <div className="text-xs text-slate-400 text-center py-8">
          Cargando carta dinamométrica...
        </div>
      )}

      {!loading && hayCurvas && (
        <div style={{ width: "100%", minWidth: 0 }}>
          {/* El div que Plotly controla directamente — sin overflow ni clipping */}
          <div
            ref={chartRef}
            style={{ width: "100%", height: 520 }}
          />
        </div>
      )}

      {!loading && !hayCurvas && seleccionados.length > 0 && (
        <p className="text-sm text-slate-500 text-center py-8">
          No se encontraron puntos CS para las mediciones seleccionadas.
        </p>
      )}
    </div>
  );
}
