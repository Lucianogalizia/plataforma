"use client";

import { useEffect, useRef, useState } from "react";
import { PuntoMapa } from "@/lib/api";

interface MapaSumergenciaProps {
  puntos: PuntoMapa[];
  height?: number;
}

// Renderizado usando canvas básico (fallback sin mapas externos)
// Se integra con deck.gl cuando está disponible
export default function MapaSumergencia({ puntos, height = 500 }: MapaSumergenciaProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [tooltip, setTooltip] = useState<{ x: number; y: number; punto: PuntoMapa } | null>(null);

  // Proyección simple: lat/lon → canvas pixels
  const pts = puntos.filter((p) => p.lat != null && p.lon != null && p.Sumergencia != null);

  const latMin = Math.min(...pts.map((p) => p.lat));
  const latMax = Math.max(...pts.map((p) => p.lat));
  const lonMin = Math.min(...pts.map((p) => p.lon));
  const lonMax = Math.max(...pts.map((p) => p.lon));
  const sumMin = Math.min(...pts.map((p) => p.Sumergencia!));
  const sumMax = Math.max(...pts.map((p) => p.Sumergencia!));

  const project = (lat: number, lon: number, w: number, h: number) => {
    const x = ((lon - lonMin) / (lonMax - lonMin || 1)) * (w - 40) + 20;
    const y = h - ((lat - latMin) / (latMax - latMin || 1)) * (h - 40) - 20;
    return { x, y };
  };

  const sumColor = (s: number): string => {
    const t = (s - sumMin) / (sumMax - sumMin || 1);
    const r = Math.round(56 + t * 199);
    const g = Math.round(189 - t * 100);
    const b = Math.round(248 - t * 200);
    return `rgb(${r},${g},${b})`;
  };

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || pts.length === 0) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const W = canvas.width;
    const H = canvas.height;

    ctx.clearRect(0, 0, W, H);

    // Fondo
    ctx.fillStyle = "#0f172a";
    ctx.fillRect(0, 0, W, H);

    // Grid
    ctx.strokeStyle = "#1e293b";
    ctx.lineWidth = 1;
    for (let i = 0; i < 10; i++) {
      ctx.beginPath();
      ctx.moveTo((W / 10) * i, 0);
      ctx.lineTo((W / 10) * i, H);
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(0, (H / 10) * i);
      ctx.lineTo(W, (H / 10) * i);
      ctx.stroke();
    }

    // Puntos con halo de calor
    pts.forEach((p) => {
      const { x, y } = project(p.lat, p.lon, W, H);
      const r = 14;
      const grad = ctx.createRadialGradient(x, y, 0, x, y, r);
      const col = sumColor(p.Sumergencia!);
      grad.addColorStop(0, col.replace("rgb", "rgba").replace(")", ",0.5)"));
      grad.addColorStop(1, "rgba(0,0,0,0)");
      ctx.fillStyle = grad;
      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI * 2);
      ctx.fill();

      // Punto central
      ctx.fillStyle = col;
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fill();
    });
  }, [pts, sumMin, sumMax]);

  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const W = canvas.width;
    const H = canvas.height;

    const found = pts.find((p) => {
      const { x, y } = project(p.lat, p.lon, W, H);
      return Math.hypot(mx - x, my - y) < 10;
    });

    if (found) {
      setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, punto: found });
    } else {
      setTooltip(null);
    }
  };

  return (
    <div className="relative">
      <canvas
        ref={canvasRef}
        width={900}
        height={height}
        className="w-full rounded-lg"
        style={{ maxHeight: height }}
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setTooltip(null)}
      />
      {tooltip && (
        <div
          className="absolute z-10 bg-[#1e293b] border border-[#334155] rounded-lg p-3 text-xs shadow-xl pointer-events-none"
          style={{ left: tooltip.x + 12, top: tooltip.y - 40 }}
        >
          <p className="font-bold text-slate-200">{tooltip.punto.NO_key}</p>
          {tooltip.punto.nivel_5 && <p className="text-slate-400">Batería: {tooltip.punto.nivel_5}</p>}
          <p className="text-slate-400">Origen: {tooltip.punto.ORIGEN}</p>
          <p className="text-sky-400 font-semibold">Sumergencia: {tooltip.punto.Sumergencia?.toFixed(1)} m</p>
          <p className="text-slate-400">Días: {tooltip.punto.Dias_desde_ultima?.toFixed(0)}</p>
          <p className="text-slate-500">{tooltip.punto.DT_plot_str}</p>
        </div>
      )}
      {pts.length === 0 && (
        <div className="absolute inset-0 flex items-center justify-center text-slate-500 text-sm">
          Sin datos con coordenadas para mostrar.
        </div>
      )}
    </div>
  );
}
