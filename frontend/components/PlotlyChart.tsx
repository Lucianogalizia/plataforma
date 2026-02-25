"use client";

import { useEffect, useRef } from "react";

interface PlotlyChartProps {
  data: any[];
  layout?: Record<string, any>;
  title?: string;
  height?: number;
}

const BASE_LAYOUT = {
  paper_bgcolor: "transparent",
  plot_bgcolor:  "transparent",
  font:          { color: "#94a3b8", size: 11 },
  margin:        { t: 36, r: 16, b: 48, l: 48 },
  xaxis: {
    gridcolor: "#1e293b", linecolor: "#334155",
    tickfont:  { color: "#64748b", size: 10 },
  },
  yaxis: {
    gridcolor: "#1e293b", linecolor: "#334155",
    tickfont:  { color: "#64748b", size: 10 },
  },
  legend: { font: { color: "#94a3b8" }, bgcolor: "transparent" },
};

const CONFIG = {
  displaylogo:     false,
  responsive:      true,
  modeBarButtonsToRemove: ["sendDataToCloud", "editInChartStudio"],
};

export default function PlotlyChart({ data, layout = {}, title, height = 280 }: PlotlyChartProps) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!ref.current) return;

    const merged = {
      ...BASE_LAYOUT,
      ...layout,
      title: title ? { text: title, font: { color: "#cbd5e1", size: 13 }, x: 0.02 } : undefined,
      height,
      xaxis: { ...BASE_LAYOUT.xaxis, ...(layout.xaxis || {}) },
      yaxis: { ...BASE_LAYOUT.yaxis, ...(layout.yaxis || {}) },
    };

    // @ts-ignore
    import("plotly.js-dist-min").then((Plotly) => {
      Plotly.react(ref.current!, data, merged, CONFIG);
    });

    return () => {
      // @ts-ignore
      import("plotly.js-dist-min").then((Plotly) => {
        if (ref.current) Plotly.purge(ref.current);
      });
    };
  }, [data, layout, title, height]);

  return <div ref={ref} className="w-full" />;
}
