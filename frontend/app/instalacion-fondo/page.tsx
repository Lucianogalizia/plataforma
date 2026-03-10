"use client";

import { useEffect, useState, useCallback } from "react";
import api, { clearApiCache } from "@/lib/api";

interface InstalacionFondoInfo {
  exists: boolean;
  updated_at: string | null;
  file?: string;
  size_kb?: number;
  error?: string;
}

export default function InstalacionFondoPage() {
  const [info, setInfo] = useState<InstalacionFondoInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [iframeKey, setIframeKey] = useState(0);

  const API_URL =
    process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

  const cargarInfo = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.getInstalacionFondoInfo();
      setInfo(data);
    } catch {
      setInfo({
        exists: false,
        updated_at: null,
        error: "Error conectando con el backend",
      });
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    cargarInfo();
  }, [cargarInfo]);

  const handleRefresh = () => {
    clearApiCache("/api/instalacion-fondo/info");
    setIframeKey((k) => k + 1);
    cargarInfo();
  };

  const formatDate = (iso: string | null) => {
    if (!iso) return "—";
    try {
      return new Date(iso).toLocaleString("es-AR", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
    } catch {
      return iso;
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155] flex-shrink-0">
        <div>
          <h1 className="text-xl font-bold text-slate-100">
            Instalación de Fondo — Viewer IF
          </h1>
          {info?.exists && (
            <p className="text-xs text-slate-400 mt-1">
              Última actualización: {formatDate(info.updated_at)}
              {info.size_kb ? ` · ${info.size_kb} KB` : ""}
            </p>
          )}
        </div>
        <button
          onClick={handleRefresh}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-emerald-700 hover:bg-emerald-600 text-white disabled:opacity-50 transition-colors"
        >
          <svg
            className={`w-4 h-4 ${loading ? "animate-spin" : ""}`}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
            />
          </svg>
          Actualizar
        </button>
      </div>

      {/* Content — ocupa todo el resto */}
      <div className="flex-1 relative">
        {loading && !info ? (
          <div className="absolute inset-0 flex items-center justify-center bg-[#0f172a]">
            <div className="text-center">
              <div className="w-8 h-8 border-2 border-emerald-400 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
              <p className="text-slate-400 text-sm">Cargando visualizador...</p>
            </div>
          </div>
        ) : info?.exists ? (
          <iframe
            key={iframeKey}
            src={`${API_URL}/api/instalacion-fondo/dashboard`}
            className="w-full h-full border-0"
            title="Instalación de Fondo — Viewer IF"
            sandbox="allow-scripts allow-same-origin"
          />
        ) : (
          <div className="absolute inset-0 flex items-center justify-center bg-[#0f172a]">
            <div className="text-center max-w-md mx-auto px-6">
              <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg
                  className="w-8 h-8 text-emerald-500"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
                  />
                </svg>
              </div>
              <h2 className="text-lg font-semibold text-slate-200 mb-2">
                Dashboard no disponible
              </h2>
              <p className="text-sm text-slate-400 mb-1">
                {info?.error ||
                  "No se encontró el archivo visualizador_if.html en GCS."}
              </p>
              <p className="text-xs text-slate-500">
                Ejecutá{" "}
                <span className="font-mono bg-slate-800 px-1 rounded">
                  generar_json_IF_con_export_CSV.py
                </span>{" "}
                en Jupyter para generar y subir el dashboard.
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
