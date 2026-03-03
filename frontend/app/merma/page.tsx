"use client";

import { useEffect, useState, useCallback } from "react";
import api, { clearApiCache } from "@/lib/api";

interface MermaInfo {
  exists: boolean;
  updated_at: string | null;
  file?: string;
  size_kb?: number;
  error?: string;
}

export default function MermaPage() {
  const [info, setInfo] = useState<MermaInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [iframeKey, setIframeKey] = useState(0);

  const API_URL =
    process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

  const cargarInfo = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.getMermaInfo();
      setInfo(data);
    } catch {
      setInfo({ exists: false, updated_at: null, error: "Error conectando con el backend" });
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    cargarInfo();
  }, [cargarInfo]);

  const handleRefresh = () => {
    clearApiCache("/api/merma");
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
      <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155]">
        <div>
          <h1 className="text-xl font-bold text-slate-100">
            Análisis de MERMA
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
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-sky-600 hover:bg-sky-500 text-white disabled:opacity-50 transition-colors"
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

      {/* Content */}
      <div className="flex-1 relative">
        {loading && !info ? (
          <div className="absolute inset-0 flex items-center justify-center bg-[#0f172a]">
            <div className="text-center">
              <div className="w-8 h-8 border-2 border-sky-400 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
              <p className="text-slate-400 text-sm">Cargando dashboard...</p>
            </div>
          </div>
        ) : info?.exists ? (
          <iframe
            key={iframeKey}
            src={`${API_URL}/api/merma/dashboard`}
            className="w-full h-full border-0"
            title="Dashboard MERMA"
            sandbox="allow-scripts allow-same-origin"
          />
        ) : (
          <div className="absolute inset-0 flex items-center justify-center bg-[#0f172a]">
            <div className="text-center max-w-md mx-auto px-6">
              <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg
                  className="w-8 h-8 text-slate-500"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
                  />
                </svg>
              </div>
              <h2 className="text-lg font-semibold text-slate-200 mb-2">
                Dashboard no disponible
              </h2>
              <p className="text-sm text-slate-400 mb-1">
                {info?.error || "No se encontró el archivo dashboard_master.html en GCS."}
              </p>
              <p className="text-xs text-slate-500">
                Ejecutá el script de generación de MERMA para subir el dashboard.
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
