"use client";

import { useEffect, useState, useCallback } from "react";
import api, { clearApiCache } from "@/lib/api";

interface AlertasPresionInfo {
  exists: boolean;
  updated_at: string | null;
  file?: string;
  size_kb?: number;
  error?: string;
}

export default function AlertasPresionPage() {
  const [info, setInfo] = useState<AlertasPresionInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [iframeKey, setIframeKey] = useState(0);

  const API_URL =
    process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

  const cargarInfo = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.getAlertasPresionInfo();
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
    clearApiCache("/api/alertas-presion");
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
    <div className="flex flex-col" style={{ height: "100vh" }}>
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155] flex-shrink-0">
        <div>
          <h1 className="text-xl font-bold text-slate-100">
            Predicción Alta Presión
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
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg bg-red-700 hover:bg-red-600 text-white disabled:opacity-50 transition-colors"
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
      <div className="flex-1 relative min-h-0">
        {loading && !info ? (
          <div className="absolute inset-0 flex items-center justify-center bg-[#0f172a]">
            <div className="text-center">
              <div className="w-8 h-8 border-2 border-red-400 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
              <p className="text-slate-400 text-sm">Cargando predicciones...</p>
            </div>
          </div>
        ) : info?.exists ? (
          <iframe
            key={iframeKey}
            src={`${API_URL}/api/alertas-presion/dashboard`}
            className="w-full border-0"
            style={{ height: "100%" }}
            title="Predicción Alta Presión"
            sandbox="allow-scripts allow-same-origin"
          />
        ) : (
          <div className="absolute inset-0 flex items-center justify-center bg-[#0f172a]">
            <div className="text-center max-w-md mx-auto px-6">
              <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg
                  className="w-8 h-8 text-red-500"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                  />
                </svg>
              </div>
              <h2 className="text-lg font-semibold text-slate-200 mb-2">
                Dashboard no disponible
              </h2>
              <p className="text-sm text-slate-400 mb-1">
                {info?.error ||
                  "No se encontró el archivo alertas_presion.html en GCS."}
              </p>
              <p className="text-xs text-slate-500">
                Ejecutá{" "}
                <span className="font-mono bg-slate-800 px-1 rounded">
                  presion_final_completo.py
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
