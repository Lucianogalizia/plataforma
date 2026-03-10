"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import Sidebar from "@/components/Sidebar";
import KPICard from "@/components/KPICard";
import api from "@/lib/api";
import { Activity, Map, Bot, TrendingUp, AlertCircle } from "lucide-react";

interface SystemInfo {
  version: string;
  gcs_ok: boolean;
  openai_ok: boolean;
  din_count: number;
  niv_count: number;
  uptime_seg: number;
}

export default function DashboardPage() {
  const [info, setInfo] = useState<SystemInfo | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.getInfo()
      .then(setInfo)
      .catch((e) => setError(e.message));
  }, []);

  const MODULOS = [
    {
      href: "/dina/mediciones",
      icon: TrendingUp,
      label: "Mediciones",
      desc: "Cartas dinamométricas CS, histórico de Sumergencia, datos DIN + NIV por pozo.",
      color: "sky",
    },
    {
      href: "/dina/estadisticas",
      icon: Activity,
      label: "Estadísticas",
      desc: "Snapshot global, KPIs, tendencias por variable, cobertura DIN vs NIV, semáforo AIB.",
      color: "green",
    },
    {
      href: "/dina/mapa",
      icon: Map,
      label: "Mapa de Sumergencia",
      desc: "Heatmap de densidad con sumergencia por pozo. Filtros por batería y validación.",
      color: "orange",
    },
    {
      href: "/dina/diagnosticos",
      icon: Bot,
      label: "Diagnósticos IA",
      desc: "Análisis automático con IA de cartas dinamométricas. Tabla global y diagnóstico por pozo.",
      color: "purple",
    },
    {
      href: "/merma",
      icon: TrendingUp,
      label: "Análisis de MERMA",
      desc: "Dashboard interactivo de análisis de merma de producción. Generado por script local.",
      color: "amber",
    },
    {
      href: "/instalacion-fondo",
      icon: Activity,
      label: "Instalación de Fondo",
      desc: "Visualizador IF por pozo: tubing, varillas, cañería y punzados. Generado por script local.",
      color: "emerald",
    },
  ];

  const colorBorder: Record<string, string> = {
    sky:     "border-sky-500/30    hover:border-sky-400",
    green:   "border-green-500/30  hover:border-green-400",
    orange:  "border-orange-500/30 hover:border-orange-400",
    purple:  "border-purple-500/30 hover:border-purple-400",
    amber:   "border-amber-500/30  hover:border-amber-400",
    emerald: "border-emerald-500/30 hover:border-emerald-400",
  };
  const colorIcon: Record<string, string> = {
    sky:     "text-sky-400",
    green:   "text-green-400",
    orange:  "text-orange-400",
    purple:  "text-purple-400",
    amber:   "text-amber-400",
    emerald: "text-emerald-400",
  };

  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className="flex-1 p-8 space-y-8">
        <div>
          <h1 className="text-2xl font-bold text-slate-100">Dashboard</h1>
          <p className="text-slate-400 text-sm mt-1">
            Plataforma de análisis dinamométrico DINA
          </p>
        </div>

        {/* KPIs sistema */}
        {info && (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <KPICard
              title="Archivos DIN indexados"
              value={info.din_count.toLocaleString("es-AR")}
              color="sky"
            />
            <KPICard
              title="Archivos NIV indexados"
              value={info.niv_count.toLocaleString("es-AR")}
              color="green"
            />
            <KPICard
              title="GCS"
              value={info.gcs_ok ? "✅ Conectado" : "❌ Sin conexión"}
              color={info.gcs_ok ? "green" : "red"}
            />
            <KPICard
              title="IA (OpenAI)"
              value={info.openai_ok ? "✅ Disponible" : "⚠️ No configurada"}
              color={info.openai_ok ? "green" : "yellow"}
            />
          </div>
        )}

        {error && (
          <div className="card border-red-500/30 flex gap-3 items-center">
            <AlertCircle size={16} className="text-red-400 shrink-0" />
            <span className="text-sm text-red-400">
              No se pudo conectar con el backend: {error}
            </span>
          </div>
        )}

        {/* Módulos */}
        <div>
          <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-4">
            Módulos disponibles
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {MODULOS.map((m) => {
              const Icon = m.icon;
              return (
                <Link
                  key={m.href}
                  href={m.href}
                  className={`card border transition-colors ${colorBorder[m.color]} group`}
                >
                  <div className="flex items-start gap-4">
                    <div className={`mt-1 ${colorIcon[m.color]}`}>
                      <Icon size={22} />
                    </div>
                    <div>
                      <h3 className="font-semibold text-slate-200 group-hover:text-white transition-colors">
                        {m.label}
                      </h3>
                      <p className="text-sm text-slate-400 mt-1">{m.desc}</p>
                    </div>
                  </div>
                </Link>
              );
            })}
          </div>
        </div>

        {/* Info sistema */}
        {info && (
          <div className="card text-xs text-slate-500 space-y-1">
            <p>Versión API: {info.version}</p>
            <p>Uptime: {Math.round(info.uptime_seg)}s</p>
          </div>
        )}
      </main>
    </div>
  );
}
