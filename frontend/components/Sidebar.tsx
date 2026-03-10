"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  TrendingUp,
  Map,
  Bot,
  Activity,
  ChevronRight,
  Wrench,
  Users,
  BarChart2,
} from "lucide-react";

const NAV = [
  {
    label: "Dashboard",
    href: "/",
    icon: LayoutDashboard,
  },
  {
    label: "DINA",
    icon: Activity,
    children: [
      { label: "Mediciones",   href: "/dina/mediciones",   icon: TrendingUp },
      { label: "Estadísticas", href: "/dina/estadisticas", icon: LayoutDashboard },
      { label: "Mapa",         href: "/dina/mapa",         icon: Map },
      { label: "Diagnósticos", href: "/dina/diagnosticos", icon: Bot },
      { label: "Acciones",     href: "/dina/acciones",     icon: Wrench },
    ],
  },
  {
    label: "PI Vision",
    href: "/pi-vision",
    icon: Activity,
    disabled: true,
    badge: "Próximamente",
  },
  {
    label: "MERMA",
    href: "/merma",
    icon: TrendingUp,
  },
  {
    label: "Histórico de Pérdidas",
    href: "/historico-perdidas",
    icon: BarChart2,
  },
  {
    label: "Alertas Llenado BM",
    href: "/alertas-llenado",
    icon: Activity,
  },
  {
    label: "Controles Históricos",
    href: "/controles-historicos",
    icon: BarChart2,
  },
  {
    label: "Instalación de Fondo",
    href: "/instalacion-fondo",
    icon: Activity,
  },
  {
    label: "RRHH",
    href: "/rrhh",
    icon: Users,
  },
];

export default function Sidebar() {
  const path = usePathname();

  return (
    <aside className="w-60 min-h-screen bg-[#1e293b] border-r border-[#334155] flex flex-col">
      {/* Logo */}
      <div className="px-6 py-5 border-b border-[#334155]">
        <span className="text-xl font-bold text-sky-400 tracking-wide">
          DINA
        </span>
        <span className="text-xs text-slate-400 ml-2">Plataforma</span>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
        {NAV.map((item) => {
          if ("children" in item && item.children) {
            return (
              <div key={item.label}>
                <p className="px-3 py-1 text-xs font-semibold text-slate-500 uppercase tracking-wider mt-3 mb-1">
                  {item.label}
                </p>
                {item.children.map((child) => {
                  const active = path === child.href || path.startsWith(child.href);
                  const Icon = child.icon;
                  return (
                    <Link
                      key={child.href}
                      href={child.href}
                      className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors ${
                        active
                          ? "bg-sky-500/20 text-sky-400 font-medium"
                          : "text-slate-400 hover:bg-slate-700/50 hover:text-slate-200"
                      }`}
                    >
                      <Icon size={16} />
                      {child.label}
                      {active && <ChevronRight size={12} className="ml-auto" />}
                    </Link>
                  );
                })}
              </div>
            );
          }

          const active = path === item.href;
          const Icon = item.icon;
          return (
            <Link
              key={item.href || item.label}
              href={item.href || "#"}
              className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors ${
                item.disabled
                  ? "text-slate-600 cursor-not-allowed"
                  : active
                  ? "bg-sky-500/20 text-sky-400 font-medium"
                  : "text-slate-400 hover:bg-slate-700/50 hover:text-slate-200"
              }`}
            >
              <Icon size={16} />
              {item.label}
              {item.badge && (
                <span className="ml-auto text-[10px] bg-slate-700 text-slate-400 px-1.5 py-0.5 rounded">
                  {item.badge}
                </span>
              )}
            </Link>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-6 py-4 border-t border-[#334155] text-xs text-slate-500">
        v1.0.0 — Plataforma DINA
      </div>
    </aside>
  );
}
