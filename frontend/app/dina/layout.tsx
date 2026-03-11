"use client";

import { useEffect, useState } from "react";
import Sidebar from "@/components/Sidebar";
import api from "@/lib/api";
import { useRouter, usePathname } from "next/navigation";
import Link from "next/link";

const TABS = [
  { label: "📈 Mediciones",   href: "/dina/mediciones"   },
  { label: "📊 Estadísticas", href: "/dina/estadisticas" },
  { label: "🗺️ Mapa",         href: "/dina/mapa"         },
  { label: "🤖 Diagnósticos", href: "/dina/diagnosticos" },
];

export default function DinaLayout({ children }: { children: React.ReactNode }) {
  const [pozos, setPozos] = useState<string[]>([]);
  const [pozo, setPozo] = useState<string>("");
  const [busqueda, setBusqueda] = useState<string>("");
  const [abierto, setAbierto] = useState(false);
  const pathname = usePathname();

  useEffect(() => {
    api.getPozos().then((r) => {
      setPozos(r.pozos);
      if (r.pozos.length > 0) {
        const stored = sessionStorage.getItem("dina_pozo_sel");
        setPozo(stored && r.pozos.includes(stored) ? stored : r.pozos[0]);
      }
    }).catch(() => {});
  }, []);

  const handlePozo = (p: string) => {
    setPozo(p);
    setBusqueda("");
    setAbierto(false);
    sessionStorage.setItem("dina_pozo_sel", p);
    window.dispatchEvent(new CustomEvent("dina:pozo", { detail: p }));
  };

  const pozosFiltrados = busqueda
    ? pozos.filter(p => p.toLowerCase().includes(busqueda.toLowerCase()))
    : pozos;

  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Header con selector de pozo */}
        <div className="border-b border-[#334155] bg-[#1e293b]/50 px-6 py-3 flex items-center gap-4 flex-wrap">
          <span className="text-sm text-slate-400 whitespace-nowrap font-semibold tracking-wide">POZO</span>
          <div className="relative">
            <input
              type="text"
              value={abierto ? busqueda : pozo}
              onChange={(e) => { setBusqueda(e.target.value); setAbierto(true); }}
              onFocus={() => { setBusqueda(""); setAbierto(true); }}
              onBlur={() => setTimeout(() => setAbierto(false), 150)}
              placeholder="Buscar pozo..."
              className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 w-56 focus:outline-none focus:border-sky-500"
            />
            {abierto && pozosFiltrados.length > 0 && (
              <div className="absolute z-50 top-full left-0 mt-1 w-56 max-h-64 overflow-y-auto bg-[#1e293b] border border-[#334155] rounded shadow-xl">
                {pozosFiltrados.map((p) => (
                  <div
                    key={p}
                    onMouseDown={() => handlePozo(p)}
                    className={"px-3 py-1.5 text-sm cursor-pointer " + (p === pozo ? "bg-sky-500/20 text-sky-400" : "text-slate-200 hover:bg-[#334155]")}
                  >
                    {p}
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Tabs */}
          <div className="flex gap-1 ml-2 flex-wrap">
            {TABS.map((tab) => {
              const active = pathname === tab.href;
              return (
                <Link
                  key={tab.href}
                  href={tab.href}
                  className={`px-3 py-1.5 rounded text-sm transition-colors ${
                    active
                      ? "bg-sky-500/20 text-sky-400 font-medium"
                      : "text-slate-400 hover:text-slate-200 hover:bg-slate-700/50"
                  }`}
                >
                  {tab.label}
                </Link>
              );
            })}
          </div>
        </div>

        {/* Contenido */}
        <main className="flex-1 p-6 overflow-auto min-w-0">
          {children}
        </main>
      </div>
    </div>
  );
}
