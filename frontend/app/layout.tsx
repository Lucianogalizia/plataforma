"use client";

import { useEffect, useMemo, useState } from "react";
import Sidebar from "@/components/Sidebar";
import api from "@/lib/api";
import { usePathname } from "next/navigation";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";

const TABS = [
  { label: "📈 Mediciones",   href: "/dina/mediciones"   },
  { label: "📊 Estadísticas", href: "/dina/estadisticas" },
  { label: "🗺️ Mapa",         href: "/dina/mapa"         },
  { label: "🤖 Diagnósticos", href: "/dina/diagnosticos" },
];

export default function DinaLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  const pozosQ = useQuery({
    queryKey: ["pozos"],
    queryFn: () => api.getPozos(),
  });

  const pozos = useMemo(() => pozosQ.data?.pozos ?? [], [pozosQ.data]);

  const [pozo, setPozo] = useState<string>("");

  // Inicializar selección una sola vez cuando llegan pozos
  useEffect(() => {
    if (!pozos.length) return;

    const stored = sessionStorage.getItem("dina_pozo_sel");
    const sel = stored && pozos.includes(stored) ? stored : pozos[0];

    setPozo(sel);
    // avisar a páginas si ya están escuchando
    window.dispatchEvent(new CustomEvent("dina:pozo", { detail: sel }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pozos.length]);

  const handlePozo = (p: string) => {
    setPozo(p);
    sessionStorage.setItem("dina_pozo_sel", p);
    window.dispatchEvent(new CustomEvent("dina:pozo", { detail: p }));
  };

  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Header con selector de pozo */}
        <div className="border-b border-[#334155] bg-[#1e293b]/50 px-6 py-3 flex items-center gap-4 flex-wrap">
          <span className="text-sm text-slate-400 whitespace-nowrap">Pozo (NO=):</span>

          <select
            value={pozo}
            onChange={(e) => handlePozo(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 w-56"
            disabled={pozosQ.isLoading || pozos.length === 0}
          >
            {pozos.map((p) => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>

          {pozosQ.isLoading && (
            <span className="text-xs text-slate-400">Cargando pozos…</span>
          )}

          {pozosQ.isError && (
            <span className="text-xs text-red-300">No se pudieron cargar los pozos.</span>
          )}

          {/* Tabs */}
          <div className="flex gap-2 ml-auto flex-wrap">
            {TABS.map((t) => {
              const active = pathname?.startsWith(t.href);
              return (
                <Link
                  key={t.href}
                  href={t.href}
                  className={[
                    "px-3 py-1.5 rounded text-sm border",
                    active
                      ? "bg-slate-200/10 border-slate-500 text-slate-100"
                      : "bg-transparent border-[#334155] text-slate-300 hover:bg-slate-200/5",
                  ].join(" ")}
                >
                  {t.label}
                </Link>
              );
            })}
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 min-w-0 overflow-auto">{children}</div>
      </div>
    </div>
  );
}
