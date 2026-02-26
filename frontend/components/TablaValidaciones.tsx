"use client";

import { useState, useEffect, useMemo } from "react";
import api, { PuntoMapa, FilaValidacion } from "@/lib/api";

interface TablaValidacionesProps {
  pozos: PuntoMapa[];
}

const SORT_COLS: { key: keyof FilaValidacion; label: string }[] = [
  { key: "pozo",              label: "Pozo" },
  { key: "bateria",           label: "Batería" },
  { key: "fecha_medicion",    label: "Fecha" },
  { key: "Dias_desde_ultima", label: "Días" },
  { key: "sumergencia_m",     label: "Sumergencia" },
  { key: "base",              label: "Base" },
  { key: "comentario",        label: "Comentario" },
];

export default function TablaValidaciones({ pozos }: TablaValidacionesProps) {
  const [filas, setFilas]       = useState<FilaValidacion[]>([]);
  const [loading, setLoading]   = useState(false);
  const [saving, setSaving]     = useState<Record<number, boolean>>({});
  const [saved, setSaved]       = useState<Record<number, boolean>>({});
  const [usuario, setUsuario]   = useState("");
  const [editIdx, setEditIdx]   = useState<number | null>(null);
  const [editComentario, setEditComentario] = useState("");
  const [sortKey, setSortKey]   = useState<keyof FilaValidacion | null>(null);
  const [sortAsc, setSortAsc]   = useState(true);

  // Cargar validaciones desde el backend al montar o cuando cambian los pozos
  useEffect(() => {
    if (!pozos.length) return;
    setLoading(true);
    setEditIdx(null);
    api.getTablaValidaciones()
      .then((res) => {
        // Filtrar solo los pozos que están en la lista actual
        const noKeys = new Set(pozos.map((p) => p.NO_key));
        const filtradas = res.filas.filter((f) => noKeys.has(f.pozo));
        setFilas(filtradas);
      })
      .catch(() => {
        // Fallback: construir filas básicas desde los puntos sin comentarios
        setFilas(pozos.map((p) => ({
          validada:         true,
          pozo:             p.NO_key,
          bateria:          p.nivel_5 || "",
          fecha_medicion:   p.DT_plot_str || "",
          sumergencia_m:    p.Sumergencia ?? null,
          base:             "",
          comentario:       "",
          usuario:          "",
          _no_key:          p.NO_key,
          _fecha_key:       p.DT_plot_str || "",
          lat:              p.lat,
          lon:              p.lon,
          Dias_desde_ultima: p.Dias_desde_ultima ?? null,
        })));
      })
      .finally(() => setLoading(false));
  }, [pozos]);

  const sorted = useMemo(() => {
    if (!sortKey) return filas;
    return [...filas].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number")
        return sortAsc ? av - bv : bv - av;
      return sortAsc
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
  }, [filas, sortKey, sortAsc]);

  function handleSort(key: keyof FilaValidacion) {
    if (sortKey === key) setSortAsc((a) => !a);
    else { setSortKey(key); setSortAsc(true); }
  }

  const handleCheckbox = async (sortedIdx: number, valida: boolean) => {
    const r = sorted[sortedIdx];
    const realIdx = filas.indexOf(r);
    // Actualizar local inmediatamente
    setFilas((prev) => {
      const n = [...prev];
      n[realIdx] = { ...n[realIdx], validada: valida };
      return n;
    });
    setSaving((p) => ({ ...p, [sortedIdx]: true }));
    try {
      await api.saveValidacion(r._no_key, {
        fecha_key: r._fecha_key,
        validada:  valida,
        comentario: r.comentario,
        usuario:   usuario || "anónimo",
      });
      setSaved((p) => ({ ...p, [sortedIdx]: true }));
      setTimeout(() => setSaved((p) => ({ ...p, [sortedIdx]: false })), 2000);
    } catch {}
    setSaving((p) => ({ ...p, [sortedIdx]: false }));
  };

  const handleGuardarComentario = async (sortedIdx: number) => {
    const r = sorted[sortedIdx];
    const realIdx = filas.indexOf(r);
    setSaving((p) => ({ ...p, [sortedIdx]: true }));
    try {
      await api.saveValidacion(r._no_key, {
        fecha_key:  r._fecha_key,
        validada:   r.validada,
        comentario: editComentario,
        usuario:    usuario || "anónimo",
      });
      // Actualizar local con el comentario guardado
      setFilas((prev) => {
        const n = [...prev];
        n[realIdx] = { ...n[realIdx], comentario: editComentario };
        return n;
      });
      setSaved((p) => ({ ...p, [sortedIdx]: true }));
      setTimeout(() => setSaved((p) => ({ ...p, [sortedIdx]: false })), 2000);
    } catch {}
    setSaving((p) => ({ ...p, [sortedIdx]: false }));
    setEditIdx(null);
  };

  const exportCSV = () => {
    const cols = ["pozo","bateria","fecha_medicion","Dias_desde_ultima","sumergencia_m","base","validada","comentario","usuario"];
    const header = cols.join(",");
    const body = filas.map((r) =>
      cols.map((c) => JSON.stringify((r as any)[c] ?? "")).join(",")
    );
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([[header, ...body].join("\n")], { type: "text/csv" }));
    a.download = "pozos_validaciones.csv";
    a.click();
  };

  if (loading) {
    return <p className="text-slate-500 text-sm animate-pulse">Cargando validaciones…</p>;
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <label className="text-xs text-slate-400 whitespace-nowrap">Tu nombre:</label>
        <input
          type="text" placeholder="ej: jperez" value={usuario}
          onChange={(e) => setUsuario(e.target.value)}
          className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 placeholder-slate-600 w-40"
        />
        {sortKey && (
          <button
            onClick={() => { setSortKey(null); setSortAsc(true); }}
            className="text-xs px-2 py-1 rounded border border-slate-600 text-slate-400 hover:border-red-400 hover:text-red-400 transition-colors">
            ✕ orden
          </button>
        )}
        <span className="text-xs text-slate-500 ml-auto">{filas.length} pozos</span>
      </div>

      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto max-h-[480px] overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 z-10">
              <tr>
                <th className="text-left text-xs text-slate-500 px-3 py-2.5 bg-[#1e293b] border-b border-[#334155] whitespace-nowrap">
                  ✅ Válida
                </th>
                {SORT_COLS.map((c) => (
                  <th key={c.key} onClick={() => handleSort(c.key)}
                    className="text-left text-xs text-slate-500 px-3 py-2.5 bg-[#1e293b] border-b border-[#334155] whitespace-nowrap cursor-pointer select-none hover:text-sky-400 transition-colors">
                    {c.label}{sortKey === c.key ? (sortAsc ? " ▲" : " ▼") : " ↕"}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sorted.map((r, i) => (
                <tr key={`${r._no_key}-${i}`} className="border-b border-[#334155] hover:bg-slate-800/40">
                  <td className="px-3 py-2 text-center">
                    {saving[i] ? <span className="text-xs text-slate-500">…</span>
                    : saved[i]  ? <span className="text-green-400 text-xs">✓</span>
                    : <input type="checkbox" checked={r.validada}
                        onChange={(e) => handleCheckbox(i, e.target.checked)}
                        className="accent-sky-400 w-4 h-4 cursor-pointer" />}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-slate-300">{r.pozo}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.bateria || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.fecha_medicion || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">
                    {r.Dias_desde_ultima != null ? Number(r.Dias_desde_ultima).toFixed(0) : "—"}
                  </td>
                  <td className="px-3 py-2 text-xs font-semibold text-sky-300">
                    {r.sumergencia_m != null ? r.sumergencia_m.toFixed(1) : "—"}
                  </td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.base || "—"}</td>
                  <td className="px-3 py-2 text-xs min-w-[180px]">
                    {editIdx === i ? (
                      <div className="flex gap-1">
                        <input autoFocus type="text" value={editComentario}
                          onChange={(e) => setEditComentario(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") handleGuardarComentario(i);
                            if (e.key === "Escape") setEditIdx(null);
                          }}
                          className="bg-[#0f172a] border border-sky-500 rounded px-2 py-0.5 text-xs text-slate-200 w-36" />
                        <button onClick={() => handleGuardarComentario(i)}
                          className="text-green-400 hover:text-green-300 text-xs font-bold">✓</button>
                        <button onClick={() => setEditIdx(null)}
                          className="text-slate-500 hover:text-slate-300 text-xs">✕</button>
                      </div>
                    ) : (
                      <button
                        onClick={() => { setEditIdx(i); setEditComentario(r.comentario); }}
                        className="text-left w-full group" title="Clic para editar comentario">
                        {r.comentario
                          ? <span className="text-amber-300">{r.comentario}</span>
                          : <span className="text-slate-600 group-hover:text-slate-400 italic">+ agregar</span>}
                      </button>
                    )}
                  </td>
                </tr>
              ))}
              {sorted.length === 0 && (
                <tr>
                  <td colSpan={8} className="px-3 py-8 text-center text-slate-500 text-xs">
                    Sin pozos para mostrar.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="flex gap-3">
        <button onClick={exportCSV}
          className="text-xs px-3 py-1.5 border border-slate-600 rounded text-slate-400 hover:border-sky-400 hover:text-sky-400 transition-colors">
          ⬇️ Exportar CSV (incluye comentarios)
        </button>
      </div>
    </div>
  );
}
