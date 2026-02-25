"use client";

import { useState, useEffect, useMemo } from "react";
import api, { PuntoMapa } from "@/lib/api";

interface TablaValidacionesProps {
  pozos: PuntoMapa[];
}

interface RowState {
  NO_key: string;
  nivel_5?: string;
  ORIGEN?: string;
  DT_plot_str?: string;
  Dias_desde_ultima?: number | null;
  Sumergencia?: number | null;
  PE?: number | null;
  PB?: number | null;
  NM?: number | null;
  NC?: number | null;
  ND?: number | null;
  lat?: number;
  lon?: number;
  valida: boolean;
  comentario: string;
  usuario: string;
  fecha_key: string;
}

function pozosToRows(pozos: PuntoMapa[]): RowState[] {
  return pozos.map((p) => ({
    ...p,
    valida: true,
    comentario: "",
    usuario: "",
    fecha_key: p.DT_plot_str || "",
  }));
}

const HEADERS: { key: keyof RowState | "_valida" | "_comentario"; label: string }[] = [
  { key: "_valida",           label: "✅ Válida" },
  { key: "NO_key",            label: "Pozo" },
  { key: "nivel_5",           label: "Batería" },
  { key: "ORIGEN",            label: "Origen" },
  { key: "DT_plot_str",       label: "Fecha" },
  { key: "Dias_desde_ultima", label: "Días" },
  { key: "Sumergencia",       label: "Sumergencia" },
  { key: "PB",                label: "PB" },
  { key: "NM",                label: "NM" },
  { key: "NC",                label: "NC" },
  { key: "ND",                label: "ND" },
  { key: "_comentario",       label: "Comentario" },
];

export default function TablaValidaciones({ pozos }: TablaValidacionesProps) {
  const [rows, setRows]     = useState<RowState[]>(() => pozosToRows(pozos));
  const [saving, setSaving] = useState<Record<number, boolean>>({});
  const [saved, setSaved]   = useState<Record<number, boolean>>({});
  const [usuario, setUsuario] = useState("");
  const [editIdx, setEditIdx] = useState<number | null>(null);
  const [editComentario, setEditComentario] = useState("");
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  useEffect(() => {
    setRows(pozosToRows(pozos));
    setEditIdx(null);
  }, [pozos]);

  const sorted = useMemo(() => {
    if (!sortKey || sortKey === "_valida" || sortKey === "_comentario") return rows;
    return [...rows].sort((a, b) => {
      const av = (a as any)[sortKey];
      const bv = (b as any)[sortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number") return sortAsc ? av - bv : bv - av;
      return sortAsc ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
    });
  }, [rows, sortKey, sortAsc]);

  function handleSort(key: string) {
    if (key === "_valida" || key === "_comentario") return;
    if (sortKey === key) setSortAsc((a) => !a);
    else { setSortKey(key); setSortAsc(true); }
  }

  const handleCheckbox = async (idx: number, valida: boolean) => {
    const r = sorted[idx];
    const realIdx = rows.indexOf(r);
    setRows((prev) => { const n = [...prev]; n[realIdx] = { ...n[realIdx], valida }; return n; });
    setSaving((p) => ({ ...p, [idx]: true }));
    try {
      await api.saveValidacion(r.NO_key, {
        fecha_key: r.fecha_key, validada: valida,
        comentario: r.comentario, usuario: usuario || "anónimo",
      });
      setSaved((p) => ({ ...p, [idx]: true }));
      setTimeout(() => setSaved((p) => ({ ...p, [idx]: false })), 2000);
    } catch {}
    setSaving((p) => ({ ...p, [idx]: false }));
  };

  const handleGuardarComentario = async (idx: number) => {
    const r = sorted[idx];
    const realIdx = rows.indexOf(r);
    setSaving((p) => ({ ...p, [idx]: true }));
    try {
      await api.saveValidacion(r.NO_key, {
        fecha_key: r.fecha_key, validada: r.valida,
        comentario: editComentario, usuario: usuario || "anónimo",
      });
      setRows((prev) => { const n = [...prev]; n[realIdx] = { ...n[realIdx], comentario: editComentario }; return n; });
      setSaved((p) => ({ ...p, [idx]: true }));
      setTimeout(() => setSaved((p) => ({ ...p, [idx]: false })), 2000);
    } catch {}
    setSaving((p) => ({ ...p, [idx]: false }));
    setEditIdx(null);
  };

  const exportCSV = () => {
    const cols = ["NO_key","nivel_5","ORIGEN","DT_plot_str","Sumergencia","PB","NM","NC","ND","valida","comentario"];
    const header = cols.join(",");
    const body = rows.map((r) => cols.map((c) => JSON.stringify((r as any)[c] ?? "")).join(","));
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([[header, ...body].join("\n")], { type: "text/csv" }));
    a.download = "pozos_sumergencia.csv";
    a.click();
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <label className="text-xs text-slate-400 whitespace-nowrap">Tu nombre:</label>
        <input type="text" placeholder="ej: jperez" value={usuario} onChange={(e) => setUsuario(e.target.value)}
          className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 placeholder-slate-600 w-40" />
        {sortKey && (
          <button onClick={() => { setSortKey(null); setSortAsc(true); }}
            className="text-xs px-2 py-1 rounded border border-slate-600 text-slate-400 hover:border-red-400 hover:text-red-400 transition-colors ml-2">
            ✕ orden
          </button>
        )}
        <span className="text-xs text-slate-500 ml-auto">{rows.length} pozos</span>
      </div>

      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto max-h-[480px] overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 z-10">
              <tr>
                {HEADERS.map((h) => (
                  <th key={h.key}
                    onClick={() => handleSort(h.key)}
                    className={`text-left text-xs text-slate-500 px-3 py-2.5 bg-[#1e293b] border-b border-[#334155] whitespace-nowrap select-none
                      ${h.key !== "_valida" && h.key !== "_comentario" ? "cursor-pointer hover:text-sky-400 transition-colors" : ""}`}>
                    {h.label}
                    {sortKey === h.key ? (sortAsc ? " ▲" : " ▼") : (h.key !== "_valida" && h.key !== "_comentario" ? " ↕" : "")}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sorted.map((r, i) => (
                <tr key={`${r.NO_key}-${i}`} className="border-b border-[#334155] hover:bg-slate-800/40">
                  <td className="px-3 py-2 text-center">
                    {saving[i] ? <span className="text-xs text-slate-500">…</span>
                    : saved[i]  ? <span className="text-green-400 text-xs">✓</span>
                    : <input type="checkbox" checked={r.valida} onChange={(e) => handleCheckbox(i, e.target.checked)} className="accent-sky-400 w-4 h-4 cursor-pointer" />}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-slate-300">{r.NO_key}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.nivel_5 || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.ORIGEN || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.DT_plot_str || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.Dias_desde_ultima != null ? r.Dias_desde_ultima.toFixed(0) : "—"}</td>
                  <td className="px-3 py-2 text-xs font-semibold text-sky-300">{r.Sumergencia != null ? r.Sumergencia.toFixed(1) : "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.PB ?? "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.NM ?? "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.NC ?? "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.ND ?? "—"}</td>
                  <td className="px-3 py-2 text-xs min-w-[180px]">
                    {editIdx === i ? (
                      <div className="flex gap-1">
                        <input autoFocus type="text" value={editComentario} onChange={(e) => setEditComentario(e.target.value)}
                          onKeyDown={(e) => { if (e.key === "Enter") handleGuardarComentario(i); if (e.key === "Escape") setEditIdx(null); }}
                          className="bg-[#0f172a] border border-sky-500 rounded px-2 py-0.5 text-xs text-slate-200 w-36" />
                        <button onClick={() => handleGuardarComentario(i)} className="text-green-400 hover:text-green-300 text-xs font-bold">✓</button>
                        <button onClick={() => setEditIdx(null)} className="text-slate-500 hover:text-slate-300 text-xs">✕</button>
                      </div>
                    ) : (
                      <button onClick={() => { setEditIdx(i); setEditComentario(r.comentario); }}
                        className="text-left w-full group" title="Clic para editar comentario">
                        {r.comentario
                          ? <span className="text-amber-300">{r.comentario}</span>
                          : <span className="text-slate-600 group-hover:text-slate-400 italic">+ agregar</span>}
                      </button>
                    )}
                  </td>
                </tr>
              ))}
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
