"use client";

import { useState, useRef } from "react";
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

export default function TablaValidaciones({ pozos }: TablaValidacionesProps) {
  const [rows, setRows] = useState<RowState[]>(() =>
    pozos.map((p) => ({
      ...p,
      valida: true,
      comentario: "",
      usuario: "",
      fecha_key: p.DT_plot_str || "",
    }))
  );
  const [saving, setSaving] = useState<Record<number, boolean>>({});
  const [saved, setSaved] = useState<Record<number, boolean>>({});
  const [usuario, setUsuario] = useState("");
  const [pozo_form, setPozoForm] = useState("");
  const [fecha_form, setFechaForm] = useState("");
  const [valida_form, setValidaForm] = useState(true);
  const [comentario_form, setComentarioForm] = useState("");
  const [guardandoForm, setGuardandoForm] = useState(false);
  const [okForm, setOkForm] = useState(false);

  const handleCheckbox = async (idx: number, valida: boolean) => {
    const r = rows[idx];
    setRows((prev) => {
      const n = [...prev];
      n[idx] = { ...n[idx], valida };
      return n;
    });
    setSaving((p) => ({ ...p, [idx]: true }));
    try {
      await api.saveValidacion(r.NO_key, {
        fecha_key: r.fecha_key,
        validada: valida,
        comentario: r.comentario,
        usuario: usuario || "anónimo",
      });
      setSaved((p) => ({ ...p, [idx]: true }));
      setTimeout(() => setSaved((p) => ({ ...p, [idx]: false })), 2000);
    } catch {
      /* silently ignore */
    }
    setSaving((p) => ({ ...p, [idx]: false }));
  };

  const handleGuardarForm = async () => {
    if (!pozo_form) return;
    setGuardandoForm(true);
    try {
      await api.saveValidacion(pozo_form, {
        fecha_key: fecha_form,
        validada: valida_form,
        comentario: comentario_form,
        usuario: usuario || "anónimo",
      });
      setOkForm(true);
      setTimeout(() => setOkForm(false), 3000);
    } catch {/* ignore */}
    setGuardandoForm(false);
  };

  const exportCSV = () => {
    const cols = ["NO_key", "nivel_5", "ORIGEN", "DT_plot_str", "Sumergencia", "PB", "NM", "NC", "ND", "valida", "comentario"];
    const header = cols.join(",");
    const body = rows.map((r) =>
      cols.map((c) => JSON.stringify((r as Record<string, unknown>)[c] ?? "")).join(",")
    );
    const csv = [header, ...body].join("\n");
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
    a.download = "pozos_sumergencia.csv";
    a.click();
  };

  const pozoOpts = [...new Set(rows.map((r) => r.NO_key))];
  const fechaOpts = rows
    .filter((r) => r.NO_key === pozo_form)
    .map((r) => r.fecha_key)
    .filter(Boolean);

  return (
    <div className="space-y-4">
      {/* Usuario global */}
      <div className="flex items-center gap-3">
        <label className="text-xs text-slate-400 whitespace-nowrap">Tu nombre:</label>
        <input
          type="text"
          placeholder="ej: jperez"
          value={usuario}
          onChange={(e) => setUsuario(e.target.value)}
          className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 placeholder-slate-600 w-40"
        />
      </div>

      {/* Tabla */}
      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto max-h-[480px] overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 z-10">
              <tr>
                {["✅ Válida", "Pozo", "Batería", "Origen", "Fecha", "Días", "Sumergencia", "PB", "NM", "NC", "ND"].map((h) => (
                  <th key={h} className="text-left text-xs text-slate-500 px-3 py-2.5 bg-[#1e293b] border-b border-[#334155] whitespace-nowrap">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i} className="border-b border-[#334155] hover:bg-slate-800/40">
                  <td className="px-3 py-2 text-center">
                    {saving[i] ? (
                      <span className="text-xs text-slate-500">…</span>
                    ) : saved[i] ? (
                      <span className="text-green-400 text-xs">✓</span>
                    ) : (
                      <input
                        type="checkbox"
                        checked={r.valida}
                        onChange={(e) => handleCheckbox(i, e.target.checked)}
                        className="accent-sky-400 w-4 h-4 cursor-pointer"
                      />
                    )}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-slate-300">{r.NO_key}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.nivel_5 || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.ORIGEN || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.DT_plot_str || "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">
                    {r.Dias_desde_ultima != null ? r.Dias_desde_ultima.toFixed(0) : "—"}
                  </td>
                  <td className="px-3 py-2 text-xs font-semibold text-sky-300">
                    {r.Sumergencia != null ? r.Sumergencia.toFixed(1) : "—"}
                  </td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.PB ?? "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.NM ?? "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.NC ?? "—"}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{r.ND ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <p className="text-xs text-slate-500">Total: {rows.length} pozos</p>

      {/* Formulario de comentario */}
      <div className="card space-y-3">
        <h4 className="text-sm font-medium text-slate-300">💬 Agregar / editar comentario</h4>
        <div className="grid grid-cols-2 gap-3">
          <select
            value={pozo_form}
            onChange={(e) => { setPozoForm(e.target.value); setFechaForm(""); }}
            className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 col-span-1"
          >
            <option value="">— Elegir pozo —</option>
            {pozoOpts.map((p) => <option key={p} value={p}>{p}</option>)}
          </select>
          <select
            value={fecha_form}
            onChange={(e) => setFechaForm(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200"
            disabled={!pozo_form}
          >
            <option value="">— Fecha —</option>
            {fechaOpts.map((f) => <option key={f} value={f}>{f}</option>)}
          </select>
        </div>
        <div className="grid grid-cols-3 gap-3">
          <label className="flex items-center gap-2 text-sm text-slate-300">
            <input
              type="checkbox"
              checked={valida_form}
              onChange={(e) => setValidaForm(e.target.checked)}
              className="accent-sky-400 w-4 h-4"
            />
            Válida
          </label>
          <input
            type="text"
            placeholder="Comentario"
            value={comentario_form}
            onChange={(e) => setComentarioForm(e.target.value)}
            className="bg-[#0f172a] border border-[#334155] rounded px-3 py-1.5 text-sm text-slate-200 col-span-2"
          />
        </div>
        <div className="flex gap-3 items-center">
          <button
            onClick={handleGuardarForm}
            disabled={!pozo_form || guardandoForm}
            className="px-4 py-2 bg-sky-600 hover:bg-sky-500 disabled:opacity-40 rounded text-sm font-medium text-white transition-colors"
          >
            {guardandoForm ? "Guardando…" : "💾 Guardar comentario"}
          </button>
          {okForm && <span className="text-green-400 text-sm">✅ Guardado</span>}
        </div>
      </div>

      {/* Export */}
      <div className="flex gap-3">
        <button
          onClick={exportCSV}
          className="text-xs px-3 py-1.5 border border-slate-600 rounded text-slate-400 hover:border-sky-400 hover:text-sky-400 transition-colors"
        >
          ⬇️ Exportar CSV
        </button>
      </div>
    </div>
  );
}
