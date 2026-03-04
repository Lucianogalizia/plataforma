"use client";

import { useEffect, useState, useCallback, useMemo } from "react";
import {
  Plus, X, Pencil, Trash2, ChevronDown, ChevronUp,
  Search, Filter, CheckCircle2, Clock, Loader2,
  Wrench, Calendar, Layers, AlertCircle,
  ArrowUpDown, ArrowUp, ArrowDown,
  Download, List, FolderOpen,
} from "lucide-react";

// ==========================================================
// Types
// ==========================================================

interface PozoItem { nombre_pozo: string; bateria: string; }

interface Accion {
  id: string;
  nombre_pozo: string;
  bateria: string;
  sist_extraccion: string;
  fecha_accion: string;
  fecha_realizacion: string | null;
  fecha_fin: string | null;
  tipo: string;
  tipo_accion: string;
  recurso: string;
  neta_incremental: number;
  bruta_incremental: number;
  inyeccion: number;
  accion: string;
  estado: "EN PROCESO" | "FINALIZADO";
  creado_utc: string;
  modificado_utc: string;
}

interface FormData {
  nombre_pozo: string; bateria: string; sist_extraccion: string;
  fecha_accion: string; fecha_realizacion: string;
  fecha_fin: string; tipo: string; tipo_accion: string;
  recurso: string; neta_incremental: string; bruta_incremental: string;
  inyeccion: string; accion: string;
}

interface GrupoPozo {
  nombre_pozo: string;
  bateria: string;
  acciones: Accion[];
  en_proceso: number;
  finalizadas: number;
}

type SortKey = "nombre_pozo" | "bateria" | "sist_extraccion" | "tipo" | "fecha_accion" | "estado";
type SortDir = "asc" | "desc";
type Vista = "plana" | "agrupada";

const SIST_EXTRACCION = ["AIB", "BES", "PCP", "SWABBING", "SURGENTE", "OTRO"];
const TIPOS = ["Superficie", "Fondo"];
const TIPOS_ACCION = ["Optimización", "Operativa"];
const RECURSOS = ["eléctricos", "Grúa", "Operador BES", "Operador PCP", "Pulling", "WO", "químicos", "CT"];
const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// --- Caché local para GETs (evita requests duplicados) ---
const _getCache = new Map<string, { ts: number; data: unknown }>();
const _inflight = new Map<string, Promise<unknown>>();
const _GET_TTL = 3 * 60 * 1000; // 3 minutos

async function cachedGet<T>(url: string): Promise<T> {
  const now = Date.now();
  const hit = _getCache.get(url);
  if (hit && now - hit.ts < _GET_TTL) return hit.data as T;
  const inf = _inflight.get(url);
  if (inf) return inf as Promise<T>;
  const p: Promise<T> = fetch(url).then(r => {
    if (!r.ok) throw new Error(`Error ${r.status}`);
    return r.json() as Promise<T>;
  }).then(data => {
    _getCache.set(url, { ts: Date.now(), data });
    return data;
  }).finally(() => _inflight.delete(url));
  _inflight.set(url, p as Promise<unknown>);
  return p;
}

function clearAccionesCache() { _getCache.clear(); }
const FORM_EMPTY: FormData = {
  nombre_pozo: "", bateria: "", sist_extraccion: "",
  fecha_accion: "", fecha_realizacion: "", fecha_fin: "", tipo: "",
  tipo_accion: "", recurso: "", neta_incremental: "", bruta_incremental: "",
  inyeccion: "", accion: "",
};

// ==========================================================
// Helpers
// ==========================================================

function fmt(d: string | null | undefined) { return d ? d.slice(0, 10) : "—"; }

function estadoBadge(estado: string) {
  if (estado === "FINALIZADO")
    return (
      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-emerald-500/15 text-emerald-400 border border-emerald-500/25">
        <CheckCircle2 size={11} />FINALIZADO
      </span>
    );
  return (
    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-amber-500/15 text-amber-400 border border-amber-500/25">
      <Clock size={11} />EN PROCESO
    </span>
  );
}

function tipoBadge(tipo: string) {
  const isSup = tipo === "Superficie";
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium border ${isSup ? "bg-sky-500/10 text-sky-400 border-sky-500/20" : "bg-violet-500/10 text-violet-400 border-violet-500/20"}`}>
      <Layers size={10} />{tipo}
    </span>
  );
}

function exportCSV(acciones: Accion[], scope: "visible" | "all", allAcciones: Accion[]) {
  const data = scope === "all" ? allAcciones : acciones;
  const headers = ["Pozo","Bateria","Sist.Extraccion","Tipo","Tipo Accion","Recurso","Neta Incremental","Bruta Incremental","Inyeccion","Fecha Accion","Fecha Realizacion","Fecha Fin","Estado","Descripcion"];
  const rows = data.map(a => [
    a.nombre_pozo, a.bateria, a.sist_extraccion, a.tipo,
    a.tipo_accion, a.recurso, a.neta_incremental ?? "", a.bruta_incremental ?? "", a.inyeccion ?? "",
    fmt(a.fecha_accion), fmt(a.fecha_realizacion), fmt(a.fecha_fin),
    a.estado, '"' + (a.accion || "").replace(/"/g, '""') + '"'
  ].join(","));
  const csv = [headers.join(","), ...rows].join("\n");
  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "acciones_" + new Date().toISOString().slice(0, 10) + ".csv";
  a.click();
  URL.revokeObjectURL(url);
}

// ==========================================================
// Modal
// ==========================================================

function Modal({ open, onClose, title, children }: {
  open: boolean; onClose: () => void; title: string; children: React.ReactNode;
}) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ background: "rgba(0,0,0,0.7)", backdropFilter: "blur(4px)" }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="bg-[#1e293b] border border-[#334155] rounded-2xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155]">
          <h2 className="text-base font-semibold text-slate-100 flex items-center gap-2">
            <Wrench size={16} className="text-sky-400" />{title}
          </h2>
          <button onClick={onClose} className="text-slate-500 hover:text-slate-300 rounded-lg p-1 hover:bg-slate-700">
            <X size={18} />
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}

// ==========================================================
// Export Modal
// ==========================================================

function ExportModal({ open, onClose, onExport }: {
  open: boolean; onClose: () => void; onExport: (scope: "visible" | "all") => void;
}) {
  return (
    <Modal open={open} onClose={onClose} title="Exportar a CSV">
      <div className="px-6 py-5 space-y-4">
        <p className="text-sm text-slate-400">Que querés exportar?</p>
        <div className="grid grid-cols-2 gap-3">
          {([
            { scope: "visible" as const, label: "Solo visible", desc: "Lo que se muestra con los filtros aplicados", icon: Filter },
            { scope: "all"     as const, label: "Todo",         desc: "Todas las acciones sin importar los filtros", icon: Download },
          ]).map(({ scope, label, desc, icon: Icon }) => (
            <button key={scope} onClick={() => { onExport(scope); onClose(); }}
              className="flex flex-col items-start gap-2 p-4 bg-[#0f172a] border border-[#334155] rounded-xl hover:border-sky-500/50 hover:bg-sky-500/5 transition-all text-left">
              <div className="flex items-center gap-2">
                <Icon size={15} className="text-sky-400" />
                <span className="text-sm font-semibold text-slate-200">{label}</span>
              </div>
              <p className="text-xs text-slate-500">{desc}</p>
            </button>
          ))}
        </div>
        <div className="flex justify-end pt-2">
          <button onClick={onClose} className="px-4 py-2 rounded-lg text-sm text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors">
            Cancelar
          </button>
        </div>
      </div>
    </Modal>
  );
}

// ==========================================================
// Pozo Combobox
// ==========================================================

function PozoCombobox({ value, onChange, pozos }: {
  value: string; onChange: (val: string, bateria: string) => void; pozos: PozoItem[];
}) {
  const [query, setQuery] = useState(value);
  const [open, setOpen]   = useState(false);

  const filtered = useMemo(() => {
    const q = query.toLowerCase();
    if (!q.trim()) return pozos.slice(0, 80);
    return pozos.filter(p => p.nombre_pozo.toLowerCase().includes(q)).slice(0, 80);
  }, [query, pozos]);

  useEffect(() => { setQuery(value); }, [value]);

  function select(p: PozoItem) {
    setQuery(p.nombre_pozo); setOpen(false); onChange(p.nombre_pozo, p.bateria);
  }

  return (
    <div className="relative">
      <div className="relative">
        <Search size={13} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500 pointer-events-none" />
        <input type="text" value={query} placeholder="Buscar pozo..."
          onChange={e => { setQuery(e.target.value); setOpen(true); }}
          onFocus={() => setOpen(true)}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
          className="w-full pl-8 pr-3 py-2 bg-[#0f172a] border border-[#334155] rounded-lg text-sm text-slate-100 placeholder-slate-600 focus:outline-none focus:border-sky-500 focus:ring-1 focus:ring-sky-500/30 transition-colors" />
      </div>
      {open && filtered.length > 0 && (
        <div className="absolute z-50 top-full mt-1 w-full bg-[#1e293b] border border-[#334155] rounded-lg shadow-xl max-h-52 overflow-y-auto">
          {filtered.map(p => (
            <button key={p.nombre_pozo} type="button" onMouseDown={() => select(p)}
              className={"w-full text-left px-3 py-2 text-sm hover:bg-sky-500/10 transition-colors flex items-center justify-between " + (p.nombre_pozo === value ? "text-sky-400 bg-sky-500/10" : "text-slate-300")}>
              <span>{p.nombre_pozo}</span>
              <span className="text-xs text-slate-500">{p.bateria}</span>
            </button>
          ))}
          {pozos.filter(p => p.nombre_pozo.toLowerCase().includes(query.toLowerCase())).length > 80 && (
            <p className="text-center py-2 text-xs text-slate-600">Mostrando 80 — refiná la búsqueda</p>
          )}
        </div>
      )}
    </div>
  );
}

// ==========================================================
// Accion Form
// ==========================================================

function AccionForm({ form, setForm, pozos, onSubmit, onCancel, loading, isEdit }: {
  form: FormData; setForm: (f: FormData) => void; pozos: PozoItem[];
  onSubmit: () => void; onCancel: () => void; loading: boolean; isEdit: boolean;
}) {
  function set(field: keyof FormData, value: string) {
    const next = { ...form, [field]: value };
    if (field === "nombre_pozo") {
      const found = pozos.find(p => p.nombre_pozo === value);
      next.bateria = found?.bateria ?? "";
    }
    setForm(next);
  }

  const lbl = "block text-xs font-medium text-slate-400 mb-1.5";
  const inp = "w-full bg-[#0f172a] border border-[#334155] rounded-lg px-3 py-2 text-sm text-slate-100 placeholder-slate-600 focus:outline-none focus:border-sky-500 focus:ring-1 focus:ring-sky-500/30 transition-colors";

  return (
    <div className="px-6 py-5 space-y-5">
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={lbl}>Pozo <span className="text-red-400">*</span></label>
          <PozoCombobox value={form.nombre_pozo}
            onChange={(val, bat) => setForm({ ...form, nombre_pozo: val, bateria: bat })}
            pozos={pozos} />
        </div>
        <div>
          <label className={lbl}>Batería</label>
          <input type="text" value={form.bateria} readOnly
            className={inp + " opacity-60 cursor-not-allowed"} placeholder="Auto-completa desde pozo" />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={lbl}>Sist. Extracción <span className="text-red-400">*</span></label>
          <select value={form.sist_extraccion} onChange={e => set("sist_extraccion", e.target.value)} className={inp + " cursor-pointer"}>
            <option value="">Seleccionar...</option>
            {SIST_EXTRACCION.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
        <div>
          <label className={lbl}>Tipo <span className="text-red-400">*</span></label>
          <div className="flex gap-3 mt-2">
            {TIPOS.map(t => (
              <label key={t} className={"flex items-center gap-2 cursor-pointer px-4 py-2 rounded-lg border text-sm font-medium transition-all " + (form.tipo === t ? (t === "Superficie" ? "bg-sky-500/20 border-sky-500 text-sky-300" : "bg-violet-500/20 border-violet-500 text-violet-300") : "border-[#334155] text-slate-400 hover:border-slate-500")}>
                <input type="radio" name="tipo" value={t} checked={form.tipo === t} onChange={() => set("tipo", t)} className="sr-only" />
                <Layers size={13} />{t}
              </label>
            ))}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={lbl}>Tipo de Acción <span className="text-red-400">*</span></label>
          <select value={form.tipo_accion} onChange={e => set("tipo_accion", e.target.value)} className={inp + " cursor-pointer"}>
            <option value="">Seleccionar...</option>
            {TIPOS_ACCION.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div>
          <label className={lbl}>Recurso <span className="text-red-400">*</span></label>
          <select value={form.recurso} onChange={e => set("recurso", e.target.value)} className={inp + " cursor-pointer"}>
            <option value="">Seleccionar...</option>
            {RECURSOS.map(r => <option key={r} value={r}>{r}</option>)}
          </select>
        </div>
      </div>

      <div className="grid grid-cols-3 gap-4">
        <div>
          <label className={lbl}>Neta Incremental <span className="text-red-400">*</span></label>
          <input type="number" step="any" value={form.neta_incremental} onChange={e => set("neta_incremental", e.target.value)}
            placeholder="0.00" className={inp} />
        </div>
        <div>
          <label className={lbl}>Bruta Incremental <span className="text-red-400">*</span></label>
          <input type="number" step="any" value={form.bruta_incremental} onChange={e => set("bruta_incremental", e.target.value)}
            placeholder="0.00" className={inp} />
        </div>
        <div>
          <label className={lbl}>Inyección <span className="text-red-400">*</span></label>
          <input type="number" step="any" value={form.inyeccion} onChange={e => set("inyeccion", e.target.value)}
            placeholder="0.00" className={inp} />
        </div>
      </div>

      <div className="grid grid-cols-3 gap-4">
        {([
          { label: "Fecha Acción",      field: "fecha_accion"      as const, req: true  },
          { label: "Fecha Realización", field: "fecha_realizacion" as const, req: false },
          { label: "Fecha Fin",         field: "fecha_fin"         as const, req: false },
        ]).map(({ label, field, req }) => (
          <div key={field}>
            <label className={lbl}>
              <Calendar size={11} className="inline mr-1" />{label}
              {req ? <span className="text-red-400"> *</span> : <span className="text-slate-600 font-normal"> (opcional)</span>}
            </label>
            <input type="date" value={form[field]} onChange={e => set(field, e.target.value)} className={inp} />
            {field === "fecha_fin" && form.fecha_fin && (
              <button onClick={() => set("fecha_fin", "")} className="mt-1 text-xs text-slate-500 hover:text-red-400 transition-colors">
                × Quitar fecha fin
              </button>
            )}
          </div>
        ))}
      </div>

      <div className="flex items-center gap-2">
        <span className="text-xs text-slate-500">Estado resultante:</span>
        {estadoBadge(form.fecha_fin ? "FINALIZADO" : "EN PROCESO")}
      </div>

      <div>
        <label className={lbl}>Descripción de la Acción <span className="text-red-400">*</span></label>
        <textarea value={form.accion} onChange={e => set("accion", e.target.value)}
          rows={4} placeholder="Describí la acción de optimización..."
          className={inp + " resize-none leading-relaxed"} />
      </div>

      <div className="flex justify-end gap-3 pt-2 border-t border-[#334155]">
        <button onClick={onCancel} className="px-4 py-2 rounded-lg text-sm font-medium text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors">
          Cancelar
        </button>
        <button onClick={onSubmit} disabled={loading}
          className="px-5 py-2 rounded-lg text-sm font-semibold bg-sky-600 hover:bg-sky-500 text-white transition-colors disabled:opacity-50 flex items-center gap-2">
          {loading && <Loader2 size={14} className="animate-spin" />}
          {isEdit ? "Guardar cambios" : "Crear acción"}
        </button>
      </div>
    </div>
  );
}

// ==========================================================
// Confirm Delete
// ==========================================================

function ConfirmDelete({ open, onClose, onConfirm, loading }: {
  open: boolean; onClose: () => void; onConfirm: () => void; loading: boolean;
}) {
  return (
    <Modal open={open} onClose={onClose} title="Eliminar acción">
      <div className="px-6 py-5 space-y-5">
        <div className="flex items-start gap-3 p-4 bg-red-500/10 border border-red-500/20 rounded-xl">
          <AlertCircle size={18} className="text-red-400 mt-0.5 shrink-0" />
          <p className="text-sm text-slate-300">Esta operacion no se puede deshacer.</p>
        </div>
        <div className="flex justify-end gap-3">
          <button onClick={onClose} className="px-4 py-2 rounded-lg text-sm text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors">Cancelar</button>
          <button onClick={onConfirm} disabled={loading}
            className="px-5 py-2 rounded-lg text-sm font-semibold bg-red-600 hover:bg-red-500 text-white transition-colors disabled:opacity-50 flex items-center gap-2">
            {loading && <Loader2 size={14} className="animate-spin" />}Eliminar
          </button>
        </div>
      </div>
    </Modal>
  );
}

// ==========================================================
// Sort Header with inline filter dropdown
// ==========================================================

function SortHeader({ label, sortKey, current, dir, onClick, filterValue, onFilter, filterOpts }: {
  label: string; sortKey: SortKey; current: SortKey; dir: SortDir;
  onClick: (k: SortKey) => void;
  filterValue: string; onFilter: (v: string) => void; filterOpts: string[];
}) {
  const [showFilter, setShowFilter] = useState(false);
  const active = current === sortKey;

  return (
    <div className="relative flex items-center gap-1 group/hdr">
      <button onClick={() => onClick(sortKey)}
        className={"flex items-center gap-1 text-xs font-semibold uppercase tracking-wider transition-colors hover:text-sky-400 " + (active ? "text-sky-400" : "text-slate-500")}>
        {label}
        {active
          ? (dir === "asc" ? <ArrowUp size={11} /> : <ArrowDown size={11} />)
          : <ArrowUpDown size={11} className="opacity-0 group-hover/hdr:opacity-100 transition-opacity" />}
      </button>
      {filterOpts.length > 0 && (
        <button onClick={() => setShowFilter(!showFilter)}
          className={"p-0.5 rounded transition-colors " + (filterValue ? "text-sky-400" : "text-slate-600 hover:text-slate-400")}>
          <Filter size={10} />
        </button>
      )}
      {filterValue && (
        <button onClick={() => onFilter("")} className="text-slate-600 hover:text-red-400 text-xs transition-colors">×</button>
      )}
      {showFilter && (
        <div className="absolute top-full left-0 mt-1 z-50 bg-[#1e293b] border border-[#334155] rounded-lg shadow-xl min-w-[150px]">
          <div className="p-1">
            <button onClick={() => { onFilter(""); setShowFilter(false); }}
              className="w-full text-left px-3 py-1.5 text-xs text-slate-400 hover:bg-slate-700 rounded">Todos</button>
            {filterOpts.map(o => (
              <button key={o} onClick={() => { onFilter(o); setShowFilter(false); }}
                className={"w-full text-left px-3 py-1.5 text-xs rounded hover:bg-sky-500/10 transition-colors " + (filterValue === o ? "text-sky-400 bg-sky-500/10" : "text-slate-300")}>
                {o}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ==========================================================
// Expanded detail panel (shared between views)
// ==========================================================

function DetailPanel({ accion, onEdit, onDelete }: {
  accion: Accion;
  onEdit: (a: Accion) => void;
  onDelete: (id: string) => void;
}) {
  return (
    <div className="px-6 pb-5 pt-3 bg-[#0f172a]/60 border-t border-[#1e2d3d]">
      <div className="grid grid-cols-3 gap-4 mb-4">
        {([
          { label: "Tipo de Acción",     value: accion.tipo_accion ?? "—" },
          { label: "Recurso",            value: accion.recurso ?? "—" },
          { label: "Fecha Realización",  value: fmt(accion.fecha_realizacion) },
        ]).map(({ label, value }) => (
          <div key={label} className="bg-[#1e293b] rounded-lg p-3 border border-[#334155]">
            <p className="text-xs text-slate-500 mb-1">{label}</p>
            <p className="text-sm text-slate-300 font-medium">{value}</p>
          </div>
        ))}
      </div>
      <div className="grid grid-cols-3 gap-4 mb-4">
        {([
          { label: "Neta Incremental",   value: accion.neta_incremental != null ? String(accion.neta_incremental) : "—" },
          { label: "Bruta Incremental",  value: accion.bruta_incremental != null ? String(accion.bruta_incremental) : "—" },
          { label: "Inyección",          value: accion.inyeccion != null ? String(accion.inyeccion) : "—" },
        ]).map(({ label, value }) => (
          <div key={label} className="bg-[#1e293b] rounded-lg p-3 border border-[#334155]">
            <p className="text-xs text-slate-500 mb-1">{label}</p>
            <p className="text-sm text-slate-300 font-medium">{value}</p>
          </div>
        ))}
      </div>
      <div className="grid grid-cols-2 gap-4 mb-4">
        {([
          { label: "Fecha Fin",         value: fmt(accion.fecha_fin) },
          { label: "Últ. modificación", value: (accion.modificado_utc ?? "").slice(0, 16).replace("T", " ") || "—" },
        ]).map(({ label, value }) => (
          <div key={label} className="bg-[#1e293b] rounded-lg p-3 border border-[#334155]">
            <p className="text-xs text-slate-500 mb-1">{label}</p>
            <p className="text-sm text-slate-300 font-medium">{value}</p>
          </div>
        ))}
      </div>
      <div className="bg-[#1e293b] rounded-lg p-4 border border-[#334155]">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">Descripción</p>
        <p className="text-sm text-slate-300 leading-relaxed whitespace-pre-wrap">{accion.accion}</p>
      </div>
      <div className="mt-3 flex justify-end gap-2">
        <button onClick={() => onEdit(accion)}
          className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium bg-sky-500/10 text-sky-400 border border-sky-500/20 hover:bg-sky-500/20 transition-colors">
          <Pencil size={12} />Editar
        </button>
        <button onClick={() => onDelete(accion.id)}
          className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium bg-red-500/10 text-red-400 border border-red-500/20 hover:bg-red-500/20 transition-colors">
          <Trash2 size={12} />Eliminar
        </button>
      </div>
    </div>
  );
}

// ==========================================================
// Main Page
// ==========================================================

export default function AccionesPage() {
  const [acciones,    setAcciones]  = useState<Accion[]>([]);
  const [allAcciones, setAll]       = useState<Accion[]>([]);
  const [pozos,       setPozos]     = useState<PozoItem[]>([]);
  const [kpis,        setKpis]      = useState({ total: 0, en_proceso: 0, finalizadas: 0 });
  const [loading,     setLoading]   = useState(true);
  const [error,       setError]     = useState("");

  const [filterBusqueda, setFilterBusqueda] = useState("");
  const [colFilters, setColFilters]         = useState<Partial<Record<SortKey, string>>>({});
  const [sortKey, setSortKey]               = useState<SortKey>("fecha_accion");
  const [sortDir, setSortDir]               = useState<SortDir>("desc");
  const [vista, setVista]                   = useState<Vista>("plana");

  const [expandedId,    setExpandedId]    = useState<string | null>(null);
  const [expandedPozo,  setExpandedPozo]  = useState<string | null>(null);
  const [expandedAccId, setExpandedAccId] = useState<string | null>(null);

  const [modalOpen,  setModalOpen]  = useState(false);
  const [editOpen,   setEditOpen]   = useState(false);
  const [editId,     setEditId]     = useState<string | null>(null);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleteId,   setDeleteId]   = useState<string | null>(null);
  const [exportOpen, setExportOpen] = useState(false);
  const [form,       setForm]       = useState<FormData>(FORM_EMPTY);
  const [saving,     setSaving]     = useState(false);
  const [deleting,   setDeleting]   = useState(false);

  // --- Fetch ---

  const fetchAll = useCallback(async () => {
    setLoading(true); setError("");
    try {
      const qs = new URLSearchParams();
      if (filterBusqueda) qs.set("busqueda", filterBusqueda);
      const [filtRes, allRes, kpisRes] = await Promise.all([
        cachedGet<{ acciones: Accion[] }>(API + "/api/acciones?" + qs),
        cachedGet<{ acciones: Accion[] }>(API + "/api/acciones"),
        cachedGet<{ total: number; en_proceso: number; finalizadas: number }>(API + "/api/acciones/kpis"),
      ]);
      setAcciones(filtRes.acciones ?? []);
      setAll(allRes.acciones ?? []);
      setKpis(kpisRes);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error cargando datos");
    } finally { setLoading(false); }
  }, [filterBusqueda]);

  const fetchPozos = useCallback(async () => {
    try {
      const res = await cachedGet<{ pozos: PozoItem[] }>(API + "/api/acciones/pozos-lista");
      setPozos(res.pozos ?? []);
    } catch { /* silencioso */ }
  }, []);

  useEffect(() => { fetchPozos(); }, [fetchPozos]);
  useEffect(() => { fetchAll(); }, [fetchAll]);

  // --- Sort + col filter (client-side) ---

  function handleSort(key: SortKey) {
    if (sortKey === key) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortKey(key); setSortDir("asc"); }
  }

  function setColFilter(key: SortKey, val: string) {
    setColFilters(prev => ({ ...prev, [key]: val }));
  }

  const processed = useMemo(() => {
    let data = [...acciones];
    Object.entries(colFilters).forEach(([k, v]) => {
      if (v) data = data.filter(a => String(a[k as keyof Accion] ?? "") === v);
    });
    data.sort((a, b) => {
      const av = String(a[sortKey] ?? "");
      const bv = String(b[sortKey] ?? "");
      const cmp = av.localeCompare(bv, "es", { sensitivity: "base" });
      return sortDir === "asc" ? cmp : -cmp;
    });
    return data;
  }, [acciones, colFilters, sortKey, sortDir]);

  const colOpts = useMemo(() => ({
    nombre_pozo:     [...new Set(acciones.map(a => a.nombre_pozo))].sort(),
    bateria:         [...new Set(acciones.map(a => a.bateria).filter(Boolean))].sort(),
    sist_extraccion: SIST_EXTRACCION.filter(s => acciones.some(a => a.sist_extraccion === s)),
    tipo:            TIPOS.filter(t => acciones.some(a => a.tipo === t)),
    fecha_accion:    [...new Set(acciones.map(a => (a.fecha_accion || "").slice(0, 7)).filter(Boolean))].sort().reverse(),
    estado:          ["EN PROCESO", "FINALIZADO"],
  }), [acciones]);

  // --- Grouped view ---

  const grupos = useMemo<GrupoPozo[]>(() => {
    const map = new Map<string, GrupoPozo>();
    processed.forEach(a => {
      if (!map.has(a.nombre_pozo)) {
        map.set(a.nombre_pozo, { nombre_pozo: a.nombre_pozo, bateria: a.bateria, acciones: [], en_proceso: 0, finalizadas: 0 });
      }
      const g = map.get(a.nombre_pozo)!;
      g.acciones.push(a);
      if (a.estado === "EN PROCESO") g.en_proceso++; else g.finalizadas++;
    });
    return Array.from(map.values());
  }, [processed]);

  const activeColFilters = Object.values(colFilters).filter(Boolean).length;

  // --- CRUD ---

  async function handleCreate() {
    if (!form.nombre_pozo || !form.sist_extraccion || !form.fecha_accion || !form.tipo || !form.accion || !form.tipo_accion || !form.recurso || !form.neta_incremental || !form.bruta_incremental || !form.inyeccion) {
      alert("Completá los campos obligatorios (*)"); return;
    }
    setSaving(true);
    try {
      const res = await fetch(API + "/api/acciones", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...form,
          fecha_realizacion: form.fecha_realizacion || null,
          fecha_fin: form.fecha_fin || null,
          neta_incremental: parseFloat(form.neta_incremental),
          bruta_incremental: parseFloat(form.bruta_incremental),
          inyeccion: parseFloat(form.inyeccion),
        }),
      });
      if (!res.ok) throw new Error(await res.text());
      clearAccionesCache();
      setModalOpen(false); setForm(FORM_EMPTY); fetchAll();
    } catch (e: unknown) { alert(e instanceof Error ? e.message : "Error"); }
    finally { setSaving(false); }
  }

  function openEdit(accion: Accion) {
    setEditId(accion.id);
    setForm({
      nombre_pozo: accion.nombre_pozo, bateria: accion.bateria,
      sist_extraccion: accion.sist_extraccion, fecha_accion: accion.fecha_accion ?? "",
      fecha_realizacion: accion.fecha_realizacion ?? "", fecha_fin: accion.fecha_fin ?? "",
      tipo: accion.tipo, tipo_accion: accion.tipo_accion ?? "",
      recurso: accion.recurso ?? "",
      neta_incremental: String(accion.neta_incremental ?? ""),
      bruta_incremental: String(accion.bruta_incremental ?? ""),
      inyeccion: String(accion.inyeccion ?? ""),
      accion: accion.accion,
    });
    setEditOpen(true);
  }

  async function handleEdit() {
    if (!editId) return;
    setSaving(true);
    try {
      const res = await fetch(API + "/api/acciones/" + editId, {
        method: "PUT", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...form,
          fecha_realizacion: form.fecha_realizacion || null,
          fecha_fin: form.fecha_fin || null,
          neta_incremental: form.neta_incremental ? parseFloat(form.neta_incremental) : null,
          bruta_incremental: form.bruta_incremental ? parseFloat(form.bruta_incremental) : null,
          inyeccion: form.inyeccion ? parseFloat(form.inyeccion) : null,
        }),
      });
      if (!res.ok) throw new Error(await res.text());
      clearAccionesCache();
      setEditOpen(false); setEditId(null); setForm(FORM_EMPTY); fetchAll();
    } catch (e: unknown) { alert(e instanceof Error ? e.message : "Error"); }
    finally { setSaving(false); }
  }

  async function handleDelete() {
    if (!deleteId) return;
    setDeleting(true);
    try {
      const res = await fetch(API + "/api/acciones/" + deleteId, { method: "DELETE" });
      if (!res.ok) throw new Error(await res.text());
      clearAccionesCache();
      setDeleteOpen(false); setDeleteId(null); setExpandedId(null); setExpandedAccId(null);
      fetchAll();
    } catch (e: unknown) { alert(e instanceof Error ? e.message : "Error"); }
    finally { setDeleting(false); }
  }

  function openDeleteConfirm(id: string) { setDeleteId(id); setDeleteOpen(true); }

  // --- Column headers config ---

  const sortHeaders: { label: string; key: SortKey }[] = [
    { label: "Pozo",       key: "nombre_pozo"     },
    { label: "Batería",    key: "bateria"          },
    { label: "Sist. Ext.", key: "sist_extraccion"  },
    { label: "Tipo",       key: "tipo"             },
    { label: "F. Acción",  key: "fecha_accion"     },
    { label: "Estado",     key: "estado"           },
  ];

  const colsPlana = "grid-cols-[1.8fr_1fr_1fr_1fr_1fr_1fr_auto]";

  // ==========================================================
  // Render
  // ==========================================================

  return (
    <div className="min-h-screen bg-[#0f172a] text-slate-100">
      <div className="max-w-7xl mx-auto px-6 py-8 space-y-6">

        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
              <Wrench size={22} className="text-sky-400" />Acciones de Optimización
            </h1>
            <p className="text-sm text-slate-500 mt-1">Seguimiento de intervenciones por pozo</p>
          </div>
          <div className="flex items-center gap-2">
            <button onClick={() => setExportOpen(true)}
              className="flex items-center gap-2 px-3 py-2 rounded-xl border border-[#334155] text-slate-400 hover:text-slate-200 hover:border-slate-500 text-sm font-medium transition-all">
              <Download size={15} />CSV
            </button>
            <button onClick={() => { setForm(FORM_EMPTY); setModalOpen(true); }}
              className="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-sky-600 hover:bg-sky-500 text-white text-sm font-semibold transition-all shadow-lg shadow-sky-900/40">
              <Plus size={16} />Nueva Acción
            </button>
          </div>
        </div>

        {/* KPIs */}
        <div className="grid grid-cols-3 gap-4">
          {([
            { label: "Total acciones", value: kpis.total,       color: "text-sky-400",     bg: "bg-sky-500/10 border-sky-500/20"       },
            { label: "En proceso",     value: kpis.en_proceso,  color: "text-amber-400",   bg: "bg-amber-500/10 border-amber-500/20"   },
            { label: "Finalizadas",    value: kpis.finalizadas, color: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/20"},
          ]).map(({ label, value, color, bg }) => (
            <div key={label} className={"rounded-xl border p-5 " + bg}>
              <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">{label}</p>
              <p className={"text-3xl font-bold " + color}>{value}</p>
            </div>
          ))}
        </div>

        {/* Toolbar */}
        <div className="flex items-center gap-3">
          <div className="relative flex-1">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
            <input type="text" placeholder="Buscar en descripción..."
              value={filterBusqueda} onChange={e => setFilterBusqueda(e.target.value)}
              className="w-full pl-9 pr-4 py-2 bg-[#1e293b] border border-[#334155] rounded-lg text-sm text-slate-100 placeholder-slate-600 focus:outline-none focus:border-sky-500 focus:ring-1 focus:ring-sky-500/30 transition-colors" />
          </div>

          {activeColFilters > 0 && (
            <div className="flex items-center gap-2">
              <span className="text-xs text-sky-400 bg-sky-500/10 border border-sky-500/20 px-2 py-1 rounded-lg">
                {activeColFilters} filtro{activeColFilters > 1 ? "s" : ""} activo{activeColFilters > 1 ? "s" : ""}
              </span>
              <button onClick={() => setColFilters({})} className="text-xs text-slate-500 hover:text-red-400 transition-colors">
                × Limpiar
              </button>
            </div>
          )}

          {/* Vista toggle */}
          <div className="flex items-center bg-[#1e293b] border border-[#334155] rounded-lg p-0.5">
            {([
              { v: "plana"    as Vista, icon: List,       label: "Plana"    },
              { v: "agrupada" as Vista, icon: FolderOpen, label: "Por pozo" },
            ]).map(({ v, icon: Icon, label }) => (
              <button key={v} onClick={() => setVista(v)}
                className={"flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-all " + (vista === v ? "bg-sky-600 text-white shadow" : "text-slate-500 hover:text-slate-300")}>
                <Icon size={13} />{label}
              </button>
            ))}
          </div>
        </div>

        {/* Table */}
        <div className="bg-[#1e293b] border border-[#334155] rounded-xl overflow-hidden">

          {/* Header */}
          {vista === "plana" ? (
            <div className={"grid " + colsPlana + " px-4 py-3 border-b border-[#334155] bg-[#162032]"}>
              {sortHeaders.map(({ label, key }) => (
                <div key={key} className="px-2">
                  <SortHeader label={label} sortKey={key} current={sortKey} dir={sortDir}
                    onClick={handleSort}
                    filterValue={colFilters[key] ?? ""}
                    onFilter={v => setColFilter(key, v)}
                    filterOpts={colOpts[key] ?? []} />
                </div>
              ))}
              <div />
            </div>
          ) : (
            <div className="grid grid-cols-[2fr_1fr_1fr_1.5fr_auto] px-4 py-3 border-b border-[#334155] bg-[#162032]">
              {["Pozo", "Batería", "Acciones", "Estado", ""].map(h => (
                <div key={h} className="px-2 text-xs font-semibold text-slate-500 uppercase tracking-wider">{h}</div>
              ))}
            </div>
          )}

          {/* Body */}
          {loading ? (
            <div className="flex items-center justify-center py-16 gap-3 text-slate-500">
              <Loader2 size={20} className="animate-spin" /><span className="text-sm">Cargando...</span>
            </div>
          ) : error ? (
            <div className="flex items-center justify-center py-16 gap-3 text-red-400">
              <AlertCircle size={18} /><span className="text-sm">{error}</span>
            </div>
          ) : processed.length === 0 ? (
            <div className="text-center py-16">
              <Wrench size={32} className="text-slate-700 mx-auto mb-3" />
              <p className="text-slate-500 text-sm">No hay acciones registradas</p>
            </div>

          ) : vista === "plana" ? (
            /* ── VISTA PLANA ── */
            <div className="divide-y divide-[#1e2d3d]">
              {processed.map(accion => {
                const expanded = expandedId === accion.id;
                return (
                  <div key={accion.id}>
                    <div className={"grid " + colsPlana + " px-4 py-3.5 items-center cursor-pointer hover:bg-[#162032]/60 transition-colors group"}
                      onClick={() => setExpandedId(expanded ? null : accion.id)}>
                      <div className="px-2"><p className="text-sm font-medium text-slate-200 truncate">{accion.nombre_pozo}</p></div>
                      <div className="px-2"><p className="text-xs text-slate-400 truncate">{accion.bateria || "—"}</p></div>
                      <div className="px-2">
                        <span className="text-xs font-mono text-slate-300 bg-slate-800 px-2 py-0.5 rounded">{accion.sist_extraccion}</span>
                      </div>
                      <div className="px-2">{tipoBadge(accion.tipo)}</div>
                      <div className="px-2"><p className="text-xs text-slate-400">{fmt(accion.fecha_accion)}</p></div>
                      <div className="px-2">{estadoBadge(accion.estado)}</div>
                      <div className="px-2 flex items-center gap-1">
                        <button onClick={e => { e.stopPropagation(); openEdit(accion); }}
                          className="p-1.5 rounded-lg text-slate-600 hover:text-sky-400 hover:bg-sky-500/10 transition-colors opacity-0 group-hover:opacity-100">
                          <Pencil size={13} />
                        </button>
                        <button onClick={e => { e.stopPropagation(); openDeleteConfirm(accion.id); }}
                          className="p-1.5 rounded-lg text-slate-600 hover:text-red-400 hover:bg-red-500/10 transition-colors opacity-0 group-hover:opacity-100">
                          <Trash2 size={13} />
                        </button>
                        <div className="text-slate-600 ml-1">
                          {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                        </div>
                      </div>
                    </div>
                    {expanded && <DetailPanel accion={accion} onEdit={openEdit} onDelete={openDeleteConfirm} />}
                  </div>
                );
              })}
            </div>

          ) : (
            /* ── VISTA AGRUPADA ── */
            <div className="divide-y divide-[#1e2d3d]">
              {grupos.map(grupo => {
                const gExp = expandedPozo === grupo.nombre_pozo;
                return (
                  <div key={grupo.nombre_pozo}>
                    {/* Fila pozo (padre) */}
                    <div className="grid grid-cols-[2fr_1fr_1fr_1.5fr_auto] px-4 py-4 items-center cursor-pointer hover:bg-[#162032]/60 transition-colors group"
                      onClick={() => setExpandedPozo(gExp ? null : grupo.nombre_pozo)}>
                      <div className="px-2 flex items-center gap-2">
                        <span className={"transition-transform duration-200 inline-block " + (gExp ? "rotate-90" : "")}>
                          <ChevronDown size={14} className="text-slate-500" />
                        </span>
                        <p className="text-sm font-semibold text-slate-100">{grupo.nombre_pozo}</p>
                      </div>
                      <div className="px-2"><p className="text-xs text-slate-400">{grupo.bateria || "—"}</p></div>
                      <div className="px-2">
                        <span className="text-xs bg-slate-800 text-slate-300 px-2 py-0.5 rounded-full font-medium">
                          {grupo.acciones.length} acción{grupo.acciones.length !== 1 ? "es" : ""}
                        </span>
                      </div>
                      <div className="px-2 flex items-center gap-2 flex-wrap">
                        {grupo.en_proceso > 0 && (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold bg-amber-500/15 text-amber-400 border border-amber-500/25">
                            <Clock size={10} />{grupo.en_proceso} en proceso
                          </span>
                        )}
                        {grupo.finalizadas > 0 && (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold bg-emerald-500/15 text-emerald-400 border border-emerald-500/25">
                            <CheckCircle2 size={10} />{grupo.finalizadas} finalizada{grupo.finalizadas !== 1 ? "s" : ""}
                          </span>
                        )}
                      </div>
                      <div className="px-2">
                        <button
                          onClick={e => { e.stopPropagation(); setForm({ ...FORM_EMPTY, nombre_pozo: grupo.nombre_pozo, bateria: grupo.bateria }); setModalOpen(true); }}
                          className="p-1.5 rounded-lg text-slate-600 hover:text-sky-400 hover:bg-sky-500/10 transition-colors opacity-0 group-hover:opacity-100"
                          title="Agregar acción a este pozo">
                          <Plus size={13} />
                        </button>
                      </div>
                    </div>

                    {/* Acciones hijas */}
                    {gExp && (
                      <div className="bg-[#0d1b2a] border-t border-[#1e2d3d]">
                        {grupo.acciones.map((accion, i) => {
                          const aExp = expandedAccId === accion.id;
                          const isLast = i === grupo.acciones.length - 1;
                          return (
                            <div key={accion.id} className={isLast ? "" : "border-b border-[#1e2d3d]"}>
                              <div className={"grid " + colsPlana + " pl-10 pr-4 py-3 items-center cursor-pointer hover:bg-[#162032]/40 transition-colors group"}
                                onClick={() => setExpandedAccId(aExp ? null : accion.id)}>
                                <div className="px-2"><p className="text-xs text-slate-300 font-medium">{fmt(accion.fecha_accion)}</p></div>
                                <div className="px-2"><p className="text-xs text-slate-500">{accion.bateria || "—"}</p></div>
                                <div className="px-2">
                                  <span className="text-xs font-mono text-slate-400 bg-slate-800/60 px-2 py-0.5 rounded">{accion.sist_extraccion}</span>
                                </div>
                                <div className="px-2">{tipoBadge(accion.tipo)}</div>
                                <div className="px-2">
                                  <p className="text-xs text-slate-400 truncate" title={accion.accion}>
                                    {accion.accion.length > 55 ? accion.accion.slice(0, 55) + "…" : accion.accion}
                                  </p>
                                </div>
                                <div className="px-2">{estadoBadge(accion.estado)}</div>
                                <div className="px-2 flex items-center gap-1">
                                  <button onClick={e => { e.stopPropagation(); openEdit(accion); }}
                                    className="p-1.5 rounded-lg text-slate-600 hover:text-sky-400 hover:bg-sky-500/10 transition-colors opacity-0 group-hover:opacity-100">
                                    <Pencil size={12} />
                                  </button>
                                  <button onClick={e => { e.stopPropagation(); openDeleteConfirm(accion.id); }}
                                    className="p-1.5 rounded-lg text-slate-600 hover:text-red-400 hover:bg-red-500/10 transition-colors opacity-0 group-hover:opacity-100">
                                    <Trash2 size={12} />
                                  </button>
                                  <div className="text-slate-600 ml-1">
                                    {aExp ? <ChevronUp size={13} /> : <ChevronDown size={13} />}
                                  </div>
                                </div>
                              </div>
                              {aExp && <DetailPanel accion={accion} onEdit={openEdit} onDelete={openDeleteConfirm} />}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {/* Footer */}
          {!loading && processed.length > 0 && (
            <div className="px-6 py-3 border-t border-[#334155] bg-[#162032] flex items-center justify-between">
              <p className="text-xs text-slate-600">
                {vista === "plana"
                  ? processed.length + " acción" + (processed.length !== 1 ? "es" : "")
                  : grupos.length + " pozo" + (grupos.length !== 1 ? "s" : "") + " · " + processed.length + " acción" + (processed.length !== 1 ? "es" : "")}
                {(activeColFilters > 0 || filterBusqueda) && " (filtrado)"}
              </p>
              <p className="text-xs text-slate-600">
                Ordenado por <span className="text-slate-400">{sortKey}</span> {sortDir === "asc" ? "↑" : "↓"}
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Modals */}
      <Modal open={modalOpen} onClose={() => { setModalOpen(false); setForm(FORM_EMPTY); }} title="Nueva Acción de Optimización">
        <AccionForm form={form} setForm={setForm} pozos={pozos}
          onSubmit={handleCreate} onCancel={() => { setModalOpen(false); setForm(FORM_EMPTY); }}
          loading={saving} isEdit={false} />
      </Modal>

      <Modal open={editOpen} onClose={() => { setEditOpen(false); setEditId(null); setForm(FORM_EMPTY); }} title="Editar Acción">
        <AccionForm form={form} setForm={setForm} pozos={pozos}
          onSubmit={handleEdit} onCancel={() => { setEditOpen(false); setEditId(null); setForm(FORM_EMPTY); }}
          loading={saving} isEdit={true} />
      </Modal>

      <ConfirmDelete open={deleteOpen} onClose={() => { setDeleteOpen(false); setDeleteId(null); }}
        onConfirm={handleDelete} loading={deleting} />

      <ExportModal open={exportOpen} onClose={() => setExportOpen(false)}
        onExport={scope => exportCSV(processed, scope, allAcciones)} />
    </div>
  );
}
