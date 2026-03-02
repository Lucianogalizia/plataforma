"use client";

import { useEffect, useState, useCallback, useMemo } from "react";
import {
  Plus, X, Pencil, Trash2, ChevronDown, ChevronUp,
  Search, Filter, CheckCircle2, Clock, Loader2,
  Wrench, Calendar, Layers, AlertCircle,
} from "lucide-react";

// ==========================================================
// Types
// ==========================================================

interface PozoItem {
  nombre_pozo: string;
  bateria: string;
}

interface Accion {
  id: string;
  nombre_pozo: string;
  bateria: string;
  sist_extraccion: string;
  fecha_accion: string;
  fecha_realizacion: string | null;
  fecha_fin: string | null;
  tipo: string;
  accion: string;
  estado: "EN PROCESO" | "FINALIZADO";
  creado_utc: string;
  modificado_utc: string;
}

interface FormData {
  nombre_pozo: string;
  bateria: string;
  sist_extraccion: string;
  fecha_accion: string;
  fecha_realizacion: string;
  fecha_fin: string;
  tipo: string;
  accion: string;
}

const SIST_EXTRACCION = ["AIB", "BES", "PCP", "SWABBING", "SURGENTE", "OTRO"];
const TIPOS = ["Superficie", "Fondo"];

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

const FORM_EMPTY: FormData = {
  nombre_pozo: "",
  bateria: "",
  sist_extraccion: "",
  fecha_accion: "",
  fecha_realizacion: "",
  fecha_fin: "",
  tipo: "",
  accion: "",
};

// ==========================================================
// Helpers
// ==========================================================

function fmt(d: string | null | undefined) {
  if (!d) return "—";
  return d.slice(0, 10);
}

function estadoBadge(estado: string) {
  if (estado === "FINALIZADO") {
    return (
      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-emerald-500/15 text-emerald-400 border border-emerald-500/25">
        <CheckCircle2 size={11} />
        FINALIZADO
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-amber-500/15 text-amber-400 border border-amber-500/25">
      <Clock size={11} />
      EN PROCESO
    </span>
  );
}

function tipoBadge(tipo: string) {
  const isSuperficie = tipo === "Superficie";
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium border ${
      isSuperficie
        ? "bg-sky-500/10 text-sky-400 border-sky-500/20"
        : "bg-violet-500/10 text-violet-400 border-violet-500/20"
    }`}>
      <Layers size={10} />
      {tipo}
    </span>
  );
}

// ==========================================================
// Modal Component
// ==========================================================

function Modal({
  open, onClose, title, children,
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  children: React.ReactNode;
}) {
  if (!open) return null;
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ background: "rgba(0,0,0,0.7)", backdropFilter: "blur(4px)" }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div className="bg-[#1e293b] border border-[#334155] rounded-2xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-[#334155]">
          <h2 className="text-base font-semibold text-slate-100 flex items-center gap-2">
            <Wrench size={16} className="text-sky-400" />
            {title}
          </h2>
          <button
            onClick={onClose}
            className="text-slate-500 hover:text-slate-300 transition-colors rounded-lg p-1 hover:bg-slate-700"
          >
            <X size={18} />
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}

// ==========================================================
// Form Component
// ==========================================================

function AccionForm({
  form,
  setForm,
  pozos,
  onSubmit,
  onCancel,
  loading,
  isEdit,
}: {
  form: FormData;
  setForm: (f: FormData) => void;
  pozos: PozoItem[];
  onSubmit: () => void;
  onCancel: () => void;
  loading: boolean;
  isEdit: boolean;
}) {
  function set(field: keyof FormData, value: string) {
    const next = { ...form, [field]: value };
    if (field === "nombre_pozo") {
      const found = pozos.find((p) => p.nombre_pozo === value);
      next.bateria = found?.bateria ?? "";
    }
    setForm(next);
  }

  const labelCls = "block text-xs font-medium text-slate-400 mb-1.5";
  const inputCls =
    "w-full bg-[#0f172a] border border-[#334155] rounded-lg px-3 py-2 text-sm text-slate-100 placeholder-slate-600 focus:outline-none focus:border-sky-500 focus:ring-1 focus:ring-sky-500/30 transition-colors";
  const selectCls = inputCls + " cursor-pointer";

  return (
    <div className="px-6 py-5 space-y-5">
      {/* Row 1: Pozo + Batería */}
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelCls}>Pozo <span className="text-red-400">*</span></label>
          <select
            value={form.nombre_pozo}
            onChange={(e) => set("nombre_pozo", e.target.value)}
            className={selectCls}
          >
            <option value="">Seleccionar pozo...</option>
            {pozos.map((p) => (
              <option key={p.nombre_pozo} value={p.nombre_pozo}>
                {p.nombre_pozo}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className={labelCls}>Batería</label>
          <input
            type="text"
            value={form.bateria}
            readOnly
            className={inputCls + " opacity-60 cursor-not-allowed"}
            placeholder="Auto-completa desde pozo"
          />
        </div>
      </div>

      {/* Row 2: Sist. Extracción + Tipo */}
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelCls}>Sist. Extracción <span className="text-red-400">*</span></label>
          <select
            value={form.sist_extraccion}
            onChange={(e) => set("sist_extraccion", e.target.value)}
            className={selectCls}
          >
            <option value="">Seleccionar...</option>
            {SIST_EXTRACCION.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
        <div>
          <label className={labelCls}>Tipo <span className="text-red-400">*</span></label>
          <div className="flex gap-3 mt-2">
            {TIPOS.map((t) => (
              <label
                key={t}
                className={`flex items-center gap-2 cursor-pointer px-4 py-2 rounded-lg border text-sm font-medium transition-all ${
                  form.tipo === t
                    ? t === "Superficie"
                      ? "bg-sky-500/20 border-sky-500 text-sky-300"
                      : "bg-violet-500/20 border-violet-500 text-violet-300"
                    : "border-[#334155] text-slate-400 hover:border-slate-500"
                }`}
              >
                <input
                  type="radio"
                  name="tipo"
                  value={t}
                  checked={form.tipo === t}
                  onChange={() => set("tipo", t)}
                  className="sr-only"
                />
                <Layers size={13} />
                {t}
              </label>
            ))}
          </div>
        </div>
      </div>

      {/* Row 3: Fechas */}
      <div className="grid grid-cols-3 gap-4">
        <div>
          <label className={labelCls}>
            <Calendar size={11} className="inline mr-1" />
            Fecha Acción <span className="text-red-400">*</span>
          </label>
          <input
            type="date"
            value={form.fecha_accion}
            onChange={(e) => set("fecha_accion", e.target.value)}
            className={inputCls}
          />
        </div>
        <div>
          <label className={labelCls}>
            <Calendar size={11} className="inline mr-1" />
            Fecha Realización
          </label>
          <input
            type="date"
            value={form.fecha_realizacion}
            onChange={(e) => set("fecha_realizacion", e.target.value)}
            className={inputCls}
          />
        </div>
        <div>
          <label className={labelCls}>
            <Calendar size={11} className="inline mr-1" />
            Fecha Fin <span className="text-slate-600 font-normal">(opcional)</span>
          </label>
          <input
            type="date"
            value={form.fecha_fin}
            onChange={(e) => set("fecha_fin", e.target.value)}
            className={inputCls}
          />
          {form.fecha_fin && (
            <button
              onClick={() => set("fecha_fin", "")}
              className="mt-1 text-xs text-slate-500 hover:text-red-400 transition-colors"
            >
              × Quitar fecha fin
            </button>
          )}
        </div>
      </div>

      {/* Estado preview */}
      <div className="flex items-center gap-2">
        <span className="text-xs text-slate-500">Estado resultante:</span>
        {estadoBadge(form.fecha_fin ? "FINALIZADO" : "EN PROCESO")}
      </div>

      {/* Acción */}
      <div>
        <label className={labelCls}>
          Descripción de la Acción <span className="text-red-400">*</span>
        </label>
        <textarea
          value={form.accion}
          onChange={(e) => set("accion", e.target.value)}
          rows={4}
          placeholder="Describí la acción de optimización a realizar o realizada..."
          className={inputCls + " resize-none leading-relaxed"}
        />
      </div>

      {/* Buttons */}
      <div className="flex justify-end gap-3 pt-2 border-t border-[#334155]">
        <button
          onClick={onCancel}
          className="px-4 py-2 rounded-lg text-sm font-medium text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors"
        >
          Cancelar
        </button>
        <button
          onClick={onSubmit}
          disabled={loading}
          className="px-5 py-2 rounded-lg text-sm font-semibold bg-sky-600 hover:bg-sky-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
        >
          {loading && <Loader2 size={14} className="animate-spin" />}
          {isEdit ? "Guardar cambios" : "Crear acción"}
        </button>
      </div>
    </div>
  );
}

// ==========================================================
// Delete Confirm Modal
// ==========================================================

function ConfirmDelete({
  open, onClose, onConfirm, loading,
}: {
  open: boolean;
  onClose: () => void;
  onConfirm: () => void;
  loading: boolean;
}) {
  return (
    <Modal open={open} onClose={onClose} title="Eliminar acción">
      <div className="px-6 py-5 space-y-5">
        <div className="flex items-start gap-3 p-4 bg-red-500/10 border border-red-500/20 rounded-xl">
          <AlertCircle size={18} className="text-red-400 mt-0.5 shrink-0" />
          <p className="text-sm text-slate-300">
            ¿Estás seguro que querés eliminar esta acción? Esta operación no se puede deshacer.
          </p>
        </div>
        <div className="flex justify-end gap-3">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded-lg text-sm font-medium text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors"
          >
            Cancelar
          </button>
          <button
            onClick={onConfirm}
            disabled={loading}
            className="px-5 py-2 rounded-lg text-sm font-semibold bg-red-600 hover:bg-red-500 text-white transition-colors disabled:opacity-50 flex items-center gap-2"
          >
            {loading && <Loader2 size={14} className="animate-spin" />}
            Eliminar
          </button>
        </div>
      </div>
    </Modal>
  );
}

// ==========================================================
// Main Page
// ==========================================================

export default function AccionesPage() {
  // --- Data ---
  const [acciones, setAcciones] = useState<Accion[]>([]);
  const [pozos, setPozos] = useState<PozoItem[]>([]);
  const [kpis, setKpis] = useState({ total: 0, en_proceso: 0, finalizadas: 0 });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // --- Filters ---
  const [filterPozo, setFilterPozo] = useState("");
  const [filterBateria, setFilterBateria] = useState("");
  const [filterEstado, setFilterEstado] = useState("");
  const [filterTipo, setFilterTipo] = useState("");
  const [filterSist, setFilterSist] = useState("");
  const [filterMes, setFilterMes] = useState("");
  const [filterBusqueda, setFilterBusqueda] = useState("");

  // --- UI ---
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [showFilters, setShowFilters] = useState(false);

  // --- Modal crear ---
  const [modalOpen, setModalOpen] = useState(false);
  const [form, setForm] = useState<FormData>(FORM_EMPTY);
  const [saving, setSaving] = useState(false);

  // --- Modal editar ---
  const [editOpen, setEditOpen] = useState(false);
  const [editId, setEditId] = useState<string | null>(null);

  // --- Modal eliminar ---
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleteId, setDeleteId] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);

  // ==========================================================
  // Fetch
  // ==========================================================

  const fetchAll = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const qs = new URLSearchParams();
      if (filterPozo)    qs.set("nombre_pozo", filterPozo);
      if (filterBateria) qs.set("bateria", filterBateria);
      if (filterEstado)  qs.set("estado", filterEstado);
      if (filterTipo)    qs.set("tipo", filterTipo);
      if (filterSist)    qs.set("sist_extraccion", filterSist);
      if (filterMes)     qs.set("mes", filterMes);
      if (filterBusqueda) qs.set("busqueda", filterBusqueda);

      const [accionesRes, kpisRes] = await Promise.all([
        fetch(`${API}/api/acciones?${qs}`).then((r) => r.json()),
        fetch(`${API}/api/acciones/kpis`).then((r) => r.json()),
      ]);

      setAcciones(accionesRes.acciones ?? []);
      setKpis(kpisRes);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error cargando datos");
    } finally {
      setLoading(false);
    }
  }, [filterPozo, filterBateria, filterEstado, filterTipo, filterSist, filterMes, filterBusqueda]);

  const fetchPozos = useCallback(async () => {
    try {
      const res = await fetch(`${API}/api/acciones/pozos-lista`).then((r) => r.json());
      setPozos(res.pozos ?? []);
    } catch {
      // silencioso
    }
  }, []);

  useEffect(() => {
    fetchPozos();
  }, [fetchPozos]);

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

  // ==========================================================
  // Derived data for filter dropdowns
  // ==========================================================

  const baterias = useMemo(() => {
    const set = new Set(acciones.map((a) => a.bateria).filter(Boolean));
    return Array.from(set).sort();
  }, [acciones]);

  const meses = useMemo(() => {
    const set = new Set(
      acciones.map((a) => a.fecha_accion?.slice(0, 7)).filter(Boolean)
    );
    return Array.from(set).sort().reverse();
  }, [acciones]);

  // ==========================================================
  // CRUD handlers
  // ==========================================================

  async function handleCreate() {
    if (!form.nombre_pozo || !form.sist_extraccion || !form.fecha_accion || !form.tipo || !form.accion) {
      alert("Completá los campos obligatorios (*)");
      return;
    }
    setSaving(true);
    try {
      const body: Record<string, string | null> = {
        nombre_pozo:       form.nombre_pozo,
        bateria:           form.bateria,
        sist_extraccion:   form.sist_extraccion,
        fecha_accion:      form.fecha_accion,
        fecha_realizacion: form.fecha_realizacion || null,
        fecha_fin:         form.fecha_fin || null,
        tipo:              form.tipo,
        accion:            form.accion,
      };
      const res = await fetch(`${API}/api/acciones`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error(await res.text());
      setModalOpen(false);
      setForm(FORM_EMPTY);
      fetchAll();
    } catch (e: unknown) {
      alert(e instanceof Error ? e.message : "Error guardando");
    } finally {
      setSaving(false);
    }
  }

  function openEdit(accion: Accion) {
    setEditId(accion.id);
    setForm({
      nombre_pozo:       accion.nombre_pozo,
      bateria:           accion.bateria,
      sist_extraccion:   accion.sist_extraccion,
      fecha_accion:      accion.fecha_accion ?? "",
      fecha_realizacion: accion.fecha_realizacion ?? "",
      fecha_fin:         accion.fecha_fin ?? "",
      tipo:              accion.tipo,
      accion:            accion.accion,
    });
    setEditOpen(true);
  }

  async function handleEdit() {
    if (!editId) return;
    setSaving(true);
    try {
      const body: Record<string, string | null> = {
        nombre_pozo:       form.nombre_pozo,
        bateria:           form.bateria,
        sist_extraccion:   form.sist_extraccion,
        fecha_accion:      form.fecha_accion,
        fecha_realizacion: form.fecha_realizacion || null,
        fecha_fin:         form.fecha_fin || null,
        tipo:              form.tipo,
        accion:            form.accion,
      };
      const res = await fetch(`${API}/api/acciones/${editId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error(await res.text());
      setEditOpen(false);
      setEditId(null);
      setForm(FORM_EMPTY);
      fetchAll();
    } catch (e: unknown) {
      alert(e instanceof Error ? e.message : "Error actualizando");
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete() {
    if (!deleteId) return;
    setDeleting(true);
    try {
      const res = await fetch(`${API}/api/acciones/${deleteId}`, { method: "DELETE" });
      if (!res.ok) throw new Error(await res.text());
      setDeleteOpen(false);
      setDeleteId(null);
      setExpandedId(null);
      fetchAll();
    } catch (e: unknown) {
      alert(e instanceof Error ? e.message : "Error eliminando");
    } finally {
      setDeleting(false);
    }
  }

  function clearFilters() {
    setFilterPozo("");
    setFilterBateria("");
    setFilterEstado("");
    setFilterTipo("");
    setFilterSist("");
    setFilterMes("");
    setFilterBusqueda("");
  }

  const activeFilters = [filterPozo, filterBateria, filterEstado, filterTipo, filterSist, filterMes, filterBusqueda].filter(Boolean).length;

  // ==========================================================
  // Render
  // ==========================================================

  return (
    <div className="min-h-screen bg-[#0f172a] text-slate-100">
      <div className="max-w-7xl mx-auto px-6 py-8 space-y-6">

        {/* ─── Header ─── */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
              <Wrench size={22} className="text-sky-400" />
              Acciones de Optimización
            </h1>
            <p className="text-sm text-slate-500 mt-1">
              Seguimiento de intervenciones por pozo
            </p>
          </div>
          <button
            onClick={() => { setForm(FORM_EMPTY); setModalOpen(true); }}
            className="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-sky-600 hover:bg-sky-500 text-white text-sm font-semibold transition-all shadow-lg shadow-sky-900/40 hover:shadow-sky-900/60"
          >
            <Plus size={16} />
            Nueva Acción
          </button>
        </div>

        {/* ─── KPIs ─── */}
        <div className="grid grid-cols-3 gap-4">
          {[
            { label: "Total acciones", value: kpis.total, color: "text-sky-400", bg: "bg-sky-500/10 border-sky-500/20" },
            { label: "En proceso", value: kpis.en_proceso, color: "text-amber-400", bg: "bg-amber-500/10 border-amber-500/20" },
            { label: "Finalizadas", value: kpis.finalizadas, color: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/20" },
          ].map(({ label, value, color, bg }) => (
            <div key={label} className={`rounded-xl border p-5 ${bg}`}>
              <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">{label}</p>
              <p className={`text-3xl font-bold ${color}`}>{value}</p>
            </div>
          ))}
        </div>

        {/* ─── Filters bar ─── */}
        <div className="bg-[#1e293b] border border-[#334155] rounded-xl p-4 space-y-4">
          {/* Top row: search + toggle */}
          <div className="flex items-center gap-3">
            <div className="relative flex-1">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
              <input
                type="text"
                placeholder="Buscar en descripción de acciones..."
                value={filterBusqueda}
                onChange={(e) => setFilterBusqueda(e.target.value)}
                className="w-full pl-9 pr-4 py-2 bg-[#0f172a] border border-[#334155] rounded-lg text-sm text-slate-100 placeholder-slate-600 focus:outline-none focus:border-sky-500 focus:ring-1 focus:ring-sky-500/30 transition-colors"
              />
            </div>
            <button
              onClick={() => setShowFilters(!showFilters)}
              className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium border transition-all ${
                showFilters || activeFilters > 0
                  ? "bg-sky-500/20 border-sky-500/40 text-sky-400"
                  : "border-[#334155] text-slate-400 hover:border-slate-500 hover:text-slate-300"
              }`}
            >
              <Filter size={14} />
              Filtros
              {activeFilters > 0 && (
                <span className="bg-sky-500 text-white text-xs rounded-full w-4 h-4 flex items-center justify-center">
                  {activeFilters}
                </span>
              )}
            </button>
            {activeFilters > 0 && (
              <button
                onClick={clearFilters}
                className="text-xs text-slate-500 hover:text-red-400 transition-colors"
              >
                × Limpiar
              </button>
            )}
          </div>

          {/* Expanded filters */}
          {showFilters && (
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 pt-2 border-t border-[#334155]">
              {[
                {
                  label: "Pozo", value: filterPozo, setter: setFilterPozo,
                  opts: Array.from(new Set(acciones.map((a) => a.nombre_pozo))).sort(),
                },
                {
                  label: "Batería", value: filterBateria, setter: setFilterBateria,
                  opts: baterias,
                },
                {
                  label: "Estado", value: filterEstado, setter: setFilterEstado,
                  opts: ["EN PROCESO", "FINALIZADO"],
                },
                {
                  label: "Tipo", value: filterTipo, setter: setFilterTipo,
                  opts: TIPOS,
                },
                {
                  label: "Sist. Ext.", value: filterSist, setter: setFilterSist,
                  opts: SIST_EXTRACCION,
                },
                {
                  label: "Mes", value: filterMes, setter: setFilterMes,
                  opts: meses,
                },
              ].map(({ label, value, setter, opts }) => (
                <div key={label}>
                  <label className="block text-xs text-slate-500 mb-1">{label}</label>
                  <select
                    value={value}
                    onChange={(e) => setter(e.target.value)}
                    className="w-full bg-[#0f172a] border border-[#334155] rounded-lg px-2 py-1.5 text-xs text-slate-100 focus:outline-none focus:border-sky-500 transition-colors cursor-pointer"
                  >
                    <option value="">Todos</option>
                    {opts.map((o) => (
                      <option key={o} value={o}>{o}</option>
                    ))}
                  </select>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* ─── Table ─── */}
        <div className="bg-[#1e293b] border border-[#334155] rounded-xl overflow-hidden">
          {/* Table header */}
          <div className="grid grid-cols-[1.8fr_1fr_1fr_1fr_1fr_1fr_auto] gap-0 px-4 py-3 border-b border-[#334155] bg-[#162032]">
            {["Pozo", "Batería", "Sist. Ext.", "Tipo", "F. Acción", "Estado", ""].map((h) => (
              <div key={h} className="text-xs font-semibold text-slate-500 uppercase tracking-wider px-2">
                {h}
              </div>
            ))}
          </div>

          {/* Rows */}
          {loading ? (
            <div className="flex items-center justify-center py-16 gap-3 text-slate-500">
              <Loader2 size={20} className="animate-spin" />
              <span className="text-sm">Cargando acciones...</span>
            </div>
          ) : error ? (
            <div className="flex items-center justify-center py-16 gap-3 text-red-400">
              <AlertCircle size={18} />
              <span className="text-sm">{error}</span>
            </div>
          ) : acciones.length === 0 ? (
            <div className="text-center py-16">
              <Wrench size={32} className="text-slate-700 mx-auto mb-3" />
              <p className="text-slate-500 text-sm">No hay acciones registradas</p>
              <p className="text-slate-600 text-xs mt-1">
                {activeFilters > 0 ? "Probá ajustando los filtros" : "Hacé click en «Nueva Acción» para empezar"}
              </p>
            </div>
          ) : (
            <div className="divide-y divide-[#1e2d3d]">
              {acciones.map((accion) => {
                const expanded = expandedId === accion.id;
                return (
                  <div key={accion.id}>
                    {/* Main row */}
                    <div
                      className="grid grid-cols-[1.8fr_1fr_1fr_1fr_1fr_1fr_auto] gap-0 px-4 py-3.5 items-center cursor-pointer hover:bg-[#162032]/60 transition-colors group"
                      onClick={() => setExpandedId(expanded ? null : accion.id)}
                    >
                      <div className="px-2">
                        <p className="text-sm font-medium text-slate-200 truncate">{accion.nombre_pozo}</p>
                      </div>
                      <div className="px-2">
                        <p className="text-xs text-slate-400 truncate">{accion.bateria || "—"}</p>
                      </div>
                      <div className="px-2">
                        <span className="text-xs font-mono text-slate-300 bg-slate-800 px-2 py-0.5 rounded">
                          {accion.sist_extraccion}
                        </span>
                      </div>
                      <div className="px-2">
                        {tipoBadge(accion.tipo)}
                      </div>
                      <div className="px-2">
                        <p className="text-xs text-slate-400">{fmt(accion.fecha_accion)}</p>
                      </div>
                      <div className="px-2">
                        {estadoBadge(accion.estado)}
                      </div>
                      <div className="px-2 flex items-center gap-1">
                        <button
                          onClick={(e) => { e.stopPropagation(); openEdit(accion); }}
                          className="p-1.5 rounded-lg text-slate-600 hover:text-sky-400 hover:bg-sky-500/10 transition-colors opacity-0 group-hover:opacity-100"
                          title="Editar"
                        >
                          <Pencil size={13} />
                        </button>
                        <button
                          onClick={(e) => { e.stopPropagation(); setDeleteId(accion.id); setDeleteOpen(true); }}
                          className="p-1.5 rounded-lg text-slate-600 hover:text-red-400 hover:bg-red-500/10 transition-colors opacity-0 group-hover:opacity-100"
                          title="Eliminar"
                        >
                          <Trash2 size={13} />
                        </button>
                        <div className="text-slate-600 ml-1">
                          {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                        </div>
                      </div>
                    </div>

                    {/* Expanded detail */}
                    {expanded && (
                      <div className="px-6 pb-5 pt-2 bg-[#0f172a]/50 border-t border-[#1e2d3d]">
                        <div className="grid grid-cols-3 gap-4 mb-4">
                          {[
                            { label: "Fecha Realización", value: fmt(accion.fecha_realizacion) },
                            { label: "Fecha Fin", value: fmt(accion.fecha_fin) },
                            { label: "Última modificación", value: accion.modificado_utc?.slice(0, 16).replace("T", " ") ?? "—" },
                          ].map(({ label, value }) => (
                            <div key={label} className="bg-[#1e293b] rounded-lg p-3 border border-[#334155]">
                              <p className="text-xs text-slate-500 mb-1">{label}</p>
                              <p className="text-sm text-slate-300 font-medium">{value}</p>
                            </div>
                          ))}
                        </div>
                        <div className="bg-[#1e293b] rounded-lg p-4 border border-[#334155]">
                          <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
                            Descripción de la acción
                          </p>
                          <p className="text-sm text-slate-300 leading-relaxed whitespace-pre-wrap">
                            {accion.accion}
                          </p>
                        </div>
                        <div className="mt-3 flex justify-end gap-2">
                          <button
                            onClick={() => openEdit(accion)}
                            className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium bg-sky-500/10 text-sky-400 border border-sky-500/20 hover:bg-sky-500/20 transition-colors"
                          >
                            <Pencil size={12} />
                            Editar
                          </button>
                          <button
                            onClick={() => { setDeleteId(accion.id); setDeleteOpen(true); }}
                            className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium bg-red-500/10 text-red-400 border border-red-500/20 hover:bg-red-500/20 transition-colors"
                          >
                            <Trash2 size={12} />
                            Eliminar
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {/* Footer count */}
          {!loading && acciones.length > 0 && (
            <div className="px-6 py-3 border-t border-[#334155] bg-[#162032]">
              <p className="text-xs text-slate-600">
                Mostrando {acciones.length} acción{acciones.length !== 1 ? "es" : ""}
                {activeFilters > 0 && " (filtrado)"}
              </p>
            </div>
          )}
        </div>
      </div>

      {/* ─── Modal: Nueva acción ─── */}
      <Modal
        open={modalOpen}
        onClose={() => { setModalOpen(false); setForm(FORM_EMPTY); }}
        title="Nueva Acción de Optimización"
      >
        <AccionForm
          form={form}
          setForm={setForm}
          pozos={pozos}
          onSubmit={handleCreate}
          onCancel={() => { setModalOpen(false); setForm(FORM_EMPTY); }}
          loading={saving}
          isEdit={false}
        />
      </Modal>

      {/* ─── Modal: Editar acción ─── */}
      <Modal
        open={editOpen}
        onClose={() => { setEditOpen(false); setEditId(null); setForm(FORM_EMPTY); }}
        title="Editar Acción"
      >
        <AccionForm
          form={form}
          setForm={setForm}
          pozos={pozos}
          onSubmit={handleEdit}
          onCancel={() => { setEditOpen(false); setEditId(null); setForm(FORM_EMPTY); }}
          loading={saving}
          isEdit={true}
        />
      </Modal>

      {/* ─── Modal: Confirmar eliminación ─── */}
      <ConfirmDelete
        open={deleteOpen}
        onClose={() => { setDeleteOpen(false); setDeleteId(null); }}
        onConfirm={handleDelete}
        loading={deleting}
      />
    </div>
  );
}
