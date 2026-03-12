"use client";

import { useEffect, useState, useMemo } from "react";
import api from "@/lib/api";

interface Parte {
  well_legal_name: string;
  rig_name: string;
  event_id: string;
  date_ops_start: string;
  date_ops_end: string;
  event_objective_1: string;
  event_objective_2: string;
  step_no: number;
  time_from: string;
  time_to: string;
  loc_fed_lease_no: string;
  activity_class_desc: string;
  activity_code_desc: string;
  activity_duration: number;
  expr1: string;
  [key: string]: unknown;
}

interface Evento {
  event_id: string;
  rig_name: string;
  date_ops_start: string;
  date_ops_end: string;
  event_objective_1: string;
  event_objective_2: string;
}

export default function PartesDiariosPage() {
  const [data, setData] = useState<Parte[]>([]);
  const [loading, setLoading] = useState(true);
  const [pozoFiltro, setPozoFiltro] = useState("");
  const [eventoFiltro, setEventoFiltro] = useState("");

  useEffect(() => {
    setLoading(true);
    api.getPartesDiariosDatos({ limit: 50000 })
      .then((res: { data: Parte[] }) => setData(res.data || []))
      .finally(() => setLoading(false));
  }, []);

  // Pozos únicos
  const pozos = useMemo(() => {
    const set = new Set(data.map((d) => d.well_legal_name).filter(Boolean));
    return Array.from(set).sort();
  }, [data]);

  // Filtrar por pozo
  const datosPorPozo = useMemo(() => {
    if (!pozoFiltro) return data;
    return data.filter(
      (d) => d.well_legal_name?.toLowerCase().includes(pozoFiltro.toLowerCase())
    );
  }, [data, pozoFiltro]);

  // Eventos únicos del pozo seleccionado
  const eventos = useMemo(() => {
    const map = new Map<string, Evento>();
    datosPorPozo.forEach((d) => {
      if (d.event_id && !map.has(d.event_id)) {
        map.set(d.event_id, {
          event_id: d.event_id,
          rig_name: d.rig_name,
          date_ops_start: d.date_ops_start,
          date_ops_end: d.date_ops_end,
          event_objective_1: d.event_objective_1,
          event_objective_2: d.event_objective_2,
        });
      }
    });
    return Array.from(map.values()).sort((a, b) =>
      (b.date_ops_start || "").localeCompare(a.date_ops_start || "")
    );
  }, [datosPorPozo]);

  // Filas filtradas por evento
  const filas = useMemo(() => {
    if (!eventoFiltro) return datosPorPozo;
    return datosPorPozo.filter((d) => d.event_id === eventoFiltro);
  }, [datosPorPozo, eventoFiltro]);

  // Exportar a Excel (CSV)
  function exportarCSV() {
    const cols = ["step_no","time_from","time_to","rig_name","loc_fed_lease_no","well_legal_name","activity_class_desc","activity_code_desc","activity_duration","expr1"];
    const header = cols.join(",");
    const rows = filas.map((f) =>
      cols.map((c) => `"${String(f[c] ?? "").replace(/"/g, '""')}"`).join(",")
    );
    const csv = [header, ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "partes_diarios.csv";
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div style={{ padding: "24px", color: "#fff" }}>
      <h1 style={{ fontSize: "20px", fontWeight: "bold", marginBottom: "4px" }}>
        Partes Diarios de Torre
      </h1>

      {/* Filtros */}
      <div style={{
        background: "#1a1a2e", borderRadius: "8px", padding: "20px",
        marginBottom: "24px", border: "1px solid #333"
      }}>
        <h2 style={{ color: "#e53e3e", fontWeight: "bold", marginBottom: "16px" }}>Filtros</h2>
        <div style={{ display: "flex", gap: "24px", flexWrap: "wrap" }}>
          <div style={{ flex: 1, minWidth: "200px" }}>
            <label style={{ display: "block", marginBottom: "6px", fontSize: "13px", color: "#aaa" }}>Pozo</label>
            <input
              type="text"
              value={pozoFiltro}
              onChange={(e) => { setPozoFiltro(e.target.value); setEventoFiltro(""); }}
              placeholder="Ej: YPF.SC.BB-101"
              style={{
                width: "100%", padding: "8px 12px", borderRadius: "6px",
                background: "#fff", color: "#000", border: "1px solid #555", fontSize: "14px"
              }}
              list="pozos-list"
            />
            <datalist id="pozos-list">
              {pozos.map((p) => <option key={p} value={p} />)}
            </datalist>
          </div>
          <div style={{ flex: 2, minWidth: "300px" }}>
            <label style={{ display: "block", marginBottom: "6px", fontSize: "13px", color: "#aaa" }}>Evento</label>
            <select
              value={eventoFiltro}
              onChange={(e) => setEventoFiltro(e.target.value)}
              style={{
                width: "100%", padding: "8px 12px", borderRadius: "6px",
                background: "#fff", color: "#000", border: "1px solid #555", fontSize: "14px"
              }}
            >
              <option value="">— Todos los eventos —</option>
              {eventos.map((ev) => (
                <option key={ev.event_id} value={ev.event_id}>
                  {ev.rig_name} | {ev.date_ops_start} → {ev.date_ops_end} | {ev.event_objective_1} {ev.event_objective_2}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {/* Tabla */}
      <div style={{
        background: "#1a1a2e", borderRadius: "8px", padding: "20px", border: "1px solid #333"
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
          <h2 style={{ color: "#e53e3e", fontWeight: "bold" }}>Detalle intervención</h2>
          <button
            onClick={exportarCSV}
            style={{
              padding: "6px 16px", borderRadius: "6px", border: "1px solid #aaa",
              background: "transparent", color: "#fff", cursor: "pointer", fontSize: "13px"
            }}
          >
            Exportar a Excel
          </button>
        </div>

        {loading ? (
          <p style={{ color: "#aaa" }}>Cargando...</p>
        ) : filas.length === 0 ? (
          <p style={{ color: "#aaa" }}>Sin datos para los filtros seleccionados.</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #444" }}>
                  {["step_no","time_from","time_to","rig_name","loc_fed_lease_no","well_legal_name",
                    "activity_class_desc","activity_code_desc","activity_duration","expr1"].map((col) => (
                    <th key={col} style={{ padding: "8px 12px", textAlign: "left", color: "#aaa", whiteSpace: "nowrap" }}>
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filas.map((f, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid #2a2a3e" }}>
                    {["step_no","time_from","time_to","rig_name","loc_fed_lease_no","well_legal_name",
                      "activity_class_desc","activity_code_desc","activity_duration","expr1"].map((col) => (
                      <td key={col} style={{ padding: "8px 12px", color: "#ddd", maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {String(f[col] ?? "—")}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}