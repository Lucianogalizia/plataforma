"use client";

import { useState, useMemo } from "react";

export interface ColDef {
  key: string;
  label: string;
  render?: (val: any, row: any) => React.ReactNode;
  className?: string;
}

interface SortableTableProps {
  cols: ColDef[];
  rows: Record<string, any>[];
  maxHeight?: string;
  title?: string;
  emptyMsg?: string;
}

function downloadCSV(cols: ColDef[], rows: Record<string, any>[], title: string) {
  const header = cols.map((c) => c.label).join(",");
  const body = rows
    .map((r) => cols.map((c) => {
      const v = r[c.key];
      const s = v == null ? "" : String(v);
      return s.includes(",") ? `"${s}"` : s;
    }).join(","))
    .join("\n");
  const blob = new Blob([header + "\n" + body], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${title || "tabla"}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

export default function SortableTable({
  cols, rows, maxHeight = "420px", title = "tabla", emptyMsg = "Sin datos.",
}: SortableTableProps) {
  const [sortKey, setSortKey]   = useState<string | null>(null);
  const [sortAsc, setSortAsc]   = useState(true);

  const sorted = useMemo(() => {
    if (!sortKey) return rows;
    return [...rows].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number") {
        return sortAsc ? av - bv : bv - av;
      }
      return sortAsc
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
  }, [rows, sortKey, sortAsc]);

  function handleSort(key: string) {
    if (sortKey === key) setSortAsc((a) => !a);
    else { setSortKey(key); setSortAsc(true); }
  }

  return (
    <div className="card p-0 overflow-hidden">
      {/* Toolbar */}
      <div className="flex items-center justify-end gap-2 px-3 py-2 border-b border-[#334155]">
        <span className="text-xs text-slate-500 mr-auto">{rows.length} filas</span>
        <button
          onClick={() => downloadCSV(cols, sorted, title)}
          title="Descargar CSV"
          className="text-xs px-2 py-1 rounded border border-slate-600 text-slate-400 hover:border-sky-400 hover:text-sky-400 transition-colors"
        >
          ⬇ CSV
        </button>
        {sortKey && (
          <button
            onClick={() => { setSortKey(null); setSortAsc(true); }}
            title="Limpiar orden"
            className="text-xs px-2 py-1 rounded border border-slate-600 text-slate-400 hover:border-red-400 hover:text-red-400 transition-colors"
          >
            ✕ orden
          </button>
        )}
      </div>

      {/* Tabla */}
      <div className="overflow-x-auto overflow-y-auto" style={{ maxHeight }}>
        <table className="w-full text-xs">
          <thead className="sticky top-0 z-10">
            <tr>
              {cols.map((c) => (
                <th
                  key={c.key}
                  onClick={() => handleSort(c.key)}
                  className="text-left text-xs text-slate-400 px-3 py-2.5 bg-[#1e293b] border-b border-[#334155] whitespace-nowrap cursor-pointer select-none hover:text-sky-400 transition-colors"
                >
                  {c.label}
                  {sortKey === c.key ? (sortAsc ? " ▲" : " ▼") : " ↕"}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((row, i) => (
              <tr key={i} className="border-b border-[#334155] hover:bg-slate-800/40">
                {cols.map((c) => (
                  <td key={c.key} className={`px-3 py-2 text-slate-300 whitespace-nowrap ${c.className || ""}`}>
                    {c.render ? c.render(row[c.key], row) : (row[c.key] ?? "—")}
                  </td>
                ))}
              </tr>
            ))}
            {sorted.length === 0 && (
              <tr>
                <td colSpan={cols.length} className="px-3 py-8 text-center text-slate-500">
                  {emptyMsg}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
