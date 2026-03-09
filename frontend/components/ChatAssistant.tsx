"use client";

import { useState, useRef, useEffect } from "react";
import { MessageCircle, X, Send, Loader2, Bot } from "lucide-react";

// ─── Tipos ────────────────────────────────────────────────
interface Mensaje {
  role: "user" | "assistant";
  content: string;
}

// ─── URL del backend (misma variable que usa el resto del frontend) ────────────
const API_BASE =
  process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

// ─── Componente principal ─────────────────────────────────
export default function ChatAssistant() {
  const [abierto, setAbierto]       = useState(false);
  const [mensajes, setMensajes]     = useState<Mensaje[]>([
    {
      role: "assistant",
      content:
        "Hola. Soy el asistente de DINA.\n\nPuedo consultarte sobre diagnósticos de pozos, acciones de optimización y el estado general del campo.\n\n¿En qué te ayudo?",
    },
  ]);
  const [input, setInput]           = useState("");
  const [cargando, setCargando]     = useState(false);
  const bottomRef                   = useRef<HTMLDivElement>(null);
  const inputRef                    = useRef<HTMLTextAreaElement>(null);

  // Auto-scroll al último mensaje
  useEffect(() => {
    if (abierto) {
      bottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [mensajes, abierto]);

  // Focus al abrir
  useEffect(() => {
    if (abierto) {
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [abierto]);

  // ─── Enviar mensaje ──────────────────────────────────────
  async function enviar() {
    const texto = input.trim();
    if (!texto || cargando) return;

    const nuevosMensajes: Mensaje[] = [
      ...mensajes,
      { role: "user", content: texto },
    ];
    setMensajes(nuevosMensajes);
    setInput("");
    setCargando(true);

    try {
      // Historial sin el mensaje del sistema inicial
      const historial = nuevosMensajes.slice(1, -1).map((m) => ({
        role:    m.role,
        content: m.content,
      }));

      const res = await fetch(`${API_BASE}/api/chat`, {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ mensaje: texto, historial }),
      });

      if (!res.ok) {
        throw new Error(`Error ${res.status}`);
      }

      const data: { respuesta: string; tools_usadas: string[] } =
        await res.json();

      setMensajes([
        ...nuevosMensajes,
        { role: "assistant", content: data.respuesta },
      ]);
    } catch (err) {
      setMensajes([
        ...nuevosMensajes,
        {
          role:    "assistant",
          content: "⚠️ No pude conectar con el servidor. Verificá que el backend esté corriendo.",
        },
      ]);
    } finally {
      setCargando(false);
    }
  }

  // Enter envía, Shift+Enter nueva línea
  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      enviar();
    }
  }

  // ─── Render de un mensaje ────────────────────────────────
  function BurbujaMensaje({ msg }: { msg: Mensaje }) {
    const esUsuario = msg.role === "user";
    return (
      <div
        className={`flex gap-2 ${esUsuario ? "justify-end" : "justify-start"}`}
      >
        {/* Avatar asistente */}
        {!esUsuario && (
          <div className="flex-shrink-0 w-7 h-7 rounded-full bg-dina-accent/20 border border-dina-accent/40 flex items-center justify-center mt-0.5">
            <Bot size={14} className="text-dina-accent" />
          </div>
        )}

        <div
          className={`max-w-[82%] rounded-xl px-3 py-2 text-sm leading-relaxed whitespace-pre-wrap ${
            esUsuario
              ? "bg-dina-accent text-slate-900 font-medium"
              : "bg-slate-700/70 text-slate-100 border border-slate-600/40"
          }`}
        >
          {msg.content}
        </div>
      </div>
    );
  }

  // ─── UI ──────────────────────────────────────────────────
  return (
    <>
      {/* ── Panel de chat ─────────────────────────────────── */}
      {abierto && (
        <div
          className="fixed bottom-20 right-4 z-50 flex flex-col"
          style={{
            width:        "360px",
            height:       "520px",
            background:   "#1e293b",
            border:       "1px solid #334155",
            borderRadius: "16px",
            boxShadow:    "0 20px 60px rgba(0,0,0,0.5)",
          }}
        >
          {/* Header */}
          <div
            className="flex items-center justify-between px-4 py-3 border-b border-slate-700"
            style={{ borderRadius: "16px 16px 0 0" }}
          >
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-dina-green animate-pulse" />
              <span className="text-sm font-semibold text-slate-100">
                Asistente DINA
              </span>
            </div>
            <button
              onClick={() => setAbierto(false)}
              className="text-slate-400 hover:text-slate-100 transition-colors"
            >
              <X size={16} />
            </button>
          </div>

          {/* Mensajes */}
          <div className="flex-1 overflow-y-auto px-3 py-3 flex flex-col gap-3 scrollbar-thin scrollbar-thumb-slate-600">
            {mensajes.map((m, i) => (
              <BurbujaMensaje key={i} msg={m} />
            ))}

            {/* Indicador de carga */}
            {cargando && (
              <div className="flex gap-2 justify-start">
                <div className="flex-shrink-0 w-7 h-7 rounded-full bg-dina-accent/20 border border-dina-accent/40 flex items-center justify-center mt-0.5">
                  <Bot size={14} className="text-dina-accent" />
                </div>
                <div className="bg-slate-700/70 border border-slate-600/40 rounded-xl px-3 py-2 flex items-center gap-2">
                  <Loader2 size={13} className="text-dina-accent animate-spin" />
                  <span className="text-xs text-slate-400">Consultando datos...</span>
                </div>
              </div>
            )}

            <div ref={bottomRef} />
          </div>

          {/* Sugerencias rápidas (solo cuando no hay historial largo) */}
          {mensajes.length <= 2 && !cargando && (
            <div className="px-3 pb-2 flex flex-wrap gap-1">
              {[
                "Pozos críticos",
                "KPIs del campo",
                "Acciones pendientes",
              ].map((sugerencia) => (
                <button
                  key={sugerencia}
                  onClick={() => {
                    setInput(sugerencia);
                    setTimeout(() => inputRef.current?.focus(), 50);
                  }}
                  className="text-xs px-2 py-1 rounded-full border border-dina-accent/40 text-dina-accent hover:bg-dina-accent/10 transition-colors"
                >
                  {sugerencia}
                </button>
              ))}
            </div>
          )}

          {/* Input */}
          <div className="px-3 pb-3">
            <div className="flex gap-2 items-end bg-slate-800/60 border border-slate-600/60 rounded-xl px-3 py-2">
              <textarea
                ref={inputRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Preguntá sobre pozos, diagnósticos..."
                disabled={cargando}
                rows={1}
                className="flex-1 bg-transparent text-sm text-slate-100 placeholder-slate-500 resize-none outline-none leading-relaxed disabled:opacity-50"
                style={{ maxHeight: "80px" }}
              />
              <button
                onClick={enviar}
                disabled={!input.trim() || cargando}
                className="flex-shrink-0 w-7 h-7 rounded-lg bg-dina-accent disabled:opacity-30 flex items-center justify-center hover:bg-sky-300 transition-colors"
              >
                <Send size={13} className="text-slate-900" />
              </button>
            </div>
            <p className="text-[10px] text-slate-600 mt-1 text-center">
              Solo responde con datos reales del sistema
            </p>
          </div>
        </div>
      )}

      {/* ── Botón flotante ────────────────────────────────── */}
      <button
        onClick={() => setAbierto((v) => !v)}
        className="fixed bottom-4 right-4 z-50 w-13 h-13 rounded-full shadow-lg flex items-center justify-center transition-all hover:scale-105 active:scale-95"
        style={{
          width:      "52px",
          height:     "52px",
          background: abierto ? "#334155" : "#38bdf8",
          boxShadow:  "0 4px 20px rgba(56,189,248,0.35)",
        }}
        title="Asistente DINA"
      >
        {abierto ? (
          <X size={22} className="text-slate-300" />
        ) : (
          <MessageCircle size={22} className="text-slate-900" />
        )}
      </button>
    </>
  );
}
