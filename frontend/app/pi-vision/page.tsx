"use client";

import Sidebar from "@/components/Sidebar";

export default function PiVisionPage() {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className="flex-1 p-8 flex items-center justify-center">
        <div className="text-center space-y-4">
          <div className="text-6xl">🔧</div>
          <h1 className="text-2xl font-bold text-slate-200">PI Vision</h1>
          <p className="text-slate-400">Integración en desarrollo. Próximamente disponible.</p>
        </div>
      </main>
    </div>
  );
}
