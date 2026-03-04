"use client";
import Sidebar from "@/components/Sidebar";

export default function RRHHLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        <main className="flex-1 overflow-auto min-w-0">
          {children}
        </main>
      </div>
    </div>
  );
}
