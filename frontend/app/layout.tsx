import type { Metadata } from "next";
import "./globals.css";
import ChatAssistant from "@/components/ChatAssistant";
import TopBar from "@/components/TopBar";

export const metadata: Metadata = {
  title: "Plataforma DINA",
  description: "Análisis dinamométrico de pozos petroleros",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="es">
      <body>
        <TopBar />
        {children}
        <ChatAssistant />
      </body>
    </html>
  );
}
