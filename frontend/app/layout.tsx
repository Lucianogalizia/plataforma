import type { Metadata } from "next";
import "./globals.css";
import ChatAssistant from "@/components/ChatAssistant";

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
        {children}
        <ChatAssistant />
      </body>
    </html>
  );
}
