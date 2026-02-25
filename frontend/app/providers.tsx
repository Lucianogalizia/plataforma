"use client";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";

export default function Providers({ children }: { children: React.ReactNode }) {
  const [client] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 60_000,      // 1 min: "fresco" (no refetch al volver rápido)
            gcTime: 30 * 60_000,    // 30 min: guardado en memoria
            refetchOnWindowFocus: false,
            retry: 1
          }
        }
      })
  );

  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
