import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        "dina-dark": "#0f172a",
        "dina-card": "#1e293b",
        "dina-border": "#334155",
        "dina-accent": "#38bdf8",
        "dina-green": "#22c55e",
        "dina-yellow": "#eab308",
        "dina-orange": "#f97316",
        "dina-red": "#ef4444",
      },
    },
  },
  plugins: [],
};
export default config;
