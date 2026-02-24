"use client";

interface KPICardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  color?: "sky" | "green" | "yellow" | "orange" | "red" | "default";
  icon?: React.ReactNode;
}

const colorMap = {
  sky:     "text-sky-400",
  green:   "text-green-400",
  yellow:  "text-yellow-400",
  orange:  "text-orange-400",
  red:     "text-red-400",
  default: "text-slate-200",
};

export default function KPICard({
  title,
  value,
  subtitle,
  color = "default",
  icon,
}: KPICardProps) {
  return (
    <div className="card flex flex-col gap-1">
      <div className="flex items-center justify-between">
        <span className="text-xs text-slate-400 uppercase tracking-wide">{title}</span>
        {icon && <span className="text-slate-500">{icon}</span>}
      </div>
      <span className={`text-2xl font-bold ${colorMap[color]}`}>
        {typeof value === "number"
          ? value.toLocaleString("es-AR")
          : value}
      </span>
      {subtitle && (
        <span className="text-xs text-slate-500">{subtitle}</span>
      )}
    </div>
  );
}
