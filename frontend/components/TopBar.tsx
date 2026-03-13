import Image from "next/image";

export default function TopBar() {
  return (
    <div className="fixed top-0 right-0 z-50 px-4 py-2 bg-[#0f172a] border-b border-l border-[#334155] rounded-bl-lg pointer-events-none">
      <Image
        src="/logo-clear.png"
        alt="Clear Petroleum"
        width={130}
        height={44}
        className="object-contain"
        priority
      />
    </div>
  );
}
