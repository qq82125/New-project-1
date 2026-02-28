"use client";

const TABS = [
  { key: "", label: "All" },
  { key: "regulatory", label: "Regulatory" },
  { key: "media", label: "Media" },
  { key: "evidence", label: "Evidence" },
  { key: "company", label: "Company" },
  { key: "procurement", label: "Procurement" }
];

type Props = {
  value: string;
  onChange: (v: string) => void;
};

export default function GroupTabs({ value, onChange }: Props) {
  return (
    <div className="flex flex-wrap gap-2">
      {TABS.map((t) => {
        const active = (value || "") === t.key;
        return (
          <button
            key={t.key || "all"}
            className={`rounded-full border px-3 py-1 text-xs ${
              active ? "border-blue-400 bg-blue-500/20 text-blue-200" : "border-line bg-panel text-muted hover:text-text"
            }`}
            onClick={() => onChange(t.key)}
          >
            {t.label}
          </button>
        );
      })}
    </div>
  );
}
