"use client";

type Props = {
  checked: boolean;
  onChange: (v: boolean) => void;
  label?: string;
};

export default function CompactToggle({ checked, onChange, label = "Compact mode" }: Props) {
  return (
    <label className="flex cursor-pointer items-center gap-2 text-sm text-muted">
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
      <span>{label}</span>
    </label>
  );
}
