"use client";

type Props = {
  value: string;
  onChange: (v: string) => void;
};

export default function SearchBar({ value, onChange }: Props) {
  return (
    <input
      className="w-full rounded-md border border-line bg-bg px-3 py-2 text-sm outline-none focus:border-blue-400"
      placeholder="搜索标题/摘要/source_id/url"
      value={value}
      onChange={(e) => onChange(e.target.value)}
    />
  );
}
