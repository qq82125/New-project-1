"use client";

type Props = {
  pageIndex: number;
  hasPrev: boolean;
  hasNext: boolean;
  onPrev: () => void;
  onNext: () => void;
  onFirst?: () => void;
  disabled?: boolean;
};

export default function Pagination({ pageIndex, hasPrev, hasNext, onPrev, onNext, onFirst, disabled = false }: Props) {
  return (
    <div className="flex items-center gap-2">
      <button
        className="rounded border border-line bg-panel px-3 py-2 text-sm disabled:opacity-40"
        disabled={!hasPrev || disabled}
        onClick={onPrev}
      >
        Prev
      </button>
      <span className="px-2 text-sm text-muted">Page {pageIndex}</span>
      <button
        className="rounded border border-line bg-panel px-3 py-2 text-sm disabled:opacity-40"
        disabled={!hasNext || disabled}
        onClick={onNext}
      >
        Next
      </button>
      {onFirst ? (
        <button
          className="rounded border border-line bg-panel px-3 py-2 text-sm disabled:opacity-40"
          disabled={pageIndex <= 1 || disabled}
          onClick={onFirst}
        >
          First
        </button>
      ) : null}
    </div>
  );
}

