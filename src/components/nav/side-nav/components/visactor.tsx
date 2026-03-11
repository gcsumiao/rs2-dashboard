export default function VisActor() {
  return (
    <div className="relative my-2 flex flex-col items-center justify-center gap-y-2 px-4 py-4">
      <div className="dot-matrix absolute left-0 top-0 -z-10 h-full w-full" />
      <span className="text-xs text-muted-foreground">RS2 Analytics</span>
      <p className="text-center text-xs text-muted-foreground">
        UTC metrics
        <br />
        default: 2026-02-01 to 2026-02-28
      </p>
    </div>
  );
}
