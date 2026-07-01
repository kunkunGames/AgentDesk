agentdesk.routines.register({
  name: "script-only-summary",
  tick(ctx) {
    const priorCount =
      ctx.checkpoint && Number.isFinite(ctx.checkpoint.summaryCount)
        ? ctx.checkpoint.summaryCount
        : 0;
    const summaryCount = priorCount + 1;

    return {
      action: "complete",
      result: {
        routineId: ctx.routine.id,
        runId: ctx.run.id,
        summaryCount,
        freshContextGuaranteed: ctx.routine.fresh_context_guaranteed,
      },
      checkpoint: {
        summaryCount,
        lastRunAt: ctx.now,
      },
      lastResult: `script summary completed (${summaryCount})`,
    };
  },
});
