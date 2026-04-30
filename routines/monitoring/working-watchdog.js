agentdesk.routines.register({
  name: "monitoring-working-watchdog",
  tick(ctx) {
    const agent = ctx.agent || {};
    const previousCount =
      ctx.checkpoint && Number.isFinite(ctx.checkpoint.heartbeatCount)
        ? ctx.checkpoint.heartbeatCount
        : 0;
    const heartbeatCount = previousCount + 1;
    const agentStatus = agent.status || "unknown";
    const isIdle = Boolean(agent.is_idle);

    return {
      action: "complete",
      result: {
        status: "ok",
        mode: "script_only",
        agentStatus,
        isIdle,
        currentTaskId: agent.current_task_id || null,
        currentThreadChannelId: agent.current_thread_channel_id || null,
        heartbeatCount,
        checkedAt: ctx.now,
      },
      checkpoint: {
        heartbeatCount,
        lastRequestedAt: ctx.now,
      },
      lastResult: `script-only heartbeat: monitoring=${agentStatus}, idle=${isIdle}, count=${heartbeatCount}`,
    };
  },
});
