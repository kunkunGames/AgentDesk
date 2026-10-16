import { useEffect, useState } from "react";
import { getCampaignExecution, type CampaignExecution } from "../../api/campaignExecution";
import type { Tr } from "./campaignPresentation";

export default function CampaignExecutionStatus({ id, tr }: { id: string; tr: Tr }) {
  const [open, setOpen] = useState(false);
  const [state, setState] = useState<CampaignExecution | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    if (!open) return;
    let alive = true;
    let busy = false;
    async function refresh() {
      if (busy) return;
      busy = true;
      try {
        const value = await getCampaignExecution(id);
        if (alive) { setState(value); setError(null); }
      } catch (cause) { if (alive) setError(cause instanceof Error ? cause.message : String(cause)); }
      finally { busy = false; }
    }
    void refresh();
    const timer = window.setInterval(() => { if (document.visibilityState === "visible") void refresh(); }, 15_000);
    return () => { alive = false; window.clearInterval(timer); };
  }, [id, open]);
  return <section className="campaign-execution">
    <button aria-expanded={open} onClick={() => setOpen(!open)}>{tr("에이전트 실행 현황", "Agent execution")}</button>
    {open && <div className="campaign-execution-body">
      {error && <p role="alert">{tr("상태를 갱신하지 못했습니다: ", "Could not refresh state: ")}{error}</p>}
      {!state && !error && <p>{tr("불러오는 중…", "Loading…")}</p>}
      {state && !state.enrolled && <p>{tr("자동 그래프 실행이 등록되지 않은 캠페인입니다.", "This campaign is not enrolled in automatic graph execution.")}</p>}
      {state?.enrolled && <>
        <p><strong>{state.goal.objective}</strong></p>
        {state.hold_reason && <p role="status">{tr("보류: ", "Held: ")}{state.hold_reason}</p>}
        {state.preflight && !state.preflight.ready && <div role="status"><strong>{tr("실행 전 조건 대기", "Prerequisites waiting")}</strong><ul>{state.preflight.checks.filter(c=>!c.ready).map((c,i)=><li key={i}>{c.requirement.kind} · {String(c.requirement.name ?? c.requirement.path ?? c.requirement.agent_id ?? "")} · {c.reason_code}</li>)}</ul></div>}
        <dl className="campaign-execution-metrics">
          <div><dt>{tr("활성 그래프", "Active graph")}</dt><dd>{state.active_version ?? tr("계획 대기", "Awaiting plan")}</dd></div>
          <div><dt>{tr("발행 작업 / 한도", "Published tasks / limit")}</dt><dd>{state.tasks.length} / {state.goal.budget.max_dispatches}</dd></div>
          <div><dt>{state.goal.schedule ? tr("부모 제어 작업 누적 시간", "Parent control dispatch lifetime") : tr("누적 디스패치 시간", "Cumulative dispatch lifetime")}</dt><dd>{Math.ceil(state.elapsed_dispatch_seconds)} / {state.goal.budget.max_elapsed_secs}s</dd></div>
          <div><dt>{tr("관측된 도구 호출", "Observed tool calls")}</dt><dd>{state.observed_tool_calls}</dd></div>
        </dl>
        {state.goal.schedule && state.occurrence_budget && <p>{tr("회차별 업무 예산", "Budget for each occurrence")}: {state.occurrence_budget.max_dispatches} {tr("작업", "tasks")} · {state.occurrence_budget.max_elapsed_secs}s · {tr("노드 제한", "Node timeout")} {state.occurrence_budget.node_timeout_secs}s · {tr("관측 도구 한도", "Observed tool limit")} {state.occurrence_budget.max_observed_tool_calls ?? tr("미설정", "Unconfigured")}</p>}
        <p className="campaign-muted">{tr("도구 호출은 수신된 이벤트 기준입니다. 실시간 과금 정보는 제공되지 않습니다.", "Tool counts reflect received events. Live billing data is unavailable.")}</p>
        {state.dag_learning && <>
          <dl className="campaign-execution-metrics">
            <div><dt>{tr("최초 계획·승격 검토", "Bootstrap / promotion review")}</dt><dd>{state.dag_learning.superior_agent_id}</dd></div>
            <div><dt>{tr("검증 완료 DAG / 검토 기준", "Verified DAGs / review threshold")}</dt><dd>{state.dag_learning.sample_count} / {state.dag_learning.min_samples}</dd></div>
            {state.dag_learning.samples_until_review !== undefined && <div><dt>{tr("최근 검토 이후 / 다음 검토까지", "New samples / remaining until review")}</dt><dd>{state.dag_learning.new_samples_since_review} / {state.dag_learning.samples_until_review}</dd></div>}
            <div><dt>{tr("DAG 템플릿", "DAG template")}</dt><dd>{state.dag_learning.active_template ? `v${state.dag_learning.active_template.version}` : tr("축적·검토 중", "Accumulating / awaiting review")}</dd></div>
          </dl>
          {state.dag_learning.review_wait_reason && <p role="status">{tr("템플릿 검토 대기: ", "Template review waiting: ")}{state.dag_learning.review_wait_reason}</p>}
          {state.dag_learning.reviews.length > 0 && <details><summary>{tr("템플릿 승격 검토 이력", "Template promotion reviews")}</summary><ul>{state.dag_learning.reviews.map((r) => <li key={r.id}>{r.superior_agent_id} · {r.sample_count} · {r.status}{r.reason && <> · {r.reason}</>}</li>)}</ul></details>}
        </>}
        {state.dag_learning?.recurring === false && <p role="status">{tr("반복 실행이 설정되지 않았습니다. 추가 검증 완료 실행이 있어야 다음 템플릿 검토가 가능합니다.", "Recurrence is not configured. Further verified executions are needed for the next template review.")}</p>}
        {state.metrics && <details><summary>{tr("계획·검증·실행 지표", "Planning, validation and execution metrics")}</summary>
          <ul>{state.metrics.roles.map(r=><li key={`${r.role}:${r.status}`}>{r.role} · {r.status} · {r.dispatches} · {Math.ceil(r.elapsed_seconds)}s</li>)}</ul>
          <p>{tr("시간은 대기 시간을 포함한 디스패치 수명입니다. 모델·사용량은 관측된 값만 표시합니다.", "Time is dispatch lifetime including waits. Models and usage appear only when observed.")}</p>
          <ul>{state.metrics.profiles.map(p=><li key={p.task_key}>{p.role} · {p.accepted ? tr("업무 수락", "Business accepted") : tr("업무 미수락", "Business not accepted")} · {p.observed_provider ?? tr("실행 제공자 미확인", "Provider unobserved")} · {p.observed_model ?? tr("모델 미확인", "Model unobserved")}</li>)}</ul>
        </details>}
        {state.goal.schedule && <p>{tr("예약", "Schedule")}: {state.goal.schedule.timezone} · {state.goal.schedule.daily_minutes.map((m) => `${String(Math.floor(m / 60)).padStart(2, "0")}:${String(m % 60).padStart(2, "0")}`).join(", ")}</p>}
        <ul>{state.versions.map((v) => <li key={v.version}>{tr("그래프", "Graph")} v{v.version} · {v.status} · <code>{v.digest.slice(0, 12)}</code></li>)}</ul>
        {state.failures.length > 0 && <details><summary>{tr("포착된 실패", "Captured failures")} ({state.failures.length})</summary><ul>{state.failures.map((f) => <li key={f.id}><strong>{f.category}</strong>: {f.evidence.failure?.summary ?? f.evidence.failure?.error ?? f.evidence.failure?.reason ?? f.evidence.reason ?? f.task_key}{f.evidence.dispatch_id && <small> · {tr("디스패치", "Dispatch")} {f.evidence.dispatch_id}</small>}</li>)}</ul></details>}
        {state.occurrences.length > 0 && <details><summary>{tr("예약 실행 이력", "Scheduled executions")}</summary><ul>{state.occurrences.map((o) => <li key={o.child_id}>{o.scheduled_at} · {o.status ?? tr("상태 미확인", "State unobserved")} · {o.child_id}{o.published_work_tasks !== undefined && <small> · {tr("업무 수락", "Business accepted")} {o.accepted_work_tasks} / {o.published_work_tasks}</small>}{o.sampled && <small> · {tr("DAG 표본 반영", "DAG sample recorded")}</small>}{o.hold_reason && <p role="status">{tr("보류: ", "Held: ")}{o.hold_reason}</p>}</li>)}</ul></details>}
      </>}
    </div>}
  </section>;
}
