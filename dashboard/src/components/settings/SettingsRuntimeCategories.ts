export interface ConfigField {
  key: string;
  labelKo: string;
  labelEn: string;
  descriptionKo: string;
  descriptionEn: string;
  inputKind?: "number" | "toggle" | "text";
  unit?: string;
  min?: number;
  max?: number;
  step?: number;
}

export const CATEGORIES: Array<{
  id: string;
  titleKo: string;
  titleEn: string;
  descriptionKo: string;
  descriptionEn: string;
  fields: ConfigField[];
}> = [
  {
    id: "dispatch",
    titleKo: "디스패치 제한",
    titleEn: "Dispatch Limits",
    descriptionKo: "자동 재시도 횟수 같은 운영 제한을 조정합니다.",
    descriptionEn: "Adjusts operational limits such as automatic retries.",
    fields: [
      {
        key: "maxRetries",
        labelKo: "최대 재시도 횟수",
        labelEn: "Max retries",
        descriptionKo: "자동 재시도가 허용되는 최대 횟수입니다.",
        descriptionEn: "Maximum number of automatic retries allowed.",
        unit: "",
        min: 1,
        max: 10,
        step: 1,
      },
    ],
  },
  {
    id: "autoQueue",
    titleKo: "자동 큐",
    titleEn: "Auto Queue",
    descriptionKo: "auto-queue entry 실패 재시도 상한과 복구 동작을 조절합니다.",
    descriptionEn: "Controls retry ceilings and recovery behavior for auto-queue entries.",
    fields: [
      {
        key: "internalTasksEnabled",
        labelKo: "내부 칸반 작업 허용",
        labelEn: "Allow internal Kanban tasks",
        descriptionKo: "켜면 GitHub 이슈 없이도 칸반 작업을 오토 큐로 실행합니다. 기본값은 꺼짐입니다.",
        descriptionEn: "Allows Auto Queue to run Kanban tasks without a GitHub issue. Off by default.",
        inputKind: "toggle",
      },
      {
        key: "maxEntryRetries",
        labelKo: "Entry 최대 재시도 횟수",
        labelEn: "Entry max retries",
        descriptionKo: "dispatch 생성 실패가 이 횟수에 도달하면 entry를 failed로 전환합니다.",
        descriptionEn: "Turns an entry into failed after this many dispatch creation failures.",
        unit: "",
        min: 1,
        max: 10,
        step: 1,
      },
      {
        key: "dispatchRateLimitGateEnabled",
        labelKo: "Rate Limit 디스패치 게이트",
        labelEn: "Rate limit dispatch gate",
        descriptionKo: "Provider rate limit이 포화 상태일 때 auto-queue dispatch 생성을 보류합니다.",
        descriptionEn: "Defers auto-queue dispatch creation while the target provider is saturated.",
        inputKind: "toggle",
      },
      {
        key: "dispatchRateLimitGateDangerPct",
        labelKo: "디스패치 게이트 기준",
        labelEn: "Dispatch gate threshold",
        descriptionKo: "이 사용률 이상이면 rate-limit 디스패치 게이트가 entry를 보류합니다.",
        descriptionEn: "Defers entries when provider utilization is at or above this percentage.",
        unit: "%",
        min: 80,
        max: 100,
        step: 1,
      },
    ],
  },
  {
    id: "campaignDag",
    titleKo: "캠페인 DAG",
    titleEn: "Campaign DAG",
    descriptionKo: "최초 계획과 템플릿 승격 검토 담당 및 검증된 DAG 축적 기준을 설정합니다.",
    descriptionEn: "Configures the bootstrap planner, template reviewer and verified DAG accumulation threshold.",
    fields: [
      {
        key: "campaignSuperiorAgentId", inputKind: "text",
        labelKo: "상위 에이전트 ID", labelEn: "Superior agent ID",
        descriptionKo: "최초 DAG와 템플릿 승격을 검토하는 등록 에이전트입니다. 기본값은 codex입니다. 후속 계획과 업무는 기존 담당자가 처리합니다.",
        descriptionEn: "Existing agent assigned to the first DAG and template promotion review. Defaults to codex; later plans and work keep their enrolled agents.",
      },
      {
        key: "campaignDagTemplateMinSamples",
        labelKo: "템플릿 검토 전 DAG 축적 수", labelEn: "Verified DAGs per template review",
        descriptionKo: "실행과 독립 검증이 완료된 회차를 한 건씩 셉니다. 기본 10건이며 재시도·수정 버전은 중복 집계하지 않습니다.",
        descriptionEn: "Counts each independently verified completed occurrence once. Defaults to 10; retries and revisions do not add samples.",
        min: 1, max: 100, step: 1,
      },
    ],
  },
  {
    id: "alerts",
    titleKo: "알림 임계값",
    titleEn: "Alert Thresholds",
    descriptionKo: "사용량 경고를 얼마나 이르게 띄울지 조절합니다.",
    descriptionEn: "Controls how early usage warnings appear.",
    fields: [
      {
        key: "rateLimitWarningPct",
        labelKo: "Rate Limit 경고 수준",
        labelEn: "Rate limit warning level",
        descriptionKo: "이 비율 이상 사용 시 경고 상태로 표시합니다.",
        descriptionEn: "Shows warning state above this usage percentage.",
        unit: "%",
        min: 50,
        max: 99,
        step: 1,
      },
      {
        key: "rateLimitDangerPct",
        labelKo: "Rate Limit 위험 수준",
        labelEn: "Rate limit danger level",
        descriptionKo: "이 비율 이상 사용 시 위험 상태로 표시합니다.",
        descriptionEn: "Shows danger state above this usage percentage.",
        unit: "%",
        min: 60,
        max: 100,
        step: 1,
      },
    ],
  },
  {
    id: "cache",
    titleKo: "캐시 TTL",
    titleEn: "Cache TTL",
    descriptionKo: "사용량 정보를 얼마나 오래 캐시할지 정합니다.",
    descriptionEn: "Controls how long usage data stays cached.",
    fields: [
      {
        key: "rateLimitStaleSec",
        labelKo: "Rate Limit stale 판정",
        labelEn: "Rate limit stale threshold",
        descriptionKo: "이 시간 이후 사용량 데이터를 오래된 것으로 봅니다.",
        descriptionEn: "Marks usage data stale after this duration.",
        unit: "s",
        min: 30,
        max: 1800,
        step: 30,
      },
    ],
  },
];
