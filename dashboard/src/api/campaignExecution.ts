import { z } from "zod";
import { request } from "./httpClient";

const stateSchema = z.discriminatedUnion("enrolled", [
  z.object({ enrolled: z.literal(false) }),
  z.looseObject({
    enrolled: z.literal(true),
    active_version: z.number().nullable(),
    hold_reason: z.string().nullable(),
    observed_tool_calls: z.number(),
    preflight: z.object({ ready: z.boolean(), checks: z.array(z.object({ ready: z.boolean(), reason_code: z.string(), requirement: z.looseObject({ kind: z.string() }) })) }).nullable().optional(),
    metrics: z.object({ roles: z.array(z.object({ role:z.string(),status:z.string(),dispatches:z.number(),elapsed_seconds:z.number() })),
      profiles: z.array(z.looseObject({ task_key:z.string(),role:z.string(),accepted:z.boolean(),observed_provider:z.string().nullable(),observed_model:z.string().nullable(),observed_usage:z.unknown() })) }).optional(),
    dag_learning: z.object({
      schema_version: z.number(), family_id: z.string().nullable(),
      superior_agent_id: z.string(), min_samples: z.number(), sample_count: z.number(),
      review_wait_reason: z.string().nullable().optional(),
      new_samples_since_review:z.number().optional(), samples_until_review:z.number().optional(),
      last_review_sample_count:z.number().optional(), recurring:z.boolean().optional(),
      active_template: z.looseObject({ id: z.string(), version: z.number(), status: z.string() }).nullable(),
      reviews: z.array(z.looseObject({ id: z.string(), sample_count: z.number(), threshold: z.number(),
        status: z.string(), reason: z.string().nullable(), superior_agent_id: z.string() })),
    }).optional(),
    elapsed_dispatch_seconds: z.number(),
    occurrence_budget: z.object({ max_dispatches: z.number(), max_elapsed_secs: z.number(), node_timeout_secs: z.number(), max_evolutions: z.number(), max_observed_tool_calls: z.number().nullable() }).nullable().optional(),
    goal: z.looseObject({ objective: z.string(), success: z.array(z.string()), constraints: z.array(z.string()),
      budget: z.object({ max_dispatches: z.number(), max_elapsed_secs: z.number(), max_evolutions: z.number() }),
      schedule: z.object({ timezone: z.string(), daily_minutes: z.array(z.number()) }).optional(),
    }),
    versions: z.array(z.looseObject({ version: z.number(), status: z.string(), digest: z.string() })),
    tasks: z.array(z.looseObject({ task_key: z.string(), card_id: z.string(), role: z.string(), accepted: z.boolean() })),
    failures: z.array(z.looseObject({ id: z.string(), category: z.string(), task_key: z.string(),
      evidence: z.looseObject({ dispatch_id: z.string().optional(), reason:z.string().optional(),
        failure: z.looseObject({ summary: z.string().optional(), error: z.string().optional(), reason: z.string().optional() }).optional() }),
    })),
    occurrences: z.array(z.object({ campaign_id: z.string(), child_id: z.string(), scheduled_at: z.string(), status:z.string().optional(),hold_reason:z.string().nullable().optional(),accepted_work_tasks:z.number().optional(),published_work_tasks:z.number().optional(),sampled:z.boolean().optional() })),
  }),
]);
export type CampaignExecution = z.infer<typeof stateSchema>;
export function getCampaignExecution(id: string) {
  return request(`/api/campaigns/${encodeURIComponent(id)}/execution`, { suppressErrorToast: true }, stateSchema);
}
