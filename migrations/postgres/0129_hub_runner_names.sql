-- Coordinated fleet cutover: stop all nodes before this migration. Historical
-- migrations stay immutable; the active schema uses one naming vocabulary.
DO $$
BEGIN
    IF NOT pg_try_advisory_xact_lock(7801100::bigint) THEN
        RAISE EXCEPTION 'Stop the hub before applying the coordinated node naming migration';
    END IF;
    IF EXISTS (SELECT 1 FROM node_execution_leases WHERE expires_at > clock_timestamp())
       OR EXISTS (SELECT 1 FROM intake_outbox WHERE status IN ('claimed','accepted','spawned','dispatched')) THEN
        RAISE EXCEPTION 'Drain node executions before applying the coordinated naming migration';
    END IF;
END
$$;
LOCK TABLE worker_nodes, worker_mcp_endpoints IN ACCESS EXCLUSIVE MODE;

ALTER TABLE worker_nodes RENAME TO cluster_nodes;
ALTER TABLE cluster_nodes RENAME CONSTRAINT worker_nodes_pkey TO cluster_nodes_pkey;
ALTER TABLE cluster_nodes ALTER COLUMN effective_role SET DEFAULT 'runner';
ALTER INDEX idx_worker_nodes_status_heartbeat RENAME TO idx_cluster_nodes_status_heartbeat;
ALTER INDEX idx_worker_nodes_effective_role RENAME TO idx_cluster_nodes_effective_role;

ALTER TABLE worker_mcp_endpoints RENAME TO node_mcp_endpoints;
ALTER TABLE node_mcp_endpoints RENAME CONSTRAINT worker_mcp_endpoints_pkey TO node_mcp_endpoints_pkey;
ALTER TABLE node_mcp_endpoints RENAME CONSTRAINT worker_mcp_endpoints_instance_id_fkey TO node_mcp_endpoints_instance_id_fkey;
ALTER TABLE node_mcp_endpoints DROP CONSTRAINT node_mcp_endpoints_instance_id_fkey;
ALTER TABLE node_mcp_endpoints ADD CONSTRAINT node_mcp_endpoints_instance_id_fkey
    FOREIGN KEY(instance_id) REFERENCES cluster_nodes(instance_id) ON UPDATE CASCADE ON DELETE CASCADE;
ALTER INDEX idx_worker_mcp_endpoints_endpoint_healthy RENAME TO idx_node_mcp_endpoints_endpoint_healthy;
ALTER INDEX idx_intake_outbox_worker_pending RENAME TO idx_intake_outbox_runner_pending;

UPDATE cluster_nodes
SET role = CASE role WHEN 'leader' THEN 'hub' WHEN 'worker' THEN 'runner' ELSE role END,
    effective_role = CASE effective_role WHEN 'leader' THEN 'hub' WHEN 'worker' THEN 'runner' ELSE effective_role END,
    capabilities = (capabilities - 'intake_worker' - 'execution_readiness' - 'execution_readiness_version')
        || CASE WHEN capabilities ? 'intake_worker'
            THEN jsonb_build_object('intake_runner', capabilities->'intake_worker')
            ELSE '{}'::jsonb END,
    status = 'offline', last_heartbeat_at = NULL;

ALTER TABLE cluster_nodes ADD CONSTRAINT cluster_nodes_role_valid
    CHECK (role IN ('hub', 'runner', 'auto'));
ALTER TABLE cluster_nodes ADD CONSTRAINT cluster_nodes_effective_role_valid
    CHECK (effective_role IN ('hub', 'runner'));

-- PL/pgSQL bodies are text, so a table rename alone cannot update this lookup.
CREATE OR REPLACE FUNCTION reserve_node_execution(node_id TEXT, bot TEXT, channel TEXT)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE slots INTEGER; occupied BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('agentdesk.execution_capacity.v1'),hashtext(node_id));
    SELECT CASE WHEN capabilities #>> '{execution_capacity,version}'='1'
        THEN (capabilities #>> '{execution_capacity,slots}')::INTEGER END
      INTO slots FROM cluster_nodes WHERE instance_id=node_id;
    IF slots IS NULL THEN RETURN; END IF;
    SELECT count(*) INTO occupied FROM node_execution_occupancy(node_id) o
     WHERE (o.provider,o.channel_id)<>(bot,channel);
    IF slots<1 OR occupied>=slots THEN
        RAISE EXCEPTION 'node execution capacity exhausted: %', node_id
          USING ERRCODE='23514', CONSTRAINT='node_execution_capacity_available';
    END IF;
END
$$;

-- A device identity normally survives a role or hostname change. This explicit
-- maintenance operation is for operators who really need to rename that identity.
-- Run with every AgentDesk process stopped; SQL errors roll back all references.
CREATE FUNCTION rename_cluster_node_identity(old_id TEXT, new_id TEXT)
RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE ref RECORD; changed BIGINT; result JSONB := '{}'::jsonb;
BEGIN
    IF old_id IS NULL OR new_id IS NULL OR old_id = new_id
       OR old_id !~ '^[A-Za-z0-9_.-]{1,128}$' OR new_id !~ '^[A-Za-z0-9_.-]{1,128}$' THEN
        RAISE EXCEPTION 'Provide two distinct valid node identifiers';
    END IF;
    IF NOT pg_try_advisory_xact_lock(7801100::bigint) THEN
        RAISE EXCEPTION 'Stop every AgentDesk process before renaming a node';
    END IF;
    LOCK TABLE cluster_nodes IN ACCESS EXCLUSIVE MODE;
    IF NOT EXISTS (SELECT 1 FROM cluster_nodes WHERE instance_id=old_id)
       OR EXISTS (SELECT 1 FROM cluster_nodes WHERE instance_id=new_id) THEN
        RAISE EXCEPTION 'Source node must exist and destination node must not exist';
    END IF;
    IF EXISTS (SELECT 1 FROM node_execution_leases WHERE expires_at > clock_timestamp())
       OR EXISTS (SELECT 1 FROM intake_outbox WHERE status IN ('claimed','accepted','spawned','dispatched')) THEN
        RAISE EXCEPTION 'Drain node executions before renaming a node';
    END IF;
    -- ON UPDATE CASCADE carries the endpoint foreign key. No aliases are kept.
    UPDATE cluster_nodes SET instance_id=new_id, status='offline', last_heartbeat_at=NULL,
        capabilities=capabilities-'execution_readiness'-'execution_readiness_version'
        WHERE instance_id=old_id;
    FOR ref IN SELECT * FROM (VALUES
        ('node_execution_assignments','instance_id'),
        ('node_execution_leases','instance_id'),
        ('sessions','instance_id'),
        ('agents','default_execution_node_id'),
        ('dispatch_semaphore_holdings','holder_instance_id'),
        ('intake_outbox','forwarded_by_instance_id'),
        ('intake_outbox','owner_instance_id'),
        ('intake_outbox','target_instance_id'),
        ('intake_session_owners','owner_instance_id'),
        ('message_outbox','circuit_owner_instance_id'),
        ('message_outbox_circuit_authority','owner_instance_id'),
        ('resource_locks','holder_instance_id'),
        ('test_phase_runs','holder_instance_id')
    ) AS refs(table_name,column_name) LOOP
        EXECUTE format('UPDATE public.%I SET %I=$1 WHERE %I=$2',
            ref.table_name,ref.column_name,ref.column_name) USING new_id,old_id;
        GET DIAGNOSTICS changed = ROW_COUNT;
        result := result || jsonb_build_object(ref.table_name||'.'||ref.column_name,changed);
    END LOOP;
    FOR ref IN SELECT * FROM (VALUES
        ('agents','execution_requirements',ARRAY['nodes']),
        ('intake_outbox','execution_requirements',ARRAY['nodes']),
        ('task_dispatches','required_capabilities',ARRAY['execution','nodes']),
        ('dispatch_outbox','required_capabilities',ARRAY['execution','nodes'])
    ) AS refs(table_name,column_name,json_path) LOOP
        EXECUTE format(
            'UPDATE public.%1$I SET %2$I=jsonb_set(%2$I,$3,
               (SELECT jsonb_agg(CASE WHEN value=to_jsonb($2) THEN to_jsonb($1) ELSE value END ORDER BY ord)
                FROM jsonb_array_elements(%2$I #> $3) WITH ORDINALITY AS entry(value,ord)),false)
             WHERE jsonb_typeof(%2$I #> $3)=''array'' AND (%2$I #> $3) @> jsonb_build_array($2)',
            ref.table_name,ref.column_name) USING new_id,old_id,ref.json_path;
        GET DIAGNOSTICS changed = ROW_COUNT;
        result := result || jsonb_build_object(ref.table_name||'.'||ref.column_name,changed);
    END LOOP;
    RETURN result;
END
$$;
