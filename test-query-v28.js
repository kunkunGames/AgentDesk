const { execSync } = require('child_process');
const fs = require('fs');

const optimized_query_lateral_true = `
      SELECT r.id
      FROM auto_queue_runs r
      JOIN LATERAL (
        SELECT e.updated_at
        FROM auto_queue_entries e
        WHERE e.run_id = r.id AND e.status = 'pending'
        ORDER BY e.updated_at ASC
        LIMIT 1
      ) e ON true
      WHERE r.status = 'active'
      ORDER BY e.updated_at ASC
      LIMIT 50
`;


execSync(`sudo -u postgres psql -d agentdesk -c "TRUNCATE auto_queue_entries, auto_queue_runs CASCADE"`);

fs.writeFileSync('setup_data3.sql', `
DO $$
DECLARE
    r_id TEXT;
    i INT;
    j INT;
BEGIN
    FOR i IN 1..50 LOOP
        r_id := 'run_' || i;
        INSERT INTO auto_queue_runs (id, status) VALUES (r_id, 'active');
    END LOOP;

    FOR i IN 1..50 LOOP
        r_id := 'run_' || i;
        FOR j IN 1..10000 LOOP
            INSERT INTO auto_queue_entries (id, run_id, status, updated_at) VALUES ('entry_' || i || '_' || j, r_id, 'pending', NOW() - (j * interval '1 minute'));
        END LOOP;
    END LOOP;
END $$;
`);
execSync('sudo -u postgres psql -d agentdesk -f setup_data3.sql');

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${optimized_query_lateral_true}" > qopt_lat.out`);

console.log('QOPT LATERAL output:\n', fs.readFileSync('qopt_lat.out', 'utf8'));
