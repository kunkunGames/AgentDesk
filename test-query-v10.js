const { execSync } = require('child_process');
const fs = require('fs');

execSync(`sudo -u postgres psql -d agentdesk -c "TRUNCATE auto_queue_entries, auto_queue_runs CASCADE"`);

fs.writeFileSync('setup_data.sql', `
DO $$
DECLARE
    r_id TEXT;
    i INT;
    j INT;
BEGIN
    FOR i IN 1..200000 LOOP
        r_id := 'run_' || i;
        INSERT INTO auto_queue_runs (id, status) VALUES (r_id, 'active');
    END LOOP;

    FOR i IN 1..50 LOOP
        r_id := 'run_' || i;
        FOR j IN 1..50 LOOP
            INSERT INTO auto_queue_entries (id, run_id, status, updated_at) VALUES ('entry_' || i || '_' || j, r_id, 'pending', NOW() - (j * interval '1 minute'));
        END LOOP;
    END LOOP;
END $$;
`);
execSync('sudo -u postgres psql -d agentdesk -f setup_data.sql');

const query1 = `
      SELECT r.id
      FROM auto_queue_runs r
      WHERE r.status = 'active' AND EXISTS (
        SELECT 1
        FROM auto_queue_entries e
        WHERE e.run_id = r.id AND e.status = 'pending'
      )
      ORDER BY (
        SELECT MIN(e.updated_at)
        FROM auto_queue_entries e
        WHERE e.run_id = r.id AND e.status = 'pending'
      ) ASC LIMIT 50
`;

const query_fast = `
      SELECT r.id
      FROM auto_queue_runs r
      JOIN LATERAL (
        SELECT e.updated_at
        FROM auto_queue_entries e
        WHERE e.run_id = r.id AND e.status = 'pending'
        ORDER BY e.updated_at ASC
        LIMIT 1
      ) oldest_entry ON true
      WHERE r.status = 'active'
      ORDER BY oldest_entry.updated_at ASC
      LIMIT 50
`;

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query1}" > q1.out`);
execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query_fast}" > qf.out`);

console.log('Q1 output:\n', fs.readFileSync('q1.out', 'utf8'));
console.log('\n\nQF output:\n', fs.readFileSync('qf.out', 'utf8'));
