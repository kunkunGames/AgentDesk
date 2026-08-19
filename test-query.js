const { execSync } = require('child_process');

// Generate 100000 entries
const fs = require('fs');

fs.writeFileSync('setup_data.sql', `
DO $$
DECLARE
    r_id TEXT;
    i INT;
    j INT;
BEGIN
    FOR i IN 1..50 LOOP
        r_id := 'run_' || i;
        INSERT INTO auto_queue_runs (id, status) VALUES (r_id, 'active');
        FOR j IN 1..2000 LOOP
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

const query2 = `
      SELECT r.id
      FROM auto_queue_runs r
      JOIN auto_queue_entries e ON e.run_id = r.id
      WHERE r.status = 'active' AND e.status = 'pending'
      GROUP BY r.id
      ORDER BY MIN(e.updated_at) ASC
      LIMIT 50
`;

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query1}" > q1.out`);
execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query2}" > q2.out`);

console.log('Q1 output:\n', fs.readFileSync('q1.out', 'utf8'));
console.log('\n\nQ2 output:\n', fs.readFileSync('q2.out', 'utf8'));
