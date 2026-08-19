const { execSync } = require('child_process');
const fs = require('fs');

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
