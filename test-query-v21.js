const { execSync } = require('child_process');
const fs = require('fs');

const optimized_query_cte = `
      WITH pending_runs AS (
        SELECT run_id, MIN(updated_at) as min_updated_at
        FROM auto_queue_entries
        WHERE status = 'pending'
        GROUP BY run_id
      )
      SELECT r.id
      FROM auto_queue_runs r
      JOIN pending_runs pr ON pr.run_id = r.id
      WHERE r.status = 'active'
      ORDER BY pr.min_updated_at ASC
      LIMIT 50
`;

const q1 = `
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


execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${q1}" > q1.out`);
execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${optimized_query_cte}" > qopt.out`);

console.log('Q1 output:\n', fs.readFileSync('q1.out', 'utf8'));
console.log('\n\nQOPT output:\n', fs.readFileSync('qopt.out', 'utf8'));
