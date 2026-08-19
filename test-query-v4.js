const { execSync } = require('child_process');
const fs = require('fs');

const query4 = `
      SELECT r.id
      FROM auto_queue_runs r
      WHERE r.status = 'active' AND EXISTS (
        SELECT 1
        FROM auto_queue_entries e
        WHERE e.run_id = r.id AND e.status = 'pending'
      )
      ORDER BY (
        SELECT e.updated_at
        FROM auto_queue_entries e
        WHERE e.run_id = r.id AND e.status = 'pending'
        ORDER BY e.updated_at ASC
        LIMIT 1
      ) ASC LIMIT 50
`;

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query4}" > q4.out`);

console.log('Q4 output:\n', fs.readFileSync('q4.out', 'utf8'));
