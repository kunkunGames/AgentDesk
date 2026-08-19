const { execSync } = require('child_process');
const fs = require('fs');

const query3 = `
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

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query3}" > q3.out`);

console.log('Q3 output:\n', fs.readFileSync('q3.out', 'utf8'));
