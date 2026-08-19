const { execSync } = require('child_process');
const fs = require('fs');

const query8 = `
      SELECT r.id
      FROM auto_queue_runs r
      WHERE r.status = 'active'
      AND EXISTS (
        SELECT 1
        FROM auto_queue_entries e
        WHERE e.run_id = r.id AND e.status = 'pending'
      )
      LIMIT 50
`;

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query8}" > q8.out`);
console.log('Q8 output:\n', fs.readFileSync('q8.out', 'utf8'));
