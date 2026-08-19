const { execSync } = require('child_process');
const fs = require('fs');

const query7 = `
      SELECT r.id
      FROM auto_queue_runs r
      WHERE r.status = 'active' AND EXISTS (
        SELECT 1
        FROM auto_queue_entries e
        WHERE e.run_id = r.id AND e.status = 'pending'
      )
      ORDER BY r.id ASC LIMIT 50
`;

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query7}" > q7.out`);
console.log('Q7 output:\n', fs.readFileSync('q7.out', 'utf8'));
