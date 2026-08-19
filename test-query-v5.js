const { execSync } = require('child_process');
const fs = require('fs');

const query5 = `
      SELECT DISTINCT r.id
      FROM auto_queue_runs r
      JOIN auto_queue_entries e ON e.run_id = r.id AND e.status = 'pending'
      WHERE r.status = 'active'
      ORDER BY r.id ASC
      LIMIT 50
`;

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query5}" > q5.out`);
console.log('Q5 output:\n', fs.readFileSync('q5.out', 'utf8'));

const query6 = `
      SELECT r.id
      FROM auto_queue_runs r
      WHERE r.status = 'active' AND EXISTS (
        SELECT 1
        FROM auto_queue_entries e
        WHERE e.run_id = r.id AND e.status = 'pending'
      )
      LIMIT 50
`;

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query6}" > q6.out`);
console.log('\nQ6 output:\n', fs.readFileSync('q6.out', 'utf8'));
