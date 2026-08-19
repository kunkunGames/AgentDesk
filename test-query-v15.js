const { execSync } = require('child_process');
const fs = require('fs');

const qf_distinct2 = `
      SELECT r.id
      FROM auto_queue_runs r
      JOIN auto_queue_entries e ON e.run_id = r.id
      WHERE r.status = 'active' AND e.status = 'pending'
      GROUP BY r.id
      ORDER BY r.id ASC
      LIMIT 50
`
execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${qf_distinct2}" > qfd2.out`);
console.log('QFD2 output:\n', fs.readFileSync('qfd2.out', 'utf8'));
