const { execSync } = require('child_process');
const fs = require('fs');

const query_fast2 = `
      SELECT e.run_id as id
      FROM auto_queue_entries e
      JOIN auto_queue_runs r ON e.run_id = r.id
      WHERE e.status = 'pending' AND r.status = 'active'
      GROUP BY e.run_id
      ORDER BY MIN(e.updated_at) ASC
      LIMIT 50
`;

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query_fast2}" > qf2.out`);
console.log('QF2 output:\n', fs.readFileSync('qf2.out', 'utf8'));

const qf_distinct = `
      SELECT DISTINCT r.id
      FROM auto_queue_runs r
      JOIN auto_queue_entries e ON e.run_id = r.id AND e.status = 'pending'
      WHERE r.status = 'active'
      ORDER BY r.id ASC
      LIMIT 50
`
execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${qf_distinct}" > qfd.out`);
console.log('QFD output:\n', fs.readFileSync('qfd.out', 'utf8'));
