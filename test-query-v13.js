const { execSync } = require('child_process');
const fs = require('fs');

const query_fast_lateral = `
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

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${query_fast_lateral}" > qf_lateral.out`);

console.log('QF Lateral output:\n', fs.readFileSync('qf_lateral.out', 'utf8'));
