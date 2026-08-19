const { execSync } = require('child_process');
const fs = require('fs');

const optimized_query = `
      SELECT r.id
      FROM auto_queue_runs r
      JOIN (
        SELECT e.run_id, MIN(e.updated_at) as min_updated_at
        FROM auto_queue_entries e
        WHERE e.status = 'pending'
        GROUP BY e.run_id
      ) e_min ON e_min.run_id = r.id
      WHERE r.status = 'active'
      ORDER BY e_min.min_updated_at ASC LIMIT 50
`;

execSync(`sudo -u postgres psql -d agentdesk -c "EXPLAIN ANALYZE ${optimized_query}" > qopt.out`);

console.log('\n\nQOPT output:\n', fs.readFileSync('qopt.out', 'utf8'));
