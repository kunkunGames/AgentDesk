#!/bin/bash
# Auto-queue monitor script for Claude Code Monitor tool
# Checks active queue entries every 30s, reports stuck/complete/anomalies

DB="$HOME/.adk/release/data/agentdesk.sqlite"
INTERVAL=30
STUCK_THRESHOLD_MIN=30
NOTIFY_CHANNEL="${AQ_MONITOR_CHANNEL:-1479671298497183835}"

notify_anomaly() {
  local msg="$1"
  sqlite3 "$DB" "INSERT INTO message_outbox (target, content, bot, source) VALUES ('channel:$NOTIFY_CHANNEL', '$msg', 'notify', 'auto-queue-monitor');" 2>/dev/null
  sqlite3 "$DB" "PRAGMA wal_checkpoint(TRUNCATE);" 2>/dev/null
}

while true; do
  # Get active entries
  ENTRIES=$(sqlite3 "$DB" "
    SELECT k.github_issue_number, k.status, k.review_status, e.status as q_status,
           td.status as dispatch_status,
           CAST(ROUND((julianday('now') - julianday(COALESCE(td.updated_at, e.created_at))) * 1440) AS INTEGER) as age_min
    FROM auto_queue_entries e
    JOIN kanban_cards k ON k.id = e.kanban_card_id
    JOIN auto_queue_runs r ON r.id = e.run_id
    LEFT JOIN task_dispatches td ON td.id = e.dispatch_id
    WHERE r.status = 'active' AND e.status NOT IN ('done', 'skipped')
    ORDER BY e.created_at;
  " 2>/dev/null)

  if [ -z "$ENTRIES" ]; then
    # Don't exit — new entries may be added. Just report and continue.
    :
  fi

  # Check each entry
  echo "$ENTRIES" | while IFS='|' read issue card_status review_status q_status dispatch_status age_min; do
    # Stuck: dispatched but no active session for too long
    if [ "$q_status" = "dispatched" ] && [ "$dispatch_status" = "dispatched" ] && [ "${age_min:-0}" -gt "$STUCK_THRESHOLD_MIN" ]; then
      # Verify no active session
      SESSION=$(sqlite3 "$DB" "
        SELECT status FROM sessions
        WHERE active_dispatch_id = (
          SELECT dispatch_id FROM auto_queue_entries e
          JOIN kanban_cards k ON k.id = e.kanban_card_id
          WHERE k.github_issue_number = $issue AND e.status = 'dispatched'
          LIMIT 1
        );
      " 2>/dev/null)
      if [ -z "$SESSION" ] || [ "$SESSION" = "disconnected" ] || [ "$SESSION" = "idle" ]; then
        echo "STUCK: #${issue} dispatched ${age_min}분 경과, 세션 없음 (session=${SESSION:-none})"
        notify_anomaly "🚨 [자동큐 모니터] STUCK: #${issue} dispatched ${age_min}분 경과, 세션 없음"
      fi
    fi

    # Dispatch completed but card not progressing
    if [ "$dispatch_status" = "completed" ] && [ "$q_status" = "dispatched" ]; then
      echo "ANOMALY: #${issue} dispatch completed but queue entry still 'dispatched' (card=${card_status})"
      notify_anomaly "⚠️ [자동큐 모니터] ANOMALY: #${issue} dispatch 완료인데 entry 미갱신 (card=${card_status})"
    fi

    # Review stuck
    if [ "$card_status" = "review" ] && [ "${age_min:-0}" -gt 60 ]; then
      echo "REVIEW_LONG: #${issue} 리뷰 ${age_min}분 경과 (review_status=${review_status})"
      notify_anomaly "⏰ [자동큐 모니터] 리뷰 장기화: #${issue} ${age_min}분 경과"
    fi
  done

  sleep $INTERVAL
done
