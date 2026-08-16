1. **Change the log message in `src/services/dispatches/outbox_queue.rs`**.
   - The current log message is: `"[dispatch-outbox] post-notify dispatch bookkeeping failed after status may already be dispatched (non-atomic helper)"`.
   - Update it to: `"[dispatch-outbox] post-notify bookkeeping: failed to mark dispatch as dispatched (it may already be dispatched)"`.
   - This improves observability and clarifies the error state without changing runtime behavior.
2. **Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.**
3. **Submit the change.**
   - Submit the change using branch `jules/courier/post-notify-bookkeeping-observability` and commit message `Courier: Improve post-notify bookkeeping observability`.
