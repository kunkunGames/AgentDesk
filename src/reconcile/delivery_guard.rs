//! Read-only classification of typed delivery receipts and legacy guard rows.

use chrono::Utc;

use super::{
    DeliveryKvGuardRow, DeliveryTypedGuardRow, DispatchDeliveryEventMismatch,
    DispatchDeliveryEventReconcileStats, is_completed_delivery_status,
};

pub(super) fn classify_delivery_kv_guard_mismatches(
    stats: &mut DispatchDeliveryEventReconcileStats,
    mismatches: &mut Vec<DispatchDeliveryEventMismatch>,
    row: DeliveryKvGuardRow,
) {
    let has_notified = row.notified_count > 0;
    let typed_status = row.typed_status.as_deref();
    if typed_status.is_none() {
        let expected = if has_notified { "sent" } else { "reserved" };
        let mismatch = DispatchDeliveryEventMismatch::missing_typed(row.dispatch_id, expected);
        stats.record_mismatch(&mismatch.kind);
        mismatches.push(mismatch);
        return;
    }

    let typed_is_completed = typed_status.is_some_and(is_completed_delivery_status);
    if has_notified && !typed_is_completed {
        if typed_status == Some("reserved")
            && row
                .typed_reserved_until
                .as_ref()
                .is_some_and(|reserved_until| *reserved_until > Utc::now())
        {
            return;
        }
        let mismatch =
            DispatchDeliveryEventMismatch::notified_status(row.dispatch_id, row.typed_status);
        stats.record_mismatch(&mismatch.kind);
        mismatches.push(mismatch);
        return;
    }
}

pub(super) fn classify_delivery_typed_guard_mismatches(
    stats: &mut DispatchDeliveryEventReconcileStats,
    mismatches: &mut Vec<DispatchDeliveryEventMismatch>,
    row: DeliveryTypedGuardRow,
) {
    if row.has_reserving || row.has_notified {
        return;
    }
    let status = row.typed_status.as_str();
    if status == "reserved" {
        if row
            .reserved_until
            .as_ref()
            .is_some_and(|reserved_until| *reserved_until > Utc::now())
        {
            return;
        }
        let mismatch = DispatchDeliveryEventMismatch::missing_kv_meta(row.dispatch_id, "reserved");
        stats.record_mismatch(&mismatch.kind);
        mismatches.push(mismatch);
    } else if is_completed_delivery_status(status) {
        // A completed delivery (sent/fallback/duplicate/skipped) with no guard
        // key lost its `dispatch_notified:*` dedupe guard; recovery rebuilds it.
        let mismatch = DispatchDeliveryEventMismatch::missing_kv_meta(row.dispatch_id, "sent");
        stats.record_mismatch(&mismatch.kind);
        mismatches.push(mismatch);
    }
}
