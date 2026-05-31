#!/bin/bash
sed -i 's/for row in &rows {/for row in rows {/g' src/reconcile.rs

sed -i 's/fn classify_delivery_kv_guard_mismatches(/fn classify_delivery_kv_guard_mismatches(/g' src/reconcile.rs
sed -i 's/    row: &DeliveryKvGuardRow,/    row: DeliveryKvGuardRow,/g' src/reconcile.rs
sed -i 's/row.dispatch_id.clone()/row.dispatch_id/g' src/reconcile.rs
sed -i 's/row.typed_status.clone()/row.typed_status/g' src/reconcile.rs

sed -i 's/fn classify_delivery_typed_guard_mismatches(/fn classify_delivery_typed_guard_mismatches(/g' src/reconcile.rs
sed -i 's/    row: &DeliveryTypedGuardRow,/    row: DeliveryTypedGuardRow,/g' src/reconcile.rs
