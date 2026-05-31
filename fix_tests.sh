#!/bin/bash
sed -i 's/            &DeliveryKvGuardRow {/            DeliveryKvGuardRow {/g' src/reconcile.rs
sed -i 's/            &DeliveryTypedGuardRow {/            DeliveryTypedGuardRow {/g' src/reconcile.rs
