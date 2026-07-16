git checkout src/reconcile.rs
sed -i "s/AND SUBSTRING(key FROM LENGTH('dispatch_reserving:') + 1) > \$1/AND key > 'dispatch_reserving:' || \$1/g" src/reconcile.rs
sed -i "s/AND SUBSTRING(key FROM LENGTH('dispatch_notified:') + 1) > \$1/AND key > 'dispatch_notified:' || \$1/g" src/reconcile.rs
sed -i "s/WHERE e.dispatch_id = SUBSTRING(m.key FROM LENGTH('dispatch_reserving:') + 1)/WHERE m.key = 'dispatch_reserving:' || e.dispatch_id/g" src/reconcile.rs
sed -i "s/WHERE td.id = SUBSTRING(m.key FROM LENGTH('dispatch_notified:') + 1)/WHERE m.key = 'dispatch_notified:' || td.id/g" src/reconcile.rs
