cat << 'DIFF' > patch.diff
--- src/dispatch/dispatch_create.rs
+++ src/dispatch/dispatch_create.rs
@@ -739,33 +739,26 @@
         };

         if let Some((wt_path, wt_branch, managed_created)) = worktree_target
-            && let Ok(mut obj) =
-                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&base)
+            && let Some(obj) = context_with_session_strategy.as_object_mut()
         {
             obj.insert("worktree_path".to_string(), json!(wt_path.clone()));
             if let Some(wt_branch) = wt_branch {
                 obj.insert("worktree_branch".to_string(), json!(wt_branch));
             }
             if managed_created {
                 obj.insert("managed_worktree".to_string(), json!(true));
                 obj.insert("managed_worktree_cleanup".to_string(), json!("terminal"));
             }
             tracing::info!(
                 "[dispatch] {} dispatch for card {}: injecting worktree_path={}",
                 dispatch_type,
                 kanban_card_id,
                 wt_path
             );
-            base = serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or(base);
         }
         if dispatch_type == "review-decision"
-            && let Ok(mut obj) =
-                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&base)
+            && let Some(obj) = context_with_session_strategy.as_object_mut()
         {
-            inject_review_dispatch_identifiers(pg_pool, kanban_card_id, dispatch_type, &mut obj)
+            inject_review_dispatch_identifiers(pg_pool, kanban_card_id, dispatch_type, obj)
                 .await;
-            base = serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or(base);
         }
-        base
+        serde_json::to_string(&context_with_session_strategy)?
     };
     // #3605 (T2): the broader "skip kickoff" set — review-family plus inert
DIFF
git restore src/dispatch/dispatch_create.rs && patch -p0 < patch.diff
