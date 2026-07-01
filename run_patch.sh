#!/bin/bash
set -ex

sed -i 's/    let runs = store\n        \.list_runs(&routine_id, query\.limit\.unwrap_or(20))/    let limit = clamp_api_limit(Some(query.limit.unwrap_or(20).max(0) as usize)) as i64;\n    let runs = store\n        .list_runs(\&routine_id, limit)/g' src/server/routes/routines.rs
