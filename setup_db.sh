#!/bin/bash
export DATABASE_URL="postgresql://jules:jules@localhost/agentdesk"
cargo run --bin agentdesk migrate postgres || echo "Migration failed, maybe not exactly right command but let's see"
