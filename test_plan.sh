#!/bin/bash
git diff --check
cargo check --all-targets --manifest-path Cargo.toml
python3 scripts/generate_inventory_docs.py
