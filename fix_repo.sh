#!/bin/bash
set -ex
cd /app
rm -rf .git
git init
git remote add origin https://github.com/kunkunGames/AgentDesk.git
git fetch origin main
git checkout main || git checkout -b main FETCH_HEAD
git reset --hard FETCH_HEAD
git clean -fd
