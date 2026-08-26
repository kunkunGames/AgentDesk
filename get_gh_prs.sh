#!/bin/bash
gh pr list --state open --limit 100 --json number,title,headRefName,baseRefName,author,labels,body,updatedAt > gh_prs.json
