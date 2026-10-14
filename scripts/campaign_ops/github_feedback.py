"""Complete, bounded PR feedback reads with a stable identity across pages."""
import json
import re


IDENTITY_FIELDS = "number url headRefOid baseRefOid state updatedAt"
NODE_FIELDS = {
    "comments": "id databaseId author{login} body url createdAt updatedAt",
    "reviews": "id databaseId author{login} body url state submittedAt createdAt updatedAt commit{oid}",
}
MAX_PAGES = 10
MAX_BYTES = 4 * 1024 * 1024


def _pull_request(executable, repo, number, fields, reader, read_evidence, after=None):
    owner, name = repo.split("/")
    variables = ",$after:String" if after is not None else ""
    query = ("query($owner:String!,$name:String!,$number:Int!" + variables + "){"
             "repository(owner:$owner,name:$name){pullRequest(number:$number){" + fields + "}}}")
    args = ["api", "graphql", "-f", "query=" + query, "-f", "owner=" + owner,
            "-f", "name=" + name, "-F", "number=" + str(number)]
    if after is not None:
        args += ["-f", "after=" + after]
    response = reader(executable, args)
    read_evidence.append({"argv": [executable, *args], "response": response})
    if response.get("errors"):
        raise ValueError("GitHub feedback query did not complete")
    pr = response["data"]["repository"]["pullRequest"]
    if not isinstance(pr, dict):
        raise ValueError("GitHub feedback target is unavailable")
    return pr


def _identity(pr, repo, number):
    if (pr["number"] != number or pr["url"] != f"https://github.com/{repo}/pull/{number}"
            or not all(re.fullmatch(r"[0-9a-f]{40}", pr[key]) for key in ["headRefOid", "baseRefOid"])):
        raise ValueError("GitHub feedback target identity does not match")
    return tuple(pr[key] for key in IDENTITY_FIELDS.split())


def feedback(executable, repo, number, reader):
    """Read all issue comments and PR reviews; review threads remain a separate read."""
    identity, totals = None, {}
    captured, pagination, read_evidence = {}, {}, []
    for connection, node_fields in NODE_FIELDS.items():
        rows, seen_ids, seen_cursors, after, total = [], set(), set(), None, None
        for page_number in range(1, MAX_PAGES + 1):
            cursor = ",after:$after" if after is not None else ""
            fields = (IDENTITY_FIELDS + " " + connection + "(first:100" + cursor + "){"
                      "totalCount pageInfo{hasNextPage endCursor} nodes{" + node_fields + "}}")
            pr = _pull_request(executable, repo, number, fields, reader, read_evidence, after)
            current = _identity(pr, repo, number)
            if identity is not None and current != identity:
                raise ValueError("PR identity or feedback changed during pagination")
            identity = current
            page = pr[connection]
            if (not isinstance(page["totalCount"], int) or isinstance(page["totalCount"], bool)
                    or page["totalCount"] < 0 or (total is not None and page["totalCount"] != total)):
                raise ValueError("PR feedback count changed during pagination")
            total = page["totalCount"]
            for row in page["nodes"]:
                if not isinstance(row.get("id"), str) or not row["id"] or row["id"] in seen_ids:
                    raise ValueError("PR feedback contains missing or duplicate source IDs")
                seen_ids.add(row["id"])
                rows.append(row)
            captured[connection] = rows
            if len(json.dumps({"feedback": captured, "read_evidence": read_evidence}).encode()) > MAX_BYTES:
                raise ValueError("PR feedback exceeds the bounded report limit")
            info = page["pageInfo"]
            if not info["hasNextPage"]:
                totals[connection] = total
                pagination[connection] = {"total_count": total, "fetched_count": len(rows),
                                          "pages": page_number, "has_next_page": False}
                break
            after = info["endCursor"]
            if not isinstance(after, str) or not after or after in seen_cursors:
                raise ValueError("PR feedback pagination cursor did not advance")
            seen_cursors.add(after)
        else:
            raise ValueError("PR feedback pagination exceeds the bounded inspection limit")
    final = _pull_request(executable, repo, number,
                          IDENTITY_FIELDS + " comments{totalCount} reviews{totalCount}", reader, read_evidence)
    if _identity(final, repo, number) != identity or any(final[k]["totalCount"] != v for k, v in totals.items()):
        raise ValueError("PR identity or feedback changed during pagination")
    report = {"schema_version": 1, "repository": repo, "number": number,
              "head": final["headRefOid"], "base": final["baseRefOid"], "state": final["state"],
              "updated_at": final["updatedAt"], "scope": "issue_comments_and_pull_request_reviews",
              **captured, "pagination": pagination, "read_evidence": read_evidence,
              "complete": all(len(captured[k]) == v for k, v in totals.items())}
    if len(json.dumps(report).encode()) > MAX_BYTES:
        raise ValueError("PR feedback exceeds the bounded report limit")
    return report
