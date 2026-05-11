var test = require("node:test");
var assert = require("node:assert");
var fs = require("fs");
var path = require("path");

function normalizeStaticMemberAccess(content) {
  return content
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\/\/[^\r\n]*/g, "")
    .replace(/\?\s*\./g, ".")
    .replace(/\[\s*(["'`])(db|query|execute)\1\s*\]/g, ".$2")
    .replace(/\.+/g, ".")
    .replace(/\s+/g, "");
}

function hasRawDbAccess(content) {
  var normalized = normalizeStaticMemberAccess(content);
  return normalized.includes("agentdesk.db.query") || normalized.includes("agentdesk.db.execute");
}

test("triage-rules avoids raw agentdesk.db.* access", () => {
  var content = fs.readFileSync(path.join(__dirname, "../triage-rules.js"), "utf8");
  assert.equal(
    hasRawDbAccess(content),
    false,
    "triage-rules should not use raw agentdesk.db query/execute access",
  );
});

test("triage-rules raw db guard detects common access variants", () => {
  assert.ok(hasRawDbAccess("agentdesk.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk?.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk.db?.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk?.db?.execute('DELETE')"));
  assert.ok(hasRawDbAccess("agentdesk['db'].query('SELECT 1')"));
  assert.ok(hasRawDbAccess('agentdesk?.["db"]?.execute("DELETE")'));
  assert.ok(hasRawDbAccess("agentdesk[`db`].query('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk /* comment */ . db . execute('DELETE')"));
  assert.ok(hasRawDbAccess("agentdesk.db['query']('SELECT 1')"));
  assert.ok(hasRawDbAccess('agentdesk.db?.["execute"]("DELETE")'));
  assert.ok(hasRawDbAccess("agentdesk.db[`execute`]('DELETE')"));
});
