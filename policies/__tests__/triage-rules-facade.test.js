var test = require("node:test");
var assert = require("node:assert");
var fs = require("fs");
var path = require("path");

var rawDbAccessPattern =
  /agentdesk\s*(?:\?\s*)?(?:\.\s*db|\.\s*\[\s*["'`]db["'`]\s*\]|\[\s*["'`]db["'`]\s*\])\s*(?:\?\s*)?(?:\.\s*(?:query|execute)|\.\s*\[\s*["'`](?:query|execute)["'`]\s*\]|\[\s*["'`](?:query|execute)["'`]\s*\])/;

test("triage-rules avoids raw agentdesk.db.* access", () => {
  var content = fs.readFileSync(path.join(__dirname, "../triage-rules.js"), "utf8");
  assert.equal(
    rawDbAccessPattern.test(content),
    false,
    "triage-rules should not use raw agentdesk.db query/execute access",
  );
});

test("triage-rules raw db guard detects common access variants", () => {
  assert.ok(rawDbAccessPattern.test("agentdesk.db.query('SELECT 1')"));
  assert.ok(rawDbAccessPattern.test("agentdesk?.db.query('SELECT 1')"));
  assert.ok(rawDbAccessPattern.test("agentdesk.db?.query('SELECT 1')"));
  assert.ok(rawDbAccessPattern.test("agentdesk?.db?.execute('DELETE')"));
  assert.ok(rawDbAccessPattern.test("agentdesk['db'].query('SELECT 1')"));
  assert.ok(rawDbAccessPattern.test('agentdesk?.["db"]?.execute("DELETE")'));
  assert.ok(rawDbAccessPattern.test("agentdesk[`db`].query('SELECT 1')"));
  assert.ok(rawDbAccessPattern.test("agentdesk . db . execute('DELETE')"));
  assert.ok(rawDbAccessPattern.test("agentdesk.db['query']('SELECT 1')"));
  assert.ok(rawDbAccessPattern.test('agentdesk.db?.["execute"]("DELETE")'));
  assert.ok(rawDbAccessPattern.test("agentdesk.db[`execute`]('DELETE')"));
});
