var test = require("node:test");
var assert = require("node:assert");
var fs = require("fs");
var path = require("path");

test("triage-rules avoids raw agentdesk.db.* access", () => {
  var content = fs.readFileSync(path.join(__dirname, "../triage-rules.js"), "utf8");
  assert.ok(!content.includes("agentdesk.db.query"), "triage-rules should not use agentdesk.db.query");
  assert.ok(!content.includes("agentdesk.db.execute"), "triage-rules should not use agentdesk.db.execute");
});
