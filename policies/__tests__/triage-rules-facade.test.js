var test = require("node:test");
var assert = require("node:assert");
var fs = require("fs");
var path = require("path");

function stripJsComments(content) {
  var output = "";
  var quote = null;
  var escaped = false;

  for (var i = 0; i < content.length; i++) {
    var ch = content[i];
    var next = content[i + 1];

    if (quote) {
      output += ch;
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === quote) {
        quote = null;
      }
      continue;
    }

    if (ch === "\"" || ch === "'" || ch === "`") {
      quote = ch;
      output += ch;
      continue;
    }

    if (ch === "/" && next === "/") {
      while (i < content.length && content[i] !== "\n" && content[i] !== "\r") i++;
      output += "\n";
      continue;
    }

    if (ch === "/" && next === "*") {
      i += 2;
      while (i < content.length && !(content[i] === "*" && content[i + 1] === "/")) i++;
      i += 1;
      output += " ";
      continue;
    }

    output += ch;
  }

  return output;
}

function normalizeStaticMemberAccess(content) {
  return stripJsComments(content)
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
  assert.ok(hasRawDbAccess('const u = "https://example.com"; agentdesk.db.query("SELECT 1")'));
  assert.ok(hasRawDbAccess("agentdesk.db['query']('SELECT 1')"));
  assert.ok(hasRawDbAccess('agentdesk.db?.["execute"]("DELETE")'));
  assert.ok(hasRawDbAccess("agentdesk.db[`execute`]('DELETE')"));
  assert.equal(hasRawDbAccess("// agentdesk.db.query('SELECT 1')"), false);
});
