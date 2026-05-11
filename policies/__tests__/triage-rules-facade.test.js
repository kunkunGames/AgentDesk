var test = require("node:test");
var assert = require("node:assert");
var fs = require("fs");
var path = require("path");

function canStartRegex(output) {
  var trimmed = output.replace(/\s+$/g, "");
  if (!trimmed) return true;
  var wordMatch = trimmed.match(/[A-Za-z_$][A-Za-z0-9_$]*$/);
  if (wordMatch) {
    return [
      "await",
      "case",
      "delete",
      "in",
      "instanceof",
      "of",
      "return",
      "throw",
      "typeof",
      "void",
      "yield"
    ].includes(wordMatch[0]);
  }
  return /[=(:,[!&|?;{}>]/.test(trimmed[trimmed.length - 1]);
}

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

    if (ch === "/" && next !== "/" && next !== "*" && canStartRegex(output)) {
      output += ch;
      var inClass = false;
      for (i += 1; i < content.length; i++) {
        ch = content[i];
        output += ch;
        if (ch === "\\") {
          if (i + 1 < content.length) {
            i += 1;
            output += content[i];
          }
          continue;
        }
        if (ch === "[") {
          inClass = true;
          continue;
        }
        if (ch === "]") {
          inClass = false;
          continue;
        }
        if (ch === "/" && !inClass) break;
      }
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
    .replace(/[\r\n]+\s*(?=[.\[])/g, "")
    .replace(/[\r\n]+/g, ";")
    .replace(/\?\s*\./g, ".")
    .replace(/\[\s*(["'`])(db|query|execute)\1\s*\]/g, ".$2")
    .replace(/\.+/g, ".")
    .replace(/[()]/g, "")
    .replace(/\s+/g, "");
}

function hasRawDbAccess(content) {
  var normalized = normalizeStaticMemberAccess(content);
  return /(^|[^A-Za-z0-9_$])agentdesk\.db([^A-Za-z0-9_$]|$)/.test(normalized);
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
  assert.ok(hasRawDbAccess("agentdesk\n.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk\n['db'].execute('DELETE')"));
  assert.ok(hasRawDbAccess("agentdesk?.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk.db?.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk?.db?.execute('DELETE')"));
  assert.ok(hasRawDbAccess("agentdesk['db'].query('SELECT 1')"));
  assert.ok(hasRawDbAccess('agentdesk?.["db"]?.execute("DELETE")'));
  assert.ok(hasRawDbAccess("agentdesk[`db`].query('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk /* comment */ . db . execute('DELETE')"));
  assert.ok(hasRawDbAccess('const u = "https://example.com"; agentdesk.db.query("SELECT 1")'));
  assert.ok(hasRawDbAccess("const re = /https?:\\/\\//; agentdesk.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("return /https?:\\/\\//.test(url) && agentdesk.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("(agentdesk.db).query('SELECT 1')"));
  assert.ok(hasRawDbAccess("(agentdesk['db']).execute('DELETE')"));
  assert.ok(hasRawDbAccess("const db = agentdesk.db; db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const db = agentdesk.db\ndb.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { query } = agentdesk.db; query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { query } = agentdesk.db\nquery('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { execute: run } = agentdesk.db; run('DELETE')"));
  assert.ok(hasRawDbAccess("const { query: q$ } = agentdesk.db; q$('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk.db['query']('SELECT 1')"));
  assert.ok(hasRawDbAccess('agentdesk.db?.["execute"]("DELETE")'));
  assert.ok(hasRawDbAccess("agentdesk.db[`execute`]('DELETE')"));
  assert.equal(hasRawDbAccess("// agentdesk.db.query('SELECT 1')"), false);
  assert.equal(hasRawDbAccess("agentdesk.database.query('SELECT 1')"), false);
});
