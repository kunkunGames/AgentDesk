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
      "else",
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
  return /[=(:,[!&|?;{}>)]/.test(trimmed[trimmed.length - 1]);
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

function readRegexLiteral(content, startIndex) {
  var inClass = false;

  for (var i = startIndex + 1; i < content.length; i++) {
    var ch = content[i];
    if (ch === "\\") {
      i += 1;
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
    if (ch === "/" && !inClass) return i;
  }

  return content.length - 1;
}

function readQuotedLiteral(content, startIndex, quote) {
  var escaped = false;

  for (var i = startIndex + 1; i < content.length; i++) {
    var ch = content[i];
    if (escaped) {
      escaped = false;
    } else if (ch === "\\") {
      escaped = true;
    } else if (ch === quote) {
      return i;
    }
  }

  return content.length - 1;
}

function readTemplateLiteralExpressions(content, startIndex) {
  var expression = "";
  var escaped = false;

  for (var i = startIndex; i < content.length; i++) {
    var ch = content[i];
    var next = content[i + 1];

    if (escaped) {
      escaped = false;
      continue;
    }

    if (ch === "\\") {
      escaped = true;
      continue;
    }

    if (ch === "`") {
      return { expression: expression, endIndex: i };
    }

    if (ch === "$" && next === "{") {
      var nestedExpression = readTemplateExpression(content, i + 2);
      expression += " " + nestedExpression.expression + " ";
      i = nestedExpression.endIndex;
    }
  }

  return { expression: expression, endIndex: content.length - 1 };
}

function readTemplateExpression(content, startIndex) {
  var expression = "";
  var depth = 1;

  for (var i = startIndex; i < content.length; i++) {
    var ch = content[i];
    var next = content[i + 1];

    if (ch === "\"" || ch === "'") {
      var quoteEnd = readQuotedLiteral(content, i, ch);
      expression += content.slice(i, quoteEnd + 1);
      i = quoteEnd;
      continue;
    }

    if (ch === "`") {
      var template = readTemplateLiteralExpressions(content, i + 1);
      expression += "`" + template.expression + "`";
      i = template.endIndex;
      continue;
    }

    if (ch === "/" && next === "/") {
      while (i < content.length && content[i] !== "\n" && content[i] !== "\r") {
        expression += content[i];
        i += 1;
      }
      i -= 1;
      continue;
    }

    if (ch === "/" && next === "*") {
      expression += ch + next;
      i += 2;
      while (i < content.length && !(content[i] === "*" && content[i + 1] === "/")) {
        expression += content[i];
        i += 1;
      }
      if (i < content.length) expression += "*/";
      i += 1;
      continue;
    }

    if (ch === "/" && next !== "/" && next !== "*" && canStartRegex(expression)) {
      var regexEnd = readRegexLiteral(content, i);
      expression += content.slice(i, regexEnd + 1);
      i = regexEnd;
      continue;
    }

    if (ch === "{") {
      depth += 1;
      expression += ch;
      continue;
    }

    if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        return { expression: expression, endIndex: i };
      }
      expression += ch;
      continue;
    }

    expression += ch;
  }

  return { expression: expression, endIndex: content.length - 1 };
}

function stripJsStrings(content) {
  var output = "";
  var quote = null;
  var escaped = false;

  for (var i = 0; i < content.length; i++) {
    var ch = content[i];

    if (quote) {
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === quote) {
        quote = null;
        output += ch;
      }
      continue;
    }

    if (ch === "`") {
      var template = readTemplateLiteralExpressions(content, i + 1);
      output += "`" + stripJsStrings(stripJsComments(template.expression)) + "`";
      i = template.endIndex;
      continue;
    }

    if (ch === "\"" || ch === "'") {
      quote = ch;
      output += ch;
      continue;
    }

    output += ch;
  }

  return output;
}

function isIdentifierChar(ch) {
  return Boolean(ch) && /[A-Za-z0-9_$]/.test(ch);
}

function normalizeWhitespaceBoundaries(content) {
  var output = "";

  for (var i = 0; i < content.length; i++) {
    var ch = content[i];
    if (!/\s/.test(ch)) {
      output += ch;
      continue;
    }

    var start = i;
    while (i + 1 < content.length && /\s/.test(content[i + 1])) i += 1;

    var previous = start > 0 ? content[start - 1] : "";
    var next = i + 1 < content.length ? content[i + 1] : "";
    if (isIdentifierChar(previous) && isIdentifierChar(next)) output += ";";
  }

  return output;
}

function normalizeStaticMemberAccess(content) {
  var normalized = stripJsStrings(stripJsComments(content)
    .replace(/\\u\{([0-9a-fA-F]+)\}/g, function(_match, codePoint) {
      return String.fromCodePoint(parseInt(codePoint, 16));
    })
    .replace(/\\u([0-9a-fA-F]{4})/g, function(_match, codePoint) {
      return String.fromCharCode(parseInt(codePoint, 16));
    })
    .replace(/[\r\n]+\s*(?=[.\[])/g, "")
    .replace(/[\r\n]+/g, ";")
    .replace(/\?\s*\./g, ".")
    .replace(/(["'`])db\1\s*:/g, "db:")
    .replace(/\[\s*(["'`])(db|query|execute)\1\s*\]/g, ".$2")
  )
    .replace(/\.{2,}(?=(db|query|execute)\b)/g, ".")
    .replace(/\s*\.\s*/g, ".")
    .replace(/\s*\[\s*/g, "[")
    .replace(/\s*\]\s*/g, "]");

  return normalizeWhitespaceBoundaries(normalized);
}

function findMatchingBrace(content, startIndex) {
  var depth = 0;

  for (var i = startIndex; i < content.length; i++) {
    var ch = content[i];
    if (ch === "{") depth += 1;
    if (ch === "}") {
      depth -= 1;
      if (depth === 0) return i;
    }
  }

  return -1;
}

function splitTopLevelDestructuringFields(content) {
  var fields = [];
  var start = 0;
  var depth = 0;

  for (var i = 0; i < content.length; i++) {
    var ch = content[i];
    if (ch === "{" || ch === "[" || ch === "(") {
      depth += 1;
      continue;
    }
    if (ch === "}" || ch === "]" || ch === ")") {
      depth -= 1;
      continue;
    }
    if (ch === "," && depth === 0) {
      fields.push(content.slice(start, i));
      start = i + 1;
    }
  }

  fields.push(content.slice(start));
  return fields;
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function collectAliasesForObject(normalized, objectName, fromIndex) {
  var aliases = [];
  var content = normalized.slice(fromIndex);
  var escapedObjectName = escapeRegExp(objectName);
  var objectReference = "\\(*" + escapedObjectName + "\\)*";
  var patterns = [
    new RegExp("(^|[;{:(,])(?:const|let|var);?([A-Za-z_$][A-Za-z0-9_$]*)=" + objectReference + "(?=$|[^A-Za-z0-9_$])", "g"),
    new RegExp("(^|[;{])for\\((?:const|let|var);?([A-Za-z_$][A-Za-z0-9_$]*)=" + objectReference + "(?=$|[^A-Za-z0-9_$])", "g"),
    new RegExp("(^|[;{])for(?:const|let|var);?([A-Za-z_$][A-Za-z0-9_$]*)=" + objectReference + "(?=$|[^A-Za-z0-9_$])", "g"),
    new RegExp("(^|[;{])for\\(([A-Za-z_$][A-Za-z0-9_$]*)=" + objectReference + "(?=$|[^A-Za-z0-9_$])", "g"),
    new RegExp("(^|[;{])for([A-Za-z_$][A-Za-z0-9_$]*)=" + objectReference + "(?=$|[^A-Za-z0-9_$])", "g"),
    new RegExp("(^|[;{:(,])([A-Za-z_$][A-Za-z0-9_$]*)=" + objectReference + "(?=$|[^A-Za-z0-9_$])", "g")
  ];

  for (var p = 0; p < patterns.length; p++) {
    var match;
    while ((match = patterns[p].exec(content)) !== null) {
      var alias = match[2];
      if (alias !== objectName) {
        aliases.push({ name: alias, fromIndex: fromIndex + match.index + match[0].length });
      }
    }
  }

  return aliases;
}

function collectAgentdeskNames(normalized) {
  var names = [{ name: "agentdesk", fromIndex: 0 }];
  var seen = { agentdesk: true };

  for (var i = 0; i < names.length; i++) {
    var aliases = collectAliasesForObject(normalized, names[i].name, names[i].fromIndex);
    for (var j = 0; j < aliases.length; j++) {
      if (seen[aliases[j].name]) continue;
      seen[aliases[j].name] = true;
      names.push(aliases[j]);
    }
  }

  return names;
}

function hasDbDestructuringFromObject(normalized, objectName) {
  var escapedObjectName = escapeRegExp(objectName);
  var objectReference = "\\(*" + escapedObjectName + "\\)*";

  for (var i = 0; i < normalized.length; i++) {
    if (normalized[i] !== "{") continue;

    var prefix = normalized.slice(0, i);
    var startsDeclaration = /(^|[;{:(,])(const|let|var)?$/.test(prefix)
      || /(^|[;{])for\((const|let|var)?$/.test(prefix)
      || /(^|[;{])for(const|let|var)?$/.test(prefix);
    if (!startsDeclaration) continue;

    var endIndex = findMatchingBrace(normalized, i);
    if (endIndex === -1) continue;

    var suffix = normalized.slice(endIndex + 1);
    if (!new RegExp("^=" + objectReference + "($|[^A-Za-z0-9_$])").test(suffix)) continue;

    var fields = splitTopLevelDestructuringFields(normalized.slice(i + 1, endIndex));
    for (var j = 0; j < fields.length; j++) {
      if (/^\.?db($|:|=)/.test(fields[j])) return true;
      if (/^\.\.\.[A-Za-z_$][A-Za-z0-9_$]*$/.test(fields[j])) return true;
    }
  }

  return false;
}

function hasDbAccessFromObject(normalized, objectName, fromIndex) {
  var content = normalized.slice(fromIndex);
  var escapedObjectName = escapeRegExp(objectName);
  var objectReference = "\\(*" + escapedObjectName + "\\)*";
  return new RegExp("(^|[^A-Za-z0-9_$])" + objectReference + "\\.db([^A-Za-z0-9_$]|$)").test(content)
    || hasDbDestructuringFromObject(content, objectName);
}

function hasRawDbAccess(content) {
  var normalized = normalizeStaticMemberAccess(content);
  var agentdeskNames = collectAgentdeskNames(normalized);
  for (var i = 0; i < agentdeskNames.length; i++) {
    if (hasDbAccessFromObject(normalized, agentdeskNames[i].name, agentdeskNames[i].fromIndex)) {
      return true;
    }
  }
  return false;
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
  assert.ok(hasRawDbAccess("agentdesk.\\u0064b.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("agentdesk.\\u{64}b.query('SELECT 1')"));
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
  assert.ok(hasRawDbAccess("if (ok) /https?:\\/\\//.test(url); agentdesk.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("if (ok) {} else /https?:\\/\\//.test(url); agentdesk.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("return agentdesk.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("throw agentdesk.db.execute('DELETE')"));
  assert.ok(hasRawDbAccess("(agentdesk.db).query('SELECT 1')"));
  assert.ok(hasRawDbAccess("(agentdesk['db']).execute('DELETE')"));
  assert.ok(hasRawDbAccess("const db = agentdesk.db; db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const db = agentdesk.db\ndb.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { query } = agentdesk.db; query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { query } = agentdesk.db\nquery('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { execute: run } = agentdesk.db; run('DELETE')"));
  assert.ok(hasRawDbAccess("const { query: q$ } = agentdesk.db; q$('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { db } = agentdesk; db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { db: rawDb } = agentdesk; rawDb.execute('DELETE')"));
  assert.ok(hasRawDbAccess('const { "db": rawDb } = agentdesk; rawDb.query("SELECT 1")'));
  assert.ok(hasRawDbAccess("const { ['db']: rawDb } = agentdesk; rawDb.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { cards, db: rawDb } = agentdesk\nrawDb['query']('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { db: { query } } = agentdesk; query('SELECT 1')"));
  assert.ok(hasRawDbAccess("if (ok) { const { db } = agentdesk; db.query('SELECT 1'); }"));
  assert.ok(hasRawDbAccess("function run() { const { db: rawDb } = agentdesk; rawDb.execute('DELETE'); }"));
  assert.ok(hasRawDbAccess("for (const { db } = agentdesk; ; ) db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("switch (kind) { case 'x': const { db } = agentdesk; db.query('SELECT 1'); }"));
  assert.ok(hasRawDbAccess("const ad = agentdesk; ad.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("if (ok) { const ad = agentdesk; ad.db.execute('DELETE'); }"));
  assert.ok(hasRawDbAccess("const ad = agentdesk; return ad.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const ad = agentdesk; const { db } = ad; db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const ad = agentdesk; const next = ad; next.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { ...ad } = agentdesk; ad.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("const { cards, ...ad } = agentdesk; ad.db.execute('DELETE')"));
  assert.ok(hasRawDbAccess("let ad; for (ad = agentdesk; ; ) ad.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("function run(ad = agentdesk) { ad.db.query('SELECT 1'); }"));
  assert.ok(hasRawDbAccess("const run = (ad = agentdesk) => ad.db.execute('DELETE')"));
  assert.ok(hasRawDbAccess("function run({ db } = agentdesk) { db.query('SELECT 1'); }"));
  assert.ok(hasRawDbAccess("const run = ({ db: rawDb } = agentdesk) => rawDb.execute('DELETE')"));
  assert.ok(hasRawDbAccess("(agentdesk).db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("((agentdesk)).db.execute('DELETE')"));
  assert.ok(hasRawDbAccess("const ad = (agentdesk); ad.db.query('SELECT 1')"));
  assert.ok(hasRawDbAccess("function run(ad = (agentdesk)) { ad.db.query('SELECT 1'); }"));
  assert.ok(hasRawDbAccess("function run({ db } = (agentdesk)) { db.query('SELECT 1'); }"));
  assert.ok(hasRawDbAccess("agentdesk.db['query']('SELECT 1')"));
  assert.ok(hasRawDbAccess('agentdesk.db?.["execute"]("DELETE")'));
  assert.ok(hasRawDbAccess("agentdesk.db[`execute`]('DELETE')"));
  assert.ok(hasRawDbAccess('const x = `${agentdesk.db.query("SELECT 1")}`;'));
  assert.ok(hasRawDbAccess('const x = `${agentdesk["db"].execute("DELETE")}`;'));
  assert.equal(hasRawDbAccess("// agentdesk.db.query('SELECT 1')"), false);
  assert.equal(hasRawDbAccess('const msg = "agentdesk.db.query";'), false);
  assert.equal(hasRawDbAccess("const msg = `agentdesk.db.query`;"), false);
  assert.equal(hasRawDbAccess('const msg = `${"agentdesk.db.query"}`;'), false);
  assert.equal(hasRawDbAccess("agentdesk.database.query('SELECT 1')"), false);
  assert.equal(hasRawDbAccess("const { database } = agentdesk; database.query('SELECT 1')"), false);
  assert.equal(hasRawDbAccess("const { db } = other; db.query('SELECT 1')"), false);
  assert.equal(hasRawDbAccess("const ad = other; ad.db.query('SELECT 1')"), false);
  assert.equal(hasRawDbAccess("function run(ad = other) { ad.db.query('SELECT 1'); }"), false);
  assert.equal(hasRawDbAccess("const ad = (other); ad.db.query('SELECT 1')"), false);
  assert.equal(hasRawDbAccess("const { ...ad } = other; ad.db.query('SELECT 1')"), false);
});
