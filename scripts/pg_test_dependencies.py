"""Resolve literal libtest children without treating diagnostic strings as calls."""
from __future__ import annotations

import re

try:
    from rust_lex import CODE, LITERAL, StripState, lex_segments
except ModuleNotFoundError:
    from scripts.rust_lex import CODE, LITERAL, StripState, lex_segments


def _tokens(source: str) -> list[tuple[str, str]]:
    state = StripState()
    result: list[tuple[str, str]] = []
    for line in source.splitlines(keepends=True):
        continuation = state.in_string or state.raw_hashes is not None
        for kind, text in lex_segments(line, state):
            if kind == CODE:
                result.extend((CODE, value) for value in re.findall(r"::|[A-Za-z_][A-Za-z0-9_]*|[^\s]", text))
            elif kind == LITERAL:
                if continuation and result and result[-1][0] == LITERAL:
                    result[-1] = (LITERAL, result[-1][1] + text)
                else:
                    result.append((LITERAL, text))
            continuation = False
    return result


def _string(token: tuple[str, str]) -> str | None:
    if token[0] != LITERAL:
        return None
    match = re.fullmatch(r'"([^"\\\n]*)"', token[1])
    if match:
        return match[1]
    match = re.fullmatch(r'r(?P<hashes>#{0,255})"(?P<value>[^\n]*)"(?P=hashes)', token[1])
    return match["value"] if match else None


def _closing(tokens: list[tuple[str, str]], start: int, left: str, right: str) -> int | None:
    depth = 0
    for index in range(start, len(tokens)):
        if tokens[index] == (CODE, left):
            depth += 1
        elif tokens[index] == (CODE, right):
            depth -= 1
            if depth == 0:
                return index
    return None


def _depths(tokens: list[tuple[str, str]]) -> list[int]:
    depth = 0
    result = []
    for token in tokens:
        result.append(depth)
        if token[0] == CODE:
            depth += token[1] in {"(", "[", "{"}
            depth -= token[1] in {")", "]", "}"}
    return result


def _literal_imports(raw):
    brace = re.fullmatch(r"(.+)::\{([^{}]+)\}", raw.strip())
    entries = [brace[1] + "::" + item.strip() for item in brace[2].split(",") if item.strip()] if brace else [raw]
    for entry in entries:
        path, separator, alias = entry.strip().partition(" as ")
        parts = tuple(part.strip() for part in path.split("::") if part.strip())
        if parts:
            yield alias.strip() if separator else parts[-1], parts


def _scope_receivers(tokens, receivers, standard_library_visible, resolve_import):
    """Items apply throughout their block; items in sibling blocks do not."""
    receivers = set(receivers)
    depth = _depths(tokens)
    declarations = {tokens[index + 1][1] for index, token in enumerate(tokens[:-1])
                    if depth[index] == 0 and token[0] == CODE and token[1] in {"struct", "enum", "type", "mod"}}
    receivers = {path for path in receivers if path.startswith("::") or path.split("::")[0] not in declarations}
    standard_library_visible = standard_library_visible and "std" not in declarations
    if not standard_library_visible:
        receivers.discard("std::process::Command")
    for index, token in enumerate(tokens):
        if token != (CODE, "use") or depth[index] != 0:
            continue
        end = next((n for n in range(index + 1, len(tokens)) if tokens[n] == (CODE, ";") and depth[n] == 0), None)
        if end is None:
            continue
        raw = re.sub(r"\s*::\s*", "::", " ".join(value for _, value in tokens[index + 1:end]))
        for name, target in resolve_import(raw):
            receivers.discard(name)
            if target == ("std", "process", "Command") and (standard_library_visible or raw.startswith("::std::")):
                receivers.add(name)
            if name == "std":
                standard_library_visible = target == ("std",) and "std" not in declarations
    if not standard_library_visible:
        receivers.discard("std::process::Command")
    return receivers, standard_library_visible


def _names_before(tokens, call, variable, initial):
    """Only completed, direct let statements can replace the loop binding."""
    depth = _depths(tokens)
    names = set(initial)
    for index, token in enumerate(tokens[:call]):
        if token == (CODE, variable) and depth[index] == 0 and tokens[index + 1:index + 2] == [(CODE, "=")]:
            if tokens[index + 2:index + 3] != [(CODE, "=")] and (index == 0 or tokens[index - 1][1] not in {"let", "mut"}):
                if any(tokens[n] == (CODE, ";") and depth[n] == 0 for n in range(index + 1, call)):
                    raise ValueError("isolated libtest loop binding is assigned before the child call")
        if token != (CODE, "let") or depth[index] != 0:
            continue
        previous = tokens[index - 1] if index else None
        if previous is not None and previous not in {(CODE, ";"), (CODE, "}")}:
            continue
        end = next((n for n in range(index + 1, call) if tokens[n] == (CODE, ";") and depth[n] == 0), None)
        if end is None:
            continue
        equal = next((n for n in range(index + 1, end) if tokens[n] == (CODE, "=") and depth[n] == 0), None)
        if equal is None or (CODE, variable) not in tokens[index + 1:equal]:
            continue
        value = _string(tokens[equal + 1]) if equal + 2 == end else None
        names = {value} if value is not None else None
    return names


def _child_targets(tokens, variable, initial, receivers, standard_library_visible):
    values = [value for _, value in tokens]
    depth = _depths(tokens)
    executables = {"::std::env::current_exe"}
    if standard_library_visible:
        executables.add("std::env::current_exe")
    targets = set()
    unsupported = False
    for receiver in receivers:
        for executable in executables:
            prefix = [value for _, value in _tokens(receiver + "::new(" + executable + "().unwrap())")]
            for start in range(len(tokens) - len(prefix) + 1):
                if values[start:start + len(prefix)] != prefix or any(kind != CODE for kind, _ in tokens[start:start + len(prefix)]):
                    continue
                if start and (values[start - 1] in {"::", "."} or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", values[start - 1]) and values[start - 1] not in {"return", "break"}):
                    continue
                index = start + len(prefix)
                exact = False
                while index + 2 < len(tokens) and values[index] == "." and values[index + 2] == "(":
                    method = values[index + 1]
                    end = _closing(tokens, index + 2, "(", ")")
                    if end is None:
                        break
                    args = tokens[index + 3:end]
                    if method == "args" and not exact:
                        if len(args) == 8 and args[-2] == (CODE, ","):
                            args = args[:-2] + args[-1:]
                        exact = (len(args) == 7 and args[0] == (CODE, "[") and args[-1] == (CODE, "]")
                                 and args[2] == args[4] == (CODE, ",") and args[3] == (CODE, variable)
                                 and _string(args[1]) == "--exact" and _string(args[5]) == "--nocapture")
                        if not exact:
                            break
                    elif method == "output":
                        if not args and exact:
                            statement = next((n + 1 for n in range(start - 1, -1, -1) if tokens[n] == (CODE, ";") and depth[n] == 0), 0)
                            if depth[start] != 0 or any(tokens[n] == (CODE, "|") and depth[n] == 0 for n in range(statement, start)):
                                unsupported = True
                                break
                            names = _names_before(tokens, start, variable, initial)
                            if names is None:
                                raise ValueError("isolated libtest loop binding is dynamic at the child call")
                            targets.update(names)
                        break
                    elif method not in {"env", "env_remove", "env_clear", "current_dir", "stdin", "stdout", "stderr"}:
                        break
                    index = end + 1
    if unsupported:
        raise ValueError("isolated libtest child uses an unsupported nested scope")
    return targets


def isolated_test_targets(source: str, declared_tests: set[str], *, command_receivers: set[str], standard_library_visible: bool = True, resolve_import=None) -> set[str]:
    """Recognize exact literal-array loops executing the current libtest binary.

    Support direct loops and direct Command chains through output(). Resolve
    completed literal let shadows at each call. Nested execution and dynamic
    bindings are reported as unanalyzable when a standard child chain is seen.
    """
    if "current_exe" not in source or "--exact" not in source:
        return set()
    resolve_import = resolve_import or _literal_imports
    tokens = _tokens(source)
    if tokens and tokens[0] == (CODE, "{") and _closing(tokens, 0, "{", "}") == len(tokens) - 1:
        tokens = tokens[1:-1]
    depth = _depths(tokens)
    receivers = set(command_receivers) | {"::std::process::Command"}
    if standard_library_visible:
        receivers.add("std::process::Command")
    receivers, standard_library_visible = _scope_receivers(tokens, receivers, standard_library_visible, resolve_import)
    targets: set[str] = set()
    for index in range(len(tokens) - 3):
        if tokens[index] != (CODE, "for") or tokens[index + 2:index + 4] != [(CODE, "in"), (CODE, "[")]:
            continue
        variable = tokens[index + 1][1]
        if tokens[index + 1][0] != CODE or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", variable):
            continue
        end = _closing(tokens, index + 3, "[", "]")
        if end is None or tokens[end + 1:end + 2] != [(CODE, "{")]:
            continue
        literals = tokens[index + 4:end]
        if literals and literals[-1] == (CODE, ","):
            literals = literals[:-1]
        if not literals or len(literals) % 2 != 1 or any(token != (CODE, ",") for token in literals[1::2]):
            continue
        names = [_string(token) for token in literals[::2]]
        if any(name is None or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)+", name) for name in names):
            continue
        close = _closing(tokens, end + 1, "{", "}")
        if close is None:
            continue
        body = tokens[end + 2:close]
        local_receivers, local_std = _scope_receivers(body, receivers, standard_library_visible, resolve_import)
        selected = _child_targets(body, variable, names, local_receivers, local_std)
        if selected and depth[index] != 0:
            raise ValueError("isolated libtest loop uses an unsupported nested scope")
        unknown = selected - declared_tests
        if unknown:
            raise ValueError("isolated libtest target is absent from the source inventory: " + ", ".join(sorted(unknown)))
        targets.update(selected)
    return targets
