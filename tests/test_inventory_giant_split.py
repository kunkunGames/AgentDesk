"""Unit tests for the #3036 production/test LoC split and giant-file registry.

Covers ``scripts/generate_inventory_docs.py``:
  * ``test_line_count`` only counts lines inside ``#[cfg(test)] mod`` blocks
    (inline ``#[cfg(test)]`` guards on production items stay production);
  * the giant-file flag keys off production LoC;
  * the registry loader and validator reject ghosts, unregistered new giants,
    and deadline-less entries.
"""

from __future__ import annotations

import importlib.util
import sys
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "generate_inventory_docs.py"

_SPEC = importlib.util.spec_from_file_location("generate_inventory_docs", SCRIPT_PATH)
GEN = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
sys.modules[_SPEC.name] = GEN
_SPEC.loader.exec_module(GEN)


def _src(body: str) -> str:
    return textwrap.dedent(body).lstrip("\n")


class ProdTestSplitTest(unittest.TestCase):
    def test_cfg_test_mod_block_counts_as_test(self) -> None:
        text = _src(
            """
            pub fn a() {}
            pub fn b() {}

            #[cfg(test)]
            mod tests {
                #[test]
                fn t() {}
            }
            """
        )
        prod, test = GEN.split_prod_test_lines(text)
        self.assertEqual(test, 5)  # the #[cfg(test)] line through closing brace
        self.assertEqual(prod, GEN.line_count(text) - 5)

    def test_inline_cfg_test_guard_stays_production(self) -> None:
        # Inline #[cfg(test)] guards on struct fields / conditional logic shape
        # the production surface and must NOT be counted as test LoC.
        text = _src(
            """
            pub struct Runtime {
                #[cfg(test)]
                test_hook: bool,
                value: u32,
            }

            impl Runtime {
                pub fn run(&self) {
                    #[cfg(test)]
                    self.note();
                }
            }
            """
        )
        _prod, test = GEN.split_prod_test_lines(text)
        self.assertEqual(test, 0)

    def test_cfg_all_test_feature_module_counts_as_test(self) -> None:
        # Whole test modules gated by a compound cfg expression (common in this
        # tree, e.g. legacy-sqlite-tests) must also count as test LoC.
        text = _src(
            """
            pub fn a() {}

            #[cfg(all(test, feature = "legacy-sqlite-tests"))]
            mod tests {
                #[test]
                fn t() {}
            }
            """
        )
        _prod, test = GEN.split_prod_test_lines(text)
        self.assertEqual(test, 5)

    def test_nested_all_test_not_feature_counts_as_test(self) -> None:
        # Balanced-paren predicate as used in src/server/routes/escalation.rs.
        text = _src(
            """
            pub fn a() {}

            #[cfg(all(test, not(feature = "legacy-sqlite-tests")))]
            mod tests {
                #[test]
                fn t() {}
            }
            """
        )
        _prod, test = GEN.split_prod_test_lines(text)
        self.assertEqual(test, 5)

    def test_quoted_test_feature_is_not_treated_as_test_cfg(self) -> None:
        # A feature literally containing "test" must NOT be mistaken for the
        # `test` cfg flag, and predicates that do not gate the module out of
        # production builds must not count as test.
        for predicate in (
            '#[cfg(feature = "legacy-sqlite-tests")]',
            '#[cfg(feature = "test")]',
            '#[cfg(all(unix, feature = "legacy-sqlite-tests"))]',
            '#[cfg(not(test))]',
            '#[cfg(any(test, feature = "x"))]',
        ):
            text = predicate + "\nmod m {\n    fn f() {}\n}\n"
            _prod, test = GEN.split_prod_test_lines(text)
            self.assertEqual(test, 0, predicate)

    def test_cfg_requires_test_predicate_logic(self) -> None:
        self.assertTrue(GEN.cfg_requires_test("test"))
        self.assertTrue(
            GEN.cfg_requires_test('all(test, feature = "legacy-sqlite-tests")')
        )
        self.assertTrue(
            GEN.cfg_requires_test('all(test, not(feature = "legacy-sqlite-tests"))')
        )
        self.assertTrue(GEN.cfg_requires_test("any(test, all(test, unix))"))
        self.assertFalse(GEN.cfg_requires_test("not(test)"))
        self.assertFalse(GEN.cfg_requires_test('any(test, feature = "x")'))
        self.assertFalse(GEN.cfg_requires_test('feature = "test"'))

    def test_giant_flag_uses_production_lines(self) -> None:
        # 1200 raw lines, but 400 of them are inside a #[cfg(test)] mod block,
        # leaving 800 production lines -> below the 1000 threshold -> not giant.
        prod_body = "\n".join(f"fn f{i}() {{}}" for i in range(800))
        test_lines = "\n".join("    // t" for _ in range(398))
        text = prod_body + "\n#[cfg(test)]\nmod tests {\n" + test_lines + "\n}\n"
        prod, test = GEN.split_prod_test_lines(text)
        self.assertLess(prod, GEN.GIANT_FILE_THRESHOLD)
        self.assertGreaterEqual(test, 1)


class TestOnlySubtreeTest(unittest.TestCase):
    def _with_src(self, files: dict[str, str]):
        from contextlib import contextmanager
        from tempfile import TemporaryDirectory
        from unittest import mock

        @contextmanager
        def ctx():
            with TemporaryDirectory() as tmp:
                root = Path(tmp)
                for rel, body in files.items():
                    target = root / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(_src(body), encoding="utf-8")
                src_root = root / "src"
                with mock.patch.object(GEN, "REPO_ROOT", root), mock.patch.object(
                    GEN, "SRC_ROOT", src_root
                ):
                    yield root

        return ctx()

    def test_child_of_cfg_test_parent_mod_is_test_only(self) -> None:
        files = {
            "src/server/routes/mod.rs": (
                """
                pub mod kanban;
                #[cfg(all(test, feature = "legacy-sqlite-tests"))]
                mod routes_tests;
                """
            ),
            "src/server/routes/kanban.rs": "pub fn k() {}\n",
            "src/server/routes/routes_tests/common.rs": "pub fn helper() {}\n",
        }
        with self._with_src(files):
            test_only = {GEN.rel_posix(p) for p in GEN.test_only_module_files()}
        self.assertIn("src/server/routes/routes_tests/common.rs", test_only)
        self.assertNotIn("src/server/routes/kanban.rs", test_only)

    def test_module_declared_in_both_contexts_stays_production(self) -> None:
        # `schema` is declared once unconditionally and once under test; it must
        # remain production (declared in a non-test context wins).
        files = {
            "src/db/mod.rs": (
                """
                pub mod schema;
                #[cfg(all(test, feature = "legacy-sqlite-tests"))]
                mod schema_extra;
                """
            ),
            "src/db/schema.rs": "pub fn s() {}\n",
            "src/db/schema_extra.rs": "pub fn e() {}\n",
        }
        with self._with_src(files):
            test_only = {GEN.rel_posix(p) for p in GEN.test_only_module_files()}
        self.assertNotIn("src/db/schema.rs", test_only)
        self.assertIn("src/db/schema_extra.rs", test_only)


class RegistryParseTest(unittest.TestCase):
    def test_strip_comment_keeps_hash_inside_string(self) -> None:
        self.assertEqual(
            GEN._strip_toml_comment('decompose_issue = "#3036"  # note'),
            'decompose_issue = "#3036"',
        )


class RegistryValidationTest(unittest.TestCase):
    def _module(self, path: str, prod: int, *, giant: bool) -> "GEN.ModuleEntry":
        flags = ("giant-file",) if giant else ()
        return GEN.ModuleEntry(
            module_path=path.replace("/", "::"),
            file_path=path,
            line_count=prod,
            prod_line_count=prod,
            test_line_count=0,
            flags=flags,
        )

    def _patch_registry(self, grandfathered, entries, baseline_paths=None):
        if baseline_paths is None:
            baseline_paths = list(grandfathered)
        return (
            lambda: (
                list(grandfathered),
                [dict(e) for e in entries],
                list(baseline_paths) if baseline_paths is not None else None,
            )
        )

    def test_unregistered_giant_fails(self) -> None:
        modules = [self._module("src/a.rs", 1500, giant=True)]
        orig = GEN.load_giant_file_registry
        GEN.load_giant_file_registry = self._patch_registry([], [])
        try:
            with self.assertRaises(GEN.ParseError) as ctx:
                GEN.build_giant_registrations(modules)
        finally:
            GEN.load_giant_file_registry = orig
        self.assertIn("unregistered giant", str(ctx.exception))

    def test_ghost_registration_fails(self) -> None:
        modules = [self._module("src/a.rs", 200, giant=False)]
        orig = GEN.load_giant_file_registry
        GEN.load_giant_file_registry = self._patch_registry(["src/a.rs"], [])
        try:
            with self.assertRaises(GEN.ParseError) as ctx:
                GEN.build_giant_registrations(modules)
        finally:
            GEN.load_giant_file_registry = orig
        self.assertIn("ghost registration", str(ctx.exception))

    def test_entry_missing_deadline_fails(self) -> None:
        modules = [self._module("src/a.rs", 1500, giant=True)]
        entry = {"file": "src/a.rs", "owner": "team", "decompose_issue": "#1"}
        orig = GEN.load_giant_file_registry
        GEN.load_giant_file_registry = self._patch_registry([], [entry])
        try:
            with self.assertRaises(GEN.ParseError) as ctx:
                GEN.build_giant_registrations(modules)
        finally:
            GEN.load_giant_file_registry = orig
        self.assertIn("deadline", str(ctx.exception))

    def test_grandfather_swap_in_outside_baseline_fails(self) -> None:
        # `src/b.rs` is a current giant grandfathered in, but it is NOT in the
        # frozen baseline (a swap-in for the retired `src/a.rs`). Even though the
        # count did not grow, this must fail: new giants need an [[entry]].
        modules = [
            self._module("src/b.rs", 1500, giant=True),
        ]
        orig = GEN.load_giant_file_registry
        GEN.load_giant_file_registry = self._patch_registry(
            ["src/b.rs"], [], baseline_paths=["src/a.rs"]
        )
        try:
            with self.assertRaises(GEN.ParseError) as ctx:
                GEN.build_giant_registrations(modules)
        finally:
            GEN.load_giant_file_registry = orig
        self.assertIn("frozen", str(ctx.exception))
        self.assertIn("src/b.rs", str(ctx.exception))

    def test_grandfather_shrink_within_baseline_passes(self) -> None:
        # Removing a path from grandfathered (decomposed) while it remains in the
        # frozen baseline is allowed.
        modules = [self._module("src/a.rs", 1500, giant=True)]
        orig = GEN.load_giant_file_registry
        GEN.load_giant_file_registry = self._patch_registry(
            ["src/a.rs"], [], baseline_paths=["src/a.rs", "src/retired.rs"]
        )
        try:
            regs = GEN.build_giant_registrations(modules)
        finally:
            GEN.load_giant_file_registry = orig
        self.assertEqual([r.file_path for r in regs], ["src/a.rs"])

    def test_missing_baseline_paths_fails(self) -> None:
        modules = [self._module("src/a.rs", 1500, giant=True)]
        orig = GEN.load_giant_file_registry
        GEN.load_giant_file_registry = lambda: (["src/a.rs"], [], None)
        try:
            with self.assertRaises(GEN.ParseError) as ctx:
                GEN.build_giant_registrations(modules)
        finally:
            GEN.load_giant_file_registry = orig
        self.assertIn("grandfathered_baseline_paths", str(ctx.exception))

    def test_valid_registry_builds_rows(self) -> None:
        modules = [
            self._module("src/grand.rs", 1200, giant=True),
            self._module("src/tracked.rs", 1500, giant=True),
        ]
        entry = {
            "file": "src/tracked.rs",
            "owner": "team",
            "deadline": "2026-08-31",
            "decompose_issue": "#3036",
        }
        orig = GEN.load_giant_file_registry
        GEN.load_giant_file_registry = self._patch_registry(["src/grand.rs"], [entry])
        try:
            regs = GEN.build_giant_registrations(modules)
        finally:
            GEN.load_giant_file_registry = orig
        by_path = {r.file_path: r for r in regs}
        self.assertEqual(by_path["src/grand.rs"].deadline, "")
        self.assertEqual(by_path["src/tracked.rs"].deadline, "2026-08-31")
        self.assertEqual(by_path["src/tracked.rs"].owner, "team")


if __name__ == "__main__":
    unittest.main()
