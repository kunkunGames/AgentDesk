"""Unit tests for the multi-provider E2E matrix runner."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts" / "e2e"))

import run_multi_provider_matrix as matrix  # noqa: E402


class MatrixConfig(unittest.TestCase):
    def test_load_channel_ids_from_agentdesk_yaml_shape(self):
        yaml = """
agents:
  - id: adk-claude-pipe-e2e
    channels:
      claude: {id: "111"}
  - id: adk-claude-tui-e2e
    channels:
      claude: {id: "222"}
  - id: adk-claude-e-e2e
    channels:
      claude: {id: "333"}
  - id: adk-codex-pipe-e2e
    channels:
      codex: {id: "444"}
  - id: adk-codex-tui-e2e
    channels:
      codex: {id: "555"}
"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "agentdesk.yaml"
            path.write_text(yaml, encoding="utf-8")
            resolved = matrix.load_channel_ids(path)

        self.assertEqual(resolved["claude-pipe"], "111")
        self.assertEqual(resolved["claude-tui"], "222")
        self.assertEqual(resolved["claude-e"], "333")
        self.assertEqual(resolved["codex-pipe"], "444")
        self.assertEqual(resolved["codex-tui"], "555")

    def test_parse_cells_rejects_unknown_cell(self):
        with self.assertRaises(ValueError):
            matrix.parse_cells("claude-pipe,unknown")


if __name__ == "__main__":
    unittest.main()
