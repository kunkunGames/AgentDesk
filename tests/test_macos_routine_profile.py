import json
from pathlib import Path
import unittest

from scripts.stage_macos_routine_profile import profile_files


class NativeRoutineProfileTests(unittest.TestCase):
    def setUp(self):
        self.files = profile_files(
            Path('/Users/operator'), Path('/Users/operator/.adk/profiles/test'),
            {'modelProviders': {'openai': [{'id': 'z-ai/glm-5.3',
                'baseUrl': 'https://fixture.invalid/v1', 'envKey': 'FIXTURE_KEY',
                'apiKey': 'must-never-be-copied'}]}},
            {'default_agent': 'existing-agent', 'mcp': {'fixture': {'command': 'unused'}}},
        )

    def test_profile_reuses_auth_binding_without_copying_credentials(self):
        self.assertNotIn('must-never-be-copied', '\n'.join(self.files.values()))
        model = json.loads(self.files['qwen.json'])['modelProviders']['openai'][0]
        self.assertEqual(model['envKey'], 'FIXTURE_KEY')
        self.assertEqual(model['baseUrl'], 'https://fixture.invalid/v1')

    def test_heuristic_opt_out_retains_finite_native_limits_and_bounded_batch(self):
        bounds = json.loads(self.files['qwen.json'])['model']
        self.assertTrue(bounds['skipLoopDetection'])
        self.assertEqual((bounds['maxWallTimeSeconds'], bounds['maxToolCalls'],
                          bounds['maxSessionTurns']), (1800, 60, 30))
        env = json.loads(self.files['launchd-environment.json'])
        self.assertEqual(env['AGENTDESK_MONOLITH_PR_BATCH_SIZE'], '1')
        self.assertEqual(env['AGENTDESK_PR_BATCH_SIZE'], '1')
        wrapper = self.files['bin/qwen']
        self.assertIn('${QWEN_CODE_SYSTEM_SETTINGS_PATH:-', wrapper)
        self.assertIn('umask 077', wrapper)
        self.assertIn('--exclude-tools agent', wrapper)

    def test_opencode_uses_existing_agent_with_delegation_and_mcp_disabled(self):
        config = json.loads(self.files['opencode.json'])
        self.assertEqual(config['default_agent'], 'existing-agent')
        self.assertEqual(config['agent']['existing-agent']['steps'], 24)
        self.assertEqual(config['permission']['task'], 'deny')
        self.assertEqual(config['mcp']['fixture'], {'enabled': False})
        self.assertEqual(config['plugin'], [])


if __name__ == '__main__':
    unittest.main()
