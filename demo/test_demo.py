"""Offline regression checks; no robot or real CSE is contacted."""

from __future__ import annotations

import copy
import importlib.util
import os
import runpy
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("demo_run", Path(__file__).with_name("run.py"))
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


class MotionTests(unittest.TestCase):
    def test_normal_completion_sends_three_stops(self):
        send = Mock()
        with patch.object(demo.time, "sleep"):
            demo.run_motion(send, 0, 0, 0)
        self.assertEqual(send.call_count, 3)
        self.assertTrue(all(c.args[0] == demo.velocity(0, 0) for c in send.call_args_list))

    def test_interrupt_and_transport_failure_both_attempt_stop(self):
        for error in (KeyboardInterrupt, OSError):
            with self.subTest(error=error):
                send = Mock(side_effect=[error(), None, None, None])
                with patch.object(demo.time, "sleep"), self.assertRaises(error):
                    demo.run_motion(send, 0.05, 0, 2)
                self.assertEqual(send.call_count, 4)
                self.assertEqual(send.call_args_list[0].args[0], demo.velocity(0.05, 0))
                self.assertTrue(all(c.args[0] == demo.velocity(0, 0)
                                    for c in send.call_args_list[1:]))

    def test_invalid_motion_is_never_sent(self):
        for args in ((float('nan'), 0, 1), (0, float('inf'), 1), (0.2, 0, 1),
                     (0, 0.6, 1), (0, 0, 6), (0, 0, -1)):
            send = Mock()
            with self.subTest(args=args), self.assertRaises(ValueError):
                demo.run_motion(send, *args)
            send.assert_not_called()

    def test_all_failed_stops_are_reported(self):
        send = Mock(side_effect=OSError('disconnected'))
        with patch.object(demo.time, "sleep"), self.assertRaisesRegex(RuntimeError, 'No stop'):
            demo.run_motion(send, 0, 0, 0)
        self.assertEqual(send.call_count, 3)


class ContractTests(unittest.TestCase):
    def test_command_cin_passes_real_notification_parser_and_dispatcher(self):
        from ipe.core.command import CommandDispatchManager
        from ipe.core.models import TopicSpec
        from ipe.onem2m.notification import parse_notification
        from ipe.qos.models import QoSSpec

        client = demo.CSE(SimpleNamespace(cse=SimpleNamespace(
            protocol='http', rvi='3', cse_base='TinyIoT', ae_name='ros2-ipe')), 'CAdmin')
        try:
            client.request = Mock(return_value={'m2m:cin': {'ri': 'test-cin'}})
            client.send(demo.velocity(0.05, 0.2))
            args, kwargs = client.request.call_args
            self.assertEqual(args, ('POST', '/TinyIoT/ros2-ipe/robots/robot/topics/command/cmd_vel'))
            cin = kwargs['json']['m2m:cin']
            self.assertIsInstance(cin['con'], str)
            notif = parse_notification({'m2m:sgn': {'nev': {'rep': {'m2m:cin': cin}}}})
            payload = dict(notif.con)
            command_id = payload.pop('commandId')
            self.assertTrue(command_id.startswith('demo-'))
            publish = Mock(return_value=True)
            topic = TopicSpec('robot', '/cmd_vel', 'geometry_msgs/msg/Twist',
                              'both', 'historical', QoSSpec(), access_enabled=True)
            outcome = CommandDispatchManager(publish).dispatch(topic, payload, time.time(), time.monotonic())
            self.assertTrue(outcome.published)
            publish.assert_called_once_with(topic, demo.velocity(0.05, 0.2))
        finally:
            client.session.close()

    def test_graph_escapes_labels_and_shows_mapping(self):
        rendered = demo.graph_html([{'name': '/odom', 'types': ['nav_msgs/msg/Odometry'],
                                    'publishers': ['/robot<script>'], 'subscribers': ['/ipe'],
                                    'paths': ['/TinyIoT/ros2-ipe/robots/robot/topics/observe/odom']}])
        self.assertIn('/robot&lt;script&gt;', rendered)
        self.assertNotIn('<script>', rendered)
        self.assertIn('/topics/observe/odom', rendered)


class ComposeConfigTests(unittest.TestCase):
    def load(self, **overrides):
        import config as original

        before = copy.deepcopy(original.CONFIG)
        env = {k: v for k, v in os.environ.items() if not k.startswith('IPE_')}
        env.update(overrides)
        with patch.dict(sys.modules, {'base_config': original}), patch.dict(os.environ, env, clear=True):
            settings = runpy.run_path(str(ROOT / 'deploy/compose_config.py'))
        self.assertEqual(original.CONFIG, before)
        return settings

    def test_native_config_is_preserved_and_container_state_is_portable(self):
        settings = self.load(IPE_CSE_ENDPOINT='http://192.0.2.10:3000',
                             IPE_CSE_POA='http://192.0.2.20:5050', IPE_ROS_DOMAIN_ID='91')
        config = settings['CONFIG']
        self.assertEqual(config['storage']['backend'], 'sqlite')
        self.assertEqual(config['storage']['state_db'], '/var/lib/ipe/state.db')
        self.assertEqual(config['cse']['poa'], 'http://192.0.2.20:5050')
        self.assertEqual(config['discovery']['domain_id'], 91)

    def test_demo_mapping_resolves_only_selected_topics_and_has_control_guards(self):
        from ipe.runtime.planning import resolve
        from ipe.runtime.settings import validate_config

        settings = self.load(IPE_DEMO='1')
        snapshot = {'topics': [('/odom', ['nav_msgs/msg/Odometry']),
                               ('/cmd_vel', ['geometry_msgs/msg/Twist']),
                               ('/scan', ['sensor_msgs/msg/LaserScan'])],
                    'services': [('/reset', ['std_srvs/srv/Empty'])], 'actions': []}
        resolved = resolve(validate_config(settings['CONFIG']), discovered=snapshot)
        topics = {t.interface: t for t in resolved.topics}
        self.assertEqual(set(topics), {'/odom', '/cmd_vel'})
        self.assertFalse(resolved.services)
        command = topics['/cmd_vel']
        self.assertTrue(command.access_enabled)
        self.assertEqual(command.confirm, 'auto')
        self.assertEqual(command.command.watchdog_ms, 1000)
        self.assertEqual(command.command.max_age_ms, 2000)
        self.assertEqual(topics['/odom'].sample.interval_sec, 1.0)


if __name__ == '__main__':
    unittest.main()
