"""Serial, offline regressions. The owner handler runs in-process; no socket or model."""
import argparse
import asyncio
import importlib.util
import io
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import asynccontextmanager, nullcontext, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

from hermes_cli import config, gateway_client, gateway_runtime

KEY = 'agent:main:telegram:group:-10042:11'
OTHER = 'agent:main:telegram:group:-10042:12'


class Fixture:
    def setUp(self):
        folder = self.enterContext(tempfile.TemporaryDirectory())
        self.home = Path(folder) / '.hermes'
        self.home.mkdir()
        self.scope = str(self.home / 'sessions')
        self.enterContext(patch.dict(os.environ, HOME=folder, HERMES_HOME=str(self.home)))
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        token = set_hermes_home_override(self.home)
        self.addCleanup(reset_hermes_home_override, token)
        spec = importlib.util.spec_from_file_location('walkie_fixture', Path(__file__).resolve().parents[1] / '__init__.py')
        self.plugin = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.plugin)
        self.ctx = SimpleNamespace(_manager=SimpleNamespace(home_path=self.home, owner_home=self.home),
                                   inject_message=lambda *a, **k: True)
        self.plugin._ctx = self.ctx
        self.settings = {'plugins': {'enabled': ['walkie-talkie'], 'entries': {
            'walkie-talkie': {'allow_gateway_injection': True}}}}
        self.enterContext(patch.object(config, 'load_config_readonly', side_effect=lambda *a, **kw: self.settings))

    def db(self):
        with sqlite3.connect(self.home / 'state.db') as con:
            con.executescript('CREATE TABLE sessions(id TEXT PRIMARY KEY,title TEXT);'
                              'CREATE TABLE gateway_routing(scope TEXT,session_key TEXT,entry_json TEXT,'
                              'PRIMARY KEY(scope,session_key));')

    def row(self, key, sid, *, scope=None, title='Ops Board'):
        with sqlite3.connect(self.home / 'state.db') as con:
            con.execute('INSERT OR REPLACE INTO sessions VALUES (?,?)', (sid, title))
            con.execute('INSERT OR REPLACE INTO gateway_routing VALUES (?,?,?)', (
                self.scope if scope is None else scope, key,
                json.dumps(dict(session_id=sid, platform='telegram', chat_type='group'))))

    async def send(self, target=KEY, message='hello'):
        out = self.plugin._message_topic(dict(target=target, message=message), session_id='sender')
        return json.loads(await out if asyncio.iscoroutine(out) else out)

    def wire(self, rpc=None, *, state='ready', owner=None, close_error=False, attach=None):
        self.endpoint = SimpleNamespace(profile_id=owner or str(self.home))
        self.discovery = self.enterContext(patch.object(gateway_runtime, 'discover_gateway_endpoint',
            return_value=SimpleNamespace(state=state, endpoint=self.endpoint if state == 'ready' else None)))
        self.calls = []

        @asynccontextmanager
        async def connect(endpoint):
            self.assertIs(endpoint, self.endpoint)
            async with (attach() if attach else nullcontext(rpc)) as dispatch:
                async def call(method, **params):
                    params.pop('_timeout', None)
                    self.calls.append((method, params))
                    assert dispatch is not None, 'unexpected fixture RPC'
                    return await dispatch(method, params)

                yield SimpleNamespace(rpc=call)
            if close_error:
                raise OSError('fixture close failed')

        self.connect = self.enterContext(patch.object(gateway_client, 'connect_gateway', side_effect=connect))

    def receipt(self):
        return {'ref': {'profile_id': str(self.home), 'session_id': 'canonical'}, 'admission_id': 'admission',
                'status': 'queued', 'outcome': None, 'sequence': 1, 'authority_epoch': 1,
                'execution_generation': None}


class Routes(Fixture, unittest.TestCase):
    def test_current_scope_and_default_namespace_take_only_unique_selectors(self):
        self.db()
        self.row(KEY, 'current')
        self.row(KEY, 'legacy', scope='')
        self.row(KEY, 'outside', scope='/different/sessions')
        self.row(KEY.replace('agent:main:', 'agent:alternate:'), 'foreign')
        self.row(KEY.replace('telegram', 'local'), 'unsupported')
        self.row(OTHER, 'current-other')
        self.enterContext(patch.object(self.plugin, '_topic_names', return_value={
            ('-10042', '11'): 'Shared', ('-10042', '12'): 'Shared'}))
        listed = json.loads(self.plugin._list_topics({}))
        self.assertEqual({r['session_id'] for r in listed['topics']}, {'current', 'current-other'})
        self.assertEqual(self.plugin._resolve(KEY)[0]['session_id'], 'current')
        self.assertEqual(self.plugin._resolve('current')[0]['session_id'], 'current')
        for bad in ('', 'shared', 'sha', 'ops board', 'legacy', 'outside', 'foreign'):
            self.assertIsNone(self.plugin._resolve(bad)[0], bad)
        self.row(OTHER, 'current')
        self.assertIsNone(self.plugin._resolve('current')[0])

    def test_native_registration_and_parser_call_the_same_helper(self):
        from hermes_cli.main import _attach_plugin_cli_command
        from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
        from tools.registry import registry
        manager = PluginManager(str(self.home))
        ctx = PluginContext(PluginManifest(name='walkie-talkie', version='0.1.5'), manager)
        self.plugin.register(ctx)
        tool = registry.snapshot_registration('message_topic', scope=manager.scope_key)
        self.assertTrue(tool.is_async)
        self.assertIsNotNone(registry.snapshot_registration('list_topics', scope=manager.scope_key))
        parser = argparse.ArgumentParser()
        _attach_plugin_cli_command(parser.add_subparsers(), manager._cli_commands['walkie-talkie'])
        args = parser.parse_args(['walkie-talkie', '--target', KEY, '--message-file', '-'])
        with patch.object(self.plugin, '_schedule_topic', return_value={'status': 'scheduled'}) as schedule:
            self.assertEqual(json.loads(asyncio.run(tool.handler({'target': KEY, 'message': 'tool'}))),
                             {'status': 'scheduled'})
            with patch('sys.stdin', io.StringIO('cli')), redirect_stdout(io.StringIO()) as stdout:
                args.func(args)
            self.assertEqual(json.loads(stdout.getvalue()), {'status': 'scheduled'})
            self.assertEqual(schedule.await_count, 2)
        with patch.object(self.plugin, '_schedule_topic', return_value={'status': 'unknown'}), \
                patch('sys.stdin', io.StringIO('cli')), redirect_stdout(io.StringIO()) as stdout, \
                self.assertRaises(SystemExit) as exit_event:
            args.func(args)
        self.assertNotEqual(exit_event.exception.code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {'status': 'unknown'})


class Outcomes(Fixture, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.db()
        self.row(KEY, 'physical')
        self.echo = self.enterContext(patch.object(self.plugin, '_post_visible', create=True))

    async def test_requires_live_consent_and_callers_home_before_discovery(self):
        self.wire(None)
        for consent in (False, 1, 'true', None):
            self.settings['plugins']['entries']['walkie-talkie']['allow_gateway_injection'] = consent
            self.assertIn('error', await self.send())
        self.settings['plugins']['entries']['walkie-talkie']['allow_gateway_injection'] = True
        self.settings['plugins']['disabled'] = ['walkie-talkie']
        self.assertIn('error', await self.send())
        self.settings['plugins']['disabled'] = []
        self.ctx._manager.home_path = self.home / 'profiles/foreign'
        self.assertIn('error', await self.send())
        self.discovery.assert_not_called()
        self.echo.assert_not_called()

    async def test_no_owner(self):
        self.wire(None, state='absent')
        self.assertEqual((await self.send())['status'], 'unavailable')
        self.connect.assert_not_called()

    async def test_foreign_endpoint(self):
        self.wire(None, owner='/other/profile')
        self.assertEqual((await self.send())['status'], 'refused')
        self.connect.assert_not_called()

    async def test_owner_refusal_releases_repeat_reservation(self):
        async def rpc(method, params):
            if method == 'session.resume':
                return {'session_id': 'canonical'}
            raise gateway_client.GatewayRPCError('permission_denied', 4001, {'reason': 'permission_denied'})
        self.wire(rpc, close_error=True)
        for _ in range(2):
            self.assertEqual((await self.send())['status'], 'refused')
        self.assertEqual([m for m, _ in self.calls], ['session.resume', 'prompt.submit'] * 2)
        self.echo.assert_not_called()

    async def test_route_moves_after_resume(self):
        async def rpc(method, params):
            self.row(KEY, 'reset')
            return {'session_id': 'canonical'}
        self.wire(rpc)
        out = await self.send()
        self.assertEqual(out['status'], 'refused')
        self.assertEqual([m for m, _ in self.calls], ['session.resume'])
        self.echo.assert_not_called()

    async def test_unknown_retains_repeat_reservation_with_no_replay_or_echo(self):
        async def rpc(method, params):
            if method == 'session.resume':
                return {'session_id': 'canonical', 'running': False}
            if isinstance(reply, Exception):
                raise reply
            return reply
        self.wire(rpc)
        for i, reply in enumerate((TimeoutError(), {'status': 'queued'},
                                   {**self.receipt(), 'ref': {'profile_id': '/other', 'session_id': 'canonical'}},
                                   {**self.receipt(), 'status': 'unknown'},
                                   {**self.receipt(), 'outcome': 'failed'},
                                   gateway_client.GatewayRPCError('storage_unavailable', 5001))):
            with self.subTest(reply=reply):
                out = await self.send(message=f'hello {i}')
                self.assertEqual(out['status'], 'unknown')
                self.assertIn('do not resend', out['error'].lower())
                self.assertEqual((await self.send(message=f'hello {i}'))['status'], 'dropped')
                self.assertEqual(len(self.calls), 2 * (i + 1))
                self.plugin._recent.clear()  # isolate each wire fault, not a live resend
        self.echo.assert_not_called()

    async def test_known_admission_survives_close_error_and_never_steers(self):
        async def rpc(method, params):
            if method == 'session.resume':
                return {'session_id': 'canonical', 'running': True}
            return self.receipt()
        self.wire(rpc, close_error=True)
        out = await self.send()
        self.assertEqual((out['status'], out['session_id']), ('scheduled', 'canonical'))
        self.assertEqual(self.calls[0], ('session.resume', {'session_id': 'physical', 'omit_messages': True}))
        method, submit = self.calls[1]
        self.assertEqual(method, 'prompt.submit')
        self.assertEqual(set(submit), {'session_id', 'text', 'submission_id', 'queued'})
        self.assertIs(submit['queued'], True)
        self.assertEqual(submit['session_id'], 'canonical')
        self.assertEqual(UUID(submit['submission_id']).version, 4)
        self.assertTrue(submit['text'].startswith('📻 from sender'))
        self.assertEqual(len(self.calls), 2)


class NativeOwner(Fixture, unittest.IsolatedAsyncioTestCase):
    async def lane(self, variant):
        # Based on core test_authority_cold_route_recovery.py: warm registration,
        # persistent route transition, cold authority with no in-memory mount.
        from gateway.authz_mixin import GatewayAuthorizationMixin
        from gateway.config import GatewayConfig, Platform
        from gateway.session import SessionSource, SessionStore
        from gateway.session_authority import initialize_session_authority
        from gateway.session_controls import AuthorityConnection
        from gateway.platforms.base import SendResult
        from persistence.conversation.lineage import CompressionChild
        from persistence.conversation.messages import NewMessage

        source = SessionSource(platform=Platform.TELEGRAM, chat_id='-10042', thread_id='11',
                               user_id='fixture-member', chat_type='group')
        accepted, delivered = [], []
        first, release = asyncio.Event(), asyncio.Event()

        async def handle(event):
            accepted.append((event.source.chat_id, event.source.thread_id, event.text))
            first.set()
            await release.wait()
            return 'fixture reply ' + event.text.splitlines()[-1]

        async def send_final(event, session_key, content, metadata, is_ephemeral_response, ttl, record_delivery):
            delivered.append((session_key, event.source.chat_id, str(metadata['thread_id']), content))
            record_delivery(SendResult(success=True, message_id='fixture-reply'))

        async def extract(text, *args, **kwargs):
            return SimpleNamespace(text_content=text)

        async def no_attachments(*args, **kwargs):
            pass

        adapter = SimpleNamespace(_unwrap_ephemeral=lambda text: (text, 0), _extract_response_content=extract,
                                  _send_final_text=send_final, _deliver_attachments=no_attachments)
        cfg = GatewayConfig(group_sessions_per_user=False)

        class Runner(GatewayAuthorizationMixin):
            def __init__(self, store):
                self.config, self.session_store, self._session_db = cfg, store, store.persistence
                self.adapters = {Platform.TELEGRAM: adapter}
                self._draining = False

            def _cached_agent_for(self, route):
                return None

            def _adapter_for_source(self, source):
                return self.adapters.get(source.platform)

            _handle_message = staticmethod(handle)

        async def authority(instance):
            store = SessionStore(self.home / 'sessions', cfg)
            self.addCleanup(store.close_all_db_handles)
            return await initialize_session_authority(Runner(store), profile_id=str(self.home), instance_id=instance)

        warm = await authority('warm')
        store = warm.runner.session_store
        root = warm.register(source).session_id
        key = store._generate_session_key(source)
        physical = root
        if variant == 'reset':
            root = physical = store.reset_session(key).session_id
        if variant == 'compression':
            physical = 'fixture-compression-tip'
            db = warm.persistence
            db.lineage.publish_compression_child(root, CompressionChild(session_id=physical, source='telegram'),
                [NewMessage.from_live({'role': 'user', 'content': 'fixture summary'})], require_compression_lease=False)
            self.assertIsNotNone(store.advance_compression_session(key, root, physical))
            db.routing.record_session_peer(physical, source='telegram', user_id=source.user_id, session_key=key,
                chat_id=source.chat_id, chat_type=source.chat_type, thread_id=source.thread_id,
                origin_json=json.dumps(source.to_dict()))
        cold = await authority('cold')
        self.assertNotIn(root, cold.sessions)

        class EventSink:
            def write(self, frame):
                return True

            def close(self):
                pass

        actors = []
        @asynccontextmanager
        async def attach():
            # Post-bootstrap boundary: the trusted operator grant comes from the transport,
            # never from the RPC request. Each plugin attachment owns its own native connection.
            connection = AuthorityConnection(cold, EventSink(), {
                'user_id': 'uid:1234', 'provider': 'local', 'native_bootstrap': True,
                'profile_id': str(self.home), 'instance_id': 'cold',
                'capabilities': ['session:read', 'session:submit']}, operator=True)
            actors.append(connection.actor.subject)

            async def rpc(method, params):
                from traceback import extract_tb

                try:
                    reply = await connection.dispatch({'jsonrpc': '2.0', 'id': len(self.calls),
                                                       'method': method, 'params': params})
                    if 'error' not in reply:
                        return reply['result']
                    error = gateway_client.rpc_error(reply['error'])
                    print('native dispatch', method, 'error', error.code, str(error), flush=True)
                    raise error
                except gateway_client.GatewayRPCError:
                    raise
                except Exception as exc:
                    frames = [(Path(f.filename).name, f.lineno, f.name)
                              for f in extract_tb(exc.__traceback__, limit=-10)]
                    detail = getattr(exc, 'name', None) if isinstance(exc, AttributeError) else None
                    if not isinstance(detail, str) or not detail.isidentifier() or len(detail) > 60:
                        detail = None
                    print('native dispatch', method, type(exc).__name__, detail, frames, flush=True)
                    raise
            try:
                yield rpc
            finally:
                await connection.close()

        self.wire(attach=attach)
        try:
            out = await self.send(key, 'one')
            self.assertEqual((out.get('status'), out.get('session_id')), ('scheduled', root), out)
            self.assertEqual(self.calls[0][1]['session_id'], physical)
            await asyncio.wait_for(first.wait(), 5)
            if variant == 'reset':
                self.assertEqual((await self.send(key, 'two'))['status'], 'scheduled')
                self.assertEqual(len(accepted), 1)
                rows = cold.persistence.admissions.list(session_id=root, pending_only=False)
                self.assertEqual([r.status for r in rows], ['started', 'queued'])
            self.assertFalse(cold.sessions[root].subscribers)
        finally:
            release.set()
            live = cold.sessions.get(root)
            if live is not None and live.task is not None:
                await asyncio.wait_for(live.task, 5)
        self.assertEqual([text.rsplit('\n', 1)[-1] for _, _, text in accepted],
                         ['one', 'two'] if variant == 'reset' else ['one'])
        self.assertEqual(delivered, [(key, '-10042', '11', 'fixture reply ' + text.rsplit('\n', 1)[-1])
                                     for _, _, text in accepted])
        self.assertEqual([m for m, _ in self.calls], ['session.resume', 'prompt.submit'] * len(accepted))
        rows = cold.persistence.admissions.list(session_id=root, pending_only=False)
        self.assertEqual([r.status for r in rows], ['terminal'] * len(accepted))
        self.assertEqual([r.principal_id for r in rows], actors)
        for row, (_, submit) in zip(rows, self.calls[1::2], strict=True):
            self.assertEqual(set(submit), {'session_id', 'text', 'submission_id', 'queued'})
            self.assertEqual(row.request_id, submit['submission_id'])
            self.assertEqual(row.outcome, 'completed')
            self.assertEqual(row.payload['local_operator_v1'], {
                'profile_id': str(self.home), 'session_id': root, 'principal_id': row.principal_id})
            self.assertNotIn('native_text_v1', row.payload)

    async def test_cold_existing_shared_telegram(self):
        await self.lane('existing')

    async def test_cold_fresh_reset_shared_telegram_busy_fifo(self):
        await self.lane('reset')

    async def test_cold_compression_tip_resumes_canonical_root(self):
        await self.lane('compression')
