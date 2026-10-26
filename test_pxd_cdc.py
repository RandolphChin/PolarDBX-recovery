#!/usr/bin/env python3
import json
import pathlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pxd_cdc as cdc


class TestCDC(unittest.TestCase):
    def saved(self):
        return {
            'Name': '/pxc_single-cdc-ZxYw',
            'Id': 'a' * 64,
            'Image': 'sha256:' + 'a' * 64,
            'Config': {'Image': 'polardbx/polardbx-cdc:latest',
                       'Entrypoint': [], 'Cmd': ['/bin/sh', '-c', '/home/admin/app.sh'],
                       'Env': ['FOO=BAR']},
            'HostConfig': {'NetworkMode': 'host', 'Binds': [],
                           'RestartPolicy': {'Name': 'no'}},
            'NetworkSettings': {'Networks': {}}, 'Mounts': []
        }

    def temp_backup(self, root):
        admin = root / 'admin'
        (admin / 'binlog').mkdir(parents=True)
        (admin / 'logs' / 'rocksdb' / 'Final').mkdir(parents=True)
        (admin / 'binlog' / 'binlog.000001').write_bytes(b'abc123')
        (admin / 'logs' / 'rocksdb' / 'Final' / 'CURRENT').write_bytes(b'CURRENT\n')
        (admin / 'env').mkdir()
        (admin / 'env' / 'env.properties').write_text('a=b')
        return admin

    def test_create_only_script_preserves_entrypoint(self):
        with tempfile.TemporaryDirectory() as td:
            p = pathlib.Path(td) / 'test.py'
            script = cdc.make_create_script(self.saved(), p, {'binds': {}, 'volumes': []})
            self.assertIn('docker create \\\n', script)
            self.assertIn("--entrypoint ''", script)
            self.assertIn('/home/admin/app.sh', script)
            self.assertNotIn('docker run \\\n', script)
            self.assertNotIn('  -d \\\n', script)
            path = pathlib.Path(td) / 'create.sh'
            path.write_text(script)
            subprocess.run(['bash', '-n', str(path)], check=True)

    def test_backup_index_and_corruption(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            admin = self.temp_backup(root)
            index1 = cdc.check_backup(root, admin, write=True)
            index2 = cdc.check_backup(root, admin)
            self.assertEqual(index1, index2)
            (admin / 'binlog' / 'binlog.000001').write_bytes(b'corrupted')
            with self.assertRaises(SystemExit):
                cdc.check_backup(root, admin)

    def test_missing_rocksdb_detected(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            admin = self.temp_backup(root)
            (admin / 'logs' / 'rocksdb' / 'Final' / 'CURRENT').unlink()
            with self.assertRaises(SystemExit):
                cdc.secure_regular_tree(admin)

    def test_name_dynamic(self):
        self.assertEqual(cdc.core.stem_of('pxc_single-cdc-ZxYw'), 'pxc_single-cdc')
        self.assertEqual(cdc.core.stem_of('abc_cdc-cdc-Other'), 'abc_cdc-cdc')

    def test_only_created_accepted(self):
        with tempfile.TemporaryDirectory() as td:
            p = pathlib.Path(td)
            st = {'stage': 'created', 'name': 'pxc_single-cdc-ZxYw', 'id': 'aa', 'backup_dir': str(p)}
            cdc.store_json(cdc.state_path(p, 'pxc_single'), st)
            with patch.object(cdc, 'inspect_existing', return_value={'Id': 'aa', 'State': {'Status': 'running'}}):
                with self.assertRaises(SystemExit):
                    cdc.read_state(p, 'pxc_single', ('created',), p)

    def test_restore_data_does_not_start(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            admin = self.temp_backup(root)
            out = root / 'work'
            out.mkdir()
            src = out / 'fake.inspect.json'
            st = {'stage': 'created', 'name': 'pxc_single-cdc-ZxYw',
                  'id': 'a' * 64, 'backup_dir': str(root)}
            cdc.store_json(cdc.state_path(out, 'pxc_single'), st)
            from types import SimpleNamespace
            args = SimpleNamespace()
            calls = []
            with patch.object(cdc, 'local_source', return_value=(out, root, admin, src, self.saved(), {}, 'pxc_single')), \
                 patch.object(cdc, 'check_backup'), \
                 patch.object(cdc, 'inspect_existing', return_value={'Id': 'a' * 64, 'State': {'Status': 'created'}}), \
                 patch.object(cdc, 'execute', side_effect=lambda a, capture=False: calls.append(a)), \
                 patch.object(cdc, 'verify_critical_copy') as verify:
                cdc.cmd_restore_data(args)
            self.assertTrue(calls)
            self.assertTrue(all(a[0:2] == ['docker', 'cp'] for a in calls))
            self.assertEqual(cdc.state_path(out, 'pxc_single').read_text(encoding='utf-8').count('restored'), 1)
            verify.assert_called_once()

    def test_start_requires_restored(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            admin = self.temp_backup(root)
            out = root / 'work'
            out.mkdir()
            src = out / 'fake.inspect.json'
            st = {'stage': 'restored', 'name': 'pxc_single-cdc-ZxYw',
                  'id': 'a' * 64, 'backup_dir': str(root)}
            cdc.store_json(cdc.state_path(out, 'pxc_single'), st)
            from types import SimpleNamespace
            args = SimpleNamespace(dry_run=False)
            calls = []
            with patch.object(cdc, 'local_source', return_value=(out, root, admin, src, self.saved(), {}, 'pxc_single')), \
                 patch.object(cdc, 'check_backup'), \
                 patch.object(cdc, 'inspect_existing', return_value={'Id': 'a' * 64, 'State': {'Status': 'created'}}), \
                 patch.object(cdc, 'execute', side_effect=lambda a, capture=False: calls.append(a)):
                cdc.cmd_start(args)
            self.assertEqual(calls, [['docker', 'start', 'pxc_single-cdc-ZxYw']])

if __name__ == '__main__':
    unittest.main()
