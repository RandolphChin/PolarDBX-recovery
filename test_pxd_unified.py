#!/usr/bin/env python3
"""PXD v3.0 入口与 CDC 补充校验离线测试；不会连接 Docker。"""
import argparse
from contextlib import redirect_stdout, redirect_stderr
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pxd_recovery as rescue
import pxd_health as health


class FakeProcess:
    def __init__(self, data):
        self.stdout = io.BytesIO(data)
        self.stderr = io.BytesIO(b'')
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def wait(self):
        return 0


class TestUnified(unittest.TestCase):
    def test_cdc_must_not_skip_restore_data(self):
        args = argparse.Namespace(component='cdc', dir='/none', prefix='pxc_single', dry_run=True)
        with self.assertRaises(SystemExit):
            rescue.recover(args)
        args.component = 'all'
        with self.assertRaises(SystemExit):
            rescue.recover(args)

    def test_same_prefix_not_pick_first(self):
        with self.assertRaises(SystemExit):
            rescue.core.ensure_unique(['pxc_single-cdc-A', 'pxc_single-cdc-B'])

    def test_cdc_rename_logical_match(self):
        self.assertEqual(rescue.core.stem_of('pxc_single-cdc-mIMR'), 'pxc_single-cdc')
        self.assertEqual(rescue.core.stem_of('pxc_single-cdc-Qwer'), 'pxc_single-cdc')
        self.assertEqual(rescue.core.stem_of('pxc_single-dn-0-Another-3306'), 'pxc_single-dn-0')

    def test_backup_requires_four_roles(self):
        args = argparse.Namespace(prefix='pxc_single', dir='/none', refresh=False)
        with patch.object(rescue.core, 'docker_names', return_value=['pxc_single-gms-ABC']):
            with self.assertRaises(SystemExit):
                rescue.backup(args)

    def test_no_backup_overwrite_after_id_change(self):
        args = argparse.Namespace(prefix='pxc_single', dir='/none', refresh=False)
        names = ['pxc_single-gms-A','pxc_single-dn-0-A','pxc_single-cn-A','pxc_single-cdc-A']
        old = {'Name':'/pxc_single-gms-A','Id':'a'*64,'Config':{'Entrypoint':[]}}
        new = {'Name':'/pxc_single-gms-A','Id':'b'*64,'Config':{'Entrypoint':[]}}
        with patch.object(rescue.core,'docker_names',return_value=names), \
             patch.object(rescue,'valid_saved',return_value=[(Path('/tmp/x'),old)]), \
             patch.object(rescue.core,'run_docker') as docker:
            docker.return_value.stdout=json.dumps([new])
            with self.assertRaises(SystemExit):
                rescue.backup(args)

    def fake_cdc_context(self, d):
        base = Path(d)
        admin = base/'backup'/'admin'
        (admin/'binlog').mkdir(parents=True)
        (admin/'logs'/'rocksdb'/'Final').mkdir(parents=True)
        (admin/'binlog'/'binlog.000001').write_bytes(b'ABCD12')
        (admin/'logs'/'rocksdb'/'Final'/'CURRENT').write_bytes(b'hello')
        (base/'.pxc_single-cdc.restore-state.json').write_text(json.dumps({
            'stage':'started','name':'pxc_single-cdc-abcd','id':'c'*64,
            'backup_dir':str(base/'backup')
        }))
        args = argparse.Namespace(dir=str(base),prefix='pxc_single',backup_dir=str(base/'backup'))
        source = (base, base/'backup', admin, base/'x.inspect.json', {'Id':'o'*64}, {}, 'pxc_single')
        return args, source

    def test_cdc_sha256_historical_prefix_passes_with_append(self):
        with tempfile.TemporaryDirectory() as td:
            args, source = self.fake_cdc_context(td)
            with patch.object(rescue.cdc,'local_source',return_value=source), \
                 patch.object(rescue.cdc,'check_backup'), \
                 patch.object(rescue.cdc,'unique_current',return_value='pxc_single-cdc-abcd'), \
                 patch.object(rescue.cdc,'inspect_existing',return_value={'Id':'c'*64,'State':{'Running':True}}), \
                 patch.object(rescue.subprocess,'Popen',return_value=FakeProcess(b'ABCD12')):
                result = rescue.cdc_verify(args, quiet=True)
                self.assertEqual(result, 'c'*64)

    def test_cdc_sha256_rejects_wrong_copy(self):
        with tempfile.TemporaryDirectory() as td:
            args, source = self.fake_cdc_context(td)
            with patch.object(rescue.cdc,'local_source',return_value=source), \
                 patch.object(rescue.cdc,'check_backup'), \
                 patch.object(rescue.cdc,'unique_current',return_value='pxc_single-cdc-abcd'), \
                 patch.object(rescue.cdc,'inspect_existing',return_value={'Id':'c'*64,'State':{'Running':True}}), \
                 patch.object(rescue.subprocess,'Popen',return_value=FakeProcess(b'XBXD12')):
                with self.assertRaises(SystemExit):
                    rescue.cdc_verify(args, quiet=True)

    def test_cdc_file_inode_warning_suppressed_only_with_content_proof(self):
        before={'files':{'/home/admin/binlog/binlog.000001':{'inode':1,'size':123,'mtime':1}}}
        after={'files':{'/home/admin/binlog/binlog.000001':{'inode':2,'size':130,'mtime':2}}}
        with redirect_stdout(io.StringIO()):
            report1 = health.Report()
            health.diff_cdc(report1,before,after,recreated=True,verified_prefix=False)
            self.assertEqual(report1.n['warn'],1)
            report2 = health.Report()
            health.diff_cdc(report2,before,after,recreated=True,verified_prefix=True)
            self.assertEqual(report2.n['warn'],0)
            self.assertEqual(report2.n['ok'],1)

    def test_cdc_file_missing_even_with_proof_warns(self):
        before={'files':{'/home/admin/binlog/binlog.000001':{'inode':1,'size':123}}}
        after={'files':{}}
        with redirect_stdout(io.StringIO()):
            rep=health.Report()
            health.diff_cdc(rep,before,after,recreated=True,verified_prefix=True)
            self.assertEqual(rep.n['warn'],1)

    def test_cli_help_subcommands(self):
        result = subprocess.run(['python3', str(Path(rescue.__file__)), '--help'],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode,0)
        for s in ('cdc-backup','cdc-create','cdc-restore-data','cdc-verify','recover','diff'):
            self.assertIn(s,result.stdout)


if __name__=='__main__':
    unittest.main()
