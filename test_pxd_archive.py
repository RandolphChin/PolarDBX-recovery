#!/usr/bin/env python3
"""Offline tests for v3.1 recovery archival hardening; never run actual Docker."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pxd_cdc as cdc
import pxd_images as images


class ArchiveTests(unittest.TestCase):
    def fixture(self, root):
        admin = root / 'backup' / 'admin'
        (admin / 'binlog').mkdir(parents=True)
        (admin / 'logs' / 'rocksdb').mkdir(parents=True)
        (admin / 'binlog' / 'binlog.000001').write_bytes(b'binlog-1')
        (admin / 'logs' / 'rocksdb' / 'CURRENT').write_bytes(b'CURRENT')
        return admin

    def test_missing_cdc_index_after_original_deleted_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            admin = self.fixture(root)
            source = (root, root / 'backup', admin, root/'a.inspect.json',
                      {'Id': 'a'*64, 'Image': 'sha256:'+'a'*64}, {}, 'pxc_single')
            with patch.object(cdc, 'local_source', return_value=source), \
                 patch.object(cdc, 'unique_current', return_value=None):
                with self.assertRaisesRegex(SystemExit, '不能在事故发生后生成新清单'):
                    cdc.cmd_check(argparse.Namespace())
            self.assertFalse((root/'backup'/cdc.BACKUP_INDEX).exists())

    def test_signed_cdc_backup_can_be_checked_after_original_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            admin = self.fixture(root)
            cdc.check_backup(root/'backup', admin, write=True)
            source = (root, root / 'backup', admin, root/'a.inspect.json',
                      {'Id': 'a'*64, 'Image': 'sha256:'+'a'*64}, {}, 'pxc_single')
            with patch.object(cdc, 'local_source', return_value=source), \
                 patch.object(cdc, 'unique_current', return_value=None), \
                 patch.object(cdc, 'execute', return_value=subprocess.CompletedProcess([], 0, 'sha256:'+'a'*64, '')):
                cdc.cmd_check(argparse.Namespace())

    def test_stopped_origin_may_first_sign_backup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            admin = self.fixture(root)
            source = (root, root/'backup', admin, root/'a.inspect.json',
                      {'Id': 'a'*64, 'Image': 'sha256:'+'a'*64}, {}, 'pxc_single')
            current = {'Id':'a'*64,'State':{'Status':'exited','ExitCode':0,'OOMKilled':False}}
            with patch.object(cdc,'local_source',return_value=source), \
                 patch.object(cdc,'unique_current',return_value='pxc_single-cdc-X'), \
                 patch.object(cdc,'inspect_existing',return_value=current), \
                 patch.object(cdc,'verify_critical_copy'), \
                 patch.object(cdc,'execute',return_value=subprocess.CompletedProcess([],0,'sha256:'+'a'*64,'')):
                cdc.cmd_check(argparse.Namespace())
            self.assertTrue((root/'backup'/cdc.BACKUP_INDEX).is_file())

    def test_exact_image_ids_save_and_verify(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            id1='sha256:'+'a'*64
            id2='sha256:'+'b'*64
            inspects=[('a',{'Image':id1,'Name':'/pxc_single-gms-X'}),
                      ('b',{'Image':id1,'Name':'/pxc_single-dn-0-X'}),
                      ('c',{'Image':id2,'Name':'/pxc_single-cdc-X'})]
            def fake_run(argv, **kw):
                if argv[0:3]==['docker','image','inspect']:
                    return subprocess.CompletedProcess(argv,0,argv[-1]+'\n','')
                if argv[0:3]==['docker','image','save']:
                    Path(argv[argv.index('-o')+1]).write_bytes(b'image tar '+argv[-1].encode())
                    return subprocess.CompletedProcess(argv,0,'','')
                self.fail('unexpected command: '+str(argv))
            with patch.object(images.subprocess,'run',side_effect=fake_run):
                images.export_images(inspects,root/'images')
            images.verify_images(inspects,root/'images')
            self.assertEqual(len(list((root/'images').glob('*.tar'))),2)
            with self.assertRaisesRegex(SystemExit,'禁止覆盖'):
                images.export_images(inspects,root/'images')
            (root/'images'/images.filename_for(id2)).write_bytes(b'damaged')
            with self.assertRaisesRegex(SystemExit,'损坏'):
                images.verify_images(inspects,root/'images')

    def test_image_archive_rejects_different_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            id1='sha256:'+'a'*64
            id2='sha256:'+'b'*64
            (root/'images').mkdir()
            (root/'images'/images.RECEIPT).write_text(json.dumps({'version':1,'images':{id1:{}}}))
            with self.assertRaisesRegex(SystemExit,'不一致'):
                images.verify_images([('a',{'Image':id2,'Name':'/pxc_single-dn-0-X'})],root/'images')


if __name__=='__main__':
    unittest.main()
