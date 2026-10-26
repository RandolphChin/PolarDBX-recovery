import argparse
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import pxd_registry as reg

HOST = '10.35.18.43'
PREFIX = 'pxc_single'
NAMES = {
    'gms': 'pxc_single-gms-Cand-14992',
    'dn-0': 'pxc_single-dn-0-Cand-15771',
    'cn': 'pxc_single-cn-ggBp-62261',
    'cdc': 'pxc_single-cdc-vHxu',
}
OLD = {'gms': 'a'*12, 'dn-0': 'b'*12, 'cn': 'c'*12, 'cdc': 'd'*12}
NEW = {'gms': 'e'*64, 'dn-0': 'f'*64, 'cn': '1'*64, 'cdc': '2'*64}


def fixture(path):
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE container(id INTEGER PRIMARY KEY, container_name TEXT, '
                   'container_id TEXT, host TEXT, resource_name TEXT, container_ip TEXT, gmt_modified TEXT)')
        db.execute('CREATE TABLE polardbx_cluster(pxc_name TEXT)')
        db.execute('CREATE TABLE polardbx_xdb(xdb_name TEXT, pxc_name TEXT)')
        db.execute('INSERT INTO polardbx_cluster VALUES (?)', (PREFIX,))
        db.execute('INSERT INTO polardbx_xdb VALUES (?,?)', (PREFIX+'-gms',PREFIX))
        db.execute('INSERT INTO polardbx_xdb VALUES (?,?)', (PREFIX+'-dn-0',PREFIX))
        for key, name in NAMES.items():
            resource = PREFIX if key in ('cn','cdc') else PREFIX + '-' + key
            db.execute('INSERT INTO container(container_name,container_id,host,resource_name,container_ip) '
                       'VALUES (?,?,?,?,?)', (name,OLD[key],HOST,resource,HOST))


def inspect(name):
    role = next(k for k,n in NAMES.items() if n == name)
    return {'Id': NEW[role], 'Name': '/'+name, 'Image': 'sha256:'+'9'*64,
            'State': {'Running':True}, 'NetworkSettings': {'Networks':{}}}


def args(path, action='pxd-check', apply=False):
    return argparse.Namespace(prefix=PREFIX,db=str(path),pxd_host=HOST,inspect_dir='',
                              allow_ip_mismatch=False,apply=apply,confirm=PREFIX if apply else '',
                              backup_dir=str(path.parent/'backups'),action=action)


class RegistryTests(unittest.TestCase):
    def test_readonly_check_preserves_sqlite(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'polardbx.db'; fixture(path)
            with patch.object(reg,'inspect_name',side_effect=inspect), \
                 patch.object(reg,'docker_old_id_exists',return_value=False):
                reg.run(args(path))
            with sqlite3.connect(path) as db:
                self.assertEqual([row[0] for row in db.execute('SELECT container_id FROM container ORDER BY id')],
                                 [OLD['gms'],OLD['dn-0'],OLD['cn'],OLD['cdc']])
            self.assertFalse((path.parent/'backups').exists())

    def test_update_four_rows_with_backup_and_preserve_other_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'polardbx.db'; fixture(path)
            with patch.object(reg,'inspect_name',side_effect=inspect), \
                 patch.object(reg,'docker_old_id_exists',return_value=False), \
                 patch.object(reg.os,'geteuid',return_value=1000):
                reg.run(args(path,'pxd-sync',True))
            with sqlite3.connect(path) as db:
                got = dict(db.execute('SELECT container_name, container_id FROM container'))
                for role, name in NAMES.items():
                    self.assertEqual(got[name], NEW[role][:12])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM polardbx_cluster').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM polardbx_xdb').fetchone()[0],2)
            backups=list((path.parent/'backups').glob('*.db'))
            self.assertEqual(len(backups),1)
            with sqlite3.connect(backups[0]) as db:
                self.assertEqual(db.execute('SELECT container_id FROM container WHERE container_name=?',
                                            (NAMES['gms'],)).fetchone()[0], OLD['gms'])

    def test_check_after_sync_with_original_baseline_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'polardbx.db'; fixture(path)
            folder=Path(tmp)/'inspect'; folder.mkdir()
            import json
            for role,name in NAMES.items():
                (folder/(name+'.inspect.json')).write_text(json.dumps({
                    'Name':'/'+name, 'Id':OLD[role]+'0'*52,
                    'Image':'sha256:'+'9'*64}))
            a = args(path,'pxd-sync',True)
            a.inspect_dir = str(folder)
            with patch.object(reg,'inspect_name',side_effect=inspect), \
                 patch.object(reg,'docker_old_id_exists',return_value=False), \
                 patch.object(reg.os,'geteuid',return_value=1000):
                reg.run(a)
                with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True) as db:
                    db.row_factory=sqlite3.Row
                    changes=reg.compute_plan(db,PREFIX,HOST,str(folder))
                    self.assertTrue(all(not x['changed'] for x in changes))

    def test_must_not_sync_after_pxd_delete(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'polardbx.db'; fixture(path)
            with sqlite3.connect(path) as db:
                db.execute('DELETE FROM polardbx_cluster')
            with sqlite3.connect('file:'+str(path)+'?mode=ro', uri=True) as db:
                db.row_factory=sqlite3.Row
                with self.assertRaisesRegex(RuntimeError,'集群'):
                    reg.compute_plan(db,PREFIX,HOST)

    def test_stale_or_wrong_host_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'polardbx.db'; fixture(path)
            with sqlite3.connect('file:'+str(path)+'?mode=ro', uri=True) as db:
                db.row_factory=sqlite3.Row
                with self.assertRaisesRegex(RuntimeError,'host'):
                    reg.compute_plan(db,PREFIX,'10.1.2.3')

    def test_existing_old_id_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'polardbx.db'; fixture(path)
            with sqlite3.connect('file:'+str(path)+'?mode=ro', uri=True) as db:
                db.row_factory=sqlite3.Row
                with patch.object(reg,'inspect_name',side_effect=inspect), \
                     patch.object(reg,'docker_old_id_exists',return_value=True):
                    with self.assertRaisesRegex(RuntimeError,'旧 ID'):
                        reg.compute_plan(db,PREFIX,HOST)

    def test_baseline_original_id_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'polardbx.db'; fixture(path)
            folder=Path(tmp)/'inspect'; folder.mkdir()
            import json
            for role,name in NAMES.items():
                (folder/(name+'.inspect.json')).write_text(json.dumps({
                    'Name':'/'+name, 'Id':'0'*64, 'Image':'sha256:'+'9'*64}))
            with sqlite3.connect('file:'+str(path)+'?mode=ro', uri=True) as db:
                db.row_factory=sqlite3.Row
                with patch.object(reg,'inspect_name',side_effect=inspect):
                    with self.assertRaisesRegex(RuntimeError,'旧 ID|均不一致'):
                        reg.compute_plan(db,PREFIX,HOST,str(folder))

    def test_stopped_container_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'polardbx.db'; fixture(path)
            def stopped(name):
                obj=inspect(name)
                obj['State']['Running']=False
                return obj
            with sqlite3.connect('file:'+str(path)+'?mode=ro', uri=True) as db:
                db.row_factory=sqlite3.Row
                with patch.object(reg,'inspect_name',side_effect=stopped):
                    with self.assertRaisesRegex(RuntimeError,'运行状态'):
                        reg.compute_plan(db,PREFIX,HOST)

    def test_ip_mismatch_requires_acknowledgement(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'polardbx.db'; fixture(path)
            with sqlite3.connect(path) as db:
                db.execute('UPDATE container SET container_ip=? WHERE container_name=?',
                           ('172.20.0.7', NAMES['cn']))
            def with_ip(name):
                obj=inspect(name)
                obj['NetworkSettings']['Networks']={'bridge':{'IPAddress':'172.20.0.8'}}
                return obj
            with sqlite3.connect('file:'+str(path)+'?mode=ro', uri=True) as db:
                db.row_factory=sqlite3.Row
                with patch.object(reg,'inspect_name',side_effect=with_ip), \
                     patch.object(reg,'docker_old_id_exists',return_value=False):
                    with self.assertRaisesRegex(RuntimeError,'container_ip'):
                        reg.compute_plan(db,PREFIX,HOST)
                    records=reg.compute_plan(db,PREFIX,HOST,allow_ip_mismatch=True)
                    self.assertTrue(any(c['ip_warning'] for c in records))

if __name__=='__main__':
    unittest.main()
