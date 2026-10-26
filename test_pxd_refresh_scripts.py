import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import pxd_core as core
import pxd_recovery as app

ID = 'sha256:' + 'c'*64


def fixture(role):
    n = 'pxc_single-'+role+'-X'
    return {
        'Id': 'f'*64, 'Name': '/'+n, 'Image': ID,
        'Config': {'Image': 'example/test:latest', 'Entrypoint': [], 'Cmd': ['bash','-c','true'],
                   'Env': ['PATH=/usr/bin'], 'Labels': {}, 'Hostname': 'f'*12,
                   'User': '', 'WorkingDir': ''},
        'HostConfig': {'NetworkMode': 'host', 'Privileged': False,
                       'PortBindings': {}, 'RestartPolicy': {'Name': 'no'}, 'Binds': []},
        'Mounts': [], 'NetworkSettings': {'Networks': {'host': {}}},
        'State': {'Running': True, 'Status': 'running'},
    }


class RefreshTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        roles = ('gms', 'dn-0', 'cn', 'cdc')
        guards = {}
        for role in roles:
            d = fixture(role)
            name = core.container_name(d)
            (self.root / (name + '.inspect.json')).write_text(json.dumps(d))
            guards[name] = {'binds': {}, 'volumes': []}
            if role == 'cdc':
                body = 'set -e\necho "ERROR: CDC 不允许直接 docker run"\nexit 2\n'
            else:
                body = '#!/bin/bash\necho OLD_SCRIPT\n'
            (self.root / (name + '.run.sh')).write_text(body)
        (self.root/'restore-all.sh').write_text('echo "ERROR: 四组件不得使用旧 restore-all.sh"\nexit 2\n')
        (self.root/'mount-guards.json').write_text(json.dumps({'prefix':'pxc_single', 'guards':guards}))
        self.args = argparse.Namespace(dir=str(self.root), prefix='pxc_single', apply=False, confirm='')

    def test_refresh_only_three_and_backup(self):
        with patch.object(core, 'image_config', return_value={'Env':['PATH=/usr/bin'], 'Labels':{}}):
            app.refresh_image_scripts(self.args)
            self.assertIn('OLD_SCRIPT', (self.root/'pxc_single-cn-X.run.sh').read_text())
            self.args.apply = True
            self.args.confirm = 'pxc_single'
            app.refresh_image_scripts(self.args)
        self.assertIn('IMAGE_REF=',(self.root/'pxc_single-cn-X.run.sh').read_text())
        self.assertIn('IMAGE_ID=',(self.root/'pxc_single-gms-X.run.sh').read_text())
        self.assertIn('IMAGE_REF=',(self.root/'pxc_single-dn-0-X.run.sh').read_text())
        self.assertTrue(list(self.root.glob('*.run.sh.*.bak')))
        self.assertIn('CDC 不允许直接 docker run',(self.root/'pxc_single-cdc-X.run.sh').read_text())
        self.assertIn('四组件不得使用旧 restore-all.sh',(self.root/'restore-all.sh').read_text())
        self.assertIn('example/test:latest',(self.root/'pxc_single-cn-X.inspect.json').read_text())

    def test_wrong_confirmation_refuses(self):
        with patch.object(core, 'image_config', return_value={'Env':['PATH=/usr/bin'], 'Labels':{}}):
            self.args.apply = True
            self.args.confirm = 'wrong'
            with self.assertRaises(SystemExit):
                app.refresh_image_scripts(self.args)
        self.assertIn('OLD_SCRIPT',(self.root/'pxc_single-cn-X.run.sh').read_text())

    def test_cdc_guard_prevents_refresh(self):
        (self.root/'pxc_single-cdc-X.run.sh').write_text('#!/bin/bash\ndocker run ...\n')
        with patch.object(core, 'image_config', return_value={'Env':['PATH=/usr/bin'], 'Labels':{}}):
            with self.assertRaises(SystemExit):
                app.refresh_image_scripts(self.args)
        self.assertIn('OLD_SCRIPT',(self.root/'pxc_single-cn-X.run.sh').read_text())
