import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import pxd_core as core


ID = 'sha256:' + 'a' * 64
TAG = 'polardbx/polardbx-sql:latest'


def fixture():
    return {
        'Id': 'f'*64, 'Name': '/pxc_single-cn-X-62261', 'Image': ID,
        'Config': {'Image': TAG, 'Entrypoint': ['/home/admin/entrypoint.sh', '20'],
                   'Cmd': [], 'Env': ['PATH=/usr/bin'], 'Labels': {},
                   'Hostname': 'f'*12, 'User': '', 'WorkingDir': ''},
        'HostConfig': {'NetworkMode': 'host', 'Privileged': False,
                       'PortBindings': {}, 'RestartPolicy': {'Name': 'no'}, 'Binds': []},
        'Mounts': [], 'NetworkSettings': {'Networks': {'host': {}}},
        'State': {'Running': True, 'Status': 'running'},
    }


class ImageRefSelection(unittest.TestCase):
    def run_case(self, resolved_tag, expected):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            bindir = root / 'bin'
            bindir.mkdir()
            stub = bindir / 'docker'
            stub.write_text('''#!/bin/bash
if [[ "$1 $2" == "ps -a" ]]; then exit 0; fi
if [[ "$1 $2" == "image inspect" ]]; then
  if [[ "$3" == "-f" ]]; then
    if [[ -z "$FAKE_TAG_ID" ]]; then exit 1; fi
    echo "$FAKE_TAG_ID"
    exit 0
  fi
  exit 0
fi
if [[ "$1" == "run" ]]; then
  printf '%s\\n' "$@" > "$RUN_LOG"
  exit 0
fi
exit 99
''')
            stub.chmod(0o700)
            with patch.object(core, 'image_config', return_value={'Env':['PATH=/usr/bin'], 'Labels': {}}):
                script = core.render_run_script(fixture(), 'cn.inspect.json')
            self.assertIn('"$IMAGE"', script)
            self.assertIn('IMAGE_ID=', script)
            self.assertIn('IMAGE_REF=', script)
            sp = root / 'run.sh'
            sp.write_text(script)
            self.assertEqual(subprocess.run(['bash','-n',str(sp)]).returncode,0)
            log = root / 'argv'
            env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ['PATH'],
                       RUN_LOG=str(log), FAKE_TAG_ID=resolved_tag)
            result = subprocess.run(['bash',str(sp)],env=env,capture_output=True,text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            args = log.read_text().splitlines()
            self.assertIn(expected, args)
            self.assertNotIn(TAG if expected == ID else ID, args)

    def test_matching_tag_kept(self):
        self.run_case(ID, TAG)

    def test_tag_points_to_different_id_falls_back(self):
        self.run_case('sha256:'+'b'*64, ID)

    def test_missing_tag_falls_back(self):
        self.run_case('', ID)
