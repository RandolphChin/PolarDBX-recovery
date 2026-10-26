#!/usr/bin/env python3
"""离线模拟测试：不连接真实 Docker、不删除容器。"""
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch

P = Path(__file__).with_name('pxd_core.py')
spec = importlib.util.spec_from_file_location('pxd_core', P)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def fixture(name, ep, cmd, image='polardbx/polardbx-engine:latest', image_id='sha256:'+'3'*64):
    return {
        'Id':'a'*64, 'Name':'/'+name,'Image':image_id,
        'Config':{'Image':image,'Entrypoint':ep,'Cmd':cmd,'Env':['PATH=/usr/bin','NODE_IP=10.35.18.43'],
                  'Labels':{},'Hostname':'a'*12,'User':'','WorkingDir':''},
        'HostConfig':{'NetworkMode':'host','Privileged':False,'PortBindings':{},'RestartPolicy':{'Name':'no'},
                      'Binds':[]},
        'Mounts':[],
        'NetworkSettings':{'Networks':{'host':{}}},
        'State':{'Running':True, 'Status':'running'},
    }


class PxdRecoverTests(unittest.TestCase):
    def setUp(self):
        self.patches = patch.object(mod, 'image_config', return_value={'Env':['PATH=/usr/bin'],'Labels':{}})
        self.patches.start()
        self.addCleanup(self.patches.stop)

    def test_stems_and_prefix(self):
        self.assertEqual(mod.stem_of('pxc_single-dn-0-Cand-16312'), 'pxc_single-dn-0')
        self.assertEqual(mod.stem_of('pxc_single-dn-0-AbCd-28971'), 'pxc_single-dn-0')
        self.assertEqual(mod.stem_of('pxc_single-cn-EpcE-56037'), 'pxc_single-cn')
        self.assertTrue(mod.matches_cluster('pxc_single-cdc-mIMR', 'pxc_single'))
        self.assertFalse(mod.matches_cluster('pxc_single2-dn-0-xxx', 'pxc_single'))
        self.assertIsNone(mod.stem_of('rustfs'))
        with self.assertRaises(SystemExit):
            mod.ensure_unique(['pxc_single-dn-0-Cand-16312','pxc_single-dn-0-AbCd-28971'])

    def test_shell_syntax_and_entrypoints(self):
        cases = [
            ('dn','pxc_single-dn-0-Cand-16312',[],['bash','-c','python /opt/entrypoint.py'], "--entrypoint ''"),
            ('gms','pxc_single-gms-Cand-15402',[],['bash','-c','python /opt/entrypoint.py'], "--entrypoint ''"),
            ('cn','pxc_single-cn-EpcE-56037',['/home/admin/entrypoint.sh','20'],None, "--entrypoint /home/admin/entrypoint.sh"),
            ('cdc','pxc_single-cdc-mIMR',None,['/bin/sh','-c','/home/admin/app.sh'], None),
        ]
        for label, name, ep, cmd, target in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as td:
                d = fixture(name,ep,cmd)
                result = mod.render_run_script(d,name+'.inspect.json',True)
                sp = Path(td)/'run.sh';sp.write_text(result)
                status = subprocess.run(['bash','-n',str(sp)], capture_output=True, text=True)
                self.assertEqual(status.returncode,0,status.stderr)
                if target is None:
                    self.assertNotIn('--entrypoint', result)
                else:
                    self.assertIn(target, result)
                if label=='cn':
                    self.assertRegex(result, r'(?m)^  20$')
                if label=='cdc':
                    self.assertIn('/home/admin/app.sh',result)
                self.assertIn('docker image inspect "$IMAGE"',result)
                self.assertIn('awk -v s=',result)

    def test_no_auto_delete_and_restore_all_syntax(self):
        contents = mod.render_restore_all([
            ('gms','pxc_single-gms-Cand-15402'),('dn','pxc_single-dn-0-Cand-16312'),
            ('cn','pxc_single-cn-EpcE-56037'),('cdc','pxc_single-cdc-mIMR')])
        self.assertNotIn('docker rm',contents)
        self.assertLess(contents.index('restore pxc_single-gms'),contents.index('restore pxc_single-dn-0'))
        with tempfile.NamedTemporaryFile(mode='w',suffix='.sh') as f:
            f.write(contents);f.flush()
            r=subprocess.run(['bash','-n',f.name],capture_output=True,text=True)
            self.assertEqual(r.returncode,0,r.stderr)

    def test_guards_preserve_existing_paths(self):
        with tempfile.TemporaryDirectory() as td:
            data=Path(td)/'data';data.mkdir();(data/'auto.cnf').write_text('server-uuid=test')
            d=fixture('pxc_single-dn-0-Cand-16312',[],['bash','-c','python /opt/entrypoint.py'])
            d['Mounts']=[{'Type':'bind','Source':str(data),'Destination':'/data/mysql','RW':True}]
            d['HostConfig']['Binds']=[str(data)+':/data/mysql']
            guard=mod.collect_guards(d)
            self.assertTrue(guard['binds'][str(data)]['nonempty'])
            result=mod.render_run_script(d,'a.inspect.json',True,None,guard)
            self.assertIn("stat -Lc '%d:%i'",result)
            self.assertIn('原非空挂载目录现在为空',result)
            s=Path(td)/'restore.sh';s.write_text(result)
            r=subprocess.run(['bash','-n',str(s)],capture_output=True,text=True)
            self.assertEqual(r.returncode,0,r.stderr)

    def test_argv_of_cn_cli_form(self):
        original=fixture('pxc_single-cn-EpcE-56037',['/home/admin/entrypoint.sh','20'],None)
        reconstructed=fixture('pxc_single-cn-EpcE-56037',['/home/admin/entrypoint.sh'],['20'])
        self.assertEqual(mod._view(original)['启动进程参数'],mod._view(reconstructed)['启动进程参数'])
        self.assertNotEqual(original['Config']['Entrypoint'],reconstructed['Config']['Entrypoint'])


# 使用 unittest discover 或直接运行本文件，均执行以下全部测试。
class OfflineCLITest(unittest.TestCase):
    def test_backup_delete_restore_verify_with_mock_docker(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            binary=root/'bin';binary.mkdir()
            state_path=root/'state.json'
            nodes=[
                fixture('pxc_single-gms-Cand-15402',[],['bash','-c','python /opt/entrypoint.py']),
                fixture('pxc_single-dn-0-Cand-16312',[],['bash','-c','python /opt/entrypoint.py']),
                fixture('pxc_single-cn-EpcE-56037',['/home/admin/entrypoint.sh','20'],None,'polardbx/polardbx-sql:latest'),
                fixture('pxc_single-cdc-mIMR',None,['/bin/sh','-c','/home/admin/app.sh'],'polardbx/polardbx-cdc:latest')]
            state_path.write_text(json.dumps({'containers':{d['Name'][1:]:d for d in nodes},'runs':[]}),encoding='utf-8')
            fake=binary/'docker'
            fake.write_text('''#!/usr/bin/env python3
import json,os,sys
p=os.environ['PXD_TEST_STATE']; st=json.load(open(p)); a=sys.argv[1:]
if a[:2]==['ps','-a']:
    if '--no-trunc' in a:
        print('\\n'.join(st['containers']));sys.exit(0)
    fmt=a[a.index('--format')+1] if '--format' in a else ''
    for n,d in st['containers'].items():
        print(n+('\\t'+d['Config']['Image'] if '.Image' in fmt else ''))
elif a[:2]==['image','inspect']:
    print(json.dumps([{'Config':{'Env':['PATH=/usr/bin'],'Labels':{}}}]))
elif a[0]=='inspect':
    name=a[-1]
    if name not in st['containers']: sys.exit(1)
    d=st['containers'][name]
    if '-f' in a:print('true' if d['State']['Running'] else 'false')
    else:print(json.dumps([d]))
elif a[0]=='run':
    n=a[a.index('--name')+1]
    orig={v['Name'][1:]:v for v in st.get('baseline',st['containers']).values()}
    # 如果旧容器已删除，备份数据保存于 BASELINE_FILE
    orig=json.load(open(os.environ['PXD_TEST_BASELINE']))
    d=orig[n]
    d['Id']='b'*64
    d['State']['Running']=True
    st['containers'][n]=d
    st['runs'].append(a)
    print('b'*64)
else:sys.exit(30)
json.dump(st,open(p,'w'))
''',encoding='utf-8')
            fake.chmod(0o755)
            baseline=root/'baseline.json';baseline.write_text(json.dumps({d['Name'][1:]:d for d in nodes}))
            env=dict(os.environ,PATH=str(binary)+os.pathsep+os.environ.get('PATH',''),
                     PXD_TEST_STATE=str(state_path),PXD_TEST_BASELINE=str(baseline))
            backup_dir=root/'backup'
            def cli(*args):
                return subprocess.run(['python3',str(P),*args],env=env,capture_output=True,text=True)
            r=cli('backup','--prefix','pxc_single','--dir',str(backup_dir))
            self.assertEqual(r.returncode,0,r.stderr)
            self.assertEqual(len(list(backup_dir.glob('*.run.sh'))),4)
            self.assertTrue((backup_dir/'mount-guards.json').is_file())
            saved=json.loads(state_path.read_text())
            del saved['containers']['pxc_single-dn-0-Cand-16312']
            state_path.write_text(json.dumps(saved))
            r=cli('restore','--dir',str(backup_dir),'--component','dn-0','--dry-run')
            self.assertEqual(r.returncode,0,r.stderr)
            self.assertIn('pxc_single-dn-0-Cand-16312',r.stdout)
            self.assertNotIn('pxc_single-dn-0-Cand-16312',json.loads(state_path.read_text())['containers'])
            r=cli('restore','--dir',str(backup_dir),'--component','dn-0')
            self.assertEqual(r.returncode,0,r.stderr)
            data=json.loads(state_path.read_text())
            self.assertEqual(len(data['runs']),1)
            self.assertIn('--entrypoint',data['runs'][0]);idx=data['runs'][0].index('--entrypoint')
            self.assertEqual(data['runs'][0][idx+1],'')
            r=cli('verify','pxc_single-dn-0','--dir',str(backup_dir))
            self.assertEqual(r.returncode,0,r.stderr+r.stdout)
            self.assertIn('配置一致',r.stdout)

if __name__=='__main__':
    unittest.main(verbosity=2)

# 附加回归：真实用户遇到的 :latest 与 sha256 Config.Image 比较、CDC 心跳变化。
class Regression_20261023(unittest.TestCase):
    def test_pinned_image_not_a_difference(self):
        import argparse, io, contextlib
        original=fixture('pxc_single-dn-0-Cand-16312',[],['bash','-c','python /opt/entrypoint.py'])
        restored=json.loads(json.dumps(original))
        restored['Id']='b'*64
        restored['Config']['Image']=original['Image']
        # 历史 Entrypoint [] 经 docker run --entrypoint '' 可能表现为 null，最终 argv 仍相同。
        restored['Config']['Entrypoint']=None
        restored['Path']='bash';restored['Args']=['-c','python /opt/entrypoint.py']
        original['Path']='bash';original['Args']=['-c','python /opt/entrypoint.py']
        with tempfile.TemporaryDirectory() as td:
            Path(td,'pxc_single-dn-0-Cand-16312.inspect.json').write_text(json.dumps([original]))
            def fake_docker(*a,**kw):
                from subprocess import CompletedProcess
                if a[0]=='ps': return CompletedProcess(a,0,'pxc_single-dn-0-Cand-16312\n','')
                if a[0]=='inspect':return CompletedProcess(a,0,json.dumps([restored]),'')
                raise RuntimeError(a)
            output=io.StringIO()
            with patch.object(mod,'run_docker',side_effect=fake_docker), contextlib.redirect_stdout(output):
                with self.assertRaises(SystemExit) as cm:
                    mod.cmd_verify(argparse.Namespace(dir=td,name='pxc_single-dn-0',ip_map=[]))
            self.assertEqual(cm.exception.code,0,output.getvalue())
            self.assertIn('实际镜像 ID 相同',output.getvalue())

    def test_real_image_change_fails(self):
        orig=fixture('pxc_single-dn-0-Cand-16312',[],['bash','-c','xyz'])
        newer=json.loads(json.dumps(orig)); newer['Image']='sha256:'+'f'*64
        self.assertNotEqual(mod._view(orig)['镜像 ID'],mod._view(newer)['镜像 ID'])

    def test_cdc_unchanged_container_append_is_ok(self):
        import importlib.util,io,contextlib
        sp=importlib.util.spec_from_file_location('check21',Path(__file__).with_name('pxd_health.py'))
        ck=importlib.util.module_from_spec(sp);sp.loader.exec_module(ck)
        before={'files':{'/home/admin/binlog.000001':{'inode':555,'size':100,'mtime':100}}}
        after={'files':{'/home/admin/binlog.000001':{'inode':555,'size':120,'mtime':200}}}
        rep=ck.Report();out=io.StringIO()
        with contextlib.redirect_stdout(out):ck.diff_cdc(rep,before,after,recreated=False)
        self.assertEqual(rep.n['warn'],0,out.getvalue())
        self.assertGreater(rep.n['ok'],0,out.getvalue())

    def test_cdc_file_replaced_warns(self):
        import importlib.util,io,contextlib
        sp=importlib.util.spec_from_file_location('check21',Path(__file__).with_name('pxd_health.py'))
        ck=importlib.util.module_from_spec(sp);sp.loader.exec_module(ck)
        before={'files':{'/home/admin/binlog.000001':{'inode':555,'size':100,'mtime':100}}}
        after={'files':{'/home/admin/binlog.000001':{'inode':666,'size':120,'mtime':200}}}
        rep=ck.Report();out=io.StringIO()
        with contextlib.redirect_stdout(out):ck.diff_cdc(rep,before,after,recreated=False)
        self.assertEqual(rep.n['warn'],1,out.getvalue())
