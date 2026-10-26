#!/usr/bin/env python3
# coding: utf-8
"""
PolarDB-X CDC 容器可写层恢复实验辅助工具 (v2.2)
依赖同目录的 pxd_recover_v2_1.py、Python 3.6+、Docker、bash。
仅面向已经停机冷备份且可丢失数据的实验环境。

严格手工分阶段：check -> [人工 docker rm 原 CDC] -> create -> restore-data -> start
工具不会执行 docker stop 或 docker rm，且不会在数据回填前启动新容器。
从原始 *.inspect.json 恢复完整运行参数，并复用 v2.1 的挂载源保护。
可写层恢复范围：/home/admin/{binlog,logs,env,bin}（按存在情况）；
避开原镜像程序文件和原容器的 daemon.pid，不保证其他目录状态恢复。
不能视为生产级 CDC 备份 / 全局 binlog 连续性保证。
"""
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import pxd_core as core
except ImportError:
    sys.exit('错误: 请将 pxd_cdc_recover_v2_2.py 与 pxd_recover_v2_1.py 放在同一目录')

STORED = ('binlog', 'logs', 'env', 'bin')
CRITICAL = ('binlog', 'logs/rocksdb')
BACKUP_INDEX = 'cdc-backup-checksums.json'


def fail(msg):
    raise SystemExit('ERROR: ' + msg)


def execute(argv, capture=False):
    try:
        return subprocess.run(argv, check=True, stdout=subprocess.PIPE if capture else None,
                              stderr=subprocess.PIPE if capture else None,
                              universal_newlines=True)
    except subprocess.CalledProcessError as e:
        err = (e.stderr or '').strip() if capture else '请检查上面的 docker 错误'
        fail('%s 失败: %s' % (argv[0:2], err))
    except FileNotFoundError:
        fail('未找到命令: %s' % argv[0])


def get_docker_names():
    p = execute(['docker', 'ps', '-a', '--format', '{{.Names}}'], capture=True)
    return p.stdout.splitlines()


def inspect_existing(name):
    p = execute(['docker', 'inspect', name], capture=True)
    data = json.loads(p.stdout)
    if len(data) != 1:
        fail('docker inspect 返回异常')
    return data[0]


def local_source(args):
    out = Path(args.dir).expanduser().resolve()
    manifest = core.load_manifest(str(out))
    prefix = args.prefix or manifest.get('prefix') or ''
    if not prefix:
        fail('必须提供 --prefix 集群前缀，或在 mount-guards.json 中保存 prefix')
    inspects = []
    for path in sorted(out.glob('*.inspect.json')):
        d = core.load_inspect(str(path))
        name = core.container_name(d)
        if core.stem_of(name) == prefix + '-cdc':
            inspects.append((path, d))
    if len(inspects) != 1:
        fail('inspect 备份中预期找到唯一 %s-cdc，实际找到 %d 个' % (prefix, len(inspects)))
    src_path, saved = inspects[0]
    name = core.container_name(saved)
    guard = (manifest.get('guards') or {}).get(name)
    if not guard:
        fail('缺少 %s 的 mount-guards.json 原始挂载保护记录；请先核查备份' % name)
    if not saved.get('Image'):
        fail('inspect.json 中缺少原始 Image ID')
    backup = Path(args.backup_dir).expanduser().resolve()
    admin = backup / 'admin'
    return out, backup, admin, src_path, saved, guard, prefix


def unique_current(prefix):
    matches = [n for n in get_docker_names() if core.stem_of(n) == prefix + '-cdc']
    if len(matches) > 1:
        fail('匹配到多个同逻辑 CDC 容器: %s' % ', '.join(matches))
    return matches[0] if matches else None


def secure_regular_tree(root):
    if root.is_symlink() or not root.is_dir():
        fail('备份目录不存在或为软链接: %s' % root)
    counts = {}
    for part in STORED:
        folder = root / part
        if not folder.exists():
            continue
        if not folder.is_dir() or folder.is_symlink():
            fail('异常的备份目录 %s' % folder)
        counts[part] = len([p for p in folder.rglob('*') if p.is_file()])
    if not any((root / 'binlog').glob('binlog.[0-9]*')):
        fail('没有找到历史全局 binlog（admin/binlog/binlog.NNNNNN）')
    rocks = root / 'logs' / 'rocksdb'
    if not rocks.is_dir() or not any(p.is_file() for p in rocks.rglob('*')):
        fail('没有找到非空的 admin/logs/rocksdb')
    return counts


def fingerprint(root):
    files = {}
    for part in STORED:
        d = root / part
        if not d.exists():
            continue
        for f in sorted(d.rglob('*')):
            rel = str(f.relative_to(root))
            if f.is_symlink():
                files[rel] = {'link': os.readlink(str(f))}
            elif f.is_file():
                h = hashlib.sha256()
                with f.open('rb') as src:
                    for block in iter(lambda: src.read(1024 * 1024), b''):
                        h.update(block)
                files[rel] = {'sha256': h.hexdigest(), 'size': f.stat().st_size}
    return files


def store_json(p, obj):
    fd = os.open(str(p), os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.chmod(str(p), 0o600)


def check_backup(backup, admin, write=False):
    counts = secure_regular_tree(admin)
    current = fingerprint(admin)
    idx = backup / BACKUP_INDEX
    if idx.exists():
        try:
            saved = json.loads(idx.read_text(encoding='utf-8'))
        except Exception as e:
            fail('备份校验文件损坏: %s' % e)
        if saved.get('files') != current:
            fail('备份校验 SHA256 不一致，拒绝继续；备份内容可能已改变')
    elif write:
        store_json(idx, {'version': 1, 'files': current})
    else:
        fail('缺少 %s：必须先执行 check 阶段' % idx)
    print('[ok] 备份校验通过: %d 文件；主要目录文件数=%s' % (len(current), counts))
    return current


def state_path(out, prefix):
    return out / ('.%s-cdc.restore-state.json' % prefix)


def cmd_check(args):
    out, backup, admin, path, saved, guard, prefix = local_source(args)
    secure_regular_tree(admin)
    current = unique_current(prefix)
    origin_verified = False
    if current:
        d = inspect_existing(current)
        if d['Id'] != saved.get('Id'):
            fail('当前 CDC ID 不等于原 inspect 记录；不要覆盖或删除未知容器')
        state = d.get('State', {})
        if state.get('Status') != 'exited':
            fail('当前 CDC 状态不是 exited: %s' % state.get('Status'))
        if state.get('OOMKilled') or state.get('ExitCode') == 137:
            fail('CDC 可能被 SIGKILL/OOM 杀死，不能把现有文件视为干净的冷备份')
        print('[ok] 原始容器已停止: %s (%s)，ExitCode=%s' %
              (current, d['Id'][:12], state.get('ExitCode')))
        verify_critical_copy(admin, current)
        origin_verified = True
    else:
        print('[提示] 原始 CDC 已不存在；只核验此前签封的校验清单')
        if not (backup / BACKUP_INDEX).is_file():
            fail('原 CDC 已删除，且备份缺少事先保存的 SHA256 校验清单；'
                 '不能在事故发生后生成新清单并宣称备份可信。请先人工确认数据来源')
    # 只有仍能从已停止的原容器独立核对文件时，才允许首次签封冷备份。
    check_backup(backup, admin, write=origin_verified)
    imgid = execute(['docker', 'image', 'inspect', '-f', '{{.Id}}', saved['Image']], capture=True).stdout.strip()
    if imgid != saved['Image']:
        fail('镜像实际 ID 不匹配')
    print('[ok] 原始镜像仍在本机: %s' % imgid)
    print('回填范围: %s' % ', '.join('/home/admin/' + d for d in STORED if (admin / d).exists()))
    print('下一步: 确认实验环境可中断且允许数据丢失后，人工删除已停止的原 CDC 容器。')
    print('工具自身绝不会执行 docker rm。')


def make_create_script(saved, src_path, guard):
    original = core.render_run_script(saved, src_path.name, pin_image=True, guards=guard)
    # 精确替换 v2.1 生成的一条 docker run -d；保留完整预检（源挂载 inode、容器去重、镜像）。
    needle = "\ndocker run \\\n  -d \\\n"
    if original.count(needle) != 1:
        fail('v2.1 生成的脚本不是预期 docker run -d 格式，拒绝自动转换')
    b = original.replace(needle, "\ndocker create \\\n", 1)
    b = b.replace('echo "已创建 $NAME"', 'echo "[created-only] $NAME (尚未启动)"')
    return b


def cmd_create(args):
    out, backup, admin, src_path, saved, guard, prefix = local_source(args)
    check_backup(backup, admin)
    current = unique_current(prefix)
    if current:
        fail('CDC 容器仍存在: %s。工具不会自动删除（停止状态也不可以）' % current)
    script = make_create_script(saved, src_path, guard)
    dest = out / (core.container_name(saved) + '.cdc.create.sh')
    fd = os.open(str(dest), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o700)
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        f.write(script)
    os.chmod(str(dest), 0o700)
    execute(['bash', '-n', str(dest)])
    if args.dry_run:
        print('[dry-run] 已安全生成 create-only 脚本: %s' % dest)
        print('还没有创建 Docker 容器，也没有恢复数据。')
        return
    execute(['bash', str(dest)])
    name = core.container_name(saved)
    d = inspect_existing(name)
    if d.get('State', {}).get('Status') != 'created':
        fail('容器创建后状态不是 created，请检查: %s' % d.get('State', {}).get('Status'))
    store_json(state_path(out, prefix), {'stage': 'created', 'name': name, 'id': d['Id'],
                                         'backup_dir': str(backup)})
    print('[ok] CDC 已创建但未启动: %s' % name)
    print('下一步: restore-data')


def read_state(out, prefix, expected, backup):
    p = state_path(out, prefix)
    if not p.exists():
        fail('恢复阶段记录不存在，请先执行 create')
    st = json.loads(p.read_text(encoding='utf-8'))
    if st.get('stage') not in expected:
        fail('恢复阶段不正确: %s (期望 %s)' % (st.get('stage'), expected))
    if st.get('backup_dir') != str(backup):
        fail('备份路径与 create 阶段不一致')
    curr = inspect_existing(st['name'])
    if curr['Id'] != st['id']:
        fail('目标容器 ID 改变，拒绝对未知容器操作')
    if curr.get('State', {}).get('Status') != 'created':
        fail('目标容器不是 created 状态（当前 %s），不能在运行中回填' % curr.get('State', {}).get('Status'))
    return st


def verify_critical_copy(admin, name):
    # 再复制一次已创建容器内的关键目录，用文件哈希逐项对比，避免目录层级放错或内容不一致。
    for component in CRITICAL:
        src = admin / component
        with tempfile.TemporaryDirectory(prefix='pxd-cdc-verify-') as tmp:
            dest = Path(tmp) / src.name
            execute(['docker', 'cp', name + ':/home/admin/' + component, str(dest)])
            fa = fingerprint_selected(src)
            fb = fingerprint_selected(dest)
            if fa != fb:
                fail('回填后文件校验不一致: /home/admin/%s' % component)
            print('[ok] 回填校验: /home/admin/%s (%d 文件)' % (component, len(fa)))


def fingerprint_selected(root):
    result = {}
    for f in sorted(root.rglob('*')):
        if f.is_file() and not f.is_symlink():
            h = hashlib.sha256()
            with f.open('rb') as src:
                for block in iter(lambda: src.read(1024 * 1024), b''):
                    h.update(block)
            result[str(f.relative_to(root))] = h.hexdigest()
    return result


def cmd_restore_data(args):
    out, backup, admin, src_path, saved, guard, prefix = local_source(args)
    check_backup(backup, admin)
    st = read_state(out, prefix, ('created',), backup)
    name = st['name']
    for component in STORED:
        source = admin / component
        if source.is_dir():
            # 目标 /home/admin 一定存在（原镜像），复制目录到其下避免多出 admin/admin 层级。
            execute(['docker', 'cp', '-a', str(source), name + ':/home/admin/'])
            print('[copied] /home/admin/%s' % component)
    verify_critical_copy(admin, name)
    st['stage'] = 'restored'
    store_json(state_path(out, prefix), st)
    print('[ok] 冷数据已回填。新容器尚未启动。下一步: start')


def cmd_start(args):
    out, backup, admin, src_path, saved, guard, prefix = local_source(args)
    check_backup(backup, admin)
    st = read_state(out, prefix, ('restored',), backup)
    if args.dry_run:
        print('[dry-run] 将启动已经回填数据的容器: %s' % st['name'])
        return
    execute(['docker', 'start', st['name']])
    st['stage'] = 'started'
    store_json(state_path(out, prefix), st)
    print('[started] %s；仍需执行原 v2.1 verify + SQL snapshot/diff 验收。' % st['name'])


def main():
    p = argparse.ArgumentParser(description='CDC 冷备份恢复分阶段工具，不自动删除容器')
    sub = p.add_subparsers(dest='action')
    sub.required = True
    for act in ('check', 'create', 'restore-data', 'start'):
        x = sub.add_parser(act)
        x.add_argument('--prefix', default='', help='集群名前缀，如 pxc_single（不可写随机容器后缀）')
        x.add_argument('--dir', default='~/pxd-recover', help='已有 v2.1 inspect 备份工作目录')
        x.add_argument('--backup-dir', default='~/pxd-recover/cdc-backup-01', help='包含 admin 目录的冷备份路径')
        if act in ('create', 'start'):
            x.add_argument('--dry-run', action='store_true')
    args = p.parse_args()
    os.umask(0o077)
    {'check': cmd_check, 'create': cmd_create, 'restore-data': cmd_restore_data,
     'start': cmd_start}[args.action](args)


if __name__ == '__main__':
    main()
