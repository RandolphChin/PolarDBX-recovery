#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PolarDB-X (PXD) 四组件统一容器恢复入口 v3.2。

仅 Python 标准库 + Docker + Bash；snapshot 额外依赖 mysql CLI。
底层采用经过 DN/CN/GMS 实测的 inspect 重放器，并为 CDC 提供冷备份与两阶段重建。
危险动作（stop/rm）始终由操作员显式执行；不自动删除任何容器或数据。
"""
import argparse
import datetime
import glob
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

import pxd_core as core
import pxd_health as health
import pxd_cdc as cdc
import pxd_images as images
import pxd_registry as registry

DEFAULT_DIR = '~/pxd-recover'
COMPONENTS = ('gms', 'dn', 'dn-0', 'cn', 'cdc', 'all')
BINLOG_RE = re.compile(r'^binlog\.[0-9]{6,}$')


def fail(message):
    raise SystemExit('错误: ' + message)


def base_args(parser, include_prefix=True):
    parser.add_argument('--dir', default=DEFAULT_DIR, help='原始 inspect 备份及快照目录')
    if include_prefix:
        parser.add_argument('--prefix', default='', help='集群逻辑前缀，例如 pxc_single')


def valid_saved(dir_path, prefix=''):
    out = Path(dir_path).expanduser()
    files = sorted(out.glob('*.inspect.json'))
    inspects = []
    for f in files:
        d = core.load_inspect(str(f))
        name = core.container_name(d)
        if not name or not core.stem_of(name):
            fail('备份中有未知容器名: ' + name)
        if prefix and not core.matches_cluster(name, prefix):
            continue
        inspects.append((f, d))
    if inspects:
        core.ensure_unique([core.container_name(d) for _, d in inspects])
    return inspects


def cluster_prefix(args):
    if args.prefix:
        return args.prefix
    p = core.load_manifest(os.path.expanduser(args.dir)).get('prefix')
    if p:
        return p
    fail('请指定 --prefix；或者先使用 backup --prefix 创建清单')


def inventory(args):
    prefix = cluster_prefix(args)
    names = core.docker_names(prefix)
    inspects = valid_saved(args.dir, prefix)
    backups = {core.stem_of(core.container_name(d)): d for _, d in inspects}
    print('集群:', prefix)
    print('%-22s %-34s %-11s %-14s %s' % ('逻辑身份', 'Docker 容器', '角色', '状态', '原始配置'))
    for n in sorted(names, key=lambda n: (core.ROLE_ORDER.index(core.detect_role(n, '')), n)):
        d = json.loads(core.run_docker('inspect', n).stdout)[0]
        stem = core.stem_of(n)
        role = core.detect_role(n, (d.get('Config') or {}).get('Image'))
        state = (d.get('State') or {}).get('Status')
        save = backups.get(stem)
        print('%-22s %-34s %-11s %-14s %s' % (stem, n, role, state,
              '有' if save else '无'))
    existing = {core.stem_of(n) for n in names}
    for stem, d in sorted(backups.items()):
        if stem not in existing:
            print('%-22s %-34s %-11s %-14s 有' % (
                stem, '(已删除)', core.detect_role(core.container_name(d), ''), 'missing'))


def resolve(args):
    """唯一解析运行或已停止的组件实际 Docker 名称；仅输出名字，适合 shell 赋值。"""
    prefix = cluster_prefix(args)
    if not re.fullmatch(r'(?:gms|cn|cdc|dn-[0-9]+)', args.component):
        fail('resolve 的组件必须是 gms/cn/cdc/dn-N，请明确 DN 序号')
    stem = prefix + '-' + args.component
    names = [n for n in core.docker_names(prefix) if core.stem_of(n) == stem]
    if len(names) != 1:
        fail('当前 %s 对应 %d 个容器（要求恰好一个）：%s' % (stem,len(names),','.join(names)))
    print(names[0])


def backup(args):
    if not args.prefix:
        fail('backup 必须指定 --prefix，避免误采集其他集群')
    names = core.docker_names(args.prefix)
    if not names:
        fail('当前 docker ps -a 找不到该集群容器')
    roles = {core.detect_role(n, '') for n in names}
    if not {'gms', 'dn', 'cn', 'cdc'}.issubset(roles):
        fail('四组件完整备份要求 GMS/DN/CN/CDC 都在 docker ps -a；当前缺少 %s' %
             ','.join(sorted({'gms','dn','cn','cdc'} - roles)))
    old = {core.stem_of(core.container_name(d)): d for _, d in valid_saved(args.dir, args.prefix)}
    for n in names:
        now = json.loads(core.run_docker('inspect', n).stdout)[0]
        previous = old.get(core.stem_of(n))
        if previous and previous.get('Id') != now.get('Id') and not args.refresh:
            fail('已有 %s 的旧容器 inspect (ID %s)，当前是新 ID %s；为保护历史备份，需先归档旧工作目录，或显式使用 --refresh' %
                 (core.stem_of(n), (previous.get('Id') or '')[:12], (now.get('Id') or '')[:12]))
    opts = argparse.Namespace(dir=args.dir, prefix=args.prefix, ip_map=[], pin_image=True)
    core.cmd_backup(opts)
    # v2.1 核心工具会生成每个容器的 docker run 脚本和 restore-all.sh。
    # 对 CDC/所有组件一键恢复加执行级别封锁，防止尚未回填时提前启动 CDC。
    out = Path(args.dir).expanduser()
    for n in names:
        if core.stem_of(n) == args.prefix + '-cdc':
            core.write_file(str(out / (n + '.run.sh')),
                '#!/usr/bin/env bash\nset -euo pipefail\n'
                'echo "ERROR: CDC 不允许直接 docker run；请使用 pxd_recovery.py cdc-create / cdc-restore-data / cdc-start。" >&2\n'
                'exit 2\n', 0o700)
    core.write_file(str(out / 'restore-all.sh'),
        '#!/usr/bin/env bash\nset -euo pipefail\n'
        'echo "ERROR: 四组件不得使用旧 restore-all.sh；GMS/DN/CN 使用 recover，CDC 使用冷备份分阶段流程。" >&2\n'
        'exit 2\n', 0o700)
    print('\n[安全] 已禁用自动生成的 CDC *.run.sh 和 restore-all.sh，避免跳过 CDC 数据回填。')
    print('[提示] CDC 还须优雅停止后执行 cdc-backup；inspect.json 不能备份可写层。')



def image_archives(args):
    prefix = cluster_prefix(args)
    entries = valid_saved(args.dir, prefix)
    if not entries:
        fail('没有此集群的原始 inspect，无法确认需要保存哪些 Image ID')
    role_set = {core.detect_role(core.container_name(d), '') for _, d in entries}
    if not {'gms', 'dn', 'cn', 'cdc'}.issubset(role_set):
        fail('镜像归档要求完整四组件 inspect；缺少 %s' %
             ','.join(sorted({'gms','dn','cn','cdc'} - role_set)))
    dest = args.image_dir or str(Path(args.dir).expanduser() / 'image-archives')
    if args.action == 'export-images':
        images.export_images(entries, dest)
    else:
        images.verify_images(entries, dest)


def plan(args):
    prefix = cluster_prefix(args)
    entries = valid_saved(args.dir, prefix)
    if not entries:
        fail('没有原始 inspect 备份')
    man = core.load_manifest(os.path.expanduser(args.dir))
    print('恢复计划（只读） 集群=%s' % prefix)
    for f, d in sorted(entries, key=lambda e: (core.ROLE_ORDER.index(core.detect_role(core.container_name(e[1]), '')), core.container_name(e[1]))):
        name = core.container_name(d)
        role = core.detect_role(name, '')
        stem = core.stem_of(name)
        img = (d.get('Image') or '')[:20]
        saved_guards = (man.get('guards') or {}).get(name) or {}
        print('\n%s  %s  image=%s' % (role.upper(), stem, img))
        print('  原名: %s' % name)
        ep, cmd = (d.get('Config') or {}).get('Entrypoint'), (d.get('Config') or {}).get('Cmd')
        print('  Entrypoint: %s; Cmd 参数个数: %d（不展示可能含密码的命令内容）' %
              ('null' if ep is None else '[]' if not ep else '自定义(%d)' % len(ep), len(cmd or [])))
        print('  Mount guards: bind=%d volumes=%d' %
              (len(saved_guards.get('binds') or {}), len(saved_guards.get('volumes') or [])))
        if role == 'cdc':
            print('  ⚠ CDC 必须先冷备份；不能直接用 run.sh / restore-all.sh 代替回填状态')
    print('\n注意：IP/端口/原容器全名均来自每次部署的 inspect，不使用固定值。')



def refresh_image_scripts(args):
    """从原始 inspect 重建 GMS/DN/CN 的恢复脚本，不触及 CDC 恢复保护。"""
    prefix = cluster_prefix(args)
    out = Path(args.dir).expanduser()
    entries = valid_saved(args.dir, prefix)
    roles = {core.detect_role(core.container_name(d), '') for _, d in entries}
    if not {'gms', 'dn', 'cn', 'cdc'}.issubset(roles):
        fail('原始 inspect 未包含完整四组件，拒绝刷新恢复脚本')
    man = core.load_manifest(str(out))
    if man.get('prefix') != prefix:
        fail('mount-guards.json 的集群名与命令参数不同')
    guards = man.get('guards') or {}
    # 不可通过离线脚本刷新操作意外重新启用 CDC 一步启动或 restore-all。
    cdc = next(core.container_name(d) for _, d in entries
               if core.detect_role(core.container_name(d), '') == 'cdc')
    cdc_run = out / (cdc + '.run.sh')
    restore_all = out / 'restore-all.sh'
    if (not cdc_run.is_file() or 'CDC 不允许直接 docker run' not in cdc_run.read_text(encoding='utf-8')
            or not restore_all.is_file()
            or '四组件不得使用旧 restore-all.sh' not in restore_all.read_text(encoding='utf-8')):
        fail('当前备份的 CDC/restore-all 安全封锁不完整，拒绝刷新')
    plans = []
    for src, d in entries:
        name = core.container_name(d)
        if core.detect_role(name, '') == 'cdc':
            continue
        if name not in guards:
            fail('缺少原始 mount guard: ' + name)
        script_file = out / (name + '.run.sh')
        if not script_file.is_file():
            fail('找不到原始恢复脚本: ' + str(script_file))
        # 当前镜像若不可检查，构建 argv/环境变量覆盖可能不完整，拒绝刷新。
        image_id = d.get('Image')
        if not image_id or core.image_config(image_id) is None:
            fail('无法读取备份 Image ID 的镜像配置，拒绝刷新: ' + name)
        body = core.render_run_script(d, src.name, pin_image=True, guards=guards[name])
        result = subprocess.run(['bash', '-n'], input=body, capture_output=True, text=True)
        if result.returncode:
            fail('生成的恢复脚本语法校验未通过: ' + name)
        plans.append((script_file, body))
    print('原始 inspect、mount-guards、CDC 冷备份和 SQLite 均不会修改。')
    for path, body in plans:
        print(('[一致] ' if path.read_text(encoding='utf-8') == body else
               '[将刷新] ' if not args.apply else '[刷新] ') + path.name)
    if not args.apply:
        print('[只读] 如确认需要刷新，请增加 --apply --confirm ' + prefix)
        return
    if args.confirm != prefix:
        fail('--confirm 必须与集群名前缀一致，拒绝写入脚本')
    for path, body in plans:
        backup = core.save_with_rotation(str(path), body, 0o700)
        if backup:
            print('[备份旧脚本] ' + backup)
    print('[ok] 已刷新 GMS/DN/CN 备用恢复脚本；CDC 与 restore-all 安全封锁保持不变。')


def recover(args):
    if args.component == 'cdc':
        fail('CDC 必须使用 cdc-check -> cdc-create -> cdc-restore-data -> cdc-start；禁止直接 run 恢复')
    if args.component == 'all':
        fail('为避免直接重建无状态备份的 CDC，禁止 all 自动一键恢复。请分别恢复 GMS/DN/CN，再使用 CDC 分阶段流程。')
    if args.component not in COMPONENTS and not re.fullmatch(r'dn-[0-9]+', args.component):
        fail('未知组件: ' + args.component)
    opts = argparse.Namespace(dir=args.dir, prefix=cluster_prefix(args), component=args.component,
                              dry_run=args.dry_run)
    core.cmd_restore(opts)
    print("[提示] 容器 ID 可能变化，请执行 pxd-check / pxd-sync 与 PXD 0.7.4 管理库同步后再使用 PXD 管理命令。")


def verify(args):
    opts = argparse.Namespace(dir=args.dir, name=args.name, ip_map=[])
    core.cmd_verify(opts)
    if core.stem_of(args.name) and core.stem_of(args.name).endswith('-cdc'):
        print('[提示] CDC 的可写层与历史日志还须运行 cdc-verify 才能判断。')


def health_snapshot(args):
    opts = argparse.Namespace(dir=args.dir, prefix=cluster_prefix(args), label=args.label,
                              host=args.host, cn_port=args.cn_port, user=args.user,
                              mysql_bin=args.mysql_bin, count=args.count,
                              no_marker=args.no_marker, no_sql=args.no_sql, wait=args.wait)
    health.cmd_snapshot(opts)


def cdc_args(args):
    return argparse.Namespace(dir=args.dir, prefix=cluster_prefix(args),
                              backup_dir=args.backup_dir, dry_run=getattr(args, 'dry_run', False))


def cdc_cold_backup(args):
    ca = cdc_args(args)
    out, target, admin, src, saved, guard, prefix = cdc.local_source(ca)
    current = cdc.unique_current(prefix)
    if not current:
        fail('找不到原始 CDC 容器。冷备份必须在删除前执行')
    d = cdc.inspect_existing(current)
    if d.get('Id') != saved.get('Id'):
        fail('备份的 inspect 对应的 CDC ID 与当前不一致；先人工确认备份，不允许误取其他实例')
    st = d.get('State') or {}
    if st.get('Status') != 'exited' or st.get('OOMKilled') or st.get('ExitCode') == 137:
        fail('CDC 必须先优雅停止并变为 exited，且不得是 OOM/137 退出；请检查 docker inspect State')
    if target.exists():
        fail('目标冷备份目录已存在，不允许覆盖: %s' % target)
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='.pxd-cdc-staging-', dir=str(target.parent)))
    try:
        subprocess.run(['docker', 'cp', '-a', current + ':/home/admin', str(work / 'admin')], check=True)
        cdc.secure_regular_tree(work / 'admin')
        cdc.verify_critical_copy(work / 'admin', current)
        cdc.check_backup(work, work / 'admin', write=True)
        seal = {
            'version': 3, 'original_container_id': d['Id'], 'original_name': current,
            'image_id': d.get('Image'), 'created_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'note': 'Docker stopped copy /home/admin; known CDC state outside this tree is NOT covered.',
        }
        cdc.store_json(work / 'cold-backup-metadata.json', seal)
        work.rename(target)
    except Exception:
        print('[ERROR] 冷备份未完成，暂存目录仍在: %s' % work, file=sys.stderr)
        raise
    print('[ok] CDC 冷备份完成: %s' % target)
    print('请先运行 cdc-check，并独立确认 /home/admin 之外的有状态文件，然后才可以人工 docker rm 原 CDC。')


def cdc_verify(args, quiet=False):
    ca = cdc_args(args)
    out, target, admin, src, saved, guard, prefix = cdc.local_source(ca)
    cdc.check_backup(target, admin)
    name = cdc.unique_current(prefix)
    if not name:
        fail('当前找不到 CDC 容器')
    d = cdc.inspect_existing(name)
    if not (d.get('State') or {}).get('Running'):
        fail('CDC 容器未运行')
    stpath = cdc.state_path(out, prefix)
    if not stpath.is_file():
        fail('缺少 create/restore-data/start 阶段回执: %s' % stpath)
    state = json.loads(stpath.read_text(encoding='utf-8'))
    if state.get('stage') != 'started' or state.get('id') != d.get('Id') or state.get('backup_dir') != str(target):
        fail('CDC 阶段回执与当前容器 ID/备份路径不匹配')
    old_files = sorted(p for p in (admin / 'binlog').iterdir() if p.is_file() and BINLOG_RE.fullmatch(p.name))
    if not old_files:
        fail('备份中没有标准 binlog.NNNNNN 文件')
    for old in old_files:
        size = old.stat().st_size
        expected = cdc.fingerprint_selected(old.parent).get(old.name)
        if not expected:
            fail('备份历史文件校验项缺失 ' + old.name)
        # docker exec 只传 argv，不经过 shell；文件内容流式散列，不把 binlog 装入内存。
        cmd = ['docker', 'exec', name, 'head', '-c', str(size), '/home/admin/binlog/' + old.name]
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
            digest = hashlib.sha256()
            count = 0
            while True:
                block = p.stdout.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
                count += len(block)
            error = p.stderr.read().decode('utf-8', 'replace')
            rc = p.wait()
        if rc or count != size or digest.hexdigest() != expected:
            fail('%s 历史前缀比对失败：原 %s bytes，新读取 %s bytes，原因 %s' %
                 (old.name, size, count, error.strip()))
        if not quiet:
            print('[ok] CDC 历史 binlog 前缀 SHA256 一致: %s (%s bytes)' % (old.name, size))
    if not quiet:
        print('[ok] 恢复阶段回执与当前 CDC ID 相符：%s' % d['Id'][:12])
        print('[注意] 以上证明本地历史文件前缀内容；RocksDB 当前可变，仍需检查日志及下游消费连续性。')
    return d['Id']


def diff(args):
    before_file = Path(args.dir).expanduser() / 'snapshots' / (args.before + '.json')
    after_file = Path(args.dir).expanduser() / 'snapshots' / (args.after + '.json')
    for p in (before_file, after_file):
        if not p.is_file():
            fail('找不到快照: %s' % p)
    cdc_proof_id = None
    if args.cdc_backup_dir:
        cargs = argparse.Namespace(dir=args.dir, prefix=args.prefix, backup_dir=args.cdc_backup_dir)
        try:
            cdc_proof_id = cdc_verify(cargs, quiet=True)
        except SystemExit as e:
            print('[注意] CDC 历史前缀校验未通过，将保留原差异警告: %s' % e, file=sys.stderr)
        if cdc_proof_id:
            after = json.loads(after_file.read_text(encoding='utf-8'))
            ids = [info.get('id') for name, info in (after.get('containers') or {}).items()
                   if core.stem_of(name) and core.stem_of(name).endswith('-cdc')]
            if ids != [cdc_proof_id]:
                print('[注意] 当前 CDC ID 与 after 快照的 CDC ID 不同，不把文件校验结果用于旧快照对比')
                cdc_proof_id = None
    opts = argparse.Namespace(dir=args.dir, before=args.before, after=args.after,
                              cdc_verified=bool(cdc_proof_id))
    if cdc_proof_id:
        print('[独立证据] 当前 CDC 容器与 after 快照 ID 一致，冷备份历史 binlog 前缀 SHA256 一致；inode 变化允许。')
    health.cmd_diff(opts)


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description='PXD 四组件统一恢复/校验入口 v3.2：不自动删除容器')
    sub = p.add_subparsers(dest='action', required=True)
    x = sub.add_parser('inventory', help='动态列出当前容器和已备份的逻辑节点')
    base_args(x)
    x.set_defaults(func=inventory)
    x = sub.add_parser('resolve', help='动态解析唯一逻辑组件的完整 Docker 容器名（供手工 stop/rm）')
    base_args(x)
    x.add_argument('--component', required=True, help='gms/cn/cdc/dn-0/dn-1 等')
    x.set_defaults(func=resolve)
    x = sub.add_parser('backup', help='保存四组件原始 inspect、固定镜像、挂载保护、run.sh')
    base_args(x)
    x.add_argument('--refresh', action='store_true', help='允许替换已经重建过的逻辑节点备份；慎用')
    x.set_defaults(func=backup)
    for act, detail in (
        ('export-images', '导出 inspect 引用的固定 Image ID，保存为离线 Docker 镜像归档'),
        ('verify-images', '离线核验镜像 tar 与 SHA256；无须运行 Docker'),
    ):
        x = sub.add_parser(act, help=detail)
        base_args(x)
        x.add_argument('--image-dir', default='', help='镜像归档保存位置（默认 <dir>/image-archives）')
        x.set_defaults(func=image_archives)
    x = sub.add_parser('plan', help='检查原始参数及重建计划（无敏感环境变量输出）')
    base_args(x)
    x.set_defaults(func=plan)
    x = sub.add_parser('refresh-image-scripts', help='从原始 inspect 安全刷新 GMS/DN/CN 恢复脚本中的镜像标签选择；不重建容器')
    base_args(x)
    x.add_argument('--apply', action='store_true', help='写入脚本；默认只读预览')
    x.add_argument('--confirm', default='', help='写入时必须等于集群名')
    x.set_defaults(func=refresh_image_scripts)
    x = sub.add_parser('recover', help='恢复缺失的 GMS/DN/CN；CDC 必须走冷备份分阶段流程')
    base_args(x)
    x.add_argument('--component', required=True, help='gms/dn-0/cn/dn 等；禁止 cdc/all 快捷模式')
    x.add_argument('--dry-run', action='store_true')
    x.set_defaults(func=recover)
    x = sub.add_parser('verify', help='检查恢复后容器的镜像 ID、运行参数、挂载及网络')
    base_args(x, include_prefix=False)
    x.add_argument('name', help='如 pxc_single-dn-0 或 pxc_single-cdc')
    x.set_defaults(func=verify)
    x = sub.add_parser('snapshot', help='前后快照：容器状态、存储、binlog、SQL 标记行')
    base_args(x)
    x.add_argument('label')
    x.add_argument('--host', help='CN 的 IP，不自动猜测')
    x.add_argument('--cn-port', type=int)
    x.add_argument('--user', default='polardbx_root')
    x.add_argument('--mysql-bin', default='mysql')
    x.add_argument('--wait', type=int, default=0)
    x.add_argument('--no-marker', action='store_true')
    x.add_argument('--no-sql', action='store_true')
    x.add_argument('--count', action='append', default=[])
    x.set_defaults(func=health_snapshot)
    x = sub.add_parser('diff', help='对比快照，可通过 CDC 冷备份前缀校验替代 inode 假警告')
    base_args(x)
    x.add_argument('before')
    x.add_argument('after')
    x.add_argument('--cdc-backup-dir', default='', help='提供 CDC 冷备份路径，额外验证历史文件 SHA256')
    x.set_defaults(func=diff)
    for action, func in [
        ('cdc-backup', cdc_cold_backup), ('cdc-check', lambda a: cdc.cmd_check(cdc_args(a))),
        ('cdc-create', lambda a: cdc.cmd_create(cdc_args(a))),
        ('cdc-restore-data', lambda a: cdc.cmd_restore_data(cdc_args(a))),
        ('cdc-start', lambda a: cdc.cmd_start(cdc_args(a))),
        ('cdc-verify', cdc_verify),
    ]:
        x = sub.add_parser(action)
        base_args(x)
        x.add_argument('--backup-dir', required=True, help='CDC 停机后冷备份目录（含 admin/）')
        if action in ('cdc-create', 'cdc-start'):
            x.add_argument('--dry-run', action='store_true')
        x.set_defaults(func=func)
    for action in ('pxd-check', 'pxd-sync'):
        x = sub.add_parser(action, help='检查/同步 PXD 0.7.4 管理数据库中的旧 Docker 容器 ID')
        x.add_argument('--prefix', required=True, help='集群名，如 pxc_single')
        x.add_argument('--db', default='~/.pxd/polardbx.db', help='PXD SQLite 管理数据库')
        x.add_argument('--pxd-host', required=True, help='PXD container.host 所记录的 Docker 宿主机，如 10.35.18.43')
        x.add_argument('--inspect-dir', default='', help='可选：事前完整 *.inspect.json 目录，执行镜像/旧 ID 双重校验')
        x.add_argument('--backup-dir', default='', help='写入前 SQLite 备份保存目录')
        x.add_argument('--allow-ip-mismatch', action='store_true', help='仅用于只读查看 IP 差异；存在差异时禁止同步')
        if action == 'pxd-sync':
            x.add_argument('--apply', action='store_true', help='明确执行事务更新；默认只读')
            x.add_argument('--confirm', default='', help='二次确认必须等于集群名')
        else:
            x.set_defaults(apply=False, confirm='')
        x.set_defaults(func=registry.run)
    args = p.parse_args()
    try:
        args.func(args)
    except RuntimeError as e:
        fail(str(e))
    except KeyboardInterrupt:
        fail('用户中断。若在执行阶段，请检查容器与阶段回执后再继续')


if __name__ == '__main__':
    main()
