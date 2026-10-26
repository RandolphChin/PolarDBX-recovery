#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PXD 0.7.4 container_id reconciliation, deliberately separate from container recovery.

Never deletes/stops/restarts Docker containers, edits bind mounts or mutates XDB/cluster
records. Default action is read-only. Sync is explicit and requires a consistent
4-role snapshot on the *same* Docker engine that PXD manages.
"""
import datetime
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys

ROLES = ('gms', 'dn-0', 'cn', 'cdc')
REQUIRED_TABLES = ('container', 'polardbx_cluster', 'polardbx_xdb')
REQUIRED_COLS = {
    'container': {'id', 'container_name', 'container_id', 'host', 'resource_name', 'container_ip'},
    'polardbx_cluster': {'pxc_name'},
    'polardbx_xdb': {'xdb_name', 'pxc_name'},
}


def error(msg):
    raise RuntimeError(msg)


def qid(name):
    return '"' + name.replace('"', '""') + '"'


def role_of(name, prefix):
    if not name.startswith(prefix + '-'):
        return None
    for role in ROLES:
        if re.fullmatch(re.escape(prefix + '-' + role) + r'-.+', name):
            return role
    return None


def ensure_db(conn, prefix):
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not set(REQUIRED_TABLES).issubset(tables):
        error('PXD SQLite 缺少预期表，拒绝在未知版本上运行: %s' % sorted(set(REQUIRED_TABLES) - tables))
    for t, required in REQUIRED_COLS.items():
        cols = {r[1] for r in conn.execute('PRAGMA table_info(' + qid(t) + ')')}
        if not required.issubset(cols):
            error('表 %s 字段结构不同，缺少: %s' % (t, sorted(required - cols)))
    n = conn.execute('SELECT COUNT(*) FROM polardbx_cluster WHERE pxc_name=?', (prefix,)).fetchone()[0]
    if n != 1:
        error('集群 %s 在 polardbx_cluster 表中有 %s 条记录（预期 1）；请先检查 pxd list' % (prefix, n))


def inspect_name(name):
    p = subprocess.run(['docker', 'inspect', '--type', 'container', name],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        error('Docker 没找到容器 %s: %s' % (name, p.stderr.strip()))
    try:
        info = json.loads(p.stdout)
    except ValueError:
        error('docker inspect 输出不是有效 JSON: ' + name)
    if not isinstance(info, list) or len(info) != 1:
        error('docker inspect 返回结果不唯一: ' + name)
    return info[0]


def docker_old_id_exists(old):
    p = subprocess.run(['docker', 'inspect', '--type', 'container', old],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        return False
    try:
        data = json.loads(p.stdout)
    except ValueError:
        error('Docker 旧 ID 检测返回异常 JSON')
    return bool(data)


def load_baselines(inspect_dir, prefix):
    if not inspect_dir:
        return None
    path = Path(inspect_dir).expanduser().resolve()
    if not path.is_dir():
        error('指定的 inspect 目录不存在: %s' % path)
    docs = {}
    for file in sorted(path.glob('*.inspect.json')):
        try:
            d = json.loads(file.read_text(encoding='utf-8'))
        except (ValueError, OSError) as e:
            error('无效 inspect 文件 %s: %s' % (file, e))
        if isinstance(d, list) and len(d) == 1:
            d = d[0]
        name = (d.get('Name') or '').lstrip('/')
        if name.startswith(prefix + '-'):
            if name in docs:
                error('基线 inspect 中重复容器名: ' + name)
            docs[name] = d
    if set(role_of(name, prefix) for name in docs) != set(ROLES) or len(docs) != len(ROLES):
        error('基线 inspect 必须恰好包含当前集群 GMS/DN-0/CN/CDC 四份配置')
    return docs


def candidate_ips(d):
    out = set()
    settings = d.get('NetworkSettings') or {}
    for s in [settings] + list((settings.get('Networks') or {}).values()):
        ip = s.get('IPAddress') if isinstance(s, dict) else None
        if ip:
            out.add(ip)
    return out


def compute_plan(conn, prefix, host, inspect_dir=None, allow_ip_mismatch=False):
    ensure_db(conn, prefix)
    xdbs = {r[0] for r in conn.execute('SELECT xdb_name FROM polardbx_xdb WHERE pxc_name=?', (prefix,))}
    allrows = conn.execute('''SELECT id, container_name, container_id, host,
                                     resource_name, container_ip FROM container''').fetchall()
    rows = [r for r in allrows if r['container_name'].startswith(prefix + '-')]
    if len(rows) != 4:
        error('集群名前缀匹配到 %d 条 container 记录，不是预期的四组件单 DN 集群' % len(rows))
    names = [r['container_name'] for r in rows]
    if len(set(names)) != 4:
        error('PXD container 表里有重复的 container_name')
    roles = [role_of(name, prefix) for name in names]
    if sorted(roles) != sorted(ROLES):
        error('角色不匹配：应为 GMS、DN-0、CN、CDC 各一；实际 %s' % roles)
    if any(r['host'] != host for r in rows):
        error('数据库 host 与指定 --pxd-host 不同，或存在多机；当前仅支持单 Docker Host')
    if any(r['resource_name'] not in {prefix} | xdbs for r in rows):
        bad = [(r['container_name'], r['resource_name']) for r in rows if r['resource_name'] not in {prefix} | xdbs]
        error('container.resource_name 不能对应集群或所属 XDB，拒绝同步: %s' % bad)
    baselines = load_baselines(inspect_dir, prefix)
    changes = []
    for r in sorted(rows, key=lambda r: ROLES.index(role_of(r['container_name'], prefix))):
        name, old_id = r['container_name'], r['container_id']
        if not isinstance(old_id, str) or not re.fullmatch(r'(?:[0-9a-f]{10}|[0-9a-f]{12}|[0-9a-f]{64})', old_id):
            error('PXD container_id 格式异常，拒绝修改: ' + name)
        d = inspect_name(name)
        full = d.get('Id') or ''
        if (d.get('Name') or '').lstrip('/') != name or not re.fullmatch('[0-9a-f]{64}', full):
            error('Docker 容器名/ID 与 PXD 不匹配: ' + name)
        if not (d.get('State') or {}).get('Running'):
            error('新容器未处于运行状态，拒绝登记: ' + name)
        if baselines is not None:
            before = baselines.get(name)
            if not before or not (
                    isinstance(before.get('Id'), str)
                    and re.fullmatch(r'[0-9a-f]{64}', before['Id'])
                    and (before['Id'].startswith(old_id) or full.startswith(old_id))
            ):
                error('原始 inspect 容器 ID 与 PXD 登记/当前 Docker ID 均不一致: ' + name)
            if before.get('Image') != d.get('Image'):
                error('镜像 Image ID 与原始 inspect 不一致: ' + name)
        # PXD 0.7.4 stores Docker's short_id as 10 hex chars.
        # Preserve the registered ID length (10, 12, or 64) across recovery.
        new_id = full[:len(old_id)]
        if old_id != new_id:
            if docker_old_id_exists(old_id):
                error('旧 ID 在 Docker 上仍存在，拒绝把另一容器冒充为已删除实例: %s' % name)
        ips = candidate_ips(d)
        db_ip = r['container_ip'] or ''
        mismatch = bool(db_ip and db_ip != host and ips and db_ip not in ips)
        if mismatch and not allow_ip_mismatch:
            error('%s 的 PXD container_ip=%s 与当前 Docker IP=%s 不同；此工具不自动修改 IP。请检查网络后再决定。' %
                  (name, db_ip, sorted(ips)))
        changes.append({
            'rowid': r['id'], 'name': name, 'role': role_of(name, prefix),
            'host': host, 'resource_name': r['resource_name'],
            'old_id': old_id, 'new_id': new_id, 'current_full_id': full,
            'db_ip': db_ip, 'docker_ips': sorted(ips), 'ip_warning': mismatch,
            'changed': old_id != new_id,
        })
    return changes


def show(changes):
    for c in changes:
        kind = '更新' if c['changed'] else '一致'
        print('[%-2s] %-5s %-34s PXD=%s Docker=%s host=%s' %
              (kind, c['role'], c['name'], c['old_id'], c['new_id'], c['host']))
        if c['ip_warning']:
            print('  [警告] PXD IP=%s，Docker IP=%s；本工具不会修改 container_ip' %
                  (c['db_ip'], ','.join(c['docker_ips'])))
    print('合计：%s 条符合修改条件，%s 条已经一致。' %
          (sum(c['changed'] for c in changes), sum(not c['changed'] for c in changes)))


def backup_sqlite(src, backup_dir):
    dest_dir = Path(backup_dir).expanduser().resolve()
    dest_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(dest_dir, 0o700)
    stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')
    dest = dest_dir / ('polardbx-before-id-sync-' + stamp + '.db')
    with sqlite3.connect(src.as_uri() + '?mode=ro', uri=True) as ro:
        with sqlite3.connect(dest) as out:
            ro.backup(out)
    os.chmod(dest, 0o600)
    return dest


def run(args):
    prefix = args.prefix
    if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_-]*', prefix or ''):
        error('无效的集群前缀')
    host = args.pxd_host
    if not host:
        error('必须使用 --pxd-host 明确指定 PXD 注册的 Docker 宿主机')
    src = Path(args.db).expanduser().resolve()
    if not src.is_file():
        error('PXD SQLite 文件不存在: ' + str(src))
    if args.apply and os.geteuid() == 0:
        error('不允许以 root 身份修改 PXD 用户数据库；请使用原 PXD 用户运行')
    if args.apply and not args.confirm == prefix:
        error('写入需要同时指定 --apply --confirm %s' % prefix)
    if args.apply and args.action != 'pxd-sync':
        error('仅 pxd-sync 支持 --apply')
    connstr = src.as_uri() + '?mode=ro'
    with sqlite3.connect(connstr, uri=True) as db:
        db.row_factory = sqlite3.Row
        plan = compute_plan(db, prefix, host, args.inspect_dir, args.allow_ip_mismatch)
    if args.apply and any(c['ip_warning'] for c in plan):
        error('检测到容器 IP 变化。pxd-sync 仅处理 ID；请先确认 PXD IP 记录，不能带 IP 差异写入。')
    print('PXD 容器 ID 核对（当前 Docker context 必须是 PXD 实际管理的宿主机）')
    show(plan)
    if not args.apply:
        print('[只读] 未修改 PXD SQLite；若准备更新，先确认映射并添加 --apply --confirm %s' % prefix)
        return
    change_list = [x for x in plan if x['changed']]
    if not change_list:
        print('[ok] ID 已完全一致，无需写入。')
        return
    dest = backup_sqlite(src, args.backup_dir or src.parent / 'pxd-registry-backups')
    print('[备份] PXD SQLite 已保存: ' + str(dest))
    # BEGIN IMMEDIATE serializes SQLite writers. CAS protects against DB changes
    # since the initial read; Docker identity is rechecked before the transaction.
    for c in change_list:
        d = inspect_name(c['name'])
        if d.get('Id') != c['current_full_id'] or not (d.get('State') or {}).get('Running'):
            error('Docker 容器在备份期间发生变化，拒绝修改: ' + c['name'])
    with sqlite3.connect(src) as db:
        db.isolation_level = None
        db.execute('PRAGMA busy_timeout=5000')
        db.execute('BEGIN IMMEDIATE')
        try:
            ensure_db(db, prefix)
            for c in change_list:
                cursor = db.execute('''UPDATE container SET container_id=?,
                                     gmt_modified=CURRENT_TIMESTAMP
                                     WHERE id=? AND container_name=? AND container_id=?
                                       AND host=? AND resource_name=?''',
                                    (c['new_id'], c['rowid'], c['name'], c['old_id'], c['host'], c['resource_name']))
                if cursor.rowcount != 1:
                    error('并发冲突或元数据变化，全部回滚: ' + c['name'])
            db.execute('COMMIT')
        except Exception:
            db.execute('ROLLBACK')
            raise
    print('[ok] 已更新 %d 条 container.container_id；未删除容器、未清理挂载、未更改其他 PXD 表。' % len(change_list))
    print('[提示] 现在可以执行 pxd list 和 pxd-check 复核。pxd delete 会删除数据目录，不要把它用作无损测试。')
