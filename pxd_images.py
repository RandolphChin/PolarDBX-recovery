#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Export exact Docker image IDs referenced by saved PolarDB-X inspect snapshots.

Image tar files and receipt are stored in a private folder. No container is changed.
`verify` works even when the Docker daemon is unavailable.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

RECEIPT = 'image-archive-manifest.json'
IMAGE_ID = re.compile(r'^sha256:([0-9a-f]{64})$')


def fail(s):
    raise SystemExit('ERROR: ' + s)


def wanted_ids(inspects):
    """Return mapping from full image ID to archived logical node names."""
    ids = {}
    for _, d in inspects:
        name = (d.get('Name') or '').lstrip('/')
        image_id = d.get('Image')
        if not IMAGE_ID.fullmatch(image_id or ''):
            fail('inspect 缺少可验证的 sha256 Image ID: ' + name)
        ids.setdefault(image_id, []).append(name)
    if not ids:
        fail('没有 inspect 快照')
    return {i: sorted(v) for i, v in sorted(ids.items())}


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def filename_for(image_id):
    m = IMAGE_ID.fullmatch(image_id)
    if not m:
        fail('Image ID 格式错误: ' + str(image_id))
    return 'image-' + m.group(1) + '.tar'


def _safe_directory(image_dir):
    image_dir = Path(image_dir).expanduser()
    if image_dir.is_symlink():
        fail('镜像归档目录不能是符号链接: ' + str(image_dir))
    image_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(str(image_dir), 0o700)
    return image_dir


def export_images(inspects, image_dir):
    """Save exact images once, never silently overwrite an existing archive."""
    ids = wanted_ids(inspects)
    out = _safe_directory(image_dir)
    receipt = out / RECEIPT
    if receipt.exists() or any((out / filename_for(i)).exists() for i in ids):
        fail('目标存在镜像归档；禁止覆盖。请先运行 verify-images，或指定另一个 --image-dir')
    for image_id in ids:
        try:
            p = subprocess.run(['docker', 'image', 'inspect', '-f', '{{.Id}}', image_id],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        except FileNotFoundError:
            fail('没有找到 docker 命令')
        if p.returncode or p.stdout.strip() != image_id:
            fail('原 Image ID 不在本机，无法导出: ' + image_id + ' ' + p.stderr.strip()[:200])
    records = {}
    for image_id, nodes in ids.items():
        dest = out / filename_for(image_id)
        fd, path = tempfile.mkstemp(prefix='.image-staging-', suffix='.tar', dir=str(out))
        os.close(fd)
        try:
            p = subprocess.run(['docker', 'image', 'save', '-o', path, image_id],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if p.returncode:
                fail('docker image save 失败: ' + image_id + ' ' + p.stderr.strip()[:500])
            size = os.stat(path).st_size
            if size == 0:
                fail('docker image save 输出为空: ' + image_id)
            checksum = file_sha256(path)
            os.chmod(path, 0o600)
            os.replace(path, dest)
        finally:
            if os.path.exists(path):
                os.unlink(path)
        records[image_id] = {'file': dest.name, 'bytes': size, 'sha256': checksum, 'nodes': nodes}
        print('[saved] %s (%d bytes), 用于 %s' % (dest.name, size, ', '.join(nodes)))
    payload = {'version': 1, 'images': records}
    fd, path = tempfile.mkstemp(prefix='.manifest-staging-', dir=str(out))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.chmod(path, 0o600)
        os.replace(path, receipt)
    finally:
        if os.path.exists(path):
            os.unlink(path)
    print('[ok] 镜像归档完成: %s；请把目录与 inspect、CDC 冷备份一起安全归档' % out)


def verify_images(inspects, image_dir):
    """Verify offline file integrity and image IDs recorded in sealed manifest."""
    ids = wanted_ids(inspects)
    folder = Path(image_dir).expanduser()
    manifest = folder / RECEIPT
    if not manifest.is_file() or manifest.is_symlink():
        fail('缺少可信镜像归档清单: ' + str(manifest))
    try:
        payload = json.loads(manifest.read_text(encoding='utf-8'))
    except (ValueError, OSError) as e:
        fail('镜像归档清单不可读取: ' + str(e))
    records = payload.get('images') if payload.get('version') == 1 else None
    if not isinstance(records, dict) or set(records) != set(ids):
        fail('镜像归档 Image ID 与当前 inspect 快照不一致（可能用错部署的备份）')
    for image_id, nodes in ids.items():
        info = records[image_id]
        name = filename_for(image_id)
        if info.get('file') != name or info.get('nodes') != nodes:
            fail('镜像归档身份不匹配: ' + image_id)
        p = folder / name
        if not p.is_file() or p.is_symlink():
            fail('镜像归档文件不存在或为软链接: ' + str(p))
        if p.stat().st_size != info.get('bytes') or file_sha256(p) != info.get('sha256'):
            fail('镜像归档被修改或损坏: ' + str(p))
        print('[ok] %s SHA256 一致（%d bytes）' % (p.name, p.stat().st_size))
    print('[ok] 所有镜像归档文件验证通过；此检查不需要 Docker，也不启动容器。')
