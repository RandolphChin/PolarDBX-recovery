#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pxd_recover_check.py —— 容器「删除 → 重建」前后的快照与对比

回答三个问题：
  1. 重建后服务是否生效（容器在跑、DN 选出 Leader、CN 能连、能写入）
  2. DN / GMS 是否被重新初始化（server_uuid、数据文件 inode、binlog 文件是否还是原来那批）
  3. binlog 是延续还是重新开始（DN/GMS 的 binlog 在宿主机挂载目录；CDC 的 binlog 在容器内，
     另外还对比 CN 上 `show binary logs` 的结果）

用法：
  # ① 删除容器之前
  python3 pxd_recover_check.py snapshot before --prefix pxc_single --host <CN的IP> [--count 库名.表名 ...]
  # ② 删除并重建容器（docker rm -f ...; bash xxx.run.sh 或 restore-all.sh）
  # ③ 等服务起来之后
  python3 pxd_recover_check.py snapshot after  --prefix pxc_single --host <CN的IP> --wait 120
  # ④ 对比
  python3 pxd_recover_check.py diff before after

说明：
  * 容器名不写死：从 `docker ps -a` 里按 --prefix（容器名前缀，通常是集群名）匹配；
    不指定 --prefix 时，取镜像名含 polardbx 的容器。
  * 前后两次快照之间，容器按「去掉随机后缀和端口后的名字」对应，例如
    pxc_single-dn-0-Cand-16312 → pxc_single-dn-0，pxc_single-cn-VsFW-64257 → pxc_single-cn。
    所以集群换了端口、容器名后缀变了，也能对上；重建后找不到的容器会报 ✘。
  * --host 必须显式指定，本工具不会自动探测本机 IP（一台机器可能有多个）。
  * CN 端口默认取 CN 容器名末尾的数字（例如 pxc_single-cn-VsFW-64257 → 64257），可用 --cn-port 覆盖。
  * CN 密码从环境变量 PXD_CN_PASSWORD 读取，没有就交互输入；不会写进快照文件。
  * 默认会在 CN 上建一个很小的库 pxd_recover_test（表 marker），每次快照写入一行带标签的记录：
    删除前写入的那一行，重建后还能读到，就证明数据确实延续；重建后新写入的一行同时是写入测试。
    不想写入就加 --no-marker；测试结束后可以 drop database pxd_recover_test。
  * 快照保存在 <dir>/snapshots/<标签>.json，权限 600。
只依赖 Python 3.6+ 标准库，以及宿主机上的 docker 和 mysql 客户端。
"""
import argparse
import datetime
import getpass
import json
import os
import re
import subprocess
import sys
import time

DEFAULT_DIR = "~/pxd-recover"
ROLE_ORDER = ["gms", "dn", "cn", "cdc", "other"]
MARKER_DB = "pxd_recover_test"
FIND_FMT = r"%p\t%s\t%i\t%Ts\n"          # 交给 find 自己解释转义：路径、大小、inode、mtime(秒)
IDENTITY_FILES = ("auto.cnf", "ibdata1")  # 数据目录的“身份证”：重新初始化后这两个文件会被重新创建


def die(msg, code=1):
    print("错误: " + msg, file=sys.stderr)
    sys.exit(code)


def role_of(name):
    m = re.search(r"-(gms|dn|cn|cdc)-", name.lower() + "-")
    return m.group(1) if m else "other"


# 去掉随机后缀和端口：pxc_single-dn-0-Cand-16312 → pxc_single-dn-0；pxc_single-cn-VsFW-64257 → pxc_single-cn
STEM_RE = re.compile(r"^(.*?-(?:gms|cn|cdc|dn)(?:-\d+)?)(?:-.*)?$", re.I)


def stem_of(name):
    m = STEM_RE.match(name)
    return m.group(1) if m else name


def discover(prefix):
    """从 docker ps -a 里按前缀找容器；没有前缀时取镜像名含 polardbx 的。"""
    p = dock("ps", "-a", "--format", "{{.Names}}\t{{.Image}}")
    if p.returncode != 0:
        die("docker ps -a 失败: %s" % p.stderr.strip())
    names = []
    for line in p.stdout.splitlines():
        n, _, img = line.partition("\t")
        if ((n.startswith(prefix + "-") and role_of(n) != "other") if prefix else
                ("polardbx" in img.lower() and role_of(n) != "other")):
            names.append(n)
    groups = {}
    for n in names:
        groups.setdefault(stem_of(n), []).append(n)
    duplicates = {k: v for k, v in groups.items() if len(v) > 1}
    if duplicates:
        die("逻辑身份重复，不能生成无歧义的快照: " + json.dumps(duplicates, ensure_ascii=False))
    return sorted(names, key=lambda n: (ROLE_ORDER.index(role_of(n)), n))


def run(cmd, env=None):
    try:
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              universal_newlines=True, env=env)
    except FileNotFoundError:
        die("找不到命令: %s" % cmd[0])


def dock(*args):
    return run(["docker"] + list(args))


# --------------------------------------------------------------------------- 采集

def collect_container(name):
    p = dock("inspect", name)
    if p.returncode != 0:
        return {"exists": False}
    try:
        d = json.loads(p.stdout)[0]
    except (ValueError, IndexError):
        return {"exists": False}
    st = d.get("State") or {}
    return {"exists": True, "id": d.get("Id"), "running": bool(st.get("Running")),
            "status": st.get("Status"), "started_at": st.get("StartedAt"),
            "exit_code": st.get("ExitCode"), "restart_count": d.get("RestartCount") or 0}


def parse_find(out):
    rows = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) == 4 and parts[1].isdigit() and parts[2].isdigit() and parts[3].isdigit():
            rows[parts[0]] = {"size": int(parts[1]), "inode": int(parts[2]), "mtime": int(parts[3])}
    return rows


def collect_dbnode(name):
    """GMS / DN：数据与日志目录是宿主机挂载的，文件 inode 能证明是不是同一批物理文件。"""
    info = {"files": {}, "server_uuid": None, "paxos_role": None, "errors": []}
    p = dock("exec", name, "find", "/data/mysql", "/data/mysql-log", "-xdev", "-type", "f",
             "(", "-name", "auto.cnf", "-o", "-name", "ibdata1",
             "-o", "-name", "*.[0-9][0-9][0-9][0-9][0-9][0-9]", ")", "-printf", FIND_FMT)
    info["files"] = parse_find(p.stdout)
    if not info["files"]:
        info["errors"].append("find 没有返回文件: " + (p.stderr.strip().splitlines() or ["(无输出)"])[-1])
    for path in info["files"]:
        if os.path.basename(path) == "auto.cnf":
            c = dock("exec", name, "cat", path)
            m = re.search(r"server-uuid\s*=\s*(\S+)", c.stdout)
            if m:
                info["server_uuid"] = m.group(1)
            break
    r = dock("exec", name, "myc", "-N", "-e", "select ROLE from information_schema.alisql_cluster_global")
    lines = [l.strip() for l in r.stdout.splitlines() if l.strip()]
    if r.returncode == 0 and lines:
        info["paxos_role"] = ",".join(lines)
    else:
        info["errors"].append("无法通过 myc 读取 Paxos 角色")
    return info


def collect_cdc(name):
    """CDC 文件可在可写层或挂载卷；须结合 inspect 的实际 Mounts 判断。"""
    info = {"files": {}, "mounts": [], "errors": []}
    p = dock("inspect", name)
    if p.returncode == 0:
        try:
            d = json.loads(p.stdout)[0]
            info["mounts"] = [{"type": m.get("Type"), "source": m.get("Source"),
                               "destination": m.get("Destination")} for m in d.get("Mounts", [])]
        except (ValueError, IndexError):
            info["errors"].append("CDC inspect 无法解析挂载")
    # 避免全盘 find：只扫描现存挂载目的目录、以及 CDC 常见工作目录。
    paths = [m.get("destination") for m in info["mounts"] if m.get("destination")]
    paths += ["/home/admin/polardbx-binlog.standalone", "/home/admin"]
    visited = set()
    for path in paths:
        if path in visited:
            continue
        visited.add(path)
        # -xdev 仅扫描一个挂载设备；若目的目录是独立 mount，则必须单独以其为根。
        p = dock("exec", name, "find", path, "-xdev", "-type", "f", "-name",
                 "binlog.[0-9]*", "-printf", FIND_FMT)
        if p.returncode == 0:
            info["files"].update(parse_find(p.stdout))
    if not info["files"]:
        info["errors"].append("未发现 CDC binlog 文件；路径可能由配置决定，不能推断文件丢失")
    return info


def cn_port_from_names(names):
    for n in names:
        if role_of(n) == "cn":
            m = re.search(r"-(\d+)$", n)
            if m:
                return int(m.group(1))
    return None


def collect_sql(args, names, label):
    port = args.cn_port or cn_port_from_names(names)
    if not port:
        die("无法从容器名推断 CN 端口，请用 --cn-port 指定")
    pwd = os.environ.get("PXD_CN_PASSWORD")
    if pwd is None:
        pwd = getpass.getpass("CN 密码 (%s@%s:%s): " % (args.user, args.host, port))
    env = dict(os.environ)
    env["MYSQL_PWD"] = pwd

    def q(sql):
        p = run([args.mysql_bin, "--protocol=TCP", "-h", args.host, "-P", str(port), "-u", args.user,
                 "-N", "-B", "-e", sql], env=env)
        ok = p.returncode == 0
        return ok, ([l.split("\t") for l in p.stdout.splitlines()] if ok else []), p.stderr.strip()

    out = {"host": args.host, "port": port, "connected": False, "errors": []}
    deadline = time.time() + max(args.wait, 0)
    while True:
        ok, rows, err = q("show databases")
        if ok or time.time() >= deadline:
            break
        time.sleep(5)
    if not ok:
        out["errors"].append("连接/查询失败: " + (err.splitlines()[-1] if err else "未知错误"))
        return out
    out["connected"] = True
    out["databases"] = sorted(r[0] for r in rows)

    def num(x):
        return int(x) if x.isdigit() else x

    ok, rows, err = q("show binary logs")
    out["binary_logs"] = [[r[0], num(r[1]) if len(r) > 1 else None] for r in rows] if ok else None
    if not ok:
        out["errors"].append("show binary logs 失败: " + err)
    ok, rows, err = q("show master status")
    out["master_status"] = rows[0][:2] if ok and rows else None
    ok, rows, err = q("show storage")
    out["storage"] = rows if ok else None
    if not ok:
        out["errors"].append("show storage 失败: " + err)

    out["marker"] = None
    if not args.no_marker:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        steps = [
            "create database if not exists %s" % MARKER_DB,
            "create table if not exists %s.marker (label varchar(64) not null primary key, "
            "taken_at varchar(32) not null)" % MARKER_DB,
            "insert into %s.marker values ('%s', '%s') on duplicate key update taken_at = values(taken_at)"
            % (MARKER_DB, label, ts),
        ]
        wrote = True
        for s in steps:
            ok, _, err = q(s)
            if not ok:
                wrote = False
                out["errors"].append("标记行写入失败: " + err)
                break
        out["marker_written"] = wrote
        ok, rows, err = q("select label, taken_at from %s.marker order by label" % MARKER_DB)
        if ok:
            out["marker"] = {r[0]: (r[1] if len(r) > 1 else "") for r in rows}

    out["counts"] = {}
    for spec in args.count or []:
        ok, rows, err = q("select count(*) from %s" % spec)
        out["counts"][spec] = int(rows[0][0]) if ok and rows and rows[0][0].isdigit() else None
        if not ok:
            out["errors"].append("count %s 失败: %s" % (spec, err))
    return out


# --------------------------------------------------------------------------- snapshot

def cmd_snapshot(args):
    if not re.match(r"^[A-Za-z0-9_.-]{1,40}$", args.label):
        die("标签只能包含字母、数字、_ . -（最长 40）")
    if not args.no_sql and not args.host:
        die("需要 --host <CN 的 IP>（本工具不会自动探测本机 IP）；只想看容器和文件可加 --no-sql")
    for spec in args.count or []:
        if not re.match(r"^[A-Za-z0-9_$]+\.[A-Za-z0-9_$]+$", spec):
            die("--count 需要「库名.表名」形式，收到: %r" % spec)

    d = os.path.expanduser(args.dir)
    names = discover(args.prefix)
    if not names:
        die("docker ps -a 里没有找到容器（--prefix=%r）。请用 --prefix 指定容器名前缀，例如 --prefix pxc_single"
            % args.prefix)

    snap = {"label": args.label, "taken_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "prefix": args.prefix, "containers": {}, "data": {}, "sql": None}
    for n in names:
        r = role_of(n)
        c = collect_container(n)
        c["role"] = r
        c["stem"] = stem_of(n)
        snap["containers"][n] = c
        if c.get("exists") and c.get("running"):
            if r in ("gms", "dn"):
                snap["data"][n] = collect_dbnode(n)
            elif r == "cdc":
                snap["data"][n] = collect_cdc(n)
    if not args.no_sql:
        snap["sql"] = collect_sql(args, names, args.label)

    snap_dir = os.path.join(d, "snapshots")
    os.makedirs(snap_dir, exist_ok=True)
    path = os.path.join(snap_dir, args.label + ".json")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)
    os.chmod(path, 0o600)

    print("快照 [%s] 已保存: %s\n" % (args.label, path))
    for n in names:
        c = snap["containers"][n]
        if not c.get("exists"):
            print("  %-4s %-30s 不存在" % (c["role"], n))
            continue
        extra = ""
        dd = snap["data"].get(n)
        if dd:
            extra = "  文件 %d 个" % len(dd["files"])
            if dd.get("server_uuid"):
                extra += "  server_uuid=%s" % dd["server_uuid"]
            if dd.get("paxos_role"):
                extra += "  角色=%s" % dd["paxos_role"]
        print("  %-4s %-30s %-8s ID=%s%s" % (c["role"], n, c["status"], (c["id"] or "")[:12], extra))
    s = snap["sql"]
    if s is not None:
        if s["connected"]:
            print("\n  CN %s:%s 已连接；库 %d 个；binlog 文件 %s 个；标记行 %s" % (
                s["host"], s["port"], len(s["databases"]),
                len(s["binary_logs"]) if s.get("binary_logs") is not None else "?",
                len(s["marker"]) if s.get("marker") is not None else "-"))
        else:
            print("\n  CN 连接失败: %s" % "; ".join(s["errors"]))
    for n, dd in snap["data"].items():
        for e in dd["errors"]:
            print("  [注意] %s: %s" % (n, e))


# --------------------------------------------------------------------------- diff

class Report(object):
    def __init__(self):
        self.n = {"ok": 0, "bad": 0, "warn": 0}
        self.reinit_evidence = []     # 判断“被重新初始化”的证据
        self.reinit_checked = []      # 已经确认“未被重新初始化”的容器

    def ok(self, m):
        self.n["ok"] += 1
        print("  ✔ " + m)

    def bad(self, m):
        self.n["bad"] += 1
        print("  ✘ " + m)

    def warn(self, m):
        self.n["warn"] += 1
        print("  ⚠ " + m)

    def info(self, m):
        print("  ℹ " + m)


def short(names, limit=4):
    names = sorted(names)
    if not names:
        return "无"
    if len(names) <= limit:
        return "%d 个: %s" % (len(names), ", ".join(names))
    return "%d 个: %s ... %s" % (len(names), ", ".join(names[:2]), names[-1])


def diff_dbnode(rep, name, nb, na):
    fb, fa = nb.get("files", {}), na.get("files", {})
    evid_before = len(rep.reinit_evidence)

    ub, ua = nb.get("server_uuid"), na.get("server_uuid")
    if ub and ua:
        if ub == ua:
            rep.ok("server_uuid 没变（%s）" % ub)
        else:
            rep.bad("server_uuid 变了：%s → %s，数据目录很可能被重新初始化" % (ub, ua))
            rep.reinit_evidence.append("%s: server_uuid 变了" % name)
    else:
        rep.warn("没有读到 server_uuid（auto.cnf），无法用它判断")

    ident = sorted(p for p in fb if os.path.basename(p) in IDENTITY_FILES)
    for p in ident:
        if p not in fa:
            rep.bad("%s 重建后不见了" % p)
            rep.reinit_evidence.append("%s: %s 不见了" % (name, os.path.basename(p)))
        elif fa[p]["inode"] != fb[p]["inode"]:
            rep.bad("%s 的 inode 变了（%d → %d），文件被重新创建过" % (p, fb[p]["inode"], fa[p]["inode"]))
            rep.reinit_evidence.append("%s: %s inode 变了" % (name, os.path.basename(p)))
        else:
            rep.ok("%s 还是同一个物理文件（inode %d）" % (p, fb[p]["inode"]))

    logs = sorted(p for p in fb if p not in ident)
    if not logs:
        rep.warn("重建前没有采集到 binlog 类文件（*.NNNNNN），无法判断 binlog 是否延续")
    else:
        # 每一组（同目录、同前缀，如 mysql_bin.*）里编号最大的是 kill 之前正在写的文件，其余都已经写完
        active = {}
        for p in logs:
            g = os.path.split(p)[0], os.path.basename(p).rsplit(".", 1)[0]
            if g not in active or p > active[g]:
                active[g] = p
        keep = 0
        for p in logs:
            o, n = fb[p], fa.get(p)
            is_last = p in active.values()
            if n is None:
                rep.warn("%s 重建后不在原路径；可能有合法 purge/轮转，需要查 SHOW CONSENSUS LOGS" % p)
            elif n["inode"] != o["inode"]:
                rep.warn("%s 的 inode 变了，日志文件可能被轮转或重建；结合 server_uuid、数据文件及 Consensus 核实" % p)
            elif n["size"] == o["size"] or (is_last and n["size"] > o["size"]):
                keep += 1
            elif is_last:
                keep += 1
                rep.warn("%s 是 kill 之前正在写的文件，重建后小了 %d 字节（%d → %d）。inode 没变，"
                         "多半是崩溃恢复截掉了未提交的尾部，不属于重新初始化" % (p, o["size"] - n["size"], o["size"], n["size"]))
            else:
                rep.bad("%s 是已经写完的文件，大小却变了（%d → %d）" % (p, o["size"], n["size"]))
        if keep == len(logs):
            rep.ok("重建前的 %d 个 binlog 类文件全部还在，inode 相同，已写完的文件大小没变：binlog 是延续的" % keep)
        new = [os.path.basename(p) for p in fa if p not in fb and os.path.basename(p) not in IDENTITY_FILES]
        if new:
            rep.info("重建后新增 %s（mysqld 启动时通常会切换到新的 binlog 文件，属正常现象）" % short(new))

    role = na.get("paxos_role")
    if role is None:
        rep.warn("读不到 Paxos 角色（容器里的 myc 执行失败），请手动确认 DN 已选出 Leader")
    elif role == "Leader":
        rep.ok("Paxos 角色: Leader")
    else:
        rep.warn("Paxos 角色是 %s，不是 Leader；多副本下可能正常，须结合集群选主状态判断" % role)
    if len(rep.reinit_evidence) == evid_before and (ub and ua or ident):
        rep.reinit_checked.append(name)       # 有可比对的证据，而且没有发现“重新初始化”的迹象


def diff_cdc(rep, nb, na, recreated=False, verified_prefix=False):
    """区分正常日志增长、轮转与丢失；CDC 未重建时不误报“重新生成”。"""
    fb, fa = nb.get("files", {}), na.get("files", {})
    if not fb and not fa:
        rep.warn("前后都没有在容器内找到 binlog.NNNNNN 文件，无法对比")
        return

    rep.info("容器内 binlog 文件  前: %s" % short([os.path.basename(p) for p in fb]))
    rep.info("容器内 binlog 文件  后: %s" % short([os.path.basename(p) for p in fa]))
    if not fb:
        rep.warn("前快照未采集到 CDC binlog，无法评价连续性")
        return

    gone = [p for p in fb if p not in fa]
    replaced = [p for p in fb if p in fa and fb[p].get("inode") is not None
                and fa[p].get("inode") is not None
                and fb[p]["inode"] != fa[p]["inode"]]
    shrunk = [p for p in fb if p in fa and isinstance(fb[p].get("size"), int)
              and isinstance(fa[p].get("size"), int)
              and fa[p]["size"] < fb[p]["size"]]
    grown = [p for p in fb if p in fa and isinstance(fb[p].get("size"), int)
             and isinstance(fa[p].get("size"), int)
             and fa[p]["size"] > fb[p]["size"]]

    if not recreated:
        rep.info("CDC 容器 ID 未变，本轮未删除/重建 CDC；只检查其运行期文件变化")
        if not gone and not replaced and not shrunk:
            if grown:
                rep.ok("CDC binlog 路径与 inode 保留、文件大小正常增长（%d 个）；mtime 变化属预期" % len(grown))
            else:
                rep.ok("CDC binlog 路径与 inode 保留、文件大小未缩小；mtime 变化不代表文件重建")
        else:
            rep.warn("CDC 文件发生轮转/替换或缩小，需结合 CDC 日志及消费位点确认（不代表本轮 DN 恢复失败）")
    else:
        if not gone and not replaced and not shrunk:
            rep.ok("CDC binlog 原路径、inode 保留且大小未缩小；历史完整性仍须检查消费位点")
        elif verified_prefix and not gone and not shrunk:
            rep.ok("CDC 重建后 inode 改变属文件回填：独立冷备份 SHA256 历史前缀校验通过；下游位点仍需核实")
        else:
            rep.warn("CDC 重建前后的 binlog 文件发生缺失/替换/缩小；需检查挂载及消费位点")
    if gone:
        rep.info("后快照缺少: %s" % short([os.path.basename(p) for p in gone]))
    if replaced:
        rep.info("同路径 inode 改变: %s" % short([os.path.basename(p) for p in replaced]))
    if shrunk:
        rep.info("文件变小: %s" % short([os.path.basename(p) for p in shrunk]))


def diff_sql(rep, sb, sa):
    print("\n== CN / 数据库层面（%s:%s）" % (sa.get("host"), sa.get("port")) if sa else "\n== CN / 数据库层面")
    if sb is None or sa is None:
        rep.warn("其中一份快照没有采集 SQL 部分（用了 --no-sql？），跳过")
        return
    if not sa.get("connected"):
        rep.bad("重建后 CN 连不上: %s" % "; ".join(sa.get("errors", [])))
        return
    if not sb.get("connected"):
        rep.warn("重建前的快照里 CN 就没连上，无法对比数据库层面")
        return
    rep.ok("重建后 CN 可连接")

    miss = [d for d in sb["databases"] if d not in sa["databases"]]
    if miss:
        rep.bad("重建后缺少这些库: %s" % ", ".join(miss))
    else:
        rep.ok("重建前的 %d 个库全部还在" % len(sb["databases"]))
    added = [d for d in sa["databases"] if d not in sb["databases"]]
    if added:
        rep.info("重建后多出的库: %s" % ", ".join(added))

    mb, ma = sb.get("marker"), sa.get("marker")
    if mb is None or ma is None:
        rep.info("没有标记行数据（--no-marker 或写入失败），跳过标记行对比")
    else:
        lost = [k for k in mb if k not in ma]
        if lost:
            rep.bad("删除前写入的标记行丢失: %s —— 数据没有延续" % ", ".join(lost))
        else:
            rep.ok("删除前写入的标记行 %s 重建后仍能读到：数据延续" % ", ".join(sorted(mb)))
        if sa.get("marker_written") and sa.get("marker") is not None:
            rep.ok("重建后写入新标记行并读回成功：写入正常")
        else:
            rep.bad("重建后写入标记行失败: %s" % "; ".join(sa.get("errors", [])))

    for spec in sorted(set(sb.get("counts", {})) | set(sa.get("counts", {}))):
        cb, ca = sb.get("counts", {}).get(spec), sa.get("counts", {}).get(spec)
        if cb is None or ca is None:
            rep.warn("%s 行数没取到（前 %s / 后 %s）" % (spec, cb, ca))
        elif cb == ca:
            rep.ok("%s 行数一致（%d）" % (spec, cb))
        else:
            rep.bad("%s 行数不一致：%d → %d（若这期间有业务写入，则属正常）" % (spec, cb, ca))

    bl, al = sb.get("binary_logs"), sa.get("binary_logs")
    print("\n== CN 上 show binary logs（CDC 全局 binlog）")
    if not bl or al is None:
        rep.warn("没有采集到 show binary logs，无法判断 CDC binlog")
    else:
        rep.info("重建前: %s" % short([x[0] for x in bl]))
        rep.info("重建后: %s" % short([x[0] for x in al]))
        nw = dict((x[0], x[1]) for x in al)
        missing = [n for n, _ in bl if n not in nw]
        changed = [n for n, s in bl[:-1] if n in nw and nw[n] != s]
        last_n, last_s = bl[-1]
        shrunk = last_n in nw and isinstance(last_s, int) and isinstance(nw[last_n], int) and nw[last_n] < last_s
        unchanged = [n for n, s in bl if n in nw and nw[n] == s]       # 旧文件原样还在
        if last_n in nw and isinstance(last_s, int) and isinstance(nw[last_n], int) and nw[last_n] > last_s:
            unchanged.append(last_n)                                    # 最后一个文件可以继续变大
        if not missing and not changed and not shrunk:
            rep.ok("重建前的 %d 个全局 binlog 文件名/大小符合保留预期；仍需核对消费进度与连续性" % len(bl))
        elif not unchanged:
            rep.warn("重建前的 %d 个全局 binlog 未见原样保留；可能轮转或丢失，需进一步核对连续性（重建后列表: %s）"
                     % (len(bl), short([x[0] for x in al])))
        else:
            rep.warn("重建前的 binlog 只有 %d/%d 个原样保留，其余缺失或被重新生成" % (len(unchanged), len(bl)))
        if missing:
            rep.info("新列表缺少旧文件: %s" % short(missing))
        if changed or shrunk:
            rep.info("同名但大小对不上（重新生成的同名文件）: %s" % short(changed + ([last_n] if shrunk else [])))
    if sb.get("master_status") or sa.get("master_status"):
        rep.info("show master status  前: %s  后: %s" % (sb.get("master_status"), sa.get("master_status")))
    if sb.get("storage") != sa.get("storage") and sb.get("storage") is not None:
        rep.info("show storage 输出前后不同（可能只是时间/状态字段变化），请肉眼确认 DN 状态正常")


def keyed(snap):
    """stem → 实际容器名；同一个 stem 对应多个容器时退回用完整容器名，避免对错。"""
    groups = {}
    for n, c in snap["containers"].items():
        groups.setdefault(c.get("stem") or stem_of(n), []).append(n)
    out = {}
    for stem, ns in groups.items():
        if len(ns) == 1:
            out[stem] = ns[0]
        else:
            for n in ns:
                out[n] = n
    return out


def cmd_diff(args):
    d = os.path.expanduser(args.dir)

    def load(label):
        p = os.path.join(d, "snapshots", label + ".json")
        if not os.path.exists(p):
            die("找不到快照 %s" % p)
        with open(p, encoding="utf-8") as f:
            return json.load(f)

    b, a = load(args.before), load(args.after)
    rep = Report()
    print("对比 [%s] %s  →  [%s] %s" % (b["label"], b["taken_at"], a["label"], a["taken_at"]))

    kb, ka = keyed(b), keyed(a)          # 对应关系：stem → 实际容器名
    keys = sorted(set(kb) | set(ka), key=lambda k: (ROLE_ORDER.index(role_of(k)), k))
    for k in keys:
        role = role_of(k)
        nb_name, na_name = kb.get(k), ka.get(k)
        cb = b["containers"].get(nb_name) if nb_name else None
        ca = a["containers"].get(na_name) if na_name else None
        print("\n== %s  (%s)" % (k, role.upper()))
        rep.info("容器名  前: %s   后: %s" % (nb_name or "-", na_name or "-"))
        if nb_name and na_name and nb_name != na_name:
            rep.info("名字不同，是按“%s”对应起来的（端口或随机后缀变了）" % k)
        if not ca or not ca.get("exists"):
            rep.bad("重建后的快照里找不到这个容器（docker ps -a 里没有以前缀匹配到它）")
            continue
        if not cb or not cb.get("exists"):
            rep.warn("重建前的快照里没有这个容器，无法对比")
            continue
        if cb["id"] == ca["id"]:
            rep.info("容器 ID 没变，说明这个容器没有被删除重建；下面的对比只说明“前后一致”")
        else:
            rep.ok("容器已重建（ID %s → %s）" % (cb["id"][:12], ca["id"][:12]))
        if ca["running"]:
            rep.ok("正在运行（启动于 %s）" % ca["started_at"])
            if ca.get("restart_count"):
                rep.warn("RestartCount=%s，容器在反复重启" % ca["restart_count"])
        else:
            rep.bad("没有在运行（status=%s, exit=%s），请看 docker logs %s" % (ca["status"], ca["exit_code"], na_name))
            continue
        nb, na = b["data"].get(nb_name), a["data"].get(na_name)
        if role in ("gms", "dn"):
            if nb is None or na is None:
                rep.warn("缺少文件采集数据（前快照时容器没在运行？）")
            else:
                diff_dbnode(rep, k, nb, na)
        elif role == "cdc":
            if nb is None or na is None:
                rep.warn("缺少文件采集数据（前快照时容器没在运行？）")
            else:
                diff_cdc(rep, nb, na, recreated=(cb["id"] != ca["id"]),
                         verified_prefix=bool(getattr(args, "cdc_verified", False)))

    diff_sql(rep, b.get("sql"), a.get("sql"))

    print("\n" + "=" * 60)
    print("统计: ✔ %d   ✘ %d   ⚠ %d" % (rep.n["ok"], rep.n["bad"], rep.n["warn"]))
    evid_names = set(e.split(":")[0] for e in rep.reinit_evidence)
    unchecked = [k for k in keys if role_of(k) in ("gms", "dn")
                 and k not in rep.reinit_checked and k not in evid_names]
    if rep.reinit_evidence:
        print("DN/GMS 重新初始化: 有证据 —— " + "; ".join(rep.reinit_evidence))
    elif unchecked:
        print("DN/GMS 重新初始化: 无法完全判断 —— 未能检查: %s%s" % (
            ", ".join(unchecked),
            "；已确认没有重新初始化: " + ", ".join(rep.reinit_checked) if rep.reinit_checked else ""))
    elif rep.reinit_checked:
        print("DN/GMS 重新初始化: 没有（%s 的 server_uuid / 数据文件 inode 都没变）" % ", ".join(rep.reinit_checked))
    else:
        print("DN/GMS 重新初始化: 无法判断（没有采集到可用的文件证据）")
    if rep.n["bad"]:
        print("结论: 有 %d 项异常，见上面的 ✘" % rep.n["bad"])
    elif rep.n["warn"]:
        print("结论: 没有发现异常，但有 %d 项无法确认或需要你判断，见上面的 ⚠" % rep.n["warn"])
    else:
        print("结论: 全部通过")
    sys.exit(1 if rep.n["bad"] else 0)


def main():
    ap = argparse.ArgumentParser(description="容器删除/重建前后的快照与对比")
    sub = ap.add_subparsers(dest="cmd")
    sub.required = True

    p = sub.add_parser("snapshot", help="采集一份快照")
    p.add_argument("label", help="快照标签，例如 before / after")
    p.add_argument("--dir", default=DEFAULT_DIR, help="快照保存目录，默认 %s" % DEFAULT_DIR)
    p.add_argument("--prefix", default="", help="容器名前缀（集群名），从 docker ps -a 匹配；"
                                                "不指定则取镜像名含 polardbx 的容器")
    p.add_argument("--host", help="CN 的 IP，必须显式指定（不自动探测）")
    p.add_argument("--cn-port", type=int, help="CN 端口，默认取 CN 容器名末尾的数字")
    p.add_argument("--user", default="polardbx_root", help="CN 账号，默认 polardbx_root")
    p.add_argument("--mysql-bin", default="mysql", help="mysql 客户端路径，默认 mysql")
    p.add_argument("--count", action="append", default=[], metavar="库.表",
                   help="记录该表的 count(*)，可重复使用")
    p.add_argument("--no-marker", action="store_true", help="不在 CN 上写标记行")
    p.add_argument("--no-sql", action="store_true", help="不连接 CN，只采集容器和文件信息")
    p.add_argument("--wait", type=int, default=0, help="CN 暂时连不上时，最多等待的秒数（重建后建议 120）")
    p.set_defaults(func=cmd_snapshot)

    p = sub.add_parser("diff", help="对比两份快照")
    p.add_argument("before")
    p.add_argument("after")
    p.add_argument("--dir", default=DEFAULT_DIR)
    p.set_defaults(func=cmd_diff)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
