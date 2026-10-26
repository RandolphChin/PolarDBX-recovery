#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pxd_recover.py —— 从 docker inspect 的原始 JSON 生成 PolarDB-X (pxd) 容器的恢复脚本

为什么不直接用 runlike：
  Config.Entrypoint 有三种状态，runlike 分不清：
    null            镜像没有 entrypoint            -> 不加 --entrypoint
    []              显式清空了镜像的 entrypoint    -> 必须加 --entrypoint ""   (DN / GMS)
    ["a", "b"]      自定义 entrypoint              -> --entrypoint a，其余参数放到镜像名之后 (CN)
  漏掉 `--entrypoint ""` 时，镜像默认的 entrypoint 会把 `bash -c ...` 当参数吞掉，容器起不来。
  注意：`docker inspect --format '{{json .Config.Entrypoint}}'` 会把 [] 显示成 null，
  所以本工具直接读 inspect 的原始 JSON。

关于 IP：
  本工具不会探测本机 IP（一台机器可能有多个）。脚本里所有地址（NODE_IP、POD_IP 等）
  都原样取自 inspect.json。确实需要换 IP 时，用 --ip-map 旧IP=新IP 显式指定，可重复使用。
  --ip-map 只替换环境变量里完整匹配的 IPv4（10.0.0.4 不会误伤 10.0.0.43）；
  挂载路径（如 10_35_18_43_16088 这种数据目录名）和 hostname 不会改。
  注意：换 IP 通常不是只改环境变量就够，GMS 里记录的节点地址不会跟着变，请先确认再用。

用法：
  python3 pxd_recover.py backup   [--prefix pxc_single] [--dir ~/pxd-recover] [--pin-image] [--ip-map 旧=新]
      保存正在运行（或已停止）容器的 inspect.json，并生成 run.sh 和 restore-all.sh
  python3 pxd_recover.py generate [--dir ~/pxd-recover] [--pin-image] [--ip-map 旧=新]
      不连接 docker 容器，只根据目录里已有的 *.inspect.json 重新生成脚本
  python3 pxd_recover_v2.py restore --component dn-0 [--prefix pxc_single] [--dry-run]
      根据已备份的逻辑身份恢复缺失组件，绝不主动删除已有容器
  python3 pxd_recover_v2.py verify <原始容器名> [--dir ~/pxd-recover]
      重建之后，对比新容器和保存的 inspect.json 是否一致（不打印环境变量的值）
      保存的 inspect.json 始终是原始内容，--ip-map 只影响生成的脚本和 verify 的比较

只依赖 Python 3.6+ 标准库及 Linux/Docker/Bash。默认通过 Image ID 固定镜像。
备份阶段额外记录挂载源的设备号/inode 和非空状态，预防错误挂载。
生成的文件可能包含明文密码：inspect.json 为 600，脚本为 700。
不能从 inspect 恢复容器可写层；必须事先做好真正的数据库备份。
"""
import argparse
import datetime
import glob
import ipaddress
import json
import stat
import os
import re
import shlex
import subprocess
import sys

ROLE_ORDER = ["gms", "dn", "cn", "cdc", "other"]      # 恢复顺序：GMS → DN → CN → CDC
ROLE_WAIT = {"gms": 20, "dn": 20, "cn": 30, "cdc": 30, "other": 10}   # 每个容器启动后等待秒数
INSPECT_SUFFIX = ".inspect.json"
DEFAULT_DIR = "~/pxd-recover"
MANIFEST = "mount-guards.json"
STEM_RE = re.compile(r"^(.*?-(?:gms|cn|cdc|dn-\d+))(?:-.+)?$", re.I)
COMPONENT_RE = re.compile(r"^(?:gms|cn|cdc|dn-\d+)$", re.I)

_IMG_CACHE = {}


# --------------------------------------------------------------------------- 基础工具

def die(msg, code=1):
    print("错误: " + msg, file=sys.stderr)
    sys.exit(code)


def run_docker(*args, **kw):
    """运行 docker 子命令，返回 CompletedProcess。check=True 时失败直接退出。"""
    check = kw.get("check", True)
    try:
        p = subprocess.run(["docker"] + list(args), stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, universal_newlines=True)
    except FileNotFoundError:
        die("找不到 docker 命令")
    if check and p.returncode != 0:
        die("docker %s 失败: %s" % (" ".join(args), p.stderr.strip()))
    return p


def write_file(path, text, mode):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.chmod(path, mode)


def save_with_rotation(path, text, mode):
    """写文件；已存在且内容不同时，把旧文件改名为 <path>.<时间>.bak，避免覆盖掉可用的旧版本。"""
    rotated = None
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            old = f.read()
        if old == text:
            os.chmod(path, mode)
            return None
        ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        rotated = "%s.%s.bak" % (path, ts)
        os.rename(path, rotated)
    write_file(path, text, mode)
    return rotated


def load_inspect(path):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError) as e:
        die("无法读取 %s: %s" % (path, e))
    if isinstance(data, list):
        if not data:
            die("%s 是空列表" % path)
        data = data[0]
    if not isinstance(data, dict) or "Config" not in data:
        die("%s 不是 docker inspect 的输出" % path)
    return data


def container_name(d):
    return (d.get("Name") or "").lstrip("/")


def detect_role(name, image):
    m = re.search(r"-(gms|dn|cn|cdc)-", name.lower() + "-")
    if m:
        return m.group(1)
    image = (image or "").lower()
    if "polardbx-cdc" in image:
        return "cdc"
    if "polardbx-sql" in image:
        return "cn"
    if "polardbx-engine" in image:
        return "dn"
    return "other"


def stem_of(name):
    """容器的逻辑身份；匹配具体实例时不依赖随机后缀或端口。"""
    m = STEM_RE.fullmatch(name)
    return m.group(1) if m else None


def matches_cluster(name, prefix):
    if prefix and not name.startswith(prefix + "-"):
        return False
    stem = stem_of(name)
    return bool(stem and (not prefix or stem.startswith(prefix + "-")))


def ensure_unique(names):
    groups = {}
    for name in names:
        stem = stem_of(name)
        if not stem:
            die("无法解析逻辑身份: %s，请核实容器名称" % name)
        groups.setdefault(stem, []).append(name)
    duplicates = {k: v for k, v in groups.items() if len(v) > 1}
    if duplicates:
        die("发现逻辑身份重复，拒绝自动选择: %s" % json.dumps(duplicates, ensure_ascii=False))
    return groups


def docker_names(prefix=""):
    p = run_docker("ps", "-a", "--format", "{{.Names}}\t{{.Image}}")
    selected = []
    for line in p.stdout.splitlines():
        name, _, image = line.partition("\t")
        if (matches_cluster(name, prefix) if prefix else
                ("polardbx" in image.lower() and stem_of(name))):
            selected.append(name)
    ensure_unique(selected)
    return selected


def collect_guards(d):
    """backup 时记录宿主机挂载源身份。重建时避免 -v 自动创建错误的空目录。"""
    mounts = d.get("Mounts") or []
    h = d.get("HostConfig") or {}
    bind_paths = set()
    volumes = set()
    for m in mounts:
        if m.get("Type") == "bind" and m.get("Source"):
            bind_paths.add(m["Source"])
        if m.get("Type") == "volume" and m.get("Name"):
            volumes.add(m["Name"])
    for b in h.get("Binds") or []:
        src = b.split(":", 1)[0]
        if src.startswith("/"):
            bind_paths.add(src)
    saved = {}
    for path in sorted(bind_paths):
        try:
            st = os.stat(path)
            if stat.S_ISDIR(st.st_mode):
                with os.scandir(path) as items:
                    nonempty = any(True for _ in items)
                kind = "dir"
            elif stat.S_ISREG(st.st_mode):
                nonempty, kind = st.st_size > 0, "file"
            else:
                nonempty, kind = False, "other"
            saved[path] = {"device": st.st_dev, "inode": st.st_ino,
                           "kind": kind, "nonempty": nonempty}
        except OSError as exc:
            die("无法检查原始挂载目录 %s: %s。未保存不可靠的备份" % (path, exc))
    return {"binds": saved, "volumes": sorted(volumes)}


def load_manifest(out_dir):
    path = os.path.join(out_dir, MANIFEST)
    if not os.path.isfile(path):
        return {"guards": {}, "prefix": ""}
    with open(path, encoding="utf-8") as f:
        result = json.load(f)
    return result


def image_config(ref):
    """取镜像自带的 Config，用来去掉镜像默认的环境变量/标签/工作目录。取不到返回 None。"""
    if not ref:
        return None
    if ref in _IMG_CACHE:
        return _IMG_CACHE[ref]
    cfg = None
    try:
        p = subprocess.run(["docker", "image", "inspect", ref], stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, universal_newlines=True)
        if p.returncode == 0:
            cfg = (json.loads(p.stdout)[0].get("Config")) or {}
    except (OSError, ValueError, IndexError):
        cfg = None
    _IMG_CACHE[ref] = cfg
    return cfg


def entrypoint_desc(ep):
    if ep is None:
        return "镜像默认"
    if len(ep) == 0:
        return "清空 (--entrypoint '')"
    return " ".join(ep)


# --------------------------------------------------------------------------- IP 处理（只做显式指定，不探测本机）

IP_RE = re.compile(r"(?<![0-9.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9]|\.[0-9])")


def parse_ip_map(items):
    """['10.0.0.1=10.0.0.2', ...] -> [(旧, 新)]；格式不对直接退出。"""
    pairs = []
    for it in items or []:
        old, _, new = it.partition("=")
        try:
            pairs.append((str(ipaddress.IPv4Address(old.strip())), str(ipaddress.IPv4Address(new.strip()))))
        except ValueError:
            die("--ip-map 需要「旧IP=新IP」的 IPv4 形式，收到: %r" % it)
    return pairs


def apply_ip_map(text, ip_map):
    """只替换完整的 IPv4 片段，一次扫描完成（A→B、B→C 不会连环替换）。返回 (新文本, 替换次数)。"""
    mapping = dict(ip_map or [])
    if not mapping:
        return text, 0
    hits = [0]

    def sub(m):
        v = m.group(0)
        if v in mapping:
            hits[0] += 1
            return mapping[v]
        return v

    return IP_RE.sub(sub, text), hits[0]


def find_ips(env_list):
    """列出环境变量值里出现的 IPv4（只看 IP，不暴露其他内容）。"""
    ips = set()
    for e in env_list or []:
        for m in IP_RE.finditer(e.partition("=")[2]):
            try:
                ipaddress.IPv4Address(m.group(0))
            except ValueError:
                continue
            ips.add(m.group(0))
    return sorted(ips, key=lambda s: tuple(int(x) for x in s.split(".")))


# --------------------------------------------------------------------------- 生成 docker run

def build_run(d, pin_image=True, ip_map=None, image_shell_var=False):
    """返回 (docker run 命令文本, 需要预先存在的宿主机路径列表, 提示列表, 镜像是否能读到默认配置)"""
    c = d.get("Config") or {}
    h = d.get("HostConfig") or {}
    cid = d.get("Id") or ""

    img_cfg = image_config(d.get("Image")) or image_config(c.get("Image"))
    img_known = img_cfg is not None
    img_cfg = img_cfg or {}

    opts, notes = [], []

    def add(*parts):
        opts.append(" ".join(shlex.quote(str(p)) for p in parts))

    # ---- 基本
    add("-d")
    if c.get("OpenStdin"):
        add("-i")
    if c.get("Tty"):
        add("-t")
    add("--name", container_name(d))
    hn = c.get("Hostname")
    if hn and hn != cid[:12]:            # 12 位短 ID 是 docker 自动生成的，不需要指定
        add("--hostname", hn)
    if c.get("User") and c.get("User") != img_cfg.get("User"):
        add("--user", c["User"])
    wd = c.get("WorkingDir")
    if wd and wd != img_cfg.get("WorkingDir"):
        add("--workdir", wd)

    # ---- 网络
    net = h.get("NetworkMode") or "default"
    if net not in ("default", "bridge"):
        add("--network", net)
    if not (net == "host" or net.startswith("container:") or net == "none"):
        for cport, bl in sorted((h.get("PortBindings") or {}).items()):
            for b in (bl or [None]):
                b = b or {}
                ip, hp = b.get("HostIp"), b.get("HostPort")
                if hp:
                    spec = (ip + ":" if ip else "") + hp + ":" + cport
                elif ip:
                    spec = ip + "::" + cport
                else:
                    spec = cport
                add("-p", spec)
    for x in h.get("ExtraHosts") or []:
        add("--add-host", x)
    for x in h.get("Dns") or []:
        add("--dns", x)

    # ---- 权限 / 命名空间
    if h.get("Privileged"):
        add("--privileged")
    for x in h.get("CapAdd") or []:
        add("--cap-add", x)
    for x in h.get("CapDrop") or []:
        add("--cap-drop", x)
    for x in h.get("SecurityOpt") or []:
        add("--security-opt", x)
    for x in h.get("GroupAdd") or []:
        add("--group-add", x)
    for dev in h.get("Devices") or []:
        add("--device", "%s:%s:%s" % (dev.get("PathOnHost"), dev.get("PathInContainer"),
                                      dev.get("CgroupPermissions") or "rwm"))
    if h.get("PidMode"):
        add("--pid", h["PidMode"])
    ipc = h.get("IpcMode") or ""
    if ipc == "host" or ipc.startswith("container:"):
        add("--ipc", ipc)
    if h.get("UTSMode"):
        add("--uts", h["UTSMode"])
    if h.get("Init"):
        add("--init")
    if h.get("ReadonlyRootfs"):
        add("--read-only")
    for k, v in sorted((h.get("Sysctls") or {}).items()):
        add("--sysctl", "%s=%s" % (k, v))
    for u in h.get("Ulimits") or []:
        add("--ulimit", "%s=%s:%s" % (u.get("Name"), u.get("Soft"), u.get("Hard")))

    # ---- 资源
    mem = h.get("Memory") or 0
    if mem > 0:
        add("--memory", mem)
        swap = h.get("MemorySwap") or 0
        if swap != 0 and swap != 2 * mem:       # 2 倍内存是 docker 的默认值，不用重复写
            add("--memory-swap", swap)
    if (h.get("MemoryReservation") or 0) > 0:
        add("--memory-reservation", h["MemoryReservation"])
    if (h.get("NanoCpus") or 0) > 0:
        add("--cpus", "%g" % (h["NanoCpus"] / 1e9))
    if (h.get("CpuShares") or 0) > 0:
        add("--cpu-shares", h["CpuShares"])
    if h.get("CpusetCpus"):
        add("--cpuset-cpus", h["CpusetCpus"])
    if (h.get("PidsLimit") or 0) > 0:
        add("--pids-limit", h["PidsLimit"])
    if (h.get("ShmSize") or 0) > 0 and h["ShmSize"] != 67108864:
        add("--shm-size", h["ShmSize"])
    if h.get("OomScoreAdj"):
        add("--oom-score-adj", h["OomScoreAdj"])
    rp = h.get("RestartPolicy") or {}
    if rp.get("Name") not in (None, "", "no"):
        add("--restart", rp["Name"] + (":%s" % rp["MaximumRetryCount"] if rp.get("MaximumRetryCount") else ""))
    if h.get("Runtime") and h["Runtime"] != "runc":
        add("--runtime", h["Runtime"])
    lc = h.get("LogConfig") or {}
    if lc.get("Type") not in (None, "", "json-file"):
        add("--log-driver", lc["Type"])
    for k, v in sorted((lc.get("Config") or {}).items()):
        add("--log-opt", "%s=%s" % (k, v))
    if c.get("StopSignal") and c.get("StopSignal") != img_cfg.get("StopSignal"):
        add("--stop-signal", c["StopSignal"])
    if c.get("StopTimeout") is not None:
        add("--stop-timeout", c["StopTimeout"])

    # ---- 挂载：优先用创建时的 Binds（保留 :ro/:z 等选项），再补上 Binds 没覆盖的挂载
    binds = list(h.get("Binds") or [])
    covered = set(b.split(":")[1] for b in binds if len(b.split(":")) > 1)
    for m in d.get("Mounts") or []:
        dst, typ = m.get("Destination"), m.get("Type")
        if dst in covered:
            continue
        if typ == "bind":
            src = m.get("Source")
        elif typ == "volume":
            src = m.get("Name")
        else:
            continue
        binds.append("%s:%s%s" % (src, dst, "" if m.get("RW", True) else ":ro"))
    must_exist = []
    for b in binds:
        add("--volume", b)
        src = b.split(":")[0]
        if src.startswith("/"):
            must_exist.append(src)
    for path, o in sorted((h.get("Tmpfs") or {}).items()):
        add("--tmpfs", path + (":" + o if o else ""))

    # ---- 环境变量 / 标签：去掉镜像自带的，只保留创建容器时额外指定的
    img_env = set(img_cfg.get("Env") or [])
    ip_hits = 0
    for e in c.get("Env") or []:
        if e not in img_env:
            e2, k = apply_ip_map(e, ip_map)
            ip_hits += k
            add("--env", e2)
    if ip_map:
        notes.append("按 --ip-map 替换了环境变量里的 IP，共 %d 处（%s）；挂载路径和 hostname 未改" %
                     (ip_hits, ", ".join("%s → %s" % p for p in ip_map)))
        for old, _ in ip_map:
            if any(old.replace(".", "_") in b for b in binds):
                notes.append("挂载路径里的 %s 是已有数据目录名，保持不变" % old.replace(".", "_"))
    if not img_known:
        notes.append("本机没有找到该镜像，无法区分镜像自带的环境变量，已全部写入（含 PATH 等，无害）")
    img_labels = img_cfg.get("Labels") or {}
    for k, v in sorted((c.get("Labels") or {}).items()):
        if img_labels.get(k) != v:
            add("--label", "%s=%s" % (k, v))

    # ---- entrypoint / cmd（本工具存在的核心原因）
    ep, cmd = c.get("Entrypoint"), c.get("Cmd")
    tail = []
    if ep is None:
        pass
    elif len(ep) == 0:
        add("--entrypoint", "")
    else:
        add("--entrypoint", ep[0])
        tail.extend(ep[1:])          # docker 的 --entrypoint 只接受一个可执行文件，其余参数放镜像名之后
    tail.extend(cmd or [])

    # ---- 无法还原的项
    networks = list((d.get("NetworkSettings") or {}).get("Networks") or {})
    if len(networks) > 1:
        die("%s 接入多个 Docker 网络 %s，当前工具拒绝不完整恢复；须补充 network connect 逻辑" %
            (container_name(d), networks))
    if h.get("VolumesFrom"):
        die("%s 使用 VolumesFrom，当前工具不支持安全还原，请先处理此依赖" % container_name(d))
    if h.get("Links"):
        die("%s 使用 Links，当前工具不支持安全还原，请先处理此依赖" % container_name(d))
    for n, detail in ((d.get("NetworkSettings") or {}).get("Networks") or {}).items():
        if (detail or {}).get("IPAMConfig"):
            die("%s 的 %s 网络存在静态 IPAMConfig，当前工具拒绝丢弃静态 IP" % (container_name(d), n))

    image = d.get("Image") if (pin_image and d.get("Image")) else c.get("Image")
    lines = ["docker run"] + opts + (['"$IMAGE"'] if image_shell_var else [shlex.quote(image)]) + [shlex.quote(t) for t in tail]
    return " \\\n  ".join(lines), must_exist, notes


def render_run_script(d, src_file, pin_image=True, ip_map=None, guards=None):
    name = container_name(d)
    stem = stem_of(name)
    if not stem:
        die("容器名不是可识别的 PolarDB-X 组件: %s" % name)
    cmd, must_exist, notes = build_run(d, pin_image, ip_map, image_shell_var=pin_image)
    info = guards or {}
    binds_guard = info.get("binds") or {}
    image = d.get("Image") if pin_image else (d.get("Config") or {}).get("Image")
    out = [
        "#!/usr/bin/env bash",
        "# 根据 %s 生成；包含敏感配置，禁止上传。" % src_file,
    ]
    for n in notes:
        out.append("# 注意: " + n)
    if not guards:
        out.append("# 注意: 未发现 mount-guards.json 对应记录，只校验宿主机路径存在，无法校验设备/inode。")
    out += [
        "set -euo pipefail", "umask 077", "",
        "NAME=%s" % shlex.quote(name),
        "STEM=%s" % shlex.quote(stem),
        "IMAGE=%s" % shlex.quote(image),
        "",
        "# 按逻辑身份检查，防止同一 DN/CN/GMS/CDC 创建重复容器。",
        "mapfile -t matches < <(docker ps -a --format '{{.Names}}' | awk -v s=\"$STEM\" '$0 == s || index($0, s \"-\") == 1 {print}')",
        "if (( ${#matches[@]} > 0 )); then",
        "  printf 'ERROR: 逻辑节点 %s 已存在: %s\\n' \"$STEM\" \"${matches[*]}\" >&2",
        "  exit 1",
        "fi",
        "docker image inspect \"$IMAGE\" >/dev/null || { echo 'ERROR: 原镜像不存在，拒绝改用 latest' >&2; exit 1; }",
        "",
    ]
    if pin_image:
        original_ref = (d.get("Config") or {}).get("Image") or ""
        image_id = d.get("Image") or ""
        # 基线保存的 Image ID 是唯一身份。标签仅在仍解析为该 ID 时可用于容器创建。
        # 选择动作写进脚本，恢复当时再查本机标签，避免 backup->restore 期间 latest 漂移。
        select = [
            "IMAGE_ID=%s" % shlex.quote(image_id),
            "IMAGE_REF=%s" % shlex.quote(original_ref),
            'IMAGE="$IMAGE_ID"',
            'docker image inspect "$IMAGE_ID" >/dev/null || { echo "ERROR: 备份的 Image ID 不存在，拒绝改用 latest" >&2; exit 1; }',
            'if [[ -n "$IMAGE_REF" ]]; then',
            "  if resolved=$(docker image inspect -f '{{.Id}}' \"$IMAGE_REF\" 2>/dev/null); then",
            '    if [[ "$resolved" == "$IMAGE_ID" ]]; then IMAGE="$IMAGE_REF"; fi',
            '  fi',
            'fi',
            'if [[ "$IMAGE" == "$IMAGE_ID" ]]; then',
            '  echo "[image] 原标签不可用或已漂移，使用备份的固定 Image ID"',
            'else',
            '  echo "[image] 原标签与固定 Image ID 匹配，保留原镜像名称: $IMAGE"',
            'fi',
        ]
        out[ out.index("IMAGE=%s" % shlex.quote(image)) : out.index("IMAGE=%s" % shlex.quote(image)) + 1 ] = select
    if must_exist:
        out += ["# 挂载源检查：不可让 docker -v 静默创建空的数据目录。"]
        for path in sorted(set(must_exist)):
            q = shlex.quote(path)
            out.append("[[ -e %s ]] || { echo %s >&2; exit 1; }" %
                       (q, shlex.quote("ERROR: 挂载源不存在: " + path)))
            g = binds_guard.get(path)
            if g:
                out.append("actual=$(stat -Lc '%%d:%%i' -- %s)" % q)
                expect = "%s:%s" % (g["device"], g["inode"])
                out.append("[[ \"$actual\" == %s ]] || { echo %s >&2; exit 1; }" %
                           (shlex.quote(expect), shlex.quote("ERROR: 挂载源设备号/inode 改变: " + path)))
                if g.get("kind") == "dir":
                    out.append("[[ -d %s ]] || { echo %s >&2; exit 1; }" %
                               (q, shlex.quote("ERROR: 原挂载目录类型改变: " + path)))
                    if g.get("nonempty"):
                        out.append("[[ -n $(find %s -mindepth 1 -maxdepth 1 -print -quit) ]] || { echo %s >&2; exit 1; }" %
                                   (q, shlex.quote("ERROR: 原非空挂载目录现在为空: " + path)))
                elif g.get("kind") == "file":
                    out.append("[[ -f %s ]] || { echo %s >&2; exit 1; }" %
                               (q, shlex.quote("ERROR: 原挂载文件类型改变: " + path)))
    for volume in info.get("volumes") or []:
        out.append("docker volume inspect %s >/dev/null || { echo %s >&2; exit 1; }" %
                   (shlex.quote(volume), shlex.quote("ERROR: 原 Docker volume 不存在: " + volume)))
    out.append("")
    out += [cmd, "", "echo \"已创建 $NAME\""]
    return "\n".join(out) + "\n"


def render_restore_all(entries):
    entries = sorted(entries, key=lambda e: (ROLE_ORDER.index(e[0]), e[1]))
    out = [
        "#!/usr/bin/env bash",
        "# GMS -> DN -> CN -> CDC；只会创建缺失的容器，不会删除任何容器。",
        "set -euo pipefail", "cd \"$(dirname \"$0\")\"", "",
        "restore() {",
        "  local name=\"$1\" stem=\"$2\" wait=\"$3\"",
        "  local -a matches",
        "  mapfile -t matches < <(docker ps -a --format '{{.Names}}' | awk -v s=\"$stem\" '$0 == s || index($0, s \"-\") == 1 {print}')",
        "  if (( ${#matches[@]} > 1 )); then echo \"[ERROR] 逻辑节点重复: $stem: ${matches[*]}\" >&2; exit 1; fi",
        "  if (( ${#matches[@]} == 1 )); then",
        "    if [[ $(docker inspect -f '{{.State.Running}}' \"${matches[0]}\") != 'true' ]]; then",
        "      echo \"[ERROR] 容器存在但未运行: ${matches[0]}；请检查或手动 docker start\" >&2; exit 1",
        "    fi",
        "    echo \"[skip] 已运行: ${matches[0]}\"; return 0",
        "  fi",
        "  echo \"[run] $name\"",
        "  bash \"./$name.run.sh\"",
        "  echo \"[wait] ${wait}s\"; sleep \"$wait\"",
        "  if [[ $(docker inspect -f '{{.State.Running}}' \"$name\") != 'true' ]]; then",
        "    echo \"[ERROR] 新容器启动后未运行: $name\" >&2; exit 1",
        "  fi",
        "}", "",
    ]
    for role, name in entries:
        out.append("restore %s %s %s  # %s" %
                   (shlex.quote(name), shlex.quote(stem_of(name)), ROLE_WAIT[role], role))
    out += ["", "docker ps --format 'table {{.Names}}\\t{{.Status}}'"]
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------- 子命令

def emit_all(out_dir, inspects, pin_image, ip_map=None, guards=None):
    """inspects: [(文件名, dict)]；写 run.sh 和 restore-all.sh，打印汇总。"""
    entries, rows = [], []
    ensure_unique([container_name(d) for _, d in inspects])
    for src_file, d in inspects:
        name = container_name(d)
        c = d.get("Config") or {}
        role = detect_role(name, c.get("Image"))
        script = render_run_script(d, src_file, pin_image, ip_map, (guards or {}).get(name))
        rot = save_with_rotation(os.path.join(out_dir, name + ".run.sh"), script, 0o700)
        entries.append((role, name))
        shown = [apply_ip_map(e, ip_map)[0] for e in (c.get("Env") or [])]   # 脚本里实际会用到的 IP
        rows.append((role, name, ", ".join(find_ips(shown)) or "-", entrypoint_desc(c.get("Entrypoint")), rot))
    rot = save_with_rotation(os.path.join(out_dir, "restore-all.sh"), render_restore_all(entries), 0o700)

    rows.sort(key=lambda r: (ROLE_ORDER.index(r[0]), r[1]))
    print("\n%-6s %-32s %-16s %s" % ("角色", "容器名", "env 中的 IP", "Entrypoint"))
    for role, name, ips, ep, r in rows:
        print("%-6s %-32s %-16s %s" % (role, name, ips, ep))
        if r:
            print("       (旧的 run.sh 已另存为 %s)" % os.path.basename(r))
    if ip_map:
        print("\n已按 --ip-map 替换环境变量里的 IP: %s" % ", ".join("%s → %s" % p for p in ip_map))
        print("（只改环境变量；挂载路径和 hostname 不变。上表为替换后的 IP）")
    print("\n输出目录: %s" % out_dir)
    print("整体恢复: bash %s" % os.path.join(out_dir, "restore-all.sh"))
    if rot:
        print("(旧的 restore-all.sh 已另存为 %s)" % os.path.basename(rot))


def cmd_backup(args):
    ip_map = parse_ip_map(args.ip_map)
    out_dir = os.path.expanduser(args.dir)
    os.makedirs(out_dir, exist_ok=True)
    os.chmod(out_dir, 0o700)

    names = docker_names(args.prefix)
    if not names:
        die("没有找到容器（prefix=%r）。可用 --prefix 指定集群名，例如 --prefix pxc_single" % args.prefix)

    # 仅供参考的快照（Status 列每次都不同），直接覆盖，不做 .bak 轮转
    write_file(os.path.join(out_dir, "docker-ps.txt"),
               run_docker("ps", "-a", "--no-trunc").stdout, 0o600)

    inspects = []
    guards = {}
    for n in sorted(names):
        raw = run_docker("inspect", n).stdout
        try:
            d = json.loads(raw)[0]
        except (ValueError, IndexError):
            die("无法解析 docker inspect %s 的输出" % n)
        fname = container_name(d) + INSPECT_SUFFIX
        rot = save_with_rotation(os.path.join(out_dir, fname), raw, 0o600)
        if rot:
            print("(%s 内容有变化，旧版本已另存为 %s)" % (fname, os.path.basename(rot)))
        inspects.append((fname, d))
        guards[container_name(d)] = collect_guards(d)
    manifest = {"version": 2, "prefix": args.prefix, "guards": guards}
    save_with_rotation(os.path.join(out_dir, MANIFEST), json.dumps(manifest, indent=2, ensure_ascii=False), 0o600)
    emit_all(out_dir, inspects, args.pin_image, ip_map, guards)


def cmd_generate(args):
    ip_map = parse_ip_map(args.ip_map)
    out_dir = os.path.expanduser(args.dir)
    files = sorted(glob.glob(os.path.join(out_dir, "*" + INSPECT_SUFFIX)))
    if not files:
        die("%s 下没有 *%s 文件，请先执行 backup" % (out_dir, INSPECT_SUFFIX))
    man = load_manifest(out_dir)
    emit_all(out_dir, [(os.path.basename(f), load_inspect(f)) for f in files],
             args.pin_image, ip_map, man.get("guards"))


def _view(d):
    c, h = d.get("Config") or {}, d.get("HostConfig") or {}
    return {
        "镜像 ID": d.get("Image") or c.get("Image"),
        "启动进程参数": list(c.get("Entrypoint") or []) + list(c.get("Cmd") or []),
        "端口绑定": h.get("PortBindings") or {},
        "Docker 网络": sorted(((d.get("NetworkSettings") or {}).get("Networks") or {}).keys()),
        "网络模式": h.get("NetworkMode"),
        "privileged": bool(h.get("Privileged")),
        "内存限制": h.get("Memory") or 0,
        "重启策略": (h.get("RestartPolicy") or {}).get("Name") or "no",
        "用户": c.get("User") or "",
        "WorkingDir": c.get("WorkingDir") or "",
        "挂载": sorted((m.get("Source") or m.get("Name") or "", m.get("Destination") or "",
                      bool(m.get("RW", True))) for m in (d.get("Mounts") or [])),
    }


def _env(d, ip_map=None):
    env = {}
    for e in (d.get("Config") or {}).get("Env") or []:
        k, _, v = e.partition("=")
        env[k] = apply_ip_map(v, ip_map)[0]
    return env


def cmd_verify(args):
    """支持原完整名称或逻辑名称，例如 pxc_single-dn-0。"""
    ip_map = parse_ip_map(args.ip_map)
    out_dir = os.path.expanduser(args.dir)
    inspect_files = glob.glob(os.path.join(out_dir, "*" + INSPECT_SUFFIX))
    options = []
    for f in inspect_files:
        n = os.path.basename(f)[:-len(INSPECT_SUFFIX)]
        if n == args.name or stem_of(n) == args.name:
            options.append(f)
    if len(options) != 1:
        die("verify 在备份中找到 %d 个匹配 %s 的原容器，必须唯一" % (len(options), args.name))
    old_path = options[0]
    old = load_inspect(old_path)
    stem = stem_of(container_name(old))
    p = run_docker("ps", "-a", "--format", "{{.Names}}")
    current = [n for n in p.stdout.splitlines() if stem_of(n) == stem]
    if len(current) != 1:
        die("verify 在当前 docker ps -a 中找到 %d 个逻辑节点 %s，必须唯一" % (len(current), stem))
    current_name = current[0]
    p = run_docker("inspect", current_name)
    new = json.loads(p.stdout)[0]
    print("对比逻辑节点 %s：原容器=%s，当前=%s" % (stem, container_name(old), current_name))

    ok = True
    st = new.get("State") or {}
    if st.get("Running"):
        print("[ok  ] 运行状态: Running")
    else:
        ok = False
        print("[差异] 运行状态: %s (ExitCode=%s)，请查看 docker logs %s" %
              (st.get("Status"), st.get("ExitCode"), current_name))
    # Config.Image 只是 docker run 使用的引用：旧容器可能是 :latest，
    # 恢复时为避免版本漂移而使用 sha256:<ID>。只有容器的 .Image 才能比较镜像身份。
    old_ref = (old.get("Config") or {}).get("Image")
    new_ref = (new.get("Config") or {}).get("Image")
    if old_ref != new_ref and old.get("Image") == new.get("Image"):
        print("[提示] 镜像启动引用不同（原镜像标签 → 固定 Image ID），实际镜像 ID 相同；不视为配置差异")
    if old.get("Image") != new.get("Image"):
        ok = False
        print("[差异] 实际镜像 ID 与原容器不同，不能确认二进制版本一致")
    old_ep = (old.get("Config") or {}).get("Entrypoint")
    new_ep = (new.get("Config") or {}).get("Entrypoint")
    old_cmd = (old.get("Config") or {}).get("Cmd")
    new_cmd = (new.get("Config") or {}).get("Cmd")
    if old_ep != new_ep or old_cmd != new_cmd:
        print("[提示] Entrypoint/Cmd 字段分配不同。将按最终 argv 判断是否相同（CN 多参数 Entrypoint 可能产生此差异）")

    # .Path + .Args 是 Docker 记录的最终启动命令，能辅助验证 Entrypoint/Cmd 重排。
    if old.get("Path") and new.get("Path") and (
        old.get("Path"), old.get("Args") or []
    ) != (new.get("Path"), new.get("Args") or []):
        ok = False
        print("[差异] Docker 记录的实际启动命令（Path/Args）不同；请人工核对 entrypoint")

    vo, vn = _view(old), _view(new)
    for k in vo:
        if vo[k] == vn[k]:
            continue
        ok = False
        print("[差异] %s" % k)
        if k == "启动进程参数":
            print("   有效启动参数不同（可能含敏感信息，此处不直接显示）。")
        elif k == "挂载":
            so, sn = set(vo[k]), set(vn[k])
            for x in sorted(so - sn):
                print("   仅旧:", x)
            for x in sorted(sn - so):
                print("   仅新:", x)
        else:
            print("   旧:", vo[k])
            print("   新:", vn[k])

    eo, en = _env(old, ip_map), _env(new)   # --ip-map 只作用在保存的旧配置上，再与新容器比较
    only_old, only_new = sorted(set(eo) - set(en)), sorted(set(en) - set(eo))
    changed = sorted(k for k in set(eo) & set(en) if eo[k] != en[k])
    if only_old or only_new or changed:
        ok = False
        print("[差异] 环境变量（只显示变量名，不显示值）")
        if only_old:
            print("   仅旧:", ", ".join(only_old))
        if only_new:
            print("   仅新:", ", ".join(only_new))
        if changed:
            print("   值不同:", ", ".join(changed))

    print("配置一致 ✔" if ok else "存在差异 ✘")
    sys.exit(0 if ok else 1)


def cmd_restore(args):
    """从备份恢复指定逻辑组件；不删除原容器，不依赖完整随机容器名。"""
    out_dir = os.path.expanduser(args.dir)
    man = load_manifest(out_dir)
    prefix = args.prefix or man.get("prefix")
    if not prefix:
        die("restore 需要 --prefix；或事先 backup --prefix 保存集群名")
    files = sorted(glob.glob(os.path.join(out_dir, "*" + INSPECT_SUFFIX)))
    if not files:
        die("找不到 inspect 备份，删除容器后不能从 docker ps 反推启动参数")
    old = [(os.path.basename(f), load_inspect(f)) for f in files]
    ensure_unique([container_name(d) for _, d in old])
    choices = []
    for fname, d in old:
        name = container_name(d)
        if not matches_cluster(name, prefix):
            continue
        stem = stem_of(name)
        cpt = stem[len(prefix) + 1:]
        if args.component == "all" or cpt == args.component or (args.component == "dn" and cpt.startswith("dn-")):
            choices.append((detect_role(name, (d.get("Config") or {}).get("Image")), name, stem))
    if not choices:
        die("没有找到组件 %s 对应的 inspect 备份" % args.component)
    if args.component == "dn" and len(choices) != 1:
        die("匹配多个 DN，请用 --component dn-0 / dn-1 明确指定")
    choices.sort(key=lambda x: (ROLE_ORDER.index(x[0]), x[1]))
    for role, name, stem in choices:
        current_names = docker_names(prefix)
        matches = [n for n in current_names if stem_of(n) == stem]
        if matches:
            p = run_docker("inspect", "-f", "{{.State.Running}}", matches[0])
            if p.stdout.strip() == "true":
                print("[skip] %s 已在运行: %s" % (stem, matches[0]))
                continue
            die("%s 存在但已停止 (%s)。不要覆盖，请先排查，必要时 docker start" % (stem, matches[0]))
        script = os.path.join(out_dir, name + ".run.sh")
        if not os.path.isfile(script):
            die("缺少恢复脚本 %s；请执行 generate" % script)
        print("[restore%s] %s -> %s（来自原 inspect）" %
              (" dry-run" if args.dry_run else "", stem, name))
        if args.dry_run:
            continue
        p = subprocess.run(["bash", script], cwd=out_dir)
        if p.returncode:
            die("恢复脚本退出码 %s: %s" % (p.returncode, script))
        print("[created] %s，仍需运行 verify 和数据库验收" % name)


def main():
    ap = argparse.ArgumentParser(description="从 docker inspect 生成 PolarDB-X (pxd) 容器恢复脚本")
    sub = ap.add_subparsers(dest="cmd")
    sub.required = True

    def ip_arg(p):
        p.add_argument("--ip-map", action="append", default=[], metavar="旧IP=新IP",
                       help="显式替换环境变量里的 IP，可重复使用；不指定就原样使用 inspect.json 里的 IP"
                            "（本工具不会自动探测本机 IP）")

    def common(p):
        p.add_argument("--dir", default=DEFAULT_DIR, help="工作目录，默认 %s" % DEFAULT_DIR)
        p.add_argument("--pin-image", action="store_true", default=True,
                       help="固定镜像 ID（默认启用，避免 latest 漂移）")
        p.add_argument("--use-tag", dest="pin_image", action="store_false",
                       help="不推荐：使用原镜像标签而不是 ID")
        ip_arg(p)

    p = sub.add_parser("backup", help="保存 inspect.json 并生成 run.sh / restore-all.sh")
    common(p)
    p.add_argument("--prefix", default="", help="容器名前缀（集群名），默认按镜像名含 polardbx 匹配")
    p.set_defaults(func=cmd_backup)

    p = sub.add_parser("generate", help="根据已有的 inspect.json 重新生成脚本（不需要容器还在）")
    common(p)
    p.set_defaults(func=cmd_generate)

    p = sub.add_parser("verify", help="对比重建后的容器与保存的 inspect.json")
    p.add_argument("name", help="完整容器名或逻辑身份（如 pxc_single-dn-0）")
    p.add_argument("--dir", default=DEFAULT_DIR)
    ip_arg(p)
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("restore", help="仅创建不存在的逻辑组件；不会自动 docker rm")
    p.add_argument("--dir", default=DEFAULT_DIR)
    p.add_argument("--prefix", default="", help="集群名前缀，默认从备份的 mount-guards.json 读取")
    p.add_argument("--component", default="all", help="all/gms/dn/dn-0/cn/cdc")
    p.add_argument("--dry-run", action="store_true", help="只展示准备恢复的逻辑节点，不创建")
    p.set_defaults(func=cmd_restore)

    args = ap.parse_args()
    if args.cmd == "restore" and args.component not in ("all", "dn") and not COMPONENT_RE.fullmatch(args.component):
        die("--component 只能是 all/gms/dn/dn-0/cn/cdc 等")
    args.func(args)


if __name__ == "__main__":
    main()
