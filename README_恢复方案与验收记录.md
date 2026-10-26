# PolarDB-X单宿主机四组件容器恢复与 PXD ID 同步方案

## 1. 应用场景
使用 PXD 安装PolarDB-X（pxd版本0.7.4），单宿主机四组件容器运行条件下，如果不小心删除了容器，导致数据库无法访问，如下教程可以实现恢复容器的重建，恢复数据库的正常访问且不丢失数据库表。
容器重新建立后，PXD管理的容器ID会变更，导致pxd命令失效，需要把新容器ID同步至PXD，是否同步容器ID不影响数据库使用（可选）。

### 已观察到且不构成恢复失败的现象
- `docker ps` 的 IMAGE 显示 `f6bb1f4f5288`、`c2c97119763f`、`7336bf20d3ae` 等短 Image ID： 实际 `.Image` 与原始镜像一致，只是 `.Config.Image` 引用名称不同。
- GMS 刚启动后第一次 `SHOW STORAGE` 出现 `ERR_GMS_GET_CONNECTION`/认证超时，随后重试成功、`IS_HEALTHY=true`：恢复时须等待数据库**真正就绪**，不能只依赖 Docker `Up`。
## 2. 工具目录说明
| 文件 | 作用 |
|---|---|
| `pxd_recovery.py` | 统一命令入口：备份、计划、非 CDC 重建、CDC 阶段恢复、PXD 同步 |
| `pxd_core.py` | 重放 Docker inspect、镜像标签身份校验、挂载 guard、配置 verify |
| `pxd_cdc.py` | CDC 冷备份文件校验、创建/回填/启动三阶段状态机 |
| `pxd_registry.py` | **PXD 0.7.4 SQLite** 的 `container_id` 对账与事务同步，支持原有 10/12/64 位 ID |
| `pxd_health.py` | SQL/容器快照及 diff 辅助验证 |
| `pxd_images.py` | 通过完整 Image ID 导出/校验 Docker 镜像归档 |
| `test_pxd_*.py` | 离线回归测试，无须在真实 Docker 集群做破坏性实验 |
|  |  |
## 3. 不可违反的安全边界

1. **绝不执行 `pxd delete pxc_single` 做验证**：PXD 0.7.4 此命令可能在 Docker 旧容器 ID 已失效时仍删除 DN/GMS 本地数据目录及管理登记。`pxd delete` 是整个集群销毁动作。
2.  DN/GMS 的 `/data/mysql` 是宿主机 bind mount；**inspect 不是 DN/GMS 数据备份**。不要删除 `~/.pxd/data/polarx/...`，重要数据库还需独立数据库备份、磁盘快照及恢复演练。挂载目录设备/inode guard 防止误映射新空目录。
3. CDC 本次 `/home/admin/binlog` 和 `/home/admin/logs/rocksdb` 位于容器可写层，**没有 CDC 冷备份，删除容器就可能永久丢失历史 binlog 与状态**。这通常不会直接删除 DN 中表/表数据，但可能造成下游复制丢数据、必须全量重新同步。
5. CDC 不允许直接调用 `recover --component cdc`、`restore-all.sh` 或 CDC 的 `.run.sh`。须遵循 **停机冷备份 → 核对 → 删除 → 创建但不启动 → 回填 → 启动 → 验证**。
6. `pxd-sync` 只更新管理数据库 `container.container_id`；**不会重建已被 `pxd delete` 清除的集群元数据**、不会修正网络 IP、不适用于多机多 DN。不匹配会拒绝写入；默认只读，写入必须 `--apply --confirm <prefix>` 并自动进行 SQLite 备份。
7. inspect/env/run.sh/manifest 可能含密码、地址及内部部署信息，备份目录建议 `chmod 700`，不可直接公开上传。CDC 冷备份、镜像归档可能体积很大。

## 4. 事前准备：每一个恢复周期重新建立完整基线

**成功恢复四个容器，其 ID 全部改变。原有 `~/pxd-recover-newcluster/` 是上次恢复实验的历史基线，不应反复覆盖。**
下一次要用 `--inspect-dir` 做 PXD 身份双重校验时，原始 inspect 必须来自**下一次容器删除之前的那一代容器**。
**建议现在额外创建新目录（不覆盖旧目录）：**
```bash
cd ~/pxd-recover
APP="$HOME/pxd-recover/pxd_recovery.py"
PREFIX=pxc_single
HOST=10.35.18.43
NEXT="$HOME/pxd-recover-baseline-posttest-20261023"

# 请先确认 NEXT 不存在，不要重复在已使用的目录中执行
if [ -e "$NEXT" ]; then echo "ERROR: 基线目录已存在，换一个目录"; else
  umask 077
  mkdir -m 700 "$NEXT"
  python3 "$APP" backup --prefix "$PREFIX" --dir "$NEXT"
fi

python3 "$APP" pxd-check --prefix "$PREFIX" --pxd-host "$HOST" --inspect-dir "$NEXT"
```
上述只保存 inspect、挂载 guard、恢复脚本；若希望断网也能恢复精确镜像，另执行 `export-images`（需要空间）；**这些操作本身还没有备份 CDC 可写层，也没有复制 DN/GMS 数据。**

```bash
# 断网下可执行
python3 "$APP" export-images --prefix "$PREFIX" --dir "$NEXT"
python3 "$APP" verify-images --prefix "$PREFIX" --dir "$NEXT"
```
另外，维护 `~/.pxd/polardbx.db` 的独立、一致性 SQLite 备份，备份时优先用 SQLite Backup API，不要在写入期间随意拷贝裸数据库文件。保留上次 `pxd-sync` 生成的自动备份。

## 5. 单组件 DN / GMS / CN 的误删恢复（通用流程）

以下命令示例使用**将要发生事故之前建立的基线目录**，`dn-0` 可替换为 `gms` 或 `cn`。不要未经确认复制删除命令去操作正在服务的容器。
```bash
cd ~/pxd-recover
APP="$HOME/pxd-recover/pxd_recovery.py"
PREFIX=pxc_single
HOST=10.35.18.43
BASE="$HOME/pxd-recover-baseline-posttest-20261023"
ROLE=dn-0                           # 只选一个：gms / dn-0 / cn

python3 "$APP" plan --prefix "$PREFIX" --dir "$BASE"
python3 "$APP" recover --prefix "$PREFIX" --component "$ROLE" --dir "$BASE" --dry-run
```
**删除前**：记录原容器 ID、`docker inspect` 挂载位置及相应数据库校验信息。DN/GMS 须确认 `/data/mysql` 是正确、已有数据的 bind mount；有重要数据时应另行独立备份。若容器仍存在但可用，不要为重建而重建。
-----------------模拟容器删除 BEGIN----------------------
如确实需要移除某个容器，必须人工确认完整实际容器名；示例只展示停机流程，**不放入自动化脚本**：
```bash
NAME="pxc_single-dn-0-Cand-15771" # 示例！未来部署随机后缀可能变化
# 手动确认 NAME 与实际待恢复的容器一一对应后：
docker inspect "$NAME" --format '{{.Id}} {{.Name}} {{.State.Status}}'
docker stop -t 120 "$NAME"
docker inspect "$NAME" -f '{{.State.Status}}'   # 预期 exited
# 仅在完全确认备份及数据挂载安全时，人工执行：docker rm "$NAME"
```
-----------------模拟容器删除 END----------------------
删除后按顺序：
```bash
python3 "$APP" recover --prefix "$PREFIX" --component "$ROLE" --dir "$BASE"
python3 "$APP" verify "$PREFIX-$ROLE" --dir "$BASE"

# 等数据库真正健康后，通过 CN 执行：
mysql -h "$HOST" -P 62261 -u polardbx_root -p \
  -e 'SELECT 1; SHOW DATABASES; SHOW STORAGE;'
```
对 DN/GMS，建议前后比较 `server_uuid`、`auto.cnf` inode、`ibdata1` inode；文件 inode 一致只能证明特定文件身份延续，不能替代完整业务备份校验。

**接着单独处理 PXD ID（先只读）：**
```bash
python3 "$APP" pxd-check --prefix "$PREFIX" --pxd-host "$HOST" --inspect-dir "$BASE"
python3 "$APP" pxd-sync  --prefix "$PREFIX" --pxd-host "$HOST" --inspect-dir "$BASE"

# 只有确认恰好需要更新刚恢复的那个容器，且 SQL/数据验证通过后才执行：
python3 "$APP" pxd-sync --prefix "$PREFIX" --pxd-host "$HOST" \
  --inspect-dir "$BASE" --backup-dir "$HOME/pxd-test-baseline" \
  --apply --confirm "$PREFIX"

python3 "$APP" pxd-check --prefix "$PREFIX" --pxd-host "$HOST" --inspect-dir "$BASE"
/home/randy/software/polardbx-venvs/pxd/bin/pxd list
```

预期最后 0 条待更新、四条一致。同步不会更换 DN/GMS 数据路径。

## 6. CDC 冷备份与误删恢复（分阶段）(可选执行)

### 6.1 事故之前：为当前 CDC 建立冷备份

```bash
APP="$HOME/pxd-recover/pxd_recovery.py"
PREFIX=pxc_single
BASE="$HOME/pxd-recover-baseline-posttest-20261023" # 必须是当前 CDC 的 inspect
CDC=pxc_single-cdc-vHxu                # 示例：实际以本次 Docker 名称为准
COLD="$BASE/cdc-cold-backup-01"       # 必须是不存在的目录

# 记录 CDC 日志位置与文件（可选但推荐）
mysql -h 10.35.18.43 -P 62261 -u polardbx_root -p \
  -e 'SHOW MASTER STATUS; SHOW BINARY LOGS;'

docker inspect "$CDC" --format '{{.Id}} {{.Name}} {{.State.Status}}'
docker stop -t 120 "$CDC"
docker inspect "$CDC" --format 'Status={{.State.Status}} ExitCode={{.State.ExitCode}} OOMKilled={{.State.OOMKilled}}'

# 只在 exited、非 OOM/137、与基线 ID 相同的条件下执行：
python3 "$APP" cdc-backup --prefix "$PREFIX" --dir "$BASE" --backup-dir "$COLD"
python3 "$APP" cdc-check  --prefix "$PREFIX" --dir "$BASE" --backup-dir "$COLD"

# 若只是“制作事前冷备份”，检查通过后恢复同一个 CDC；不要 docker rm。
docker start "$CDC"
```

确认 CDC 健康及 SQL binlog 可读；`cdc-check` 通过证明复制文件/校验清单一致，不直接证明下游复制已无缺口。CDC `/home/admin` 以外的有状态数据仍需人工确认。冷备份目录和 inspect 对应的原容器 ID 应匹配。

### 6.2 CDC 已被误删：使用配套冷备份恢复

前提：第 6.1 节的 CDC 冷备份已经存在、源容器已确认删除，并且保存的 `inspect.json` 正确。**不要删除尚存在且正在运行的容器**；这不是为当前健康容器强行重新恢复的命令。

```bash
APP="$HOME/pxd-recover/pxd_recovery.py"
PREFIX=pxc_single
HOST=10.35.18.43
BASE="$HOME/pxd-recover-baseline-posttest-20261023"
COLD="$BASE/cdc-cold-backup-01"

# 如果旧容器尚在，先执行 cdc-check；如果已经误删，不要再运行要求源容器存在的 cdc-check。
# 创建（尚不启动）→ 回填可写层 → 启动：严格按顺序执行
python3 "$APP" cdc-create       --prefix "$PREFIX" --dir "$BASE" --backup-dir "$COLD"
python3 "$APP" cdc-restore-data --prefix "$PREFIX" --dir "$BASE" --backup-dir "$COLD"
python3 "$APP" cdc-start        --prefix "$PREFIX" --dir "$BASE" --backup-dir "$COLD"

python3 "$APP" verify "$PREFIX-cdc" --dir "$BASE"
python3 "$APP" cdc-verify --prefix "$PREFIX" --dir "$BASE" --backup-dir "$COLD"
mysql -h "$HOST" -P 62261 -u polardbx_root -p \
  -e "SHOW MASTER STATUS; SHOW BINARY LOGS; SHOW BINLOG EVENTS IN 'binlog.000001' LIMIT 10;"
```

注意：以上 `SHOW BINLOG EVENTS` 文件名适用于本次环境，其他集群请使用 `SHOW BINARY LOGS` 实测的文件名。`SHOW MASTER STATUS` Position 随新事务/心跳可能增长；历史文件**前缀 SHA256 一致**才是已保存前缀内容一致的证据。还需检查 CDC 日志、下游消费/位点延续。
### 6.3 如果没有 CDC 冷备份
不要声称 CDC 历史 binlog 可从 inspect 恢复。直接删除无冷备份的 CDC 容器可能永久丢失历史 binlog/RocksDB 和复制进度；通常不会直接删除 DN 中的业务表记录，但下游 CDC 消费连续性可能受到影响，可能需要全量重建订阅。具体修复策略取决于该 PXD CDC 部署和下游系统，不能默认无损。
## 7. 镜像名称优化的工作原理
`pxd_core.py` 所生成的 GMS/DN/CN `.run.sh` 在容器**创建时**先校验备份 `Image ID` 是否存在，再检查备份的原始镜像标签（如 `polardbx/polardbx-sql:latest`）是否解析到**同一完整 Image ID**：

- 相同：使用原标签运行，`docker ps` 以后能显示标签。
- 不同或标签不存在：使用固定 `sha256:` ID；禁止悄悄使用已漂移的 `latest`。
- 固定 ID 不存在：拒绝恢复，需先恢复镜像归档。
## 8. 离线验证及常用命令
```bash
cd ~/pxd-recover
python3 -m py_compile pxd_recovery.py pxd_core.py pxd_registry.py pxd_cdc.py pxd_health.py pxd_images.py
python3 -m unittest discover -p 'test_pxd_*.py' -q
python3 pxd_recovery.py --help

# 核查 PXD 与四个容器的一致性（只读）
python3 pxd_recovery.py pxd-check --prefix pxc_single --pxd-host 10.35.18.43 \
  --inspect-dir "$HOME/pxd-recover-baseline-posttest-20261023"
```
## 9. 本工具未覆盖的场景
- `pxd delete` 已经删除集群行、宿主机 DN/GMS 数据目录已丢失、原数据库物理数据损坏：不能仅靠 `pxd-sync` 修复；需要从独立数据库/磁盘备份恢复。
- 跨宿主机部署、网络 IP 已改变、多个 DN/多 CN、不同 PXD SQLite schema：当前同步流程会拒绝或不支持。

**最终原则：先保存正确的容器身份与数据基线，恢复容器并验证业务，再安全同步 PXD ID；CDC 必须保护可写层，DN/GMS 必须保护宿主机数据目录。**
