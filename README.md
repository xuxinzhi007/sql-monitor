# SQL Monitor

本地 SQL 日志监控与独立日志排查工具，使用 Go 标准库实现，页面通过 `go:embed` 内嵌。SQL 采集只读，默认关注慢查询与 SQL 错误，支持重复模板聚合、耗时、代码位置与有效 trace 筛选。SQL 事件最近最多 **2,000 条滚动内存**；日志排查仅保存用户显式导入的临时副本。无 SQLite 持久化、不连接业务数据库、不执行 SQL。

## 实时监控三个入口

启动命令不变。首页是一次调用的周期；SQL 聚合在 `/sql`，日志文件导入在 `/logs`。`/overview` 与 `/` 打开同一页。

- **调用**（`/`、`/overview`、`/requests`）：一行一次接口调用。点开后按周期看：谁触发（uid / tenant）、这次调用、中间的 SQL、结果耗时。有 trace / span 时按 span 归属 SQL。本地没有 trace 时，只保留代码路径对上该服务的 SQL，例如 `TagService` 只保留 `tag.go`，不把同时段的 `episode.go` 算进来。
- **日志**（`/live`）：没有请求身份的行，例如锁、后台轮询。点开只说明它不是一次调用，不把空的 trace / span 当成请求。有 trace 的行会回到对应的调用周期。

普通日志与 SQL 独立保留：最多 2,000 条普通日志、字符串内容预算 8 MiB，先到上限先淘汰；原文和消息各最多 64 KiB。超长行保留前缀并标记截断，不再完全静默丢弃。实时日志列表只显示接口操作与耗时；详情把 Kratos JSON 日志和 `resp=` / `req=` 里的 JSON 缩进展开，并把常见 Unix 秒、毫秒和 `YYYYMMDD` 标成可读时间。累计行数/级别与截断、淘汰计数独立于保留窗口；筛选结果、异常分组、请求追溯仅基于当前保留内容。文件起始截断、重启、轮转、保留淘汰都可能造成边界缺失；没有结束记录不等于卡住，日志间隔不等于某方法耗时，代码位置不等于调用栈。

页面须先确认“查看本地日志”才请求 `/api/live?raw=1`；普通消息、请求/响应体与原文可能含敏感信息，并非通用脱敏结果。隐藏内容会停止原文拉取并清除页面快照；后端仍按容量保留。新 API 只支持显式 GET，保留 loopback Host/Origin 检查与 `no-store`，最多两个并发快照读取。不要公开页面或分享原文。

本次未新增接口执行客户端，未修改 data-service；飞书仍只发送原有 SQL 异常汇总，**不会将普通业务日志/请求正文发送到飞书**。更准确的阶段与方法耗时需要业务已有 span 或明确阶段开始/结束记录。

## 第一次使用：先用真实 SQL 验证

需要 Go 1.24+。工具本身无外部依赖；独立演示项目使用 GORM + SQLite **内存数据库**，需要 CGO 和本机 C 编译器。以下命令在 `sql-monitor` 目录执行：

```bash
go build -o sql-monitor ./cmd/sql-monitor
(cd examples/gorm-demo && CGO_ENABLED=1 go run .) | ./sql-monitor --stdin
```

打开 **http://127.0.0.1:8767/sql**。演示会实际执行并校验 CREATE、3 次参数化 INSERT、6 次参数化 SELECT，以及 1 次预期的不存在表错误。使用标准 GORM logger（stdout、关闭颜色），没有伪造日志；1ns 慢查询阈值仅用于演示，让“值得关注”确定有内容，不代表数据库存在性能问题。

演示结束后，页面显示 **输入 EOF**，仍保留采集结果。监控的启动说明写入 stderr，不往 stdout 回显 SQL。按 Ctrl+C 退出监控；重新运行管道才能读取新输入。

1. 默认“值得关注”可查看慢查询与预期 SQL 错误。
2. 切换“重复查询”，同一位置的 6 次 SELECT 应合并，参数隐藏为 `?`。
3. 点击详情看耗时、代码位置；标准 GORM 不含请求 trace，因此显示“未关联”。
4. 只有显式点击“显示选中事件原文”，才请求原始 SQL / 错误。

只想跟随文件，可另开终端：

```bash
# 在工具目录；演示只使用本机内存数据库
(cd examples/gorm-demo && CGO_ENABLED=1 go run .) > /tmp/sql-monitor-demo.log
./sql-monitor --log /tmp/sql-monitor-demo.log
```

`go run` 的依赖首次使用可能需要下载。依赖已缓存时可加 `GOPROXY=off`；本项目已使用离线缓存验证。

## 飞书 SQL 汇总通知

不改变 SQL 采集、页面筛选和 2,000 条滚动保留。通知为可选后台功能，普通 `go run ./cmd/sql-monitor --log ...` 不发送消息。

在本目录创建私有配置后运行：

```bash
cp -n feishu.example.json sql-monitor.local.json
chmod 600 sql-monitor.local.json
# 编辑 feishu.app_id、app_secret、recipients，并将 enabled 改为 true
# 如果已有 sql-monitor.local.json，直接编辑，勿覆盖
go run ./cmd/sql-monitor --config sql-monitor.local.json
```

可用 `feishu test` 子命令验证配置与发送链路（替代独立的 `feishu-message/send.py`）：

```bash
go run ./cmd/sql-monitor feishu test --config sql-monitor.local.json --check   # 仅离线校验，不联网不发送
go run ./cmd/sql-monitor feishu test --config sql-monitor.local.json          # 向每个收件人发送一条测试消息
```

启动时 stderr 会明确显示飞书通知的开关状态：未提供含 `feishu.enabled=true` 的 `--config` 时打印关闭原因，避免“静默不发送”被误认为故障。常见错误码：`10014` App Secret 无效、`230013` 机器人对该用户不可用、`99991672` 缺少消息发送权限。

- `feishu.recipients`：1–20 个 `{ "name": "备注名", "email": "企业邮箱" }`，每人独立收到同一张卡片；不使用姓名查询账号。账号必须在应用可用范围内，应用需要机器人消息发送权限，不需要通讯录查询权限。
- `interval_seconds`：10–86400 秒，示例为 300 秒；`slow_ms`：正数毫秒阈值，示例为 200。此阈值仅用于通知，不改变页面原有慢查询筛选。
- `project`：卡片标题中的项目/环境名称。配置变更需重启进程生效；配置文件包含凭证时拒绝组/其他用户可访问的权限。默认本地配置文件名已加入本仓库 Git 忽略，其他路径需自行保护。
- 按**采集时间窗口**累计成功解析的全部 SQL，错误与慢查询允许重叠；缺少耗时的事件不参与慢查询判断。不是请求耗时、数据库全量统计，也不是事件发生时间统计。
- 有错误或达到慢阈值才发送；同一窗口只发送一份汇总给每个收件人，持续异常会在后续窗口再次汇总。无异常/无输入不发，不能据此判断服务正常。
- 卡片包含总数、错误数、慢查询数，以及按模板指纹和代码位置分组的异常 Top 5；按错误数、慢查询数、异常次数、最大已知耗时降序排列。发送项目名、模板指纹和截断的代码位置，不发送 SQL 正文、参数、trace、原始错误或导入日志。没有已知耗时显示“未知”。
- 周期计数独立于 2,000 条页面保留；每周期最多保留 1,000 个异常分组。超限异常仍计入总数，卡片明确显示未纳入明细的条数，此时排名不完整。
- 示例的 `initial_bytes: 0` 从现有文件末尾开始，避免首次启动把旧 SQL 当作新通知；若改为正数，首次读入的历史尾部也计入当前采集窗口。文件被截断/轮转后重新读取的内容也可能重复统计；stdin 无法区分历史回放与实时输入。
- 网络发送在独立协程进行，单次请求最长 10 秒；一个收件人失败不影响其他收件人。最多排队 2 份汇总，队列满时丢弃新汇总并输出诊断，不阻塞采集。发送失败不自动重试、不落盘；退出取消在途请求，未到周期的统计和待发汇总不会补发。网络失败不一定表示未送达。
- 日志中的 `recipient 1/2/...` 对应配置列表顺序；错误码 `230013` 表示收件人不在机器人可用范围内。凭证仅由后台读取，不通过 `/api/state` 返回，不写进通知日志。启用通知会向飞书官方 HTTPS API 外发上述摘要。

## 通用日志排查

启动工具后打开顶部 **日志排查**（`/logs`），选择或拖入一个任意后端的文本日志。**这是独立导入，不读取 SQL 配置的 `--log`，也不依赖 SQL 采集状态**；SQL 使用 `--stdin` 或 `--log` 文件缺失时仍可使用。支持 `.log`、`.txt`、`.jsonl` 以及其他扩展名，不要求 SQL 格式，不推断时间、级别或请求归属。SQL 页和只读 SQL API 不变。

**选择或拖入即确认上传和显示未经脱敏的原文。** 浏览器通过 `File.slice` 每次发送最多 **1 MiB**，不整文件读入内存；服务在本机临时目录保存独立副本，不外发、不修改或删除用户原文件。可查看上传进度、取消、完成后浏览末尾/搜索/上下文/分页、导入另一个文件或移除副本。每页一次一个导入，替换时先删除旧副本。日志可能包含口令和个人信息，请勿共享页面或复制敏感内容。

### 导入限制与生命周期

- 固定默认上限：**2 GiB/文件、最多 4 个文件、总声明长度 4 GiB**，含未完成上传；创建时预留容量，避免并发超额。最多 4 个并发上传请求，请求体最多 1 MiB（元数据 4 KiB）。
- 第一次有效导入时才创建进程私有临时目录（`os.MkdirTemp`，0700），副本 0600；随机 256-bit ID 作为能力凭证，文件路径仅由服务生成。显示名去除路径/控制字符，限 180 UTF-8 bytes，永不用于文件路径。不要分享 ID/游标。
- 所有字节按顺序上传，偏移必须等于已提交长度；失败块不推进偏移，可重试相同块。收到全部声明字节并 finalize 后才允许读取；空文件也须 finalize。没有任意路径参数或 SQL 来源回退。
- 副本是静态快照，**不跟随原文件新增**；需要新内容请重新导入。上传期间不要修改所选文件，副本只代表实际收到的字节。
- **30 分钟无访问后过期**（创建、成功块上传、finalize、已完成副本读取续期），每分钟清理并在访问时检查。取消/移除会删除副本；正常 SIGINT/SIGTERM 退出清理目录并停止清理协程。读取持有租约，移除等待活跃读取结束，之后访问返回 410。
- ID 只保存在页面内存。关闭/刷新页面尽力发送删除请求；未能发送的副本依靠过期清理。服务崩溃、SIGKILL 或断电无法保证清理；可能需手动清理系统临时目录下遗留的 `sql-monitor-imports-*`，不要删除原日志。删除不是安全擦除。

### 有界搜索

- 字面、区分大小写搜索；每行仅前 **16 KiB** 可搜索，超长部分有界跳过，展示带截断标记。无末尾换行的末行也可查，非法 UTF-8 显示为替换字符。
- 浏览器逐块扫描，每次约 2 MiB、每页最多 100 条，可取消/继续；仅保留当前页原文和最近 20 页游标，不缓存全部匹配。进度与定位使用字节偏移。
- 末尾最多读取 256 KiB；选中日志的前后文各最多读取 256 KiB，默认前后各 10 行。超长物理行可能使可展示行数不足。
- 搜索仍按原文匹配。JSON 行在列表显示级别、uid、tenant 和接口摘要，前后文缩进展开 `resp=` / `req=`。无后台搜索任务、无数据库、无新依赖或外部网络请求。签名游标绑定导入 ID、版本和查询，不能跨文件重放。

### API

全部请求保留可信 loopback Host / Origin 检查、无 CORS、`no-store` 等安全头。只有下列**精确导入端点**允许写方法（不接受查询串或转义路径别名）；SQL 路由仍仅 GET。写请求必须有且只有一个与 `http://` + 请求 Host **完全一致的 Origin**，缺失/null/其他源均拒绝。元数据使用 `application/json`，块使用 `application/octet-stream`，不接受表单上传。

| 路由 | 参数与返回 |
| --- | --- |
| `POST /api/logs/imports` | JSON `{"name":"application.log","size":123}`；创建未完成导入，201 返回 `id,name,size,offset,complete,chunk_size,expires_at`。初始 `offset=0,complete=false,chunk_size=1048576`。 |
| `POST /api/logs/imports/chunk` | `X-Log-Import: <id>`、`X-Log-Offset: <十进制已提交偏移>`；原始字节体 1..1048576 bytes，不能超出声明长度。200 返回更新后的导入信息。失败/不确定的块可重试原偏移，已提交偏移重放返回 409；页面在上传错误时移除副本并要求重新导入。 |
| `POST /api/logs/imports/finalize` | `X-Log-Import: <id>`，JSON `{}`；校验声明长度、已提交长度、物理长度一致后完成，200 返回导入信息；可重复 finalize。 |
| `DELETE /api/logs/imports` | `X-Log-Import: <id>`，JSON `{}`；取消未完成或移除已完成导入，200 `{"removed":true}`。 |
| `GET /api/logs/search` | 必须 `raw=1&id=<id>`；`q` 默认空（全部行），单行 UTF-8，最多 1024 bytes；`limit=1..100` 默认 100；`cursor` 首次省略，之后原样回传，关键词须与游标一致。 |
| `GET /api/logs/tail` | 必须 `raw=1&id=<id>`；`limit=1..100` 默认 100；可带该导入的 `version` 校验。返回导入快照末尾，不读取原文件。 |
| `GET /api/logs/context` | 必须 `raw=1&id=<id>`、`version` 和物理行起点 `offset`；`before`、`after` 各 `0..50`，默认 10。 |

读取统一返回 `lines: [{offset,text,truncated}]`，以及 `version,cursor,start_cursor,size,offset,done,window_start,window_end,limited`；`cursor` 是下一块，`start_cursor` 可重放本块；非搜索游标为空，搜索窗口字段为零。未知/重复/非法查询或长度参数 400；不可信来源 403；方法 405；取消读取 408；未完成、偏移/游标/版本冲突、非法行起点 409；不存在/已移除/过期 ID 410；文件/请求体过大 413；内容类型 415；容量或并发超限 429；磁盘写入/清理失败 500（不泄露服务路径）。并发读取最多 4 个，请求取消由 `request.Context` 传入扫描；700ms 协作式时间预算不能抢占内核中的普通文件读取。

合成基准（仅生成 500,000 条混合日志，无生产数据）：

```bash
go test -run '^$' -bench BenchmarkLogs500k -benchmem -benchtime=1x -v
```

## 零业务代码侵入接入现有 Go 项目

**前提：应用已经输出支持格式的 SQL 日志。** 工具只读文件或输入流，不注入业务代码、不改应用配置，也不会自动开启 GORM 日志。没有日志就无法观察；“未观察到 SQL”不等于“没有执行查询”。

编译工具后，在想放监控配置的工作目录运行其二进制：

```bash
/path/to/sql-monitor init
/path/to/sql-monitor --config ./sql-monitor.json
```

`init` 创建权限为 `0600` 的 `sql-monitor.json`，**拒绝覆盖任何已有文件或符号链接**：

```json
{
  "source": {"mode": "file", "path": "./application.log"},
  "port": 8767,
  "max_events": 2000,
  "initial_bytes": 2097152
}
```

将 `source.path` 改成应用实际输出的日志路径。它只是**监控器的输入配置**，不是应用配置；不会创建源日志、编辑 `.gitignore`、读取应用密钥或探测数据库。只对当前目录的 `go.mod` 做有限检测，可能提示 Go module / GORM 依赖，但不会推断应用日志开关或启动参数。

已有 SQL 输出在 stdout 的程序，可直接运行 `your-existing-command | /path/to/sql-monitor --stdin`。如果应用把 SQL 输出到 stderr，自行决定是否使用 `2>&1` 合流；混合输出可能增加忽略行、解析错误或破坏双行日志的邻接关系。工具不会猜测每个项目的启动参数。

若应用没有输出 SQL，先根据该应用已有配置与 logger 的实际行为启用合适日志等级；标准 GORM 默认 Warn 通常只输出慢查询和错误，不输出所有正常查询。工具不能补回没写出来的日志。需要保留原终端输出时可使用 `tee` 写日志文件，再让监控只读跟随；注意 `tee` 默认覆盖、`tee -a` 才追加，文件可能含敏感参数。

## 配置与参数

配置文件**必须显式指定 `--config`，不自动发现**。优先级：默认值 < 配置文件 < 显式 CLI 参数（与参数顺序无关）。

- 配置内相对 `source.path`：相对配置文件所在目录。
- CLI `--log` 与 `--config` 的相对路径：相对启动时工作目录。
- 不展开 `~` 或环境变量；需要时用 shell 展开或绝对路径。
- `source.mode` 接受 `file` / `stdin` / `k8s`。stdin 配置为 `{"source":{"mode":"stdin"}}`，不可带非空 path；k8s 配置见「Kubernetes 日志源」一节。
- `--log` 覆盖配置中的 stdin 模式；`--stdin` 覆盖配置中的文件模式。显式同时提供 `--stdin` 与 `--log` 会报错（包括 `--stdin=false`）。`--k8s-*` 标志与显式 `--stdin` / `--log` 同时出现也会报错。
- JSON 限制 64 KiB，拒绝未知字段和非法类型。可省略字段以使用默认值。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--config` | 不读取配置 | 显式 JSON 配置路径 |
| `--stdin` | false | 从输入流读取，永久 EOF 后保留 UI |
| `--log` | `/tmp/data-service-local.log` | 兼容旧版本的只读文件默认路径 |
| `--port` | 8767 | 仅监听 `127.0.0.1` |
| `--max-events` | 2000 | 保留条数，1..2000 |
| `--initial-bytes` | 2097152 | 初次文件读取末尾最多 2 MiB；0 表示仅新增；stdin 不使用此限制 |
| `--k8s-env` | 配置里的 `default` | 选择 `k8s.environments[]` 中的环境名 |
| `--k8s-lane` | 环境里的 `lane` | 覆盖 lane 标签值（`x2robot.io/lane`） |
| `--k8s-kubeconfig` | 环境里的 `kubeconfig` | 覆盖 kubeconfig；也可脱离配置文件独立使用 |
| `--k8s-context` | 环境里的 `context` | 覆盖 kubeconfig context |
| `--k8s-namespace` | 环境里的 `namespace` | 覆盖命名空间；留空则用 context 默认 |
| `--k8s-app` | 环境里的 `app` | 覆盖 app 标签值 |
| `--k8s-container` | 环境里的 `container` | 多容器 pod 时指定容器 |
| `--k8s-tail` | 200 | 首次回溯行数；0 表示不回放 |
| `--k8s-kubectl` | PATH 里的 `kubectl` | kubectl 可执行文件路径 |

## Kubernetes 日志源（source.mode = "k8s"）

用 `kubectl logs -f` 拉集群里某个 lane 的 data-service pod 日志，喂进现有解析/UI 通路。实现走 `kubectl` 子进程，不引入 client-go，保持工具零外部依赖；本期不做 UI 内切换环境。

```json
{
  "source": {
    "mode": "k8s",
    "k8s": {
      "default": "dev2",
      "environments": [
        {
          "name": "dev2",
          "kubeconfig": "~/Downloads/ns-cloud-dev.readonly.yml",
          "context": "dev-user-context",
          "namespace": "ns-cloud-dev",
          "app": "data-service",
          "lane": "dev2"
        }
      ]
    }
  }
}
```

- `k8s.environments[]` 是列表（不是 map），保留顺序，`name` 唯一标识环境。
- `k8s.default` 是 `--k8s-env` 缺省时选中的环境；`environments[].kubeconfig` 相对路径相对配置文件目录解析（与 `source.path` 一致），不展开 `~` 或环境变量。
- `app` 必填，避免 `kubectl logs -l` 无选择器时全量拉日志；`lane` 是环境标签值。
- `lane_label` 可选，覆盖 lane 标签键（默认 `x2robot.io/lane`）；多集群标签键不一致时可写如 `team.io/rail`，最终选择器为 `app=…,<lane_label>=<lane>`。
- `tail` 是首次回溯行数（默认 200），指针字段可显式写 0 表示不回放。

启动：

```bash
./sql-monitor --config sql-monitor.local.json --k8s-env dev2
# 换环境无需改配置：./sql-monitor --config sql-monitor.local.json --k8s-lane dev3
```

`/api/status` 的 `source_mode` 为 `k8s`，`path` 显示 `ns-cloud-dev lane=dev2 app=data-service`。

**lane 语义与权限边界**：目标集群在同一个命名空间内用 pod 标签区分环境，标签 `x2robot.io/lane`（与 `env` 一致）取值 `dev` / `dev1` / `dev2` / `dev3` / `test`。当前只读 kubeconfig 的 ServiceAccount 仅授予 `ns-cloud-dev` 命名空间的 pods / pods/log 等资源读取权限；`ns-cloud-test` / `ns-cloud-prod` 等其它命名空间一律 Forbidden，无法绕过。要读 test 命名空间需管理员签发另一份 kubeconfig，届时在 `k8s.environments[]` 加一条即可。

**滚动发布**：发版会换 pod，`kubectl logs -f` 随之退出，工具自动退避 1 秒后用 `--tail=0` 重连（不回放，避免重复统计），重新解析 `-l` 选择器接上新 pod。

工具自动识别以下三种格式，没有任意日志自动理解功能。可以在同一输入中遇到不同格式，但 GORM 两行必须相邻。

## 支持的日志格式

### 标准 GORM 双行日志

```text
2026/09/24 10:20:30 /demo/main.go:42 SLOW SQL >= 200ms
[203.100ms] [rows:1] SELECT id FROM demo_items WHERE id = 42
```

支持标准日期/时间＋`.go:行号` 头部、ANSI 颜色、普通 SQL、`SLOW SQL >= duration` 和错误信息头部。`[rows:-]` 显示未知；不会当作 0。错误信息仅在显式原文接口返回。时间按监控进程本地时区解释；若应用时区不同，时间筛选可能偏移。

GORM 可把 SQL 拆成真实多行。工具保守地**只接受紧邻头部的第一个 SQL 物理行，并始终标记完整性不确定**；后续正文不拼接、不当成独立 SQL。孤立耗时行、格式损坏、缺失 SQL 的头部会形成解析失败或不完整记录计数。请求正文不能仅因包含 SELECT 被采集。自定义前缀、第三方 logger 或交错双行日志不保证支持。

### 约定结构化 JSON（推荐完整 SQL）

每行一个对象，SQL 内换行须 JSON 转义：

```json
{"level":"DEBUG","ts":"2026-09-24T10:20:30Z","msg":"SQL Query","sql":"SELECT id FROM demo_items WHERE id = 42","elapsed":"12.5ms","rows":1,"source":"internal/data/demo.go:42","trace.id":"11111111111111111111111111111111","span.id":"2222222222222222"}
```

- `msg` 必须为 `SQL Query`、`Slow SQL` 或 `SQL Error`，`sql` 必须为非空字符串。
- `elapsed` 使用 Go duration，如 `12.5ms` / `500µs` / `1.2s`；缺失或无效显示未知。
- `rows` 是返回／影响行数，不是扫描行数。
- `source`、`level`、`ts` 可选，时间建议 RFC3339。
- `trace.id` / `span.id` 可选，仅接受 32 / 16 位非全零十六进制；不生成虚假 trace。
- `error` 为可选原始错误，仅显式原文接口返回。

### 既有 CompactLogger

```text
[2026-09-24T10:20:30Z] DEBUG [demo] [] caller.go:10 - source=internal/data/demo.go:42 | SQL Query | elapsed=12.5ms | rows=1 | sql=SELECT id FROM demo_items WHERE id=42
```

继续兼容原服务 JSON / Compact 格式。Compact 同样仅保留单个物理 SQL 行、标记完整性不确定。MySQL 慢日志、任意 JSON、其他 ORM 文本不自动支持。

## 页面诊断与边界

- 能力条显示启动以来累计输入字节、已识别 SQL、各格式 SQL 数量，以及具有有效耗时／代码位置／trace 的 SQL 数量。它们不随内存淘汰归零；**表格聚合仅覆盖保留事件和筛选范围**。
- 页面同时显示忽略行、解析失败、超长行、不完整记录和轮转次数，不显示被忽略的原始正文。
- 无 SQL：先检查输入字节和最近读入时间，再检查忽略/解析计数及应用真实日志格式；切换“全部 SQL”并清除筛选。“值得关注”为空也可能只是没有慢查询或错误。
- 文件每约 200ms 跟随，缺失或不可读时重试，临时 EOF 不结束最后一行。支持截断、重建和轮转时重置 framing；两次读取之间已丢失的数据无法恢复。
- stdin 有界读取；永久 EOF 才提交无末尾换行的有效最后一行。读取错误不提交未完成行；错误后不自动重连。EOF / 错误后 UI 保留；Ctrl+C 即使输入闲置也能退出。
- 单物理行最多 1 MiB，超长跳过并计数；只缓存一个有界 GORM 头部。SQL 原文最多 64K 字符、模板 16K 字符、错误 8K 字符；截断时标记。无磁盘事件存储。
- 超过容量自动淘汰最早事件及其原文。暂停只暂停页面刷新，清空视图只隐藏已采集事件；后台仍继续采集。详情是打开时快照，已淘汰原文无法读取。
- 相同模板＋位置重复不等于 N+1，慢查询不等于缺索引；缺少 trace 无法可靠关联接口或定时任务。日志无法解释锁等待、扫描量或接口总耗时。

## 安全与运行

参数默认替换为 `?`，注释默认隐藏；双引号内容也被隐藏，其他 SQL 方言的模板不保证语义保真。它不是完整的敏感信息脱敏器：表名、列名、代码路径仍可见，原始 SQL 暂存在本机内存。勿公开页面或原文。

HTTP 保留只读 SQL 路由、Host / Origin 校验、无 CORS、无缓存与原文显式访问约束；只有上述导入端点可写入/移除临时副本，但不是多用户鉴权系统。不要代理到外网。源日志的权限、磁盘占用与轮转由使用者管理。

`index.html` 和 `logs.html` 内嵌到二进制，程序可从其他目录运行；修改页面需重新构建。当前验证平台为 macOS；stdin 实现使用 Unix 非阻塞文件描述符，Windows 不支持。工具可 `CGO_ENABLED=0 go build`，仅演示 SQLite 和 race 测试要求 CGO。

端口占用时先打开已有页面，或改 `--port 8768`，不要结束不明进程。连接中断时检查 `sql-monitor` 进程是否仍运行。

## 项目结构

代码按依赖方向分层，库代码全部在 `internal/`，进程入口在 `cmd/sql-monitor`：

- `cmd/sql-monitor`：唯一 `main`，注册 SIGINT/SIGTERM 后委托给 `app.Run`。
- `internal/app`：运行时装配——解析 CLI 与 JSON 配置、启动 HTTP 服务与数据源/通知协程；定义通知接口 `Notifier`。
- `internal/config`：JSON 配置加载、校验与 `init` 生成（0600 权限），零内部依赖。
- `internal/monitor`：核心采集与聚合，暴露数据源 `Source` 与 `Sink` 接口，零内部依赖。
- `internal/source/k8s`：`kubectl logs -f` 数据源，实现 `monitor.Source`。
- `internal/feishu`：飞书通知渠道，实现 `app.Notifier`。
- `internal/web`：HTTP 处理器、`go:embed` 页面与导入存储。
- `examples/`：`gorm-demo`（独立 module 的演示）与 `python`（归档对照）。

依赖单向：`config`、`monitor` 为最底层；`web` 与 `source/k8s` 依赖 `monitor`，`feishu` 依赖 `config` 与 `monitor`，`app` 装配全部。数据源与通知渠道都是接口，新增渠道（其它 IM、云日志源等）只需实现接口并在 `app` 装配，无需改动核心。无第三方依赖、无数据库，可直接开源。

## 开发与验证

在工具根目录执行：

```bash
CGO_ENABLED=1 GOPROXY=off go test -race ./...
go vet ./...
```

`examples/gorm-demo` 是独立 Go module，**根目录 `./...` 不包含它**，还需显式执行：

```bash
(cd examples/gorm-demo && CGO_ENABLED=1 GOPROXY=off go test -race ./...)
(cd examples/gorm-demo && CGO_ENABLED=1 GOPROXY=off go vet ./...)
```

演示测试构建真实监控二进制，把实际 GORM 日志分别经 stdin 和文件输入，验证 SQL 数量、慢查询/错误、重复指纹、能力计数、默认脱敏、显式原文和 EOF 后 HTTP 可用性；临时服务随测试退出。GORM v1.31.1、SQLite driver v1.6.0，间接 x/text 使用缓存可用的 v0.21.0。

`examples/python/monitor.py` / `test_monitor.py` 保留为归档对照，未修改，不是 Go 运行依赖。
