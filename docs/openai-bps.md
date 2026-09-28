# OpenAI BPS 内置端点

协议基线：CPA `cpa-plugin-oai-basispoints` **v0.2.2**，提交 `25ec8d06225edbb6f4293ffc29fba15960e43a30`。

直接移植协议实现及测试，替换 CPA 宿主回调为 Qerkai HTTP 适配。原 MIT 许可证、来源及改动说明见 `backend/internal/pkg/basispoints/`。不包含上次试验版本的兼容或配置迁移。

## 使用方法

在系统设置的网关转发区域，强制 WS 上方打开“启用 OpenAI BPS 端点”，选择全部分组或指定分组，填写启用模型（每行一个），保存。

- 默认关闭，仅限 **OpenAI OAuth**。API Key、setup-token、其他平台账号及未命中的分组/模型沿用原路径。
- 与强制 WS、WS 优化调度互斥。界面切换时自动关闭另一套，保留配置参数；后端拒绝冲突保存。两套均可关闭。
- 普通模型无需填写映射。例如 `gpt-5.5`、`gpt-5.6-sol`、`gpt-6-astra` 各自请求同名上游。
- BPS 设置里的“上游传输方式”独立控制 HTTP / SSE 或自动模式。新配置和升级前的旧配置默认 HTTP；选择自动后，对命中 BPS 的请求优先尝试 WS，不关联账号级 Responses WS、ctx_pool、passthrough 或强制 HTTP 设置。账号代理仍按原配置使用。
- 自动模式每轮生成独立建连、最多尝试一次握手，默认超时 5 秒，可设 1～30 秒。连接失败、握手超时、404/5xx 等可回退同一 BPS 端点的 HTTP；401/403/407/429 不回退。有效 WS 建连后发送失败、断流、超时或协议错误不跨通道重发。未交付时工具格式纠正的一次重生成仍沿用参考规则。
- `-bps`、`-basispoints` 别名可使用默认上游模型，或者填写 `别名=上游模型`。高级配置与参考插件对应：地址、默认模型、映射、超时、响应上限、认证模式、工具版本。
- 账号管理的测试窗口可选“原端点”或“BPS 端点”。默认原端点；BPS 测试要求总开关开启，使用该账号及其代理，不依赖分组、不代换账号。
- 配置模型名单不代表上游承诺支持该模型；实际权限和可用性通过测试确认。

## 请求、工具和缓存

命中时使用 `https://bps.openai.com/basispoints/api/responses`：HTTP 模式发送 POST，自动模式先尝试对应的 `wss://` 握手并发送 `response.create`。下游可用 Responses、Chat Completions、Claude Messages 或 Responses WS。WS 入口保留原来的并发槽位和每轮计费钩子；BPS 不使用 Codex WS 连接池、State 注入或 502/503 业务重试，也不会回退原 Codex 端点。

复用 Qerkai OAuth 刷新和 HTTP 连接池，按参考实现设置 OAuth/账号身份以及 Office/Excel 客户端头。请求体、推理等级、turn/task 标识、agent_iteration、工具目录、工具中继和终态验证沿用参考源码。

- `run_officejs` 只作为中继封装，转换后交给客户端执行真实工具。function、custom、namespace、动态工具目录、工具选择、并行约束、JSON Schema 参数校验及补丁原文保持参考行为。
- 后续请求须携带完整历史；工具结果回放保持原始调用身份。工具缓存按 API Key、账号、模型和会话隔离，不接受客户端伪造内部缓存范围。
- 自定义工具使用 `ctc_` 条目 ID；历史 `call_id` 保持配对。即使原生工具缓存已被淘汰，或当前回合没有原工具目录，也根据历史中已有的名称和原文重建中继；新调用仍受当前目录和 tool_choice 限制，历史不因此重新执行。不对畸形 function JSON 进行猜测修复。
- 正文实时增量交付；工具整批校验后交付。未交付时非法工具最多纠正重生成一次；已交付（包括网络首帧通知）后出现工具错误、断流或失败，输出错误事件，不重跑、不发送成功终态。
- BPS 同样遵循“OpenAI Responses 首 token 统计口径”：语义/可见输出复用既有事件判断，网络首帧记录真实上游非终止事件并即时发送 `response.created` / `response.in_progress`。统计发生在协议缓冲之前，不将终态重建出的通知当作上游网络首帧；工具仍需整批校验。客户端 HTTP/SSE、WS 及兼容格式均使用同一计时机制。
- 内嵌图片上传同源 attachments。附件缓存上限 512 条，同凭据相同图片合并并发上传；跨凭据隔离，失败不缓存，等待者可取消。
- 保留 prompt_cache_key，usage 和 cached_tokens 使用实际返回值；缓存命中由上游决定。
- 账号调度、并发限制、最终使用记录和计费沿用宿主。记录实际 BPS 端点、真实返回模型。运维页面没有改动。
- 使用记录的“类型”按实际 BPS 上游传输记录：上游 WS 显示 WS；HTTP 则按请求显示流式或同步。自动模式发生 HTTP 回退时也如实记录，不按客户端入口或配置值推测。
- 账号 BPS 测试会显示实际使用 WebSocket 还是 HTTP/SSE；降智检测和所有入口使用同一传输配置。WS 消息复用正文增量、完整工具校验和首 token 观察逻辑，客户端取消会立即关闭握手中或读取中的连接。
- 降智检测（答题、无日志 `hi` 补测、调度恢复复检）对命中 BPS 模型且属于启用分组的 OpenAI OAuth 账号走 BPS；全部分组模式不要求账号分组。检测直接比较真实上游模型，不受下游模型显示改写影响，别名按实际发送的映射模型比较。模型日志在 SQL 限制最近 3 条之前按端点过滤，BPS/原端点不混用证据；没有匹配日志则主动补测。关闭 BPS 或未匹配时沿用原测试及 State 注入路径。已有 State 有效期调度限制保持原规则。
- 管理员使用记录新增默认可见的“端点”列，依据每条记录的 `upstream_endpoint` 显示 BPS / 原端点；悬停显示具体上游路径。原入站/上游路径列改名为“请求路径”，历史记录未保存端点时显示 `—`，不按当前开关倒推。

## 参考版本边界

与 v0.2.2 一致：不支持 Fast/priority/flex、独立 compact、仅 previous_response_id 续聊、多代理 v2 密文、JSON/schema text.format、精确独立 count_tokens；这些请求明确报错。text.verbosity 沿用参考行为，未实现上游映射。

WS 没有跨请求连接池，缓存读仍以真实上游 usage 为准。自动模式在握手失败时可能增加配置的握手等待；本次升级不保证消除上游 403，也不保证 WS 一定比 HTTP 快。HTTP/HTTPS/SOCKS5 等账号代理继续由宿主维护；BPS WS 支持 HTTP CONNECT、SOCKS5/SOCKS5H，不支持的代理类型直接交回同代理的 HTTP 路径，不绕过代理。

当前附件上传适配针对图片。真实测试中，TXT、PDF、DOCX 通过 `input_file + file_data` 直传均收到上游 HTTP 422，PDF 的 data URI 和纯 Base64 两种写法都失败；因此不能把文档附件直传视为已支持。客户端先提取 PDF / DOCX 文字、再作为文本或工具结果提供内容可用。此结论仅针对已测试的请求方式，不推断上游所有潜在文件协议。

模型目录复制对应规范模型的容量、apply_patch 和已支持客户端工具能力，去除 Fast、Code Mode、Responses Lite、多代理 v2 的能力声明，并重新计算 ETag。

关闭 BPS 后的新请求按原路径执行；进行中的请求按原快照完成。BPS WS 会话在每轮复查路由，路由不再匹配时明确结束会话，要求重连，避免跨端点混用工具历史。配置在启动时加载、保存后即时发布，过期后后台刷新，普通请求不等待 SQL。

## v0.2.2 全面对照复核（2026-09-28）

逐文件对照上述版本的源码、README、更新记录和测试：参考协议目录共 14 个实现文件，13 个已保留或适配；34 个 Go 测试文件中保留 32 个，只有模型配置夹具的读取路径作了调整。剩余实现是 CPA 源认证文件管理，剩余两个测试分别验证该管理功能和 CPA 插件发行包。

| 范围 | Qerkai 对照结果 |
| --- | --- |
| 请求地址、OAuth/账号身份、Office/Excel 请求头、User-Agent | 使用同版源码，应用层生成规则一致；底层 HTTP 仍由 Qerkai 宿主管理。 |
| 模型映射、推理等级、上下文策略、task/turn/iteration | 保留同版构造和校验规则，模型目录保持补丁能力与参考限制。 |
| HTTP/SSE、上游 WS、代理、超时、取消 | 保留一次握手、安全回退和已建连不跨通道重放规则；取消绑定 Qerkai 请求上下文。 |
| 编程工具 | function、custom/apply_patch、namespace、动态工具目录、时钟/异步提问中继、工具选择及整批校验均保留。工具由客户端执行。 |
| 工具历史 | 保留 ctc_ 修复、完整历史、缺失目录/缓存淘汰后的重建；增加 Qerkai 租户/账号/模型/会话缓存隔离。 |
| 图片与缓存 | 同源图片上传、512 条缓存、凭据隔离和并发去重；prompt_cache_key 及真实 usage/cached_tokens 保留。 |
| 流式与失败 | 正文真实增量、索引和空白保留；工具完整校验；错误后不伪造成功，未交付时最多一次工具格式纠正。 |
| Qerkai 入口与管理 | Responses、Chat、Messages、客户端 WS、账号测试和降智检测共用 BPS 执行器；分组/模型范围、日志用量及开关回归通过。 |

按用户要求保留的宿主差异：BPS 的传输权限只由 BPS 配置控制，默认 HTTP，与账号级 WS 无关；首帧口径继续使用 Qerkai 设置；OAuth 更新、配置持久化、账号及管理员鉴权由 Qerkai 负责。CPA 的动态库注册/商店发行、源 JSON 编辑页面、页面管理密钥记忆和该 JSON 内的 WS 开关不移植。

本轮另补齐客户端 WS 两轮请求的 HTTP→HTTP、WS→WS、HTTP→WS、WS→HTTP 集成验证，全部通过。每轮读取当前 BPS 配置，每次 WS 生成独立建连，账号 WS 关闭和强制 HTTP 字段不覆盖 BPS 选择；模型映射、缓存用量和断开清理保持正确。修正了两处仍称 BPS 只用 HTTP 的旧注释，运行逻辑没有新增调整。

对齐范围包含参考项目本身的限制：Fast/priority/flex、独立 compact、仅 ID 续聊、代理 v2 密文、JSON Schema 输出及精确 count_tokens 仍不支持；文档附件直传不作为已验证能力。代码和模拟测试对齐不等于真实上游账号权限、WS 可用性或 403 问题已解决。

## v0.2.2 升级验证（2026-09-28）

- 协议包 124 个顶层测试、675 个测试及子测试全部通过，无跳过；包括参考版本的 WS、错误边界、工具历史回放、工具选择及 Qerkai 原生适配测试。
- 服务、设置接口及仓储相关回归 66 个顶层测试、165 个测试及子测试全部通过，无跳过。调度恢复使用本地 PostgreSQL 的隔离 schema 验证，测试结束后清理。
- BPS 的 HTTP/自动模式分别验证账号 WS 未设置、关闭、ctx_pool、passthrough、强制 HTTP，不受这些账号字段影响；同时验证模型/分组/账号类型范围、互斥保存及旧配置默认 HTTP。
- 真实本地 WS 套接字验证流式/非流式、Responses/Chat/Messages 转换、真实模型观察、网络通知即时下发、断开取消、HTTP CONNECT 账号代理、custom apply_patch 原文与工具结果往返。后续回合没有当前工具目录时，历史仍完整回放。
- 模拟两账号通过 HTTP 100/500 并发和 WS 100/500 并发。WS 明确保持全部请求同时在途，峰值分别为 100、500；独立运行耗时约 50/664 ms，仅代表短响应测试夹具。
- Windows 本机瞬间发起 500 次 TCP 建连曾出现 `connectex: actively refused`。500 并发测试改为约 1 ms 间隔建立连接，所有连接到齐后才释放响应；没有修改生产握手次数或回退规则，也不据此宣称瞬时 500 建连或真实上游压测通过。
- 关闭 BPS 的路由判断基准约 20 ns/op，0 B/op、0 allocs/op；强制 WS、分组范围、原 WS 优化池和 HTTP State 注入相关回归通过。
- 前端设置、账号测试弹窗、分组选择共 48 项测试通过；修改文件 lint、类型检查和生产构建通过。Windows 和 Linux amd64 后端构建通过。
- 本次未调用真实 OpenAI 账号，不将模拟缓存 usage 当作真实缓存命中证明，也不据此宣称解决上游 403。Windows 当前缺少 C 编译器，未运行 Go race detector。

新增自动化检查：

```powershell
# backend；调度恢复测试另需 QUALITY_TEST_PG_DSN 指向本地 PostgreSQL
go test ./internal/pkg/basispoints -count=1
go test -tags unit ./internal/service ./internal/handler/admin ./internal/repository -run 'TestBPS|TestAccountQualityBPS|TestModelAudit' -count=1
go test -tags unit ./internal/service -run '^TestBPSUpstreamWSConcurrentStreams$' -count=1 -v
```

## 验证记录（2026-09-27）

- 保留参考项目全部 22 个协议测试文件；只排除与 Qerkai 无关的 CPA 插件商店发行测试。
- 协议测试通过；额外覆盖原生适配与 CPA 请求/响应一致、补丁工具回放、租户隔离、图片 100 并发去重、取消释放、提交前纠正/提交后失败。
- 网关验证覆盖四种客户端入口、用量/缓存统计、互斥及局部保存、账号类型/分组限制、别名目录、8 个同名模型转发、开关关闭及账号端点测试。
- 本地两模拟账号：100 并发 100 成功、500 并发 500 成功。短响应场景耗时约 21/97 ms，进程 Go 堆采样峰值约 19/35 MiB；仅用于验证并发正确性，不代表真实 BPS 的网络吞吐或长上下文开销。
- 关闭开关的路由判断基准约 16 ns/op，0 B/op、0 allocs/op。过期配置遇到阻塞 SQL 时，请求仍立即返回原路径。
- OpenAI、调度、State、降智、设置及相关 handler/repository 回归通过；前端设置/账号测试通过，类型检查、构建及修改组件 lint 通过。
- Windows 当前未配置 C 编译器，未运行 Go race detector。
- 首 token / 降智检测补齐后：协议套件通过；服务与仓储回归共 85 个顶层测试通过、无跳过，包含三种口径、真实通知即时发送、终态重建不伪造网络首帧、通知后的工具校验和错误边界。
- 降智回归覆盖答题、无日志补测、原端点/BPS 日志隔离、别名及真实模型不一致；使用隔离 PostgreSQL schema 验证恢复复检及 100 个账号的并发恢复，100/500 并发 BPS 模拟也通过。
- 新后端已构建并重启本地 8081，健康检查和页面访问均为 200；随后使用用户新导入的账号完成下述真实流式复测。

### 本地真实账号验证

使用用户导入的 OpenAI OAuth 账号及其已配置代理，通过本地 `127.0.0.1:8081` 验证。测试时临时将账号加入本地 API Key 对应分组，完成后恢复原分组。

- 实际账号管理使用 `components/admin/account/AccountTestModal.vue`；端点选择已接入这个弹窗，并在重启后的浏览器确认可见、可切换。选择 BPS 的 `gpt-6-astra` 测试成功，输出实际 BPS 地址、模型、用量和缓存数量。
- `gpt-5.6-luna`、`gpt-5.6-sol`、`gpt-5.6-terra`、`gpt-6-astra`、`gpt-6-sol`、`gpt-6-luna` 均完成真实 SSE 请求，返回模型与请求相同。
- `gpt-5.5`、`codex-auto-review` 返回上游 HTTP 403，提示 `Model access has changed`。保留用户模型配置，不自动改用其他模型；这两个模型未验证可用。
- function 工具调用 → 本地执行 → 完整历史携带工具结果 → 模型最终回复，通过。
- custom `apply_patch` 返回有效原文补丁，创建隔离目录中的 Python 加法函数，两个本地断言通过；回传工具结果后模型回复 `CODE_OK`。
- 图片上传及识别通过，正确识别红色正方形。
- 客户端 Responses WebSocket 同一连接两轮完整历史请求均成功；第二轮上游报告 `cached_tokens=22307`。账号测试及普通 SSE 也观察到真实缓存命中，但不保证每次请求命中。
- 单个真实账号并发上限 40、客户端用户并发上限 100 下，30 个同时发起的 `gpt-6-astra` 流式请求全部 HTTP 200、收到完成事件并准确回复 `OK`；返回模型全部一致，30 条全部报告缓存命中。修正测试脚本逐字节读取 SSE 的性能问题后测得总耗时中位数约 8.22 秒、范围 3.73–19.97 秒，与服务端 30 条 HTTP 日志耗时吻合。未使用修正前的客户端耗时作性能结论。
- Responses 非流式及 Chat Completions 非流式成功。当前本地分组禁止 `/v1/messages`，该入口的实测被宿主权限规则返回 403，未改分组权限绕过；Messages 转换及流式行为有自动化测试覆盖。
- 端点列相关现有前端/语言检查共 50 项通过，lint、类型检查、构建通过；重启后浏览器确认新列默认可见，真实 BPS 日志的标识和路径均正确。
- 新账号的流式编程复测：custom `apply_patch` 创建两个 Python 文件，function `run_tests` 执行 4 个 unittest 全部通过，真实工具结果回传后模型正常结束。首份补丁缺少结束标记，工具拒绝执行并反馈错误后，模型自行补全；没有静默修补模型输出。
- 新账号图片复测正确识别随机标记、三个图形及其颜色；PDF / DOCX 提取文字后正确读取随机标记、项目名并计算金额。原始文档附件直传失败，见上方边界说明。6 条成功记录均为 BPS 流式、上游模型一致；网络通知先于回答 / 工具完成下发，服务端首 token 与客户端首事件时间相符。
- 缓存补测使用固定 `prompt_cache_key`：首次总输入 22377、缓存读 0；重复请求缓存读 22309；同会话携带完整历史继续请求，总输入 22401、缓存读仍为 22309。数据库缓存读与上游一致。此前按步骤更换会话标识的功能测试均未命中缓存，已修正测试脚本，未修改网关缓存逻辑。

这些结果验证当前账号和代理下的实际功能，不代表所有账号的模型权限或持续高负载可用性。

复现主要自动化检查：

```powershell
# backend
go test ./internal/pkg/basispoints -count=1
go test -tags unit ./internal/service ./internal/handler/admin ./internal/repository -run 'TestBPS|TestModelAudit' -count=1
go test -tags unit ./internal/service -run '^TestBPSGatewayConcurrentStreams$' -bench '^BenchmarkBPSDisabledRoute$' -benchmem -v
# frontend
pnpm exec vitest run src/views/admin/__tests__/SettingsView.spec.ts src/components/admin/account/__tests__/AccountTestModal.spec.ts src/views/admin/settings/__tests__/ForceOpenAIWSGroups.spec.ts
pnpm run build
```
