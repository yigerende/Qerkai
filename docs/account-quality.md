# 降智检测

## 部署与迁移

1. 先升级 Qerkai，启动时自动执行 `236_account_quality_detection.sql`。
2. 在“公告”上方的“降智检测设置”配置并开启检测。默认关闭，避免升级后自动消耗额度。
3. 两个 space 的原题库仍保存在原数据库，可“导出原检测配置”，在 Qerkai 的“检测配置”导入后保存。导入不会自动开启。
4. 再升级 space，设置查询间隔、结果有效期、条件组合及踢出/切组动作。space 不再发起答题或读取使用日志。

## 检测行为

- 目标为所有未删除的 OpenAI 账号。Qerkai 复用现有单账号测试服务及该账号的网络配置；space 不传代理。
- 答题与模型日志独立定时，答题并发 1-8（默认 4），模型查询并发最多 2，每周期最多 32 个到期账号；同一类任务用 PostgreSQL 会话锁防多实例重入。
- 标准答案支持纯答案或唯一的 `FINAL_ANSWER=` 行。也支持关键词、正则；可只看内容、只看耗时、或同时判断。耗时必须小于阈值。
- 每次有效答题后换下一题。网络、401、429、空响应及不完整响应记为检测失败，不累计答错次数，不切换题目。
- 模型检查只读每账号最近 3 条匹配发送模型的日志，不取全部日志；旧日志不会重复计数。相同模型的日期/latest 版本后缀视为版本别名。
- 两项异常/恢复次数独立。保存配置生成新版本，在途旧版本结果不对外生效。
- 结果与历史独立存表，不修改账号原调度、分组、状态。历史每账号有上限，默认 200。

## 对外接口

均在现有 admin 鉴权下，使用已有管理员令牌/管理密钥，不开放匿名访问。

| 方法 | 路径 | 参数 |
| --- | --- | --- |
| GET | `/api/v1/admin/account-quality/capabilities` | 无 |
| GET | `/api/v1/admin/account-quality/accounts/:id` | 单账号 ID |
| POST | `/api/v1/admin/account-quality/results` | `{"account_ids":[1,2]}`，最多 100 个唯一正整数 |
| GET | `/api/v1/admin/account-quality/accounts/:id/history` | 历史上限按已保存配置 |
| GET/PUT | `/api/v1/admin/account-quality/settings` | 设置与统计/保存完整配置 |
| POST | `/api/v1/admin/account-quality/run` | `{"account_ids":[]}` 全部，或指定最多 100 个；仅设置到期时间 |

结果接口返回 `version:1`、`server_now`、精简策略 `settings` 和 `accounts`。每账号有 `revision`、`version`、`question`、`model`；两种结果分别带 `status`、`degraded`、`failures`、`successes`、`checked_at`、`evidence_at`、`streak_started_at`、`next_at`、`error`。无有效结果时 version 为空。

结果读取不产生探测任务。最多两个并发查询，每次 5 秒数据库超时；账号页面只按当前页分批查询，隐藏列后停止该列查询。

space 只在所选条件有当前轮转周期的新鲜、已确认结果时执行动作；未知、过期、错误及禁用检测不当作异常。正常恢复同样需要新鲜证据。重复读取不累加次数，已完成动作不重做，失败动作可重试。同母号的踢出仍走原串行及间隔控制。

## 验证范围

单元测试覆盖答案匹配、连续计数、换题、模型日志去重、只读批量查询、版本隔离、锁竞争、单账号测试复用、space 过期/旧周期/错误/重复结果、切组恢复与同母号串行。另用隔离 PostgreSQL 实测迁移重复执行、双实例抢锁、32 账号有界并发、两种结果同时保存、历史上限和新服务实例读取。页面使用隔离模拟 API 检查，不调用真实账号。
